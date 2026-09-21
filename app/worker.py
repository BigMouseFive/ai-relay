"""一个浏览器 tab 对应一个 Worker。

Worker 以持久化 attempt 为边界执行任务：一旦发送 click 已发生但结果无法确认，
任务进入 outcome_unknown，而不是自动再次发送同一 prompt。
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Callable, Optional

from .response_contract import ResponseValidationError, validate_result
from .schemas import BackendType, TaskPhase, TaskStatus, WorkerState
from .sites.base import SendOutcomeUnknownError, SiteAdapter
from .store import TaskRecord, TaskStore
from .webbridge import WebbridgeError

logger = logging.getLogger("ai-relay.worker")

RECOVER_INTERVAL = 30
POLL_INTERVAL = 2
PAGE_LOAD_WAIT = 3
TAB_CLEANUP_TIMEOUT = 5


class TaskTimeoutError(RuntimeError):
    pass


class SentTaskError(RuntimeError):
    """消息已确认送出后，结果读取过程出现了不确定异常。"""


class TaskCancelledError(RuntimeError):
    pass


class Worker:
    def __init__(self, worker_id: str, site: str, model: str, adapter: SiteAdapter,
                 store: TaskStore, timeout_seconds: float = 600,
                 stall_seconds: float = 120, hard_timeout_seconds: float = 300,
                 on_retry: Optional[Callable[[TaskRecord, str, str], bool]] = None,
                 target_id: Optional[str] = None,
                 tab_group_title: str = "A"):
        self.worker_id = worker_id
        self.site = site
        self.target_id = target_id or f"legacy-webbridge-{site}"
        self.backend_type = BackendType.webbridge
        self.tab_group_title = tab_group_title
        self.model = model
        self.adapter = adapter
        self.store = store
        self.timeout_seconds = timeout_seconds
        self.stall_seconds = stall_seconds
        self.hard_timeout_seconds = hard_timeout_seconds
        self.on_retry = on_retry
        self.state = WorkerState.starting
        self.detail: Optional[str] = None
        self.current_task_id: Optional[str] = None
        self.done_count = 0
        self.fail_count = 0
        self._recover_task: Optional[asyncio.Task] = None
        self._run_task: Optional[asyncio.Task] = None

    def cancel_current_task(self) -> bool:
        if self._run_task and not self._run_task.done():
            self._run_task.cancel()
            return True
        return False

    async def start(self) -> None:
        self.state = WorkerState.starting
        self.detail = None
        try:
            await self._open_tab()
            if not await self.adapter.is_logged_in():
                self._degrade("未登录，请在浏览器中手动登录，将自动恢复")
                return
            await self.adapter.ensure_model(self.model)
        except Exception as e:
            self._degrade(f"启动失败: {self._error_text(e)}")
            return
        self.state = WorkerState.idle
        logger.info("worker %s 就绪（%s / %s）", self.worker_id, self.site, self.model or "默认")

    async def stop(self) -> None:
        for task in (self._recover_task, self._run_task):
            if task and not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        # 服务停机后不保留 ai-relay 自己创建的 session；失败仅记录，不阻塞退出。
        with suppress(Exception):
            async with asyncio.timeout(TAB_CLEANUP_TIMEOUT):
                await self.adapter.client.close_session(self.adapter.session)

    async def _open_tab(self) -> None:
        with suppress(Exception):
            async with asyncio.timeout(TAB_CLEANUP_TIMEOUT):
                await self.adapter.client.close_session(self.adapter.session)
        await self.adapter.client.navigate(
            self.adapter.home_url, self.adapter.session, new_tab=True,
            group_title=self.tab_group_title)
        await asyncio.sleep(PAGE_LOAD_WAIT)

    async def _rebuild_tab(self, reason: str) -> bool:
        """回收不可信 tab；仅在确认没有继续执行旧任务后接受下一任务。"""
        try:
            await self._open_tab()
            if not await self.adapter.is_logged_in():
                self._degrade("未登录，请在浏览器中手动登录，将自动恢复")
                return False
            await self.adapter.ensure_model(self.model)
            return True
        except Exception as e:
            self._degrade(f"{reason}: {self._error_text(e)}")
            return False

    def _degrade(self, detail: str) -> None:
        self.state = WorkerState.degraded
        self.detail = detail
        logger.warning("worker %s 进入异常状态: %s", self.worker_id, detail)
        self._start_recover()

    def _start_recover(self) -> None:
        if self._recover_task is None or self._recover_task.done():
            self._recover_task = asyncio.create_task(self._recover_loop())

    async def _recover_loop(self) -> None:
        while self.state == WorkerState.degraded:
            await asyncio.sleep(RECOVER_INTERVAL)
            try:
                await self._open_tab()
                if not await self.adapter.is_logged_in():
                    self.detail = "未登录，请在浏览器中手动登录，将自动恢复"
                    continue
                await self.adapter.ensure_model(self.model)
            except Exception as e:
                self.detail = f"恢复失败: {self._error_text(e)}"
                continue
            self.state = WorkerState.idle
            self.detail = None
            logger.info("worker %s 已恢复", self.worker_id)

    def start_task(self, task: TaskRecord) -> None:
        self.state = WorkerState.busy
        self.current_task_id = task.task_id
        self._run_task = asyncio.create_task(self.run_task(task))

    async def run_task(self, task: TaskRecord) -> None:
        try:
            current = self.store.get(task.task_id)
            if current is None or current.status != TaskStatus.running:
                self.store.mark_running(
                    task.task_id, worker_id=self.worker_id, actual_site=self.site,
                    actual_target_id=self.target_id, actual_backend_type=self.backend_type)
            self.store.start_attempt(
                task.task_id, self.worker_id, self.site, self.model,
                target_id=self.target_id, backend_type=self.backend_type)
            logger.info("任务 %s 开始执行（worker %s）", task.task_id[:8], self.worker_id)
            async with asyncio.timeout(self.timeout_seconds):
                answer = await self._execute(task)
        except asyncio.CancelledError:
            current = self.store.get(task.task_id)
            if current and current.cancel_requested:
                with suppress(Exception):
                    self.store.mark_cancelled(task.task_id, "用户请求取消")
                # 关闭/重开 tab，避免旧页面仍生成时被下一任务复用。
                await self._rebuild_tab("任务取消后重建 tab")
            else:
                # 服务停机的取消不能自动重新提交到浏览器。
                with suppress(Exception):
                    self.store.mark_interrupted(task.task_id, "服务停止，任务执行中断")
                self.fail_count += 1
            raise
        except (SendOutcomeUnknownError, SentTaskError, TaskTimeoutError) as e:
            # click 已确认或执行阶段已开始，不自动重试，避免产生重复的真实 AI 请求。
            with suppress(Exception):
                self.store.mark_outcome_unknown(task.task_id, self._error_text(e))
            self.fail_count += 1
            await self._rebuild_tab("任务结果未知后重建 tab")
            logger.warning("任务 %s 进入结果未知状态: %s", task.task_id[:8], e)
        except asyncio.TimeoutError as e:
            # 端到端 timeout 若发生在 click 前仍可安全重试；发送阶段之后则必须人工确认。
            phase = (self.store.get(task.task_id) or task).phase
            if phase in {TaskPhase.verifying_send, TaskPhase.generating,
                         TaskPhase.collecting_result}:
                with suppress(Exception):
                    self.store.mark_outcome_unknown(task.task_id, "任务端到端超时")
                self.fail_count += 1
                await self._rebuild_tab("发送后超时重建 tab")
            else:
                await self._rebuild_tab("发送前超时重建 tab")
                if self.on_retry is not None and self.on_retry(task, "任务端到端超时", "pre_send_timeout"):
                    logger.info("任务 %s 发送前超时，已安全重试", task.task_id[:8])
                else:
                    self.store.mark_failed(task.task_id, "任务端到端超时", error_code="pre_send_timeout")
                    self.fail_count += 1
        except TaskCancelledError as e:
            with suppress(Exception):
                self.store.mark_cancelled(task.task_id, str(e))
            logger.info("任务 %s 已取消", task.task_id[:8])
        except ResponseValidationError as e:
            # 已取得确定回答但不满足调用方 JSON 契约；允许由 pool 按预算重试。
            error = self._error_text(e)
            if self.on_retry is None or not self.on_retry(task, error, e.code):
                self.fail_count += 1
                logger.warning("任务 %s 输出契约校验最终失败 [%s]: %s", task.task_id[:8], e.code, error)
        except Exception as e:
            error = self._error_text(e)
            code = self._error_code(e)
            # 发送前失败可以安全重试，但先让 worker 回到干净 tab。
            await self._rebuild_tab("发送前失败后重建 tab")
            if self.on_retry is not None:
                retried = self.on_retry(task, error, code)
                if not retried:
                    self.fail_count += 1
                    logger.warning("任务 %s 最终失败 [%s]: %s", task.task_id[:8], code, error)
            else:
                self.store.mark_failed(task.task_id, error, error_code=code)
                self.fail_count += 1
                logger.warning("任务 %s 失败 [%s]: %s", task.task_id[:8], code, error)
        else:
            try:
                self.store.mark_done(task.task_id, answer)
            except ResponseValidationError as error:
                # 返回已确定，因此格式/Schema 不合格可以作为一条新 attempt 安全重试。
                if self.on_retry is None or not self.on_retry(task, self._error_text(error), error.code):
                    self.fail_count += 1
                    logger.warning("任务 %s 输出契约校验最终失败 [%s]: %s",
                                   task.task_id[:8], error.code, error)
            except Exception as e:
                # 真实结果已拿到但无法可靠落库时，不能向 API 误报 done。
                # 即使数据库持续故障，mark_outcome_unknown 也会先修正内存状态。
                with suppress(Exception):
                    self.store.mark_outcome_unknown(
                        task.task_id, f"结果已获取但持久化失败: {self._error_text(e)}")
                self.fail_count += 1
                self._degrade(f"任务完成状态持久化失败: {self._error_text(e)}")
                logger.exception("任务 %s 完成结果持久化失败", task.task_id[:8])
            else:
                self.done_count += 1
                logger.info("任务 %s 完成，答案 %d 字", task.task_id[:8], len(answer))
        finally:
            self.current_task_id = None
            if self.state == WorkerState.busy:
                self.state = WorkerState.idle

    async def _execute(self, task: TaskRecord) -> str:
        """执行一次 attempt。发送前错误可由 pool 重试；发送后错误不自动重放。"""
        await self._check_cancel(task)
        self.store.set_phase(task.task_id, TaskPhase.opening_chat)
        await self.adapter.new_chat()
        await asyncio.sleep(PAGE_LOAD_WAIT)
        await self._check_cancel(task)

        self.store.set_phase(task.task_id, TaskPhase.configuring_model)
        await self.adapter.ensure_model(self.model)
        baseline = (await self.adapter.poll_once()).get("answer")

        self.store.set_phase(task.task_id, TaskPhase.sending)
        self.store.update_attempt(task.task_id, phase=TaskPhase.sending, send_state="sending")
        await self.adapter.send_prompt(task.execution_prompt())
        # send_prompt 只有在已确认当前用户消息或生成态后才返回。
        self.store.set_phase(task.task_id, TaskPhase.verifying_send)
        self.store.update_attempt(task.task_id, phase=TaskPhase.verifying_send,
                                  send_state="sent_confirmed")
        logger.info("任务 %s 提示词已确认发送（%d 字）", task.task_id[:8], len(task.prompt))

        started = time.monotonic()
        hard_deadline = started + self.hard_timeout_seconds
        stall_deadline = started + self.stall_seconds
        previous_answer: Optional[str] = None
        stable_contract_answer: Optional[str] = None
        stable_contract_polls = 0
        last_progress_answer = baseline
        try:
            while True:
                await asyncio.sleep(POLL_INTERVAL)
                await self._check_cancel(task)
                now = time.monotonic()
                if now > hard_deadline:
                    raise TaskTimeoutError(f"硬超时（{self.hard_timeout_seconds}s）")
                if now > stall_deadline:
                    raise TaskTimeoutError(f"生成停滞超过 {self.stall_seconds}s")

                self.store.set_phase(task.task_id, TaskPhase.generating)
                snap = await self.adapter.poll_once()
                site_error = snap.get("error")
                if site_error:
                    raise RuntimeError(f"站点返回错误: {site_error}")
                answer = snap.get("answer")
                # 只有确实有新文本时才刷新停滞计时，不能仅信任一个冻结的 stop 按钮。
                if answer and answer != baseline and answer != last_progress_answer:
                    last_progress_answer = answer
                    stall_deadline = time.monotonic() + self.stall_seconds

                if snap.get("generating"):
                    previous_answer = None
                    # 某些网页（实测 MiniMax）在回答完成后仍长期保留 stop-button。
                    # 仅对声明了 JSON Schema 的任务，连续三次读到完全相同且已通过
                    # 契约的完整结果时安全结束；不接受普通文本或不完整 JSON。
                    if answer and answer != baseline and task.response_format is not None:
                        try:
                            validate_result(answer, task.response_format)
                        except ResponseValidationError:
                            stable_contract_answer = None
                            stable_contract_polls = 0
                        else:
                            if answer == stable_contract_answer:
                                stable_contract_polls += 1
                            else:
                                stable_contract_answer = answer
                                stable_contract_polls = 1
                            if stable_contract_polls >= 3:
                                self.store.set_phase(task.task_id, TaskPhase.collecting_result)
                                return answer
                    else:
                        stable_contract_answer = None
                        stable_contract_polls = 0
                    continue
                if not answer or answer == baseline:
                    previous_answer = None
                    continue
                # provider 已无生成指示，且连续两次读到同一目标回答后再确认完成。
                if answer == previous_answer:
                    self.store.set_phase(task.task_id, TaskPhase.collecting_result)
                    return answer
                previous_answer = answer
        except TaskCancelledError:
            raise
        except Exception as e:
            raise SentTaskError(self._error_text(e)) from e

    async def _check_cancel(self, task: TaskRecord) -> None:
        current = self.store.get(task.task_id)
        if current and current.cancel_requested:
            raise TaskCancelledError("任务已请求取消")

    @staticmethod
    def _error_text(error: BaseException) -> str:
        return str(error) or type(error).__name__

    @staticmethod
    def _error_code(error: BaseException) -> str:
        if isinstance(error, WebbridgeError):
            return "bridge_error"
        if isinstance(error, asyncio.TimeoutError):
            return "task_timeout"
        if isinstance(error, ValueError):
            return "configuration_error"
        return "pre_send_error"
