"""Worker：一个 webbridge session（一个浏览器 tab）的抽象。

状态机：starting → idle → busy → idle ...
任何阶段出问题都可能进入 degraded，由恢复协程周期重试直到回到 idle。
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Optional

from .schemas import WorkerState
from .sites.base import SiteAdapter
from .store import TaskRecord, TaskStore
from .webbridge import WebbridgeError

logger = logging.getLogger("ai-relay.worker")

# 连续 WebbridgeError 多少次后认为 tab 异常
MAX_CONSEC_ERRORS = 3
# 未登录/异常时恢复重试间隔
RECOVER_INTERVAL = 30
# 轮询间隔
POLL_INTERVAL = 2
# 页面加载等待
PAGE_LOAD_WAIT = 3


class TaskTimeoutError(RuntimeError):
    pass


class Worker:
    def __init__(self, worker_id: str, site: str, model: str, adapter: SiteAdapter,
                 store: TaskStore, timeout_seconds: float = 600,
                 stall_seconds: float = 120):
        self.worker_id = worker_id
        self.site = site
        self.model = model
        self.adapter = adapter
        self.store = store
        self.timeout_seconds = timeout_seconds
        # 生成停滞上限：超过该时长回答没有任何进展视为卡死（如 deepseek 僵尸"正在生成"态），
        # 抛 WebbridgeError 走重建/重试路径
        self.stall_seconds = stall_seconds
        self.state = WorkerState.starting
        self.detail: Optional[str] = None
        self.current_task_id: Optional[str] = None
        self.done_count = 0
        self.fail_count = 0
        self._recover_task: Optional[asyncio.Task] = None
        self._run_task: Optional[asyncio.Task] = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        """打开站点首页，检查登录态并选模型。"""
        self.state = WorkerState.starting
        self.detail = None
        try:
            await self._open_tab()
            if not await self.adapter.is_logged_in():
                self._degrade("未登录，请在浏览器中手动登录，将自动恢复")
                return
            await self.adapter.ensure_model(self.model)
        except WebbridgeError as e:
            self._degrade(f"启动失败: {e}")
            return
        self.state = WorkerState.idle
        logger.info("worker %s 就绪（%s / %s）", self.worker_id, self.site, self.model or "默认")

    async def stop(self) -> None:
        for t in (self._recover_task, self._run_task):
            if t and not t.done():
                t.cancel()
                with suppress(asyncio.CancelledError):
                    await t

    async def _open_tab(self) -> None:
        # 先清掉 session 残留的旧 tab（重启/恢复时会累积多个，导致命令打错 tab），
        # 保证一个 worker 始终只有一个 tab
        with suppress(Exception):
            await self.adapter.client.close_session(self.adapter.session)
        await self.adapter.client.navigate(
            self.adapter.home_url, self.adapter.session,
            new_tab=True, group_title="ai-relay")
        await asyncio.sleep(PAGE_LOAD_WAIT)

    def _degrade(self, detail: str) -> None:
        self.state = WorkerState.degraded
        self.detail = detail
        logger.warning("worker %s 进入异常状态: %s", self.worker_id, detail)
        self._start_recover()

    def _start_recover(self) -> None:
        if self._recover_task is None or self._recover_task.done():
            self._recover_task = asyncio.create_task(self._recover_loop())

    async def _recover_loop(self) -> None:
        """周期性尝试恢复：查登录态 → 选模型 → 回到 idle。"""
        while self.state == WorkerState.degraded:
            await asyncio.sleep(RECOVER_INTERVAL)
            try:
                logged_in = await self.adapter.is_logged_in()
            except WebbridgeError:
                # tab 可能已被关闭，重新打开再查
                try:
                    await self._open_tab()
                    logged_in = await self.adapter.is_logged_in()
                except WebbridgeError as e:
                    self.detail = f"恢复失败: {e}"
                    continue
            if not logged_in:
                self.detail = "未登录，请在浏览器中手动登录，将自动恢复"
                continue
            try:
                await self.adapter.ensure_model(self.model)
            except WebbridgeError as e:
                self.detail = f"恢复失败: {e}"
                continue
            self.state = WorkerState.idle
            self.detail = None
            logger.info("worker %s 已恢复", self.worker_id)

    # ---- 任务执行 ----

    def start_task(self, task: TaskRecord) -> None:
        """指派任务（同步置 busy，避免 dispatcher 重复指派）。"""
        self.state = WorkerState.busy
        self.current_task_id = task.task_id
        self._run_task = asyncio.create_task(self.run_task(task))

    async def run_task(self, task: TaskRecord) -> None:
        self.store.mark_running(task.task_id, worker_id=self.worker_id,
                                actual_site=self.site)
        logger.info("任务 %s 开始执行（worker %s）", task.task_id[:8], self.worker_id)
        try:
            answer = await self._execute(task)
        except asyncio.CancelledError:
            self.store.mark_failed(task.task_id, "任务被取消")
            raise
        except Exception as e:
            error = str(e) or type(e).__name__
            self.store.mark_failed(task.task_id, error)
            self.fail_count += 1
            logger.warning("任务 %s 失败: %s", task.task_id[:8], error)
        else:
            self.store.mark_done(task.task_id, answer)
            self.done_count += 1
            logger.info("任务 %s 完成，答案 %d 字", task.task_id[:8], len(answer))
        finally:
            self.current_task_id = None
            if self.state == WorkerState.busy:
                self.state = WorkerState.idle

    async def _execute(self, task: TaskRecord) -> str:
        try:
            return await self._attempt(task)
        except WebbridgeError:
            # tab 异常：重建 tab 后重试一次
            logger.warning("worker %s tab 异常，重建后重试任务 %s",
                           self.worker_id, task.task_id[:8])
            try:
                await self._open_tab()
                await self.adapter.ensure_model(self.model)
                return await self._attempt(task)
            except WebbridgeError as e:
                self._degrade(f"tab 异常且重试失败: {e}")
                raise

    async def _attempt(self, task: TaskRecord) -> str:
        """跑一遍完整流程；连续 WebbridgeError 超阈值则抛出。"""
        errors = 0
        while True:
            try:
                return await self._run_once(task)
            except WebbridgeError as e:
                errors += 1
                if errors >= MAX_CONSEC_ERRORS:
                    raise
                logger.warning("worker %s WebbridgeError(%d/%d): %s",
                               self.worker_id, errors, MAX_CONSEC_ERRORS, e)
                await asyncio.sleep(1)

    async def _run_once(self, task: TaskRecord) -> str:
        await self.adapter.new_chat()
        await asyncio.sleep(PAGE_LOAD_WAIT)
        # 记录基线：发送前的最后一条回答，用于识别"新回答"（deepseek 必需）
        baseline = (await self.adapter.poll_once()).get("answer")
        await self.adapter.send_prompt(task.prompt)
        logger.info("任务 %s 提示词已发送（%d 字）", task.task_id[:8], len(task.prompt))

        deadline = time.monotonic() + self.timeout_seconds
        stall_deadline = time.monotonic() + self.stall_seconds
        prev_answer: Optional[str] = None
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            now = time.monotonic()
            if now > deadline:
                raise TaskTimeoutError("任务超时")
            if now > stall_deadline:
                # 长时间无进展（僵尸"正在生成"/卡死）：抛 WebbridgeError 触发重建重试
                raise WebbridgeError(f"生成停滞超过 {self.stall_seconds}s（无进展）")
            snap = await self.adapter.poll_once()
            # 站点侧明确报错（如 deepseek"网络异常"）：快速失败，不傻等超时
            site_error = snap.get("error")
            if site_error:
                raise RuntimeError(f"站点返回错误: {site_error}")
            answer = snap.get("answer")
            if snap.get("generating"):
                # 站点有活动指示（停止按钮/思考中）：信任站点，重置停滞计时
                stall_deadline = time.monotonic() + self.stall_seconds
                prev_answer = None
                continue
            # 无活动指示且没有新回答：计入停滞（僵尸态），重置稳定计数继续等
            if not answer or answer == baseline:
                prev_answer = None
                continue
            # 连续 2 次轮询文本不变才算完成
            if answer == prev_answer:
                return answer
            prev_answer = answer
            # 回答文本有变化，视为有进展，重置停滞计时
            stall_deadline = time.monotonic() + self.stall_seconds
