"""ACP 与 OpenAI-compatible 外部执行 worker。

这两个 worker 只在被调度到对应 target 的任务运行时才启动子进程或发出 HTTP 请求。
启动阶段仅做本机命令/环境变量配置检查，绝不主动调用模型。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import signal
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from .acp_protocol import AcpConnection, AcpConnectionClosed, AcpProtocolError, AcpRpcError
from .config import AcpTargetConfig, CursorAcpTargetConfig, OpenAICompatibleTargetConfig
from .response_contract import ResponseValidationError
from .schemas import BackendType, TaskPhase, TaskStatus, WorkerState
from .store import TaskRecord, TaskStore

logger = logging.getLogger("ai-relay.external-workers")


class ExternalWorkerError(RuntimeError):
    code = "external_error"
    retryable = False
    outcome_unknown = False


class RetryableExternalWorkerError(ExternalWorkerError):
    retryable = True


class ExternalOutcomeUnknownError(ExternalWorkerError):
    code = "external_outcome_unknown"
    outcome_unknown = True


class ExternalTaskCancelledError(ExternalWorkerError):
    code = "cancelled"


class AcpExecutionError(ExternalWorkerError):
    code = "acp_execution_error"


class AcpOutputLimitError(AcpExecutionError):
    code = "acp_output_limit"


class ApiResponseError(ExternalWorkerError):
    def __init__(self, message: str, *, code: str, status_code: Optional[int] = None,
                 retryable: bool = False, outcome_unknown: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.retryable = retryable
        self.outcome_unknown = outcome_unknown


@dataclass(frozen=True)
class ExternalResult:
    text: str
    external_request_id: Optional[str] = None
    process_exit_code: Optional[int] = None
    provider_status_code: Optional[int] = None
    usage: Optional[dict[str, Any]] = None


RetryCallback = Callable[[TaskRecord, str, str], bool]


class ExternalWorker:
    """非浏览器 target 的统一执行生命周期。"""

    backend_type: BackendType
    site: Optional[str] = None

    def __init__(self, worker_id: str, target_id: str, model: str, store: TaskStore,
                 timeout_seconds: float, on_retry: RetryCallback) -> None:
        self.worker_id = worker_id
        self.target_id = target_id
        self.model = model
        self.store = store
        self.timeout_seconds = timeout_seconds
        self.on_retry = on_retry
        self.state = WorkerState.starting
        self.detail: Optional[str] = None
        self.current_task_id: Optional[str] = None
        self.done_count = 0
        self.fail_count = 0
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
            await self._validate_ready()
        except Exception as error:
            self._degrade(self._error_text(error))
            return
        self.state = WorkerState.idle
        logger.info("worker %s 就绪（%s / %s）", self.worker_id,
                    self.backend_type.value, self.model or "默认")

    async def stop(self) -> None:
        if self._run_task and not self._run_task.done():
            self._run_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._run_task

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
                    actual_target_id=self.target_id, actual_backend_type=self.backend_type,
                )
            self.store.start_attempt(
                task.task_id, self.worker_id, self.site, self.model,
                target_id=self.target_id, backend_type=self.backend_type,
            )
            current = self.store.get(task.task_id)
            if current and current.cancel_requested:
                raise ExternalTaskCancelledError("任务已请求取消")
            async with asyncio.timeout(self.timeout_seconds):
                result = await self._execute(task)
        except asyncio.CancelledError:
            current = self.store.get(task.task_id)
            if current and current.cancel_requested:
                with suppress(Exception):
                    self.store.mark_cancelled(task.task_id, "用户请求取消")
            else:
                with suppress(Exception):
                    self.store.mark_interrupted(task.task_id, "服务停止，外部执行中断")
                self.fail_count += 1
            raise
        except ExternalOutcomeUnknownError as error:
            with suppress(Exception):
                self.store.mark_outcome_unknown(task.task_id, self._error_text(error))
            self.fail_count += 1
            logger.warning("任务 %s 外部执行结果未知: %s", task.task_id[:8], error)
        except ExternalTaskCancelledError as error:
            with suppress(Exception):
                self.store.mark_cancelled(task.task_id, self._error_text(error))
        except asyncio.TimeoutError:
            # timeout 期间请求/CLI 可能已经接收 prompt，不能盲目重发。
            with suppress(Exception):
                self.store.mark_outcome_unknown(task.task_id, "外部执行端到端超时")
            self.fail_count += 1
        except ExternalWorkerError as error:
            if isinstance(error, ApiResponseError) and error.status_code is not None:
                with suppress(Exception):
                    self.store.update_attempt(task.task_id, provider_status_code=error.status_code)
            await self._handle_external_error(task, error)
        except Exception as error:
            await self._handle_external_error(
                task, ExternalWorkerError(self._error_text(error)))
        else:
            try:
                self.store.mark_done(
                    task.task_id, result.text,
                    external_request_id=result.external_request_id,
                    process_exit_code=result.process_exit_code,
                    provider_status_code=result.provider_status_code,
                    usage=result.usage,
                )
            except ResponseValidationError as error:
                # 外部 target 已返回确定文本；Schema 不合格可安全安排新的 attempt。
                if not self.on_retry(task, self._error_text(error), error.code):
                    self.fail_count += 1
                    logger.warning("任务 %s 外部输出契约校验最终失败 [%s]: %s",
                                   task.task_id[:8], error.code, error)
            except Exception as error:
                with suppress(Exception):
                    self.store.mark_outcome_unknown(
                        task.task_id, f"外部结果已获取但持久化失败: {self._error_text(error)}")
                self._degrade(f"任务完成状态持久化失败: {self._error_text(error)}")
                self.fail_count += 1
            else:
                self.done_count += 1
                logger.info("任务 %s 外部执行完成（worker %s，答案 %d 字）",
                            task.task_id[:8], self.worker_id, len(result.text))
        finally:
            self.current_task_id = None
            if self.state == WorkerState.busy:
                self.state = WorkerState.idle

    async def _handle_external_error(self, task: TaskRecord, error: ExternalWorkerError) -> None:
        code = getattr(error, "code", "external_error")
        if getattr(error, "outcome_unknown", False):
            with suppress(Exception):
                self.store.mark_outcome_unknown(task.task_id, self._error_text(error))
            self.fail_count += 1
            return
        if getattr(error, "retryable", False):
            if self.on_retry(task, self._error_text(error), code):
                return
        with suppress(Exception):
            self.store.mark_failed(task.task_id, self._error_text(error), error_code=code)
        self.fail_count += 1

    def _degrade(self, detail: str) -> None:
        self.state = WorkerState.degraded
        self.detail = detail
        logger.warning("worker %s 进入异常状态: %s", self.worker_id, detail)

    async def _validate_ready(self) -> None:
        raise NotImplementedError

    async def _execute(self, task: TaskRecord) -> ExternalResult:
        raise NotImplementedError

    @staticmethod
    def _error_text(error: BaseException) -> str:
        return str(error) or type(error).__name__


class AcpWorker(ExternalWorker):
    backend_type = BackendType.acp

    def __init__(self, worker_id: str, target: AcpTargetConfig, store: TaskStore,
                 default_timeout_seconds: float, on_retry: RetryCallback) -> None:
        super().__init__(
            worker_id, target.id, target.model, store,
            target.timeout_seconds or default_timeout_seconds, on_retry,
        )
        self.target = target
        self._active_process: Optional[asyncio.subprocess.Process] = None
        self._resolved_command: Optional[str] = None

    def _resolve_command(self) -> str:
        command = os.path.expanduser(self.target.command)
        if os.path.isabs(command) or os.sep in command:
            if os.path.isfile(command) and os.access(command, os.X_OK):
                return command
            raise AcpExecutionError(f"ACP 命令不可执行: {command}")
        candidates = [
            shutil.which(command),
            os.path.expanduser(f"~/.local/bin/{command}"),
            os.path.expanduser(f"~/.cursor/bin/{command}"),
            f"/opt/homebrew/bin/{command}",
        ]
        for candidate in candidates:
            if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        raise AcpExecutionError(
            f"ACP 命令不可用: {self.target.command}（当前服务 PATH 未找到；可配置绝对路径）")

    async def _validate_ready(self) -> None:
        self._resolved_command = self._resolve_command()
        path = os.path.abspath(os.path.expanduser(self.target.working_directory))
        if not os.path.isdir(path):
            raise AcpExecutionError(f"ACP 工作目录不存在: {path}")
        if self.target.api_key_env and not os.environ.get(self.target.api_key_env):
            raise AcpExecutionError(
                f"ACP API key 环境变量未设置: {self.target.api_key_env}")
        self._working_directory = path
        if self.target.verify_auth_on_start:
            await self._verify_auth()

    async def _verify_auth(self) -> None:
        """只读预检 Cursor 登录/Keychain 状态；不创建聊天、不提交 prompt。"""
        started = time.monotonic()
        logger.info("worker %s ACP CLI 认证预检开始: command=%s timeout=%.1fs",
                    self.worker_id, self._resolved_command or self.target.command,
                    self.target.auth_check_timeout_seconds)
        try:
            process = await asyncio.create_subprocess_exec(
                self._resolved_command or self.target.command, "status", cwd=self._working_directory,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env=self._child_env(),
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self.target.auth_check_timeout_seconds)
        except (OSError, asyncio.TimeoutError) as error:
            logger.warning("worker %s ACP CLI 认证预检失败，用时 %.3fs: %s",
                           self.worker_id, time.monotonic() - started, error)
            raise AcpExecutionError(f"ACP 认证预检失败: {error}") from error
        elapsed = time.monotonic() - started
        logger.info(
            "worker %s ACP CLI 认证预检结束: elapsed=%.3fs exit=%s stdout_bytes=%d stderr_bytes=%d",
            self.worker_id, elapsed, process.returncode, len(stdout), len(stderr))
        if process.returncode != 0:
            detail = (stderr or stdout).decode("utf-8", errors="replace").strip()
            if "keychain is locked" in detail.lower():
                raise AcpExecutionError("ACP Keychain 已锁定，请先解锁登录钥匙串")
            raise AcpExecutionError(f"ACP 认证预检失败: {detail or f'退出码 {process.returncode}'}")

    async def stop(self) -> None:
        if self._active_process and self._active_process.returncode is None:
            await self._terminate_process(self._active_process)
        await super().stop()

    async def _execute(self, task: TaskRecord) -> ExternalResult:
        await self._check_cancel(task)
        attempt_started = time.monotonic()
        self.store.set_phase(task.task_id, TaskPhase.sending)
        self.store.update_attempt(task.task_id, phase=TaskPhase.sending, send_state="sending")

        prompt = task.execution_prompt()
        argv = self._build_argv(prompt)
        stdin: Optional[int] = None
        input_data: Optional[bytes] = None
        safe_argv = [arg if arg != prompt else f"<prompt:{len(prompt)} chars>" for arg in argv]
        logger.info(
            "任务 %s ACP CLI 子进程启动准备: worker=%s timeout=%.1fs cwd=%s argv=%s",
            task.task_id[:8], self.worker_id, self.timeout_seconds, self._working_directory,
            safe_argv)

        spawn_started = time.monotonic()
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=self._working_directory,
                stdin=stdin,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                # 明确最小环境，不传入 host 全量 secrets；仅可选映射一个 Cursor key。
                env=self._child_env(),
            )
        except FileNotFoundError as error:
            self._degrade(f"ACP 命令不可用: {self.target.command}")
            raise AcpExecutionError(f"ACP 命令不可用: {self.target.command}") from error
        except OSError as error:
            raise RetryableExternalWorkerError(f"ACP 子进程无法启动: {error}") from error

        spawn_elapsed = time.monotonic() - spawn_started
        logger.info("任务 %s ACP CLI 子进程已启动: pid=%s spawn_elapsed=%.3fs",
                    task.task_id[:8], process.pid, spawn_elapsed)
        self._active_process = process
        self.store.set_phase(task.task_id, TaskPhase.generating)
        self.store.update_attempt(task.task_id, phase=TaskPhase.generating,
                                  send_state="sent_confirmed")
        try:
            stdout, stderr, io_metrics = await self._communicate_limited(
                process, input_data, task.task_id)
        except asyncio.CancelledError:
            logger.info("任务 %s ACP CLI 执行被取消，准备终止子进程 pid=%s elapsed=%.3fs",
                        task.task_id[:8], process.pid, time.monotonic() - attempt_started)
            await self._terminate_process(process)
            raise
        finally:
            self._active_process = None
        total_elapsed = time.monotonic() - attempt_started
        process_exit_code = io_metrics.get("process_exit_code", process.returncode)
        logger.info(
            "任务 %s ACP CLI 子进程结束: pid=%s exit=%s total_elapsed=%.3fs stdout_bytes=%d stderr_bytes=%d stdout_first_after=%s stderr_first_after=%s process_exit_after=%s process_group_cleaned=%s",
            task.task_id[:8], process.pid, process_exit_code, total_elapsed,
            len(stdout), len(stderr), io_metrics.get("stdout_first_after"),
            io_metrics.get("stderr_first_after"), io_metrics.get("process_exit_after"),
            io_metrics.get("process_group_cleaned", False))
        if process_exit_code != 0:
            detail = stderr.decode("utf-8", errors="replace").strip() or "无 stderr 输出"
            self.store.update_attempt(task.task_id, process_exit_code=process_exit_code)
            # CLI 已启动并接收 prompt 后的非零退出无法证明没有副作用，绝不自动重放。
            raise ExternalOutcomeUnknownError(f"ACP 退出码 {process_exit_code}: {detail}")
        text = stdout.decode("utf-8", errors="replace").strip()
        if not text:
            raise AcpExecutionError("ACP 未返回文本结果")
        return ExternalResult(text=text, process_exit_code=process_exit_code)

    def _build_argv(self, prompt: str) -> list[str]:
        """构建 Cursor Agent CLI 的非交互、只读命令，不经 shell。"""
        argv = [self._resolved_command or self.target.command, *self.target.args, "--print",
                "--output-format", self.target.output_format,
                "--mode", self.target.mode]
        if self.target.endpoint:
            argv.extend(["--endpoint", self.target.endpoint])
        if self.target.trust_workspace:
            argv.append("--trust")
        if self.target.pass_workspace:
            argv.extend(["--workspace", self._working_directory])
        if self.model:
            argv.extend(["--model", self.model])
        argv.append(prompt)
        return argv

    def _child_env(self) -> dict[str, str]:
        env = {key: value for key, value in os.environ.items()
               if key in {"HOME", "LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"}}
        if self.target.api_key_env:
            key = os.environ.get(self.target.api_key_env)
            if not key:
                raise AcpExecutionError(
                    f"ACP API key 环境变量未设置: {self.target.api_key_env}")
            env["CURSOR_API_KEY"] = key
        return env

    async def _communicate_limited(
        self, process: asyncio.subprocess.Process, input_data: Optional[bytes], task_id: str,
    ) -> tuple[bytes, bytes, dict[str, Any]]:
        """边读边限流，并在主进程退出后清理继承 pipe 的 helper 进程。"""
        started = time.monotonic()
        metrics: dict[str, Any] = {
            "stdout_first_after": None,
            "stderr_first_after": None,
            "process_exit_after": None,
            "process_exit_code": None,
            "pipe_drain_after_exit": 0.0,
            "process_group_cleaned": False,
        }
        if input_data is not None and process.stdin:
            process.stdin.write(input_data)
            await process.stdin.drain()
            process.stdin.close()
        assert process.stdout is not None and process.stderr is not None

        async def read_limited(stream: asyncio.StreamReader, name: str) -> bytes:
            chunks: list[bytes] = []
            size = 0
            first_key = f"{name}_first_after"
            while chunk := await stream.read(64 * 1024):
                now = time.monotonic()
                if metrics[first_key] is None:
                    metrics[first_key] = round(now - started, 3)
                    logger.info(
                        "任务 %s ACP CLI 首次 %s 输出: after=%.3fs chunk_bytes=%d",
                        task_id[:8], name, now - started, len(chunk))
                size += len(chunk)
                if size > self.target.output_max_chars:
                    raise ExternalOutcomeUnknownError(
                        f"ACP 输出超过上限 {self.target.output_max_chars} 字节，已拒绝保存不完整结果")
                chunks.append(chunk)
            logger.info("任务 %s ACP CLI %s 流结束: elapsed=%.3fs bytes=%d",
                        task_id[:8], name, time.monotonic() - started, size)
            return b"".join(chunks)

        pgid = process.pid
        readers = [asyncio.create_task(read_limited(process.stdout, "stdout")),
                   asyncio.create_task(read_limited(process.stderr, "stderr"))]
        wait_task = asyncio.create_task(self._wait_process_exit(process))
        try:
            process_exit_code = await wait_task
            metrics["process_exit_after"] = round(time.monotonic() - started, 3)
            metrics["process_exit_code"] = process_exit_code
            pipes_open = any(not reader.done() for reader in readers)
            logger.info(
                "任务 %s ACP CLI 主进程退出: pid=%s exit=%s after=%.3fs pipes_still_open=%s",
                task_id[:8], process.pid, process_exit_code,
                metrics["process_exit_after"], pipes_open)
            if pipes_open:
                drain_started = time.monotonic()
                try:
                    await asyncio.wait_for(
                        asyncio.shield(asyncio.gather(*readers)),
                        timeout=self.target.pipe_drain_after_exit_seconds,
                    )
                except asyncio.TimeoutError:
                    metrics["process_group_cleaned"] = True
                    metrics["pipe_drain_after_exit"] = round(time.monotonic() - drain_started, 3)
                    logger.warning(
                        "任务 %s ACP CLI 主进程已退出但 pipe 未关闭，清理 process group: pgid=%s drain=%.3fs",
                        task_id[:8], pgid, metrics["pipe_drain_after_exit"])
                    await self._terminate_process_group(pgid, task_id)
                # SIGTERM/SIGKILL 之后 pipe 应尽快 EOF；若仍不 EOF，让 attempt 总超时兜底。
            stdout, stderr = await asyncio.gather(*readers)
            return stdout, stderr, metrics
        except Exception:
            await self._terminate_process(process)
            if process.returncode is not None:
                await self._terminate_process_group(pgid, task_id)
            await asyncio.gather(*readers, return_exceptions=True)
            raise
        finally:
            if not wait_task.done():
                wait_task.cancel()
                with suppress(asyncio.CancelledError):
                    await wait_task

    async def _check_cancel(self, task: TaskRecord) -> None:
        current = self.store.get(task.task_id)
        if current and current.cancel_requested:
            raise ExternalTaskCancelledError("任务已请求取消")

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        started = time.monotonic()
        logger.info("ACP CLI 终止子进程: pid=%s graceful_timeout=%.1fs",
                    process.pid, self.target.graceful_shutdown_seconds)
        with suppress(ProcessLookupError):
            if hasattr(os, "killpg"):
                os.killpg(process.pid, signal.SIGTERM)
            else:  # pragma: no cover - macOS/Linux 都会走上方分支
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=self.target.graceful_shutdown_seconds)
        except asyncio.TimeoutError:
            logger.warning("ACP CLI 子进程 SIGTERM 超时，发送 SIGKILL: pid=%s elapsed=%.3fs",
                           process.pid, time.monotonic() - started)
            with suppress(ProcessLookupError):
                if hasattr(os, "killpg"):
                    os.killpg(process.pid, signal.SIGKILL)
                else:  # pragma: no cover
                    process.kill()
            with suppress(Exception):
                await process.wait()
        logger.info("ACP CLI 子进程已终止: pid=%s elapsed=%.3fs exit=%s",
                    process.pid, time.monotonic() - started, process.returncode)

    async def _terminate_process_group(self, pgid: int, task_id: str) -> None:
        if not hasattr(os, "killpg"):
            return
        started = time.monotonic()
        logger.info("任务 %s ACP CLI 清理 process group: pgid=%s graceful_timeout=%.1fs",
                    task_id[:8], pgid, self.target.graceful_shutdown_seconds)
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except PermissionError as error:
            logger.warning("任务 %s ACP CLI process group SIGTERM 权限不足: pgid=%s error=%s",
                           task_id[:8], pgid, error)
        await asyncio.sleep(0)
        # 不能等待已退出的主进程；给 helper 一段优雅退出时间，再兜底 SIGKILL。
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    self._wait_process_group_empty,
                    pgid,
                    self.target.graceful_shutdown_seconds,
                ),
                timeout=self.target.graceful_shutdown_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning("任务 %s ACP CLI process group SIGTERM 超时，发送 SIGKILL: pgid=%s elapsed=%.3fs",
                           task_id[:8], pgid, time.monotonic() - started)
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError as error:
                logger.warning("任务 %s ACP CLI process group SIGKILL 权限不足: pgid=%s error=%s",
                               task_id[:8], pgid, error)
        logger.info("任务 %s ACP CLI process group 清理完成: pgid=%s elapsed=%.3fs",
                    task_id[:8], pgid, time.monotonic() - started)

    async def _wait_process_exit(self, process: asyncio.subprocess.Process) -> int:
        """Return the main process exit code without waiting for pipe EOF.

        asyncio's Process.wait() may be coupled to subprocess transport cleanup
        on some platforms. Poll both Process.returncode and waitpid(WNOHANG),
        tolerating whichever child watcher observes process exit first.
        """
        while True:
            if process.returncode is not None:
                return int(process.returncode)
            try:
                waited_pid, status = os.waitpid(process.pid, os.WNOHANG)
            except ChildProcessError:
                if process.returncode is not None:
                    return int(process.returncode)
                # Another child watcher has reaped it but returncode has not
                # propagated yet; yield once and check again.
                await asyncio.sleep(0.01)
                if process.returncode is not None:
                    return int(process.returncode)
                return 0
            if waited_pid == process.pid:
                if os.WIFEXITED(status):
                    return os.WEXITSTATUS(status)
                if os.WIFSIGNALED(status):
                    return 128 + os.WTERMSIG(status)
                return 255
            await asyncio.sleep(0.02)

    @staticmethod
    def _wait_process_group_empty(pgid: int, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                return
            except PermissionError:
                # The group still exists but cannot be signalled/probed from
                # this process on the current platform; keep waiting until the
                # caller's timeout decides whether to escalate.
                pass
            time.sleep(0.05)


class CursorAcpWorker(ExternalWorker):
    """Real Cursor ACP v1 worker.

    Each worker owns one long-lived ``agent acp`` stdio connection. Each relay
    task gets a fresh ACP session so independent tasks cannot share context.
    The legacy ``AcpWorker`` above intentionally remains the one-shot Cursor
    CLI implementation for existing ``type: acp`` configurations.
    """

    backend_type = BackendType.cursor_acp

    def __init__(self, worker_id: str, target: CursorAcpTargetConfig, store: TaskStore,
                 default_timeout_seconds: float, on_retry: RetryCallback) -> None:
        super().__init__(
            worker_id, target.id, target.model, store,
            target.timeout_seconds or default_timeout_seconds, on_retry,
        )
        self.target = target
        self._working_directory: Optional[str] = None
        self._resolved_command: Optional[str] = None
        self._connection: Optional[AcpConnection] = None
        self._session_id: Optional[str] = None
        self._prompt_future: Optional[asyncio.Future[Any]] = None
        self._prompt_started = False
        self._response_parts: list[str] = []
        self._sessions_created = 0
        self._session_lock = asyncio.Lock()

    def _resolve_command(self) -> str:
        command = os.path.expanduser(self.target.command)
        if os.path.isabs(command) or os.sep in command:
            if os.path.isfile(command) and os.access(command, os.X_OK):
                return command
            raise AcpExecutionError(f"Cursor ACP 命令不可执行: {command}")
        candidates = [
            shutil.which(command),
            os.path.expanduser(f"~/.local/bin/{command}"),
            os.path.expanduser(f"~/.cursor/bin/{command}"),
            f"/opt/homebrew/bin/{command}",
        ]
        for candidate in candidates:
            if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        raise AcpExecutionError(
            f"Cursor ACP 命令不可用: {self.target.command}（可配置绝对路径）")

    async def _validate_ready(self) -> None:
        self._resolved_command = self._resolve_command()
        path = os.path.abspath(os.path.expanduser(self.target.working_directory))
        if not os.path.isdir(path):
            raise AcpExecutionError(f"Cursor ACP 工作目录不存在: {path}")
        if self.target.api_key_env and not os.environ.get(self.target.api_key_env):
            raise AcpExecutionError(
                f"Cursor ACP API key 环境变量未设置: {self.target.api_key_env}")
        self._working_directory = path
        await self._ensure_connection()

    async def _ensure_connection(self) -> AcpConnection:
        if self._connection and self._connection.is_running:
            return self._connection
        if not self._resolved_command or not self._working_directory:
            raise AcpExecutionError("Cursor ACP worker 尚未完成本地配置检查")
        if self._connection:
            await self._connection.close()
        connection = AcpConnection(
            [self._resolved_command, *self.target.args],
            cwd=self._working_directory,
            env=self._child_env(),
            output_max_chars=self.target.output_max_chars,
            graceful_shutdown_seconds=self.target.graceful_shutdown_seconds,
            request_handler=self._handle_agent_request,
            notification_handler=self._handle_notification,
        )
        try:
            await connection.start()
            await asyncio.wait_for(
                connection.initialize(protocol_version=self.target.protocol_version),
                timeout=self.target.initialize_timeout_seconds,
            )
        except Exception:
            await connection.close()
            raise
        self._connection = connection
        self._sessions_created = 0
        logger.info(
            "worker %s 已建立 Cursor ACP 连接（agent=%s）",
            self.worker_id, connection.agent_info.get("name", "unknown"),
        )
        return connection

    async def stop(self) -> None:
        if self._connection and self._session_id:
            with suppress(Exception):
                await self._cancel_session()
        await super().stop()
        if self._connection:
            await self._connection.close()
            self._connection = None
        self._session_id = None

    async def _execute(self, task: TaskRecord) -> ExternalResult:
        await self._check_cancel(task)
        connection = await self._ensure_connection()
        async with self._session_lock:
            self._response_parts = []
            self._prompt_started = False
            self.store.set_phase(task.task_id, TaskPhase.sending)
            self.store.update_attempt(
                task.task_id, phase=TaskPhase.sending, send_state="sending")
            try:
                session = await connection.request(
                    "session/new", {"cwd": self._working_directory, "mcpServers": []},
                    timeout=self.target.request_timeout_seconds,
                )
                if not isinstance(session, dict) or not isinstance(session.get("sessionId"), str):
                    raise AcpExecutionError("Cursor ACP session/new 返回缺少 sessionId")
                self._session_id = session["sessionId"]
                self._sessions_created += 1
                self.store.update_attempt(
                    task.task_id, external_request_id=self._session_id,
                )
                await self._check_cancel(task)
                prompt_future = await connection.begin_request(
                    "session/prompt",
                    {
                        "sessionId": self._session_id,
                        "prompt": [{"type": "text", "text": task.execution_prompt()}],
                    },
                )
                self._prompt_future = prompt_future
                self._prompt_started = True
                self.store.set_phase(task.task_id, TaskPhase.generating)
                self.store.update_attempt(
                    task.task_id, phase=TaskPhase.generating,
                    send_state="sent_confirmed",
                )
                try:
                    result = await asyncio.wait_for(
                        asyncio.shield(prompt_future),
                        timeout=self.target.request_timeout_seconds,
                    )
                except asyncio.TimeoutError as error:
                    raise ExternalOutcomeUnknownError(
                        "Cursor ACP session/prompt 响应超时，结果未知") from error
            except asyncio.CancelledError:
                await self._cancel_session()
                raise
            except AcpRpcError as error:
                if error.code == -32800:
                    raise ExternalTaskCancelledError("Cursor ACP prompt 已取消") from error
                if self._prompt_started:
                    raise ExternalOutcomeUnknownError(str(error)) from error
                raise AcpExecutionError(str(error)) from error
            except AcpConnectionClosed as error:
                if self._prompt_started:
                    raise ExternalOutcomeUnknownError(str(error)) from error
                raise RetryableExternalWorkerError(str(error)) from error
            except AcpProtocolError as error:
                if self._prompt_started:
                    raise ExternalOutcomeUnknownError(str(error)) from error
                raise RetryableExternalWorkerError(str(error)) from error
            finally:
                self._prompt_future = None
                session_id = self._session_id
                self._session_id = None
                if session_id:
                    await self._finish_session(connection, session_id)

            if not isinstance(result, dict):
                raise AcpExecutionError("Cursor ACP session/prompt 返回格式无效")
            stop_reason = result.get("stopReason")
            if stop_reason == "cancelled":
                raise ExternalTaskCancelledError("Cursor ACP prompt 已取消")
            if stop_reason not in {"end_turn", "max_tokens", "max_turn_requests"}:
                raise AcpExecutionError(
                    f"Cursor ACP prompt 未正常结束: {stop_reason or 'unknown'}")
            text = "".join(self._response_parts).strip()
            if not text:
                raise AcpExecutionError("Cursor ACP 未返回文本结果")
            self.store.set_phase(task.task_id, TaskPhase.collecting_result)
            return ExternalResult(text=text, external_request_id=session_id)

    async def _finish_session(self, connection: AcpConnection, session_id: str) -> None:
        capabilities = connection.agent_capabilities or {}
        session_caps = capabilities.get("sessionCapabilities") or {}
        can_close = "close" in session_caps or "close" in capabilities
        if can_close and connection.is_running:
            with suppress(Exception):
                await connection.request(
                    "session/close", {"sessionId": session_id},
                    timeout=self.target.session_close_timeout_seconds,
                )
        if (
            not can_close
            and self._sessions_created >= self.target.max_sessions_per_connection
        ):
            await connection.close()
            if self._connection is connection:
                self._connection = None
            self._sessions_created = 0

    async def _cancel_session(self) -> None:
        connection = self._connection
        session_id = self._session_id
        if not connection or not session_id or not connection.is_running:
            return
        with suppress(Exception):
            await connection.notify("session/cancel", {"sessionId": session_id})
        if self._prompt_future and not self._prompt_future.done():
            with suppress(Exception):
                await asyncio.wait_for(
                    asyncio.shield(self._prompt_future), timeout=5.0)

    async def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if method != "session/update":
            return
        if self._session_id and params.get("sessionId") != self._session_id:
            return
        update = params.get("update") or {}
        if not isinstance(update, dict):
            return
        kind = update.get("sessionUpdate")
        if kind == "agent_message_chunk":
            text = self._text_from_content(update.get("content"))
            if text:
                self._response_parts.append(text)
        elif kind in {"tool_call", "tool_call_update", "plan"}:
            logger.debug("Cursor ACP %s update: %s", kind, update.get("toolCallId", "-"))

    async def _handle_agent_request(self, method: str, params: dict[str, Any]) -> Any:
        if method == "session/request_permission":
            options = params.get("options") or []
            reject = next(
                (item for item in options
                 if isinstance(item, dict) and item.get("kind") == "reject_once"),
                None,
            )
            if reject and reject.get("optionId"):
                return {"outcome": {"outcome": "selected", "optionId": reject["optionId"]}}
            return {"outcome": {"outcome": "cancelled"}}
        if method == "fs/read_text_file":
            return await self._read_workspace_file(params)
        raise AcpRpcError(-32601, f"ai-relay 不支持 ACP 请求: {method}")

    async def _read_workspace_file(self, params: dict[str, Any]) -> dict[str, str]:
        if not self._working_directory:
            raise AcpRpcError(-32603, "工作目录尚未初始化")
        raw_path = params.get("path")
        if not isinstance(raw_path, str) or not os.path.isabs(raw_path):
            raise AcpRpcError(-32602, "fs/read_text_file.path 必须是绝对路径")
        try:
            path = Path(raw_path).expanduser().resolve()
            root = Path(self._working_directory).resolve()
            path.relative_to(root)
        except (OSError, ValueError) as error:
            raise AcpRpcError(-32602, "只能读取工作目录内的文件") from error
        if not path.is_file():
            raise AcpRpcError(-32000, f"文件不存在: {path}")
        try:
            content = await asyncio.to_thread(path.read_text, encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise AcpRpcError(-32000, f"读取文件失败: {error}") from error
        line = params.get("line")
        limit = params.get("limit")
        if isinstance(line, int) and line > 1 or isinstance(limit, int) and limit >= 0:
            lines = content.splitlines(keepends=True)
            start = max(0, line - 1) if isinstance(line, int) and line > 0 else 0
            end = start + limit if isinstance(limit, int) and limit >= 0 else None
            content = "".join(lines[start:end])
        if len(content) > self.target.output_max_chars:
            raise AcpRpcError(-32000, "文件内容超过 relay 输出上限")
        return {"content": content}

    def _child_env(self) -> dict[str, str]:
        env = {
            key: value for key, value in os.environ.items()
            if key in {"HOME", "LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"}
        }
        if self.target.api_key_env:
            key = os.environ.get(self.target.api_key_env)
            if not key:
                raise AcpExecutionError(
                    f"Cursor ACP API key 环境变量未设置: {self.target.api_key_env}")
            env["CURSOR_API_KEY"] = key
        return env

    async def _check_cancel(self, task: TaskRecord) -> None:
        current = self.store.get(task.task_id)
        if current and current.cancel_requested:
            raise ExternalTaskCancelledError("任务已请求取消")

    @staticmethod
    def _text_from_content(content: Any) -> str:
        if isinstance(content, dict):
            return content.get("text", "") if content.get("type") == "text" else ""
        if isinstance(content, list):
            return "".join(CursorAcpWorker._text_from_content(item) for item in content)
        return content if isinstance(content, str) else ""


class OpenAICompatibleWorker(ExternalWorker):
    backend_type = BackendType.openai_compatible

    def __init__(self, worker_id: str, target: OpenAICompatibleTargetConfig, store: TaskStore,
                 default_timeout_seconds: float, on_retry: RetryCallback) -> None:
        super().__init__(
            worker_id, target.id, target.model, store,
            target.timeout_seconds or default_timeout_seconds, on_retry,
        )
        self.target = target
        self._client: Optional[httpx.AsyncClient] = None
        self._api_key: Optional[str] = None

    async def _validate_ready(self) -> None:
        key = os.environ.get(self.target.api_key_env)
        if not key:
            raise ExternalWorkerError(
                f"API key 环境变量未设置: {self.target.api_key_env}")
        self._api_key = key
        self._client = httpx.AsyncClient(
            base_url=self.target.base_url.rstrip("/"),
            timeout=self.timeout_seconds,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        )
        if self.target.verify_model_on_start:
            await self._verify_model_available()

    async def _verify_model_available(self) -> None:
        """只读校验配置模型仍被上游公开；失败则 target 启动为 degraded。"""
        if not self._client:
            raise ExternalWorkerError("OpenAI-compatible client 未初始化")
        try:
            response = await self._client.get("models")
        except httpx.HTTPError as error:
            raise ExternalWorkerError(f"API 模型列表检查失败: {error}") from error
        if response.status_code >= 400:
            raise ExternalWorkerError(
                f"API 模型列表检查失败: HTTP {response.status_code}"
            )
        try:
            payload = response.json()
            rows = payload.get("data", [])
            model_ids = {
                str(row.get("id"))
                for row in rows
                if isinstance(row, dict) and row.get("id")
            }
        except (TypeError, ValueError) as error:
            raise ExternalWorkerError(f"API 模型列表响应无效: {error}") from error
        if self.target.model not in model_ids:
            raise ExternalWorkerError(
                f"API 配置模型不存在: {self.target.model}；上游当前模型: "
                f"{', '.join(sorted(model_ids)) or '(empty)'}"
            )

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
        await super().stop()

    async def _execute(self, task: TaskRecord) -> ExternalResult:
        if not self._client or not self._api_key:
            raise ExternalWorkerError("OpenAI-compatible client 未初始化")
        current = self.store.get(task.task_id)
        if current and current.cancel_requested:
            raise ExternalTaskCancelledError("任务已请求取消")

        model = task.model or self.target.model
        # task.model 总会持久化最终模型；只有与 target 默认模型不同才属于调用方覆盖。
        is_override = bool(task.model and task.model != self.target.model)
        if is_override and not self.target.allow_model_override:
            raise ExternalWorkerError("该 API target 不允许覆盖模型")
        if is_override and task.model not in self.target.allowed_models:
            raise ExternalWorkerError(f"请求模型不在 allowlist: {task.model}")

        self.store.set_phase(task.task_id, TaskPhase.sending)
        self.store.update_attempt(task.task_id, phase=TaskPhase.sending, send_state="sending")
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": task.execution_prompt()}],
            "stream": False,
        }
        if self.target.max_output_tokens:
            body["max_tokens"] = self.target.max_output_tokens
        headers = {"Idempotency-Key": task.task_id}
        try:
            # 使用相对路径，保留 base_url 中的 /v1 前缀。
            response = await self._client.post("chat/completions", json=body, headers=headers)
        except httpx.ConnectError as error:
            raise RetryableExternalWorkerError(f"API 连接失败: {error}") from error
        except httpx.ConnectTimeout as error:
            raise RetryableExternalWorkerError(f"API 连接超时: {error}") from error
        except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError) as error:
            raise ExternalOutcomeUnknownError(f"API 请求已发出但响应不确定: {error}") from error
        except httpx.HTTPError as error:
            raise ExternalOutcomeUnknownError(f"API 请求状态不确定: {error}") from error

        request_id = response.headers.get("x-request-id") or response.headers.get("request-id")
        # 已收到明确 HTTP 响应，外部请求的发送结果已确认；后续即使内容不合格，
        # 也属于可由 relay 按预算安全重试的确定失败，不应留在 sending。
        self.store.update_attempt(
            task.task_id,
            phase=TaskPhase.collecting_result,
            send_state="sent_confirmed",
            provider_status_code=response.status_code,
        )
        if response.status_code in {408, 409, 425, 429}:
            raise ApiResponseError(
                self._response_error(response), code="api_retryable_status",
                status_code=response.status_code, retryable=True,
            )
        if response.status_code >= 500:
            raise ApiResponseError(
                self._response_error(response), code="api_server_error",
                status_code=response.status_code,
                retryable=self.target.retry_server_errors,
                outcome_unknown=not self.target.retry_server_errors,
            )
        if response.status_code >= 400:
            code = "api_auth_error" if response.status_code in {401, 403} else "api_request_error"
            raise ApiResponseError(self._response_error(response), code=code,
                                   status_code=response.status_code)
        try:
            data = response.json()
            choice = data["choices"][0]
            message = choice["message"]
            content = message.get("content")
            if content is None:
                finish_reason = choice.get("finish_reason")
                message_keys = sorted(message) if isinstance(message, dict) else []
                raise ValueError(
                    f"message.content 缺失（finish_reason={finish_reason!r}, "
                    f"message_keys={message_keys}）"
                )
            text = self._content_to_text(content)
        except (KeyError, IndexError, TypeError, ValueError) as error:
            raise ApiResponseError(
                f"API 响应格式无效: {error}",
                code="api_invalid_response",
                status_code=response.status_code,
                retryable=True,
            ) from error
        if not text.strip():
            raise ApiResponseError(
                "API 返回空回答",
                code="api_empty_response",
                status_code=response.status_code,
                retryable=True,
            )
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else None
        external_id = str(data.get("id")) if data.get("id") else request_id
        self.store.set_phase(task.task_id, TaskPhase.collecting_result)
        return ExternalResult(
            text=text, external_request_id=external_id,
            provider_status_code=response.status_code, usage=usage,
        )

    @staticmethod
    def _content_to_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            return "".join(parts)
        raise TypeError("message.content 不是 string 或内容块数组")

    @staticmethod
    def _response_error(response: httpx.Response) -> str:
        try:
            data = response.json()
            error = data.get("error", data)
            if isinstance(error, dict):
                return str(error.get("message") or error)
        except ValueError:
            pass
        return f"API HTTP {response.status_code}"
