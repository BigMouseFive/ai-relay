"""Minimal ACP v1 stdio JSON-RPC client used by the Cursor ACP worker."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from collections.abc import Coroutine
from typing import Any, Callable, Optional

logger = logging.getLogger("ai-relay.acp-protocol")


class AcpProtocolError(RuntimeError):
    """The ACP peer returned an invalid or unsuccessful protocol response."""


class AcpConnectionClosed(AcpProtocolError):
    """The ACP subprocess or stdio transport closed unexpectedly."""


class AcpRpcError(AcpProtocolError):
    """A JSON-RPC error response from the ACP agent."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"ACP JSON-RPC 错误 {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


RequestHandler = Callable[[str, dict[str, Any]], Coroutine[Any, Any, Any]]
NotificationHandler = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]


class AcpConnection:
    """One ACP agent subprocess and its newline-delimited JSON-RPC transport.

    The connection owns the subprocess pipes. Consequently, two instances can
    use the same ``agent acp`` command without any process identifier in the
    ACP payload: the pipe pair itself identifies the peer.
    """

    def __init__(
        self,
        argv: list[str],
        *,
        cwd: str,
        env: Optional[dict[str, str]] = None,
        output_max_chars: int = 200_000,
        graceful_shutdown_seconds: float = 5.0,
        request_handler: Optional[RequestHandler] = None,
        notification_handler: Optional[NotificationHandler] = None,
    ) -> None:
        self.argv = argv
        self.cwd = cwd
        self.env = env
        self.output_max_chars = output_max_chars
        self.graceful_shutdown_seconds = graceful_shutdown_seconds
        self.request_handler = request_handler
        self.notification_handler = notification_handler
        self.process: Optional[asyncio.subprocess.Process] = None
        self.agent_capabilities: dict[str, Any] = {}
        self.agent_info: dict[str, Any] = {}
        self._next_id = 1
        self._pending: dict[Any, asyncio.Future[Any]] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._stderr_task: Optional[asyncio.Task[None]] = None
        self._request_tasks: set[asyncio.Task[None]] = set()
        self._stderr_tail: list[str] = []
        self._closed = False

    @property
    def is_running(self) -> bool:
        return bool(self.process and self.process.returncode is None and not self._closed)

    @property
    def stderr_tail(self) -> str:
        return "\n".join(self._stderr_tail)

    async def start(self) -> None:
        if self.process is not None:
            raise AcpProtocolError("ACP 连接已经启动")
        try:
            self.process = await asyncio.create_subprocess_exec(
                *self.argv,
                cwd=self.cwd,
                env=self.env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                limit=max(64 * 1024, min(self.output_max_chars + 1, 8 * 1024 * 1024)),
            )
        except OSError as error:
            raise AcpProtocolError(f"ACP 子进程无法启动: {error}") from error
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())

    async def initialize(
        self,
        *,
        client_name: str = "ai-relay",
        client_version: str = "1.0",
        protocol_version: int = 1,
    ) -> dict[str, Any]:
        result = await self.request(
            "initialize",
            {
                "protocolVersion": protocol_version,
                "clientCapabilities": {
                    "fs": {"readTextFile": True, "writeTextFile": False},
                },
                "clientInfo": {
                    "name": client_name,
                    "title": "AI Relay",
                    "version": client_version,
                },
            },
        )
        if not isinstance(result, dict):
            raise AcpProtocolError("ACP initialize 返回格式无效")
        selected = result.get("protocolVersion")
        if selected != protocol_version:
            raise AcpProtocolError(
                f"ACP 协议版本不兼容: agent={selected!r}, client={protocol_version}")
        self.agent_capabilities = result.get("agentCapabilities") or {}
        self.agent_info = result.get("agentInfo") or {}
        return result

    async def request(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> Any:
        future = await self.begin_request(method, params)
        try:
            if timeout is None:
                return await future
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError:
            self._remove_future(future)
            if not future.done():
                future.cancel()
            raise

    async def begin_request(
        self, method: str, params: Optional[dict[str, Any]] = None,
    ) -> asyncio.Future[Any]:
        if not self.is_running or self.process is None or self.process.stdin is None:
            raise AcpConnectionClosed("ACP 连接未运行")
        request_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future
        message: dict[str, Any] = {
            "jsonrpc": "2.0", "id": request_id, "method": method,
        }
        if params is not None:
            message["params"] = params
        try:
            await self._send(message)
        except BaseException:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise
        return future

    async def notify(self, method: str, params: Optional[dict[str, Any]] = None) -> None:
        if not self.is_running:
            raise AcpConnectionClosed("ACP 连接未运行")
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        await self._send(message)

    async def close(self) -> None:
        if self._closed and self.process is None:
            return
        self._closed = True
        for future in self._pending.values():
            if not future.done():
                future.set_exception(AcpConnectionClosed("ACP 连接已关闭"))
        self._pending.clear()
        for task in list(self._request_tasks):
            task.cancel()
        if self._request_tasks:
            await asyncio.gather(*self._request_tasks, return_exceptions=True)
        process = self.process
        if process and process.returncode is None:
            await self._terminate_process(process)
        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        for task in (self._reader_task, self._stderr_task):
            if task:
                with suppress(asyncio.CancelledError, Exception):
                    await task
        self._reader_task = None
        self._stderr_task = None
        self.process = None

    async def _send(self, message: dict[str, Any]) -> None:
        process = self.process
        if not process or process.stdin is None or process.returncode is not None:
            raise AcpConnectionClosed("ACP 进程已经退出")
        encoded = (
            json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        if len(encoded) > self.output_max_chars:
            raise AcpProtocolError("ACP JSON-RPC 消息超过大小上限")
        async with self._write_lock:
            try:
                process.stdin.write(encoded)
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionError) as error:
                raise AcpConnectionClosed("写入 ACP stdin 失败") from error

    async def _read_stdout(self) -> None:
        process = self.process
        if not process or process.stdout is None:
            return
        try:
            while True:
                raw = await process.stdout.readline()
                if not raw:
                    break
                if len(raw) > self.output_max_chars:
                    raise AcpProtocolError("ACP stdout 消息超过大小上限")
                try:
                    message = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise AcpProtocolError("ACP stdout 包含非法 JSON-RPC 消息") from error
                if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                    raise AcpProtocolError("ACP stdout 不是 JSON-RPC 2.0 消息")
                if "id" in message and ("result" in message or "error" in message):
                    self._resolve_response(message)
                elif isinstance(message.get("method"), str):
                    await self._dispatch_message(message)
                else:
                    raise AcpProtocolError("ACP JSON-RPC 消息缺少 method 或 response 字段")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            protocol_error = error if isinstance(error, Exception) else AcpProtocolError(str(error))
            self._fail_pending(protocol_error)
            logger.warning("ACP stdout reader stopped: %s", error)
        finally:
            if not self._closed:
                self._fail_pending(AcpConnectionClosed(
                    f"ACP 进程已退出 (code={process.returncode if process else None})"
                    + (f": {self.stderr_tail}" if self.stderr_tail else "")
                ))

    async def _read_stderr(self) -> None:
        process = self.process
        if not process or process.stderr is None:
            return
        try:
            while True:
                raw = await process.stderr.readline()
                if not raw:
                    return
                text = raw.decode("utf-8", errors="replace").rstrip()
                if text:
                    self._stderr_tail.append(text)
                    del self._stderr_tail[:-20]
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.debug("ACP stderr reader stopped: %s", error)

    def _resolve_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("id")
        future = self._pending.pop(request_id, None)
        if future is None or future.done():
            logger.debug("忽略 ACP 未匹配响应 id=%r", request_id)
            return
        error = message.get("error")
        if isinstance(error, dict):
            future.set_exception(AcpRpcError(
                int(error.get("code", -32000)),
                str(error.get("message", "unknown error")),
                error.get("data"),
            ))
        elif "result" in message:
            future.set_result(message.get("result"))
        else:
            future.set_exception(AcpProtocolError("ACP 响应缺少 result/error"))

    async def _dispatch_message(self, message: dict[str, Any]) -> None:
        method = message["method"]
        params = message.get("params") or {}
        if not isinstance(params, dict):
            params = {}
        if "id" in message:
            task = asyncio.create_task(self._handle_request(message["id"], method, params))
            self._request_tasks.add(task)
            task.add_done_callback(self._request_tasks.discard)
        elif self.notification_handler is not None:
            # Preserve stdout order: a session/update notification preceding a
            # prompt response must be applied before the prompt future resumes.
            await self.notification_handler(method, params)

    async def _handle_request(self, request_id: Any, method: str, params: dict[str, Any]) -> None:
        if self.request_handler is None:
            await self._send_response(request_id, error={"code": -32601, "message": "Method not found"})
            return
        try:
            result = await self.request_handler(method, params)
        except AcpRpcError as error:
            await self._send_response(
                request_id,
                error={"code": error.code, "message": error.message, "data": error.data},
            )
        except Exception as error:
            logger.warning("ACP server request %s 处理失败: %s", method, error)
            await self._send_response(
                request_id,
                error={"code": -32000, "message": str(error) or type(error).__name__},
            )
        else:
            await self._send_response(request_id, result=result)

    async def _send_response(
        self, request_id: Any, *, result: Any = None, error: Optional[dict[str, Any]] = None,
    ) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            message["error"] = error
        else:
            message["result"] = result
        await self._send(message)

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    def _remove_future(self, target: asyncio.Future[Any]) -> None:
        for request_id, future in list(self._pending.items()):
            if future is target:
                self._pending.pop(request_id, None)
                return

    @staticmethod
    def _log_task_error(task: asyncio.Task[Any]) -> None:
        with suppress(asyncio.CancelledError):
            error = task.exception()
            if error:
                logger.warning("ACP notification handler failed: %s", error)

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        with suppress(ProcessLookupError):
            if hasattr(os, "killpg"):
                os.killpg(process.pid, signal.SIGTERM)
            else:  # pragma: no cover - macOS/Linux use killpg
                process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=self.graceful_shutdown_seconds)
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                if hasattr(os, "killpg"):
                    os.killpg(process.pid, signal.SIGKILL)
                else:  # pragma: no cover
                    process.kill()
            with suppress(Exception):
                await process.wait()
