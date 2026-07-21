"""kimi-webbridge daemon 的异步 HTTP client。

daemon 接口：POST {base}/command  body: {"action": ..., "args": {...}, "session": ...}
健康检查：GET {base}/status
"""
from __future__ import annotations

from typing import Any, Optional

import httpx


class WebbridgeError(RuntimeError):
    pass


class WebbridgeClient:
    def __init__(self, base_url: str = "http://127.0.0.1:10086", timeout: float = 60.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()

    async def status(self) -> dict:
        try:
            resp = await self._client.get(f"{self.base_url}/status")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            raise WebbridgeError(f"webbridge status 失败: {e}") from e

    async def healthy(self) -> tuple[bool, str]:
        try:
            data = await self.status()
        except WebbridgeError as e:
            return False, str(e)
        if not data.get("running"):
            return False, "daemon 未运行"
        if not data.get("extension_connected"):
            return False, "浏览器扩展未连接"
        return True, "ok"

    async def command(self, action: str, args: Optional[dict] = None,
                      session: Optional[str] = None) -> Any:
        body: dict[str, Any] = {"action": action, "args": args or {}}
        if session:
            body["session"] = session
        try:
            resp = await self._client.post(f"{self.base_url}/command", json=body)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            raise WebbridgeError(f"webbridge {action} 请求失败: {e}") from e
        if isinstance(data, dict) and data.get("success") is False:
            raise WebbridgeError(f"webbridge {action} 失败: {data.get('error') or data}")
        # daemon 返回结构：{"success": true, "data": ...} 或直接结果
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data

    # ---- 常用动作封装 ----

    async def navigate(self, url: str, session: str, new_tab: bool = True,
                       group_title: Optional[str] = None) -> Any:
        args: dict[str, Any] = {"url": url, "newTab": new_tab}
        if group_title:
            args["group_title"] = group_title
        return await self.command("navigate", args, session)

    async def snapshot(self, session: str) -> Any:
        return await self.command("snapshot", {}, session)

    async def click(self, selector: str, session: str) -> Any:
        return await self.command("click", {"selector": selector}, session)

    async def fill(self, selector: str, value: str, session: str) -> Any:
        return await self.command("fill", {"selector": selector, "value": value}, session)

    async def evaluate(self, code: str, session: str) -> Any:
        result = await self.command("evaluate", {"code": code}, session)
        # evaluate 返回 {"type": ..., "value": ...}
        if isinstance(result, dict) and "value" in result:
            return result["value"]
        return result

    async def list_tabs(self, session: str) -> Any:
        return await self.command("list_tabs", {}, session)

    async def close_session(self, session: str) -> Any:
        return await self.command("close_session", {}, session)
