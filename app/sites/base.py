"""站点适配器抽象基类。

一个适配器绑定一个 webbridge session（即一个浏览器 tab），
负责在对应 AI 网页上完成登录检查、模型选择、发消息、读结果。
"""
from __future__ import annotations

import json
from abc import ABC, abstractmethod

from ..webbridge import WebbridgeClient


class SiteAdapter(ABC):
    home_url: str

    def __init__(self, client: WebbridgeClient, session: str):
        self.client = client
        self.session = session

    @abstractmethod
    async def is_logged_in(self) -> bool:
        """检查当前页面是否已登录。"""

    @abstractmethod
    async def ensure_model(self, model: str) -> None:
        """选择/校验模型；model 为空则使用站点默认。失败抛 WebbridgeError。"""

    @abstractmethod
    async def new_chat(self) -> None:
        """开启一个新对话。"""

    @abstractmethod
    async def send_prompt(self, prompt: str) -> None:
        """填入提示词并发送。"""

    @abstractmethod
    async def poll_once(self) -> dict:
        """做一次 DOM 检查，返回 {"generating": bool, "answer": str | None}。"""

    async def _eval_json(self, code: str) -> dict:
        """evaluate 一段返回 JSON 字符串的 JS 并解析。"""
        raw = await self.client.evaluate(code, self.session)
        if isinstance(raw, dict):
            return raw
        return json.loads(raw)
