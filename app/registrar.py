"""向 ERP 心跳注册本节点（局域网动态接入）。

ERP 侧接口：POST {register_url}  body: {"url": ..., "name": ..., "token": ...}
节点靠周期心跳保持在 ERP 的在线列表里；ERP 超过 RELAY_NODE_STALE_SECONDS
收不到心跳会自动将节点置为离线，因此进程退出无需注销。
"""
from __future__ import annotations

import asyncio
import logging
import socket
from contextlib import suppress
from typing import Optional

import httpx

from .config import ErpConfig

logger = logging.getLogger("ai-relay.registrar")


def detect_lan_ip() -> str:
    """UDP 连接法探测本机局域网 IP（不产生真实流量）。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


class ErpRegistrar:
    def __init__(self, cfg: ErpConfig, port: int,
                 client: Optional[httpx.AsyncClient] = None):
        self.register_url = cfg.register_url
        self.node_name = cfg.node_name or socket.gethostname()
        self.advertise_url = (cfg.advertise_url
                              or f"http://{detect_lan_ip()}:{port}").rstrip("/")
        self.token = cfg.token
        self.interval = cfg.interval_seconds
        self._client = client
        self._own_client = client is None
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._own_client:
            self._client = httpx.AsyncClient(timeout=10)
        self._task = asyncio.create_task(self._loop())
        logger.info("ERP 注册已启动：%s -> %s（%ds 间隔）",
                    self.advertise_url, self.register_url, self.interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        if self._own_client and self._client:
            await self._client.aclose()

    async def beat_once(self) -> bool:
        """发一次心跳，返回是否成功。"""
        payload = {"url": self.advertise_url, "name": self.node_name}
        if self.token:
            payload["token"] = self.token
        try:
            resp = await self._client.post(self.register_url, json=payload)
            resp.raise_for_status()
            logger.debug("ERP 心跳成功")
            return True
        except Exception as e:
            logger.warning("ERP 心跳失败（下轮重试）: %s", e)
            return False

    async def _loop(self) -> None:
        while True:
            await self.beat_once()
            await asyncio.sleep(self.interval)
