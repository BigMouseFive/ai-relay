"""ErpRegistrar 单元测试（HTTP 全部 mock）。"""
from __future__ import annotations

import pytest

from app.config import ErpConfig
from app.registrar import ErpRegistrar, detect_lan_ip


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx
            raise httpx.HTTPStatusError("err", request=None, response=None)


class FakeClient:
    def __init__(self, status_code=200, exc=None):
        self.status_code = status_code
        self.exc = exc
        self.calls = []

    async def post(self, url, json):
        self.calls.append((url, json))
        if self.exc:
            raise self.exc
        return FakeResponse(self.status_code)


def _cfg(**kw):
    return ErpConfig(register_url="http://erp.test/api/system/relay-nodes/heartbeat", **kw)


async def test_beat_payload_defaults():
    client = FakeClient()
    r = ErpRegistrar(_cfg(), port=8600, client=client)
    ok = await r.beat_once()
    assert ok is True
    url, payload = client.calls[0]
    assert url == "http://erp.test/api/system/relay-nodes/heartbeat"
    assert payload["url"].startswith("http://")
    assert payload["url"].endswith(":8600")
    assert payload["name"]  # 主机名自动填充
    assert "token" not in payload  # 未配置 token 不上送


async def test_beat_payload_explicit_values():
    client = FakeClient()
    r = ErpRegistrar(_cfg(node_name="mac-1", advertise_url="http://192.168.1.5:8600/",
                          token="s3cret"), port=8600, client=client)
    await r.beat_once()
    _, payload = client.calls[0]
    assert payload == {"url": "http://192.168.1.5:8600", "name": "mac-1", "token": "s3cret"}


async def test_beat_http_error_returns_false():
    client = FakeClient(status_code=500)
    r = ErpRegistrar(_cfg(), port=8600, client=client)
    assert await r.beat_once() is False


async def test_beat_network_error_returns_false():
    client = FakeClient(exc=ConnectionError("refused"))
    r = ErpRegistrar(_cfg(), port=8600, client=client)
    assert await r.beat_once() is False


def test_detect_lan_ip():
    ip = detect_lan_ip()
    assert ip.count(".") == 3
