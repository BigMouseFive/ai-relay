"""KimiAdapter 单元测试（webbridge 全部 mock）。

重点锁定两个回归：
1. send_prompt 确认循环不得误点"停止"按钮（按钮带 .stop 即视为已发送）
2. "已停止内容生成"占位文本必须识别为站点错误（error 字段）
"""
from __future__ import annotations

import json

import pytest

from app.sites.kimi import KimiAdapter
from app.webbridge import WebbridgeError


class FakeClient:
    """按脚本响应的 webbridge fake。evaluate 按代码内容分派。"""

    def __init__(self):
        self.clicks = []
        self.fills = []
        # 状态模拟：fill 后编辑器有字；click 后按钮变 stop（生成中）
        self.editor_filled = False
        self.button_stop = False

    async def fill(self, selector, value, session):
        self.fills.append((selector, value))
        self.editor_filled = True

    async def click(self, selector, session):
        self.clicks.append(selector)
        if "send-button" in selector:
            self.button_stop = True  # 真实页面：点击发送后按钮立即变停止态

    async def evaluate(self, code, session):
        if "innerText.trim().length > 0" in code:
            return self.editor_filled
        if ".send-button-container.stop" in code and "segment-user" in code:
            # send_prompt 的"已发送"检测：模拟页面还没跳转，但按钮已 stop
            return self.button_stop
        if ".segment.segment-assistant" in code:
            return self.poll_response
        raise AssertionError(f"未预期的 evaluate: {code[:80]}")

    async def navigate(self, *a, **kw):
        pass


def _adapter(client):
    return KimiAdapter(client, "relay-test")


async def test_send_prompt_never_clicks_stop_button():
    client = FakeClient()
    adapter = _adapter(client)
    await adapter.send_prompt("你好")
    # 只点了一次发送：确认循环发现按钮已 stop（生成中）立即返回，没有重复点击
    assert client.clicks == [".send-button-container"]


async def test_send_prompt_retries_when_editor_empty():
    client = FakeClient()
    # 前两次 fill 不落字（模拟 Lexical 未就绪），第三次才成功
    real_fill = client.fill
    attempts = {"n": 0}

    async def flaky_fill(selector, value, session):
        attempts["n"] += 1
        if attempts["n"] >= 3:
            await real_fill(selector, value, session)
        else:
            client.fills.append((selector, value))

    client.fill = flaky_fill
    adapter = _adapter(client)
    await adapter.send_prompt("你好")
    assert attempts["n"] == 3
    assert client.clicks == [".send-button-container"]


async def test_send_prompt_raises_when_editor_never_filled():
    client = FakeClient()

    async def noop_fill(selector, value, session):
        client.fills.append((selector, value))  # 永远不落字

    client.fill = noop_fill
    adapter = _adapter(client)
    with pytest.raises(WebbridgeError, match="填入失败"):
        await adapter.send_prompt("你好")
    # 填入失败不应点发送
    assert client.clicks == []


async def test_poll_detects_stopped_placeholder():
    client = FakeClient()
    client.poll_response = json.dumps(
        {"generating": False, "answer": None, "error": "生成被中止（已停止内容生成）"})
    adapter = _adapter(client)
    snap = await adapter.poll_once()
    assert snap["error"] == "生成被中止（已停止内容生成）"
    assert snap["answer"] is None
    assert snap["generating"] is False


async def test_poll_normal_answer():
    client = FakeClient()
    client.poll_response = json.dumps({"generating": False, "answer": "42", "error": None})
    adapter = _adapter(client)
    snap = await adapter.poll_once()
    assert snap == {"generating": False, "answer": "42", "error": None}
