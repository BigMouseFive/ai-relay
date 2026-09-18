"""MinimaxAdapter 单元测试（webbridge 全部 mock）。

重点锁定：
1. ensure_model 始终把 Agent 团队 / 思考两个开关保持关闭（先读状态，开才点）
2. send_prompt 发送确认不得误点（stop-button 出现即视为已发送）
"""
from __future__ import annotations

import json

import pytest

from app.sites.minimax import MinimaxAdapter
from app.webbridge import WebbridgeError


class FakeClient:
    def __init__(self, agent_team_on=True, thinking_on=True):
        self.clicks = []
        self.fills = []
        self.toggles = {
            "agent-team-toggle": agent_team_on,
            "model-thinking-trigger-toggle": thinking_on,
        }
        self.editor_filled = False
        self.stop_button = False

    async def fill(self, selector, value, session):
        self.fills.append((selector, value))
        self.editor_filled = True

    async def click(self, selector, session):
        self.clicks.append(selector)
        if "send-button" in selector:
            self.stop_button = True

    async def evaluate(self, code, session):
        # 开关相关 JS：解析出 testid 后按当前状态模拟
        for tid in self.toggles:
            if tid in code:
                if "aria-checked') === 'true') b.click()" in code:
                    if self.toggles[tid]:
                        self.toggles[tid] = False  # 模拟点击关闭
                    return "OK"
                if "getAttribute('aria-checked')" in code:
                    return "true" if self.toggles[tid] else "false"
        if "innerText.trim().length > 0" in code or "el.tagName === 'TEXTAREA'" in code:
            return self.editor_filled
        if "assistant-active-flow" in code:
            return json.dumps({"generating": False, "answer": "42", "error": None})
        if "return hasPrompt ||" in code:
            return self.stop_button
        if "model-selector-trigger" in code:
            return "MiniMax-M3"
        raise AssertionError(f"未预期的 evaluate: {code[:100]}")


def _adapter(client):
    return MinimaxAdapter(client, "relay-test")


async def test_ensure_model_turns_both_toggles_off():
    client = FakeClient(agent_team_on=True, thinking_on=True)
    adapter = _adapter(client)
    await adapter.ensure_model("")
    assert client.toggles["agent-team-toggle"] is False
    assert client.toggles["model-thinking-trigger-toggle"] is False


async def test_ensure_model_keeps_off_toggles_untouched():
    client = FakeClient(agent_team_on=False, thinking_on=False)
    adapter = _adapter(client)
    await adapter.ensure_model("")
    assert client.toggles["agent-team-toggle"] is False
    assert client.toggles["model-thinking-trigger-toggle"] is False
    # 已关闭的开关不应被点击（无 send-button 以外的 click）
    assert client.clicks == []


async def test_send_prompt_stops_clicking_when_stop_button_appears():
    client = FakeClient()
    adapter = _adapter(client)
    await adapter.send_prompt("你好")
    assert client.clicks == ['[data-testid="send-button"]']


async def test_send_prompt_raises_when_editor_never_filled():
    client = FakeClient()

    async def noop_fill(selector, value, session):
        client.fills.append((selector, value))

    client.fill = noop_fill
    adapter = _adapter(client)
    with pytest.raises(WebbridgeError, match="填入失败"):
        await adapter.send_prompt("你好")
    assert client.clicks == []


async def test_poll_once_passes_through():
    client = FakeClient()
    adapter = _adapter(client)
    snap = await adapter.poll_once()
    assert snap == {"generating": False, "answer": "42", "error": None}
