"""agent.minimaxi.com 站点适配器（选择器来自实测调研，优先 data-testid）。"""
from __future__ import annotations

import asyncio
import json
import logging

from ..webbridge import WebbridgeError
from .base import SendOutcomeUnknownError, SiteAdapter

logger = logging.getLogger("ai-relay.minimax")

_POLL_JS = """
(() => {
  const items = [...document.querySelectorAll('[data-testid="message-item"]')]
    .filter(i => i.querySelector('[data-testid="assistant-active-flow"]'));
  const last = items[items.length - 1];
  const text = last
    ? last.querySelector('[data-testid="assistant-active-flow"]').innerText.trim() : null;
  // MiniMax 实测在回答完成后仍可能长期保留 stop-button；当前消息出现
  // “共执行 N 秒”的 turn-process-disclosure 才是可靠完成标志。
  const completed = !!last?.querySelector('[data-testid="turn-process-disclosure"]')
    || !!document.querySelector('[data-testid="turn-process-disclosure"]');
  const generating = !!document.querySelector('[data-testid="stop-button"]') && !completed;
  return JSON.stringify({ generating, answer: text, error: null });
})()
"""

_TOGGLES_OFF = [
    ("agent-team-toggle", "Agent 团队"),
    ("model-thinking-trigger-toggle", "思考"),
]


class MinimaxAdapter(SiteAdapter):
    home_url = "https://agent.minimaxi.com/"

    async def is_logged_in(self) -> bool:
        code = "(() => !!document.querySelector('[data-testid=\"message-textarea\"]'))()"
        return bool(await self.client.evaluate(code, self.session))

    async def ensure_model(self, model: str) -> None:
        for testid, name in _TOGGLES_OFF:
            await self._set_toggle_off(testid, name)
        if model:
            await self._select_model(model)

    async def _set_toggle_off(self, testid: str, name: str) -> None:
        click_js = """
        (() => {
          const b = document.querySelector('button[data-testid="%s"]');
          if (!b) return 'NO_BTN';
          if (b.getAttribute('aria-checked') === 'true') b.click();
          return 'OK';
        })()
        """ % testid
        result = await self.client.evaluate(click_js, self.session)
        if result != "OK":
            # 当前 MiniMax 页面可能不展示这些可选开关；它们不影响基础问答。
            # 仅告警并继续，避免整个 target 因 UI 版本差异被错误标记 degraded。
            logger.warning("minimax 未找到可选开关: %s（继续使用当前页面默认模式）", name)
            return
        await asyncio.sleep(0.3)
        check_js = """
        (() => {
          const b = document.querySelector('button[data-testid="%s"]');
          return b ? b.getAttribute('aria-checked') : null;
        })()
        """ % testid
        if await self.client.evaluate(check_js, self.session) == "true":
            raise WebbridgeError(f"minimax 开关未能关闭: {name}")

    async def _select_model(self, model: str) -> None:
        current_js = """
        (() => { const el = document.querySelector('button[data-testid="model-selector-trigger"]');
          return el ? el.innerText.trim() : ''; })()
        """
        current = await self.client.evaluate(current_js, self.session)
        if model in (current or ""):
            return
        await self.client.click('button[data-testid="model-selector-trigger"]', self.session)
        await asyncio.sleep(0.5)
        click_js = """
        (() => {
          const btns = [...document.querySelectorAll('button')];
          const b = btns.find(x => (x.innerText || '').trim().includes(%s));
          if (!b) return 'NOT_FOUND';
          b.click();
          return 'OK';
        })()
        """ % json.dumps(model)
        if await self.client.evaluate(click_js, self.session) != "OK":
            raise WebbridgeError(f"minimax 未找到模型: {model}")
        await asyncio.sleep(0.5)
        current = await self.client.evaluate(current_js, self.session)
        if model not in (current or ""):
            raise WebbridgeError(f"minimax 模型切换未生效: 当前 {current!r}，期望包含 {model!r}")

    async def new_chat(self) -> None:
        await self.client.navigate(self.home_url, self.session, new_tab=False)

    async def send_prompt(self, prompt: str) -> None:
        for _ in range(3):
            await self.client.fill('[data-testid="message-textarea"]', prompt, self.session)
            await asyncio.sleep(0.5)
            filled = await self.client.evaluate(
                "(() => { const el = document.querySelector('[data-testid=\"message-textarea\"]');"
                " return el ? (el.tagName === 'TEXTAREA' ? el.value : el.innerText).trim().length > 0 : false; })()",
                self.session)
            if filled:
                break
            await asyncio.sleep(1)
        else:
            raise WebbridgeError("minimax 输入框填入失败")

        # click 后禁止重放；无法确认时交给 worker 标记 outcome_unknown。
        try:
            await self.client.click('[data-testid="send-button"]', self.session)
            prompt_js = json.dumps(prompt.strip())
            for _ in range(10):
                await asyncio.sleep(0.5)
                sent = await self.client.evaluate(
                    "(() => { const expected = %s;"
                    " const users = [...document.querySelectorAll('[data-testid=\"message-item\"]')];"
                    " const hasPrompt = users.some(x => (x.innerText || '').trim() === expected);"
                    " return hasPrompt || !!document.querySelector('[data-testid=\"stop-button\"]'); })()" % prompt_js,
                    self.session)
                if sent:
                    return
        except Exception as e:
            raise SendOutcomeUnknownError(f"minimax 发送后无法确认状态: {e}") from e
        raise SendOutcomeUnknownError("minimax 点击发送后未能确认消息是否送达")

    async def poll_once(self) -> dict:
        data = await self._eval_json(_POLL_JS)
        return {"generating": bool(data.get("generating")),
                "answer": data.get("answer"), "error": data.get("error")}
