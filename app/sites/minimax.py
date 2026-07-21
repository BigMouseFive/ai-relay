"""agent.minimaxi.com 站点适配器（选择器来自实测调研，全部用稳定 data-testid）。

要求：Agent 团队和思考两个开关始终保持关闭。
"""
from __future__ import annotations

import asyncio
import json
import logging

from ..webbridge import WebbridgeError
from .base import SiteAdapter

logger = logging.getLogger("ai-relay.minimax")

# 生成中标志：发送按钮被替换为 stop-button（存在即生成中，消失即完成）
# 回答取最后一个含 assistant-active-flow 的 message-item（过程/工具调用 UI 不在其中，天然分离）
_POLL_JS = """
(() => {
  const generating = !!document.querySelector('[data-testid="stop-button"]');
  const items = [...document.querySelectorAll('[data-testid="message-item"]')]
    .filter(i => i.querySelector('[data-testid="assistant-active-flow"]'));
  const last = items[items.length - 1];
  const text = last
    ? last.querySelector('[data-testid="assistant-active-flow"]').innerText.trim() : null;
  return JSON.stringify({ generating, answer: text, error: null });
})()
"""

# 两个必须保持关闭的开关：Agent 团队、思考
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
        # Agent 团队 / 思考：始终确保关闭（开关状态跨会话持久化，先读再点，不盲切）
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
        r = await self.client.evaluate(click_js, self.session)
        if r != "OK":
            logger.warning("minimax 未找到开关: %s", name)
            return
        await asyncio.sleep(0.3)
        check_js = """
        (() => {
          const b = document.querySelector('button[data-testid="%s"]');
          return b ? b.getAttribute('aria-checked') : null;
        })()
        """ % testid
        checked = await self.client.evaluate(check_js, self.session)
        if checked == "true":
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
        r = await self.client.evaluate(click_js, self.session)
        if r != "OK":
            raise WebbridgeError(f"minimax 未找到模型: {model}")
        await asyncio.sleep(0.5)
        current = await self.client.evaluate(current_js, self.session)
        if model not in (current or ""):
            raise WebbridgeError(f"minimax 模型切换未生效: 当前 {current!r}，期望包含 {model!r}")

    async def new_chat(self) -> None:
        await self.client.navigate(self.home_url, self.session, new_tab=False)

    async def send_prompt(self, prompt: str) -> None:
        # 页面未就绪时 fill 可能静默失败，先校验内容落入编辑器再点发送
        for _ in range(3):
            await self.client.fill('[data-testid="message-textarea"]', prompt, self.session)
            await asyncio.sleep(0.5)
            filled = await self.client.evaluate(
                "(() => { const el = document.querySelector('[data-testid=\"message-textarea\"]');"
                " return el ? el.innerText.trim().length > 0 : false; })()",
                self.session)
            if filled:
                break
            await asyncio.sleep(1)
        else:
            raise WebbridgeError("minimax 输入框填入失败")
        await self.client.click('[data-testid="send-button"]', self.session)
        # 校验消息确实发出：stop-button 出现 / 进入 /mavis 会话页 / 出现用户消息气泡
        # （生成中发送按钮会被替换为 stop-button，此时绝不能再点）
        for _ in range(10):
            await asyncio.sleep(0.5)
            state = await self.client.evaluate(
                "(() => {"
                " if (document.querySelector('[data-testid=\"stop-button\"]')) return 'sent';"
                " if (location.pathname.startsWith('/mavis')) return 'sent';"
                " if (document.querySelector('[data-testid=\"message-item\"] [class*=\"user-message-bubble\"],"
                "     [data-testid=\"message-item\"].user-message-bubble')) return 'sent';"
                " if (document.querySelector('[data-testid=\"send-button\"]')) return 'pending';"
                " return 'unknown'; })()",
                self.session)
            if state in ("sent", "unknown"):
                return
            await self.client.click('[data-testid="send-button"]', self.session)
        raise WebbridgeError("minimax 消息发送失败（点击发送无反应）")

    async def poll_once(self) -> dict:
        data = await self._eval_json(_POLL_JS)
        return {"generating": bool(data.get("generating")),
                "answer": data.get("answer"),
                "error": data.get("error")}
