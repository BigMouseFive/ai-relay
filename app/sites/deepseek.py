"""chat.deepseek.com 站点适配器（选择器来自实测调研）。

注意：deepseek 的 textarea 必须用 evaluate 注入文本，webbridge fill 不可用。
"""
from __future__ import annotations

import asyncio
import json
import logging

from ..webbridge import WebbridgeError
from .base import SendOutcomeUnknownError, SiteAdapter

logger = logging.getLogger("ai-relay.deepseek")

# 生成中判定：发送按钮 svg 变停止图标 / "正在思考" / 回答后的操作按钮行未出现
# 站点侧失败（如"网络异常，请检查你的网络状况"、"服务器繁忙"）也要识别，让任务快速失败
_POLL_JS = """
(() => {
  const btn = document.querySelector('.ds-button--primary.ds-button--circle');
  const d = btn?.querySelector('svg path')?.getAttribute('d') || '';
  const isStop = d.startsWith('M2 4.88');
  const short = [...document.querySelectorAll('div')].map(x => (x.textContent||'').trim());
  const thinking = short.some(t => t.startsWith('正在思考') && t.length < 15);
  const errText = short.find(t => t.length < 30 &&
    (t.startsWith('网络异常') || t.startsWith('服务器繁忙')));
  const ans = [...document.querySelectorAll('.ds-assistant-message-main-content')];
  const last = ans[ans.length - 1] || null;
  const msg = last?.closest('.ds-message');
  const row = msg?.nextElementSibling;
  const hasActions = !!row && row.querySelectorAll('[role=button]').length >= 3;
  const generating = last ? (isStop || thinking || !hasActions) : (isStop || thinking);
  return JSON.stringify({ generating, answer: last ? last.innerText.trim() : null,
                          error: errText || null });
})()
"""

# 通过原生 setter 写入 textarea 并触发 input 事件（PROMPT 处为 json.dumps 后的字符串）
_FILL_JS = """
(() => { const ta = document.querySelector('textarea'); ta.focus();
  const s = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
  s.call(ta, %s); ta.dispatchEvent(new Event('input', {bubbles:true})); })()
"""


class DeepseekAdapter(SiteAdapter):
    home_url = "https://chat.deepseek.com/"

    async def is_logged_in(self) -> bool:
        # 页面可能短暂跳 /sign_in 再跳回，先等一会再判断
        await asyncio.sleep(5)
        code = """
        (() => JSON.stringify({
          url: location.href,
          hasTa: !!document.querySelector('textarea'),
        }))()
        """
        data = await self._eval_json(code)
        return "/sign_in" not in data.get("url", "") and bool(data.get("hasTa"))

    async def ensure_model(self, model: str) -> None:
        # 智能搜索对批量问答只会拖慢（联网检索耗时长），始终关闭（第 2 个开关）
        await self._set_toggle(1, "智能搜索", False)
        if model:
            await self._switch_on(model)
        else:
            # 默认模式：确保第 1 个开关（深度思考）处于关闭
            await self._set_toggle(0, "深度思考", False)

    async def _set_toggle(self, index: int, name: str, desired: bool) -> None:
        """把第 index 个受控开关设置为期望状态并回读校验。"""
        desired_js = "true" if desired else "false"
        click_js = """
        (() => {
          const b = document.querySelectorAll('.ds-toggle-button')[%d];
          if (!b) return 'NO_BTN';
          if ((b.getAttribute('aria-pressed') === 'true') !== %s) b.click();
          return 'OK';
        })()
        """ % (index, desired_js)
        r = await self.client.evaluate(click_js, self.session)
        if r != "OK":
            raise WebbridgeError(f"deepseek 未找到开关: {name}")
        await asyncio.sleep(0.5)
        check_js = """
        (() => {
          const b = document.querySelectorAll('.ds-toggle-button')[%d];
          return b ? b.getAttribute('aria-pressed') : null;
        })()
        """ % index
        pressed = await self.client.evaluate(check_js, self.session)
        if (pressed == "true") != desired:
            raise WebbridgeError(f"deepseek 开关未生效: {name} -> {desired}")

    async def _switch_on(self, model: str) -> None:
        model_js = json.dumps(model)
        click_js = """
        (() => {
          const btns = [...document.querySelectorAll('.ds-toggle-button')];
          const b = btns.find(x => (x.innerText || '').includes(%s));
          if (!b) return 'NOT_FOUND';
          if (b.getAttribute('aria-pressed') !== 'true') b.click();
          return 'OK';
        })()
        """ % model_js
        r = await self.client.evaluate(click_js, self.session)
        if r != "OK":
            raise WebbridgeError(f"deepseek 未找到模式开关: {model}")
        await asyncio.sleep(0.5)
        check_js = """
        (() => {
          const btns = [...document.querySelectorAll('.ds-toggle-button')];
          const b = btns.find(x => (x.innerText || '').includes(%s));
          return b ? b.getAttribute('aria-pressed') : null;
        })()
        """ % model_js
        pressed = await self.client.evaluate(check_js, self.session)
        if pressed != "true":
            raise WebbridgeError(f"deepseek 模式开关未生效: {model}")

    async def new_chat(self) -> None:
        await self.client.navigate(self.home_url, self.session, new_tab=False)

    async def send_prompt(self, prompt: str) -> None:
        # 页面未就绪时注入可能落空，先校验 textarea 有内容再点发送
        for _ in range(3):
            await self.client.evaluate(_FILL_JS % json.dumps(prompt), self.session)
            await asyncio.sleep(0.3)
            filled = await self.client.evaluate(
                "(() => { const ta = document.querySelector('textarea');"
                " return ta ? ta.value.length > 0 : false; })()",
                self.session)
            if filled:
                break
            await asyncio.sleep(1)
        else:
            raise WebbridgeError("deepseek 输入框注入失败")
        # click 后禁止盲目重试，避免 WebBridge 响应丢失时重复向真实站点发送。
        try:
            await self.client.click(
                "div[role=button].ds-button--primary.ds-button--circle", self.session)
            prompt_js = json.dumps(prompt.strip())
            for _ in range(10):
                await asyncio.sleep(0.5)
                sent = await self.client.evaluate(
                    "(() => { const expected = %s; const ta = document.querySelector('textarea');"
                    " const messages = [...document.querySelectorAll('.ds-message')];"
                    " const hasPrompt = messages.some(x => (x.innerText || '').trim() === expected);"
                    " const d = document.querySelector('.ds-button--primary.ds-button--circle svg path')?.getAttribute('d') || '';"
                    " return hasPrompt || !ta || ta.value === '' || d.startsWith('M2 4.88'); })()" % prompt_js,
                    self.session)
                if sent:
                    return
        except Exception as e:
            raise SendOutcomeUnknownError(f"deepseek 发送后无法确认状态: {e}") from e
        raise SendOutcomeUnknownError("deepseek 点击发送后未能确认消息是否送达")

    async def poll_once(self) -> dict:
        data = await self._eval_json(_POLL_JS)
        return {"generating": bool(data.get("generating")),
                "answer": data.get("answer"),
                "error": data.get("error")}
