"""kimi.com 站点适配器（选择器来自实测调研）。"""
from __future__ import annotations

import asyncio
import json

from ..webbridge import WebbridgeError
from .base import SiteAdapter

# 生成中标志：发送按钮 class 含 stop；答案取最后一条 assistant 段的 markdown
# "已停止内容生成"是 kimi 生成被中止时的占位文本，要识别为站点错误而非正常结果
_POLL_JS = """
(() => {
  const generating = !!document.querySelector('.send-button-container.stop');
  const segs = [...document.querySelectorAll('.segment.segment-assistant')];
  const last = segs[segs.length - 1];
  let text = null;
  if (last) {
    const mds = [...last.querySelectorAll('.markdown-container')];
    const t = mds.length ? mds[mds.length - 1] : last.querySelector('.segment-content-box');
    text = t ? t.innerText.trim() : null;
  }
  // 只认确切的占位文本；空文本交给调用方按"还在生成"继续等待（避免流式开始前误判）
  const stopped = !generating && text === '已停止内容生成';
  return JSON.stringify({ generating, answer: stopped ? null : text,
                          error: stopped ? '生成被中止（已停止内容生成）' : null });
})()
"""

_CURRENT_MODEL_JS = """
(() => { const el = document.querySelector('.current-model'); return el ? el.innerText : ''; })()
"""


class KimiAdapter(SiteAdapter):
    home_url = "https://www.kimi.com/"

    async def is_logged_in(self) -> bool:
        code = "(() => !!document.querySelector('.chat-input-editor'))()"
        return bool(await self.client.evaluate(code, self.session))

    async def ensure_model(self, model: str) -> None:
        if not model:
            return
        current = await self.client.evaluate(_CURRENT_MODEL_JS, self.session)
        if model in (current or ""):
            return
        # 打开模型弹层，按 innerText 子串匹配点击
        await self.client.click(".current-model", self.session)
        await asyncio.sleep(0.5)
        click_js = """
        (() => {
          const items = [...document.querySelectorAll('.model-item')];
          const it = items.find(x => (x.innerText || '').includes(%s));
          if (!it) return 'NOT_FOUND';
          it.click();
          return 'OK';
        })()
        """ % json.dumps(model)
        r = await self.client.evaluate(click_js, self.session)
        if r != "OK":
            raise WebbridgeError(f"kimi 未找到模型: {model}")
        await asyncio.sleep(0.5)
        current = await self.client.evaluate(_CURRENT_MODEL_JS, self.session)
        if model not in (current or ""):
            raise WebbridgeError(f"kimi 模型切换未生效: 当前 {current!r}，期望包含 {model!r}")

    async def new_chat(self) -> None:
        await self.client.navigate(self.home_url, self.session, new_tab=False)

    async def send_prompt(self, prompt: str) -> None:
        # 并发/页面未就绪时 fill 可能静默失败，必须先校验内容落入编辑器再点发送
        for _ in range(3):
            await self.client.fill(".chat-input-editor", prompt, self.session)
            await asyncio.sleep(0.5)
            filled = await self.client.evaluate(
                "(() => { const el = document.querySelector('.chat-input-editor');"
                " return el ? el.innerText.trim().length > 0 : false; })()",
                self.session)
            if filled:
                break
            await asyncio.sleep(1)
        else:
            raise WebbridgeError("kimi 输入框填入失败（Lexical 未就绪）")
        await self.client.click(".send-button-container", self.session)
        # 校验消息确实发出（出现用户消息段、进入 /chat/ 会话页，或按钮变为停止态）。
        # 注意：发送成功后该按钮立即变为"停止生成"按钮（带 .stop class），
        # 重试点击前必须先排除这种状态，否则会误点"停止"把生成中止（"已停止内容生成"）
        for _ in range(10):
            await asyncio.sleep(0.5)
            sent = await self.client.evaluate(
                "(() => !!document.querySelector('.segment-user')"
                " || location.pathname.startsWith('/chat')"
                " || !!document.querySelector('.send-button-container.stop'))()",
                self.session)
            if sent:
                return
            await self.client.click(".send-button-container", self.session)
        raise WebbridgeError("kimi 消息发送失败（点击发送无反应）")

    async def poll_once(self) -> dict:
        data = await self._eval_json(_POLL_JS)
        return {"generating": bool(data.get("generating")),
                "answer": data.get("answer"),
                "error": data.get("error")}
