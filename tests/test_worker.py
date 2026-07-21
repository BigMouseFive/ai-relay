"""Worker 生成停滞检测与自动重试测试（adapter 全部 fake）。"""
from __future__ import annotations

import pytest

from app.store import TaskStore
from app.worker import Worker


class FakeClient:
    def __init__(self):
        self.nav_count = 0

    async def navigate(self, *a, **kw):
        self.nav_count += 1

    async def close_session(self, *a, **kw):
        pass


class StallThenAnswerAdapter:
    """前 stall_rounds 轮任务一直无进展（僵尸态），之后正常回答。"""

    home_url = "http://fake.local/"
    session = "fake-session"

    def __init__(self, stall_rounds=1):
        self.client = FakeClient()
        self.stall_rounds = stall_rounds
        self.new_chat_count = 0
        self._round = 0
        self._polls = 0

    async def new_chat(self):
        self.new_chat_count += 1
        self._round = self.new_chat_count - 1
        self._polls = 0

    async def send_prompt(self, prompt):
        pass

    async def ensure_model(self, model):
        pass

    async def is_logged_in(self):
        return True

    async def poll_once(self):
        self._polls += 1
        if self._round < self.stall_rounds:
            return {"generating": False, "answer": None, "error": None}  # 僵尸态
        # 正常：先等两拍再给答案（走稳定判定）
        if self._polls >= 3:
            return {"generating": False, "answer": "42", "error": None}
        return {"generating": True, "answer": None, "error": None}


def _worker(adapter, stall=3):
    store = TaskStore(history_size=10, retention_seconds=60)
    return Worker("w1", "deepseek", "", adapter, store,
                  timeout_seconds=60, stall_seconds=stall)


async def test_stall_triggers_rebuild_and_retry():
    """第一轮僵尸态 → 停滞检测抛错并重建重试 → 第二轮正常完成。"""
    adapter = StallThenAnswerAdapter(stall_rounds=1)
    w = _worker(adapter, stall=3)
    task = w.store.create("1+1=?", None)
    await w.run_task(task)

    rec = w.store.get(task.task_id)
    assert rec.status.value == "done"
    assert rec.result == "42"
    assert adapter.new_chat_count >= 2  # 发生了重试（重新开新对话）


async def test_persistent_stall_fails_task():
    """每一轮都僵尸态 → 重试耗尽后任务 failed，错误信息含"停滞"。"""
    adapter = StallThenAnswerAdapter(stall_rounds=99)
    w = _worker(adapter, stall=3)
    task = w.store.create("1+1=?", None)
    await w.run_task(task)

    rec = w.store.get(task.task_id)
    assert rec.status.value == "failed"
    assert "停滞" in (rec.error or "")
