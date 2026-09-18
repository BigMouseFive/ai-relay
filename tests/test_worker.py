"""Worker 生成停滞、硬超时和发送结果未知语义测试（adapter 全部 fake）。"""
from __future__ import annotations

import pytest

from app.store import TaskStore
from app.worker import Worker
import app.worker as worker_mod


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


class AlwaysGeneratingAdapter:
    """永远 generating=True，用于验证硬超时不信任该指示。"""

    home_url = "http://fake.local/"
    session = "fake-session"

    def __init__(self):
        self.client = FakeClient()
        self.new_chat_count = 0

    async def new_chat(self):
        self.new_chat_count += 1

    async def send_prompt(self, prompt):
        pass

    async def ensure_model(self, model):
        pass

    async def is_logged_in(self):
        return True

    async def poll_once(self):
        return {"generating": True, "answer": None, "error": None}


def _worker(adapter, stall=3, hard_timeout=300, on_retry=None):
    store = TaskStore(history_size=10, retention_seconds=60)
    return Worker("w1", "deepseek", "", adapter, store,
                  timeout_seconds=60, stall_seconds=stall,
                  hard_timeout_seconds=hard_timeout, on_retry=on_retry)


async def test_stall_after_confirmed_send_becomes_outcome_unknown():
    """发送已确认后的停滞不能盲目重放同一 prompt。"""
    adapter = StallThenAnswerAdapter(stall_rounds=1)
    w = _worker(adapter, stall=3)
    task = w.store.create("1+1=?", None)
    await w.run_task(task)

    rec = w.store.get(task.task_id)
    assert rec.status.value == "outcome_unknown"
    assert "停滞" in (rec.error or "")
    assert adapter.client.nav_count >= 1  # 为隔离下一任务已回收并重建 tab


async def test_persistent_stall_becomes_outcome_unknown():
    adapter = StallThenAnswerAdapter(stall_rounds=99)
    w = _worker(adapter, stall=3)
    task = w.store.create("1+1=?", None)
    await w.run_task(task)

    rec = w.store.get(task.task_id)
    assert rec.status.value == "outcome_unknown"
    assert "停滞" in (rec.error or "")


async def test_hard_timeout_after_confirmed_send_becomes_outcome_unknown(monkeypatch):
    """长期 generating=True 时，硬超时不会自动重试已发送任务。"""
    monkeypatch.setattr(worker_mod, "PAGE_LOAD_WAIT", 0)
    monkeypatch.setattr(worker_mod, "POLL_INTERVAL", 0.05)

    store = TaskStore(history_size=10, retention_seconds=60)

    adapter = AlwaysGeneratingAdapter()
    w = Worker("w1", "deepseek", "", adapter, store,
               timeout_seconds=60, stall_seconds=60,
               hard_timeout_seconds=0.2)
    task = store.create("卡住?", None)
    await w.run_task(task)

    rec = store.get(task.task_id)
    assert rec.status.value == "outcome_unknown"
    assert "硬超时" in (rec.error or "")
    assert w.fail_count == 1
