"""Worker 生成停滞检测、硬超时与跨站点重试回调测试（adapter 全部 fake）。"""
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


async def test_hard_timeout_ignores_generating(monkeypatch):
    """长期 generating=True 时，硬超时仍触发，并走 on_retry。"""
    monkeypatch.setattr(worker_mod, "PAGE_LOAD_WAIT", 0)
    monkeypatch.setattr(worker_mod, "POLL_INTERVAL", 0.05)

    retried = []
    store = TaskStore(history_size=10, retention_seconds=60)

    def on_retry(task, reason):
        retried.append(reason)
        store.mark_failed(task.task_id, f"{reason}（硬超时测试）")
        return False

    adapter = AlwaysGeneratingAdapter()
    w = Worker("w1", "deepseek", "", adapter, store,
               timeout_seconds=60, stall_seconds=60,
               hard_timeout_seconds=0.2, on_retry=on_retry)
    task = store.create("卡住?", None)
    await w.run_task(task)

    assert retried
    assert "硬超时" in retried[0]
    rec = store.get(task.task_id)
    assert rec.status.value == "failed"
    assert w.fail_count == 1


async def test_on_retry_true_does_not_increment_fail_count(monkeypatch):
    """on_retry 返回 True（已重入队）时不增加 fail_count。"""
    monkeypatch.setattr(worker_mod, "PAGE_LOAD_WAIT", 0)
    monkeypatch.setattr(worker_mod, "POLL_INTERVAL", 0.05)

    def on_retry(task, reason):
        return True  # 假装已重新入队；不 mark_failed

    adapter = AlwaysGeneratingAdapter()
    w = _worker(adapter, stall=60, hard_timeout=0.2, on_retry=on_retry)
    task = w.store.create("重试", None)
    await w.run_task(task)

    assert w.fail_count == 0
