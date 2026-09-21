"""TaskStore 单元测试。"""
import time

import pytest

from app.response_contract import ResponseValidationError
from app.schemas import TaskStatus
from app.store import TaskStore


def test_任务生命周期_完成():
    store = TaskStore()
    task = store.create("你好", "kimi")
    assert task.status == TaskStatus.queued
    assert store.get(task.task_id) is task

    store.mark_running(task.task_id)
    assert task.status == TaskStatus.running
    assert task.started_at is not None

    store.mark_done(task.task_id, "回答内容")
    assert task.status == TaskStatus.done
    assert task.result == "回答内容"
    assert task.finished_at is not None

    info = store.get(task.task_id).to_info()
    assert info.result == "回答内容"
    assert info.elapsed_seconds is not None


def test_json_contract_validates_and_normalizes_done_result():
    store = TaskStore()
    response_format = {
        "type": "json_schema", "name": "test",
        "schema": {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}},
    }
    task = store.create("返回 JSON", None, response_format=response_format, max_retries=2)
    store.mark_running(task.task_id)
    store.mark_done(task.task_id, '{ "ok": true }')
    assert task.status == TaskStatus.done
    assert task.result == '{"ok":true}'
    assert "系统输出契约" in task.execution_prompt()


def test_json_contract_rejects_invalid_result_before_marking_done():
    store = TaskStore()
    response_format = {
        "type": "json_schema", "name": "test",
        "schema": {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}},
    }
    task = store.create("返回 JSON", None, response_format=response_format, max_retries=1)
    store.mark_running(task.task_id)
    with pytest.raises(ResponseValidationError, match="不是合法 JSON") as caught:
        store.mark_done(task.task_id, "```json {} ```")
    assert caught.value.code == "response_invalid_json"
    assert task.status == TaskStatus.running
    assert task.phase.value == "validating_result"


def test_任务生命周期_失败():
    store = TaskStore()
    task = store.create("你好", None)
    store.mark_running(task.task_id)
    store.mark_failed(task.task_id, "任务超时")
    assert task.status == TaskStatus.failed
    assert task.error == "任务超时"
    assert task.finished_at is not None


def test_完成后进入历史_新的在前():
    store = TaskStore()
    t1 = store.create("一", None)
    t2 = store.create("二", None)
    store.mark_done(t1.task_id, "r1")
    store.mark_failed(t2.task_id, "e2")
    history = store.history()
    assert [t.task_id for t in history] == [t2.task_id, t1.task_id]


def test_历史环形列表_maxlen():
    store = TaskStore(history_size=2)
    ids = []
    for i in range(3):
        t = store.create(f"p{i}", None)
        store.mark_done(t.task_id, f"r{i}")
        ids.append(t.task_id)
    history = store.history()
    assert len(history) == 2
    assert [t.task_id for t in history] == [ids[2], ids[1]]


def test_ttl_清理():
    store = TaskStore(retention_seconds=60)
    old = store.create("旧任务", None)
    new = store.create("新任务", None)
    running = store.create("进行中", None)
    store.mark_done(old.task_id, "r")
    store.mark_done(new.task_id, "r")
    store.mark_running(running.task_id)
    # 手动把 old 的完成时间改到保留期之前
    store.get(old.task_id).finished_at = time.time() - 120

    store._sweep()
    # 内存已清理，但 SQLite 副本保留（get 会回退读取）
    assert old.task_id not in store._tasks
    assert store.get(old.task_id) is not None
    assert new.task_id in store._tasks
    assert running.task_id in store._tasks


def test_discard():
    store = TaskStore()
    task = store.create("x", None)
    store.discard(task.task_id)
    assert store.get(task.task_id) is None


def test_摘要截断():
    store = TaskStore()
    task = store.create("p" * 200, "kimi")
    store.mark_done(task.task_id, "r" * 200)
    summary = task.to_summary()
    assert len(summary.prompt_preview) == 120
    assert len(summary.result_preview) == 120


def test_mark_retry_重置状态并记录tried_sites():
    store = TaskStore()
    task = store.create("你好", "kimi")
    store.mark_running(task.task_id, worker_id="w1", actual_site="kimi")
    store.mark_retry(task.task_id, "超时", failed_site="kimi")

    assert task.status == TaskStatus.queued
    assert task.retries == 1
    assert task.tried_sites == ["kimi"]
    assert task.worker_id is None
    assert task.actual_site is None
    assert task.started_at is None
    assert task.finished_at is None
    assert task.error == "超时"
    # 重试不进 history
    assert store.history() == []

    info = task.to_info()
    assert info.retries == 1
    summary = task.to_summary()
    assert summary.retries == 1


def test_mark_retry_tried_sites去重():
    store = TaskStore()
    task = store.create("x", None)
    store.mark_retry(task.task_id, "e1", failed_site="deepseek")
    store.mark_retry(task.task_id, "e2", failed_site="deepseek")
    assert task.tried_sites == ["deepseek"]
    assert task.retries == 2
