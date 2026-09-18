"""SQLite 持久化 + store 写穿 + 分页查询测试。

临时库夹具在 conftest.py（autouse）。
"""
from __future__ import annotations

import time

import pytest

from app import db as task_db
from app.schemas import TaskPhase
from app.store import TaskStore


def _make(store: TaskStore, prompt="p", site=None, **kw):
    t = store.create(prompt, site)
    for k, v in kw.items():
        setattr(t, k, v)
    return t


def test_store_writes_through_to_sqlite():
    store = TaskStore()
    t = store.create("你好", "kimi")
    store.mark_running(t.task_id, worker_id="w2", actual_site="kimi")
    store.mark_done(t.task_id, "答案")

    row = task_db.get_task(t.task_id)
    assert row is not None
    assert row["status"] == "done"
    assert row["result"] == "答案"
    assert row["actual_site"] == "kimi"
    assert row["worker_id"] == "w2"
    assert row["started_at"] is not None and row["finished_at"] is not None


def test_get_falls_back_to_sqlite():
    store = TaskStore()
    t = store.create("旧任务", None)
    store.mark_failed(t.task_id, "超时")
    store._tasks.clear()  # 模拟服务重启后内存清空

    task = store.get(t.task_id)
    assert task is not None
    assert task.status.value == "failed"
    assert task.error == "超时"
    assert task.prompt == "旧任务"


def test_discard_removes_from_sqlite():
    store = TaskStore()
    t = store.create("x", None)
    store.discard(t.task_id)
    assert task_db.get_task(t.task_id) is None


def test_pagination_order_and_pages():
    store = TaskStore()
    for i in range(45):
        t = store.create(f"任务{i}", None)
        t.created_at = time.time() + i  # 保证倒序明确
        store._persist(t)

    assert task_db.count_tasks() == 45
    page1 = task_db.list_tasks(page=1, page_size=20)
    page3 = task_db.list_tasks(page=3, page_size=20)
    assert len(page1) == 20
    assert len(page3) == 5
    # 倒序：page1 最新（任务44），page3 最旧（任务0）
    assert page1[0]["prompt"] == "任务44"
    assert page3[-1]["prompt"] == "任务0"


def test_task_filters_by_keyword_target_backend_and_time():
    from app.schemas import BackendType

    store = TaskStore()
    matched = store.create("特殊关键词", None, target_id="api-a", backend_type=BackendType.openai_compatible)
    store.mark_running(matched.task_id, actual_target_id="api-a", actual_backend_type=BackendType.openai_compatible)
    store.mark_done(matched.task_id, "匹配结果")
    other = store.create("普通任务", "kimi")
    other.created_at = matched.created_at + 10
    store._persist(other)
    store.mark_failed(other.task_id, "普通错误")

    rows = task_db.list_tasks(q="特殊关键词")
    assert [row["task_id"] for row in rows] == [matched.task_id]
    assert task_db.count_tasks(q="匹配结果") == 1
    assert task_db.count_tasks(target_id="api-a") == 1
    assert task_db.count_tasks(backend_type="openai_compatible") == 1
    assert task_db.count_tasks(created_after=matched.created_at - 1, created_before=matched.created_at + 1) == 1


def test_status_filter():
    store = TaskStore()
    t1 = store.create("a", None)
    store.create("b", None)
    store.mark_failed(t1.task_id, "err")

    assert task_db.count_tasks("failed") == 1
    assert task_db.count_tasks("queued") == 1
    failed = task_db.list_tasks(status="failed")
    assert len(failed) == 1 and failed[0]["error"] == "err"


def test_fail_unfinished_marks_zombies():
    """遗留的 queued/running 任务启动时被标记为中断失败。"""
    store = TaskStore()
    t1 = store.create("排队中", None)
    t2 = store.create("执行中", None)
    t3 = store.create("已完成", None)
    store.mark_running(t2.task_id, worker_id="w1", actual_site="kimi")
    store.mark_done(t3.task_id, "ok")

    n = task_db.fail_unfinished()

    assert n == 2
    r1 = task_db.get_task(t1.task_id)
    r2 = task_db.get_task(t2.task_id)
    assert r1["status"] == "failed" and "中断" in r1["error"]
    assert r2["status"] == "failed" and r2["finished_at"] is not None
    assert task_db.get_task(t3.task_id)["status"] == "done"  # 已完成不受影响


def test_retries_and_tried_sites_persisted():
    store = TaskStore()
    t = store.create("跨站", "kimi")
    store.mark_running(t.task_id, worker_id="w1", actual_site="kimi")
    store.mark_retry(t.task_id, "卡死", failed_site="kimi")

    row = task_db.get_task(t.task_id)
    assert row["retries"] == 1
    assert row["tried_sites"] == "kimi"
    assert row["status"] == "queued"
    assert row["actual_site"] is None

    store._tasks.clear()
    task = store.get(t.task_id)
    assert task.retries == 1
    assert task.tried_sites == ["kimi"]


def test_create_is_fail_closed_when_sqlite_write_fails(monkeypatch):
    store = TaskStore()

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(task_db, "create_or_get_idempotent", fail)
    with pytest.raises(OSError, match="disk full"):
        store.create("不能丢失", None)
    assert store._tasks == {}


def test_idempotency_reuses_same_request_and_rejects_conflict():
    store = TaskStore()
    task, reused = store.create_or_get_idempotent("幂等任务", "kimi", idempotency_key="key-1")
    same, same_reused = store.create_or_get_idempotent("幂等任务", "kimi", idempotency_key="key-1")
    assert reused is False
    assert same_reused is True
    assert same.task_id == task.task_id

    with pytest.raises(task_db.IdempotencyConflict):
        store.create_or_get_idempotent("不同内容", "kimi", idempotency_key="key-1")


def test_events_and_attempts_are_persisted():
    store = TaskStore()
    task = store.create("审计", "kimi")
    store.mark_running(task.task_id, worker_id="w1", actual_site="kimi")
    attempt_id = store.start_attempt(task.task_id, "w1", "kimi", "K2.6")
    store.set_phase(task.task_id, TaskPhase.sending)
    store.update_attempt(task.task_id, send_state="sent_confirmed")
    store.mark_done(task.task_id, "完成")

    events = task_db.list_events(task.task_id)
    attempts = task_db.list_attempts(task.task_id)
    assert any(event["event_type"] == "task_created" for event in events)
    assert any(event["event_type"] == "task_completed" for event in events)
    assert attempts[0]["attempt_id"] == attempt_id
    assert attempts[0]["send_state"] == "sent_confirmed"


def test_recover_unfinished_preserves_queued_and_interrupts_running():
    store = TaskStore()
    queued = store.create("可恢复", None)
    running = store.create("不可盲重试", None)
    store.mark_running(running.task_id, worker_id="w1", actual_site="kimi")

    result = task_db.recover_unfinished()
    assert result == {"recoverable": 1, "interrupted": 1}
    assert task_db.get_task(queued.task_id)["status"] == "queued"
    assert task_db.get_task(running.task_id)["status"] == "interrupted"


def test_target_and_external_attempt_metadata_persist():
    from app.schemas import BackendType

    store = TaskStore()
    task = store.create("API", None, target_id="api-one",
                        backend_type=BackendType.openai_compatible, model="m1")
    store.mark_running(task.task_id, worker_id="api-api-one-1", actual_target_id="api-one",
                       actual_backend_type=BackendType.openai_compatible)
    store.start_attempt(task.task_id, "api-api-one-1", None, "m1", target_id="api-one",
                        backend_type=BackendType.openai_compatible)
    store.mark_done(task.task_id, "ok", external_request_id="chatcmpl-1",
                    provider_status_code=200, usage={"total_tokens": 4})

    store._tasks.clear()
    saved = store.get(task.task_id)
    assert saved.target_id == "api-one"
    assert saved.actual_target_id == "api-one"
    assert saved.backend_type == BackendType.openai_compatible
    assert saved.upstream_request_id == "chatcmpl-1"
    assert saved.usage == {"total_tokens": 4}
    attempt = saved.to_info().attempts[0]
    assert attempt.target_id == "api-one"
    assert attempt.provider_status_code == 200
    assert attempt.usage == {"total_tokens": 4}


def test_prune_terminal_tasks_redacts_then_deletes():
    store = TaskStore()
    task = store.create("敏感提示词", None)
    store.mark_done(task.task_id, "敏感回答")
    record = task_db.get_task(task.task_id)
    cutoff = record["finished_at"] + 1

    result = task_db.prune_terminal_tasks(cutoff, cutoff - 1)
    assert result["redacted"] == 1
    redacted = task_db.get_task(task.task_id)
    assert redacted["prompt"] == "[已按保留策略清除]"
    assert redacted["result"] is None

    result = task_db.prune_terminal_tasks(cutoff, cutoff + 1)
    assert result["deleted"] == 1
    assert task_db.get_task(task.task_id) is None


def test_migrate_adds_missing_columns(tmp_path):
    """旧表缺少 retries/tried_sites 时，init 会 ALTER 补齐。"""
    import sqlite3

    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
    CREATE TABLE tasks (
      task_id TEXT PRIMARY KEY,
      site TEXT,
      actual_site TEXT,
      worker_id TEXT,
      status TEXT NOT NULL,
      prompt TEXT NOT NULL,
      result TEXT,
      error TEXT,
      created_at REAL NOT NULL,
      started_at REAL,
      finished_at REAL
    );
    """)
    conn.execute(
        "INSERT INTO tasks (task_id, status, prompt, created_at) VALUES (?,?,?,?)",
        ("abc", "done", "旧", 1.0))
    conn.commit()
    conn.close()

    task_db.close()
    task_db.init(db_path)

    cols = {r[1] for r in task_db._required_conn().execute(
        "PRAGMA table_info(tasks)").fetchall()}
    assert "retries" in cols
    assert "tried_sites" in cols

    row = task_db.get_task("abc")
    assert row["prompt"] == "旧"
    assert row["retries"] == 0 or row["retries"] is None

    # 新字段可写
    task_db.upsert_task({
        "task_id": "abc", "site": None, "actual_site": None, "worker_id": None,
        "status": "queued", "prompt": "旧", "result": None, "error": None,
        "created_at": 1.0, "started_at": None, "finished_at": None,
        "retries": 2, "tried_sites": "kimi,deepseek",
    })
    row = task_db.get_task("abc")
    assert row["retries"] == 2
    assert row["tried_sites"] == "kimi,deepseek"
