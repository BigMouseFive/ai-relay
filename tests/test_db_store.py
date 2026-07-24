"""SQLite 持久化 + store 写穿 + 分页查询测试。

临时库夹具在 conftest.py（autouse）。
"""
from __future__ import annotations

import time

from app import db as task_db
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
