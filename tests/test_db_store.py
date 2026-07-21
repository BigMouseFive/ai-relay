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
