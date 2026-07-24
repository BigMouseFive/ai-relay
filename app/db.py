"""任务记录的 SQLite 持久化层。

设计：内存 store 负责活跃任务的快速读写，本模块作为全量持久化副本——
任务每次状态变更都写穿到这里；服务重启后历史任务仍可分页查询，
GET /api/tasks/{id} 对内存未命中的任务也会回退到这里读取。
使用标准库 sqlite3，无新增依赖；写入极小（仅状态变更时），单线程事件循环
加一个锁兜底即可。
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from typing import Optional

logger = logging.getLogger("ai-relay.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  task_id      TEXT PRIMARY KEY,
  site         TEXT,           -- 请求的站点偏好（NULL = 任意）
  actual_site  TEXT,           -- 实际执行的站点（worker 站点）
  worker_id    TEXT,           -- 执行的 worker id
  status       TEXT NOT NULL,  -- queued / running / done / failed
  prompt       TEXT NOT NULL,
  result       TEXT,
  error        TEXT,
  created_at   REAL NOT NULL,
  started_at   REAL,
  finished_at  REAL,
  retries      INTEGER DEFAULT 0,  -- 已重新入队次数
  tried_sites  TEXT                -- 已尝试过的站点，逗号分隔
);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at DESC);
"""

# 存量库迁移：CREATE TABLE IF NOT EXISTS 不会给旧表加列
_MIGRATIONS = (
    ("retries", "ALTER TABLE tasks ADD COLUMN retries INTEGER DEFAULT 0"),
    ("tried_sites", "ALTER TABLE tasks ADD COLUMN tried_sites TEXT"),
)

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def init(db_path: str) -> None:
    """打开（必要时创建）数据库并初始化表结构。"""
    global _conn
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with _lock:
        if _conn is not None:  # 重复 init（如测试切换临时库）先关闭旧连接
            _conn.close()
        _conn = sqlite3.connect(db_path, check_same_thread=False)
        _conn.executescript(_SCHEMA)
        _migrate(_conn)
    logger.info("任务存储 SQLite 已就绪: %s", db_path)


def _migrate(conn: sqlite3.Connection) -> None:
    """给旧表补齐缺失列。"""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    for name, sql in _MIGRATIONS:
        if name not in cols:
            conn.execute(sql)
            logger.info("SQLite 迁移：已添加列 %s", name)
    conn.commit()


def close() -> None:
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None


def _required_conn() -> sqlite3.Connection:
    if _conn is None:
        raise RuntimeError("SQLite 未初始化（先调用 init）")
    return _conn


def upsert_task(rec: dict) -> None:
    """按 task_id 写穿整条记录（insert or replace）。字段与 TaskRecord 对齐。"""
    sql = """
    INSERT OR REPLACE INTO tasks
      (task_id, site, actual_site, worker_id, status, prompt, result, error,
       created_at, started_at, finished_at, retries, tried_sites)
    VALUES
      (:task_id, :site, :actual_site, :worker_id, :status, :prompt, :result,
       :error, :created_at, :started_at, :finished_at, :retries, :tried_sites)
    """
    with _lock:
        _required_conn().execute(sql, rec)
        _required_conn().commit()


def get_task(task_id: str) -> Optional[dict]:
    with _lock:
        cur = _required_conn().execute(
            "SELECT task_id, site, actual_site, worker_id, status, prompt, result,"
            " error, created_at, started_at, finished_at, retries, tried_sites"
            " FROM tasks WHERE task_id = ?",
            (task_id,))
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


def delete_task(task_id: str) -> None:
    with _lock:
        _required_conn().execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        _required_conn().commit()


def fail_unfinished(reason: str = "服务重启，任务中断") -> int:
    """启动时把上次运行遗留的 queued/running 任务标记为失败，返回处理条数。

    服务重启后这些任务已无人执行，不能永远留在 queued/running 状态。
    """
    import time
    with _lock:
        cur = _required_conn().execute(
            "UPDATE tasks SET status = 'failed', error = ?, finished_at = ? "
            "WHERE status IN ('queued', 'running')",
            (reason, time.time()))
        _required_conn().commit()
        return cur.rowcount


def count_tasks(status: Optional[str] = None) -> int:
    sql = "SELECT COUNT(*) FROM tasks"
    args: tuple = ()
    if status:
        sql += " WHERE status = ?"
        args = (status,)
    with _lock:
        return _required_conn().execute(sql, args).fetchone()[0]


def list_tasks(page: int = 1, page_size: int = 20,
               status: Optional[str] = None) -> list[dict]:
    """按创建时间倒序分页查询。"""
    sql = ("SELECT task_id, site, actual_site, worker_id, status, prompt, result,"
           " error, created_at, started_at, finished_at, retries, tried_sites FROM tasks")
    args: list = []
    if status:
        sql += " WHERE status = ?"
        args.append(status)
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    args += [page_size, (page - 1) * page_size]
    with _lock:
        rows = _required_conn().execute(sql, args).fetchall()
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row: tuple) -> dict:
    keys = ("task_id", "site", "actual_site", "worker_id", "status", "prompt",
            "result", "error", "created_at", "started_at", "finished_at",
            "retries", "tried_sites")
    return dict(zip(keys, row))
