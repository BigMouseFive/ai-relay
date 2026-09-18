"""任务记录的 SQLite 持久化层。

SQLite 是任务和执行审计的事实来源。所有写入都在进程内锁保护下完成，
并使用 WAL/busy_timeout 降低本地单实例服务发生短暂锁竞争时的失败概率。
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Optional

logger = logging.getLogger("ai-relay.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  task_id                TEXT PRIMARY KEY,
  site                   TEXT,
  actual_site            TEXT,
  worker_id              TEXT,
  status                 TEXT NOT NULL,
  prompt                 TEXT NOT NULL,
  result                 TEXT,
  error                  TEXT,
  created_at             REAL NOT NULL,
  started_at             REAL,
  finished_at            REAL,
  retries                INTEGER DEFAULT 0,
  tried_sites            TEXT,
  idempotency_key        TEXT,
  request_hash           TEXT,
  phase                  TEXT NOT NULL DEFAULT 'waiting_for_worker',
  error_code             TEXT,
  next_attempt_at        REAL,
  queue_deadline_at      REAL,
  cancel_requested       INTEGER NOT NULL DEFAULT 0,
  current_attempt_id     TEXT,
  requested_model        TEXT,
  allow_fallback_sites   INTEGER,
  target_id              TEXT,
  actual_target_id       TEXT,
  backend_type           TEXT,
  actual_backend_type    TEXT,
  upstream_request_id    TEXT,
  usage_json             TEXT,
  routing_mode           TEXT,
  routing_decision_json  TEXT,
  updated_at             REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at DESC);

CREATE TABLE IF NOT EXISTS task_attempts (
  attempt_id             TEXT PRIMARY KEY,
  task_id                TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  attempt_number         INTEGER NOT NULL,
  worker_id              TEXT,
  site                   TEXT,
  target_id              TEXT,
  backend_type           TEXT,
  model                  TEXT,
  external_request_id    TEXT,
  process_exit_code      INTEGER,
  provider_status_code   INTEGER,
  usage_json             TEXT,
  phase                  TEXT,
  send_state             TEXT,
  error_code             TEXT,
  error                  TEXT,
  started_at             REAL NOT NULL,
  finished_at            REAL,
  UNIQUE(task_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS idx_task_attempts_task_id ON task_attempts(task_id, attempt_number);

CREATE TABLE IF NOT EXISTS task_events (
  event_id               INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id                TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
  event_type             TEXT NOT NULL,
  payload_json           TEXT NOT NULL DEFAULT '{}',
  created_at             REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id, event_id);
"""

_MIGRATIONS = (
    ("retries", "ALTER TABLE tasks ADD COLUMN retries INTEGER DEFAULT 0"),
    ("tried_sites", "ALTER TABLE tasks ADD COLUMN tried_sites TEXT"),
    ("idempotency_key", "ALTER TABLE tasks ADD COLUMN idempotency_key TEXT"),
    ("request_hash", "ALTER TABLE tasks ADD COLUMN request_hash TEXT"),
    ("phase", "ALTER TABLE tasks ADD COLUMN phase TEXT NOT NULL DEFAULT 'waiting_for_worker'"),
    ("error_code", "ALTER TABLE tasks ADD COLUMN error_code TEXT"),
    ("next_attempt_at", "ALTER TABLE tasks ADD COLUMN next_attempt_at REAL"),
    ("queue_deadline_at", "ALTER TABLE tasks ADD COLUMN queue_deadline_at REAL"),
    ("cancel_requested", "ALTER TABLE tasks ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"),
    ("current_attempt_id", "ALTER TABLE tasks ADD COLUMN current_attempt_id TEXT"),
    ("requested_model", "ALTER TABLE tasks ADD COLUMN requested_model TEXT"),
    ("allow_fallback_sites", "ALTER TABLE tasks ADD COLUMN allow_fallback_sites INTEGER"),
    ("target_id", "ALTER TABLE tasks ADD COLUMN target_id TEXT"),
    ("actual_target_id", "ALTER TABLE tasks ADD COLUMN actual_target_id TEXT"),
    ("backend_type", "ALTER TABLE tasks ADD COLUMN backend_type TEXT"),
    ("actual_backend_type", "ALTER TABLE tasks ADD COLUMN actual_backend_type TEXT"),
    ("upstream_request_id", "ALTER TABLE tasks ADD COLUMN upstream_request_id TEXT"),
    ("usage_json", "ALTER TABLE tasks ADD COLUMN usage_json TEXT"),
    ("routing_mode", "ALTER TABLE tasks ADD COLUMN routing_mode TEXT"),
    ("routing_decision_json", "ALTER TABLE tasks ADD COLUMN routing_decision_json TEXT"),
    ("updated_at", "ALTER TABLE tasks ADD COLUMN updated_at REAL NOT NULL DEFAULT 0"),
)

_TASK_COLUMNS = (
    "task_id", "site", "actual_site", "worker_id", "status", "prompt", "result", "error",
    "created_at", "started_at", "finished_at", "retries", "tried_sites", "idempotency_key",
    "request_hash", "phase", "error_code", "next_attempt_at", "queue_deadline_at",
    "cancel_requested", "current_attempt_id", "requested_model", "allow_fallback_sites",
    "target_id", "actual_target_id", "backend_type", "actual_backend_type",
    "upstream_request_id", "usage_json", "routing_mode", "routing_decision_json", "updated_at",
)

_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


class IdempotencyConflict(ValueError):
    """同一幂等键被用于不同请求体。"""


def init(db_path: str) -> None:
    """打开（必要时创建）数据库，并执行兼容迁移。"""
    global _conn
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with _lock:
        if _conn is not None:
            _conn.close()
        _conn = sqlite3.connect(db_path, check_same_thread=False)
        _conn.execute("PRAGMA foreign_keys = ON")
        _conn.execute("PRAGMA busy_timeout = 5000")
        _conn.execute("PRAGMA journal_mode = WAL")
        _conn.execute("PRAGMA synchronous = NORMAL")
        _conn.executescript(_SCHEMA)
        _migrate(_conn)
    logger.info("任务存储 SQLite 已就绪: %s", db_path)


def _migrate(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    for name, sql in _MIGRATIONS:
        if name not in cols:
            conn.execute(sql)
            logger.info("SQLite 迁移：已添加列 %s", name)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_next_attempt ON tasks(status, next_attempt_at)")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency_key "
        "ON tasks(idempotency_key) WHERE idempotency_key IS NOT NULL")
    # 先确保旧数据库也已创建 attempts 表，随后才可做 ALTER TABLE。
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS task_attempts (
      attempt_id TEXT PRIMARY KEY,
      task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
      attempt_number INTEGER NOT NULL,
      worker_id TEXT,
      site TEXT,
      target_id TEXT,
      backend_type TEXT,
      model TEXT,
      external_request_id TEXT,
      process_exit_code INTEGER,
      provider_status_code INTEGER,
      usage_json TEXT,
      phase TEXT,
      send_state TEXT,
      error_code TEXT,
      error TEXT,
      started_at REAL NOT NULL,
      finished_at REAL,
      UNIQUE(task_id, attempt_number)
    );
    CREATE INDEX IF NOT EXISTS idx_task_attempts_task_id ON task_attempts(task_id, attempt_number);
    CREATE TABLE IF NOT EXISTS task_events (
      event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
      event_type TEXT NOT NULL,
      payload_json TEXT NOT NULL DEFAULT '{}',
      created_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id, event_id);
    """)
    attempt_cols = {row[1] for row in conn.execute("PRAGMA table_info(task_attempts)").fetchall()}
    for name, sql in (
        ("target_id", "ALTER TABLE task_attempts ADD COLUMN target_id TEXT"),
        ("backend_type", "ALTER TABLE task_attempts ADD COLUMN backend_type TEXT"),
        ("external_request_id", "ALTER TABLE task_attempts ADD COLUMN external_request_id TEXT"),
        ("process_exit_code", "ALTER TABLE task_attempts ADD COLUMN process_exit_code INTEGER"),
        ("provider_status_code", "ALTER TABLE task_attempts ADD COLUMN provider_status_code INTEGER"),
        ("usage_json", "ALTER TABLE task_attempts ADD COLUMN usage_json TEXT"),
    ):
        if name not in attempt_cols:
            conn.execute(sql)
            logger.info("SQLite 迁移：task_attempts 已添加列 %s", name)
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


def _normalise_record(rec: dict[str, Any]) -> dict[str, Any]:
    value = dict(rec)
    now = time.time()
    value.setdefault("idempotency_key", None)
    value.setdefault("request_hash", None)
    value.setdefault("phase", "waiting_for_worker")
    value.setdefault("error_code", None)
    value.setdefault("next_attempt_at", None)
    value.setdefault("queue_deadline_at", None)
    value.setdefault("cancel_requested", 0)
    value.setdefault("current_attempt_id", None)
    value.setdefault("requested_model", None)
    value.setdefault("allow_fallback_sites", None)
    value.setdefault("target_id", None)
    value.setdefault("actual_target_id", None)
    value.setdefault("backend_type", None)
    value.setdefault("actual_backend_type", None)
    value.setdefault("upstream_request_id", None)
    value.setdefault("usage_json", None)
    value.setdefault("routing_mode", None)
    value.setdefault("routing_decision_json", None)
    value.setdefault("updated_at", now)
    value.setdefault("retries", 0)
    value.setdefault("tried_sites", None)
    return {column: value.get(column) for column in _TASK_COLUMNS}


def _upsert(conn: sqlite3.Connection, rec: dict[str, Any]) -> None:
    normalised = _normalise_record(rec)
    columns = ", ".join(_TASK_COLUMNS)
    placeholders = ", ".join(f":{column}" for column in _TASK_COLUMNS)
    updates = ", ".join(
        f"{column}=excluded.{column}" for column in _TASK_COLUMNS if column != "task_id")
    conn.execute(
        f"INSERT INTO tasks ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT(task_id) DO UPDATE SET {updates}",
        normalised,
    )


def upsert_task(rec: dict[str, Any], event_type: Optional[str] = None,
                event_payload: Optional[dict[str, Any]] = None) -> None:
    """写穿任务记录；可与事件在同一事务内提交。"""
    with _lock:
        conn = _required_conn()
        try:
            conn.execute("BEGIN")
            _upsert(conn, rec)
            if event_type:
                _insert_event(conn, rec["task_id"], event_type, event_payload)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def create_task(rec: dict[str, Any], event_type: str = "task_created") -> None:
    """原子创建任务与初始事件。任务 ID 重复即抛出数据库完整性异常。"""
    with _lock:
        conn = _required_conn()
        value = _normalise_record(rec)
        columns = ", ".join(_TASK_COLUMNS)
        placeholders = ", ".join(f":{column}" for column in _TASK_COLUMNS)
        try:
            conn.execute("BEGIN")
            conn.execute(f"INSERT INTO tasks ({columns}) VALUES ({placeholders})", value)
            _insert_event(conn, value["task_id"], event_type, {
                "status": value["status"], "phase": value["phase"],
            })
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def create_or_get_idempotent(rec: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """按幂等键原子创建/复用任务，返回 (record, reused)。"""
    value = _normalise_record(rec)
    key = value.get("idempotency_key")
    if not key:
        create_task(value)
        row = get_task(value["task_id"])
        assert row is not None
        return row, False

    with _lock:
        conn = _required_conn()
        try:
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT " + ", ".join(_TASK_COLUMNS) + " FROM tasks WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
            if row:
                existing = _row_to_dict(row)
                if existing.get("request_hash") != value.get("request_hash"):
                    raise IdempotencyConflict("同一 Idempotency-Key 对应了不同请求")
                conn.commit()
                return existing, True
            columns = ", ".join(_TASK_COLUMNS)
            placeholders = ", ".join(f":{column}" for column in _TASK_COLUMNS)
            conn.execute(f"INSERT INTO tasks ({columns}) VALUES ({placeholders})", value)
            _insert_event(conn, value["task_id"], "task_created", {
                "status": value["status"], "phase": value["phase"], "idempotent": True,
            })
            conn.commit()
            return value, False
        except Exception:
            conn.rollback()
            raise


def get_task(task_id: str) -> Optional[dict[str, Any]]:
    with _lock:
        cur = _required_conn().execute(
            "SELECT " + ", ".join(_TASK_COLUMNS) + " FROM tasks WHERE task_id = ?",
            (task_id,),
        )
        row = cur.fetchone()
    return _row_to_dict(row) if row else None


def get_task_by_idempotency_key(key: str) -> Optional[dict[str, Any]]:
    with _lock:
        row = _required_conn().execute(
            "SELECT " + ", ".join(_TASK_COLUMNS) + " FROM tasks WHERE idempotency_key = ?", (key,)
        ).fetchone()
    return _row_to_dict(row) if row else None


def delete_task(task_id: str) -> None:
    with _lock:
        conn = _required_conn()
        conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        conn.commit()


def recover_unfinished(reason: str = "服务重启，执行中断") -> dict[str, int]:
    """恢复安全任务，标记不可安全重放的运行中任务。"""
    now = time.time()
    with _lock:
        conn = _required_conn()
        conn.execute("BEGIN")
        # queued/retry_wait 从未进入浏览器发送阶段，可由调度器在启动后恢复。
        recoverable = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE status IN ('queued', 'retry_wait')"
        ).fetchone()[0]
        interrupted_ids = [row[0] for row in conn.execute(
            "SELECT task_id FROM tasks WHERE status='running'").fetchall()]
        cur = conn.execute(
            "UPDATE tasks SET status='interrupted', phase='interrupted', error=?, "
            "error_code='process_interrupted', finished_at=?, updated_at=? "
            "WHERE status='running'",
            (reason, now, now),
        )
        if interrupted_ids:
            placeholders = ",".join("?" for _ in interrupted_ids)
            conn.execute(
                f"UPDATE task_attempts SET error_code='process_interrupted', error=?, finished_at=? "
                f"WHERE task_id IN ({placeholders}) AND finished_at IS NULL",
                (reason, now, *interrupted_ids),
            )
            for task_id in interrupted_ids:
                _insert_event(conn, task_id, "task_interrupted", {"reason": reason})
        conn.commit()
        return {"recoverable": recoverable, "interrupted": cur.rowcount}


def fail_unfinished(reason: str = "服务重启，任务中断") -> int:
    """旧接口兼容：将 queued/running 统一标为 failed。

    新启动流程应调用 ``recover_unfinished``，以便 queued 任务安全恢复。
    """
    now = time.time()
    with _lock:
        conn = _required_conn()
        cur = conn.execute(
            "UPDATE tasks SET status='failed', phase='failed', error=?, "
            "error_code='process_interrupted', finished_at=?, updated_at=? "
            "WHERE status IN ('queued', 'running')",
            (reason, now, now),
        )
        conn.commit()
        return cur.rowcount


def list_recoverable_tasks(now: Optional[float] = None) -> list[dict[str, Any]]:
    now = time.time() if now is None else now
    with _lock:
        rows = _required_conn().execute(
            "SELECT " + ", ".join(_TASK_COLUMNS) + " FROM tasks "
            "WHERE status IN ('queued', 'retry_wait') "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY created_at ASC",
            (now,),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def target_attempt_metrics(since: float) -> dict[str, dict[str, float]]:
    """按 target 聚合近期已完成 attempt，不读取 prompt/result。"""
    with _lock:
        rows = _required_conn().execute(
            "SELECT a.target_id, "
            "COUNT(*) AS attempts, "
            "SUM(CASE WHEN t.status = 'done' AND t.actual_target_id = a.target_id THEN 1 ELSE 0 END) AS successes, "
            "SUM(CASE WHEN a.error_code IS NOT NULL AND a.error_code != 'send_outcome_unknown' THEN 1 ELSE 0 END) AS failures, "
            "SUM(CASE WHEN a.error_code = 'send_outcome_unknown' OR t.status = 'outcome_unknown' THEN 1 ELSE 0 END) AS unknowns, "
            "AVG(CASE WHEN a.finished_at IS NOT NULL THEN MAX(0, a.finished_at - a.started_at) END) AS avg_latency_seconds "
            "FROM task_attempts a JOIN tasks t ON t.task_id = a.task_id "
            "WHERE a.target_id IS NOT NULL AND a.finished_at IS NOT NULL AND a.finished_at >= ? "
            "GROUP BY a.target_id",
            (since,),
        ).fetchall()
    return {
        row[0]: {
            "attempts": float(row[1] or 0), "successes": float(row[2] or 0),
            "failures": float(row[3] or 0), "unknowns": float(row[4] or 0),
            "avg_latency_seconds": float(row[5] or 0),
        }
        for row in rows
    }


def prune_terminal_tasks(terminal_cutoff: float, metadata_cutoff: float) -> dict[str, int]:
    """按保留策略脱敏旧终态内容，并在元数据到期后彻底删除。

    不触碰 queued/running/retry_wait，避免清理与调度竞争。
    """
    redacted_prompt = "[已按保留策略清除]"
    with _lock:
        conn = _required_conn()
        try:
            conn.execute("BEGIN")
            redacted = conn.execute(
                "UPDATE tasks SET prompt=?, result=NULL, updated_at=? "
                "WHERE status IN ('done','failed','cancelled','expired','interrupted','outcome_unknown') "
                "AND finished_at IS NOT NULL AND finished_at < ? AND prompt != ?",
                (redacted_prompt, time.time(), terminal_cutoff, redacted_prompt),
            ).rowcount
            deleted = conn.execute(
                "DELETE FROM tasks WHERE status IN ('done','failed','cancelled','expired','interrupted','outcome_unknown') "
                "AND finished_at IS NOT NULL AND finished_at < ?",
                (metadata_cutoff,),
            ).rowcount
            conn.commit()
            return {"redacted": redacted, "deleted": deleted}
        except Exception:
            conn.rollback()
            raise


def _task_filters(q: Optional[str] = None, status: Optional[str] = None,
                  target_id: Optional[str] = None, backend_type: Optional[str] = None,
                  created_after: Optional[float] = None,
                  created_before: Optional[float] = None) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    args: list[Any] = []
    if q:
        pattern = f"%{q}%"
        clauses.append("(task_id LIKE ? OR prompt LIKE ? OR COALESCE(result, '') LIKE ? OR COALESCE(error, '') LIKE ?)")
        args.extend([pattern, pattern, pattern, pattern])
    if status:
        clauses.append("status = ?")
        args.append(status)
    if target_id:
        clauses.append("(target_id = ? OR actual_target_id = ?)")
        args.extend([target_id, target_id])
    if backend_type:
        clauses.append("(backend_type = ? OR actual_backend_type = ?)")
        args.extend([backend_type, backend_type])
    if created_after is not None:
        clauses.append("created_at >= ?")
        args.append(created_after)
    if created_before is not None:
        clauses.append("created_at <= ?")
        args.append(created_before)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", args


def count_tasks(status: Optional[str] = None, *, q: Optional[str] = None,
                target_id: Optional[str] = None, backend_type: Optional[str] = None,
                created_after: Optional[float] = None,
                created_before: Optional[float] = None) -> int:
    filters, args = _task_filters(q, status, target_id, backend_type, created_after, created_before)
    with _lock:
        return _required_conn().execute("SELECT COUNT(*) FROM tasks" + filters, args).fetchone()[0]


def list_tasks(page: int = 1, page_size: int = 20,
               status: Optional[str] = None, *, q: Optional[str] = None,
               target_id: Optional[str] = None, backend_type: Optional[str] = None,
               created_after: Optional[float] = None,
               created_before: Optional[float] = None) -> list[dict[str, Any]]:
    filters, args = _task_filters(q, status, target_id, backend_type, created_after, created_before)
    sql = "SELECT " + ", ".join(_TASK_COLUMNS) + " FROM tasks" + filters
    sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    args += [page_size, (page - 1) * page_size]
    with _lock:
        rows = _required_conn().execute(sql, args).fetchall()
    return [_row_to_dict(row) for row in rows]


def _insert_event(conn: sqlite3.Connection, task_id: str, event_type: str,
                  payload: Optional[dict[str, Any]] = None) -> int:
    cur = conn.execute(
        "INSERT INTO task_events(task_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?)",
        (task_id, event_type, json.dumps(payload or {}, ensure_ascii=False, sort_keys=True), time.time()),
    )
    return int(cur.lastrowid or 0)


def append_event(task_id: str, event_type: str, payload: Optional[dict[str, Any]] = None) -> int:
    with _lock:
        conn = _required_conn()
        event_id = _insert_event(conn, task_id, event_type, payload)
        conn.commit()
        return event_id


def list_events(task_id: str, after_event_id: int = 0) -> list[dict[str, Any]]:
    with _lock:
        rows = _required_conn().execute(
            "SELECT event_id, task_id, event_type, payload_json, created_at FROM task_events "
            "WHERE task_id = ? AND event_id > ? ORDER BY event_id ASC",
            (task_id, after_event_id),
        ).fetchall()
    return [
        {"event_id": row[0], "task_id": row[1], "event_type": row[2],
         "payload": json.loads(row[3]), "created_at": row[4]}
        for row in rows
    ]


def create_attempt(rec: dict[str, Any]) -> None:
    fields = (
        "attempt_id", "task_id", "attempt_number", "worker_id", "site", "target_id",
        "backend_type", "model", "external_request_id", "process_exit_code",
        "provider_status_code", "usage_json", "phase", "send_state", "error_code", "error",
        "started_at", "finished_at",
    )
    value = {field: rec.get(field) for field in fields}
    with _lock:
        conn = _required_conn()
        conn.execute(
            "INSERT INTO task_attempts(" + ", ".join(fields) + ") VALUES (" + ", ".join("?" for _ in fields) + ")",
            tuple(value[field] for field in fields),
        )
        conn.commit()


def update_attempt(attempt_id: str, **fields: Any) -> None:
    allowed = {
        "worker_id", "site", "target_id", "backend_type", "model", "external_request_id",
        "process_exit_code", "provider_status_code", "usage_json", "phase", "send_state",
        "error_code", "error", "finished_at",
    }
    values = {key: value for key, value in fields.items() if key in allowed}
    if not values:
        return
    sql = ", ".join(f"{key} = ?" for key in values)
    with _lock:
        conn = _required_conn()
        conn.execute(f"UPDATE task_attempts SET {sql} WHERE attempt_id = ?", (*values.values(), attempt_id))
        conn.commit()


def list_attempts(task_id: str) -> list[dict[str, Any]]:
    with _lock:
        rows = _required_conn().execute(
            "SELECT attempt_id, task_id, attempt_number, worker_id, site, target_id, backend_type, "
            "model, external_request_id, process_exit_code, provider_status_code, usage_json, phase, "
            "send_state, error_code, error, started_at, finished_at FROM task_attempts "
            "WHERE task_id = ? ORDER BY attempt_number ASC", (task_id,)
        ).fetchall()
    keys = (
        "attempt_id", "task_id", "attempt_number", "worker_id", "site", "target_id", "backend_type",
        "model", "external_request_id", "process_exit_code", "provider_status_code", "usage_json",
        "phase", "send_state", "error_code", "error", "started_at", "finished_at",
    )
    attempts: list[dict[str, Any]] = [dict(zip(keys, row)) for row in rows]
    for attempt in attempts:
        attempt["usage"] = json.loads(attempt.pop("usage_json") or "null")
    return attempts


def _row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return dict(zip(_TASK_COLUMNS, row))
