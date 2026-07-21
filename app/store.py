"""任务存储：内存 dict（活跃任务）+ SQLite 写穿（全量持久化）+ 最近任务环形列表 + TTL 清理。

内存负责活跃任务的快速读写；SQLite（app.db）持久化全部任务记录，
重启后历史任务仍可分页查询、按 id 回退读取。
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Optional

from . import db as task_db
from .schemas import TaskInfo, TaskStatus, TaskSummary

logger = logging.getLogger("ai-relay.store")

# 摘要截断长度
PREVIEW_CHARS = 120
# 清理协程运行间隔
SWEEP_INTERVAL = 60


def _preview(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text[:PREVIEW_CHARS]


@dataclass
class TaskRecord:
    task_id: str
    prompt: str
    site: Optional[str] = None          # 请求指定的站点偏好，None 表示任一
    actual_site: Optional[str] = None   # 实际执行的站点（worker 站点）
    worker_id: Optional[str] = None     # 执行的 worker id
    status: TaskStatus = TaskStatus.queued
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    def elapsed_seconds(self) -> Optional[float]:
        if self.started_at is None:
            return None
        end = self.finished_at if self.finished_at is not None else time.time()
        return round(end - self.started_at, 1)

    def to_info(self) -> TaskInfo:
        return TaskInfo(
            task_id=self.task_id,
            status=self.status,
            site=self.site,
            actual_site=self.actual_site,
            worker_id=self.worker_id,
            prompt=self.prompt,
            result=self.result,
            error=self.error,
            created_at=self.created_at,
            started_at=self.started_at,
            finished_at=self.finished_at,
            elapsed_seconds=self.elapsed_seconds(),
        )

    def to_summary(self) -> TaskSummary:
        return TaskSummary(
            task_id=self.task_id,
            status=self.status,
            site=self.site,
            actual_site=self.actual_site,
            prompt_preview=_preview(self.prompt) or "",
            result_preview=_preview(self.result),
            error=self.error,
            created_at=self.created_at,
            elapsed_seconds=self.elapsed_seconds(),
        )

    def to_db_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "site": self.site,
            "actual_site": self.actual_site,
            "worker_id": self.worker_id,
            "status": self.status.value,
            "prompt": self.prompt,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }

    @classmethod
    def from_db_dict(cls, d: dict) -> "TaskRecord":
        return cls(
            task_id=d["task_id"],
            prompt=d["prompt"],
            site=d.get("site"),
            actual_site=d.get("actual_site"),
            worker_id=d.get("worker_id"),
            status=TaskStatus(d["status"]),
            result=d.get("result"),
            error=d.get("error"),
            created_at=d["created_at"],
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
        )


class TaskStore:
    def __init__(self, history_size: int = 100, retention_seconds: int = 3600):
        self._tasks: dict[str, TaskRecord] = {}
        self._history: deque[TaskRecord] = deque(maxlen=history_size)
        self._retention = retention_seconds
        self._janitor: Optional[asyncio.Task] = None

    # ---- 任务生命周期（每个状态变更都写穿 SQLite）----

    def create(self, prompt: str, site: Optional[str]) -> TaskRecord:
        task = TaskRecord(task_id=uuid.uuid4().hex, prompt=prompt, site=site)
        self._tasks[task.task_id] = task
        self._persist(task)
        return task

    def get(self, task_id: str) -> Optional[TaskRecord]:
        task = self._tasks.get(task_id)
        if task is None:
            # 内存未命中（已被 TTL 清理或服务重启过）：回退 SQLite
            row = task_db.get_task(task_id)
            if row:
                task = TaskRecord.from_db_dict(row)
        return task

    def discard(self, task_id: str) -> None:
        """丢弃未成功入队的任务。"""
        self._tasks.pop(task_id, None)
        task_db.delete_task(task_id)

    def mark_running(self, task_id: str, worker_id: Optional[str] = None,
                     actual_site: Optional[str] = None) -> None:
        task = self._tasks[task_id]
        task.status = TaskStatus.running
        task.started_at = time.time()
        task.worker_id = worker_id
        task.actual_site = actual_site
        self._persist(task)

    def mark_done(self, task_id: str, result: str) -> None:
        task = self._tasks[task_id]
        task.status = TaskStatus.done
        task.result = result
        task.finished_at = time.time()
        self._history.append(task)
        self._persist(task)

    def mark_failed(self, task_id: str, error: str) -> None:
        task = self._tasks[task_id]
        task.status = TaskStatus.failed
        task.error = error
        task.finished_at = time.time()
        self._history.append(task)
        self._persist(task)

    def history(self) -> list[TaskRecord]:
        """最近任务，新的在前。"""
        return list(reversed(self._history))

    def _persist(self, task: TaskRecord) -> None:
        try:
            task_db.upsert_task(task.to_db_dict())
        except Exception as e:
            # 持久化失败不阻断任务执行
            logger.warning("任务 %s 持久化失败: %s", task.task_id[:8], e)

    # ---- TTL 清理（仅清内存，SQLite 全量保留）----

    def start(self) -> None:
        self._janitor = asyncio.create_task(self._janitor_loop())

    async def stop(self) -> None:
        if self._janitor:
            self._janitor.cancel()
            with suppress(asyncio.CancelledError):
                await self._janitor
            self._janitor = None

    async def _janitor_loop(self) -> None:
        while True:
            await asyncio.sleep(SWEEP_INTERVAL)
            self._sweep()

    def _sweep(self) -> None:
        """清理内存中已完成且超过保留时长的任务（SQLite 副本保留）。"""
        cutoff = time.time() - self._retention
        expired = [
            tid for tid, t in self._tasks.items()
            if t.finished_at is not None and t.finished_at < cutoff
        ]
        for tid in expired:
            del self._tasks[tid]
        if expired:
            logger.info("清理过期任务 %d 个（SQLite 仍保留）", len(expired))
