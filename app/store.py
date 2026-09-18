"""任务存储：内存活跃副本 + SQLite 事实来源 + 任务事件审计。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Optional

from . import db as task_db
from .schemas import BackendType, TaskAttemptInfo, TaskInfo, TaskPhase, TaskStatus, TaskSummary

PREVIEW_CHARS = 120
SWEEP_INTERVAL = 60
TERMINAL_STATUSES = {
    TaskStatus.done, TaskStatus.failed, TaskStatus.cancelled,
    TaskStatus.expired, TaskStatus.interrupted, TaskStatus.outcome_unknown,
}


def _preview(text: Optional[str]) -> Optional[str]:
    return text[:PREVIEW_CHARS] if text is not None else None


def _sites_to_str(sites: list[str]) -> Optional[str]:
    return ",".join(sites) if sites else None


def _sites_from_str(value: Optional[str]) -> list[str]:
    return [site for site in (value or "").split(",") if site]


def _backend_type(value: Optional[str]) -> Optional[BackendType]:
    if not value:
        return None
    try:
        return BackendType(value)
    except ValueError:
        return None


def request_hash(prompt: str, site: Optional[str], target_id: Optional[str],
                 backend_type: Optional[str], model: Optional[str],
                 allow_fallback_sites: Optional[bool], routing_mode: Optional[str]) -> str:
    payload = json.dumps({
        "prompt": prompt, "site": site, "target_id": target_id,
        "backend_type": backend_type, "model": model,
        "allow_fallback_sites": allow_fallback_sites, "routing_mode": routing_mode,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class TaskRecord:
    task_id: str
    prompt: str
    site: Optional[str] = None
    actual_site: Optional[str] = None
    worker_id: Optional[str] = None
    status: TaskStatus = TaskStatus.queued
    phase: TaskPhase = TaskPhase.waiting_for_worker
    result: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    retries: int = 0
    tried_sites: list[str] = field(default_factory=list)
    idempotency_key: Optional[str] = None
    request_hash: Optional[str] = None
    model: Optional[str] = None
    allow_fallback_sites: Optional[bool] = None
    next_attempt_at: Optional[float] = None
    queue_deadline_at: Optional[float] = None
    cancel_requested: bool = False
    current_attempt_id: Optional[str] = None
    requested_model: Optional[str] = None
    target_id: Optional[str] = None
    actual_target_id: Optional[str] = None
    backend_type: Optional[BackendType] = None
    actual_backend_type: Optional[BackendType] = None
    upstream_request_id: Optional[str] = None
    usage: Optional[dict[str, Any]] = None
    routing_mode: Optional[str] = None
    routing_decision: Optional[dict[str, Any]] = None
    updated_at: float = field(default_factory=time.time)

    def elapsed_seconds(self) -> Optional[float]:
        if self.started_at is None:
            return None
        end = self.finished_at if self.finished_at is not None else time.time()
        return round(end - self.started_at, 1)

    def to_info(self) -> TaskInfo:
        return TaskInfo(
            task_id=self.task_id, status=self.status, phase=self.phase, site=self.site,
            actual_site=self.actual_site, worker_id=self.worker_id, prompt=self.prompt,
            result=self.result, error=self.error, error_code=self.error_code,
            created_at=self.created_at, started_at=self.started_at, finished_at=self.finished_at,
            elapsed_seconds=self.elapsed_seconds(), retries=self.retries,
            next_attempt_at=self.next_attempt_at, queue_deadline_at=self.queue_deadline_at,
            cancel_requested=self.cancel_requested, current_attempt_id=self.current_attempt_id,
            model=self.model, allow_fallback_sites=self.allow_fallback_sites,
            target_id=self.target_id, actual_target_id=self.actual_target_id,
            backend_type=self.backend_type, actual_backend_type=self.actual_backend_type,
            upstream_request_id=self.upstream_request_id, usage=self.usage,
            routing_mode=self.routing_mode, routing_decision=self.routing_decision,
            tried_sites=list(self.tried_sites),
            attempts=[TaskAttemptInfo.model_validate(item) for item in task_db.list_attempts(self.task_id)],
        )

    def to_summary(self) -> TaskSummary:
        return TaskSummary(
            task_id=self.task_id, status=self.status, phase=self.phase, site=self.site,
            actual_site=self.actual_site, target_id=self.target_id,
            actual_target_id=self.actual_target_id, backend_type=self.backend_type,
            actual_backend_type=self.actual_backend_type,
            prompt_preview=_preview(self.prompt) or "",
            result_preview=_preview(self.result), error=self.error, error_code=self.error_code,
            created_at=self.created_at, elapsed_seconds=self.elapsed_seconds(), retries=self.retries,
            next_attempt_at=self.next_attempt_at, queue_deadline_at=self.queue_deadline_at,
            cancel_requested=self.cancel_requested, routing_mode=self.routing_mode,
        )

    def to_db_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id, "site": self.site, "actual_site": self.actual_site,
            "worker_id": self.worker_id, "status": self.status.value, "prompt": self.prompt,
            "result": self.result, "error": self.error, "created_at": self.created_at,
            "started_at": self.started_at, "finished_at": self.finished_at, "retries": self.retries,
            "tried_sites": _sites_to_str(self.tried_sites), "idempotency_key": self.idempotency_key,
            "request_hash": self.request_hash, "phase": self.phase.value,
            "error_code": self.error_code, "next_attempt_at": self.next_attempt_at,
            "queue_deadline_at": self.queue_deadline_at,
            "cancel_requested": int(self.cancel_requested),
            "current_attempt_id": self.current_attempt_id,
            "requested_model": self.model,
            "allow_fallback_sites": (None if self.allow_fallback_sites is None else int(self.allow_fallback_sites)),
            "target_id": self.target_id,
            "actual_target_id": self.actual_target_id,
            "backend_type": self.backend_type.value if self.backend_type else None,
            "actual_backend_type": self.actual_backend_type.value if self.actual_backend_type else None,
            "upstream_request_id": self.upstream_request_id,
            "usage_json": json.dumps(self.usage, ensure_ascii=False, sort_keys=True) if self.usage else None,
            "routing_mode": self.routing_mode,
            "routing_decision_json": json.dumps(self.routing_decision, ensure_ascii=False, sort_keys=True) if self.routing_decision else None,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_db_dict(cls, value: dict[str, Any]) -> "TaskRecord":
        phase_value = value.get("phase") or TaskPhase.waiting_for_worker.value
        try:
            phase = TaskPhase(phase_value)
        except ValueError:
            phase = TaskPhase.waiting_for_worker
        return cls(
            task_id=value["task_id"], prompt=value["prompt"], site=value.get("site"),
            actual_site=value.get("actual_site"), worker_id=value.get("worker_id"),
            status=TaskStatus(value["status"]), phase=phase, result=value.get("result"),
            error=value.get("error"), error_code=value.get("error_code"),
            created_at=value["created_at"], started_at=value.get("started_at"),
            finished_at=value.get("finished_at"), retries=int(value.get("retries") or 0),
            tried_sites=_sites_from_str(value.get("tried_sites")),
            idempotency_key=value.get("idempotency_key"), request_hash=value.get("request_hash"),
            next_attempt_at=value.get("next_attempt_at"),
            queue_deadline_at=value.get("queue_deadline_at"),
            cancel_requested=bool(value.get("cancel_requested")),
            current_attempt_id=value.get("current_attempt_id"),
            model=value.get("requested_model"),
            requested_model=value.get("requested_model"),
            allow_fallback_sites=(None if value.get("allow_fallback_sites") is None else bool(value.get("allow_fallback_sites"))),
            target_id=value.get("target_id"), actual_target_id=value.get("actual_target_id"),
            backend_type=_backend_type(value.get("backend_type")),
            actual_backend_type=_backend_type(value.get("actual_backend_type")),
            upstream_request_id=value.get("upstream_request_id"),
            usage=(json.loads(value["usage_json"]) if value.get("usage_json") else None),
            routing_mode=value.get("routing_mode"),
            routing_decision=(json.loads(value["routing_decision_json"]) if value.get("routing_decision_json") else None),
            updated_at=value.get("updated_at") or value["created_at"],
        )


class TaskStore:
    def __init__(self, history_size: int = 100, retention_seconds: int = 3600,
                 max_queue_wait_seconds: int = 3600):
        self._tasks: dict[str, TaskRecord] = {}
        self._history: deque[TaskRecord] = deque(maxlen=history_size)
        self._retention = retention_seconds
        self._max_queue_wait = max_queue_wait_seconds
        self._terminal_retention: Optional[int] = None
        self._metadata_retention: Optional[int] = None
        self._janitor: Optional[asyncio.Task] = None

    def create(self, prompt: str, site: Optional[str], *, target_id: Optional[str] = None,
               backend_type: Optional[BackendType] = None, model: Optional[str] = None,
               allow_fallback_sites: Optional[bool] = None, routing_mode: Optional[str] = None,
               routing_decision: Optional[dict[str, Any]] = None,
               idempotency_key: Optional[str] = None) -> TaskRecord:
        task, reused = self.create_or_get_idempotent(
            prompt, site, target_id=target_id, backend_type=backend_type, model=model,
            allow_fallback_sites=allow_fallback_sites, routing_mode=routing_mode,
            routing_decision=routing_decision, idempotency_key=idempotency_key,
        )
        if reused:
            return task
        return task

    def create_or_get_idempotent(
        self, prompt: str, site: Optional[str], *, target_id: Optional[str] = None,
        backend_type: Optional[BackendType] = None, model: Optional[str] = None,
        allow_fallback_sites: Optional[bool] = None, routing_mode: Optional[str] = None,
        routing_decision: Optional[dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> tuple[TaskRecord, bool]:
        now = time.time()
        task = TaskRecord(
            task_id=uuid.uuid4().hex, prompt=prompt, site=site, target_id=target_id,
            backend_type=backend_type, model=model,
            allow_fallback_sites=allow_fallback_sites, routing_mode=routing_mode,
            routing_decision=routing_decision, idempotency_key=idempotency_key,
            # adaptive 的幂等键绑定调用方逻辑请求，不绑定本次随机选择出的 target/model。
            request_hash=request_hash(
                prompt, site,
                None if routing_mode == "adaptive" else target_id,
                None if routing_mode == "adaptive" else (backend_type.value if backend_type else None),
                None if routing_mode == "adaptive" else model,
                allow_fallback_sites, routing_mode),
            queue_deadline_at=now + self._max_queue_wait, updated_at=now,
        )
        try:
            row, reused = task_db.create_or_get_idempotent(task.to_db_dict())
        except Exception:
            # Fail closed: 未成功落库前绝不在内存里留下可被调度的任务。
            raise
        record = TaskRecord.from_db_dict(row)
        self._tasks[record.task_id] = record
        return record, reused

    def get(self, task_id: str) -> Optional[TaskRecord]:
        task = self._tasks.get(task_id)
        if task is None:
            row = task_db.get_task(task_id)
            if row:
                task = TaskRecord.from_db_dict(row)
        return task

    def discard(self, task_id: str) -> None:
        task_db.delete_task(task_id)
        self._tasks.pop(task_id, None)

    def _task(self, task_id: str) -> TaskRecord:
        try:
            return self._tasks[task_id]
        except KeyError:
            task = self.get(task_id)
            if task is None:
                raise KeyError(task_id)
            self._tasks[task_id] = task
            return task

    def _persist(self, task: TaskRecord, event_type: Optional[str] = None,
                 **payload: Any) -> None:
        task.updated_at = time.time()
        task_db.upsert_task(task.to_db_dict(), event_type=event_type, event_payload=payload)

    def _terminal(self, task: TaskRecord) -> None:
        if task not in self._history:
            self._history.append(task)

    def set_phase(self, task_id: str, phase: TaskPhase, **payload: Any) -> None:
        task = self._task(task_id)
        task.phase = phase
        if task.current_attempt_id:
            task_db.update_attempt(task.current_attempt_id, phase=phase.value)
        self._persist(task, "phase_changed", phase=phase.value, **payload)

    def mark_running(self, task_id: str, worker_id: Optional[str] = None,
                     actual_site: Optional[str] = None, actual_target_id: Optional[str] = None,
                     actual_backend_type: Optional[BackendType] = None) -> None:
        task = self._task(task_id)
        task.status = TaskStatus.running
        task.phase = TaskPhase.claimed
        task.started_at = time.time()
        task.worker_id = worker_id
        task.actual_site = actual_site
        task.actual_target_id = actual_target_id
        task.actual_backend_type = actual_backend_type
        task.next_attempt_at = None
        self._persist(task, "task_claimed", worker_id=worker_id, actual_site=actual_site,
                      actual_target_id=actual_target_id,
                      actual_backend_type=actual_backend_type.value if actual_backend_type else None)

    def start_attempt(self, task_id: str, worker_id: str, site: Optional[str], model: str,
                      target_id: Optional[str] = None,
                      backend_type: Optional[BackendType] = None) -> str:
        task = self._task(task_id)
        attempt_id = uuid.uuid4().hex
        task.current_attempt_id = attempt_id
        self._persist(task, "attempt_started", attempt_id=attempt_id, worker_id=worker_id,
                      site=site, target_id=target_id, backend_type=backend_type.value if backend_type else None,
                      model=model, attempt_number=task.retries + 1)
        try:
            task_db.create_attempt({
                "attempt_id": attempt_id, "task_id": task_id,
                "attempt_number": task.retries + 1, "worker_id": worker_id,
                "site": site, "target_id": target_id,
                "backend_type": backend_type.value if backend_type else None,
                "model": model, "external_request_id": None, "process_exit_code": None,
                "provider_status_code": None, "usage_json": None, "phase": task.phase.value,
                "send_state": "not_sent", "error_code": None, "error": None,
                "started_at": time.time(), "finished_at": None,
            })
        except Exception:
            task.current_attempt_id = None
            # 尝试审计无法创建时不可继续触发真实浏览器操作。
            self._persist(task, "attempt_creation_failed", attempt_id=attempt_id)
            raise
        return attempt_id

    def update_attempt(self, task_id: str, *, phase: Optional[TaskPhase] = None,
                       send_state: Optional[str] = None, error_code: Optional[str] = None,
                       error: Optional[str] = None, external_request_id: Optional[str] = None,
                       process_exit_code: Optional[int] = None,
                       provider_status_code: Optional[int] = None,
                       usage: Optional[dict[str, Any]] = None, finished: bool = False) -> None:
        task = self._task(task_id)
        if not task.current_attempt_id:
            return
        fields: dict[str, Any] = {}
        if phase is not None:
            fields["phase"] = phase.value
        if send_state is not None:
            fields["send_state"] = send_state
        if error_code is not None:
            fields["error_code"] = error_code
        if error is not None:
            fields["error"] = error
        if external_request_id is not None:
            fields["external_request_id"] = external_request_id
        if process_exit_code is not None:
            fields["process_exit_code"] = process_exit_code
        if provider_status_code is not None:
            fields["provider_status_code"] = provider_status_code
        if usage is not None:
            fields["usage_json"] = json.dumps(usage, ensure_ascii=False, sort_keys=True)
        if finished:
            fields["finished_at"] = time.time()
        task_db.update_attempt(task.current_attempt_id, **fields)

    def finish_attempt(self, task_id: str, *, error_code: Optional[str] = None,
                       error: Optional[str] = None, send_state: Optional[str] = None,
                       external_request_id: Optional[str] = None,
                       process_exit_code: Optional[int] = None,
                       provider_status_code: Optional[int] = None,
                       usage: Optional[dict[str, Any]] = None) -> None:
        self.update_attempt(
            task_id, error_code=error_code, error=error, send_state=send_state,
            external_request_id=external_request_id, process_exit_code=process_exit_code,
            provider_status_code=provider_status_code, usage=usage, finished=True)

    def mark_done(self, task_id: str, result: str, *,
                  external_request_id: Optional[str] = None,
                  process_exit_code: Optional[int] = None,
                  provider_status_code: Optional[int] = None,
                  usage: Optional[dict[str, Any]] = None) -> None:
        task = self._task(task_id)
        task.status = TaskStatus.done
        task.phase = TaskPhase.completed
        task.result = result
        task.error = None
        task.error_code = None
        task.finished_at = time.time()
        if external_request_id is not None:
            task.upstream_request_id = external_request_id
        if usage is not None:
            task.usage = usage
        self.finish_attempt(
            task_id, send_state="sent_confirmed", external_request_id=external_request_id,
            process_exit_code=process_exit_code, provider_status_code=provider_status_code,
            usage=usage)
        self._persist(task, "task_completed", result_chars=len(result))
        self._terminal(task)

    def mark_failed(self, task_id: str, error: str, *, error_code: str = "task_failed") -> None:
        task = self._task(task_id)
        task.status = TaskStatus.failed
        task.phase = TaskPhase.failed
        task.error = error
        task.error_code = error_code
        task.finished_at = time.time()
        self.finish_attempt(task_id, error_code=error_code, error=error)
        self._persist(task, "task_failed", error_code=error_code, error=error)
        self._terminal(task)

    def mark_retry(self, task_id: str, reason: str, *, failed_site: Optional[str] = None,
                   error_code: str = "retryable_error", next_attempt_at: Optional[float] = None,
                   reset_queue_deadline: bool = False) -> None:
        task = self._task(task_id)
        if failed_site and failed_site not in task.tried_sites:
            task.tried_sites.append(failed_site)
        task.retries += 1
        task.status = TaskStatus.retry_wait if next_attempt_at else TaskStatus.queued
        task.phase = TaskPhase.retry_scheduled if next_attempt_at else TaskPhase.waiting_for_worker
        task.started_at = None
        task.worker_id = None
        task.actual_site = None
        task.actual_target_id = None
        task.actual_backend_type = None
        task.upstream_request_id = None
        task.usage = None
        task.finished_at = None
        task.result = None
        task.cancel_requested = False
        task.error = reason
        task.error_code = error_code
        task.next_attempt_at = next_attempt_at
        if reset_queue_deadline:
            task.queue_deadline_at = time.time() + self._max_queue_wait
        self.finish_attempt(task_id, error_code=error_code, error=reason)
        self._persist(task, "task_retry_scheduled", reason=reason, error_code=error_code,
                      next_attempt_at=next_attempt_at, failed_site=failed_site,
                      reset_queue_deadline=reset_queue_deadline)

    def mark_outcome_unknown(self, task_id: str, error: str) -> None:
        task = self._task(task_id)
        task.status = TaskStatus.outcome_unknown
        task.phase = TaskPhase.outcome_unknown
        task.error = error
        task.error_code = "send_outcome_unknown"
        task.finished_at = time.time()
        self.finish_attempt(task_id, error_code="send_outcome_unknown", error=error,
                            send_state="outcome_unknown")
        self._persist(task, "task_outcome_unknown", error=error)
        self._terminal(task)

    def mark_interrupted(self, task_id: str, error: str = "服务重启，执行中断") -> None:
        task = self._task(task_id)
        task.status = TaskStatus.interrupted
        task.phase = TaskPhase.interrupted
        task.error = error
        task.error_code = "process_interrupted"
        task.finished_at = time.time()
        self.finish_attempt(task_id, error_code="process_interrupted", error=error)
        self._persist(task, "task_interrupted", error=error)
        self._terminal(task)

    def request_cancel(self, task_id: str) -> TaskRecord:
        task = self._task(task_id)
        task.cancel_requested = True
        self._persist(task, "cancel_requested")
        return task

    def mark_cancelled(self, task_id: str, reason: str = "任务已取消") -> None:
        task = self._task(task_id)
        task.status = TaskStatus.cancelled
        task.phase = TaskPhase.cancelled
        task.error = reason
        task.error_code = "cancelled"
        task.finished_at = time.time()
        self.finish_attempt(task_id, error_code="cancelled", error=reason)
        self._persist(task, "task_cancelled", reason=reason)
        self._terminal(task)

    def mark_expired(self, task_id: str, reason: str = "排队等待超时") -> None:
        task = self._task(task_id)
        task.status = TaskStatus.expired
        task.phase = TaskPhase.failed
        task.error = reason
        task.error_code = "queue_timeout"
        task.finished_at = time.time()
        self._persist(task, "task_expired", reason=reason)
        self._terminal(task)

    def history(self) -> list[TaskRecord]:
        return list(reversed(self._history))

    def list_recoverable(self) -> list[TaskRecord]:
        records = [TaskRecord.from_db_dict(row) for row in task_db.list_recoverable_tasks()]
        for record in records:
            self._tasks[record.task_id] = record
        return records

    def configure_database_retention(self, terminal_seconds: int, metadata_seconds: int) -> None:
        self._terminal_retention = terminal_seconds
        self._metadata_retention = metadata_seconds

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
        now = time.time()
        cutoff = now - self._retention
        if self._terminal_retention is not None and self._metadata_retention is not None:
            task_db.prune_terminal_tasks(now - self._terminal_retention,
                                         now - self._metadata_retention)
        expired = [
            tid for tid, task in self._tasks.items()
            if task.status in TERMINAL_STATUSES and task.finished_at is not None
            and task.finished_at < cutoff
        ]
        for task_id in expired:
            del self._tasks[task_id]
