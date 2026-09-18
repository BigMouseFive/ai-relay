"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class Site(str, Enum):
    kimi = "kimi"
    deepseek = "deepseek"
    minimax = "minimax"


class TaskStatus(str, Enum):
    queued = "queued"
    retry_wait = "retry_wait"
    running = "running"
    outcome_unknown = "outcome_unknown"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"
    expired = "expired"
    interrupted = "interrupted"


class TaskPhase(str, Enum):
    waiting_for_worker = "waiting_for_worker"
    retry_scheduled = "retry_scheduled"
    claimed = "claimed"
    opening_chat = "opening_chat"
    configuring_model = "configuring_model"
    sending = "sending"
    verifying_send = "verifying_send"
    generating = "generating"
    collecting_result = "collecting_result"
    cleaning_up = "cleaning_up"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    interrupted = "interrupted"
    outcome_unknown = "outcome_unknown"


class AttemptSendState(str, Enum):
    not_sent = "not_sent"
    sending = "sending"
    sent_confirmed = "sent_confirmed"
    outcome_unknown = "outcome_unknown"


class WorkerState(str, Enum):
    starting = "starting"
    idle = "idle"
    busy = "busy"
    degraded = "degraded"


class BackendType(str, Enum):
    webbridge = "webbridge"
    acp = "acp"
    openai_compatible = "openai_compatible"


class SubmitTaskRequest(BaseModel):
    prompt: str = Field(min_length=1)
    # site 是旧浏览器路由兼容参数；新调用方使用 target 精确指定执行后端。
    site: Optional[Site] = None
    target: Optional[str] = Field(default=None, min_length=1, max_length=120)
    # 未指定 target 时，可显式选择 browser 兼容模式或 adaptive 自动路由。
    routing_mode: Optional[Literal["browser", "adaptive"]] = None
    model: Optional[str] = None
    allow_fallback_sites: Optional[bool] = None

    @field_validator("prompt")
    @classmethod
    def reject_blank_prompt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("prompt 不能为空白")
        return value

    @field_validator("target")
    @classmethod
    def reject_blank_target(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.strip():
            raise ValueError("target 不能为空白")
        return value.strip() if value else value


class SubmitTaskResponse(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.queued
    phase: TaskPhase = TaskPhase.waiting_for_worker
    reused: bool = False


class TaskAttemptInfo(BaseModel):
    attempt_id: str
    task_id: str
    attempt_number: int
    worker_id: Optional[str] = None
    site: Optional[str] = None
    target_id: Optional[str] = None
    backend_type: Optional[BackendType] = None
    model: Optional[str] = None
    external_request_id: Optional[str] = None
    process_exit_code: Optional[int] = None
    provider_status_code: Optional[int] = None
    usage: Optional[dict[str, Any]] = None
    phase: Optional[TaskPhase] = None
    send_state: Optional[AttemptSendState] = None
    error_code: Optional[str] = None
    error: Optional[str] = None
    started_at: float
    finished_at: Optional[float] = None


class TaskEventInfo(BaseModel):
    event_id: int
    task_id: str
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: float


class TaskInfo(BaseModel):
    task_id: str
    status: TaskStatus
    phase: TaskPhase = TaskPhase.waiting_for_worker
    site: Optional[str] = None
    actual_site: Optional[str] = None
    target_id: Optional[str] = None
    actual_target_id: Optional[str] = None
    backend_type: Optional[BackendType] = None
    actual_backend_type: Optional[BackendType] = None
    worker_id: Optional[str] = None
    prompt: str
    result: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    elapsed_seconds: Optional[float] = None
    retries: int = 0
    next_attempt_at: Optional[float] = None
    queue_deadline_at: Optional[float] = None
    cancel_requested: bool = False
    current_attempt_id: Optional[str] = None
    model: Optional[str] = None
    allow_fallback_sites: Optional[bool] = None
    upstream_request_id: Optional[str] = None
    usage: Optional[dict[str, Any]] = None
    routing_mode: Optional[str] = None
    routing_decision: Optional[dict[str, Any]] = None
    tried_sites: list[str] = Field(default_factory=list)
    attempts: list[TaskAttemptInfo] = Field(default_factory=list)


class TaskSummary(BaseModel):
    """监控页列表用的摘要（prompt/result 截断）。"""

    task_id: str
    status: TaskStatus
    phase: TaskPhase = TaskPhase.waiting_for_worker
    site: Optional[str] = None
    actual_site: Optional[str] = None
    target_id: Optional[str] = None
    actual_target_id: Optional[str] = None
    backend_type: Optional[BackendType] = None
    actual_backend_type: Optional[BackendType] = None
    prompt_preview: str
    result_preview: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    created_at: float
    elapsed_seconds: Optional[float] = None
    retries: int = 0
    next_attempt_at: Optional[float] = None
    queue_deadline_at: Optional[float] = None
    cancel_requested: bool = False
    routing_mode: Optional[str] = None


class TaskListPage(BaseModel):
    """分页任务列表。"""

    items: list[TaskSummary]
    total: int
    page: int
    page_size: int


class WorkerInfo(BaseModel):
    worker_id: str
    site: Optional[str] = None
    target_id: str
    backend_type: BackendType
    model: str
    state: WorkerState
    current_task_id: Optional[str] = None
    detail: Optional[str] = None
    done_count: int = 0
    fail_count: int = 0


class StatsResponse(BaseModel):
    daemon_ok: bool
    daemon_detail: Optional[str] = None
    queue_size: int
    queued_tasks: list[TaskSummary] = Field(default_factory=list)
    workers: list[WorkerInfo] = Field(default_factory=list)
    recent_tasks: list[TaskSummary] = Field(default_factory=list)


class HealthResponse(BaseModel):
    # 保留现有 /health 语义：ok 仅表示服务进程响应。
    ok: bool
    daemon_ok: bool
    daemon_detail: Optional[str] = None
