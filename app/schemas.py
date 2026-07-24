"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Site(str, Enum):
    kimi = "kimi"
    deepseek = "deepseek"
    minimax = "minimax"


class TaskStatus(str, Enum):
    queued = "queued"
    running = "running"
    done = "done"
    failed = "failed"


class WorkerState(str, Enum):
    starting = "starting"
    idle = "idle"
    busy = "busy"
    degraded = "degraded"


class SubmitTaskRequest(BaseModel):
    prompt: str = Field(min_length=1)
    site: Optional[Site] = None  # 省略则任一空闲 worker 均可


class SubmitTaskResponse(BaseModel):
    task_id: str


class TaskInfo(BaseModel):
    task_id: str
    status: TaskStatus
    site: Optional[str] = None
    actual_site: Optional[str] = None  # 实际执行站点
    worker_id: Optional[str] = None    # 执行的 worker
    prompt: str
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    elapsed_seconds: Optional[float] = None
    retries: Optional[int] = None      # 已重新入队次数


class TaskSummary(BaseModel):
    """监控页列表用的摘要（prompt/result 截断）。"""
    task_id: str
    status: TaskStatus
    site: Optional[str] = None
    actual_site: Optional[str] = None  # 实际执行站点
    prompt_preview: str
    result_preview: Optional[str] = None
    error: Optional[str] = None
    created_at: float
    elapsed_seconds: Optional[float] = None
    retries: Optional[int] = None      # 已重新入队次数


class TaskListPage(BaseModel):
    """分页任务列表。"""
    items: list[TaskSummary]
    total: int
    page: int
    page_size: int


class WorkerInfo(BaseModel):
    worker_id: str
    site: str
    model: str
    state: WorkerState
    current_task_id: Optional[str] = None
    detail: Optional[str] = None  # degraded 原因等
    done_count: int = 0
    fail_count: int = 0


class StatsResponse(BaseModel):
    daemon_ok: bool
    daemon_detail: Optional[str] = None
    queue_size: int
    queued_tasks: list[TaskSummary] = []
    workers: list[WorkerInfo] = []
    recent_tasks: list[TaskSummary] = []


class HealthResponse(BaseModel):
    ok: bool
    daemon_ok: bool
    daemon_detail: Optional[str] = None
