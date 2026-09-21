"""FastAPI 应用工厂。

生产启动请使用 ``python run.py [config.yaml]``，以保证配置只加载一次并持有
同一任务库的进程锁。直接 Uvicorn 启动可使用 ``--factory app.main:create_app``。
"""
from __future__ import annotations

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import db as task_db
from .config import Config, load_config
from .instance_lock import InstanceLock
from .pool import QueueFull, WorkerPool
from .response_contract import validate_response_format
from .schemas import (
    StatsResponse, SubmitTaskRequest, SubmitTaskResponse, TaskEventInfo, TaskInfo,
    TaskListPage, TaskStatus, HealthResponse,
)
from .store import TaskRecord, TaskStore
from .webbridge import WebbridgeClient

logger = logging.getLogger("ai-relay")
STATIC_DIR = Path(__file__).parent / "static"


def _db_path(config_file: str, config: Config) -> str:
    path = config.storage.db_path
    return path if os.path.isabs(path) else str(Path(config_file).resolve().parent / path)


def _task_or_404(store: TaskStore, task_id: str) -> TaskRecord:
    task = store.get(task_id)
    if task is None:
        raise HTTPException(404, "任务不存在")
    return task


def create_app(config_path: Optional[str] = None, *, config: Optional[Config] = None,
               store: Optional[TaskStore] = None,
               pool: Optional[Any] = None) -> FastAPI:
    config_file = os.path.abspath(os.path.expanduser(
        config_path or os.environ.get("AI_RELAY_CONFIG", "config.yaml")))
    cfg = config or load_config(config_file)
    db_path = _db_path(config_file, cfg)
    # 测试或嵌入式调用方显式注入 store/pool 时，由调用方管理已初始化的任务库。
    manage_db = store is None and pool is None
    instance_lock = InstanceLock(db_path) if manage_db else None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        lock_acquired = False
        db_open = False
        st: Optional[TaskStore] = None
        pl: Optional[WorkerPool] = None
        publisher = None
        service_id = None
        try:
            if instance_lock:
                instance_lock.acquire()
                lock_acquired = True
            if manage_db:
                task_db.init(db_path)
                db_open = True
                recovery = task_db.recover_unfinished()
                if recovery["interrupted"]:
                    logger.warning("标记 %d 个遗留 running 任务为 interrupted", recovery["interrupted"])
            st = store or TaskStore(
                cfg.dashboard.history_size, cfg.task.retention_seconds,
                cfg.task.max_queue_wait_seconds,
            )
            st.configure_database_retention(
                cfg.storage.terminal_retention_seconds,
                cfg.storage.metadata_retention_seconds,
            )
            pl = pool or WorkerPool(
                cfg, st,
                WebbridgeClient(
                    cfg.webbridge.base_url,
                    timeout=cfg.webbridge.command_timeout_seconds,
                    status_timeout=cfg.webbridge.status_timeout_seconds,
                ),
            )
            assert pl is not None
            app.state.config = cfg
            app.state.store = st
            app.state.pool = pl
            app.state.db_path = db_path
            st.start()
            await pl.start()
            # 注入 store/pool 的测试/嵌入模式由宿主自行管理服务发现，避免测试发布真实 mDNS。
            if cfg.discovery.enabled and manage_db:
                from .discovery import (
                    MdnsPublisher, load_or_create_service_id, start_publisher_async,
                    stop_publisher_async,
                )
                identity_path = Path(cfg.discovery.identity_path).expanduser()
                if not identity_path.is_absolute():
                    identity_path = Path(config_file).resolve().parent / identity_path
                service_id = load_or_create_service_id(identity_path)
                publisher = MdnsPublisher(
                    service_id=service_id, port=cfg.server.port,
                    instance_name=cfg.discovery.instance_name,
                    advertise_address=cfg.discovery.advertise_address,
                )
                # zeroconf 的同步注册会阻塞事件循环，必须在线程中执行。
                await start_publisher_async(publisher)
            app.state.service_id = service_id
            app.state.mdns_publisher = publisher
            logger.info("ai-relay 已启动，监听 %s:%d", cfg.server.host, cfg.server.port)
            yield
        finally:
            if publisher:
                await stop_publisher_async(publisher)
            if pl:
                await pl.stop()
            if st:
                await st.stop()
            if db_open:
                task_db.close()
            if lock_acquired and instance_lock:
                instance_lock.release()

    app = FastAPI(title="AI 问答中转站", lifespan=lifespan)

    @app.post("/api/tasks", response_model=SubmitTaskResponse)
    async def submit_task(req: SubmitTaskRequest, request: Request):
        """兼容接口；新调用方应使用 /v1/tasks + Idempotency-Key。"""
        if len(req.prompt) > request.app.state.config.task.max_prompt_chars:
            raise HTTPException(422, "prompt 超长")
        if req.target or req.retry_policy or req.response_format:
            raise HTTPException(422, "旧 /api/tasks 不支持 target、retry_policy 或 response_format，请使用 /v1/tasks")
        try:
            task_id = await request.app.state.pool.submit(
                req.prompt, req.site.value if req.site else None)
        except QueueFull:
            raise HTTPException(429, "队列已满，请稍后再试") from None
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
        except Exception as e:
            logger.exception("兼容任务提交失败")
            raise HTTPException(503, "任务持久化或调度失败，请稍后重试") from e
        return SubmitTaskResponse(task_id=task_id)

    @app.post("/v1/tasks", response_model=SubmitTaskResponse, status_code=202)
    async def submit_task_v1(
        req: SubmitTaskRequest,
        request: Request,
        response: Response,
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ):
        cfg_local = request.app.state.config
        if len(req.prompt) > cfg_local.task.max_prompt_chars:
            raise HTTPException(422, "prompt 超长")
        response_format = req.response_format_payload()
        if response_format:
            try:
                validate_response_format(response_format)
            except ValueError as error:
                raise HTTPException(422, str(error)) from error
            if len(json.dumps(response_format["schema"], ensure_ascii=False)) > cfg_local.task.max_response_schema_chars:
                raise HTTPException(422, "response_format.schema 超过大小上限")
        if not idempotency_key:
            raise HTTPException(422, "缺少 Idempotency-Key")
        if len(idempotency_key) > 255:
            raise HTTPException(422, "Idempotency-Key 过长")
        pl = request.app.state.pool
        try:
            task, reused = await pl.submit_with_metadata(
                req.prompt, req.site.value if req.site else None, target_id=req.target,
                routing_mode=req.routing_mode, model=req.model,
                allow_fallback_sites=req.allow_fallback_sites,
                response_format=response_format,
                retry_policy_max_retries=(req.retry_policy.max_retries if req.retry_policy else None),
                idempotency_key=idempotency_key,
            )
        except task_db.IdempotencyConflict as e:
            raise HTTPException(409, "同一 Idempotency-Key 对应了不同请求") from e
        except QueueFull:
            raise HTTPException(429, "队列已满，请稍后再试") from None
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
        except Exception as e:
            logger.exception("v1 任务提交失败")
            raise HTTPException(503, "任务持久化或调度失败，请稍后重试") from e
        response.headers["Location"] = f"/v1/tasks/{task.task_id}"
        return SubmitTaskResponse(
            task_id=task.task_id, status=task.status, phase=task.phase, reused=reused)

    @app.get("/api/tasks", response_model=TaskListPage)
    @app.get("/v1/tasks", response_model=TaskListPage)
    async def list_tasks(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100),
                         status: Optional[TaskStatus] = Query(None),
                         q: Optional[str] = Query(None, max_length=200),
                         target_id: Optional[str] = Query(None, max_length=120),
                         backend_type: Optional[str] = Query(None, pattern=r"^(webbridge|acp|openai_compatible)$"),
                         created_after: Optional[float] = Query(None, ge=0),
                         created_before: Optional[float] = Query(None, ge=0)):
        if created_after is not None and created_before is not None and created_after > created_before:
            raise HTTPException(422, "created_after 不能大于 created_before")
        status_filter = status.value if status else None
        total = task_db.count_tasks(
            status_filter, q=q, target_id=target_id, backend_type=backend_type,
            created_after=created_after, created_before=created_before)
        rows = task_db.list_tasks(
            page, page_size, status_filter, q=q, target_id=target_id,
            backend_type=backend_type, created_after=created_after,
            created_before=created_before)
        items = [TaskRecord.from_db_dict(row).to_summary() for row in rows]
        return TaskListPage(items=items, total=total, page=page, page_size=page_size)

    @app.get("/v1/tasks/by-idempotency-key", response_model=TaskInfo)
    async def get_task_by_idempotency_key(
        request: Request,
        idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    ):
        if idempotency_key is None or not idempotency_key.strip():
            raise HTTPException(422, "缺少 Idempotency-Key")
        task = request.app.state.store.get_by_idempotency_key(idempotency_key)
        if task is None:
            raise HTTPException(404, "任务不存在")
        return task.to_info()

    @app.get("/api/tasks/{task_id}", response_model=TaskInfo)
    @app.get("/v1/tasks/{task_id}", response_model=TaskInfo)
    async def get_task(task_id: str, request: Request):
        return _task_or_404(request.app.state.store, task_id).to_info()

    @app.get("/v1/tasks/{task_id}/events", response_model=list[TaskEventInfo])
    async def get_task_events(task_id: str, request: Request,
                              after_event_id: int = Query(0, ge=0)):
        _task_or_404(request.app.state.store, task_id)
        return task_db.list_events(task_id, after_event_id)

    @app.post("/v1/tasks/{task_id}/cancel", response_model=TaskInfo)
    async def cancel_task(task_id: str, request: Request):
        task = _task_or_404(request.app.state.store, task_id)
        if task.status in {TaskStatus.done, TaskStatus.failed, TaskStatus.cancelled,
                           TaskStatus.expired, TaskStatus.interrupted, TaskStatus.outcome_unknown}:
            raise HTTPException(409, f"任务当前状态 {task.status.value} 不可取消")
        task = request.app.state.store.request_cancel(task_id)
        # queued/retry_wait 由 dispatcher 标记 cancelled；running 则立即取消对应协程/ACP 子进程。
        cancel_running = getattr(request.app.state.pool, "cancel_task", None)
        if cancel_running:
            cancel_running(task_id)
        return task.to_info()

    @app.post("/v1/tasks/{task_id}/retry", response_model=TaskInfo, status_code=202)
    async def retry_task(task_id: str, request: Request):
        task = _task_or_404(request.app.state.store, task_id)
        if task.status not in {TaskStatus.failed, TaskStatus.cancelled, TaskStatus.expired,
                               TaskStatus.interrupted, TaskStatus.outcome_unknown}:
            raise HTTPException(409, f"任务当前状态 {task.status.value} 不需要人工重试")
        store_local = request.app.state.store
        # 人工重试沿用原任务审计链，但不自动换站；新 attempt 会在 worker claim 时创建。
        store_local.mark_retry(task_id, "人工请求重试", error_code="manual_retry",
                               reset_queue_deadline=True)
        task = _task_or_404(store_local, task_id)
        try:
            request.app.state.pool._enqueue_existing(task)
        except QueueFull:
            store_local.mark_failed(task_id, "人工重试时队列已满", error_code="retry_queue_full")
            raise HTTPException(429, "队列已满，请稍后重试") from None
        return task.to_info()

    @app.get("/v1/capabilities")
    async def capabilities(request: Request):
        cfg_local = request.app.state.config
        workers = request.app.state.pool.workers_info()
        targets: dict[str, dict] = {}
        for worker in workers:
            entry = targets.setdefault(worker.target_id, {
                "id": worker.target_id, "type": worker.backend_type.value,
                "site": worker.site, "models": set(), "total": 0,
                "idle": 0, "busy": 0, "degraded": 0,
            })
            if worker.model:
                entry["models"].add(worker.model)
            entry["total"] += 1
            if worker.state.value in entry:
                entry[worker.state.value] += 1
        target_list = [
            {**value, "models": sorted(value["models"])}
            for _, value in sorted(targets.items())
        ]
        # sites 保留给已部署旧 dashboard；新客户端应读取 targets。
        sites: dict[str, dict] = {}
        for target in target_list:
            if target["type"] != "webbridge" or not target["site"]:
                continue
            entry = sites.setdefault(target["site"], {"models": set(), "total": 0,
                                                        "idle": 0, "busy": 0, "degraded": 0})
            entry["models"].update(target["models"])
            for key in ("total", "idle", "busy", "degraded"):
                entry[key] += target[key]
        return {
            "max_prompt_chars": cfg_local.task.max_prompt_chars,
            "queue_max_size": cfg_local.queue.max_size,
            "max_queue_wait_seconds": cfg_local.task.max_queue_wait_seconds,
            "default_allow_fallback_sites": cfg_local.task.retry_switch_site,
            "routing": {
                "default_mode": cfg_local.routing.default_mode,
                "window_seconds": cfg_local.routing.window_seconds,
                "min_samples": cfg_local.routing.min_samples,
                "exploration_weight": cfg_local.routing.exploration_weight,
            },
            "targets": target_list,
            "sites": [{"site": site, "models": sorted(value["models"]), **{key: value[key] for key in ("total", "idle", "busy", "degraded")}} for site, value in sorted(sites.items())],
        }

    @app.get("/v1/routing/metrics")
    async def routing_metrics(request: Request):
        """自动路由当前的可解释评分，不暴露 prompt/result。"""
        snapshot = getattr(request.app.state.pool, "routing_snapshot", None)
        if not snapshot:
            raise HTTPException(503, "当前 worker pool 不支持自适应路由指标")
        try:
            return snapshot()
        except ValueError as error:
            raise HTTPException(503, str(error)) from error

    @app.get("/api/stats", response_model=StatsResponse)
    async def stats(request: Request):
        pl = request.app.state.pool
        ok, detail = await pl.daemon_health()
        queued = pl.queue_snapshot()
        return StatsResponse(
            daemon_ok=ok, daemon_detail=detail, queue_size=len(queued),
            queued_tasks=queued, workers=pl.workers_info(),
            recent_tasks=[task.to_summary() for task in request.app.state.store.history()],
        )

    @app.get("/.well-known/amazon-service")
    async def service_metadata(request: Request):
        """供 LAN discovery agent 校验 mDNS 公告与服务协议。"""
        return {
            "service_type": "ai-relay",
            "service_id": getattr(request.app.state, "service_id", None),
            "api_version": 1,
            "endpoints": {
                "tasks": "/v1/tasks",
                "task": "/v1/tasks/{task_id}",
                "task_by_idempotency_key": "/v1/tasks/by-idempotency-key",
                "readiness": "/v1/readiness",
                "capabilities": "/v1/capabilities",
            },
        }

    @app.get("/v1/readiness")
    async def readiness(request: Request):
        """接单就绪状态；不同于仅表示进程存活的 /health。"""
        workers = request.app.state.pool.workers_info()
        healthy_workers = sum(worker.state.value != "degraded" for worker in workers)
        degraded_workers = sum(worker.state.value == "degraded" for worker in workers)
        queue_size = len(request.app.state.pool.queue_snapshot())
        queue_max_size = request.app.state.config.queue.max_size
        return {
            "ready": healthy_workers > 0,
            "accepting_tasks": healthy_workers > 0 and queue_size < queue_max_size,
            "queue_size": queue_size,
            "queue_max_size": queue_max_size,
            "healthy_workers": healthy_workers,
            "degraded_workers": degraded_workers,
        }

    @app.get("/health", response_model=HealthResponse)
    async def health(request: Request):
        # 按需求保留旧语义和 200 状态：ok 仅表示服务能响应。
        ok, detail = await request.app.state.pool.daemon_health()
        return HealthResponse(ok=True, daemon_ok=ok, daemon_detail=detail)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app
