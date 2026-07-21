"""FastAPI 应用入口。

启动：uvicorn app.main:app --host ... --port ...
配置文件路径可用环境变量 AI_RELAY_CONFIG 覆盖，默认 ./config.yaml
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from . import db as task_db
from .config import load_config
from .pool import QueueFull, WorkerPool
from .schemas import (HealthResponse, StatsResponse, SubmitTaskRequest,
                      SubmitTaskResponse, TaskInfo, TaskListPage, TaskStatus)
from .store import TaskStore
from .webbridge import WebbridgeClient

logger = logging.getLogger("ai-relay")

STATIC_DIR = Path(__file__).parent / "static"


def create_app(config_path: Optional[str] = None, *,
               store: Optional[TaskStore] = None,
               pool: Optional[WorkerPool] = None) -> FastAPI:
    config_file = config_path or os.environ.get("AI_RELAY_CONFIG", "config.yaml")
    config = load_config(config_file)
    # SQLite 路径相对于配置文件所在目录
    db_path = config.storage.db_path
    if not os.path.isabs(db_path):
        db_path = str(Path(config_file).resolve().parent / db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task_db.init(db_path)
        # 上次运行遗留的 queued/running 任务已无人执行，统一标记为中断失败
        interrupted = task_db.fail_unfinished()
        if interrupted:
            logger.info("标记 %d 个上次遗留的未完成任务为中断", interrupted)
        st = store or TaskStore(config.dashboard.history_size,
                                config.task.retention_seconds)
        pl = pool or WorkerPool(config, st, WebbridgeClient(config.webbridge.base_url))
        app.state.config = config
        app.state.store = st
        app.state.pool = pl
        st.start()
        await pl.start()
        # 配置了就向 ERP 心跳注册（局域网动态接入）
        registrar = None
        if config.erp.register_url:
            from .registrar import ErpRegistrar
            registrar = ErpRegistrar(config.erp, config.server.port)
            await registrar.start()
        logger.info("ai-relay 已启动，监听 %s:%d", config.server.host, config.server.port)
        yield
        if registrar:
            await registrar.stop()
        await pl.stop()
        await st.stop()
        task_db.close()

    app = FastAPI(title="AI 问答中转站", lifespan=lifespan)

    @app.post("/api/tasks", response_model=SubmitTaskResponse)
    async def submit_task(req: SubmitTaskRequest, request: Request):
        cfg = request.app.state.config
        if len(req.prompt) > cfg.task.max_prompt_chars:
            raise HTTPException(
                422, f"prompt 超长（{len(req.prompt)} > {cfg.task.max_prompt_chars} 字符）")
        try:
            task_id = await request.app.state.pool.submit(
                req.prompt, req.site.value if req.site else None)
        except QueueFull:
            raise HTTPException(429, "队列已满，请稍后再试") from None
        return SubmitTaskResponse(task_id=task_id)

    @app.get("/api/tasks", response_model=TaskListPage)
    async def list_tasks(page: int = Query(1, ge=1),
                         page_size: int = Query(20, ge=1, le=100),
                         status: Optional[str] = Query(None)):
        """分页查询全部任务（SQLite 持久化，创建时间倒序，默认 20 条/页）。"""
        from .store import TaskRecord
        status_filter = status if status in {s.value for s in TaskStatus} else None
        total = task_db.count_tasks(status_filter)
        rows = task_db.list_tasks(page, page_size, status_filter)
        items = [TaskRecord.from_db_dict(r).to_summary() for r in rows]
        return TaskListPage(items=items, total=total, page=page, page_size=page_size)

    @app.get("/api/tasks/{task_id}", response_model=TaskInfo)
    async def get_task(task_id: str, request: Request):
        task = request.app.state.store.get(task_id)
        if task is None:
            raise HTTPException(404, "任务不存在")
        return task.to_info()

    @app.get("/api/stats", response_model=StatsResponse)
    async def stats(request: Request):
        pl = request.app.state.pool
        ok, detail = await pl.daemon_health()
        queued = pl.queue_snapshot()
        return StatsResponse(
            daemon_ok=ok,
            daemon_detail=detail,
            queue_size=len(queued),
            queued_tasks=queued,
            workers=pl.workers_info(),
            recent_tasks=[t.to_summary() for t in request.app.state.store.history()],
        )

    @app.get("/health", response_model=HealthResponse)
    async def health(request: Request):
        ok, detail = await request.app.state.pool.daemon_health()
        return HealthResponse(ok=True, daemon_ok=ok, daemon_detail=detail)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


app = create_app()
