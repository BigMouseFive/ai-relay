"""Worker 池：按配置创建 worker，队列 + dispatcher 协程分发任务。"""
from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Callable, Optional

from .config import Config
from .schemas import TaskSummary, WorkerInfo, WorkerState
from .sites import create_adapter
from .sites.base import SiteAdapter
from .store import TaskRecord, TaskStore
from .webbridge import WebbridgeClient
from .worker import Worker

logger = logging.getLogger("ai-relay.pool")

# 没有空闲 worker 时 dispatcher 的等待间隔
DISPATCH_INTERVAL = 0.5


class QueueFull(Exception):
    pass


class WorkerPool:
    def __init__(self, config: Config, store: TaskStore,
                 client: Optional[WebbridgeClient] = None,
                 adapter_factory: Optional[Callable[[str, WebbridgeClient, str], SiteAdapter]] = None,
                 dispatch_interval: float = DISPATCH_INTERVAL):
        self.config = config
        self.store = store
        self.client = client
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=config.queue.max_size)
        self.workers: list[Worker] = []
        self._dispatcher: Optional[asyncio.Task] = None
        self._dispatch_interval = dispatch_interval
        self.max_retries = config.task.max_retries
        self.retry_switch_site = config.task.retry_switch_site
        factory = adapter_factory or create_adapter
        n = 0
        for wc in config.workers:
            for _ in range(wc.count):
                n += 1
                adapter = factory(wc.site, client, f"relay-w{n}")
                self.workers.append(Worker(
                    worker_id=f"w{n}", site=wc.site, model=wc.model,
                    adapter=adapter, store=store,
                    timeout_seconds=config.task.timeout_seconds,
                    stall_seconds=config.task.stall_seconds,
                    hard_timeout_seconds=config.task.hard_timeout_seconds,
                    on_retry=self._on_retry_needed))

    async def start(self) -> None:
        await asyncio.gather(*(w.start() for w in self.workers))
        self._dispatcher = asyncio.create_task(self._dispatch_loop())
        logger.info("worker 池已启动，共 %d 个 worker", len(self.workers))

    async def stop(self) -> None:
        if self._dispatcher:
            self._dispatcher.cancel()
            with suppress(asyncio.CancelledError):
                await self._dispatcher
            self._dispatcher = None
        await asyncio.gather(*(w.stop() for w in self.workers), return_exceptions=True)
        if self.client:
            await self.client.close()

    # ---- 任务提交与分发 ----

    async def submit(self, prompt: str, site: Optional[str]) -> str:
        task = self.store.create(prompt, site)
        try:
            self.queue.put_nowait(task)
        except asyncio.QueueFull:
            self.store.discard(task.task_id)
            raise QueueFull("队列已满") from None
        logger.info("任务 %s 已入队（site=%s，队列 %d）",
                    task.task_id[:8], site or "任意", self.queue.qsize())
        return task.task_id

    def _on_retry_needed(self, task: TaskRecord, reason: str) -> bool:
        """任务失败后尝试重新入队。返回 True=已重入队，False=最终失败。"""
        current = self.store.get(task.task_id) or task
        if current.retries >= self.max_retries:
            self.store.mark_failed(
                current.task_id,
                f"{reason}（已重试 {current.retries} 次）")
            logger.warning("任务 %s 重试耗尽（%d 次）: %s",
                           current.task_id[:8], current.retries, reason)
            return False
        failed_site = current.actual_site
        self.store.mark_retry(current.task_id, reason, failed_site=failed_site)
        try:
            self.queue.put_nowait(current)
        except asyncio.QueueFull:
            self.store.mark_failed(
                current.task_id, f"{reason}（重试时队列已满）")
            logger.warning("任务 %s 重试时队列已满", current.task_id[:8])
            return False
        logger.info("任务 %s 重新入队（retries=%d, tried=%s）: %s",
                    current.task_id[:8], current.retries,
                    ",".join(current.tried_sites) or "-", reason)
        return True

    async def _dispatch_loop(self) -> None:
        while True:
            # 扫描队列中第一个"有空闲匹配 worker"的任务并指派；
            # 任务留在队列里直到真正被指派，监控页可见。
            # 跳过暂时无法指派的任务（如指定站点繁忙），避免队头阻塞
            while True:
                items = self.queue._queue  # asyncio.Queue 无公开遍历接口，只读扫描
                assigned = False
                for task in list(items):
                    worker = self._find_idle_worker(task.site, task.tried_sites)
                    if worker is None:
                        continue
                    items.remove(task)
                    logger.info("任务 %s 指派给 worker %s（%s）",
                                task.task_id[:8], worker.worker_id, worker.site)
                    worker.start_task(task)
                    assigned = True
                    break
                if not assigned:
                    break
            await asyncio.sleep(self._dispatch_interval)

    def _find_idle_worker(self, site: Optional[str],
                          tried_sites: Optional[list[str]] = None) -> Optional[Worker]:
        tried = tried_sites or []

        def _idle(pred) -> Optional[Worker]:
            for w in self.workers:
                if w.state == WorkerState.idle and pred(w):
                    return w
            return None

        if not self.retry_switch_site:
            # 禁止换站：仅匹配指定站点（或任意）
            return _idle(lambda x: site is None or x.site == site)

        # 允许换站：
        # 1. 指定站点且尚未失败过该站 → 仍优先本站
        if site and site not in tried:
            w = _idle(lambda x: x.site == site)
            if w is not None:
                return w
            # 本站忙且还没失败过：继续等，不提前换站
            if not tried:
                return None

        # 2. 避开已试站点
        w = _idle(lambda x: x.site not in tried)
        if w is not None:
            return w

        # 3. 仍无：任意空闲（含已试）
        return _idle(lambda x: True)

    # ---- 监控快照 ----

    def workers_info(self) -> list[WorkerInfo]:
        return [
            WorkerInfo(
                worker_id=w.worker_id, site=w.site, model=w.model, state=w.state,
                current_task_id=w.current_task_id, detail=w.detail,
                done_count=w.done_count, fail_count=w.fail_count)
            for w in self.workers
        ]

    def queue_snapshot(self) -> list[TaskSummary]:
        # asyncio.Queue 无公开迭代接口，直接读内部 deque 做只读快照
        return [t.to_summary() for t in list(self.queue._queue)]

    async def daemon_health(self) -> tuple[bool, str]:
        if self.client is None:
            return False, "webbridge client 未配置"
        return await self.client.healthy()
