"""统一 target 调度池：WebBridge、ACP 和 OpenAI-compatible API。"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from contextlib import suppress
from typing import Any, Callable, Optional

from . import db as task_db
from .adaptive_router import AdaptiveRouter
from .config import (
    AcpTargetConfig, Config, OpenAICompatibleTargetConfig, TargetConfig,
    TaskRoutingPolicy, WebbridgeTargetConfig,
)
from .external_workers import AcpWorker, ExternalWorker, OpenAICompatibleWorker
from .schemas import BackendType, TaskSummary, WorkerInfo, WorkerState
from .sites import create_adapter
from .sites.base import SiteAdapter
from .store import TaskRecord, TaskStore
from .webbridge import WebbridgeClient
from .worker import Worker

logger = logging.getLogger("ai-relay.pool")
DISPATCH_INTERVAL = 0.5


class QueueFull(Exception):
    pass


class TaskQueue:
    """可扫描且有明确公开 API 的有界任务队列。"""

    def __init__(self, max_size: int):
        self._max_size = max_size
        self._items: deque[TaskRecord] = deque()

    def put_nowait(self, task: TaskRecord) -> None:
        if len(self._items) >= self._max_size:
            raise QueueFull("队列已满")
        self._items.append(task)

    def remove(self, task: TaskRecord) -> None:
        self._items.remove(task)

    def snapshot(self) -> list[TaskRecord]:
        return list(self._items)

    def qsize(self) -> int:
        return len(self._items)


class WorkerPool:
    def __init__(self, config: Config, store: TaskStore,
                 client: Optional[WebbridgeClient] = None,
                 adapter_factory: Optional[Callable[[str, WebbridgeClient, str], SiteAdapter]] = None,
                 dispatch_interval: float = DISPATCH_INTERVAL):
        self.config = config
        self.store = store
        self.client = client
        self.queue = TaskQueue(config.queue.max_size)
        self.workers: list[Any] = []
        self._dispatcher: Optional[asyncio.Task] = None
        self._dispatch_interval = dispatch_interval
        self.max_retries = config.task.max_retries
        self.retry_switch_site = config.task.retry_switch_site
        self.retry_initial_delay = config.task.retry_initial_delay_seconds
        self.retry_max_delay = config.task.retry_max_delay_seconds
        self._queued_ids: set[str] = set()
        self.targets: dict[str, TargetConfig] = {target.id: target for target in config.targets}
        self.adaptive_router = AdaptiveRouter(config.routing)

        adapter_factory = adapter_factory or create_adapter
        worker_number = 0
        for target in config.targets:
            for slot in range(1, target.count + 1):
                worker_number += 1
                if isinstance(target, WebbridgeTargetConfig):
                    if client is None:
                        raise ValueError("配置了 webbridge target 但 WebbridgeClient 未配置")
                    adapter = adapter_factory(target.site, client, f"relay-{target.id}-{slot}")
                    worker = Worker(
                        worker_id=f"w{worker_number}", site=target.site, model=target.model,
                        adapter=adapter, store=store,
                        timeout_seconds=target.timeout_seconds or config.task.timeout_seconds,
                        stall_seconds=config.task.stall_seconds,
                        hard_timeout_seconds=config.task.hard_timeout_seconds,
                        on_retry=self._on_retry_needed, target_id=target.id,
                        tab_group_title=config.webbridge.tab_group_title,
                    )
                elif isinstance(target, AcpTargetConfig):
                    worker = AcpWorker(
                        worker_id=f"acp-{target.id}-{slot}", target=target, store=store,
                        default_timeout_seconds=config.task.timeout_seconds,
                        on_retry=self._on_retry_needed,
                    )
                elif isinstance(target, OpenAICompatibleTargetConfig):
                    worker = OpenAICompatibleWorker(
                        worker_id=f"api-{target.id}-{slot}", target=target, store=store,
                        default_timeout_seconds=config.task.timeout_seconds,
                        on_retry=self._on_retry_needed,
                    )
                else:  # pragma: no cover - Pydantic discriminated union guarantees exhaustiveness.
                    raise ValueError(f"未知 target 类型: {target}")
                self.workers.append(worker)

    async def start(self) -> None:
        await asyncio.gather(*(worker.start() for worker in self.workers))
        self._recover_pending_tasks()
        self._dispatcher = asyncio.create_task(self._dispatch_loop())
        logger.info("统一 worker 池已启动，共 %d 个 worker", len(self.workers))

    async def stop(self) -> None:
        if self._dispatcher:
            self._dispatcher.cancel()
            with suppress(asyncio.CancelledError):
                await self._dispatcher
            self._dispatcher = None
        await asyncio.gather(*(worker.stop() for worker in self.workers), return_exceptions=True)
        if self.client:
            await self.client.close()

    # ---- 任务提交与分发 ----

    async def submit(self, prompt: str, site: Optional[str]) -> str:
        """旧 API 兼容：只在浏览器 WebBridge target 中按 site 调度。"""
        task, _ = await self.submit_with_metadata(prompt, site, routing_mode="browser")
        return task.task_id

    async def submit_with_metadata(
        self, prompt: str, site: Optional[str] = None, *, target_id: Optional[str] = None,
        routing_mode: Optional[str] = None, model: Optional[str] = None,
        allow_fallback_sites: Optional[bool] = None,
        response_format: Optional[dict[str, Any]] = None,
        retry_policy_max_retries: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> tuple[TaskRecord, bool]:
        """先持久化再入队；target 精确路由，site 保持旧浏览器路由兼容。"""
        if target_id and site:
            raise ValueError("target 与 site 不能同时指定")

        resolved_site = site
        resolved_model = model
        backend_type: Optional[BackendType] = BackendType.webbridge
        decision: Optional[dict[str, Any]] = None
        policy_name, task_policy = self._task_policy(response_format)
        mode = "target" if target_id else ("browser" if site is not None else (routing_mode or self.config.routing.default_mode))
        if task_policy is not None:
            if target_id and target_id not in task_policy.allowed_targets:
                raise ValueError(
                    f"response_format.name={policy_name} 不允许 target: {target_id}")
            if not target_id:
                if site is not None or routing_mode == "browser":
                    raise ValueError(
                        f"response_format.name={policy_name} 的 task policy 需要自动 target 路由")
                # 匹配 task policy 的请求保持自动路由，不要求调用方/ERP 指定 target。
                mode = "adaptive"
                decision = {
                    "mode": "adaptive", "selected_target": None,
                    "task_policy": policy_name,
                    "allowed_targets": list(task_policy.allowed_targets),
                    "max_in_flight_per_target": task_policy.max_in_flight_per_target,
                }
        if mode not in {"browser", "adaptive", "target"}:
            raise ValueError(f"未知 routing_mode: {mode}")
        if not target_id and mode == "adaptive":
            if site is not None:
                raise ValueError("adaptive 路由不能与 site 同时指定，请改用 target 或 browser 模式")
            if model is not None:
                raise ValueError("adaptive 路由不支持指定模型，请显式指定 target")
            # adaptive target 在真正分派每个 attempt 时才选择，安全重试可重新评分路由。
            backend_type = None
        if target_id:
            target = self.targets.get(target_id)
            if target is None:
                raise ValueError(f"未知 target: {target_id}")
            backend_type = BackendType(target.type)
            if isinstance(target, WebbridgeTargetConfig):
                resolved_site = target.site
            else:
                resolved_site = None
            if model:
                if isinstance(target, OpenAICompatibleTargetConfig):
                    if not target.allow_model_override:
                        raise ValueError(f"target 不允许覆盖模型: {target_id}")
                    if model not in target.allowed_models:
                        raise ValueError(f"请求模型不在 target allowlist: {model}")
                elif model != target.model:
                    raise ValueError(f"target 未配置请求模型: {model}")
            resolved_model = model or target.model
            if task_policy is not None:
                decision = {
                    "mode": "target", "selected_target": target_id,
                    "task_policy": policy_name,
                    "allowed_targets": list(task_policy.allowed_targets),
                    "max_in_flight_per_target": task_policy.max_in_flight_per_target,
                }
        elif mode == "browser":
            # 兼容旧 API：无 target 时严格限制为 browser workers，绝不误路由 ACP/API。
            browser_workers = [w for w in self.workers
                               if getattr(w, "backend_type", BackendType.webbridge) == BackendType.webbridge]
            if not browser_workers:
                raise ValueError("未指定 target，且没有可用 webbridge target")
            if site is not None and not any(getattr(w, "site", None) == site for w in browser_workers):
                raise ValueError(f"站点未配置 browser worker: {site}")
            if model and not any(
                (site is None or getattr(w, "site", None) == site) and model in (getattr(w, "model", "") or "")
                for w in browser_workers
            ):
                raise ValueError(f"站点未配置请求模型: {model}")

        requested_retries = retry_policy_max_retries
        effective_retries = self._effective_max_retries(requested_retries, target_id)
        task, reused = self.store.create_or_get_idempotent(
            prompt, resolved_site, target_id=target_id, backend_type=backend_type,
            model=resolved_model, allow_fallback_sites=allow_fallback_sites,
            routing_mode=mode, routing_decision=decision,
            response_format=response_format, max_retries=effective_retries,
            idempotency_key=idempotency_key,
        )
        if reused:
            return task, True
        try:
            self._enqueue_existing(task)
        except QueueFull:
            self.store.discard(task.task_id)
            raise
        logger.info("任务 %s 已入队（路由=%s，target=%s，site=%s，队列 %d）",
                    task.task_id[:8], mode, target_id or "动态", resolved_site or "-",
                    self.queue.qsize())
        return task, False

    def _enqueue_existing(self, task: TaskRecord) -> None:
        if task.task_id in self._queued_ids:
            return
        self.queue.put_nowait(task)
        self._queued_ids.add(task.task_id)

    def _recover_pending_tasks(self) -> None:
        for task in self.store.list_recoverable():
            if task.queue_deadline_at is not None and task.queue_deadline_at <= time.time():
                self.store.mark_expired(task.task_id)
                continue
            # 已删除/改名 target 的遗留任务不可安全执行，明确失败。
            if task.target_id and task.target_id not in self.targets:
                self.store.mark_failed(task.task_id, f"目标已不存在: {task.target_id}",
                                       error_code="target_removed")
                continue
            try:
                self._enqueue_existing(task)
            except QueueFull:
                break

    def _on_retry_needed(self, task: TaskRecord, reason: str,
                         error_code: str = "retryable_error") -> bool:
        """发送前明确可重试的错误进入退避队列。

        target 精确任务不会切换到其他 target；旧 browser 任务才可按原策略换站。
        """
        current = self.store.get(task.task_id) or task
        max_retries = self._target_max_retries(current)
        if current.retries >= max_retries:
            final_code = ("response_validation_exhausted"
                          if error_code.startswith("response_") else error_code)
            self.store.mark_failed(
                current.task_id, f"{reason}（已重试 {current.retries} 次）", error_code=final_code)
            return False
        failed_site = (current.actual_site
                       if current.routing_mode == "browser" and not current.target_id else None)
        delay = min(self.retry_initial_delay * (2 ** current.retries), self.retry_max_delay)
        delay *= random.uniform(0.8, 1.2)
        self.store.mark_retry(
            current.task_id, reason, failed_site=failed_site, error_code=error_code,
            next_attempt_at=time.time() + delay,
        )
        try:
            self._enqueue_existing(current)
        except QueueFull:
            self.store.mark_failed(
                current.task_id, f"{reason}（重试时队列已满）", error_code="retry_queue_full")
            return False
        logger.info("任务 %s 将在 %.1fs 后重试（路由=%s, target=%s, retries=%d）: %s",
                    current.task_id[:8], delay, current.routing_mode or "browser",
                    current.target_id or current.actual_target_id or "动态",
                    current.retries, reason)
        return True

    def cancel_task(self, task_id: str) -> bool:
        """尽力取消当前正在执行的 worker；queued 任务由 dispatcher 处理。"""
        for worker in self.workers:
            if getattr(worker, "current_task_id", None) == task_id:
                cancel = getattr(worker, "cancel_current_task", None)
                return bool(cancel and cancel())
        return False

    def _effective_max_retries(self, requested: Optional[int], target_id: Optional[str]) -> int:
        """任务预算 = 服务默认/调用方预算（取调用方更严格值）并受 target 上限约束。"""
        configured = self.max_retries
        if requested is not None:
            configured = min(configured, requested, self.config.task.max_client_requested_retries)
        if target_id:
            target = self.targets.get(target_id)
            if target and target.max_retries is not None:
                configured = min(configured, target.max_retries)
        return configured

    def _target_max_retries(self, task: TaskRecord) -> int:
        configured = task.max_retries if task.max_retries is not None else self.max_retries
        # 对 adaptive attempt 也应用当前实际 target 的较严格重试限制。
        target_id = task.target_id or task.actual_target_id
        if target_id:
            target = self.targets.get(target_id)
            if target and target.max_retries is not None:
                configured = min(configured, target.max_retries)
        return configured

    async def _dispatch_loop(self) -> None:
        while True:
            while True:
                items = self.queue.snapshot()
                assigned = False
                now = time.time()
                for task in items:
                    if task.cancel_requested:
                        self.queue.remove(task)
                        self._queued_ids.discard(task.task_id)
                        self.store.mark_cancelled(task.task_id)
                        assigned = True
                        break
                    if task.queue_deadline_at is not None and task.queue_deadline_at <= now:
                        self.queue.remove(task)
                        self._queued_ids.discard(task.task_id)
                        self.store.mark_expired(task.task_id)
                        assigned = True
                        break
                    if task.next_attempt_at is not None and task.next_attempt_at > now:
                        continue
                    worker, decision = self._find_dispatch_worker(task)
                    if worker is None:
                        continue
                    self.queue.remove(task)
                    self._queued_ids.discard(task.task_id)
                    if decision:
                        task.routing_decision = decision
                        task_db.append_event(task.task_id, "adaptive_target_selected", decision)
                    self.store.mark_running(
                        task.task_id, worker_id=worker.worker_id,
                        actual_site=getattr(worker, "site", None),
                        actual_target_id=getattr(worker, "target_id", None),
                        actual_backend_type=getattr(worker, "backend_type", None),
                    )
                    logger.info("任务 %s 指派给 worker %s（target=%s）",
                                task.task_id[:8], worker.worker_id,
                                getattr(worker, "target_id", "-"))
                    worker.start_task(task)
                    assigned = True
                    break
                if not assigned:
                    break
            self._recover_pending_tasks()
            await asyncio.sleep(self._dispatch_interval)

    def _find_dispatch_worker(self, task: TaskRecord) -> tuple[Optional[Any], Optional[dict[str, Any]]]:
        """返回当前 attempt 的 worker；adaptive 每次分派都重新计算 target。"""
        policy_name, task_policy = self._task_policy(task.response_format)
        if task.routing_mode != "adaptive":
            if (task_policy is not None and task.target_id
                    and task_policy.max_in_flight_per_target is not None):
                in_flight = sum(
                    getattr(worker, "target_id", None) == task.target_id
                    and getattr(worker, "state", None) == WorkerState.busy
                    for worker in self.workers)
                if in_flight >= task_policy.max_in_flight_per_target:
                    return None, None
            return self._find_idle_worker(task), None
        try:
            if task_policy is None:
                decision = self.adaptive_router.choose(
                    self.workers_info(), require_idle=True)
            else:
                decision = self.adaptive_router.choose(
                    self.workers_info(), require_idle=True,
                    allowed_targets=task_policy.allowed_targets,
                    max_in_flight_per_target=task_policy.max_in_flight_per_target,
                    task_policy=policy_name,
                )
        except ValueError:
            # 没有符合 policy/容量限制的 idle worker 时保留在队列中等待。
            return None, None
        worker = next((candidate for candidate in self.workers
                       if candidate.state == WorkerState.idle
                       and getattr(candidate, "target_id", None) == decision.target_id), None)
        return worker, decision.details

    def _task_policy(
        self, response_format: Optional[dict[str, Any]],
    ) -> tuple[Optional[str], Optional[TaskRoutingPolicy]]:
        if not response_format:
            return None, None
        name = response_format.get("name")
        if not isinstance(name, str):
            return None, None
        return name, self.config.routing.task_policies.get(name)

    def _find_idle_worker(self, task: TaskRecord | Optional[str],
                          tried_sites: Optional[list[str]] = None,
                          allow_fallback_sites: Optional[bool] = None,
                          model: Optional[str] = None) -> Optional[Any]:
        """匹配 idle worker。

        ``task`` 参数是新调度路径；保留 site/tried_sites/model 参数以兼容旧内部调用。
        """
        if not isinstance(task, TaskRecord):
            return self._find_legacy_idle_worker(task, tried_sites or [],
                                                 allow_fallback_sites, model)

        def idle(worker: Any) -> bool:
            return getattr(worker, "state", None) == WorkerState.idle

        # 新 target API 必须精确路由，不允许隐式跨后端 fallback。
        if task.target_id:
            return next((worker for worker in self.workers
                         if idle(worker) and getattr(worker, "target_id", None) == task.target_id
                         and self._model_matches(worker, task.model)), None)

        # 旧 browser site 路由，保留既有跨站重试行为。
        return self._find_legacy_idle_worker(task.site, task.tried_sites or [],
                                             task.allow_fallback_sites, task.model)

    def _find_legacy_idle_worker(self, site: Optional[str], tried: list[str],
                                 allow_fallback_sites: Optional[bool],
                                 model: Optional[str]) -> Optional[Any]:
        browser_workers = [worker for worker in self.workers
                           if getattr(worker, "backend_type", BackendType.webbridge) == BackendType.webbridge]
        allow_switch = self.retry_switch_site if allow_fallback_sites is None else allow_fallback_sites
        matching = lambda worker: (getattr(worker, "state", None) == WorkerState.idle
                                   and self._model_matches(worker, model))
        if not allow_switch:
            return next((worker for worker in browser_workers
                         if matching(worker) and (site is None or worker.site == site)), None)
        if site and site not in tried:
            worker = next((worker for worker in browser_workers
                           if matching(worker) and worker.site == site), None)
            if worker:
                return worker
            if not tried:
                return None
        worker = next((worker for worker in browser_workers
                       if matching(worker) and worker.site not in tried), None)
        return worker or next((worker for worker in browser_workers if matching(worker)), None)

    @staticmethod
    def _model_matches(worker: Any, model: Optional[str]) -> bool:
        return not model or model in (getattr(worker, "model", "") or "")

    # ---- 监控快照 ----

    def routing_snapshot(self) -> dict[str, Any]:
        decisions = self.adaptive_router.rank(self.workers_info())
        total = sum(decision.weight for decision in decisions)
        return {
            "default_mode": self.config.routing.default_mode,
            "window_seconds": self.config.routing.window_seconds,
            "targets": [
                {
                    "target_id": decision.target_id,
                    **decision.details,
                    "weight": round(decision.weight, 6),
                    "probability": round(decision.weight / total, 6) if total else 0,
                }
                for decision in decisions
            ],
        }

    def workers_info(self) -> list[WorkerInfo]:
        return [
            WorkerInfo(
                worker_id=worker.worker_id, site=getattr(worker, "site", None),
                target_id=getattr(worker, "target_id", worker.worker_id),
                backend_type=getattr(worker, "backend_type", BackendType.webbridge),
                model=worker.model, state=worker.state,
                current_task_id=worker.current_task_id, detail=worker.detail,
                done_count=worker.done_count, fail_count=worker.fail_count,
            )
            for worker in self.workers
        ]

    def queue_snapshot(self) -> list[TaskSummary]:
        return [task.to_summary() for task in self.queue.snapshot()]

    async def daemon_health(self) -> tuple[bool, str]:
        if not any(getattr(worker, "backend_type", None) == BackendType.webbridge
                   for worker in self.workers):
            return True, "webbridge not required"
        if self.client is None:
            return False, "webbridge client 未配置"
        return await self.client.healthy()
