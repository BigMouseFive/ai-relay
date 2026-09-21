"""自适应 target 路由：近期质量、延迟、容量和探索的确定性测试。"""
from __future__ import annotations

import asyncio
import random
import time

from app import db as task_db
from app.adaptive_router import AdaptiveRouter, TargetDecision
from app.config import Config, RoutingConfig
from app.pool import WorkerPool
from app.schemas import BackendType, WorkerInfo, WorkerState
from app.store import TaskStore


class FixedRng:
    def __init__(self, value: float):
        self.value = value

    def random(self) -> float:
        return self.value


def _worker(target_id: str, *, state=WorkerState.idle) -> WorkerInfo:
    return WorkerInfo(
        worker_id=f"w-{target_id}", target_id=target_id,
        backend_type=BackendType.openai_compatible, model="m", state=state,
    )


class DispatchWorker:
    def __init__(self, worker_id: str, target_id: str, store: TaskStore, *, state=WorkerState.idle):
        self.worker_id = worker_id
        self.target_id = target_id
        self.backend_type = BackendType.openai_compatible
        self.site = None
        self.model = "m"
        self.state = state
        self.current_task_id = None
        self.detail = None
        self.done_count = 0
        self.fail_count = 0
        self.store = store
        self.ran: list[str] = []

    async def start(self):
        pass

    async def stop(self):
        pass

    def start_task(self, task):
        self.current_task_id = task.task_id
        self.state = WorkerState.busy
        self.ran.append(task.task_id)
        self.store.mark_done(task.task_id, "{}")
        self.done_count += 1
        self.current_task_id = None
        self.state = WorkerState.idle


def _policy_config(*, allowed_count: int = 1) -> Config:
    return Config.model_validate({
        "routing": {
            "default_mode": "adaptive",
            "task_policies": {
                "listing_copy": {
                    "allowed_targets": ["allowed"],
                    "max_in_flight_per_target": 1,
                },
            },
        },
        "targets": [
            {"id": "allowed", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_ALLOWED", "model": "m", "count": allowed_count},
            {"id": "disallowed", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_DISALLOWED", "model": "m"},
        ],
    })


def _response_format(name: str) -> dict:
    return {"type": "json_schema", "name": name, "schema": {"type": "object"}}


def test_slow_or_unreliable_target_gets_lower_weight(monkeypatch):
    router = AdaptiveRouter(
        RoutingConfig(exploration_weight=0, min_samples=5, latency_reference_seconds=60),
        rng=FixedRng(0.1),
    )
    monkeypatch.setattr("app.adaptive_router.task_db.target_attempt_metrics", lambda since: {
        "fast-api": {"attempts": 20, "successes": 19, "failures": 1,
                     "unknowns": 0, "avg_latency_seconds": 8},
        "slow-kimi": {"attempts": 20, "successes": 10, "failures": 5,
                      "unknowns": 5, "avg_latency_seconds": 300},
    })
    fast = router._score("fast-api", [_worker("fast-api")],
                         {"attempts": 20, "successes": 19, "failures": 1,
                          "unknowns": 0, "avg_latency_seconds": 8})
    slow = router._score("slow-kimi", [_worker("slow-kimi")],
                         {"attempts": 20, "successes": 10, "failures": 5,
                          "unknowns": 5, "avg_latency_seconds": 300})
    assert fast.weight > slow.weight * 10
    selected = router.choose([_worker("fast-api"), _worker("slow-kimi")])
    assert selected.target_id == "fast-api"
    assert selected.details["candidates"][1]["avg_latency_seconds"] == 300.0


def test_low_sample_target_keeps_exploration_weight(monkeypatch):
    router = AdaptiveRouter(
        RoutingConfig(exploration_weight=0.2, min_samples=10), rng=FixedRng(0.5))
    monkeypatch.setattr("app.adaptive_router.task_db.target_attempt_metrics", lambda since: {
        "established": {"attempts": 50, "successes": 49, "failures": 1,
                        "unknowns": 0, "avg_latency_seconds": 10},
        "new-target": {"attempts": 0, "successes": 0, "failures": 0,
                       "unknowns": 0, "avg_latency_seconds": 0},
    })
    new = router._score("new-target", [_worker("new-target")], {
        "attempts": 0, "successes": 0, "failures": 0,
        "unknowns": 0, "avg_latency_seconds": 0,
    })
    assert new.details["exploration_bonus"] == 0.2
    assert new.weight > 0


def test_routing_snapshot_exposes_probability_without_prompt_data(monkeypatch):
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive", "exploration_weight": 0},
        "targets": [
            {"id": "api-a", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_A", "model": "m"},
            {"id": "api-b", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_B", "model": "m"},
        ],
    })
    pool = WorkerPool(config, TaskStore(), client=None)
    monkeypatch.setattr("app.adaptive_router.task_db.target_attempt_metrics", lambda since: {})
    snapshot = pool.routing_snapshot()
    assert snapshot["default_mode"] == "adaptive"
    assert {item["target_id"] for item in snapshot["targets"]} == {"api-a", "api-b"}
    assert abs(sum(item["probability"] for item in snapshot["targets"]) - 1) < 0.00001
    assert all("prompt" not in item for item in snapshot["targets"])


def test_degraded_target_is_excluded(monkeypatch):
    router = AdaptiveRouter(RoutingConfig(), rng=FixedRng(0))
    monkeypatch.setattr("app.adaptive_router.task_db.target_attempt_metrics", lambda since: {})
    selected = router.choose([_worker("healthy"), _worker("bad", state=WorkerState.degraded)])
    assert selected.target_id == "healthy"


async def test_listing_copy_never_routes_to_disallowed_target():
    config = _policy_config()
    store = TaskStore()
    pool = WorkerPool(config, store, client=None)
    allowed = DispatchWorker("w-allowed", "allowed", store)
    disallowed = DispatchWorker("w-disallowed", "disallowed", store)
    pool.workers = [allowed, disallowed]

    task, _ = await pool.submit_with_metadata(
        "listing", response_format=_response_format("listing_copy"))
    worker, decision = pool._find_dispatch_worker(task)

    assert worker is allowed
    assert decision is not None
    assert decision["task_policy"] == "listing_copy"
    assert decision["allowed_targets"] == ["allowed"]
    assert {item["target_id"] for item in decision["candidates"]} == {"allowed"}


async def test_listing_copy_waits_while_allowed_target_busy_then_dispatches():
    config = _policy_config(allowed_count=2)
    store = TaskStore()
    pool = WorkerPool(config, store, client=None, dispatch_interval=0.01)
    busy_allowed = DispatchWorker(
        "w-allowed-busy", "allowed", store, state=WorkerState.busy)
    idle_allowed = DispatchWorker("w-allowed-idle", "allowed", store)
    disallowed = DispatchWorker("w-disallowed", "disallowed", store)
    pool.workers = [busy_allowed, idle_allowed, disallowed]

    task, _ = await pool.submit_with_metadata(
        "listing", response_format=_response_format("listing_copy"))
    assert task.routing_decision is not None
    assert task.routing_decision["allowed_targets"] == ["allowed"]
    await pool.start()
    try:
        await asyncio.sleep(0.05)
        waiting = store.get(task.task_id)
        assert waiting is not None
        assert waiting.status.value == "queued"
        assert pool.queue.qsize() == 1
        assert idle_allowed.ran == []
        assert disallowed.ran == []

        busy_allowed.state = WorkerState.idle
        for _ in range(50):
            current = store.get(task.task_id)
            assert current is not None
            if current.status.value == "done":
                break
            await asyncio.sleep(0.01)

        completed = store.get(task.task_id)
        assert completed is not None
        assert completed.status.value == "done"
        assert completed.actual_target_id == "allowed"
        assert disallowed.ran == []
        assert completed.routing_decision is not None
        assert completed.routing_decision["task_policy"] == "listing_copy"
        assert completed.routing_decision["allowed_targets"] == ["allowed"]
    finally:
        await pool.stop()


async def test_other_contracts_still_use_full_adaptive_pool():
    config = _policy_config()
    store = TaskStore()
    pool = WorkerPool(config, store, client=None)
    pool.adaptive_router = AdaptiveRouter(config.routing, rng=FixedRng(0.99))
    allowed = DispatchWorker("w-allowed", "allowed", store)
    disallowed = DispatchWorker("w-disallowed", "disallowed", store)
    pool.workers = [allowed, disallowed]

    task, _ = await pool.submit_with_metadata(
        "other", response_format=_response_format("other_contract"))
    worker, decision = pool._find_dispatch_worker(task)

    assert worker is disallowed
    assert decision is not None
    assert {item["target_id"] for item in decision["candidates"]} == {
        "allowed", "disallowed"}
    assert "task_policy" not in decision


async def test_adaptive_submission_defers_target_selection_until_dispatch():
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-fast", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_FAST", "model": "m", "count": 1},
            {"id": "api-slow", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_SLOW", "model": "m", "count": 1},
        ],
    })
    store = TaskStore()
    pool = WorkerPool(config, store, client=None)

    task, reused = await pool.submit_with_metadata("adaptive", routing_mode="adaptive")
    assert reused is False
    assert task.target_id is None
    assert task.routing_mode == "adaptive"
    assert task.routing_decision is None
    assert not any(event["event_type"] == "adaptive_target_selected" for event in task_db.list_events(task.task_id))


async def test_adaptive_idempotency_preserves_logical_task_without_fixing_target():
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-a", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_A", "model": "m"},
            {"id": "api-b", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_B", "model": "m"},
        ],
    })
    pool = WorkerPool(config, TaskStore(), client=None)
    created, reused = await pool.submit_with_metadata("same", routing_mode="adaptive", idempotency_key="adaptive-key")
    assert reused is False and created.target_id is None
    repeated, reused = await pool.submit_with_metadata("same", routing_mode="adaptive", idempotency_key="adaptive-key")
    assert reused is True
    assert repeated.task_id == created.task_id
    assert repeated.target_id is None


async def test_adaptive_retry_can_reselect_a_different_target(monkeypatch):
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-a", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_A", "model": "m"},
            {"id": "api-b", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_B", "model": "m"},
        ],
    })
    store = TaskStore()
    pool = WorkerPool(config, store, client=None)
    for target_id in ("api-a", "api-b"):
        pool.workers.append(type("Worker", (), {
            "worker_id": f"w-{target_id}", "target_id": target_id,
            "backend_type": BackendType.openai_compatible, "site": None, "model": "m",
            "state": WorkerState.idle, "current_task_id": None, "detail": None,
            "done_count": 0, "fail_count": 0,
        })())
    first = TargetDecision("api-a", 1, {"mode": "adaptive", "selected_target": "api-a", "candidates": []})
    second = TargetDecision("api-b", 1, {"mode": "adaptive", "selected_target": "api-b", "candidates": []})
    decisions = iter((first, second))
    monkeypatch.setattr(pool.adaptive_router, "choose", lambda workers, require_idle=False: next(decisions))

    task, _ = await pool.submit_with_metadata("same", routing_mode="adaptive", retry_policy_max_retries=2)
    worker, decision = pool._find_dispatch_worker(task)
    assert worker is not None
    assert decision is not None
    assert worker.target_id == "api-a"
    assert decision["selected_target"] == "api-a"
    store.mark_running(task.task_id, worker_id=worker.worker_id, actual_target_id=worker.target_id,
                       actual_backend_type=worker.backend_type)
    store.start_attempt(task.task_id, worker.worker_id, None, "m", target_id=worker.target_id,
                        backend_type=worker.backend_type)
    assert pool._on_retry_needed(task, "返回非法 JSON", "response_invalid_json") is True

    retried = store.get(task.task_id)
    assert retried is not None
    worker, decision = pool._find_dispatch_worker(retried)
    assert worker is not None
    assert decision is not None
    assert worker.target_id == "api-b"
    assert decision["selected_target"] == "api-b"


async def test_explicit_target_overrides_adaptive_default(monkeypatch):
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-a", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_A", "model": "m"},
            {"id": "api-b", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_B", "model": "m"},
        ],
    })
    pool = WorkerPool(config, TaskStore(), client=None)
    monkeypatch.setattr(pool.adaptive_router, "choose", lambda workers: (_ for _ in ()).throw(AssertionError("不应调用自动路由")))
    task, _ = await pool.submit_with_metadata("explicit", target_id="api-b")
    assert task.target_id == "api-b"
    assert task.routing_mode == "target"


def test_target_attempt_metrics_include_latency_and_unknown():
    store = TaskStore()
    now = time.time()
    done = store.create("done", None, target_id="api-a", backend_type=BackendType.openai_compatible)
    store.mark_running(done.task_id, actual_target_id="api-a", actual_backend_type=BackendType.openai_compatible)
    store.start_attempt(done.task_id, "w", None, "m", target_id="api-a", backend_type=BackendType.openai_compatible)
    store.mark_done(done.task_id, "ok")
    unknown = store.create("unknown", None, target_id="api-a", backend_type=BackendType.openai_compatible)
    store.mark_running(unknown.task_id, actual_target_id="api-a", actual_backend_type=BackendType.openai_compatible)
    store.start_attempt(unknown.task_id, "w", None, "m", target_id="api-a", backend_type=BackendType.openai_compatible)
    store.mark_outcome_unknown(unknown.task_id, "timeout")
    conn = task_db._required_conn()
    conn.execute("UPDATE task_attempts SET started_at = ?, finished_at = ? WHERE task_id = ?", (now - 20, now, done.task_id))
    conn.commit()

    metric = task_db.target_attempt_metrics(now - 60)["api-a"]
    assert metric["attempts"] == 2
    assert metric["successes"] == 1
    assert metric["unknowns"] == 1
    assert metric["avg_latency_seconds"] >= 10
