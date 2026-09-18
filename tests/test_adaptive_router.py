"""自适应 target 路由：近期质量、延迟、容量和探索的确定性测试。"""
from __future__ import annotations

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


async def test_adaptive_submission_persists_decision_and_event(monkeypatch):
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-fast", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_FAST", "model": "m", "count": 1},
            {"id": "api-slow", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_SLOW", "model": "m", "count": 1},
        ],
    })
    store = TaskStore()
    pool = WorkerPool(config, store, client=None)
    decision = TargetDecision("api-fast", 0.9, {
        "mode": "adaptive", "selected_target": "api-fast", "selection_weight": 0.9,
        "candidates": [],
    })
    monkeypatch.setattr(pool.adaptive_router, "choose", lambda workers: decision)

    task, reused = await pool.submit_with_metadata("adaptive", routing_mode="adaptive")
    assert reused is False
    assert task.target_id == "api-fast"
    assert task.routing_mode == "adaptive"
    assert task.routing_decision == decision.details
    events = task_db.list_events(task.task_id)
    assert any(event["event_type"] == "adaptive_target_selected" for event in events)


async def test_adaptive_idempotency_reuses_first_target_when_scores_change(monkeypatch):
    config = Config.model_validate({
        "routing": {"default_mode": "adaptive"},
        "targets": [
            {"id": "api-a", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_A", "model": "m"},
            {"id": "api-b", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "TEST_B", "model": "m"},
        ],
    })
    pool = WorkerPool(config, TaskStore(), client=None)
    first = TargetDecision("api-a", 0.9, {"mode": "adaptive", "selected_target": "api-a", "selection_weight": 0.9, "candidates": []})
    second = TargetDecision("api-b", 0.9, {"mode": "adaptive", "selected_target": "api-b", "selection_weight": 0.9, "candidates": []})
    monkeypatch.setattr(pool.adaptive_router, "choose", lambda workers: first)
    created, reused = await pool.submit_with_metadata("same", routing_mode="adaptive", idempotency_key="adaptive-key")
    assert reused is False and created.target_id == "api-a"
    monkeypatch.setattr(pool.adaptive_router, "choose", lambda workers: second)
    repeated, reused = await pool.submit_with_metadata("same", routing_mode="adaptive", idempotency_key="adaptive-key")
    assert reused is True
    assert repeated.task_id == created.task_id
    assert repeated.target_id == "api-a"


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
