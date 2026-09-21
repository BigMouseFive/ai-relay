"""基于近期持久化 attempt 指标的可解释自适应 target 路由。"""
from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from . import db as task_db
from .config import RoutingConfig
from .schemas import WorkerInfo, WorkerState


@dataclass(frozen=True)
class TargetDecision:
    target_id: str
    weight: float
    details: dict[str, Any]


class RandomSource(Protocol):
    def random(self) -> float: ...


class AdaptiveRouter:
    """用 Beta 平滑成功率、失败/未知惩罚、延迟与容量得到概率权重。

    设计目标不是让低样本 target 立刻被永久淘汰：每个健康 target 都保留
    ``exploration_weight`` 概率，以持续获得新样本并适应 provider 状态变化。
    """

    def __init__(self, config: RoutingConfig, *, rng: RandomSource | None = None):
        self.config = config
        self._rng = rng or random.Random()

    def rank(
        self, workers: Iterable[WorkerInfo], *, require_idle: bool = False,
        allowed_targets: Iterable[str] | None = None,
        max_in_flight_per_target: int | None = None,
    ) -> list[TargetDecision]:
        grouped: dict[str, list[WorkerInfo]] = {}
        for worker in workers:
            grouped.setdefault(worker.target_id, []).append(worker)
        allowed = set(allowed_targets) if allowed_targets is not None else None

        # 完全 degraded 的 target 不参与自动选择；starting/idle/busy 都可参与。
        eligible = {
            target_id: slots
            for target_id, slots in grouped.items()
            if (allowed is None or target_id in allowed)
            and any(slot.state != WorkerState.degraded for slot in slots)
            and (not require_idle or any(slot.state == WorkerState.idle for slot in slots))
            and (max_in_flight_per_target is None
                 or sum(slot.state == WorkerState.busy for slot in slots)
                 < max_in_flight_per_target)
        }
        if not eligible:
            raise ValueError("没有可用于自适应路由的健康 target")
        metrics = task_db.target_attempt_metrics(time.time() - self.config.window_seconds)
        return [self._score(target_id, slots, metrics.get(target_id, {}))
                for target_id, slots in sorted(eligible.items())]

    def choose(
        self, workers: Iterable[WorkerInfo], *, require_idle: bool = False,
        allowed_targets: Iterable[str] | None = None,
        max_in_flight_per_target: int | None = None,
        task_policy: str | None = None,
    ) -> TargetDecision:
        allowed = tuple(allowed_targets) if allowed_targets is not None else None
        decisions = self.rank(
            workers, require_idle=require_idle, allowed_targets=allowed,
            max_in_flight_per_target=max_in_flight_per_target)
        total = sum(decision.weight for decision in decisions)
        selected = decisions[-1]
        threshold = self._rng.random() * total
        cumulative = 0.0
        for decision in decisions:
            cumulative += decision.weight
            if threshold <= cumulative:
                selected = decision
                break

        details = {
            "mode": "adaptive",
            "window_seconds": self.config.window_seconds,
            "selected_target": selected.target_id,
            "selection_weight": round(selected.weight, 6),
            "candidates": [
                {"target_id": decision.target_id, **decision.details,
                 "weight": round(decision.weight, 6)}
                for decision in decisions
            ],
        }
        if task_policy is not None:
            details.update({
                "task_policy": task_policy,
                "allowed_targets": list(allowed or ()),
                "max_in_flight_per_target": max_in_flight_per_target,
            })
        return TargetDecision(selected.target_id, selected.weight, details)

    def _score(self, target_id: str, slots: list[WorkerInfo], metrics: dict[str, float]) -> TargetDecision:
        attempts = metrics.get("attempts", 0.0)
        successes = metrics.get("successes", 0.0)
        failures = metrics.get("failures", 0.0)
        unknowns = metrics.get("unknowns", 0.0)
        avg_latency = metrics.get("avg_latency_seconds", 0.0)

        # Beta(2, 2) 先验：0 样本 target 的平滑成功率为 0.5，不会过度自信。
        success_rate = (successes + 2.0) / (attempts + 4.0)
        failure_rate = failures / attempts if attempts else 0.0
        unknown_rate = unknowns / attempts if attempts else 0.0
        latency_score = (math.exp(-avg_latency / self.config.latency_reference_seconds)
                         if avg_latency > 0 else 1.0)
        idle = sum(slot.state == WorkerState.idle for slot in slots)
        total = len(slots)
        capacity_score = 0.5 + 0.5 * ((idle + 1) / (total + 1))
        reliability = max(
            0.05,
            success_rate * (1 - self.config.failure_penalty * failure_rate)
            * (1 - self.config.outcome_unknown_penalty * unknown_rate),
        )

        # 小样本时增加一点探索，至少保留 floor 权重；样本数达标后主要靠性能。
        exploration = self.config.exploration_weight * max(
            0.0, 1.0 - attempts / self.config.min_samples)
        weight = max(0.0001, reliability * latency_score * capacity_score + exploration)
        details = {
            "attempts": int(attempts),
            "success_rate": round(success_rate, 4),
            "failure_rate": round(failure_rate, 4),
            "outcome_unknown_rate": round(unknown_rate, 4),
            "avg_latency_seconds": round(avg_latency, 3),
            "idle_slots": idle,
            "total_slots": total,
            "reliability_score": round(reliability, 6),
            "latency_score": round(latency_score, 6),
            "capacity_score": round(capacity_score, 6),
            "exploration_bonus": round(exploration, 6),
        }
        return TargetDecision(target_id, weight, details)
