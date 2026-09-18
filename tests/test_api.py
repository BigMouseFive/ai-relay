"""API 层单元测试：TestClient + 假 pool 注入，不碰真实 daemon。"""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.pool import QueueFull
from app.schemas import TaskStatus
from app.store import TaskStore

CONFIG_PATH = str(Path(__file__).resolve().parent.parent / "config.yaml")


class FakePool:
    def __init__(self, store, full=False, daemon_ok=True):
        self.store = store
        self.full = full
        self.daemon_ok = daemon_ok
        self.submitted = []
        self.submitted_targets = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def submit(self, prompt, site):
        if self.full:
            raise QueueFull("队列已满")
        task = self.store.create(prompt, site)
        self.submitted.append((prompt, site))
        return task.task_id

    async def submit_with_metadata(self, prompt, site, *, target_id=None, routing_mode=None,
                                   model=None, allow_fallback_sites=None, idempotency_key=None):
        if self.full:
            raise QueueFull("队列已满")
        task, reused = self.store.create_or_get_idempotent(
            prompt, site, target_id=target_id, model=model,
            allow_fallback_sites=allow_fallback_sites, routing_mode=routing_mode,
            idempotency_key=idempotency_key)
        self.submitted.append((prompt, site))
        self.submitted_targets.append(target_id)
        return task, reused

    def _enqueue_existing(self, task):
        if self.full:
            raise QueueFull("队列已满")

    def cancel_task(self, task_id):
        return False

    @property
    def workers(self):
        return [type("Worker", (), {"site": "kimi"})()]

    def workers_info(self):
        return []

    def queue_snapshot(self):
        return []

    async def daemon_health(self):
        return (True, "ok") if self.daemon_ok else (False, "浏览器扩展未连接")


@pytest.fixture
def store():
    return TaskStore()


@pytest.fixture
def pool(store):
    return FakePool(store)


@pytest.fixture
def client(store, pool):
    app = create_app(CONFIG_PATH, store=store, pool=pool)
    with TestClient(app) as c:
        yield c


def test_提交任务(client, store):
    r = client.post("/api/tasks", json={"prompt": "你好", "site": "kimi"})
    assert r.status_code == 200
    task_id = r.json()["task_id"]
    task = store.get(task_id)
    assert task.prompt == "你好"
    assert task.site == "kimi"
    assert task.status == TaskStatus.queued


def test_提交任务_省略site(client, pool):
    r = client.post("/api/tasks", json={"prompt": "你好"})
    assert r.status_code == 200
    assert pool.submitted[0] == ("你好", None)


def test_提交任务_超长422(client):
    r = client.post("/api/tasks", json={"prompt": "x" * 12001})
    assert r.status_code == 422
    # 边界：恰好 12000 字可提交
    r = client.post("/api/tasks", json={"prompt": "x" * 12000})
    assert r.status_code == 200


def test_提交任务_空prompt422(client):
    r = client.post("/api/tasks", json={"prompt": ""})
    assert r.status_code == 422


def test_提交任务_队列满429(store):
    app = create_app(CONFIG_PATH, store=store, pool=FakePool(store, full=True))
    with TestClient(app) as c:
        r = c.post("/api/tasks", json={"prompt": "你好"})
    assert r.status_code == 429


def test_查询任务(client, store):
    r = client.post("/api/tasks", json={"prompt": "你好", "site": "deepseek"})
    task_id = r.json()["task_id"]
    r = client.get(f"/api/tasks/{task_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["task_id"] == task_id
    assert body["status"] == "queued"
    assert body["site"] == "deepseek"
    assert body["result"] is None


def test_查询任务_各状态(client, store):
    r = client.post("/api/tasks", json={"prompt": "你好"})
    task_id = r.json()["task_id"]
    store.mark_running(task_id)
    store.mark_done(task_id, "答案")
    body = client.get(f"/api/tasks/{task_id}").json()
    assert body["status"] == "done"
    assert body["result"] == "答案"
    assert body["elapsed_seconds"] is not None

    r = client.post("/api/tasks", json={"prompt": "失败任务"})
    tid2 = r.json()["task_id"]
    store.mark_running(tid2)
    store.mark_failed(tid2, "任务超时")
    body = client.get(f"/api/tasks/{tid2}").json()
    assert body["status"] == "failed"
    assert body["error"] == "任务超时"


def test_查询任务_404(client):
    r = client.get("/api/tasks/不存在的id")
    assert r.status_code == 404


def test_list_tasks_filters_and_validates_time_range(client, store):
    first = client.post("/api/tasks", json={"prompt": "筛选关键词"}).json()["task_id"]
    second = client.post("/api/tasks", json={"prompt": "其他任务"}).json()["task_id"]
    store.mark_done(first, "筛选结果")
    store.mark_failed(second, "筛选错误")

    body = client.get("/v1/tasks", params={"q": "筛选关键词"}).json()
    assert body["total"] == 1
    assert body["items"][0]["task_id"] == first
    body = client.get("/v1/tasks", params={"status": "failed"}).json()
    assert body["total"] == 1
    assert body["items"][0]["task_id"] == second
    assert client.get("/v1/tasks", params={"created_after": 2, "created_before": 1}).status_code == 422


def test_stats(client, store):
    r = client.post("/api/tasks", json={"prompt": "s" * 200})
    task_id = r.json()["task_id"]
    store.mark_running(task_id)
    store.mark_done(task_id, "r" * 200)
    body = client.get("/api/stats").json()
    assert body["daemon_ok"] is True
    assert body["queue_size"] == 0
    assert body["workers"] == []
    assert len(body["recent_tasks"]) == 1
    # 摘要截断 120 字符
    assert len(body["recent_tasks"][0]["prompt_preview"]) == 120
    assert len(body["recent_tasks"][0]["result_preview"]) == 120


def test_stats_daemon异常(store):
    app = create_app(CONFIG_PATH, store=store, pool=FakePool(store, daemon_ok=False))
    with TestClient(app) as c:
        body = c.get("/api/stats").json()
    assert body["daemon_ok"] is False
    assert body["daemon_detail"] == "浏览器扩展未连接"


def test_health(client):
    body = client.get("/health").json()
    assert body == {"ok": True, "daemon_ok": True, "daemon_detail": "ok"}


def test_v1_submit_requires_idempotency_key(client):
    r = client.post("/v1/tasks", json={"prompt": "你好"})
    assert r.status_code == 422


def test_v1_submit_is_idempotent_and_emits_events(client):
    headers = {"Idempotency-Key": "api-key-1"}
    payload = {"prompt": "你好", "site": "kimi"}
    first = client.post("/v1/tasks", json=payload, headers=headers)
    second = client.post("/v1/tasks", json=payload, headers=headers)
    assert first.status_code == 202
    assert first.headers["location"] == f"/v1/tasks/{first.json()['task_id']}"
    assert first.json()["reused"] is False
    assert second.json()["task_id"] == first.json()["task_id"]
    assert second.json()["reused"] is True
    events = client.get(f"/v1/tasks/{first.json()['task_id']}/events")
    assert events.status_code == 200
    assert events.json()[0]["event_type"] == "task_created"


def test_v1_target_is_forwarded_to_pool(client, pool):
    r = client.post("/v1/tasks", json={"prompt": "target", "target": "api-one"},
                    headers={"Idempotency-Key": "target-key"})
    assert r.status_code == 202
    assert pool.submitted_targets == ["api-one"]


def test_old_api_rejects_target(client):
    r = client.post("/api/tasks", json={"prompt": "target", "target": "api-one"})
    assert r.status_code == 422


def test_v1_same_key_different_request_conflicts(client):
    headers = {"Idempotency-Key": "api-key-conflict"}
    assert client.post("/v1/tasks", json={"prompt": "一"}, headers=headers).status_code == 202
    r = client.post("/v1/tasks", json={"prompt": "二"}, headers=headers)
    assert r.status_code == 409


def test_v1_cancel_queued_task(client):
    headers = {"Idempotency-Key": "cancel-key"}
    task_id = client.post("/v1/tasks", json={"prompt": "取消"}, headers=headers).json()["task_id"]
    r = client.post(f"/v1/tasks/{task_id}/cancel")
    assert r.status_code == 200
    assert r.json()["cancel_requested"] is True


def test_v1_routing_metrics_without_pool_support_returns_503(client):
    r = client.get("/v1/routing/metrics")
    assert r.status_code == 503


def test_v1_capabilities(client):
    body = client.get("/v1/capabilities").json()
    assert body["max_prompt_chars"] == 12000
    assert body["queue_max_size"] == 100


def test_监控页(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "AI 问答中转站" in r.text
