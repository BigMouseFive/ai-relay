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


def test_监控页(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "AI 问答中转站" in r.text
