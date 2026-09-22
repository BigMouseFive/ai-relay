"""ACP/OpenAI-compatible worker 的隔离测试；不执行真实 agent 或远端 HTTP。"""
from __future__ import annotations

import asyncio
import json
import sys

import httpx
import pytest

from app.config import AcpTargetConfig, OpenAICompatibleTargetConfig
from app.external_workers import (
    AcpWorker, ApiResponseError, ExternalOutcomeUnknownError, ExternalResult,
    OpenAICompatibleWorker,
)
from app.schemas import BackendType, TaskStatus, WorkerState
from app.store import TaskStore


def _retry(task, reason, code):
    return False


class FakeApiWorker(OpenAICompatibleWorker):
    """绕过真实 env/client 初始化，只验证统一 worker 的状态持久化。"""

    def __init__(self, target, store, result=None, error=None):
        super().__init__("api-test-1", target, store, 30, _retry)
        self.result = result
        self.error = error

    async def _validate_ready(self):
        self._api_key = "fake"

    async def _execute(self, task):
        if self.error:
            raise self.error
        return self.result


async def test_api_worker_success_persists_target_attempt_metadata(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")
    target = OpenAICompatibleTargetConfig(
        id="api-a", type="openai_compatible", base_url="http://fake.test/v1",
        api_key_env="TEST_OPENAI_KEY", model="fake-model",
    )
    store = TaskStore()
    task = store.create("hello", None, target_id=target.id,
                        backend_type=BackendType.openai_compatible, model=target.model)
    worker = FakeApiWorker(target, store, result=ExternalResult(
        text="world", external_request_id="chatcmpl-test", provider_status_code=200,
        usage={"prompt_tokens": 1, "completion_tokens": 1},
    ))
    await worker.start()
    worker.start_task(task)
    await worker._run_task

    saved = store.get(task.task_id)
    assert saved.status == TaskStatus.done
    assert saved.actual_target_id == "api-a"
    assert saved.actual_backend_type == BackendType.openai_compatible
    assert saved.upstream_request_id == "chatcmpl-test"
    assert saved.usage == {"prompt_tokens": 1, "completion_tokens": 1}
    attempt = saved.to_info().attempts[0]
    assert attempt.target_id == "api-a"
    assert attempt.backend_type == BackendType.openai_compatible
    assert attempt.external_request_id == "chatcmpl-test"
    assert attempt.provider_status_code == 200


async def test_api_worker_outcome_unknown_is_not_retried(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")
    target = OpenAICompatibleTargetConfig(
        id="api-a", type="openai_compatible", base_url="http://fake.test/v1",
        api_key_env="TEST_OPENAI_KEY", model="fake-model",
    )
    store = TaskStore()
    task = store.create("hello", None, target_id=target.id,
                        backend_type=BackendType.openai_compatible, model=target.model)
    worker = FakeApiWorker(target, store, error=ExternalOutcomeUnknownError("read timeout"))
    await worker.start()
    worker.start_task(task)
    await worker._run_task
    assert store.get(task.task_id).status == TaskStatus.outcome_unknown


async def test_openai_worker_missing_key_degrades_without_http_call(monkeypatch):
    monkeypatch.delenv("MISSING_OPENAI_KEY", raising=False)
    target = OpenAICompatibleTargetConfig(
        id="api-missing", type="openai_compatible", base_url="http://fake.test/v1",
        api_key_env="MISSING_OPENAI_KEY", model="fake-model",
    )
    worker = OpenAICompatibleWorker("api-test-1", target, TaskStore(), 30, _retry)
    await worker.start()
    assert worker.state == WorkerState.degraded
    assert "环境变量未设置" in (worker.detail or "")


async def test_openai_model_verification_accepts_published_model(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/models"
        return httpx.Response(200, json={"data": [{"id": "available-model"}]})

    target = OpenAICompatibleTargetConfig(
        id="api-verified", type="openai_compatible", base_url="http://mock.invalid/v1",
        api_key_env="TEST_OPENAI_KEY", model="available-model", verify_model_on_start=True,
    )
    worker = OpenAICompatibleWorker("api-verified-1", target, TaskStore(), 30, _retry)
    worker._client = httpx.AsyncClient(
        base_url=target.base_url,
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer not-a-real-key"},
    )
    worker._api_key = "not-a-real-key"

    await worker._verify_model_available()
    await worker.stop()


async def test_openai_model_verification_rejects_removed_model(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [{"id": "current-model"}]})

    target = OpenAICompatibleTargetConfig(
        id="api-stale", type="openai_compatible", base_url="http://mock.invalid/v1",
        api_key_env="TEST_OPENAI_KEY", model="removed-model", verify_model_on_start=True,
    )
    worker = OpenAICompatibleWorker("api-stale-1", target, TaskStore(), 30, _retry)
    worker._client = httpx.AsyncClient(
        base_url=target.base_url,
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer not-a-real-key"},
    )
    worker._api_key = "not-a-real-key"

    with pytest.raises(Exception, match="API 配置模型不存在: removed-model"):
        await worker._verify_model_available()
    await worker.stop()


async def test_openai_chat_completions_uses_v1_path_and_task_idempotency(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["auth"] = request.headers.get("authorization")
        captured["idempotency"] = request.headers.get("idempotency-key")
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={
            "id": "chatcmpl-mock", "choices": [{"message": {"content": "mock answer"}}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3},
        })

    target = OpenAICompatibleTargetConfig(
        id="api-transport", type="openai_compatible", base_url="http://mock.invalid/v1",
        api_key_env="TEST_OPENAI_KEY", model="fake-model",
    )
    store = TaskStore()
    task = store.create("hello", None, target_id=target.id,
                        backend_type=BackendType.openai_compatible, model=target.model)
    worker = OpenAICompatibleWorker("api-transport-1", target, store, 30, _retry)
    await worker.start()
    assert worker._client is not None
    await worker._client.aclose()
    worker._client = httpx.AsyncClient(
        base_url=target.base_url, transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer not-a-real-key", "Content-Type": "application/json"},
    )
    result = await worker._execute(task)
    await worker.stop()

    assert result.text == "mock answer"
    assert result.external_request_id == "chatcmpl-mock"
    assert captured["path"] == "/v1/chat/completions"
    assert captured["auth"] == "Bearer not-a-real-key"
    assert captured["idempotency"] == task.task_id
    assert captured["body"]["model"] == "fake-model"


async def test_openai_http_200_missing_content_is_retryable_confirmed_failure(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "chatcmpl-missing-content",
            "choices": [{
                "message": {"role": "assistant", "reasoning_content": "thinking only"},
                "finish_reason": "stop",
            }],
        })

    target = OpenAICompatibleTargetConfig(
        id="api-missing-content", type="openai_compatible",
        base_url="http://mock.invalid/v1", api_key_env="TEST_OPENAI_KEY",
        model="fake-model",
    )
    store = TaskStore()
    task = store.create(
        "hello", None, target_id=target.id,
        backend_type=BackendType.openai_compatible, model=target.model,
    )
    worker = OpenAICompatibleWorker("api-missing-content-1", target, store, 30, _retry)
    await worker.start()
    assert worker._client is not None
    await worker._client.aclose()
    worker._client = httpx.AsyncClient(
        base_url=target.base_url,
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer not-a-real-key", "Content-Type": "application/json"},
    )

    with pytest.raises(ApiResponseError) as exc_info:
        await worker._execute(task)
    await worker.stop()

    assert exc_info.value.code == "api_invalid_response"
    assert exc_info.value.retryable is True
    attempt = store.get(task.task_id).to_info().attempts
    # _execute is called directly here, so no attempt row is created by run_task.
    assert attempt == []


async def test_external_worker_cancellation_becomes_cancelled(monkeypatch):
    monkeypatch.setenv("TEST_OPENAI_KEY", "not-a-real-key")
    target = OpenAICompatibleTargetConfig(
        id="api-cancel", type="openai_compatible", base_url="http://fake.test/v1",
        api_key_env="TEST_OPENAI_KEY", model="fake-model",
    )
    store = TaskStore()
    task = store.create("hello", None, target_id=target.id,
                        backend_type=BackendType.openai_compatible, model=target.model)
    store.request_cancel(task.task_id)
    worker = FakeApiWorker(target, store, result=ExternalResult(text="should not run"))
    await worker.start()
    worker.start_task(task)
    await worker._run_task
    assert store.get(task.task_id).status == TaskStatus.cancelled


def test_openai_response_content_parser():
    assert OpenAICompatibleWorker._content_to_text("hello") == "hello"
    assert OpenAICompatibleWorker._content_to_text([{"text": "a"}, "b"]) == "ab"
    with pytest.raises(TypeError):
        OpenAICompatibleWorker._content_to_text({"text": "bad"})


async def test_acp_missing_working_directory_degrades_without_process():
    target = AcpTargetConfig(
        id="acp-missing-dir", type="acp", command="agent",
        working_directory="/definitely/not/a/workspace",
    )
    worker = AcpWorker("acp-test-1", target, TaskStore(), 30, _retry)
    await worker.start()
    assert worker.state == WorkerState.degraded
    assert "工作目录不存在" in (worker.detail or "")


async def test_acp_missing_configured_key_degrades_without_process(tmp_path, monkeypatch):
    monkeypatch.delenv("MISSING_CURSOR_KEY", raising=False)
    target = AcpTargetConfig(
        id="acp-missing-key", type="acp", command="agent",
        working_directory=str(tmp_path), api_key_env="MISSING_CURSOR_KEY",
    )
    worker = AcpWorker("acp-test-1", target, TaskStore(), 30, _retry)
    await worker.start()
    assert worker.state == WorkerState.degraded
    assert "环境变量未设置" in (worker.detail or "")


async def test_acp_worker_cleans_process_group_after_agent_exits_with_inherited_pipe(tmp_path):
    script = tmp_path / "fake_agent_with_helper.py"
    script.write_text(
        "import os, sys, time\n"
        "pid = os.fork()\n"
        "if pid == 0:\n"
        "    time.sleep(30)\n"
        "    os._exit(0)\n"
        "os.write(1, b'answer-from-parent\\n')\n"
        "os._exit(0)\n",
        encoding="utf-8",
    )
    target = AcpTargetConfig(
        id="acp-helper", type="acp", command=sys.executable, args=[str(script)],
        working_directory=str(tmp_path), verify_auth_on_start=False,
        pipe_drain_after_exit_seconds=0.05, graceful_shutdown_seconds=0.2,
    )
    store = TaskStore()
    task = store.create("hello", None, target_id=target.id, backend_type=BackendType.acp)
    worker = AcpWorker("acp-helper-1", target, store, 5, _retry)
    await worker.start()
    started = asyncio.get_running_loop().time()
    worker.start_task(task)
    assert worker._run_task is not None
    await worker._run_task
    elapsed = asyncio.get_running_loop().time() - started

    saved = store.get(task.task_id)
    assert saved is not None
    assert saved.status == TaskStatus.done, (saved.error_code, saved.error)
    assert saved.result == "answer-from-parent"
    assert elapsed < 2.0
