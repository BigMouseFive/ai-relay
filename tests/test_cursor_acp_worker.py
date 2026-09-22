"""Integration tests for the real Cursor ACP worker using a local fake agent."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from app.config import CursorAcpTargetConfig
from app.external_workers import CursorAcpWorker
from app.schemas import BackendType, TaskStatus
from app.store import TaskStore


FAKE_AGENT = r'''
import json
import sys

counter = 0
for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    request_id = request.get("id")
    params = request.get("params") or {}
    if method == "initialize":
        print(json.dumps({"jsonrpc":"2.0", "id":request_id, "result":{
            "protocolVersion":1,
            "agentCapabilities":{"sessionCapabilities":{"close":{}}},
            "agentInfo":{"name":"fake-cursor", "version":"test"}
        }}), flush=True)
    elif method == "session/new":
        counter += 1
        print(json.dumps({"jsonrpc":"2.0", "id":request_id,
                          "result":{"sessionId":f"session-{counter}"}}), flush=True)
    elif method == "session/prompt":
        session_id = params["sessionId"]
        text = params["prompt"][0]["text"]
        print(json.dumps({"jsonrpc":"2.0", "method":"session/update",
                          "params":{"sessionId":session_id,"update":{
                            "sessionUpdate":"agent_message_chunk",
                            "content":{"type":"text", "text":"answer:" + text}}}}), flush=True)
        print(json.dumps({"jsonrpc":"2.0", "id":request_id,
                          "result":{"stopReason":"end_turn"}}), flush=True)
    elif method == "session/close":
        print(json.dumps({"jsonrpc":"2.0", "id":request_id, "result":{}}), flush=True)
'''


@pytest.fixture
def fake_agent(tmp_path: Path) -> list[str]:
    path = tmp_path / "fake_cursor_agent.py"
    path.write_text(FAKE_AGENT, encoding="utf-8")
    return [sys.executable, str(path)]


def _retry(task, reason, code):
    return False


@pytest.mark.asyncio
async def test_cursor_acp_worker_reuses_connection_and_creates_session_per_task(
    fake_agent: list[str], tmp_path: Path,
):
    target = CursorAcpTargetConfig(
        id="cursor-real", type="cursor_acp", command=fake_agent[0],
        args=fake_agent[1:], working_directory=str(tmp_path),
        request_timeout_seconds=5,
    )
    store = TaskStore()
    worker = CursorAcpWorker("cursor-real-1", target, store, 30, _retry)
    await worker.start()
    assert worker.state.value == "idle"
    assert worker._connection is not None
    process = worker._connection.process
    assert process is not None
    try:
        task_a = store.create("hello", None, target_id=target.id,
                              backend_type=BackendType.cursor_acp, model="")
        worker.start_task(task_a)
        assert worker._run_task is not None
        await worker._run_task
        saved_a = store.get(task_a.task_id)
        assert saved_a is not None
        assert saved_a.status == TaskStatus.done
        assert saved_a.result == "answer:hello"
        assert saved_a.upstream_request_id == "session-1"

        task_b = store.create("world", None, target_id=target.id,
                              backend_type=BackendType.cursor_acp, model="")
        worker.start_task(task_b)
        assert worker._run_task is not None
        await worker._run_task
        saved_b = store.get(task_b.task_id)
        assert saved_b is not None
        assert saved_b.status == TaskStatus.done
        assert saved_b.result == "answer:world"
        assert saved_b.upstream_request_id == "session-2"
        assert worker._connection is not None
        assert worker._connection.process is process
    finally:
        await worker.stop()
