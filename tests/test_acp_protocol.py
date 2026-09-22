"""Protocol-level tests for the real ACP stdio client."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from app.acp_protocol import AcpConnection, AcpProtocolError, AcpRpcError


FAKE_AGENT = r'''
import json
import sys

for raw in sys.stdin:
    request = json.loads(raw)
    method = request.get("method")
    request_id = request.get("id")
    params = request.get("params") or {}
    if method == "initialize":
        print(json.dumps({
            "jsonrpc": "2.0", "id": request_id,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {"sessionCapabilities": {"close": {}}},
                "agentInfo": {"name": "fake-agent", "version": "1"},
            },
        }), flush=True)
    elif method == "session/new":
        print(json.dumps({
            "jsonrpc": "2.0", "id": request_id,
            "result": {"sessionId": "sess-test"},
        }), flush=True)
    elif method == "session/prompt":
        session_id = params["sessionId"]
        print(json.dumps({
            "jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": session_id, "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hello "},
            }},
        }), flush=True)
        print(json.dumps({
            "jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": session_id, "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "world"},
            }},
        }), flush=True)
        print(json.dumps({
            "jsonrpc": "2.0", "id": request_id,
            "result": {"stopReason": "end_turn"},
        }), flush=True)
    elif method == "session/close":
        print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": {}}), flush=True)
    else:
        print(json.dumps({
            "jsonrpc": "2.0", "id": request_id,
            "error": {"code": -32601, "message": "unknown"},
        }), flush=True)
'''


@pytest.fixture
def fake_agent(tmp_path: Path) -> list[str]:
    path = tmp_path / "fake_acp_agent.py"
    path.write_text(FAKE_AGENT, encoding="utf-8")
    return [sys.executable, str(path)]


@pytest.mark.asyncio
async def test_acp_connection_handshake_and_notifications(fake_agent, tmp_path):
    updates: list[dict] = []

    async def on_notification(method: str, params: dict) -> None:
        updates.append({"method": method, "params": params})

    connection = AcpConnection(
        fake_agent, cwd=str(tmp_path), notification_handler=on_notification,
    )
    await connection.start()
    try:
        initialized = await connection.initialize()
        assert initialized["protocolVersion"] == 1
        session = await connection.request("session/new", {
            "cwd": str(tmp_path), "mcpServers": [],
        })
        result = await connection.request("session/prompt", {
            "sessionId": session["sessionId"],
            "prompt": [{"type": "text", "text": "test"}],
        })
        await asyncio.sleep(0)
        assert result == {"stopReason": "end_turn"}
        assert [item["params"]["update"]["content"]["text"] for item in updates] == [
            "hello ", "world"]
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_acp_connection_returns_json_rpc_errors(fake_agent, tmp_path):
    connection = AcpConnection(fake_agent, cwd=str(tmp_path))
    await connection.start()
    try:
        with pytest.raises(AcpRpcError) as error:
            await connection.request("unknown")
        assert error.value.code == -32601
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_acp_connection_rejects_non_json_stdout(tmp_path):
    path = tmp_path / "bad_agent.py"
    path.write_text("print('not-json', flush=True)\n", encoding="utf-8")
    connection = AcpConnection([sys.executable, str(path)], cwd=str(tmp_path))
    await connection.start()
    try:
        with pytest.raises(AcpProtocolError):
            await connection.request("initialize")
    finally:
        await connection.close()
