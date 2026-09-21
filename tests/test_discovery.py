"""mDNS 发现基础：稳定身份与服务公告参数不依赖 ERP 地址。"""
from __future__ import annotations

import json
import socket

import pytest

from app.discovery import AI_RELAY_SERVICE_TYPE, MdnsPublisher, load_or_create_service_id


def test_service_identity_is_persistent(tmp_path):
    path = tmp_path / "data" / "service-identity.json"
    first = load_or_create_service_id(path)
    second = load_or_create_service_id(path)
    assert first == second
    assert json.loads(path.read_text(encoding="utf-8"))["service_id"] == first


def test_publisher_uses_stable_dns_sd_name_without_erp_address(monkeypatch):
    monkeypatch.setattr(socket, "gethostname", lambda: "test-host")
    publisher = MdnsPublisher(service_id="12345678-1234-1234-1234-123456789abc", port=8600)
    assert publisher.service_name == f"test-host-12345678.{AI_RELAY_SERVICE_TYPE}"
    assert publisher.advertise_address == ""


class _FakeZeroconf:
    """记录注册参数，不触碰真实网络。"""

    def __init__(self, **_kwargs):
        self.registered: list[tuple[object, dict]] = []
        self.unregistered: list[object] = []
        self.closed = False

    def register_service(self, info, **kwargs):
        self.registered.append((info, kwargs))

    def unregister_service(self, info):
        self.unregistered.append(info)

    def close(self):
        self.closed = True


def test_publisher_tolerates_instance_name_conflicts(monkeypatch):
    """快速重启时的名称冲突不能关闭 LAN 发现；身份仍由 TXT service_id 保证。"""
    fake = _FakeZeroconf()
    monkeypatch.setattr("app.discovery.Zeroconf", lambda **_kwargs: fake)
    publisher = MdnsPublisher(
        service_id="12345678-1234-1234-1234-123456789abc",
        port=8600,
        advertise_address="10.0.0.5",
    )

    assert publisher.start() is True
    _info, options = fake.registered[0]
    assert options.get("allow_name_change") is True
    assert options.get("cooperating_responders") is True

    publisher.stop()
    assert fake.closed is True


def test_publisher_reports_failure_with_exception_type(monkeypatch, caplog):
    """zeroconf 的 NonUniqueNameException 无 message，日志需带异常类型。"""

    class _ConflictZeroconf(_FakeZeroconf):
        def register_service(self, info, **kwargs):
            raise RuntimeError("")

    monkeypatch.setattr("app.discovery.Zeroconf", lambda **_kwargs: _ConflictZeroconf())
    publisher = MdnsPublisher(
        service_id="12345678-1234-1234-1234-123456789abc",
        port=8600,
        advertise_address="10.0.0.5",
    )

    with caplog.at_level("WARNING"):
        assert publisher.start() is False
    assert "RuntimeError" in caplog.text


@pytest.mark.asyncio
async def test_async_publisher_helper_never_blocks_the_event_loop():
    """阻塞的 mDNS 注册必须在工作线程执行，否则 ASGI lifespan 会抛 EventLoopBlocked。"""
    import asyncio
    import threading

    from app.discovery import MdnsPublisher, start_publisher_async, stop_publisher_async

    calls: list[tuple[str, int]] = []

    class _RecordingPublisher(MdnsPublisher):
        def start(self) -> bool:
            calls.append(("start", threading.get_ident()))
            asyncio.run(asyncio.sleep(0))  # 模拟阻塞式网络工作
            return True

        def stop(self) -> None:
            calls.append(("stop", threading.get_ident()))

    publisher = _RecordingPublisher(service_id="12345678-1234-1234-1234-123456789abc", port=8600)
    event_loop_thread = threading.get_ident()

    assert await start_publisher_async(publisher) is True
    await stop_publisher_async(publisher)

    assert [name for name, _ in calls] == ["start", "stop"]
    assert all(thread_id != event_loop_thread for _, thread_id in calls)
