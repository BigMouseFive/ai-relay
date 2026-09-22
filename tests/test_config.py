"""配置严格校验与单实例锁测试。"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Config, CursorAcpTargetConfig, load_config
from app.instance_lock import InstanceAlreadyRunning, InstanceLock


def _valid() -> dict:
    return {"workers": [{"site": "kimi", "model": ""}]}


def test_default_example_is_acp_only_and_never_requires_webbridge():
    path = Path(__file__).resolve().parents[1] / "config.example.yaml"
    config = load_config(path)

    assert [(target.id, target.type, target.count) for target in config.targets] == [
        ("cursor-agent-ai-relay", "acp", 2)
    ]
    assert config.workers == []
    assert config.routing.task_policies["listing_copy"].allowed_targets == [
        "cursor-agent-ai-relay"
    ]
    assert all(target.type != "webbridge" for target in config.targets)


def test_config_rejects_unknown_field():
    value = _valid()
    value["task"] = {"max_retriess": 2}
    with pytest.raises(Exception, match="max_retriess"):
        Config.model_validate(value)


def test_config_rejects_invalid_timeout_relationship():
    value = _valid()
    value["task"] = {"timeout_seconds": 10, "hard_timeout_seconds": 11}
    with pytest.raises(Exception, match="hard_timeout_seconds"):
        Config.model_validate(value)


def test_load_config_requires_worker(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("server:\n  port: 8600\n", encoding="utf-8")
    with pytest.raises(ValueError, match="至少"):
        load_config(path)


def test_multiple_targets_and_legacy_workers_are_normalized():
    config = Config.model_validate({
        "workers": [{"site": "kimi", "model": ""}],
        "targets": [
            {"id": "api-one", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "KEY_ONE", "model": "m1", "count": 2},
            {"id": "api-two", "type": "openai_compatible", "base_url": "http://api.invalid/v1", "api_key_env": "KEY_TWO", "model": "m2", "count": 3},
            {"id": "acp-one", "type": "acp", "working_directory": ".", "count": 2},
            {"id": "cursor-acp-one", "type": "cursor_acp", "working_directory": ".", "count": 2},
        ],
    })
    assert [target.id for target in config.targets] == [
        "api-one", "api-two", "acp-one", "cursor-acp-one", "legacy-webbridge-kimi-1"]
    assert isinstance(config.targets[3], CursorAcpTargetConfig)
    assert config.targets[3].args == ["acp"]


def test_duplicate_target_ids_are_rejected():
    with pytest.raises(Exception, match="唯一"):
        Config.model_validate({"targets": [
            {"id": "same", "type": "acp", "working_directory": "."},
            {"id": "same", "type": "acp", "working_directory": "."},
        ]})


def test_task_policy_target_ids_are_validated_after_target_normalization():
    config = Config.model_validate({
        "workers": [{"site": "kimi", "model": ""}],
        "routing": {"task_policies": {
            "listing_copy": {
                "allowed_targets": ["legacy-webbridge-kimi-1"],
                "max_in_flight_per_target": 1,
            },
        }},
    })
    assert config.routing.task_policies["listing_copy"].allowed_targets == [
        "legacy-webbridge-kimi-1"]

    with pytest.raises(Exception, match="未知 target id.*missing-target"):
        Config.model_validate({
            "workers": [{"site": "kimi", "model": ""}],
            "routing": {"task_policies": {
                "listing_copy": {"allowed_targets": ["missing-target"]},
            }},
        })


def test_environment_file_loads_missing_values_without_overriding_process_env(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("environment_file: .env\nworkers:\n  - site: kimi\n    model: ''\n", encoding="utf-8")
    (tmp_path / ".env").write_text("TEST_RELAY_KEY='from-file'\n", encoding="utf-8")
    monkeypatch.delenv("TEST_RELAY_KEY", raising=False)
    load_config(path)
    assert __import__("os").environ["TEST_RELAY_KEY"] == "from-file"
    monkeypatch.setenv("TEST_RELAY_KEY", "from-process")
    load_config(path)
    assert __import__("os").environ["TEST_RELAY_KEY"] == "from-process"


def test_discovery_config_defaults_and_rejects_unknown_field():
    config = Config.model_validate(_valid())
    assert config.discovery.enabled is True
    assert config.discovery.identity_path == "data/service-identity.json"
    with pytest.raises(Exception, match="unknown"):
        Config.model_validate({**_valid(), "discovery": {"unknown": True}})


def test_instance_lock_excludes_second_owner(tmp_path):
    first = InstanceLock(str(tmp_path / "relay.db"))
    second = InstanceLock(str(tmp_path / "relay.db"))
    first.acquire()
    try:
        with pytest.raises(InstanceAlreadyRunning):
            second.acquire()
    finally:
        first.release()
