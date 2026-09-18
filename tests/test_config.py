"""配置严格校验与单实例锁测试。"""
from __future__ import annotations

import pytest

from app.config import Config, load_config
from app.instance_lock import InstanceAlreadyRunning, InstanceLock


def _valid() -> dict:
    return {"workers": [{"site": "kimi", "model": ""}]}


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
        ],
    })
    assert [target.id for target in config.targets] == [
        "api-one", "api-two", "acp-one", "legacy-webbridge-kimi-1"]


def test_duplicate_target_ids_are_rejected():
    with pytest.raises(Exception, match="唯一"):
        Config.model_validate({"targets": [
            {"id": "same", "type": "acp", "working_directory": "."},
            {"id": "same", "type": "acp", "working_directory": "."},
        ]})


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


def test_instance_lock_excludes_second_owner(tmp_path):
    first = InstanceLock(str(tmp_path / "relay.db"))
    second = InstanceLock(str(tmp_path / "relay.db"))
    first.acquire()
    try:
        with pytest.raises(InstanceAlreadyRunning):
            second.acquire()
    finally:
        first.release()
