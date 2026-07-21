"""全局测试夹具：每个测试使用独立的临时 SQLite 库。"""
from __future__ import annotations

import pytest

from app import db as task_db


@pytest.fixture(autouse=True)
def _tmp_task_db(tmp_path):
    task_db.init(str(tmp_path / "test-tasks.db"))
    yield
    task_db.close()
