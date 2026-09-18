"""基于 DB 路径的进程级单实例锁。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, TextIO

try:  # 当前支持的 macOS/Ubuntu 都提供 fcntl。
    import fcntl
except ImportError:  # pragma: no cover - 保持导入错误可读
    fcntl = None  # type: ignore[assignment]


class InstanceAlreadyRunning(RuntimeError):
    pass


class InstanceLock:
    def __init__(self, db_path: str):
        self.path = Path(f"{db_path}.lock")
        self._file: Optional[TextIO] = None

    def acquire(self) -> None:
        if fcntl is None:
            raise RuntimeError("当前平台不支持单实例文件锁")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            file.seek(0)
            owner = file.read().strip() or "未知 PID"
            file.close()
            raise InstanceAlreadyRunning(
                f"ai-relay 已在使用同一任务库（锁 {self.path}，持有者 {owner}）") from e
        file.seek(0)
        file.truncate()
        file.write(str(os.getpid()))
        file.flush()
        self._file = file

    def release(self) -> None:
        if self._file is None:
            return
        if fcntl is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        self._file.close()
        self._file = None
