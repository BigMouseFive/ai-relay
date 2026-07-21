"""ai-relay 启动入口：按 config.yaml 的 server.host / server.port 启动 uvicorn。

用法：
    .venv/bin/python run.py [config.yaml 路径]

供 install.sh 安装的系统服务（launchd / systemd）调用。
"""
from __future__ import annotations

import os
import sys

import uvicorn

from app.config import load_config


def main() -> None:
    config_path = (sys.argv[1] if len(sys.argv) > 1
                   else os.environ.get("AI_RELAY_CONFIG", "config.yaml"))
    cfg = load_config(config_path)
    os.environ.setdefault("AI_RELAY_CONFIG", config_path)
    uvicorn.run("app.main:app", host=cfg.server.host, port=cfg.server.port,
                log_level="info")


if __name__ == "__main__":
    main()
