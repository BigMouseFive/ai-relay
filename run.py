"""ai-relay 启动入口：按 config.yaml 的 server.host / server.port 启动 uvicorn。

用法：
    .venv/bin/python run.py [config.yaml 路径]

供 install.sh 安装的系统服务（launchd / systemd）调用。
"""
from __future__ import annotations

import logging
import os
import sys

import uvicorn

from app.config import load_config


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s [pid=%(process)d] %(message)s",
    )
    config_path = (sys.argv[1] if len(sys.argv) > 1
                   else os.environ.get("AI_RELAY_CONFIG", "config.yaml"))
    config_path = os.path.abspath(os.path.expanduser(config_path))
    cfg = load_config(config_path)
    # CLI 参数优先且 app 与 listener 必须使用同一份解析后的配置。
    os.environ["AI_RELAY_CONFIG"] = config_path
    from app.main import create_app

    # create_app 的 lifespan 持有单实例锁，factory/CLI 两种启动方式语义一致。
    app = create_app(config_path, config=cfg)
    uvicorn.run(app, host=cfg.server.host, port=cfg.server.port, log_level="info")


if __name__ == "__main__":
    main()
