"""配置加载与校验。"""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8600


class WebbridgeConfig(BaseModel):
    base_url: str = "http://127.0.0.1:10086"


class WorkerConfig(BaseModel):
    site: str
    model: str
    count: int = Field(default=1, ge=1)


class TaskConfig(BaseModel):
    timeout_seconds: int = 600
    retention_seconds: int = 3600
    max_prompt_chars: int = 12000
    stall_seconds: int = 120      # 生成停滞上限（无进展即重建会话重试）
    max_retries: int = 3          # 失败后允许重新入队的次数（总执行次数 = max_retries + 1）
    hard_timeout_seconds: int = 300  # 硬超时：不信任 generating，超过即失败并重试
    retry_switch_site: bool = True   # 重试时是否允许换到其他站点


class QueueConfig(BaseModel):
    max_size: int = 100


class DashboardConfig(BaseModel):
    history_size: int = 100


class StorageConfig(BaseModel):
    db_path: str = "data/ai-relay.db"   # 任务记录 SQLite 文件路径（相对配置文件所在目录）


class ErpConfig(BaseModel):
    """向 ERP 心跳注册（局域网动态接入）；register_url 为空则不注册。"""
    register_url: str = ""      # 如 http://192.168.1.10:8888/api/system/relay-nodes/heartbeat
    node_name: str = ""         # 空=自动取主机名
    advertise_url: str = ""     # 上报给 ERP 的本机地址，空=自动探测局域网 IP + server.port
    token: str = ""             # 与 ERP 的 RELAY_TOKEN 对应
    interval_seconds: int = 30


class Config(BaseModel):
    server: ServerConfig = ServerConfig()
    webbridge: WebbridgeConfig = WebbridgeConfig()
    workers: list[WorkerConfig] = []
    task: TaskConfig = TaskConfig()
    queue: QueueConfig = QueueConfig()
    dashboard: DashboardConfig = DashboardConfig()
    storage: StorageConfig = StorageConfig()
    erp: ErpConfig = ErpConfig()


def load_config(path: str | Path = "config.yaml") -> Config:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return Config.model_validate(data)
