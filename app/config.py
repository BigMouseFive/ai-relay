"""配置加载与校验。"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Annotated, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictConfigModel(BaseModel):
    """拒绝拼写错误的配置字段，避免静默回退到默认值。"""

    model_config = ConfigDict(extra="forbid")


class ServerConfig(StrictConfigModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8600, ge=1, le=65535)


class WebbridgeConfig(StrictConfigModel):
    base_url: str = "http://127.0.0.1:10086"
    command_timeout_seconds: float = Field(default=60.0, gt=0)
    status_timeout_seconds: float = Field(default=5.0, gt=0)


class WorkerConfig(StrictConfigModel):
    """旧浏览器 worker 配置；启动时转换为 webbridge target。"""

    site: Literal["kimi", "deepseek", "minimax"]
    model: str = ""
    count: int = Field(default=1, ge=1)


class TargetBaseConfig(StrictConfigModel):
    id: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    count: int = Field(default=1, ge=1, le=64)
    timeout_seconds: Optional[float] = Field(default=None, gt=0)
    max_retries: Optional[int] = Field(default=None, ge=0)


class WebbridgeTargetConfig(TargetBaseConfig):
    type: Literal["webbridge"]
    site: Literal["kimi", "deepseek", "minimax"]
    model: str = ""


class AcpTargetConfig(TargetBaseConfig):
    """Cursor Agent CLI 执行目标（type 保留为 acp 以兼容 relay 路由语义）。

    当前 Cursor CLI 的非交互形式是：
    ``agent --print --output-format text --mode ask --workspace <dir> <prompt>``。
    prompt 是位置参数，不使用 shell 拼接；默认 ask 模式为只读问答。
    """

    type: Literal["acp"]
    command: str = "agent"
    args: list[str] = Field(default_factory=list)  # 额外固定 Cursor CLI 参数
    working_directory: str
    model: str = ""
    mode: Literal["ask", "plan"] = "ask"
    output_format: Literal["text", "json", "stream-json"] = "text"
    # 可选：以安全环境变量映射到子进程 CURSOR_API_KEY，避免依赖 Keychain。
    # 不使用 --api-key 参数，避免 key 出现在进程参数列表。
    api_key_env: Optional[str] = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    endpoint: Optional[str] = None
    verify_auth_on_start: bool = True
    auth_check_timeout_seconds: float = Field(default=10.0, gt=0, le=60)
    trust_workspace: bool = True
    pass_workspace: bool = True
    output_max_chars: int = Field(default=200_000, ge=1_000, le=5_000_000)
    graceful_shutdown_seconds: float = Field(default=5.0, gt=0, le=60)


class OpenAICompatibleTargetConfig(TargetBaseConfig):
    type: Literal["openai_compatible"]
    base_url: str
    api_key_env: str = Field(min_length=1, max_length=160, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    model: str = Field(min_length=1, max_length=200)
    allow_model_override: bool = False
    allowed_models: list[str] = Field(default_factory=list)
    max_output_tokens: Optional[int] = Field(default=None, ge=1)
    # 仅在上游明确保证 Idempotency-Key 语义时启用；默认避免 5xx 后重复执行。
    retry_server_errors: bool = False

    @model_validator(mode="after")
    def validate_models(self) -> "OpenAICompatibleTargetConfig":
        if self.allow_model_override and not self.allowed_models:
            raise ValueError("allow_model_override=true 时必须配置 allowed_models")
        if not self.allow_model_override and self.allowed_models:
            raise ValueError("allow_model_override=false 时不得配置 allowed_models")
        return self


TargetConfig = Annotated[
    Union[WebbridgeTargetConfig, AcpTargetConfig, OpenAICompatibleTargetConfig],
    Field(discriminator="type"),
]


class TaskConfig(StrictConfigModel):
    timeout_seconds: int = Field(default=600, gt=0)
    retention_seconds: int = Field(default=3600, gt=0)
    max_prompt_chars: int = Field(default=12000, gt=0)
    stall_seconds: int = Field(default=120, gt=0)
    max_retries: int = Field(default=3, ge=0)
    hard_timeout_seconds: int = Field(default=300, gt=0)
    # 跨浏览器站点发送同一 prompt 会改变隐私、成本和模型语义，因此默认关闭。
    retry_switch_site: bool = False
    max_queue_wait_seconds: int = Field(default=3600, gt=0)
    retry_initial_delay_seconds: float = Field(default=3.0, gt=0)
    retry_max_delay_seconds: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def validate_timeouts(self) -> "TaskConfig":
        if self.hard_timeout_seconds > self.timeout_seconds:
            raise ValueError("hard_timeout_seconds 不能大于 timeout_seconds")
        if self.stall_seconds > self.timeout_seconds:
            raise ValueError("stall_seconds 不能大于 timeout_seconds")
        if self.retry_initial_delay_seconds > self.retry_max_delay_seconds:
            raise ValueError("retry_initial_delay_seconds 不能大于 retry_max_delay_seconds")
        return self


class QueueConfig(StrictConfigModel):
    max_size: int = Field(default=100, gt=0)


class RoutingConfig(StrictConfigModel):
    """基于近期 attempt 数据的自动 target 选择策略。"""

    default_mode: Literal["browser", "adaptive"] = "adaptive"
    window_seconds: int = Field(default=7 * 24 * 3600, gt=0)
    min_samples: int = Field(default=5, ge=1)
    exploration_weight: float = Field(default=0.08, ge=0, le=1)
    latency_reference_seconds: float = Field(default=60.0, gt=0)
    failure_penalty: float = Field(default=0.50, ge=0, le=1)
    outcome_unknown_penalty: float = Field(default=0.85, ge=0, le=1)


class DashboardConfig(StrictConfigModel):
    history_size: int = Field(default=100, gt=0)


class StorageConfig(StrictConfigModel):
    db_path: str = "data/ai-relay.db"  # 相对配置文件所在目录
    terminal_retention_seconds: int = Field(default=30 * 24 * 3600, gt=0)
    metadata_retention_seconds: int = Field(default=90 * 24 * 3600, gt=0)

    @model_validator(mode="after")
    def validate_retention(self) -> "StorageConfig":
        if self.metadata_retention_seconds < self.terminal_retention_seconds:
            raise ValueError("metadata_retention_seconds 不能小于 terminal_retention_seconds")
        return self


class ErpConfig(StrictConfigModel):
    """向 ERP 心跳注册（register_url 为空则不注册）。"""

    register_url: str = ""
    node_name: str = ""
    advertise_url: str = ""
    token: str = ""
    interval_seconds: int = Field(default=30, gt=0)


class Config(StrictConfigModel):
    # 可选环境文件，仅用于 API key 等秘密；环境变量优先，不写入 SQLite/日志。
    environment_file: str = ".env"
    server: ServerConfig = Field(default_factory=ServerConfig)
    webbridge: WebbridgeConfig = Field(default_factory=WebbridgeConfig)
    # 旧浏览器配置，保留以兼容已有 config.yaml。
    workers: list[WorkerConfig] = Field(default_factory=list)
    targets: list[TargetConfig] = Field(default_factory=list)
    task: TaskConfig = Field(default_factory=TaskConfig)
    queue: QueueConfig = Field(default_factory=QueueConfig)
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    erp: ErpConfig = Field(default_factory=ErpConfig)

    @model_validator(mode="after")
    def normalize_targets(self) -> "Config":
        targets = list(self.targets)
        explicit_ids = {target.id for target in targets}
        if len(explicit_ids) != len(targets):
            raise ValueError("targets 中 target id 必须唯一")

        # legacy workers 与新 targets 可以共存；生成稳定 target id，避免升级破坏旧配置。
        for index, worker in enumerate(self.workers, 1):
            target_id = f"legacy-webbridge-{worker.site}-{index}"
            if target_id in explicit_ids:
                raise ValueError(f"target id 与旧 workers 生成 id 冲突: {target_id}")
            targets.append(WebbridgeTargetConfig(
                id=target_id, type="webbridge", site=worker.site,
                model=worker.model, count=worker.count,
            ))
            explicit_ids.add(target_id)

        self.targets = targets
        return self


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_environment_file(config_dir: Path, env_file: str) -> None:
    if not env_file:
        return
    path = Path(env_file).expanduser()
    if not path.is_absolute():
        path = config_dir / path
    if not path.exists():
        return
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"环境文件格式错误 {path}:{line_number}")
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not _ENV_NAME.fullmatch(name):
            raise ValueError(f"环境变量名非法 {path}:{line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        # 进程环境变量（systemd/launchd/CI）优先，环境文件只补齐缺失值。
        os.environ.setdefault(name, value)


def load_config(path: str | Path = "config.yaml") -> Config:
    config_path = Path(path).expanduser().resolve()
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    config = Config.model_validate(data)
    _load_environment_file(config_path.parent, config.environment_file)
    if not config.targets:
        raise ValueError("至少需要配置一个 target 或 workers")
    # ACP 相对工作目录与配置文件绑定，而不是依赖服务管理器的 WorkingDirectory。
    for target in config.targets:
        if isinstance(target, AcpTargetConfig) and not Path(target.working_directory).is_absolute():
            target.working_directory = str((config_path.parent / target.working_directory).resolve())
    return config
