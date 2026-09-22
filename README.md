# ai-relay · AI 问答中转站

统一承接 ERP LLM 请求的本地中转服务。默认配置只使用 Cursor Agent CLI 的 ACP target，不安装/启动 Kimi WebBridge，也不会打开浏览器：

```
ERP ──HTTP/mDNS──> ai-relay (FastAPI) ──子进程──> Cursor Agent CLI (ACP)
```

可选扩展为 OpenAI-compatible API 或 Kimi/DeepSeek/MiniMax WebBridge target，但必须显式写入 `config.yaml`。

- 客户端提交提示词（最长 12000 字符，可配），拿回 task_id
- 默认 ACP worker 使用只读 `ask` 模式执行；没有空闲 worker 时任务排队
- 客户端轮询结果 API 获取纯文本回答
- 自带监控页，实时查看 daemon 健康、worker 状态、队列与任务流转

## 前置条件

1. **macOS 或 Ubuntu**；默认 ACP-only 可运行在无桌面环境的 home-server
2. Python 3.11+（Ubuntu 可用 apt 自动装，macOS 建议 `brew install python@3.12`）
3. Cursor Agent CLI `agent` 已安装并登录；也可在 ACP target 中通过 `api_key_env` 映射 key
4. 只有显式配置 WebBridge target 时，才需要桌面浏览器、WebBridge 扩展及对应站点登录态

## 一键安装启动（含开机自启）

```bash
cd ai-relay
./install.sh

# 中国大陆网络环境下：使用清华 PyPI 镜像下载 pip 和 Python 依赖
./install.sh --china-mirror
```

脚本自动完成：Python 检查（Ubuntu 自动 apt 安装）→ 创建 `.venv` 安装依赖 → 校验配置 → **注册开机自启** → 启动并健康检查。ACP-only 配置会明确跳过 WebBridge；只有配置了 browser target 才安装/启动它。

`--china-mirror` 使用清华 PyPI 镜像 `https://pypi.tuna.tsinghua.edu.cn/simple` 下载 pip 与 `requirements.txt` 中的 Python 包；它不改变 apt、WebBridge、Cursor Agent 或模型 API 的下载/访问地址。

自启机制使用用户级服务；Ubuntu ACP-only 配合 linger 可在无人登录时开机启动：

| 系统 | 机制 | 自启时机 | 崩溃自愈 |
|---|---|---|---|
| macOS | launchd LaunchAgent（`~/Library/LaunchAgents/com.ai-relay.server.plist`） | 用户登录 | `KeepAlive` 自动拉起 |
| Ubuntu | systemd 用户服务 + `loginctl enable-linger` | **开机**（无需登录） | `Restart=always` |

```bash
./install.sh uninstall   # 停止服务并移除开机自启（保留代码与 .venv）
```

## 手动启动（开发调试用）

```bash
cd ai-relay
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python run.py        # 按 config.yaml 的 server.host/port 启动
```

默认 ACP-only 启动后不会打开浏览器。只有显式配置 WebBridge target 时，每个 browser worker 才会打开独立 tab；不要手动操作这些 tab。

监控页：http://127.0.0.1:8600/ （每 2 秒自动刷新）

监控页顶部带**创建任务（调试）**面板；页面以幂等键提交、展示实际 target、重试次数、当前阶段和完整任务详情。只有配置 WebBridge target 时才会向已登录的第三方 AI 网页发送内容。

## 配置（config.yaml）

完整模板见 [`config.example.yaml`](config.example.yaml)。新部署统一使用 `targets`；旧 `workers` 浏览器配置仍兼容，并会自动转换为 WebBridge target。每种后端都可配置多个 target，每条的 `count` 是独立并发槽数。关键项：

```yaml
targets:
  - id: cursor-agent-ai-relay
    type: acp
    command: agent
    working_directory: "."
    mode: ask
    count: 2
workers: []
task:
  timeout_seconds: 600       # 每个 attempt 的端到端上限
  stall_seconds: 120         # 回答文本无进展上限
  hard_timeout_seconds: 300
  max_retries: 3             # 发送前失败或确定的输出契约失败可安全重试
  retry_switch_site: false   # 默认不把同一 prompt 发往其他站点
  max_queue_wait_seconds: 3600
  retry_initial_delay_seconds: 3
  retry_max_delay_seconds: 60
storage:
  terminal_retention_seconds: 2592000  # 30 天后清除 prompt/result
  metadata_retention_seconds: 7776000  # 90 天后删除终态任务元数据
```

- 默认模板没有 `webbridge` target，因此安装和运行都不需要浏览器。若显式启用 browser target，`webbridge.tab_group_title` 可控制 Chrome Tab Group 标签。
- 配置字段严格校验：未知字段、未知站点、无 worker、非正的队列/超时配置，以及 `routing.task_policies` 引用不存在的 target id，都会拒绝启动。
- `hard_timeout_seconds` 与 `stall_seconds` 不得大于 `timeout_seconds`。
- 同一 SQLite 数据库只能由一个 relay 实例使用；第二实例会拒绝启动。
- 可选 key 不写入 YAML：默认从与 `config.yaml` 同目录的 `.env` 加载，或由 launchd/systemd 注入同名环境变量；后者优先。`.env` 已被忽略且应设置为 `0600`。

### 执行后端：WebBridge、ACP、OpenAI-compatible API

`targets` 支持三种类型：

```yaml
targets:
  - id: cursor-agent-project-a
    type: acp
    command: agent
    working_directory: "../project-a"
    mode: ask                    # ask | plan；均为只读模式
    output_format: text          # text | json | stream-json
    trust_workspace: true
    count: 2
    timeout_seconds: 600

  - id: internal-openai-default
    type: openai_compatible
    base_url: "http://10.67.8.60:18080/v1"
    api_key_env: AI_RELAY_INTERNAL_OPENAI_API_KEY
    model: "deepseek-v4-pro"
    count: 4
    timeout_seconds: 120
    max_retries: 2
    verify_model_on_start: true  # 启动时只读 GET /models，过期模型会使 target degraded
    max_output_tokens: 8192
    retry_server_errors: false   # 默认：5xx 结果未知，不自动重发
```

- **ACP / Cursor Agent CLI**：当前机器已确认 CLI 版本为 `2026.09.10-fd3934a`，非交互命令为 `agent --print --output-format text --mode ask --workspace <dir> <prompt>`。每个并发槽启动独立子进程组，绝不使用 `shell=True`；默认 `ask` / `plan` 都是只读模式，不会传 `--force` 或 `--yolo`；取消时先 SIGTERM 再 SIGKILL，stdout/stderr 有上限。`agent status` 已确认当前用户已登录。若服务环境仍无法使用 Keychain，可在 ACP target 设置 `api_key_env: CURSOR_API_KEY`，relay 只把该变量映射为子进程的 `CURSOR_API_KEY`，不放进进程参数或日志。
- **OpenAI-compatible API**：调用 `{base_url}/chat/completions`，使用 Bearer key 与 relay task ID 作为 `Idempotency-Key`。API key 只从 `api_key_env` 环境变量读取。`base_url` 必须包含版本前缀（如 `/v1`）。建议启用 `verify_model_on_start`：relay 会只读请求 `GET /models`，配置模型已改名/下线时将 target 标记为 degraded，而不是等业务任务收到 5xx。HTTP 200 但缺少/为空的 `message.content` 是确定的不合格响应，可按 relay retry budget 安全重试。
- `target` 是 `/v1/tasks` 推荐的精确路由字段；指定 target 后不会自动跨到 ACP/API/浏览器的其他 target。
- 未指定 target 的旧调用只会调度 WebBridge browser target，绝不会意外发送到 ACP 或 API。
- API 5xx、读超时、ACP 子进程非零退出等“可能已经接收 prompt”的情况会进入 `outcome_unknown`，不会自动重复执行。
- `./install.sh` 发现缺少 `config.yaml` 时会按 ACP-only 的 `config.example.yaml` 创建一个权限为 `0600` 的模板并退出，供你检查后再次安装。

配置路径可用环境变量覆盖：`AI_RELAY_CONFIG=/path/to/config.yaml`

## 接入 ERP（IPv4 局域网自动发现）

ai-relay 使用标准 **mDNS / DNS-SD**（IPv4 multicast `224.0.0.251:5353`）公告服务；ERP 宿主机上的 `lan-discovery-agent` 自动发现、读取 metadata、主动探测 `/v1/readiness` 并维护节点健康度。双方**不配置对端地址、回调 URL 或心跳 token**。

```yaml
server:
  host: 0.0.0.0            # 让局域网 ERP 能访问 ai-relay

discovery:
  enabled: true
  instance_name: ""        # 空=主机名
  advertise_address: ""    # 多网卡时指定 LAN IPv4；空=自动选择私有 IPv4
  identity_path: data/service-identity.json
```

- 发布服务类型：`_amz-ai-relay._tcp.local.`；稳定 `service_id` 存在本机 `identity_path`，DHCP 更换 IP 不会产生新节点。
- ai-relay 暴露 `GET /.well-known/amazon-service` 与 `GET /v1/readiness`；前者提供身份/API 协议，后者提供可接单状态、队列和 worker 摘要。
- ERP 的 discovery agent 必须运行在宿主机网络（Docker `network_mode: host`），普通 Docker bridge 通常收不到 LAN mDNS 组播。
- 该模式没有请求鉴权：可信 LAN/VLAN 外的设备若可访问端口也可提交任务；请用网络隔离或主机防火墙限制访问范围。

## API

### 推荐：幂等提交（v1）

```bash
curl -X POST http://127.0.0.1:8600/v1/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 5c0f1555-7f55-4b0c-9b47-unique-per-logical-request' \
  -d '{"prompt":"用一句话解释量子计算","target":"kimi-browser-fast"}'
# => 202 Accepted
# Location: /v1/tasks/<task_id>
```

`Idempotency-Key` 必填且必须由调用方为**同一个业务请求**稳定保存。请求或响应网络中断后，用相同 key 和相同 body（包括 `target`、`model`、`retry_policy`、`response_format`）重发，会返回同一任务；同 key 不同 body 返回 `409`，从而避免重复向真实 AI 网页或 API 发送内容。

#### 结构化 JSON 输出与任务级重试

`/v1/tasks` 可要求 relay 严格校验模型输出并在**确定的输出不合格**时在 relay 内部安全重试：

```json
{
  "prompt": "生成商品文案",
  "routing_mode": "adaptive",
  "retry_policy": {"max_retries": 2},
  "response_format": {
    "type": "json_schema",
    "name": "listing_copy",
    "schema": {
      "type": "object",
      "required": ["title"],
      "properties": {"title": {"type": "string", "minLength": 1}}
    }
  }
}
```

- 仅支持 `response_format.type=json_schema`，按 Draft 2020-12 JSON Schema 严格校验。
- relay 不使用 JSON repair，也不剥离 Markdown fence 或从说明文本中提取片段；只有完整 `json.loads()` 成功且 Schema 合格的结果才标记为 `done`，并会被重新序列化为规范 JSON。
- 空回答、非法 JSON 或 Schema 不匹配会占用 `retry_policy.max_retries` 的预算；有效值受服务端 `task.max_retries` 与 `task.max_client_requested_retries` 限制。
- 每个 adaptive 的安全 retry 都会按最新健康度/容量重新选择 target；点击发送后结果不确定的 `outcome_unknown` 绝不自动重放。
- 可用 `routing.task_policies` 按 `response_format.name` 约束自动路由，而无需 ERP 指定 target。默认 `listing_copy: {allowed_targets: [cursor-agent-ai-relay], max_in_flight_per_target: 2}`，因此只使用 ACP。达到 policy 并发上限时任务保持 `queued` / `retry_wait`；显式 target 若不在 allowlist 中会被拒绝。

`GET /v1/tasks/{task_id}` 返回状态、阶段、实际站点、重试、尝试站点、结构化错误、完整 prompt/result 和 attempt 历史。若提交响应丢失，可用同一 `Idempotency-Key` 调用只读的 `GET /v1/tasks/by-idempotency-key` 找回原任务；缺失或空白 header 返回 `422`，没有匹配任务返回 `404`，且不会创建、排队或重试任务。状态包括：

- `queued` / `retry_wait` / `running`：等待、延时重试、执行中；
- `done`：已获取并稳定确认结果；
- `failed` / `cancelled` / `expired` / `interrupted`：确定的终态；
- `outcome_unknown`：发送 click 后发生超时或异常，**可能已经发送**。系统不会自动重放，请先检查 provider 历史，再由人工决定是否调用 retry。

其他 v1 接口：

- `GET /v1/capabilities` — 已配置 site/model、可用 worker 和限制；
- `GET /v1/tasks?page=&page_size=&status=` — 任务分页；非法状态返回 `422`；
- `GET /v1/tasks/by-idempotency-key` — 通过 `Idempotency-Key` 只读查询原任务；
- `GET /v1/tasks/{task_id}/events` — 持久化状态事件；
- `POST /v1/tasks/{task_id}/cancel` — queued/retry_wait 可取消；running 为协作式取消；
- `POST /v1/tasks/{task_id}/retry` — 对终态任务显式人工重试，并重置排队截止时间。

旧 `/api/tasks`、`/api/tasks/{id}`、`/api/stats` 保留作兼容接口；新客户端应使用 `/v1`。

### 自适应路径选择

`routing.default_mode: adaptive` 时，`POST /v1/tasks` 未指定 `target` 或 `site` 的新任务会在健康 target 中按权重随机选择；显式 `target` 永远优先，旧 `/api/tasks` 仍只走浏览器兼容路径。

评分使用近 `window_seconds` 内、已完成的 attempt 元数据，不读取 prompt/result：

- Beta 平滑成功率（低样本不会过度自信）；
- 明确失败率；
- `outcome_unknown` 比普通失败更高的惩罚；
- 平均执行耗时（指数衰减）；
- 当前 idle/total 容量；
- 低样本 target 的 `exploration_weight`，避免新 target 永远拿不到样本。

因此，如果 Kimi 在近期更慢、失败更多或产生更多结果未知，自动任务分配到其对应 target 的概率会降低；它不会被完全永久禁用，除非所有 worker 都是 `degraded`。可通过 `GET /v1/routing/metrics` 查看当前每个健康 target 的成功率、延迟、权重和概率；每个自动任务的候选权重也会写入 task event 和详情的 `routing_decision`。命中 task policy 时，decision 还会记录 `task_policy`、`allowed_targets` 和 `max_in_flight_per_target`。

历史上没有 attempt 数据的旧任务不会参与评分；升级后的新任务开始积累样本。相同 `Idempotency-Key` 始终复用同一个 task；对于 adaptive 任务，target 在**每个实际 attempt 分派时**按当时的健康度重新选择，因此 JSON/Schema 校验失败等确定性安全重试可自动切换到质量更高的 target；发送后结果未知仍绝不重放。

### 重启与数据保留

任务、attempt、事件均先写入 SQLite 后才被接受。重启时：

- 尚未开始执行的 `queued` / `retry_wait` 会恢复调度；
- 正在执行的 `running` 会变为 `interrupted`，不会自动重复发送；
- 已确认发送后结果未知的任务为 `outcome_unknown`，需要人工确认。

终态任务默认 30 天后脱敏完整 prompt/result，90 天后删除任务元数据（均可配置）。SQLite 文件仍应纳入主机备份与容量监控。

## 可选浏览器站点适配说明

本节仅在显式配置 WebBridge target 时适用。各站点 selector 集中在 `app/sites/`；默认 ACP-only 不加载这些 worker。

- **Kimi**：输入框是 Lexical contenteditable，用 webbridge `fill` 填入；生成中发送按钮带 `.stop` class；回答取最后一条 `.segment-assistant` 内的 `.markdown-container`（自动排除思考过程）。
- **DeepSeek**：输入框是 textarea，webbridge `fill` 不可用，采用 evaluate 原生 setter 注入；生成中发送按钮变停止图标（SVG path 特征）；回答取 `.ds-assistant-message-main-content`（天然不含思考内容）；智能搜索始终强制关闭。
- **MiniMax Agent**：全部控件用稳定 `data-testid`；输入框是 TipTap contenteditable；回答取最后一个含 `assistant-active-flow` 的 message-item。页面完成后可能仍保留 stop-button，因此以 `turn-process-disclosure` 完成摘要覆盖生成状态；Agent 团队和思考两个开关始终强制关闭。

## 测试

```bash
.venv/bin/python -m pytest tests/ -q
```

单元测试全部 mock webbridge，不会触碰真实浏览器。

## 项目结构

```
ai-relay/
├── install.sh       # 一键安装启动 + 开机自启（macOS/Ubuntu，uninstall 卸载）
├── run.py           # 启动入口（按 config.yaml 的 host/port 启动 uvicorn）
├── config.yaml
├── data/            # SQLite 任务库（ai-relay.db，自动创建）
├── logs/            # 服务日志（install.sh 安装后写入）
├── requirements.txt
├── README.md
└── app/
    ├── main.py          # FastAPI 入口与路由
    ├── config.py        # 配置加载
    ├── schemas.py       # 请求/响应模型
    ├── store.py         # 任务存储（内存 + SQLite 写穿 + TTL）
    ├── db.py            # SQLite 持久化层
    ├── pool.py          # worker 池 + 队列分发
    ├── worker.py        # worker 主循环（一个 tab 的抽象）
    ├── webbridge.py     # webbridge daemon 异步 client
    ├── discovery.py     # IPv4 mDNS/DNS-SD 服务公告与稳定 service_id
    ├── response_contract.py # JSON Schema 输出约束与严格校验
    ├── sites/           # 站点适配层（base / kimi / deepseek / minimax）
    └── static/index.html  # 监控页（含创建任务调试面板、分页任务表）
tests/               # 单元测试
```
