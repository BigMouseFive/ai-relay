# ai-relay · AI 问答中转站

基于 [kimi-webbridge](https://www.kimi.com/zh-cn/features/webbridge) 驱动本机真实浏览器中**已登录**的 AI 问答网页（Kimi / DeepSeek），把"网页版 AI 问答"包装成 HTTP API：

```
客户端 ──HTTP──> ai-relay (FastAPI) ──HTTP──> webbridge daemon ──> 浏览器 N 个 AI 网页 tab
```

- 客户端提交提示词（最长 12000 字符，可配），拿回 task_id
- 服务端为每个 AI 网页 tab 维护一个 worker：找空闲 worker → 开新对话 → 填入提示词 → 发送 → 轮询等结果
- 没有空闲 worker 时任务排队，有 worker 空出自动执行
- 客户端轮询结果 API 获取纯文本回答
- 自带监控页，实时查看 daemon 健康、worker 状态、队列与任务流转

## 前置条件

1. **macOS 或 Ubuntu**（Ubuntu 必须有桌面环境和 Chrome/Chromium——webbridge 驱动真实浏览器，纯无头服务器不可用）
2. Python 3.11+（Ubuntu 可用 apt 自动装，macOS 建议 `brew install python@3.12`）
3. 浏览器中已登录要用的站点（kimi.com / chat.deepseek.com / agent.minimaxi.com）。webbridge daemon 和浏览器扩展由安装脚本自动安装，扩展也可手动装：https://www.kimi.com/zh-cn/features/webbridge

## 一键安装启动（含开机自启）

```bash
cd ai-relay
./install.sh
```

脚本自动完成：Python 检查（Ubuntu 自动 apt 安装）→ kimi-webbridge daemon 安装与启动 → 创建 `.venv` 安装依赖 → **注册开机自启** → 启动并健康检查。

自启机制（均为用户级服务，因为 webbridge 依赖当前用户的浏览器会话）：

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

启动后每个 worker 会在浏览器打开一个 tab（标签组 `ai-relay`）。**请不要手动操作这些 tab**；tab 被关掉 worker 会自动重开，但进行中的任务会失败。

监控页：http://127.0.0.1:8600/ （每 2 秒自动刷新）

监控页顶部带**创建任务（调试）**面板：站点选项由服务端已配置 worker 动态提供（含 MiniMax）；页面以幂等键提交、展示请求/实际站点、重试次数、当前阶段和完整任务详情。页面提交会向已登录的真实第三方 AI 网页发送内容。

## 配置（config.yaml）

完整模板见 [`config.example.yaml`](config.example.yaml)。新部署统一使用 `targets`；旧 `workers` 浏览器配置仍兼容，并会自动转换为 WebBridge target。每种后端都可配置多个 target，每条的 `count` 是独立并发槽数。关键项：

```yaml
webbridge:
  command_timeout_seconds: 60
  status_timeout_seconds: 5
workers:
  - site: kimi              # kimi | deepseek | minimax
    model: "K2.6"
    count: 1
task:
  timeout_seconds: 600       # 每个 attempt 的端到端上限
  stall_seconds: 120         # 回答文本无进展上限
  hard_timeout_seconds: 300
  max_retries: 3             # 仅发送前失败允许自动重试
  retry_switch_site: false   # 默认不把同一 prompt 发往其他站点
  max_queue_wait_seconds: 3600
  retry_initial_delay_seconds: 3
  retry_max_delay_seconds: 60
storage:
  terminal_retention_seconds: 2592000  # 30 天后清除 prompt/result
  metadata_retention_seconds: 7776000  # 90 天后删除终态任务元数据
```

- 配置字段严格校验：未知字段、未知站点、无 worker、非正的队列/超时配置会拒绝启动。
- `hard_timeout_seconds` 与 `stall_seconds` 不得大于 `timeout_seconds`。
- 同一 SQLite 数据库只能由一个 relay 实例使用；第二实例会拒绝启动，避免争用浏览器 session。
- API key 不写入 YAML：默认从与 `config.yaml` 同目录的 `.env` 加载（例如 `AI_RELAY_INTERNAL_OPENAI_API_KEY='...'`），或由 launchd/systemd 注入同名环境变量；后者优先。`.env` 已被忽略且应设置为 `0600`。

### 执行后端：WebBridge、ACP、OpenAI-compatible API

`targets` 支持三种类型：

```yaml
targets:
  - id: kimi-browser-fast
    type: webbridge
    site: kimi
    model: "K2.6"
    count: 2

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
    model: "gpt-5.6-terra"
    count: 4
    timeout_seconds: 120
    max_retries: 2
    retry_server_errors: false   # 默认：5xx 结果未知，不自动重发
```

- **ACP / Cursor Agent CLI**：当前机器已确认 CLI 版本为 `2026.09.10-fd3934a`，非交互命令为 `agent --print --output-format text --mode ask --workspace <dir> <prompt>`。每个并发槽启动独立子进程组，绝不使用 `shell=True`；默认 `ask` / `plan` 都是只读模式，不会传 `--force` 或 `--yolo`；取消时先 SIGTERM 再 SIGKILL，stdout/stderr 有上限。`agent status` 已确认当前用户已登录。若服务环境仍无法使用 Keychain，可在 ACP target 设置 `api_key_env: CURSOR_API_KEY`，relay 只把该变量映射为子进程的 `CURSOR_API_KEY`，不放进进程参数或日志。
- **OpenAI-compatible API**：调用 `{base_url}/chat/completions`，使用 Bearer key 与 relay task ID 作为 `Idempotency-Key`。API key 只从 `api_key_env` 环境变量读取。`base_url` 必须包含版本前缀（如 `/v1`）。
- `target` 是 `/v1/tasks` 推荐的精确路由字段；指定 target 后不会自动跨到 ACP/API/浏览器的其他 target。
- 未指定 target 的旧调用只会调度 WebBridge browser target，绝不会意外发送到 ACP 或 API。
- API 5xx、读超时、ACP 子进程非零退出等“可能已经接收 prompt”的情况会进入 `outcome_unknown`，不会自动重复执行。
- `./install.sh` 发现缺少 `config.yaml` 时会按 `config.example.yaml` 创建一个权限为 `0600` 的模板并退出，供你检查后再次安装。

配置路径可用环境变量覆盖：`AI_RELAY_CONFIG=/path/to/config.yaml`

## 接入 ERP（局域网动态组网）

多台设备各跑一个 ai-relay，通过心跳自注册加入 ERP 的 LLM 节点池；ERP 优先用 relay 通道（网页端额度），任何失败自动回退直连 API。

```yaml
server:
  host: 0.0.0.0            # 局域网内 ERP 要能访问到本机，必须改
erp:
  register_url: "http://192.168.1.10:8888/api/system/relay-nodes/heartbeat"  # ERP 地址
  node_name: "mac-studio-1"    # 节点名，空=主机名
  advertise_url: ""            # 上报给 ERP 的本机地址，空=自动探测局域网 IP；ERP 调不通就填死
  token: ""                    # 与 ERP 系统设置里的 RELAY_TOKEN 一致（建议双方都设置）
  interval_seconds: 30
```

- 启动后立即发第一次心跳，之后每 30s 一次；ERP 超过 `RELAY_NODE_STALE_SECONDS`（默认 90s）收不到心跳自动将节点置离线，进程退出无需注销
- ERP 侧在 系统设置 → AI Relay 中转 查看节点状态、启用开关与站点偏好
- `advertise_url` 必须是 ERP 能访问的地址：本机部署用 `http://127.0.0.1:8600`，跨设备用 LAN IP 并确保 `server.host: 0.0.0.0`

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

`Idempotency-Key` 必填且必须由调用方为**同一个业务请求**稳定保存。请求或响应网络中断后，用相同 key 和相同 body（包括 `target`、`model`）重发，会返回同一任务；同 key 不同 body 返回 `409`，从而避免重复向真实 AI 网页或 API 发送内容。

`GET /v1/tasks/{task_id}` 返回状态、阶段、实际站点、重试、尝试站点、结构化错误、完整 prompt/result 和 attempt 历史。状态包括：

- `queued` / `retry_wait` / `running`：等待、延时重试、执行中；
- `done`：已获取并稳定确认结果；
- `failed` / `cancelled` / `expired` / `interrupted`：确定的终态；
- `outcome_unknown`：发送 click 后发生超时或异常，**可能已经发送**。系统不会自动重放，请先检查 provider 历史，再由人工决定是否调用 retry。

其他 v1 接口：

- `GET /v1/capabilities` — 已配置 site/model、可用 worker 和限制；
- `GET /v1/tasks?page=&page_size=&status=` — 任务分页；非法状态返回 `422`；
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

因此，如果 Kimi 在近期更慢、失败更多或产生更多结果未知，自动任务分配到其对应 target 的概率会降低；它不会被完全永久禁用，除非所有 worker 都是 `degraded`。可通过 `GET /v1/routing/metrics` 查看当前每个健康 target 的成功率、延迟、权重和概率；每个自动任务的候选权重也会写入 task event 和详情的 `routing_decision`。

历史上没有 attempt 数据的旧任务不会参与评分；升级后的新任务开始积累样本。相同 `Idempotency-Key` 的 adaptive 重试固定复用第一次选择的 task/target，不会因为实时概率变化误创建或冲突。

### 重启与数据保留

任务、attempt、事件均先写入 SQLite 后才被接受。重启时：

- 尚未进入浏览器的 `queued` / `retry_wait` 会恢复调度；
- 正在执行的 `running` 会变为 `interrupted`，不会自动重复发送；
- 已确认发送后结果未知的任务为 `outcome_unknown`，需要人工确认。

终态任务默认 30 天后脱敏完整 prompt/result，90 天后删除任务元数据（均可配置）。SQLite 文件仍应纳入主机备份与容量监控。

## 站点适配说明

各站点的 selector 集中在 `app/sites/`（kimi.py / deepseek.py / minimax.py）各文件顶部常量。网页改版导致 worker 异常时，优先检查这些 selector 是否失效（监控页会显示 degraded 及原因）。

- **Kimi**：输入框是 Lexical contenteditable，用 webbridge `fill` 填入；生成中发送按钮带 `.stop` class；回答取最后一条 `.segment-assistant` 内的 `.markdown-container`（自动排除思考过程）。
- **DeepSeek**：输入框是 textarea，webbridge `fill` 不可用，采用 evaluate 原生 setter 注入；生成中发送按钮变停止图标（SVG path 特征）；回答取 `.ds-assistant-message-main-content`（天然不含思考内容）；智能搜索始终强制关闭。
- **MiniMax Agent**：全部控件用稳定 `data-testid`；输入框是 TipTap contenteditable，`fill` 可用；生成中 send-button 被替换为 stop-button（存在即生成中）；回答取最后一个含 `assistant-active-flow` 的 message-item（过程 UI 天然分离）；Agent 团队和思考两个开关始终强制关闭。

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
    ├── registrar.py     # 向 ERP 心跳注册（局域网动态接入）
    ├── sites/           # 站点适配层（base / kimi / deepseek / minimax）
    └── static/index.html  # 监控页（含创建任务调试面板、分页任务表）
tests/               # 单元测试
```
