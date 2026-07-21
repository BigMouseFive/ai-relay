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

监控页顶部带**创建任务（调试）**面板：输入提示词、选站点（任意/kimi/deepseek）、点提交即可创建任务，提交后下方卡片实时跟踪状态，完成后直接显示结果，无需 curl 即可调试。

## 配置（config.yaml）

```yaml
server:
  host: 127.0.0.1   # 局域网接入 ERP 时需改为 0.0.0.0
  port: 8600
webbridge:
  base_url: http://127.0.0.1:10086
workers:
  - site: kimi          # kimi 或 deepseek
    model: "K2.6"       # kimi: 模型名子串，可选 "K3 · Max"/"K3 集群 · Max"/"K2.6 · Fast"
    count: 1            # 该站点开几个 tab
  - site: deepseek
    model: "深度思考"    # deepseek: 开关名子串；"深度思考"=开深度思考，"" = 普通模式
    count: 1
task:
  timeout_seconds: 600      # 单任务最长执行时间
  retention_seconds: 3600   # 已完成任务结果保留时长
  max_prompt_chars: 12000   # 超过返回 422
queue:
  max_size: 100             # 排队上限，满了返回 429
dashboard:
  history_size: 100         # 监控页最近任务条数
```

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

### 提交任务

```bash
curl -X POST http://127.0.0.1:8600/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"prompt": "用一句话解释量子计算", "site": "kimi"}'
# => {"task_id": "55d39ef0..."}
```

- `prompt` 必填；`site` 可选 `"kimi"` / `"deepseek"`，省略则由任一空闲 worker 执行
- 错误：`422` 提示词为空或超长；`429` 队列已满

### 查询结果

```bash
curl http://127.0.0.1:8600/api/tasks/55d39ef0...
```

```json
{
  "task_id": "55d39ef0...",
  "status": "done",            // queued | running | done | failed
  "site": "kimi",
  "prompt": "用一句话解释量子计算",
  "result": "量子计算是……",     // done 时有值
  "error": null,               // failed 时有原因
  "elapsed_seconds": 10.8
}
```

注意：任务全量持久化在 SQLite（`data/ai-relay.db`），**服务重启后历史任务仍可按 id 查询、可分页浏览**；上次运行遗留的 queued/running 任务会在启动时标记为"服务重启，任务中断"。内存中的活跃任务副本默认保留 1 小时（SQLite 全量保留）。

### 其他

- `GET /api/stats` — 监控聚合数据（daemon 健康、worker 状态、队列快照、最近任务）
- `GET /health` — 服务 + webbridge daemon 健康
- `GET /` — 监控页面

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
