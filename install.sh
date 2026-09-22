#!/usr/bin/env bash
# ai-relay 一键安装启动 + 开机自启（macOS / Ubuntu）
#
# 用法：
#   ./install.sh                 安装依赖、注册开机自启并立即启动
#   ./install.sh --china-mirror  使用清华 PyPI 镜像安装 Python 依赖
#   ./install.sh uninstall       停止服务并移除开机自启（保留代码与 .venv）
#
# 说明：
# - macOS 使用 launchd LaunchAgent（用户登录后自启，KeepAlive 崩溃自动拉起）
# - Ubuntu 使用 systemd 用户服务 + loginctl linger（开机自启，无需登录）
# - 默认 ACP-only，不安装/启动 kimi-webbridge，也不会打开浏览器
# - 仅当 config.yaml 明确配置 WebBridge target 时，才检查并启动浏览器桥
set -euo pipefail
# 配置、SQLite 和日志可能含有 prompt/result 或 ERP token。
umask 077

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$PROJECT_DIR/.venv"
CONFIG="$PROJECT_DIR/config.yaml"
SERVICE_LABEL="com.ai-relay.server"
WEBBRIDGE_BIN="$HOME/.kimi-webbridge/bin/kimi-webbridge"
CHINA_PYPI_INDEX="https://pypi.tuna.tsinghua.edu.cn/simple"
USE_CHINA_MIRROR=false

log()  { echo -e "\033[1;32m[ai-relay]\033[0m $*"; }
warn() { echo -e "\033[1;33m[ai-relay 警告]\033[0m $*"; }
err()  { echo -e "\033[1;31m[ai-relay 错误]\033[0m $*" >&2; }

OS="$(uname -s)"
case "$OS" in
  Darwin) PLATFORM="mac" ;;
  Linux)  PLATFORM="linux" ;;
  *) err "不支持的操作系统: $OS（仅支持 macOS 与 Ubuntu）"; exit 1 ;;
esac

ensure_config() {
  if [[ -f "$CONFIG" ]]; then
    return
  fi
  if [[ ! -f "$PROJECT_DIR/config.example.yaml" ]]; then
    err "缺少 config.yaml 和 config.example.yaml"; exit 1
  fi
  cp "$PROJECT_DIR/config.example.yaml" "$CONFIG"
  chmod 600 "$CONFIG"
  warn "已从 ACP-only 模板创建 $CONFIG，请检查 agent 命令、工作目录和 LAN 地址后重新运行安装。"
  exit 1
}

config_port() {
  "$VENV/bin/python" -c 'from app.config import load_config; import sys; print(load_config(sys.argv[1]).server.port)' "$CONFIG" 2>/dev/null || echo 8600
}

config_requires_webbridge() {
  "$VENV/bin/python" -c 'from app.config import load_config; import sys; c=load_config(sys.argv[1]); raise SystemExit(0 if any(t.type == "webbridge" for t in c.targets) else 1)' "$CONFIG"
}

uninstall() {
  log "卸载 ai-relay 服务（保留代码与 .venv）..."
  if [[ "$PLATFORM" == "mac" ]]; then
    launchctl unload "$HOME/Library/LaunchAgents/$SERVICE_LABEL.plist" 2>/dev/null || true
    rm -f "$HOME/Library/LaunchAgents/$SERVICE_LABEL.plist"
    log "已移除 LaunchAgent: $SERVICE_LABEL"
  else
    systemctl --user disable --now ai-relay 2>/dev/null || true
    rm -f "$HOME/.config/systemd/user/ai-relay.service"
    systemctl --user daemon-reload 2>/dev/null || true
    log "已移除 systemd 用户服务: ai-relay"
  fi
  log "完成。若曾单独安装 webbridge，其 daemon 保持原状态。"
  exit 0
}

usage() {
  cat <<EOF
用法：
  $0 [--china-mirror]       安装依赖、注册开机自启并立即启动
  $0 uninstall              停止服务并移除开机自启（保留代码与 .venv）

选项：
  --china-mirror            使用清华 PyPI 镜像下载 pip / Python 依赖
  -h, --help                显示本帮助
EOF
}

for arg in "$@"; do
  case "$arg" in
    --china-mirror) USE_CHINA_MIRROR=true ;;
    uninstall)
      [[ "$#" -eq 1 ]] || { err "uninstall 不能与其他参数组合"; usage; exit 2; }
      uninstall
      ;;
    -h|--help) usage; exit 0 ;;
    *) err "未知参数: $arg"; usage; exit 2 ;;
  esac
done

if [[ "$USE_CHINA_MIRROR" == true ]]; then
  log "Python 依赖将使用清华 PyPI 镜像: $CHINA_PYPI_INDEX"
fi

# ============ 1. Python 3.11+ ============
log "检查 Python ..."
if ! command -v python3 >/dev/null 2>&1; then
  if [[ "$PLATFORM" == "linux" ]]; then
    log "安装 python3 ..."
    sudo apt-get update -qq && sudo apt-get install -y -qq python3 python3-venv
  else
    err "未找到 python3，请先安装 Python 3.11+（brew install python@3.12）"; exit 1
  fi
fi
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
  err "Python 版本过低（需要 >= 3.11）: $(python3 --version 2>&1)"; exit 1
fi
if [[ "$PLATFORM" == "linux" ]] && ! python3 -m venv --help >/dev/null 2>&1; then
  log "安装 python3-venv ..."
  sudo apt-get install -y -qq python3-venv
fi
log "Python $(python3 --version | awk '{print $2}') OK"

# ============ 2. 虚拟环境 + 依赖 ============
log "准备 Python 虚拟环境 ..."
[[ -d "$VENV" ]] || python3 -m venv "$VENV"
if [[ "$USE_CHINA_MIRROR" == true ]]; then
  "$VENV/bin/pip" install -q --upgrade pip --index-url "$CHINA_PYPI_INDEX"
  "$VENV/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt" --index-url "$CHINA_PYPI_INDEX"
else
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r "$PROJECT_DIR/requirements.txt"
fi
log "依赖安装完成"

ensure_config
if ! "$VENV/bin/python" -c 'from app.config import load_config; import sys; load_config(sys.argv[1])' "$CONFIG"; then
  err "配置校验失败，请修复后重新运行安装: $CONFIG"
  exit 1
fi
PORT="$(config_port)"; PORT="${PORT:-8600}"

# ============ 3. 可选 WebBridge ============
if config_requires_webbridge; then
  log "配置包含 WebBridge target，检查 kimi-webbridge ..."
  if [[ ! -x "$WEBBRIDGE_BIN" ]]; then
    log "安装 kimi-webbridge ..."
    curl -fsSL https://cdn.kimi.com/webbridge/install.sh | bash
  fi
  "$WEBBRIDGE_BIN" start >/dev/null 2>&1 || true
  WB_STATUS="$("$WEBBRIDGE_BIN" status 2>/dev/null || true)"
  if echo "$WB_STATUS" | grep -q '"extension_connected":true'; then
    log "webbridge daemon 已连接浏览器扩展"
  else
    warn "webbridge 浏览器扩展未连接。请："
    warn "  1. 打开浏览器安装 Kimi WebBridge 扩展: https://www.kimi.com/zh-cn/features/webbridge"
    warn "  2. 在浏览器中登录要用的站点（kimi.com / chat.deepseek.com / agent.minimaxi.com）"
    if [[ "$PLATFORM" == "linux" ]]; then
      warn "  （Ubuntu 需要桌面环境与 Chrome/Chromium 浏览器；纯无头服务器无法使用 webbridge）"
    fi
  fi
else
  log "ACP/API-only 配置：跳过 kimi-webbridge，不打开浏览器"
fi

mkdir -p "$PROJECT_DIR/logs" "$PROJECT_DIR/data"
chmod 700 "$PROJECT_DIR/logs" "$PROJECT_DIR/data"
chmod 600 "$CONFIG"

# ============ 4. 注册开机自启并启动 ============
if [[ "$PLATFORM" == "mac" ]]; then
  PLIST_DIR="$HOME/Library/LaunchAgents"
  PLIST="$PLIST_DIR/$SERVICE_LABEL.plist"
  mkdir -p "$PLIST_DIR"
  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$SERVICE_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$VENV/bin/python</string>
    <string>$PROJECT_DIR/run.py</string>
    <string>$CONFIG</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$PROJECT_DIR</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>5</integer>
  <key>StandardOutPath</key>
  <string>$PROJECT_DIR/logs/ai-relay.log</string>
  <key>StandardErrorPath</key>
  <string>$PROJECT_DIR/logs/ai-relay.log</string>
</dict>
</plist>
EOF
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load -w "$PLIST"
  log "LaunchAgent 已注册并启动（登录自启 + 崩溃自动拉起）: $SERVICE_LABEL"
else
  UNIT_DIR="$HOME/.config/systemd/user"
  UNIT="$UNIT_DIR/ai-relay.service"
  mkdir -p "$UNIT_DIR"
  cat > "$UNIT" <<EOF
[Unit]
Description=ai-relay (webbridge AI Q&A relay)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR
ExecStart=$VENV/bin/python $PROJECT_DIR/run.py $CONFIG
Restart=always
RestartSec=5
StandardOutput=append:$PROJECT_DIR/logs/ai-relay.log
StandardError=append:$PROJECT_DIR/logs/ai-relay.log

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now ai-relay
  if loginctl enable-linger "$USER" 2>/dev/null || sudo -n loginctl enable-linger "$USER" 2>/dev/null; then
    log "systemd 用户服务已启动并设置开机自启（linger 已开启，无需登录）"
  else
    warn "loginctl enable-linger 失败，请手动执行: sudo loginctl enable-linger $USER"
    warn "（不开启 linger 时服务仅在登录后运行，不能开机自启）"
  fi
fi

# ============ 5. 健康检查 ============
log "等待服务就绪 ..."
READY=""
for _ in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    READY="1"; break
  fi
  sleep 1
done
if [[ -n "$READY" ]]; then
  log "✅ ai-relay 已启动: http://127.0.0.1:$PORT/ （监控页）"
else
  err "服务未能通过健康检查，请查看日志: $PROJECT_DIR/logs/ai-relay.log"
  exit 1
fi

cat <<EOF

常用命令：
  查看日志      tail -f $PROJECT_DIR/logs/ai-relay.log
  服务状态      $( [[ "$PLATFORM" == "mac" ]] && echo "launchctl list | grep $SERVICE_LABEL" || echo "systemctl --user status ai-relay" )
  重启服务      $( [[ "$PLATFORM" == "mac" ]] && echo "launchctl kickstart -k gui/\$UID/$SERVICE_LABEL" || echo "systemctl --user restart ai-relay" )
  卸载          $PROJECT_DIR/install.sh uninstall
EOF
