#!/bin/bash
# Polymarket 跟单机器人 v2 — Cloady 容器入口（v2：缺 state 时保活等待，不退出）
# 职责（不改机器人本体）：
#  1. 安全检查：config.yaml 必须是 dry_run: true，否则绝不启动机器人；
#     但容器不退出——只跑 healthcheck 保活，方便检查和修复。
#  2. state 等待：/data/state.json 不存在/为空时不退出——先跑 healthcheck
#     保活，每 10 秒检查一次，等文件播种进来再启动机器人。
#     （Cloady 往持久卷写文件要求应用处于运行状态，旧版一启动就退出
#     导致死循环；本版先保活再等 state。）
#     绝不能让机器人拿一个空 state 从 $10,000 重开，那会把正在跑的
#     持仓记录丢掉——所以没 state 时机器人不启动，只等。
#  3. 启动 healthcheck.py（后台），给 Cloady 一个监听端口。
#  4. 循环跑机器人：进程退出就等 10 秒再拉起来（代替 VM 上每 5 分钟的
#     看门狗 cron；Cloady 容器里没有 cron，靠这个循环 + 平台重启兜底）。
set -u
cd /app

BOT_PID=""
HC_PID=""

echo "[start] copy-bot-v2 entrypoint v2, $(date -u '+%Y-%m-%d %H:%M:%S')Z"

# 退出时把后台进程也带走
cleanup() {
  echo "[start] 收到停止信号，正在退出…"
  [ -n "$HC_PID" ] && kill "$HC_PID" 2>/dev/null
  [ -n "$BOT_PID" ] && kill "$BOT_PID" 2>/dev/null
  wait 2>/dev/null
  exit 0
}
trap cleanup TERM INT

# --- 1. dry_run 强制检查（不通过也不退出，只保活，绝不启动机器人）---
DRY_OK=1
if ! grep -qE '^\s*dry_run:\s*true\s*(#.*)?$' /app/config.yaml; then
  echo "[start] FATAL: config.yaml 里 dry_run 不是 true，绝不启动机器人（防误开实盘）" >&2
  DRY_OK=0
else
  echo "[start] dry_run=true 已确认（模拟盘，不会真下单）"
fi

# --- 2. 健康检查服务（后台，先启动，保活）---
python3 /app/healthcheck.py &
HC_PID=$!
echo "[start] healthcheck pid $HC_PID (port ${PORT:-3000})"

# dry_run 不通过：只保活，等人工修复 config.yaml 后再重启容器
if [ "$DRY_OK" != "1" ]; then
  echo "[start] 等待人工修复 config.yaml 后再重启容器"
  while true; do sleep 3600; done
fi

# --- 3. 等 state 播种（不退出，每 10 秒检查一次）---
mkdir -p /data
WAITED=0
while [ ! -s /data/state.json ]; do
  if [ $((WAITED % 6)) -eq 0 ]; then
    echo "[start] 等待 /data/state.json 播种中（已等 $((WAITED * 10)) 秒）——请把 VM 上的 state.json 拷进持久卷 /data"
  fi
  sleep 10
  WAITED=$((WAITED + 1))
done
echo "[start] /data/state.json 已就绪：$(ls -l /data/state.json | awk '{print $5}') bytes"

# settled_trades.csv 走符号链接（Dockerfile 里已建）；没有就建一个空的
[ -e /data/settled_trades.csv ] || : > /data/settled_trades.csv
[ -e /data/copy_trades.csv ] || echo "time_utc,target,target_tx,title,outcome,target_price,exec_price,amount_usd,size,dry_run,order_id" > /data/copy_trades.csv

# --- 4. 机器人主循环 ---
while true; do
  echo "[start] 启动 copy_bot_v2.py --state-file /data/state.json --log-file /data/copy_trades.csv"
  python3 -X faulthandler /app/copy_bot_v2.py \
    --state-file /data/state.json \
    --log-file /data/copy_trades.csv &
  BOT_PID=$!
  wait "$BOT_PID"
  CODE=$?
  echo "[start] copy_bot_v2.py 退出，code=$CODE，10 秒后重启（$(date -u '+%H:%M:%S')Z）"
  sleep 10
done
