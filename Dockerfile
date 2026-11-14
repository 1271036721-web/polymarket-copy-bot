# Polymarket 跟单模拟盘机器人 v2 — Cloady 部署用 Dockerfile
# 构建上下文 = 本目录（cloady-deploy/）。部署时须先把正在跑的机器人
# 的 copy_bot_v2.py 和 config.yaml 原样复制到本目录（见 README.md），
# 不要改它们；config.yaml 里必须是 dry_run: true（start.sh 会再校验一次）。
#
# 注意：本镜像不含 state.json。state.json 必须放在 Cloady 持久卷 /data 里，
# 由 start.sh 以 --state-file /data/state.json 传给机器人；容器本地文件
# 系统是临时的，重建/重启后会丢，只有 /data 卷里的东西能活下来。

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖（单独一层：requirements 不变时可复用构建缓存）
COPY requirements.txt .
RUN pip install -r requirements.txt

# 机器人本体 + 配置 + 入口（部署前由 README 的步骤复制进来）
COPY copy_bot_v2.py config.yaml healthcheck.py start.sh ./

# settled_trades.csv 没有 --file 参数，只能写在脚本目录下；
# 用符号链接把它指到持久卷，这样结算记录也不会丢。
# state.json / copy_trades.csv 则由 start.sh 用 --state-file / --log-file
# 直接指向 /data，不走链接。
RUN mkdir -p /data \
    && ln -sfn /data/settled_trades.csv /app/settled_trades.csv \
    && chmod +x /app/start.sh /app/healthcheck.py

# 健康检查服务监听的端口。Cloady 里部署时把服务端口设为 3000
#（或按控制台要求填 $PORT 实际值，healthcheck.py 读 $PORT 环境变量）。
EXPOSE 3000

CMD ["/app/start.sh"]
