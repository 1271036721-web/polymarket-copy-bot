# polymarket-copy-bot (Cloady deploy)

Polymarket 跟单模拟盘机器人 v2 的 Cloady 部署包。

- `copy_bot_v2.py` — 机器人本体：RTDS 推送 + CLOB WS 现价缓存 + 15 秒兜底轮询，
  跟随 TruongMyLan / royalestake / 0xC9BF 三个目标钱包，按 1/20 跟单。
- `config.yaml` — 配置。**`dry_run: true`，只模拟、不真下单。** 实盘需要另外配
  `.env`（`POLYMARKET_PRIVATE_KEY` / `POLYMARKET_WALLET_ADDRESS`），密钥绝不进仓库。
- `start.sh` — 容器入口：校验 dry_run；`/data/state.json` 缺失时不退出，
  先跑 healthcheck 保活、每 10 秒等播种；有 state 后启动机器人，退出 10 秒后重启。
- `healthcheck.py` — 健康检查服务（`GET /`、`/health`，只读 `/data/state.json`）。
- `Dockerfile` / `docker-compose.yml` — 镜像与服务定义；持久卷 `copybot-data` 挂 `/data`，
  `state.json`、`copy_trades.csv`、`settled_trades.csv` 都存在卷里。
- `requirements.txt` — Python 依赖。

部署在 Cloady（美东 USE1），应用以 Repository 方式拉取本仓库。
持仓 state 不在仓库里，通过控制台播种进持久卷。
