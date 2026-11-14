#!/usr/bin/env python3
"""
Polymarket 跟单机器人 v2 - 低延迟版（模拟盘默认，dry_run=true）

v1 的问题：每 5 秒轮询 Data API /activity，三人串行查，查完再逐个 HTTP 查现价，
一轮下来 5-10 秒；改成 2 秒轮询会把 Data API 查到 Read timed out（2026-11-13 实测）。

v2 的三层结构：
  1. 主触发：RTDS 实时推送 wss://ws-live-data.polymarket.com
     订阅 {"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}
     （官方 real-time-data-client 的格式；activity/trades 只能按 event_slug /
     market_slug 过滤，不能按钱包过滤，所以全量推送、本地按 3 个 proxy 钱包过滤。
     实测 2026-11-13：约 58 条/秒，60 秒内命中 2 笔 royalestake 的单。）
     目标交易一到就立即处理，不用等轮询。
  2. 现价缓存：CLOB 市场推送 wss://ws-subscriptions-clob.polymarket.com/ws/market
     订阅 {"assets_ids":[...],"type":"market"}，收 book（asks[0] = 最低卖价）
     和 price_change（best_ask）。缓存 <10 秒直接用，不用每次 HTTP 查
     prices-history；缓存过期/缺失时回退到 v1 的 prices-history + Gamma。
  3. 兜底轮询：每 15 秒用 ThreadPoolExecutor 并行拉 3 人的 /activity
     （只补漏，不做主触发；15 秒 × 并行不会触发 v1 在 2 秒轮询时遇到的限流）。

其余逻辑与 v1 完全相同，可直接接管 v1 的 state.json / copy_trades.csv：
  - 碎单聚合：按 (目标, token) 累计，折合 >= $5 或满 600 秒才处理一次
  - decide() 风控、2 美分滑点保护、市价跟单、dry_run 记账
  - seen_tx 去重（RTDS 和轮询走同一条 ingest，同一 tx 只处理一次）
  - 两个 WS 断线后指数退避重连，重连成功立即拉一次 activity 补漏

用法（测试，不碰正在跑的 v1）：
  cp state.json /tmp/v2test/state.json && cp copy_trades.csv /tmp/v2test/copy_trades.csv
  python3 copy_bot_v2.py --once --dry-run --state-file /tmp/v2test/state.json --log-file /tmp/v2test/copy_trades.csv
  python3 copy_bot_v2.py --dry-run --duration 90 --state-file /tmp/v2test/state.json --log-file /tmp/v2test/copy_trades.csv

正式切换（先停 v1，再跑 v2，用真实 state）：
  bash restart.sh 停掉 v1 后：python3 copy_bot_v2.py
  （或直接用 v2 的 --state-file / --log-file 指向默认 state.json / copy_trades.csv）

依赖：pip install -r requirements.txt   # v2 新增 websocket-client
实盘 SDK：pip install polymarket（与 v1 相同，dry_run=true 时用不到）
"""
import argparse, csv, json, os, queue, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

try:
    import websocket  # websocket-client
except ImportError:
    websocket = None

BASE = Path(__file__).parent
CFG = yaml.safe_load((BASE / "config.yaml").read_text())

# v2 可调参数（config.yaml 里没有就用默认值，不改动 config.yaml）
RTDS_URL = CFG.get("rtds_url", "wss://ws-live-data.polymarket.com")
CLOB_WS_URL = CFG.get("clob_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
SAFETY_POLL_SECONDS = CFG.get("v2_safety_poll_seconds", 15)  # 兜底并行轮询间隔
PRICE_CACHE_MAX_AGE = CFG.get("v2_price_cache_max_age", 10)  # 现价缓存新鲜度（秒）
SETTLE_INTERVAL_SECONDS = CFG.get("v2_settle_interval_seconds", 600)  # 自动结算间隔（秒）
SETTLE_MIN_AGE_SECONDS = CFG.get("v2_settle_min_age_seconds", 300)  # 结算价至少 5 分钟前才认，避免瞬时坏价

TARGETS_BY_ADDR = {t["address"].lower(): t for t in CFG["targets"]}
TARGET_ADDRS_LOWER = list(TARGETS_BY_ADDR.keys())


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%m-%d %H:%M:%S')}Z] {msg}", flush=True)


class Bot:
    def __init__(self, state_file, log_file, dry_run):
        self.state_file = Path(state_file)
        self.log_file = Path(log_file)
        self.settled_file = BASE / "settled_trades.csv"
        self.dry_run = dry_run
        self.state = self.load_state()
        # 事件队列：WS 线程只往里放，主线程统一处理 state（避免多线程写 state）
        self.events = queue.Queue()
        # CLOB 线程的订阅请求队列
        self.clob_sub_queue = queue.Queue()
        # 现价缓存：token -> {"ask": (price, ts), "last": (price, ts)}
        self.price_cache = {}
        self.price_lock = threading.Lock()
        self.subscribed_tokens = set()
        self.stop = threading.Event()
        self.backfill_needed = threading.Event()
        self.stats = {
            "rtds_msgs": 0, "rtds_target_hits": 0, "rtds_latency": [],
            "rtds_reconnects": 0, "clob_reconnects": 0, "clob_msgs": 0,
            "poll_cycles": 0, "poll_errors": 0, "price_cache_hits": 0,
            "price_http_fallbacks": 0,
        }

    # ---------- state / 日志 ----------
    def load_state(self):
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {"cash": CFG["capital_usd"], "positions": {}, "seen_tx": [], "trades": 0}

    def save_state(self):
        s = self.state
        s["seen_tx"] = s["seen_tx"][-5000:]
        self.state_file.write_text(json.dumps(s, indent=2))

    def append_log(self, row):
        exists = self.log_file.exists()
        with open(self.log_file, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                w.writeheader()
            w.writerow(row)

    def append_settled(self, row):
        exists = self.settled_file.exists()
        with open(self.settled_file, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                w.writeheader()
            w.writerow(row)

    # ---------- HTTP（与 v1 相同） ----------
    def get_json(self, url, timeout=10):
        r = requests.get(url, headers={"User-Agent": "PolyCopyBot/1.0"}, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def fetch_activity(self, address, limit=20):
        url = f"{CFG['data_api_base']}/activity?user={address}&limit={limit}"
        return self.get_json(url).get("data", [])

    def current_price_http(self, token_id, market_slug="", outcome_index=None):
        """v1 的取价：prices-history 为主，Gamma 备用。缓存失效时才用。"""
        try:
            now = int(time.time())
            d = self.get_json(
                f"{CFG['data_api_base']}/prices-history?token_id={token_id}&start={now-600}&end={now}",
                timeout=10,
            )
            pts = d.get("data", [])
            if pts:
                return float(pts[-1]["price"])
        except Exception as e:
            log(f"  取价失败 prices-history token={str(token_id)[:12]}..: {e}")
        if market_slug and outcome_index is not None:
            try:
                markets = self.get_json(
                    f"https://gamma-api.polymarket.com/markets?slug={market_slug}", timeout=10
                )
                if markets:
                    prices = json.loads(markets[0].get("outcomePrices", "[]"))
                    return float(prices[int(outcome_index)])
            except Exception as e:
                log(f"  取价失败 gamma slug={market_slug}: {e}")
        return None

    def price(self, token_id, market_slug="", outcome_index=None):
        """先查 WS 现价缓存（<10 秒），过期/缺失再 HTTP。返回 (price, source)。"""
        now = time.time()
        with self.price_lock:
            ent = self.price_cache.get(str(token_id))
        if ent:
            for key in ("ask", "last"):
                p = ent.get(key)
                if p and now - p[1] <= PRICE_CACHE_MAX_AGE:
                    self.stats["price_cache_hits"] += 1
                    return p[0], f"ws-{key}"
        self.stats["price_http_fallbacks"] += 1
        p = self.current_price_http(token_id, market_slug, outcome_index)
        return p, "http"

    # ---------- 实盘下单（与 v1 相同，dry_run 时不调用） ----------
    def live_buy(self, token_id, amount_usd):
        try:
            from polymarket import AsyncSecureClient
            import asyncio
        except ImportError:
            log("  实盘需要 pip install polymarket，先装好再开实盘")
            return None
        pk = os.environ.get("POLYMARKET_PRIVATE_KEY")
        wallet = os.environ.get("POLYMARKET_WALLET_ADDRESS")
        if not pk or not wallet:
            log("  缺 POLYMARKET_PRIVATE_KEY / POLYMARKET_WALLET_ADDRESS，跳过实盘")
            return None

        async def _buy():
            client = await AsyncSecureClient.create(private_key=pk, wallet=wallet)
            resp = await client.place_market_order(
                asset_id=str(token_id), side="BUY", amount=str(round(amount_usd, 2))
            )
            return resp

        try:
            resp = asyncio.run(_buy())
            if getattr(resp, "ok", False):
                return resp.order_id
            log(f"  实盘下单失败: {getattr(resp, 'message', resp)}")
            return None
        except Exception as e:
            log(f"  实盘下单异常: {e}")
            return None

    # ---------- 风控（与 v1 的 decide() 相同） ----------
    def decide(self, target, act):
        if act.get("side") != "BUY":
            return 0.0, "SELL 跳过（只跟买入）"
        title = act.get("title", "")
        for bad in CFG.get("skip_title_contains", []):
            if bad in title:
                return 0.0, f"跳过市场 {bad}"
        usdc = float(act.get("usdc_size") or 0)
        if usdc <= 0:
            return 0.0, "金额为 0"
        amount = usdc * CFG["ratio"]
        if amount < CFG["min_trade_usd"]:
            # 2026-11-13 Dylan 决定改回去：不够最低 $5 就跳过，不按 $5 保底跟
            return 0.0, f"太小 ${amount:.2f} < ${CFG['min_trade_usd']}"
        if amount > CFG["max_per_trade_usd"]:
            amount = CFG["max_per_trade_usd"]
        token = str(act.get("token_id", ""))
        held = self.state["positions"].get(token, {}).get("cost", 0.0)
        if held + amount > CFG["max_per_token_usd"]:
            amount = CFG["max_per_token_usd"] - held
            if amount < CFG["min_trade_usd"]:
                return 0.0, f"该 token 已满 ${held:.0f}"
        total_cost = sum(p["cost"] for p in self.state["positions"].values())
        if total_cost + amount > CFG["max_total_exposure_usd"]:
            return 0.0, f"总仓位已满 ${total_cost:.0f}"
        if amount > self.state["cash"]:
            return 0.0, f"现金不足 ${self.state['cash']:.0f}"
        price = float(act.get("price") or 0)
        if price > CFG["max_price"] or price < CFG["min_price"]:
            return 0.0, f"价格 {price:.2f} 超出 [{CFG['min_price']}-{CFG['max_price']}]"
        return amount, ""

    # ---------- 归入待处理池（v1 run_once 第 1 步，RTDS/轮询共用） ----------
    def ingest_act(self, target, act):
        """act 是 Data API 格式（token_id / usdc_size / size / price / timestamp / slug ...）。
        返回 True 表示新交易（之前没见过），False 表示重复/无效。"""
        tx = act.get("transaction_hash") or ""
        if not tx or tx in self.state["seen_tx"]:
            return False
        self.state["seen_tx"].append(tx)
        if act.get("side") != "BUY":
            return True  # SELL 也记 seen，但不进待处理池
        title = act.get("title", "")
        if any(bad in title for bad in CFG.get("skip_title_contains", [])):
            return True
        token = str(act.get("token_id", ""))
        name = target["name"]
        key = f"{name}|{token}"
        self.state.setdefault("pending", {})
        p = self.state["pending"].setdefault(key, {
            "target": name, "token_id": token, "title": title,
            "outcome": act.get("outcome", ""), "slug": act.get("slug", ""),
            "outcome_index": act.get("outcome_index"),
            "usdc_total": 0.0, "size_total": 0.0, "txs": [],
            "first_ts": act.get("timestamp"),
        })
        usdc = float(act.get("usdc_size") or 0)
        size = float(act.get("size") or 0)
        if usdc > 0:
            p["usdc_total"] += usdc
            p["size_total"] += size
        p["txs"].append(tx)
        # 新 token 出现：让 CLOB 线程订阅它的现价
        if token not in self.subscribed_tokens:
            self.subscribed_tokens.add(token)
            self.clob_sub_queue.put(token)
        return True

    def rtds_to_act(self, payload):
        """RTDS Trade payload -> Data API 格式。RTDS 没有 usdc_size，用 price*size 算。"""
        price = float(payload.get("price") or 0)
        size = float(payload.get("size") or 0)
        return {
            "side": payload.get("side"),
            "title": payload.get("title", ""),
            "token_id": str(payload.get("asset", "")),
            "outcome": payload.get("outcome", ""),
            "outcome_index": payload.get("outcomeIndex"),
            "slug": payload.get("slug", ""),
            "timestamp": payload.get("timestamp"),
            "transaction_hash": payload.get("transactionHash", ""),
            "price": price,
            "size": size,
            "usdc_size": price * size,
        }

    # ---------- 处理待处理池（v1 run_once 第 2 步） ----------
    def process_pending(self):
        new = 0
        self.state.setdefault("pending", {})
        now_ts = int(time.time())
        for key in list(self.state["pending"].keys()):
            p = self.state["pending"][key]
            name = p["target"]
            target = next((t for t in CFG["targets"] if t["name"] == name), None)
            if target is None:
                del self.state["pending"][key]
                continue
            target_usdc = p["usdc_total"]
            age = now_ts - int(p.get("first_ts") or now_ts)
            if target_usdc * CFG["ratio"] < CFG["min_trade_usd"] and age < 600:
                continue  # 还在攒碎单，等下一轮
            del self.state["pending"][key]
            act = {"side": "BUY", "title": p["title"], "outcome": p["outcome"],
                   "token_id": p["token_id"], "slug": p["slug"],
                   "outcome_index": p["outcome_index"],
                   "usdc_size": target_usdc,
                   "price": (target_usdc / p["size_total"] if p["size_total"] else 0)}
            ts = datetime.fromtimestamp(p["first_ts"], tz=timezone.utc).strftime("%m-%d %H:%M")
            amount, skip = self.decide(target, act)
            if skip:
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标合计 ${target_usdc:.0f}（{len(p['txs'])} 笔碎单）-> 跳过：{skip}")
                continue
            now, src = self.price(p["token_id"], p["slug"], p["outcome_index"])
            if now is None:
                log(f"{name} {ts} {p['title'][:45]} -> 跳过：取不到现价")
                continue
            target_price = float(act["price"] or 0)
            if now - target_price > CFG["max_slippage"]:
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标 ${target_price:.2f} 现价 ${now:.2f}（{src}） 滑点 {now-target_price:.2f} > {CFG['max_slippage']} -> 跳过")
                continue
            order_id = None
            if not self.dry_run:
                order_id = self.live_buy(p["token_id"], amount)
                if order_id is None:
                    log(f"{name} {ts} 实盘下单失败，跳过")
                    continue
            token = p["token_id"]
            pos = self.state["positions"].setdefault(token, {"title": p["title"], "outcome": p["outcome"], "target": name, "cost": 0.0, "size": 0.0})
            size = amount / now if now else 0
            pos["cost"] += amount
            pos["size"] += size
            self.state["cash"] -= amount
            self.state["trades"] += 1
            new += 1
            tag = "模拟" if self.dry_run else f"实盘 {order_id}"
            log(f"{name} {ts} [{tag}] 跟买 {p['title'][:45]} {p['outcome']} ${amount:.2f} @现价 ${now:.2f}（{src}，目标 ${target_price:.2f}，{len(p['txs'])} 笔碎单合计 ${target_usdc:.0f}） 现金剩 ${self.state['cash']:.0f}")
            self.append_log({
                "time_utc": datetime.now(timezone.utc).isoformat(),
                "target": name, "target_tx": ",".join(p["txs"][:3]), "title": p["title"],
                "outcome": p["outcome"], "target_price": round(target_price, 4),
                "exec_price": now, "amount_usd": round(amount, 2),
                "size": round(size, 2), "dry_run": self.dry_run, "order_id": order_id or "",
            })
        if new:
            self.save_state()
        return new

    # ---------- 自动结算：已出结果的持仓按 0/1 结算，现金加回，持仓清掉 ----------
    # 2026-11-14 Dylan 要求加：之前机器人从不结算，34 笔死仓把 $8,000 上限顶满，
    # 新单全被跳过，只能手动清。判定沿用人工核对过的办法：
    # 15 分钟窗口无价 -> 24 小时窗口 -> 7 天窗口，最后价恰好是 0.0 或 1.0 才算结算；
    # 0.5 这种长时间没成交的半价不算（ex-INOX 的教训）；最后一笔成交至少 5 分钟前，
    # 避免瞬时坏价。有待处理碎单的 token 本轮跳过。
    def settle_positions(self):
        positions = self.state.get("positions", {})
        if not positions:
            return 0
        pending_tokens = {p.get("token_id") for p in self.state.get("pending", {}).values()}
        now = int(time.time())
        settled = 0
        freed = 0.0
        for token in list(positions.keys()):
            if token in pending_tokens:
                continue
            pos = positions[token]
            last_price = None
            last_ts = 0
            for window in (900, 86400, 604800):
                try:
                    d = self.get_json(
                        f"{CFG['data_api_base']}/prices-history?token_id={token}&start={now - window}&end={now}",
                        timeout=10,
                    )
                except Exception as e:
                    log(f"  结算取价失败 {pos['title'][:30]}: {e}")
                    last_price = None
                    break
                pts = d.get("data", [])
                if not pts:
                    continue  # 这个窗口没成交，看更长的窗口
                last_price = float(pts[-1]["price"])
                last_ts = int(pts[-1].get("timestamp", 0) or 0)
                break
            if last_price is None:
                continue
            if last_price not in (0.0, 1.0):
                continue  # 还在跑（0.5 是没成交的半价，不是结算）
            if last_ts and now - last_ts < SETTLE_MIN_AGE_SECONDS:
                continue  # 刚出结果，等 5 分钟再结
            value = pos["size"] * last_price
            pnl = value - pos["cost"]
            self.state["cash"] += value
            del positions[token]
            settled += 1
            freed += pos["cost"]
            log(f"结算 {pos['title'][:45]} {pos['outcome']} 成本 ${pos['cost']:.2f} -> ${value:.2f} 盈亏 ${pnl:+.2f} 现金 ${self.state['cash']:.0f}")
            self.append_settled({
                "time_utc": datetime.now(timezone.utc).isoformat(),
                "target": pos.get("target", ""),
                "title": pos["title"], "outcome": pos["outcome"],
                "cost": round(pos["cost"], 2), "size": round(pos["size"], 2),
                "settle_price": last_price, "settled_value": round(value, 2),
                "pnl": round(pnl, 2),
            })
        if settled:
            self.save_state()
            log(f"自动结算：本轮结算 {settled} 笔，释放仓位成本 ${freed:.0f}，现金 ${self.state['cash']:.0f}")
        return settled

    # ---------- 兜底轮询：3 人并行拉 activity ----------
    def safety_poll(self):
        self.stats["poll_cycles"] += 1
        new_tx = 0
        with ThreadPoolExecutor(max_workers=3) as ex:
            futs = {ex.submit(self.fetch_activity, t["address"], 20): t for t in CFG["targets"]}
            for fut, target in futs.items():
                try:
                    acts = fut.result(timeout=12)
                except Exception as e:
                    self.stats["poll_errors"] += 1
                    log(f"{target['name']}: 兜底拉取失败 {e}")
                    continue
                for act in reversed(acts):
                    if self.ingest_act(target, act):
                        new_tx += 1
        if new_tx:
            log(f"兜底轮询：新见 {new_tx} 条交易")
            self.save_state()
        return new_tx

    # ---------- RTDS 线程 ----------
    def rtds_loop(self):
        if websocket is None:
            log("RTDS: 缺 websocket-client（pip install websocket-client），RTDS 不可用，只用兜底轮询")
            return
        backoff = 1
        while not self.stop.is_set():
            try:
                ws = websocket.create_connection(RTDS_URL, timeout=15)
                log("RTDS: 已连接 wss://ws-live-data.polymarket.com")
                ws.send(json.dumps({"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]}))
                log("RTDS: 已订阅 activity/trades（全量推送，本地按 3 个钱包过滤）")
                backoff = 1
                self.backfill_needed.set()  # （重）连上先补漏一次
                last_ping = 0.0
                while not self.stop.is_set():
                    if time.time() - last_ping > 5:
                        try:
                            ws.send("PING")
                        except Exception:
                            break
                        last_ping = time.time()
                    ws.settimeout(1)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    except Exception:
                        break
                    if raw in ("PONG", "pong"):
                        continue
                    self.stats["rtds_msgs"] += 1
                    # 快筛：原始文本里没有 3 个目标地址就直接跳过，不解析 JSON
                    low = raw.lower() if isinstance(raw, str) else ""
                    hit_addr = next((a for a in TARGET_ADDRS_LOWER if a in low), None)
                    if not hit_addr:
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    payload = msg.get("payload") if isinstance(msg, dict) else None
                    if not isinstance(payload, dict):
                        continue
                    wallet = str(payload.get("proxyWallet") or "").lower()
                    if wallet not in TARGETS_BY_ADDR:
                        continue
                    recv_ts = time.time()
                    trade_ts = payload.get("timestamp")
                    latency = recv_ts - float(trade_ts) if trade_ts else None
                    if latency is not None:
                        self.stats["rtds_latency"].append(latency)
                    self.stats["rtds_target_hits"] += 1
                    name = TARGETS_BY_ADDR[wallet]["name"]
                    lat_s = f"延迟 {latency:.1f}s" if latency is not None else "延迟未知"
                    log(f"RTDS: 命中 {name} {str(payload.get('title'))[:40]} {payload.get('outcome')} "
                        f"@{payload.get('price')} x{payload.get('size')} {payload.get('side')}（{lat_s}，tx {str(payload.get('transactionHash'))[:12]}..）")
                    self.events.put(("trade", TARGETS_BY_ADDR[wallet], self.rtds_to_act(payload), recv_ts))
                try:
                    ws.close()
                except Exception:
                    pass
            except Exception as e:
                log(f"RTDS: 连接失败 {e}")
            if self.stop.is_set():
                break
            self.stats["rtds_reconnects"] += 1
            log(f"RTDS: {backoff}s 后重连")
            self.stop.wait(backoff)
            backoff = min(backoff * 2, 30)

    # ---------- CLOB 市场推送线程（现价缓存） ----------
    def clob_loop(self):
        if websocket is None:
            log("CLOB WS: 缺 websocket-client，现价缓存不可用，只用 HTTP 取价")
            return
        backoff = 1
        while not self.stop.is_set():
            try:
                ws = websocket.create_connection(CLOB_WS_URL, timeout=15)
                log("CLOB WS: 已连接 wss://ws-subscriptions-clob.polymarket.com/ws/market")
                # 初始订阅：已有持仓 + 待处理池的 token
                for tok in list(self.state.get("positions", {}).keys()):
                    self.subscribed_tokens.add(tok)
                for p in self.state.get("pending", {}).values():
                    self.subscribed_tokens.add(str(p["token_id"]))
                if self.subscribed_tokens:
                    ws.send(json.dumps({"assets_ids": sorted(self.subscribed_tokens), "type": "market"}))
                    log(f"CLOB WS: 已订阅 {len(self.subscribed_tokens)} 个 token 的现价")
                backoff = 1
                last_ping = 0.0
                while not self.stop.is_set():
                    # 新 token 的订阅请求（主线程放入队列）
                    try:
                        while True:
                            self.clob_sub_queue.get_nowait()
                            ws.send(json.dumps({"assets_ids": sorted(self.subscribed_tokens), "type": "market"}))
                            log(f"CLOB WS: 订阅更新为 {len(self.subscribed_tokens)} 个 token")
                    except queue.Empty:
                        pass
                    if time.time() - last_ping > 10:
                        try:
                            ws.send("PING")
                        except Exception:
                            break
                        last_ping = time.time()
                    ws.settimeout(1)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    except Exception:
                        break
                    if raw in ("PONG", "pong"):
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    msgs = msg if isinstance(msg, list) else [msg]
                    for m in msgs:
                        if not isinstance(m, dict):
                            continue
                        self.stats["clob_msgs"] += 1
                        now = time.time()
                        et = m.get("event_type")
                        if et == "price_change":
                            for pc in m.get("price_changes", []):
                                tok = str(pc.get("asset_id", ""))
                                if tok not in self.subscribed_tokens:
                                    continue
                                try:
                                    ask = float(pc.get("best_ask"))
                                except (TypeError, ValueError):
                                    continue
                                with self.price_lock:
                                    self.price_cache.setdefault(tok, {})["ask"] = (ask, now)
                        elif et == "last_trade_price":
                            tok = str(m.get("asset_id", ""))
                            if tok in self.subscribed_tokens:
                                try:
                                    p = float(m.get("price"))
                                except (TypeError, ValueError):
                                    continue
                                with self.price_lock:
                                    self.price_cache.setdefault(tok, {})["last"] = (p, now)
                        elif "bids" in m and "asks" in m:
                            # book 快照：没有 event_type
                            tok = str(m.get("asset_id", ""))
                            if tok in self.subscribed_tokens and m.get("asks"):
                                try:
                                    ask = float(m["asks"][0]["price"])
                                except (TypeError, ValueError, KeyError, IndexError):
                                    continue
                                with self.price_lock:
                                    self.price_cache.setdefault(tok, {})["ask"] = (ask, now)
                try:
                    ws.close()
                except Exception:
                    pass
            except Exception as e:
                log(f"CLOB WS: 连接失败 {e}")
            if self.stop.is_set():
                break
            self.stats["clob_reconnects"] += 1
            log(f"CLOB WS: {backoff}s 后重连")
            self.stop.wait(backoff)
            backoff = min(backoff * 2, 30)

    # ---------- 主循环 ----------
    def run_loop(self, duration=None):
        log(f"启动 跟单机器人 v2 dry_run={self.dry_run} 本金 ${CFG['capital_usd']:.0f} 比例 1/{int(1/CFG['ratio'])} 目标={[t['name'] for t in CFG['targets']]}")
        if not self.dry_run:
            log("⚠️ 实盘模式：会真花钱。确认 .env 已配好、私钥不在代码里。")
        s = self.state
        log(f"当前现金 ${s['cash']:.0f} 持仓 {len(s['positions'])} 个 已跟 {s['trades']} 笔 已见交易 {len(s['seen_tx'])} 条 待处理 {len(s.get('pending', {}))} 组")
        # 启动时先扫一轮结算：重启间隔里出结果的仓位立刻结算、释放上限
        try:
            self.settle_positions()
        except Exception as e:
            log(f"启动结算异常: {e}")
        threads = []
        for target_fn, name in ((self.rtds_loop, "RTDS"), (self.clob_loop, "CLOB")):
            th = threading.Thread(target=target_fn, name=name, daemon=True)
            th.start()
            threads.append(th)
        started = time.time()
        last_poll = 0.0
        last_settle = 0.0
        try:
            while not self.stop.is_set():
                if duration is not None and time.time() - started >= duration:
                    break
                # 1) 处理 WS 来的目标交易
                try:
                    while True:
                        kind, target, act, recv_ts = self.events.get_nowait()
                        if kind == "trade":
                            self.ingest_act(target, act)
                except queue.Empty:
                    pass
                # 2) 兜底轮询：每 15 秒，或（重）连后立即补漏
                now = time.time()
                if self.backfill_needed.is_set() or now - last_poll >= SAFETY_POLL_SECONDS:
                    self.backfill_needed.clear()
                    last_poll = time.time()
                    try:
                        self.safety_poll()
                    except Exception as e:
                        log(f"兜底轮询异常: {e}")
                # 3) 处理待处理池（碎单攒够了就跟）
                try:
                    n = self.process_pending()
                    if n:
                        log(f"本轮新跟 {n} 笔")
                except Exception as e:
                    log(f"处理待处理池异常: {e}")
                # 4) 自动结算：每 10 分钟扫一轮已出结果的持仓
                if now - last_settle >= SETTLE_INTERVAL_SECONDS:
                    last_settle = time.time()
                    try:
                        self.settle_positions()
                    except Exception as e:
                        log(f"自动结算异常: {e}")
                time.sleep(0.5)
        finally:
            self.stop.set()
            for th in threads:
                th.join(timeout=3)
            self.save_state()
            self.print_stats()

    def print_stats(self):
        lat = self.stats["rtds_latency"]
        lat_s = (f"n={len(lat)} 最小 {min(lat):.1f}s 中位 {sorted(lat)[len(lat)//2]:.1f}s 最大 {max(lat):.1f}s"
                 if lat else "无命中，未测到")
        log("v2 统计："
            f"RTDS 收到 {self.stats['rtds_msgs']} 条、命中目标 {self.stats['rtds_target_hits']} 笔、"
            f"RTDS 延迟 {lat_s}、RTDS 重连 {self.stats['rtds_reconnects']} 次、"
            f"CLOB WS 收到 {self.stats['clob_msgs']} 条、重连 {self.stats['clob_reconnects']} 次、"
            f"现价缓存命中 {self.stats['price_cache_hits']} 次、HTTP 回退 {self.stats['price_http_fallbacks']} 次、"
            f"兜底轮询 {self.stats['poll_cycles']} 轮、拉取失败 {self.stats['poll_errors']} 次、"
            f"现金 ${self.state['cash']:.2f}、持仓 {len(self.state['positions'])} 个、已跟 {self.state['trades']} 笔")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="只跑一次兜底轮询就退出（不连 WS）")
    ap.add_argument("--dry-run", action="store_true", help="强制模拟盘")
    ap.add_argument("--live-once", action="store_true", help="实盘跑一次（会真花钱）")
    ap.add_argument("--state-file", default=None, help="state 文件路径（默认 config.yaml 的 state_file；测试时指向副本）")
    ap.add_argument("--log-file", default=None, help="跟单记录 CSV 路径（默认 config.yaml 的 log_file；测试时指向副本）")
    ap.add_argument("--duration", type=float, default=None, help="循环跑 N 秒后退出并打印统计（测试用）")
    args = ap.parse_args()
    dry_run = CFG.get("dry_run", True)
    if args.dry_run:
        dry_run = True
    if args.live_once:
        dry_run = False
        args.once = True
    state_file = args.state_file or (BASE / CFG.get("state_file", "state.json"))
    log_file = args.log_file or (BASE / CFG.get("log_file", "copy_trades.csv"))
    bot = Bot(state_file, log_file, dry_run)
    if args.once:
        log(f"v2 --once dry_run={dry_run} state={state_file}")
        bot.safety_poll()
        n = bot.process_pending()
        bot.settle_positions()
        bot.save_state()
        log(f"本次新跟 {n} 笔，现金 ${bot.state['cash']:.0f}")
        bot.print_stats()
        return
    bot.run_loop(duration=args.duration)


if __name__ == "__main__":
    main()
