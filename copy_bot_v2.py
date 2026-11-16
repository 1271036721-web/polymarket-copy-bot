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
  - V7 严格门槛（2026-11-16 Dylan 决定用升级后 V7 过滤）：赔率 1.30–1.80、U3 每天同类型
    2 场、否决清单（v7_veto.json）；只拦 BUY；电竞盘中全跳过
    （2026-11-16 Dylan 决定停电竞盘中跟单：cannot_score 单局胜负及
    所有电竞盘口都不跟）
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

# V7 严格门槛用的分类器：复用 v7_observe.py 的 classify / market_type_of，
# 保证门槛和观察打分是同一套口径（import 本身无副作用，只有函数/常量定义）。
# 导入失败时降级为 fail-open（v7_gate 直接放行，不拦单）。
try:
    sys.path.insert(0, str(BASE))
    from v7_observe import classify as v7_classify, market_type_of as v7_market_type_of
except Exception:
    v7_classify = None
    v7_market_type_of = None

# v2 可调参数（config.yaml 里没有就用默认值，不改动 config.yaml）
RTDS_URL = CFG.get("rtds_url", "wss://ws-live-data.polymarket.com")
CLOB_WS_URL = CFG.get("clob_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
SAFETY_POLL_SECONDS = CFG.get("v2_safety_poll_seconds", 15)  # 兜底并行轮询间隔
PRICE_CACHE_MAX_AGE = CFG.get("v2_price_cache_max_age", 10)  # 现价缓存新鲜度（秒）
SETTLE_INTERVAL_SECONDS = CFG.get("v2_settle_interval_seconds", 600)  # 自动结算间隔（秒）
SETTLE_MIN_AGE_SECONDS = CFG.get("v2_settle_min_age_seconds", 300)  # 结算价至少 5 分钟前才认，避免瞬时坏价

# V7 严格门槛（2026-11-16 Dylan 决定：用升级后 V7 严格过滤；赔率窗从 1.15–2.50 收紧到框架的 1.30–1.80）
# 只拦 BUY，不拦 SELL（V7 是买入模型；卖出有 sell_price 校验）。
V7_ODDS_LOW = 1.30    # 隐含赔率下限（1/现价）：框架稳定赔率窗 1.30–1.80
V7_ODDS_HIGH = 1.80   # 隐含赔率上限（2026-11-16 Dylan 决定用升级后 V7 严格过滤：从宽松 1.15–2.50 收紧；
                      # 回测：24 笔 would_fail 里 17 笔在窗外，28 笔已结算亏损里 17 笔在窗外、占亏损 $3,547）
V7_U3_MAX_PER_DAY = 2  # 同一天（UTC）同一目标同一盘口类型最多跟 2 个不同市场
VETO_FILE = BASE / "v7_veto.json"  # 否决清单，v7_observe.py update_vetoes 维护

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

    def sell_price(self, token_id, target_price, market_slug="", outcome_index=None):
        """卖出取价（带校验）：WS 缓存价先对目标卖价，偏离超过 max_slippage
        （默认 0.10）就不可信——CLOB WS 的 ask 偶尔会卡在 $0.99 的假价，
        之前买入已坑过几次，2026-11-15 卖出也中招（目标卖 $0.29，WS 给 $0.99）。
        目标卖价是目标真实成交价，是最可信的锚。顺序：WS 价（偏差 <=0.10 直接用）
        -> HTTP 价（prices-history/Gamma，偏差 <=0.10 用）-> 目标卖价兜底。
        目标卖价缺失时不校验，直接用原价。返回 (price, source)。"""
        now, src = self.price(token_id, market_slug, outcome_index)
        if now is None:
            return None, "none"
        if not target_price:
            return now, src
        if abs(now - target_price) <= CFG["max_slippage"]:
            return now, src
        http_p = self.current_price_http(token_id, market_slug, outcome_index)
        if http_p is not None and abs(http_p - target_price) <= CFG["max_slippage"]:
            log(f"  卖出取价：WS 价 ${now:.2f}（{src}）偏离目标卖价 ${target_price:.2f} 超过 {CFG['max_slippage']}，改用 HTTP 价 ${http_p:.2f}")
            return http_p, "http"
        log(f"  卖出取价：WS 价 ${now:.2f}（{src}）/ HTTP 价 {http_p} 均偏离目标卖价 ${target_price:.2f} 超过 {CFG['max_slippage']}，改用目标卖价")
        return target_price, "target-price"

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
        if not CFG.get("follow_enabled", True):
            return True  # 2026-11-16 Dylan 决定全部停跟单：只记 seen，不跟买入也不跟卖出
        if act.get("side") == "SELL":
            # 跟卖（2026-11-15 Dylan 决定）：目标卖出时按相同比例卖出我们持有的
            # 对应仓位（只减该目标名下的份额），不再死扛到结算。
            if not CFG.get("follow_sell_only_if_holding", True):
                return True
            if str(target.get("address", "")).lower() not in TARGETS_BY_ADDR:
                return True  # 已停跟的目标（如 0xC9BF）的卖单直接忽略
            try:
                self.process_sell(target, act)
            except Exception as e:
                log(f"  跟卖处理异常 {target['name']} {str(act.get('title', ''))[:30]}: {e}")
            return True
        if act.get("side") != "BUY":
            return True  # 其他 side 不处理
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

    # ---------- 跟卖（2026-11-15 Dylan 决定：目标卖出时按比例跟卖） ----------
    # 触发点：ingest_act 收到 SELL 就同步调 process_sell（RTDS 事件和兜底轮询
    # 都在主线程调 ingest_act，所以这里可以直接做 HTTP 查仓，不用再排队）。
    # 卖出比例 = 目标本次卖出份额 / 目标卖出前该 token 总持仓份额
    # （卖出前总量 = 查到的剩余份额 + 本次卖出份额；目标已全出时剩余为 0，
    # 比例就是 1.0，我们名下那份全卖）。卖出不设滑点跳过保护：目标要出场
    # 这个信号本身比价格重要，按当前价直接镜像卖出。
    def fetch_target_position_size(self, address, token_id):
        """查目标在该 token 上的剩余持仓份额（Data API /v2/positions）。
        返回 float（没有持仓返回 0.0）；查不到/失败返回 None（调用方跳过本次卖出）。"""
        try:
            d = self.get_json(
                f"{CFG['data_api_base']}/positions?user={address}&limit=500", timeout=10
            )
        except Exception as e:
            log(f"  跟卖查目标持仓失败: {e}")
            return None
        for row in d.get("data", []):
            if str(row.get("token_id", "")) == str(token_id):
                try:
                    return float(row.get("current_size", row.get("total_size", 0)) or 0)
                except (TypeError, ValueError):
                    return 0.0
        return 0.0

    def target_attribution(self, token, target_name):
        """该目标在我们这笔持仓里的份额：返回 (attributable_size, cost_share)。
        持仓自带 target 字段的直接认；老持仓没有 target 的，按 copy_trades.csv
        里 (title, outcome) 的买入金额占比分摊。"""
        pos = self.state["positions"].get(str(token))
        if not pos:
            return 0.0, 0.0
        owner = pos.get("target") or ""
        if owner == target_name:
            return float(pos["size"]), 1.0
        if owner:
            return 0.0, 0.0  # 整个持仓属于另一个目标
        # 老持仓：按买入金额分摊
        total = 0.0
        mine = 0.0
        try:
            with open(self.log_file, newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("title") == pos["title"] and row.get("outcome") == pos["outcome"]:
                        amt = float(row.get("amount_usd") or 0)
                        total += amt
                        if row.get("target") == target_name:
                            mine += amt
        except Exception as e:
            log(f"  跟卖归属读取 copy_trades.csv 失败: {e}")
            return 0.0, 0.0
        if total <= 0 or mine <= 0:
            return 0.0, 0.0
        share = mine / total
        return float(pos["size"]) * share, share

    def live_sell(self, token_id, amount_usd):
        """实盘卖出（与 live_buy 对称；dry_run=true 时不调用）。"""
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

        async def _sell():
            client = await AsyncSecureClient.create(private_key=pk, wallet=wallet)
            resp = await client.place_market_order(
                asset_id=str(token_id), side="SELL", amount=str(round(amount_usd, 2))
            )
            return resp

        try:
            resp = asyncio.run(_sell())
            if getattr(resp, "ok", False):
                return resp.order_id
            log(f"  实盘卖出失败: {getattr(resp, 'message', resp)}")
            return None
        except Exception as e:
            log(f"  实盘卖出异常: {e}")
            return None

    def append_sell(self, row):
        """卖出记账：单独记 copy_sells.csv（copy_trades.csv 保持 11 列买入口径不变，
        已有的分析脚本不受影响）。卖出文件跟 --log-file 走，和 copy_trades.csv
        放同一目录（测试时指向副本，不会写线上文件）。"""
        sell_file = self.log_file.parent / "copy_sells.csv"
        exists = sell_file.exists()
        with open(sell_file, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not exists:
                w.writeheader()
            w.writerow(row)

    def process_sell(self, target, act):
        """目标卖出 -> 按相同比例卖出我们持有的对应仓位（只减该目标名下的份额）。"""
        name = target["name"]
        token = str(act.get("token_id", ""))
        title = act.get("title", "")
        pos = self.state["positions"].get(token)
        if not pos:
            log(f"{name} 卖出 {title[:40]} {act.get('outcome')} -> 忽略：未持有该 token")
            return
        attr_size, share = self.target_attribution(token, name)
        if attr_size <= 0:
            log(f"{name} 卖出 {title[:40]} {act.get('outcome')} -> 忽略：该持仓不属于 {name}")
            return
        sell_size = float(act.get("size") or 0)
        if sell_size <= 0:
            return
        remaining = self.fetch_target_position_size(target["address"], token)
        if remaining is None:
            log(f"{name} 卖出 {title[:40]} -> 跳过：查不到目标持仓")
            return
        total_before = remaining + sell_size
        fraction = (sell_size / total_before) if total_before > 0 else 1.0
        fraction = min(max(fraction, 0.0), 1.0)
        # 全出（或接近全出）就把我们名下那份全卖了，不留碎屑
        if fraction >= 0.999:
            our_size = attr_size
            fraction = 1.0
        else:
            our_size = attr_size * fraction
        target_price = float(act.get("price") or 0)
        now, src = self.sell_price(token, target_price, act.get("slug", ""), act.get("outcome_index"))
        if now is None:
            log(f"{name} 卖出 {title[:40]} -> 跳过：取不到现价")
            return
        value = our_size * now
        if value < 0.50 and fraction < 1.0:
            log(f"{name} 卖出 {title[:40]} -> 跳过：跟卖金额太小 ${value:.2f}（目标仓位 {fraction:.1%}）")
            return
        attr_cost = pos["cost"] * share
        cost_basis = attr_cost * (our_size / attr_size) if attr_size else 0.0
        ts = datetime.fromtimestamp(act.get("timestamp") or int(time.time()), tz=timezone.utc).strftime("%m-%d %H:%M")
        order_id = None
        if not self.dry_run:
            order_id = self.live_sell(token, value)
            if order_id is None:
                log(f"{name} {ts} 实盘卖出失败，跳过")
                return
        pnl = value - cost_basis
        pos["size"] -= our_size
        pos["cost"] -= cost_basis
        if pos["size"] <= 0.01 or pos["cost"] <= 0.01:
            del self.state["positions"][token]
        self.state["cash"] += value
        self.state["sells"] = self.state.get("sells", 0) + 1
        self.save_state()
        tag = "模拟" if self.dry_run else f"实盘 {order_id}"
        log(f"{name} {ts} [{tag}] 跟卖 {title[:45]} {act.get('outcome')} ${value:.2f} @现价 ${now:.2f}（{src}，目标卖价 ${target_price:.2f}，目标仓位 {fraction:.1%}） 盈亏 ${pnl:+.2f} 现金剩 ${self.state['cash']:.0f}")
        self.append_sell({
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "target": name, "target_tx": act.get("transaction_hash", ""), "title": title,
            "outcome": act.get("outcome", ""), "target_price": round(target_price, 4),
            "exec_price": now, "amount_usd": round(value, 2),
            "size": round(our_size, 2), "dry_run": self.dry_run, "order_id": order_id or "",
            "frac_of_target_position": round(fraction, 4), "pnl": round(pnl, 2),
        })

    # ---------- V7 严格门槛（2026-11-16 Dylan 决定；只拦 BUY） ----------
    def load_vetoes(self):
        """读否决清单（v7_observe.py update_vetoes 维护）。
        文件不存在/损坏时当作空清单，不报错、不拦单。"""
        try:
            d = json.loads(VETO_FILE.read_text())
            if isinstance(d, dict):
                return d.get("vetoes", []) or []
        except Exception:
            pass
        return []

    def v7_u3_count(self, target_name, market_type):
        """今天（UTC）copy_trades.csv 里该目标该盘口类型已跟过的不同市场数。
        同一市场的多笔碎单只算 1 个。文件不存在/读失败按 0 计（放行）。"""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        seen = set()
        try:
            with open(self.log_file, newline="") as f:
                for row in csv.DictReader(f):
                    if row.get("target") != target_name:
                        continue
                    if not (row.get("time_utc") or "").startswith(today):
                        continue
                    if v7_market_type_of is None:
                        return 0
                    if v7_market_type_of(row.get("title", "")) != market_type:
                        continue
                    seen.add((row.get("title", ""), row.get("outcome", "")))
        except Exception:
            return 0
        return len(seen)

    def v7_gate(self, target_name, title, outcome, now):
        """严格版 V7 门槛（只拦 BUY；SELL 不走这里）。
        返回 "" 表示放行，否则返回跳过原因（调用方记日志）。
        顺序：否决清单 -> 电竞盘中跳过 -> 赔率窗 -> U3。
        - 否决清单：(target, market_type) 在 v7_veto.json 里就拦。
        - 电竞盘中：2026-11-16 Dylan 决定停电竞盘中跟单（已实现亏损
          -$3,579 里电竞盘中单局占 -$2,517）。Dota 2 / CS2 / LoL /
          Valorant / KPL 的盘口全跳过，不只单局胜负（Map 让分、
          BO3 系列赛也一样是在盘中下的）；cannot_score 的单局胜负
          （没有局内信息算不出概率）也在其中。赛前电竞系列赛如需
          恢复跟单，再单独放开。
        - 赔率窗：隐含赔率 1/now 在 1.30–1.80 外就拦（框架稳定赔率窗；
          2026-11-16 从宽松 1.15–2.50 收紧）；只对 V7 能打分的
          运动生效（not_applicable 的非体育目标直接放行；标题没带运动
          关键词的 unknown_sport 仍走赔率窗——框架覆盖足球/篮球/UFC 等，
          观察打分也是先查 Gamma 定运动再打分）。
        注意：完整 V7（独立概率 Edge、Confidence/Opportunity 子分、
        红队理由）一次分析要几分钟，跟单执行只要零点几秒，所以门槛
        里只能放进能即时算的规则；完整分析靠观察打分 + 否决清单
        异步回填（v7-copy-observe-10m 每 10 分钟打分 + update_vetoes）。
        - U3：同一天（UTC）同一目标同一盘口类型已跟 2 个不同市场，第 3 个拦。
        分类器复用 v7_observe.py 的 classify / market_type_of（同一套口径）；
        分类器不可用时 fail-open 放行。"""
        if v7_classify is None or v7_market_type_of is None:
            return ""
        kind, sport, sport_name, mt, hint = v7_classify(
            {"target": target_name, "title": title})
        for v in self.load_vetoes():
            if v.get("target") == target_name and v.get("market_type") == mt:
                return f"V7 门槛：否决清单 {target_name}|{mt}（自 {v.get('since', '')}）-> 跳过"
        if kind == "not_applicable":
            return ""
        if sport == "esports":
            return f"V7 门槛：电竞盘中不跟（{sport_name or ''} {mt}）-> 跳过"
        if now and now > 0:
            odds = 1.0 / now
            if odds < V7_ODDS_LOW or odds > V7_ODDS_HIGH:
                return f"V7 门槛：赔率 {odds:.2f} 在 1.30–1.80 窗外 -> 跳过"
        n = self.v7_u3_count(target_name, mt)
        if n >= V7_U3_MAX_PER_DAY:
            return f"V7 门槛：U3 今日 {target_name} {mt} 已跟 {n} 场 -> 跳过"
        return ""

    # ---------- 处理待处理池（v1 run_once 第 2 步） ----------
    def process_pending(self):
        if not CFG.get("follow_enabled", True):
            # 2026-11-16 Dylan 决定全部停跟单：清空待处理池，不执行任何跟单
            n = len(self.state.get("pending", {}))
            if n:
                self.state["pending"] = {}
                log(f"跟单已暂停：清空 {n} 组待处理，不跟单")
            return 0
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
            # 现价先取：V7 严格门槛要用现价算隐含赔率，且门槛要在 decide() 之前跑
            now, src = self.price(p["token_id"], p["slug"], p["outcome_index"])
            if now is None:
                log(f"{name} {ts} {p['title'][:45]} -> 跳过：取不到现价")
                continue
            gate_reason = self.v7_gate(name, p["title"], p["outcome"], now)
            if gate_reason:
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标合计 ${target_usdc:.0f}（{len(p['txs'])} 笔碎单）-> 跳过：{gate_reason}")
                continue
            amount, skip = self.decide(target, act)
            if skip:
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标合计 ${target_usdc:.0f}（{len(p['txs'])} 笔碎单）-> 跳过：{skip}")
                continue
            target_price = float(act["price"] or 0)
            if now < CFG["min_price"] or now > CFG["max_price"]:
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标 ${target_price:.2f} 现价 ${now:.2f}（{src}）超出 [{CFG['min_price']}-{CFG['max_price']}] -> 跳过")
                continue
            if target_price - now > CFG["max_slippage"]:
                # 2026-11-15 换目标部署时发现：补漏轮询会跟进几小时前的旧单，Dota 这种
                # 快结算的市场现价已跌到 $0.001/$0.017（目标买在 $0.52/$0.23），等于买已
                # 定胜负的单子。旧的上滑点保护只拦“现价比目标贵”，现价跌穿目标价也要拦。
                log(f"{name} {ts} {p['title'][:45]} {p['outcome']} 目标 ${target_price:.2f} 现价 ${now:.2f}（{src}） 跌穿 {target_price-now:.2f} > {CFG['max_slippage']} -> 跳过")
                continue
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
                log(f"RTDS: 已订阅 activity/trades（全量推送，本地按 {len(TARGETS_BY_ADDR)} 个钱包过滤）")
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
            # 待订阅 token：已有持仓 + 待处理池（买入路径也会直接加进来）
            for tok in list(self.state.get("positions", {}).keys()):
                self.subscribed_tokens.add(tok)
            for p in self.state.get("pending", {}).values():
                self.subscribed_tokens.add(str(p["token_id"]))
            if not self.subscribed_tokens:
                # 2026-11-16 清空后无持仓：空订阅会被服务端立即断开，
                # 原地是 3 秒一次的重连循环；这里空闲等 5 秒，有新 token
                # 时买入路径加进 subscribed_tokens，5 秒内就能接上。
                self.stop.wait(5)
                continue
            try:
                ws = websocket.create_connection(CLOB_WS_URL, timeout=15)
                log("CLOB WS: 已连接 wss://ws-subscriptions-clob.polymarket.com/ws/market")
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
        if not CFG.get("follow_enabled", True):
            log("⏸️ 跟单已暂停（follow_enabled=false）：不跟任何新单（买入/卖出），只做自动结算；恢复把 config.yaml 的 follow_enabled 改回 true 并重启")
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
            f"现金 ${self.state['cash']:.2f}、持仓 {len(self.state['positions'])} 个、已跟 {self.state['trades']} 笔、已卖 {self.state.get('sells', 0)} 笔")


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
