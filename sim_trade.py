#!/usr/bin/env python3
"""
sim_trade.py — V7 体育推单模拟盘自营买卖 CLI（Dylan 2026-11-16 15:08 决定）

推比赛的同时，模拟盘同步下单：本金 8000 元人民币（账本约 $1,127，汇率 7.1），金额自己分配，盘中情况不对自己卖。
仍执行 V7 分析系统：只有过 V7 工具（PUSH，或 U9 观察级 WATCH）的推单才买。

账户：与跟单机器人共用同一个 state.json（同一个 8000 元人民币模拟盘、同一笔现金、
同一份持仓）。机器人 save_state() 已改成合并写（磁盘为准的 V7-SELF 仓位 +
现金变化量合并），CLI 用同一把 fcntl 锁读写 state.json，两边不会互相覆盖。
自营仓位标记：target="V7-SELF"、source="sports-pick"，跟单逻辑（跟卖、
V7 门槛、观察打分）永远不碰它们；机器人 settle_positions() 会跳过它们，
结算/退出全归本 CLI 的 exit-check 管。

本 CLI 只有模拟记账，没有任何实盘代码：不 import polymarket SDK，
不读任何私钥/钱包环境变量，不会花真钱。

用法（cron 里调）：
  buy:  python3 sim_trade.py buy --analysis-id 41 --token-id 123... \
          --slug nfl-bal-lac-2026-11-16 --title "Ravens vs Chargers" \
          --outcome "Ravens" --outcome-side yes --price 0.65 --prob-low 0.72 \
          --no-chase-cap 0.76 \
          --kelly-frac 0.14 --sport football --market-type 胜负 --phase 盘前 \
          --polymarket-url https://polymarket.com/event/... \
          --reversal "乌鸦落后则作废" --rev-team Ravens --rev-trailing \
          --espn-league football/nfl --home-team Chargers --away-team Ravens
  sell: python3 sim_trade.py sell --token-id 123... --reason "price-drop"
  settle: python3 sim_trade.py settle
  status: python3 sim_trade.py status
  exit-check: python3 sim_trade.py exit-check   # 退出监控：每 2 分钟跑一次

买入价格保护（V7 系统，Dylan 2026-11-16 15:31 决定：价格保护根据 V7 分析系统来）：
  (a) 每单的不再追上限：买入价不得高于本单分析反转条件里的上限
      （如猛龙 0.76、维京人 0.70、美洲虎 0.75、火箭 0.80），高于就拒单；
      不传 --no-chase-cap 就拒单——没有上限等于没有 V7 价格保护。
  (b) U2：按实时可成交价算 Conservative Edge > 0，否则拒单。
  (c) U1：取不到实时可成交价（CLOB book 与 Gamma 都无 ask）就不买。
  买入只用实时报价（CLOB book bestAsk -> Gamma bestAsk/outcomePrices），
  绝不用 prices-history 上一笔成交价兜底；原来固定的 $0.03 跑价保护已删除。
  卖出不受影响：卖出永远不跳过，取不到价仍用 prices-history 兜底。

退出触发（exit-check）：
  (a) 分析自带的反转条件触发（结构化参数 --rev-lead-min / --rev-trailing，
      按实时比分判断；比分核验不到就跳过本条，不瞎卖）
  (b) 现价 <= 入场价 - 0.15
  (c) 按框架 §3 盘中重算的实时胜/覆盖概率 < 50%（比分+剩余时间+节奏，
      末节垃圾时间修正；比分核验不到或网球/电竞无时钟模型时跳过本条）
  (d) 市场结算价 0.0/1.0（且最后一笔成交 >= 5 分钟前）-> 按机器人同款规则结算
卖出永远不跳过：取不到价就用 prices-history 兜底，再取不到就记错误、下一轮重试。
"""
import argparse, csv, fcntl, json, math, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = Path(__file__).parent
STATE_FILE = BASE / "state.json"
TRADES_CSV = BASE / "copy_trades.csv"
SELLS_CSV = BASE / "copy_sells.csv"
SETTLED_CSV = BASE / "settled_trades.csv"
SIM_DIR = Path.home() / "workspace/goals/goal/hidden_files/sim_trades"
SIM_CSV = SIM_DIR / "sim_trades.csv"
SCANNER_ENV = Path.home() / "workspace/polymarket-scanner/.env"

SELF_TARGET = "V7-SELF"
SELF_SOURCE = "sports-pick"

# CSV 路径默认跟 state.json 同目录（--state-file 指向测试副本时，
# 交易/卖出/结算记录也写进副本目录，绝不污染 live 账本）。
TRADES_CSV = BASE / "copy_trades.csv"
SELLS_CSV = BASE / "copy_sells.csv"
SETTLED_CSV = BASE / "settled_trades.csv"


def use_paths(state_file, sim_dir):
    """把全局 CSV 路径指向本次运行的 state 目录（live 或测试副本）。"""
    global TRADES_CSV, SELLS_CSV, SETTLED_CSV, SIM_CSV
    d = Path(state_file).parent
    TRADES_CSV = d / "copy_trades.csv"
    SELLS_CSV = d / "copy_sells.csv"
    SETTLED_CSV = d / "settled_trades.csv"
    SIM_CSV = Path(sim_dir) / "sim_trades.csv"

# --- 风控上限（Dylan 2026-11-16 决定；与跟单机器人 $150 上限对齐） ---
STAKE_FLOOR = 10.0        # 单笔下限 $10
STAKE_CAP = 150.0         # 单笔上限 $150
PER_TOKEN_CAP = 150.0     # 同一 token 累计成本上限 $150
MAX_OPEN = 3              # 最多 3 个未结算模拟仓位
MAX_PER_GAME = 2          # 同一场比赛最多 2 个仓位
MIN_PRICE, MAX_PRICE = 0.03, 0.97
PRICE_DROP_SELL = 0.15    # (b) 现价比入场价低 0.15 就卖
KELLY_MAX = 0.25          # Quarter Kelly  fraction 合理上限，防单位传错（如传 14 而不是 0.14）
SETTLE_MIN_AGE = 300      # 结算价至少 5 分钟前才认（与机器人同款）
LOCK_TIMEOUT = 15.0

DATA_API = "https://data-api.polymarket.com/v2"
CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

UA = {"User-Agent": "sim-trade/1.0"}

# 每分钟得分波动 σ（分差用，单位：分/√分钟；简化正态近似，只用于退出触发 (c)）
SIGMA_PER_MIN = {
    "basketball": 1.6, "ncaab": 1.6,
    "football": 0.9, "ncaaf": 1.0,
    "soccer": 0.30, "hockey": 0.35,
}
# 每场比赛总时长（分钟）
GAME_MINUTES = {
    "basketball": 48, "ncaab": 40,
    "football": 60, "ncaaf": 60,
    "soccer": 90, "hockey": 60,
}
LEAGUE_SPORT = {  # ESPN league path -> 运动（用于 σ 与时钟）
    "basketball/nba": "basketball", "basketball/mens-college-basketball": "ncaab",
    "basketball/womens-college-basketball": "ncaab",
    "football/nfl": "football", "football/college-football": "ncaaf",
    "soccer/eng.1": "soccer", "soccer/esp.1": "soccer", "soccer/esp.2": "soccer",
    "soccer/ger.1": "soccer", "soccer/ita.1": "soccer", "soccer/fra.1": "soccer",
    "soccer/uefa.champions": "soccer",
    "hockey/nhl": "hockey",
    "tennis/atp": "tennis", "tennis/wta": "tennis",
}


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%m-%d %H:%M:%S')}Z] {msg}", flush=True)


def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


# ---------- state.json 加锁读写（与机器人 save_state() 同一把 fcntl 锁） ----------
class LockedState:
    """持 LOCK_EX 打开 state.json，读出 dict；退出时原地写回（不换 inode）。"""

    def __init__(self, path):
        self.path = Path(path)
        self.f = None
        self.state = None

    def __enter__(self):
        self.f = open(self.path, "r+", encoding="utf-8")
        deadline = time.time() + LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() > deadline:
                    self.f.close()
                    raise TimeoutError(f"state.json 加锁超时 {LOCK_TIMEOUT}s（可能有另一进程在写）")
                time.sleep(0.2)
        self.f.seek(0)
        raw = self.f.read()
        self.state = json.loads(raw) if raw.strip() else {"cash": 0.0, "positions": {}}
        self.state.setdefault("cash", 0.0)
        self.state.setdefault("positions", {})
        self.state.setdefault("seen_tx", [])
        self.state.setdefault("trades", 0)
        self.state.setdefault("sells", 0)
        self.state.setdefault("pending", {})
        return self.state

    def __exit__(self, *exc):
        try:
            if self.state is not None:
                self.f.seek(0)
                self.f.truncate()
                self.f.write(json.dumps(self.state, indent=2))
                self.f.flush()
                os.fsync(self.f.fileno())
        finally:
            fcntl.flock(self.f, fcntl.LOCK_UN)
            self.f.close()
        return False


def read_state(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def bankroll(state):
    return float(state.get("cash", 0.0)) + sum(
        float(p.get("cost", 0.0)) for p in state.get("positions", {}).values()
    )


def self_positions(state):
    return {t: p for t, p in state.get("positions", {}).items()
            if p.get("target") == SELF_TARGET}


# ---------- 取价：CLOB book best_ask -> Gamma outcomePrices -> prices-history ----------
def get_json(url, timeout=10):
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r.json()


def clob_ask(token_id):
    try:
        d = get_json(f"{CLOB}/book?tokenID={token_id}", timeout=8)
        asks = d.get("asks") or []
        if asks:
            return float(asks[0]["price"])
    except Exception as e:
        log(f"  CLOB book 取价失败 token={str(token_id)[:12]}..: {e}")
    return None


def gamma_price(slug, outcome_index):
    try:
        ms = get_json(f"{GAMMA}/markets?slug={slug}", timeout=10)
        if ms:
            prices = json.loads(ms[0].get("outcomePrices") or "[]")
            return float(prices[int(outcome_index)])
    except Exception as e:
        log(f"  Gamma 取价失败 slug={slug}: {e}")
    return None


def history_last_price(token_id, window=900):
    """prices-history 最近一笔成交价（+时间戳）。取不到返回 (None, 0)。"""
    now = int(time.time())
    try:
        d = get_json(
            f"{DATA_API}/prices-history?token_id={token_id}&start={now - window}&end={now}",
            timeout=10,
        )
        pts = d.get("data", []) or []
        if pts:
            return float(pts[-1]["price"]), int(pts[-1].get("timestamp", 0) or 0)
    except Exception as e:
        log(f"  prices-history 取价失败 token={str(token_id)[:12]}..: {e}")
    return None, 0


def gamma_best_ask(slug, outcome_index):
    """Gamma 实时可成交价：bestAsk 为主，取不到用 outcomePrices。取不到返回 None。"""
    try:
        ms = get_json(f"{GAMMA}/markets?slug={slug}", timeout=10)
        if ms:
            m = ms[0]
            ba = m.get("bestAsk")
            if ba is not None:
                return float(ba)
            prices = json.loads(m.get("outcomePrices") or "[]")
            return float(prices[int(outcome_index)])
    except Exception as e:
        log(f"  Gamma 取价失败 slug={slug}: {e}")
    return None


def live_ask_price(token_id, slug="", outcome_index=None):
    """买入用实时可成交价（V7 U1）：CLOB book bestAsk 为主；
    与 Gamma 偏离 > 0.10 时视为假价（CLOB 偶尔卡 $0.99，跟单机器人已中招两次）
    改用 Gamma；两者都没有 -> (None, 'none')。
    绝不用 prices-history：上一笔成交价不是实时可成交报价。"""
    ask = clob_ask(token_id)
    g = None
    if slug and outcome_index is not None:
        g = gamma_best_ask(slug, outcome_index)
    if ask is not None and g is not None and abs(ask - g) > 0.10:
        log(f"  取价：CLOB ask ${ask:.3f} 与 Gamma ${g:.3f} 偏离 > 0.10，改用 Gamma")
        return g, "gamma"
    if ask is not None:
        return ask, "clob"
    if g is not None:
        return g, "gamma"
    return None, "none"


def validated_price(token_id, slug="", outcome_index=None):
    """卖出用的校验价：CLOB ask 为主；与 prices-history 偏离 > 0.10
    就不可信（CLOB WS/Book 偶尔卡 $0.99 假价，跟单机器人 2026-11-15
    已中招两次），改用 prices-history；再取不到用 Gamma。返回 (price, source)。
    买入不用它（买入走 live_ask_price，绝不用历史价）。"""
    ask = clob_ask(token_id)
    hist, _ = history_last_price(token_id)
    if ask is not None and hist is not None and abs(ask - hist) > 0.10:
        log(f"  取价：CLOB ask ${ask:.3f} 与成交价 ${hist:.3f} 偏离 > 0.10，改用成交价")
        return hist, "history"
    if ask is not None:
        return ask, "clob"
    if hist is not None:
        return hist, "history"
    if slug and outcome_index is not None:
        g = gamma_price(slug, outcome_index)
        if g is not None:
            return g, "gamma"
    return None, "none"


# ---------- CSV 记录 ----------
def append_csv(path, fieldnames, row):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})


SIM_FIELDS = ["time_cst", "action", "analysis_id", "market", "title", "outcome",
              "side", "price", "size", "amount_usd", "pnl_usd", "reason", "balance_after"]
BUY_FIELDS = ["time_utc", "target", "target_tx", "title", "outcome", "target_price",
              "exec_price", "amount_usd", "size", "dry_run", "order_id"]
SELL_FIELDS = ["time_utc", "target", "target_tx", "title", "outcome", "target_price",
               "exec_price", "amount_usd", "size", "dry_run", "order_id",
               "frac_of_target_position", "pnl"]
SETTLE_FIELDS = ["time_utc", "target", "title", "outcome", "cost", "size",
                 "settle_price", "settled_value", "pnl"]


def now_cst():
    return datetime.now(timezone.utc).astimezone(
        timezone(__import__("datetime").timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def sim_log(action, pos, price, size, amount, pnl, reason, balance):
    append_csv(SIM_CSV, SIM_FIELDS, {
        "time_cst": now_cst(), "action": action,
        "analysis_id": pos.get("analysis_id", ""),
        "market": pos.get("slug", ""), "title": pos.get("title", ""),
        "outcome": pos.get("outcome", ""), "side": pos.get("outcome_side", ""),
        "price": round(float(price), 4) if price is not None else "",
        "size": round(float(size), 4) if size else "",
        "amount_usd": round(float(amount), 2) if amount is not None else "",
        "pnl_usd": round(float(pnl), 2) if pnl is not None else "",
        "reason": reason, "balance_after": round(float(balance), 2),
    })


# ---------- 买入 ----------
def cmd_buy(a):
    use_paths(a.state_file, a.sim_dir)
    token = str(a.token_id)
    if not (0 < a.kelly_frac <= KELLY_MAX):
        return fail(f"kelly-frac {a.kelly_frac} 不在 (0, {KELLY_MAX}]，拒绝（防单位传错）")
    if not (MIN_PRICE <= a.price <= MAX_PRICE):
        return fail(f"复核价 ${a.price:.3f} 超出 [{MIN_PRICE}-{MAX_PRICE}]，拒绝")
    if not (0 < a.prob_low < 1):
        return fail(f"prob-low {a.prob_low} 非 (0,1)，拒绝")
    cap = getattr(a, "no_chase_cap", None)
    if cap is None or not (0 < cap < 1):
        return fail("未传有效的 --no-chase-cap（分析反转条件里的不再追上限），无 V7 价格保护，拒绝买入")
    # 买前重新取实时可成交价，按 V7 价格保护：(a) 不高于本单的不再追上限；
    # (b) U2：按现价算 Edge > 0；(c) U1：取不到实时可成交价就不买。
    ask, src = live_ask_price(token, a.slug, a.outcome_index)
    if ask is None:
        return fail("无实时可成交价（CLOB book 与 Gamma 都取不到 ask），按 U1 不买")
    if ask > cap:
        return fail(f"现价 ${ask:.3f}（{src}）高于不再追上限 ${cap:.3f}，按 V7 不买")
    if a.prob_low - ask <= 0:
        return fail(f"按现价 ${ask:.3f} 算 Edge = {a.prob_low:.3f}-{ask:.3f} <= 0，不买")
    with LockedState(a.state_file) as st:
        self_pos = self_positions(st)
        if len(self_pos) >= MAX_OPEN:
            return fail(f"已有 {len(self_pos)} 个模拟仓位，达到上限 {MAX_OPEN}，不买")
        same_game = [p for p in self_pos.values() if p.get("slug") == a.slug]
        if len(same_game) >= MAX_PER_GAME:
            return fail(f"同一场 {a.slug} 已有 {len(same_game)} 个仓位，达到上限 {MAX_PER_GAME}，不买")
        if token in st["positions"] and st["positions"][token].get("target") == SELF_TARGET:
            return fail(f"该 token 已持有模拟仓位（同一 outcome 不重复买）")
        held = float(st["positions"].get(token, {}).get("cost", 0.0))
        br = bankroll(st)
        stake = a.kelly_frac * br
        stake = max(STAKE_FLOOR, min(STAKE_CAP, stake))
        if held + stake > PER_TOKEN_CAP:
            stake = PER_TOKEN_CAP - held
            if stake < STAKE_FLOOR:
                return fail(f"该 token 已持 ${held:.0f}，加仓后超 ${PER_TOKEN_CAP} 上限，不买")
        if stake > float(st["cash"]):
            return fail(f"现金 ${float(st['cash']):.2f} 不够 ${stake:.2f}，不买")
        size = stake / ask
        pos = {
            "title": a.title, "outcome": a.outcome, "target": SELF_TARGET,
            "cost": round(stake, 2), "size": round(size, 4),
            "source": SELF_SOURCE, "analysis_id": str(a.analysis_id),
            "slug": a.slug, "market_type": a.market_type, "sport": a.sport,
            "phase": a.phase, "entry_price": round(ask, 4),
            "entry_time_utc": datetime.now(timezone.utc).isoformat(),
            "prob_low": a.prob_low, "kelly_frac": a.kelly_frac,
            "no_chase_cap": cap, "stake_usd": round(stake, 2), "reversal": a.reversal or "",
            "rev_lead_min": a.rev_lead_min, "rev_trailing": bool(a.rev_trailing),
            "rev_team": a.rev_team or "",
            "espn_league": a.espn_league or "", "home_team": a.home_team or "",
            "away_team": a.away_team or "", "line": a.line,
            "total_side": a.total_side or "", "outcome_side": a.outcome_side or "yes",
            "outcome_index": a.outcome_index,
            "polymarket_url": a.polymarket_url or "",
        }
        st["positions"][token] = pos
        st["cash"] = round(float(st["cash"]) - stake, 2)
        st["trades"] = int(st.get("trades", 0)) + 1
        bal = bankroll(st)
    ts = datetime.now(timezone.utc).isoformat()
    append_csv(TRADES_CSV, BUY_FIELDS, {
        "time_utc": ts, "target": SELF_TARGET, "target_tx": f"sim:{a.analysis_id}",
        "title": a.title, "outcome": a.outcome, "target_price": round(a.price, 4),
        "exec_price": round(ask, 4), "amount_usd": round(stake, 2),
        "size": round(size, 4), "dry_run": True,
        "order_id": f"sim-buy-{a.analysis_id}-{int(time.time())}",
    })
    sim_log("BUY", pos, ask, size, stake, None, f"V7 分析 {a.analysis_id} 过关", bal)
    log(f"模拟盘买入 [{a.analysis_id}] {a.title[:40]} {a.outcome} "
        f"${stake:.2f} @ ${ask:.3f}（{src}） 现金剩 ${float(read_state(a.state_file)['cash']):.2f}")
    return ok({"bought": True, "stake_usd": round(stake, 2), "price": round(ask, 4),
               "size": round(size, 4), "cash": round(float(read_state(a.state_file)["cash"]), 2),
               "bankroll": round(bal, 2), "analysis_id": str(a.analysis_id)})


# ---------- 卖出（整仓市价卖，永远不跳过） ----------
def do_sell(st, token, reason, price_hint=None):
    """在已加锁的 state 上执行卖出。返回 (sold_dict | None, error | None)。"""
    pos = st["positions"].get(str(token))
    if not pos or pos.get("target") != SELF_TARGET:
        return None, f"token {str(token)[:12]}.. 无模拟仓位"
    price, src = validated_price(str(token), pos.get("slug", ""),
                                 pos.get("outcome_index"))
    if price is None and price_hint:
        price, src = float(price_hint), "hint"
    if price is None:
        return None, "取不到卖价，下一轮重试（不瞎卖）"
    proceeds = float(pos["size"]) * price
    pnl = proceeds - float(pos["cost"])
    ts = datetime.now(timezone.utc).isoformat()
    append_csv(SELLS_CSV, SELL_FIELDS, {
        "time_utc": ts, "target": SELF_TARGET, "target_tx": f"sim:{pos.get('analysis_id','')}",
        "title": pos["title"], "outcome": pos["outcome"],
        "target_price": pos.get("entry_price", ""), "exec_price": round(price, 4),
        "amount_usd": round(proceeds, 2), "size": round(float(pos["size"]), 4),
        "dry_run": True, "order_id": f"sim-sell-{pos.get('analysis_id','')}-{int(time.time())}",
        "frac_of_target_position": 1.0, "pnl": round(pnl, 2),
    })
    del st["positions"][str(token)]
    st["cash"] = round(float(st["cash"]) + proceeds, 2)
    st["sells"] = int(st.get("sells", 0)) + 1
    bal = bankroll(st)
    sim_log("SELL", pos, price, float(pos["size"]), proceeds, pnl, reason, bal)
    log(f"模拟盘卖出 [{pos.get('analysis_id','')}] {pos['title'][:40]} {pos['outcome']} "
        f"${proceeds:.2f} @ ${price:.3f}（{src}） 盈亏 ${pnl:+.2f} 原因：{reason} 现金 ${float(st['cash']):.2f}")
    return {"sold": True, "analysis_id": str(pos.get("analysis_id", "")),
            "title": pos["title"], "outcome": pos["outcome"],
            "entry_price": pos.get("entry_price"), "sell_price": round(price, 4),
            "proceeds_usd": round(proceeds, 2), "pnl_usd": round(pnl, 2),
            "reason": reason, "cash": round(float(st["cash"]), 2),
            "bankroll": round(bal, 2)}, None


def cmd_sell(a):
    use_paths(a.state_file, a.sim_dir)
    with LockedState(a.state_file) as st:
        sold, err = do_sell(st, a.token_id, a.reason or "manual", a.price)
    if err:
        return fail(err)
    return ok(sold)


# ---------- 结算（机器人同款规则，只结 V7-SELF 仓位） ----------
def settle_one(st, token):
    pos = st["positions"].get(str(token))
    if not pos or pos.get("target") != SELF_TARGET:
        return None
    now = int(time.time())
    last_price, last_ts = None, 0
    for window in (900, 86400, 604800):
        p, ts = history_last_price(str(token), window)
        if p is None:
            continue
        last_price, last_ts = p, ts
        break
    if last_price is None or last_price not in (0.0, 1.0):
        return None
    if last_ts and now - last_ts < SETTLE_MIN_AGE:
        return None
    value = float(pos["size"]) * last_price
    pnl = value - float(pos["cost"])
    ts = datetime.now(timezone.utc).isoformat()
    append_csv(SETTLED_CSV, SETTLE_FIELDS, {
        "time_utc": ts, "target": SELF_TARGET, "title": pos["title"],
        "outcome": pos["outcome"], "cost": round(float(pos["cost"]), 2),
        "size": round(float(pos["size"]), 4), "settle_price": last_price,
        "settled_value": round(value, 2), "pnl": round(pnl, 2),
    })
    del st["positions"][str(token)]
    st["cash"] = round(float(st["cash"]) + value, 2)
    bal = bankroll(st)
    sim_log("SETTLE", pos, last_price, float(pos["size"]), value, pnl, "市场结算", bal)
    log(f"模拟盘结算 [{pos.get('analysis_id','')}] {pos['title'][:40]} {pos['outcome']} "
        f"成本 ${float(pos['cost']):.2f} -> ${value:.2f} 盈亏 ${pnl:+.2f}")
    return {"settled": True, "analysis_id": str(pos.get("analysis_id", "")),
            "title": pos["title"], "outcome": pos["outcome"],
            "settle_price": last_price, "settled_value": round(value, 2),
            "pnl_usd": round(pnl, 2), "cash": round(float(st["cash"]), 2),
            "bankroll": round(bal, 2)}


def cmd_settle(a):
    use_paths(a.state_file, a.sim_dir)
    out = []
    with LockedState(a.state_file) as st:
        for token in list(self_positions(st).keys()):
            r = settle_one(st, token)
            if r:
                out.append(r)
    return ok({"settled": out, "count": len(out)})


def cmd_status(a):
    use_paths(a.state_file, a.sim_dir)
    st = read_state(a.state_file)
    pos = self_positions(st)
    realized = 0.0
    if SIM_CSV.exists():
        with open(SIM_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("action") in ("SELL", "SETTLE") and row.get("pnl_usd"):
                    try:
                        realized += float(row["pnl_usd"])
                    except ValueError:
                        pass
    return ok({"cash": round(float(st["cash"]), 2),
               "bankroll": round(bankroll(st), 2),
               "open_count": len(pos),
               "realized_pnl_usd": round(realized, 2),
               "positions": [
                   {"analysis_id": p.get("analysis_id"), "title": p.get("title"),
                    "outcome": p.get("outcome"), "entry_price": p.get("entry_price"),
                    "cost": p.get("cost"), "size": p.get("size"),
                    "slug": p.get("slug")} for p in pos.values()]})


def ok(d):
    print(json.dumps({"ok": True, **d}, ensure_ascii=False))
    return 0


def fail(msg):
    print(json.dumps({"ok": False, "error": msg}, ensure_ascii=False))
    log(f"模拟盘拒绝/失败：{msg}")
    return 1


# ---------- 实时比分 ----------
def _parse_clock(clock):
    """'12:00' / '05:32' -> 分钟数；'67'' -> 67；解析失败返回 None。"""
    if clock is None:
        return None
    s = str(clock).strip().replace("'", "")
    m = re.match(r"^(\d+):(\d+)$", s)
    if m:
        return int(m.group(1)) + int(m.group(2)) / 60.0
    m = re.match(r"^(\d+)$", s)
    if m:
        return float(m.group(1))
    return None


def espn_score(league, home_team, away_team):
    """ESPN scoreboard 直查。返回 dict 或 None：
    {home, away, home_score, away_score, period, clock_min, minutes_remaining,
     state, started}。state: pre/in/post。"""
    try:
        d = get_json(f"{ESPN_BASE}/{league}/scoreboard", timeout=10)
    except Exception as e:
        log(f"  ESPN 比分失败 {league}: {e}")
        return None
    for ev in d.get("events", []):
        comps = ev.get("competitions", [{}])[0]
        teams = comps.get("competitors", [])
        if len(teams) < 2:
            continue
        names = [(t.get("team", {}).get("shortDisplayName", "") or "") for t in teams]
        ht = (home_team or "").lower()
        at = (away_team or "").lower()
        hit = sum(1 for n in names if ht and ht in n.lower()) + \
            sum(1 for n in names if at and at in n.lower())
        if hit < 2 and not (ht and at and all(
                (ht in n.lower()) or (at in n.lower()) for n in names)):
            continue
        st = comps.get("status", {})
        typ = st.get("type", {})
        state = typ.get("state", "")
        period = int(st.get("period") or 0)
        clock_min = _parse_clock(st.get("displayClock"))
        scores = {}
        for t in teams:
            side = t.get("homeAway", "")
            try:
                scores[side] = float(t.get("score"))
            except (TypeError, ValueError):
                scores[side] = None
        sport = LEAGUE_SPORT.get(league, "")
        total_min = GAME_MINUTES.get(sport, 0)
        mins_rem = None
        if state == "in" and total_min:
            if sport == "soccer":
                mins_rem = max(0.0, 90.0 - (clock_min or 0))
            elif sport in ("basketball", "ncaab"):
                per = 12.0 if sport == "basketball" else 10.0
                if period <= 4:
                    mins_rem = (4 - period) * per + (clock_min or 0)
                else:
                    mins_rem = 5.0 - (clock_min or 0) if clock_min is not None else 2.5
            elif sport in ("football", "ncaaf"):
                if period <= 4:
                    mins_rem = (4 - period) * 15.0 + (clock_min or 0)
                else:
                    mins_rem = 5.0 - (clock_min or 0) if clock_min is not None else 2.5
            elif sport == "hockey":
                if period <= 3:
                    mins_rem = (3 - period) * 20.0 + (clock_min or 0)
                else:
                    mins_rem = 5.0 - (clock_min or 0) if clock_min is not None else 2.5
            mins_rem = max(0.0, mins_rem)
        return {"home": names[0], "away": names[1],
                "home_score": scores.get("home"), "away_score": scores.get("away"),
                "period": period, "clock_min": clock_min,
                "minutes_remaining": mins_rem, "state": state,
                "started": state in ("in", "post")}
    return None


def pandascore_key():
    try:
        for line in SCANNER_ENV.read_text().splitlines():
            if line.startswith("PANDASCORE_API_KEY="):
                return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


def pandascore_score(game, home_team, away_team):
    """PandaScore 进行中的比赛。返回 {home_score, away_score, state} 或 None。
    令牌只读内存，绝不打印。"""
    key = pandascore_key()
    if not key:
        return None
    try:
        r = requests.get(f"https://api.pandascore.co/{game}/matches/running",
                         headers={"Authorization": f"Bearer {key}"},
                         timeout=10, params={"page[size]": 50})
        r.raise_for_status()
        for m in r.json():
            opps = m.get("opponents", []) or []
            names = [(o.get("opponent", {}).get("name", "") or "") for o in opps]
            if len(names) < 2:
                continue
            ht = (home_team or "").lower()
            at = (away_team or "").lower()
            if not all(any((x and x in n.lower()) for n in names) for x in (ht, at)):
                continue
            res = m.get("results", []) or []
            scores = {r.get("team_id"): r.get("score") for r in res}
            out = {"state": "in", "started": True, "minutes_remaining": None,
                   "period": None, "clock_min": None}
            for o in opps:
                nm = o.get("opponent", {}).get("name", "")
                sc = scores.get(o.get("opponent", {}).get("id"))
                if ht and ht in nm.lower():
                    out["home_score"] = sc
                    out["home"] = nm
                elif at and at in nm.lower():
                    out["away_score"] = sc
                    out["away"] = nm
            return out
    except Exception as e:
        log(f"  PandaScore 比分失败 {game}: {e}")
    return None


def fetch_score(pos):
    """按仓位元数据取比分。返回 (score_dict | None, unverified_reason | '')."""
    league = pos.get("espn_league", "") or ""
    if league.startswith("pandascore:"):
        game = league.split(":", 1)[1]
        s = pandascore_score(game, pos.get("home_team", ""), pos.get("away_team", ""))
        return (s, "" if s else "PandaScore 未找到该场比赛")
    if league:
        s = espn_score(league, pos.get("home_team", ""), pos.get("away_team", ""))
        return (s, "" if s else f"ESPN {league} 未找到该场比赛")
    return None, "无比分源（网球挑战赛/KPL 等暂无 API）"


def _our_side(pos, score):
    """我方是主队还是客队：按买入时存的 outcome / rev_team 与比分里的
    队伍名做包含匹配（outcome 可能带盘口，如 'Ravens -6.5'，只取首段）。
    返回 True=主队，False=客队，None=匹配不到。"""
    home = (score.get("home") or "").lower()
    away = (score.get("away") or "").lower()
    for cand in (pos.get("rev_team", ""), pos.get("outcome", "")):
        c = (cand or "").strip().lower()
        if not c:
            continue
        token = c.split()[0].strip(",")
        if token and token in home:
            return True
        if token and token in away:
            return False
    ht = (pos.get("home_team", "") or "").lower()
    at = (pos.get("away_team", "") or "").lower()
    if ht and ht in home:
        return None  # 只知道主客队名、不知道我方是哪边时不猜
    return None


# ---------- 盘中概率重算（框架 §3 简化版，只用于退出触发 (c)） ----------
def live_prob(pos, score):
    """返回我方获胜/覆盖/大小分方向的实时概率（0-1），算不出返回 None。
    方法：剩余时间 × 比赛至今的节奏重算期望分差/总分，末节垃圾时间修正
    （NBA 领先方 ×0.85/落后方 ×1.15；NFL 领先方 ×0.7/落后方 ×1.4），
    正态近似 Φ((期望-线)/σ√剩余时间)。加时且分差在 1 个球权内按 50% 算。"""
    if not score or score.get("state") != "in":
        return None
    sport = LEAGUE_SPORT.get(pos.get("espn_league", ""), "") or pos.get("sport", "")
    if sport in ("tennis", "esports") or sport not in SIGMA_PER_MIN:
        return None
    hs, aws = score.get("home_score"), score.get("away_score")
    if hs is None or aws is None:
        return None
    t_rem = score.get("minutes_remaining")
    if t_rem is None or t_rem <= 0:
        return None
    total_min = GAME_MINUTES.get(sport, 60)
    elapsed = max(1.0, total_min - t_rem)
    mt = (pos.get("market_type") or "")
    line = pos.get("line")
    side = (pos.get("outcome_side") or "yes").lower()
    if "大小" in mt or "total" in mt.lower() or "over" in mt.lower() or "under" in mt.lower():
        if line is None:
            return None
        our_pace = hs / elapsed
        opp_pace = aws / elapsed
        exp_total = (hs + aws) + (our_pace + opp_pace) * t_rem
        sig = SIGMA_PER_MIN[sport]
        p_over = _norm_cdf((exp_total - float(line)) / (sig * math.sqrt(2 * t_rem)))
        p = p_over if (pos.get("total_side") or "over").lower() == "over" else 1 - p_over
        return max(0.01, min(0.99, p))
    our_home = _our_side(pos, score)
    if our_home is None:
        return None
    our_score = hs if our_home else aws
    opp_score = aws if our_home else hs
    lead = our_score - opp_score
    # 节奏：至今每分钟得分
    our_pace = our_score / elapsed
    opp_pace = opp_score / elapsed
    # 末节垃圾时间修正（框架 §3）
    period = score.get("period") or 0
    late = (sport in ("basketball", "ncaab") and period >= 4) or \
           (sport in ("football", "ncaaf") and period >= 4) or \
           (sport == "hockey" and period >= 3)
    if late:
        lf, tf = (0.85, 1.15) if sport in ("basketball", "ncaab") else (0.7, 1.4)
        if lead > 0:
            our_pace, opp_pace = our_pace * lf, opp_pace * tf
        elif lead < 0:
            our_pace, opp_pace = our_pace * tf, opp_pace * lf
    sig = SIGMA_PER_MIN[sport]
    if "让分" in mt or "spread" in mt.lower():
        if line is None:
            return None
        exp_margin = lead + (our_pace - opp_pace) * t_rem
        p = _norm_cdf((exp_margin - float(line)) / (sig * math.sqrt(t_rem)))
    else:  # 胜负
        exp_margin = lead + (our_pace - opp_pace) * t_rem
        p = _norm_cdf(exp_margin / (sig * math.sqrt(t_rem)))
    if side == "no":
        p = 1 - p
    # 加时 1 球权内按 50%（框架 §3：加时里让分基本是抛硬币）
    if period >= 5 and abs(lead) <= 3:
        p = 0.5
    return max(0.01, min(0.99, p))


# ---------- 退出监控 ----------
def check_position(st, token, pos):
    """返回 (action, reason, detail)：action in (None, 'sell', 'settle')。
    顺序：(d) 结算价 -> (a) 反转 / (c) 概率（只看比分，不需要现价也能判断；
    取不到现价时卖出执行会失败、下一轮重试）-> (b) 现价暴跌。"""
    entry = float(pos.get("entry_price") or 0)
    # (d) 先看结算：最后价恰好 0/1 且 >= 5 分钟前
    now = int(time.time())
    last_price, last_ts = None, 0
    for window in (900, 86400, 604800):
        p, ts = history_last_price(token, window)
        if p is None:
            continue
        last_price, last_ts = p, ts
        break
    if last_price in (0.0, 1.0):
        if not last_ts or now - last_ts >= SETTLE_MIN_AGE:
            return "settle", "市场已结算", f"结算价 {last_price}"
        return None, "", f"结算价 {last_price} 但最后一笔成交 < 5 分钟，等下一轮"
    # (a)/(c) 先看比分：不需要现价也能判断
    score, unverified = fetch_score(pos)
    prob = None
    score_note = ""
    if score is not None and score.get("state") == "in":
        hs, aws = score.get("home_score"), score.get("away_score")
        # (a) 反转条件（结构化参数）
        rev_team = (pos.get("rev_team") or "").lower()
        if hs is not None and aws is not None and rev_team:
            home_name = (score.get("home", "") or "").lower()
            rev_home = rev_team in home_name
            rev_score = hs if rev_home else aws
            opp_score = aws if rev_home else hs
            rev_lead = rev_score - opp_score
            if pos.get("rev_lead_min") is not None and rev_lead < float(pos["rev_lead_min"]):
                return "sell", "reversal", (
                    f"反转触发：{pos.get('rev_team')} 领先只剩 {rev_lead:.0f} 分，"
                    f"低于反转线 {pos['rev_lead_min']} 分（{score.get('home')} {hs:.0f}-{aws:.0f} {score.get('away')}）")
            if pos.get("rev_trailing") and rev_lead < 0:
                period = score.get("period") or 0
                past_half = period >= 3 or (pos.get("sport") == "soccer" and period >= 2)
                if past_half:
                    return "sell", "reversal", (
                        f"反转触发：{pos.get('rev_team')} 已落后 {rev_lead:.0f} 分"
                        f"（{score.get('home')} {hs:.0f}-{aws:.0f} {score.get('away')}）")
        # (c) 实时概率 < 50%
        prob = live_prob(pos, score)
        if prob is not None and prob < 0.50:
            return "sell", "prob-below-50", (
                f"实时胜率/覆盖率 {prob:.0%} < 50%（{score.get('home')} {hs:.0f}-{aws:.0f} "
                f"{score.get('away')}，第 {score.get('period')} 节，剩约 {score.get('minutes_remaining'):.0f} 分钟）")
        score_note = (f"，实时概率 {prob:.0%}" if prob is not None else "")
    elif score is None:
        score_note = f"（比分未核验：{unverified}）"
    else:
        score_note = f"（比赛状态 {score.get('state')}）"
    # (b) 再看现价
    price, src = validated_price(token, pos.get("slug", ""), pos.get("outcome_index"))
    if price is None:
        return None, "", "取不到现价，本轮跳过（不瞎卖）"
    if entry and price <= entry - PRICE_DROP_SELL:
        return "sell", "price-drop", f"现价 ${price:.3f} 比入场 ${entry:.3f} 低 {entry - price:.3f} >= 0.15"
    return None, "", f"持有中：现价 ${price:.3f}（{src}）{score_note}"


def cmd_exit_check(a):
    use_paths(a.state_file, a.sim_dir)
    sells, settles, notes, errors = [], [], [], []
    with LockedState(a.state_file) as st:
        for token in list(self_positions(st).keys()):
            pos = st["positions"][token]
            try:
                action, reason, detail = check_position(st, token, pos)
            except Exception as e:
                errors.append(f"{pos.get('title', '')[:30]} 检查异常：{e}")
                continue
            notes.append(f"[{pos.get('analysis_id','')}] {pos.get('title','')[:30]} "
                         f"{pos.get('outcome','')}: {detail}")
            if action == "settle":
                r = settle_one(st, token)
                if r:
                    settles.append({**r, "reason": reason, "detail": detail})
            elif action == "sell":
                sold, err = do_sell(st, token, f"{reason}：{detail}")
                if sold:
                    sells.append({**sold, "trigger": reason, "detail": detail})
                else:
                    errors.append(f"{pos.get('title','')[:30]} 卖出失败：{err}")
    out = {"sells": sells, "settles": settles, "notes": notes, "errors": errors}
    print(json.dumps({"ok": True, **out}, ensure_ascii=False))
    return 0


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="V7 体育推单模拟盘自营买卖（只模拟，不花真钱）")
    ap.add_argument("--state-file", default=str(STATE_FILE), help="state.json 路径（测试时指向副本）")
    ap.add_argument("--sim-dir", default=str(SIM_DIR), help="模拟盘明细 CSV 目录（测试时指向临时目录）")
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("buy")
    b.add_argument("--analysis-id", required=True)
    b.add_argument("--token-id", required=True)
    b.add_argument("--slug", required=True)
    b.add_argument("--title", required=True)
    b.add_argument("--outcome", required=True)
    b.add_argument("--outcome-side", default="yes", choices=["yes", "no"])
    b.add_argument("--outcome-index", type=int, default=0)
    b.add_argument("--price", type=float, required=True, help="交付前复核的可执行价（Gamma bestAsk/CLOB ask）")
    b.add_argument("--no-chase-cap", type=float, required=True,
                   help="V7 价格保护：本单反转条件里的不再追上限（如 0.76），现价高于它就不买；不传就拒绝")
    b.add_argument("--prob-low", type=float, required=True, help="V7 独立概率区间下界（0-1）")
    b.add_argument("--kelly-frac", type=float, required=True, help="工具 Quarter Kelly（本金占比，如 0.14）")
    b.add_argument("--sport", default="")
    b.add_argument("--market-type", default="")
    b.add_argument("--phase", default="")
    b.add_argument("--polymarket-url", default="")
    b.add_argument("--reversal", default="")
    b.add_argument("--rev-lead-min", type=float, default=None)
    b.add_argument("--rev-trailing", action="store_true")
    b.add_argument("--rev-team", default="")
    b.add_argument("--espn-league", default="")
    b.add_argument("--home-team", default="")
    b.add_argument("--away-team", default="")
    b.add_argument("--line", type=float, default=None)
    b.add_argument("--total-side", default="")

    s = sub.add_parser("sell")
    s.add_argument("--token-id", required=True)
    s.add_argument("--reason", default="manual")
    s.add_argument("--price", type=float, default=None)

    sub.add_parser("settle")
    sub.add_parser("status")
    sub.add_parser("exit-check")

    a = ap.parse_args()
    global SIM_CSV
    SIM_CSV = Path(a.sim_dir) / "sim_trades.csv"
    fn = {"buy": cmd_buy, "sell": cmd_sell, "settle": cmd_settle,
          "status": cmd_status, "exit-check": cmd_exit_check}[a.cmd]
    return fn(a)


if __name__ == "__main__":
    sys.exit(main())
