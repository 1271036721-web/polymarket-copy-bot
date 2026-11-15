#!/usr/bin/env python3
"""
V7 观察模式 watcher —— Polymarket 跟单模拟盘 v2（只读机器人文件）

用途：Dylan 2026-11-15 批准：跟单照常，事后给每笔新跟单跑 V7.0 记分，
观察一周（至 2026-11-22）再决定要不要加 V7 门。

只读：copy_trades.csv / settled_trades.csv / copy_sells.csv（后者仅备查，不写入）。
只写：v7_observation.csv、v7_observe_state.json（本脚本同目录）。
绝不碰：copy_bot_v2.py、config.yaml、state.json、机器人进程。

观察起点：state 文件首次创建时刻（started_at）。此前已存在的跟单
（excluded_before_start）永不回填——旧单打分会被事后价格/赛果污染。
新单判定（须同时满足）：target_tx 键不在观察表里、不在 excluded 里、
且 time_utc >= started_at。

命令：
  scan    找新跟单并分类，stdout 输出 JSON（供 cron worker 解析）
  score   给一笔新跟单记分并追加进观察表（按 target_tx 查原单，不用重输）
  settle  把 settled_trades.csv 的新结算行按 (title, outcome, target)
          对到观察表，未结算行填 settle_price/settled_value/pnl/settled_time_utc
  report  输出观察期聚合统计（JSON，供 11-22 复盘用）
  update_vetoes  按观察表更新否决清单 v7_veto.json（供机器人 v7_gate 读取）：
          每笔 would_fail 且 Edge <= -10% 给 (target, market-type) 记 1 次
          strike；同一 key 7 天内满 2 次就进 vetoes。veto 一直有效到
          2026-11-22 复盘或手动清除。幂等：重复跑不重复记 strike、不重复进 veto。

打分规则（框架 ~/workspace/goals/goal/files/体育-V7.0-分析框架.md）：
  verdict 只能是 would_pass / would_fail / cannot_score / not_applicable。
  - TruongMyLan / AllByMyseIf 的单：一律 not_applicable（V7 无政治/地缘/
    加密模型），reason 写“非体育市场，V7 无模型”。不建 Space 分析。
  - Sisyphus- 的 Dota 2 盘：按 V7 Dota 2 逻辑打分。先独立概率区间
    （队伍近 3 个月胜率 × 版本适应 × 选手对位 × 选边 51-52%；BP 已知
    ±5-8 点；版本大改后两周 Confidence -10；线上赛 -5），再看真实成交
    价：Conservative Edge = 概率区间下界 - 成交价隐含概率（成交价即
    隐含概率，十进制赔率 = 1/成交价）。
    would_pass 须同时满足：Edge > 0、Confidence >= 70、Opportunity >= 70、
    无 U1-U7 硬拦截。否则 would_fail 并写明哪条没过。
  - 盘中单局胜负缺局内信息（经济差/肉山/高地推进）时诚实记 cannot_score，
    不要硬编概率。BO1、替补/租借选手出战按框架判 would_fail。
  - 赔率窗 1.30-1.80 只作参考项记入 reason，绝不因此单独判 would_fail。
  - U3：同一天（UTC 日期）同一目标同一盘口类型已记分 >= 2 场时，
    第 3 场即使单独过关也判 would_fail（脚本硬拦截 would_pass）。
  - U6：Dota 2 校准表目前 0 样本，无连错停推可触发，reason 里注明。
  - U7：would_pass / would_fail 必须有至少一条针对性红队理由。
  - Space 分析（createanalysis）只给 would_pass / would_fail 建，
    填同一组数字，notes 标注“跟单观察”；cannot_score / not_applicable
    没有可填的概率数字，不建（硬编违反框架幻觉阻断）。

结算对账：一笔结算行可对应多笔观察行（同一 token 的多次碎单跟买
在 copy_trades.csv 里是多行，settled_trades.csv 里是一行）。
settled_value / pnl 按各行 amount_usd 占比分摊（cost 按结算行 cost
同比例折算，最大的一行吃舍入残差）。只对未结算行操作；已应用过的
结算行记入 state 的 applied_settlements，不再重复处理。

注意：经卖出跟随提前离场的仓位不会出现在 settled_trades.csv 里，
对应观察行将一直保持未结算——复盘时按未结算计，不追溯卖出盈亏。
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
TRADES_CSV = BASE / "copy_trades.csv"
SETTLED_CSV = BASE / "settled_trades.csv"
OBS_CSV = BASE / "v7_observation.csv"
STATE_JSON = BASE / "v7_observe_state.json"

OBS_HEADER = [
    "time_utc", "target", "target_tx", "title", "outcome",
    "target_price", "exec_price", "amount_usd",
    "v7_prob_low", "v7_prob_high", "v7_edge", "v7_confidence",
    "v7_verdict", "v7_reason",
    "settle_price", "settled_value", "pnl", "settled_time_utc",
]

NON_SCORABLE_TARGETS = {"TruongMyLan", "AllByMyseIf"}
VERDICTS = {"would_pass", "would_fail", "cannot_score", "not_applicable"}

# 否决清单（2026-11-16 Dylan 决定：V7 宽松门槛的一部分）
# 文件 v7_veto.json：{"strikes": {"target|market-type": [{tx, edge, time_utc}]},
#                     "vetoes": [{"target", "market_type", "since"}]}
VETO_FILE = BASE / "v7_veto.json"
VETO_STRIKE_EDGE = -10.0   # would_fail 且 Edge <= -10% 才记 1 次 strike
VETO_STRIKE_DAYS = 7       # 同一 key 7 天内满 2 次进 vetoes
VETO_STRIKE_COUNT = 2

CONF_FIELDS = ["conf_evidence", "conf_source", "conf_rules", "conf_model",
               "conf_freshness", "conf_independence", "conf_market"]
CONF_MAX = {"conf_evidence": 20, "conf_source": 15, "conf_rules": 15,
            "conf_model": 20, "conf_freshness": 10, "conf_independence": 10,
            "conf_market": 10}
OPP_FIELDS = ["opp_rule", "opp_evidence", "opp_base_rate", "opp_model",
              "opp_edge", "opp_exec", "opp_cross", "opp_info", "opp_tail",
              "opp_behavior"]
OPP_MAX = {"opp_rule": 10, "opp_evidence": 15, "opp_base_rate": 10,
           "opp_model": 15, "opp_edge": 15, "opp_exec": 15, "opp_cross": 5,
           "opp_info": 5, "opp_tail": 5, "opp_behavior": 5}

SPORT_KEYWORDS = [
    ("Dota 2", "esports", "Dota 2"),
    ("Counter-Strike", "esports", "CS2"),
    ("League of Legends", "esports", "LoL"),
    ("Valorant", "esports", "Valorant"),
    ("KPL", "esports", "KPL"),
    ("王者荣耀", "esports", "KPL"),
]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_time(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def load_state():
    if STATE_JSON.exists():
        return json.loads(STATE_JSON.read_text())
    return None


def init_state():
    """首次运行：建 state，当前已有的跟单全部列入 excluded（永不回填）。"""
    trades = read_trades()
    state = {
        "started_at": now_iso(),
        "excluded_before_start": sorted({r["target_tx"] for r in trades}),
        "applied_settlements": [],
        "last_scan": None,
    }
    STATE_JSON.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    return state


def get_state():
    state = load_state()
    if state is None:
        state = init_state()
        print(f"[init] 观察起点 {state['started_at']}，"
              f"此前 {len(state['excluded_before_start'])} 笔跟单列入 excluded，永不回填。",
              file=sys.stderr)
    return state


def save_state(state):
    STATE_JSON.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def read_trades():
    with open(TRADES_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_settled():
    with open(SETTLED_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_obs():
    if not OBS_CSV.exists():
        return []
    with open(OBS_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_obs(rows):
    with open(OBS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OBS_HEADER)
        w.writeheader()
        w.writerows(rows)


def ensure_obs_file():
    if not OBS_CSV.exists():
        write_obs([])


def detect_sport(title):
    for kw, sport, name in SPORT_KEYWORDS:
        if kw.lower() in title.lower():
            return sport, name
    return None, None


def market_type_of(title):
    t = title
    if "Game 1 Winner" in t or "Game 2 Winner" in t or "Game 3 Winner" in t \
            or "Game 4 Winner" in t or "Game 5 Winner" in t \
            or "Map 1 Winner" in t or "Map 2 Winner" in t \
            or "Map 3 Winner" in t:
        return "单局胜负"
    if "Handicap" in t:
        return "让局/让分"
    if "Spread" in t:
        # 让分盘（NFL/NBA 的 Spread: Colts (-10.5)、1H Spread）
        return "让分"
    if "Moneyline" in t:
        return "胜负"
    if "Over" in t or "Under" in t or "Total" in t or "O/U" in t:
        # 注意 "O/U 46.5" 这种缩写不含 Over/Under 字样，必须单独判
        return "大小"
    if "BO3" in t or "BO5" in t or "BO7" in t:
        return "系列赛胜负"
    if "Will " in t and " win" in t:
        # "Will Spain win on 2026-11-15?" 这类胜负盘
        return "胜负"
    if " vs. " in t or " vs " in t:
        # "Pacers vs. 76ers" 这类无其他关键词的对阵盘 = 胜负
        return "胜负"
    return "其他"


def classify(row):
    """返回 (kind, sport, sport_name, market_type, reason_hint)。
    kind: not_applicable / candidate_dota2 / candidate_other_sport / unknown_sport"""
    target = row["target"]
    title = row["title"]
    mt = market_type_of(title)
    if target in NON_SCORABLE_TARGETS:
        return "not_applicable", None, None, mt, "非体育市场，V7 无模型"
    sport, name = detect_sport(title)
    if sport is None:
        return "unknown_sport", None, None, mt, "标题无法对应 V7 运动，需人工判断"
    if name == "Dota 2":
        return "candidate_dota2", sport, name, mt, ""
    return "candidate_other_sport", sport, name, mt, f"{name} 按框架对应小节打分"


def cmd_scan(args):
    state = get_state()
    ensure_obs_file()
    obs = read_obs()
    obs_keys = {r["target_tx"] for r in obs}
    excluded = set(state["excluded_before_start"])
    started = parse_time(state["started_at"])
    new_rows = []
    for r in read_trades():
        key = r["target_tx"]
        if key in obs_keys or key in excluded:
            continue
        try:
            t = parse_time(r["time_utc"])
        except ValueError:
            continue
        if t < started:
            continue
        kind, sport, sport_name, mt, hint = classify(r)
        new_rows.append({
            "time_utc": r["time_utc"],
            "target": r["target"],
            "target_tx": key,
            "title": r["title"],
            "outcome": r["outcome"],
            "target_price": r["target_price"],
            "exec_price": r["exec_price"],
            "amount_usd": r["amount_usd"],
            "kind": kind,
            "sport": sport,
            "sport_name": sport_name,
            "market_type": mt,
            "hint": hint,
        })
    state["last_scan"] = now_iso()
    save_state(state)
    print(json.dumps({"new_count": len(new_rows), "new": new_rows},
                     ensure_ascii=False, indent=2))
    return 0


def u3_count(obs_rows, target, date_str, market_type):
    n = 0
    for r in obs_rows:
        if r["target"] != target or r["v7_verdict"] not in ("would_pass", "would_fail"):
            continue
        if not r["time_utc"].startswith(date_str):
            continue
        reason = r.get("v7_reason", "")
        if f"[{market_type}]" in reason or market_type in reason:
            n += 1
    return n


def cmd_score(args):
    state = get_state()
    ensure_obs_file()
    obs = read_obs()
    obs_keys = {r["target_tx"] for r in obs}
    if args.tx in obs_keys:
        print(f"拒绝：{args.tx[:16]}… 已在观察表里（去重）。", file=sys.stderr)
        return 2
    if args.tx in set(state["excluded_before_start"]):
        print("拒绝：该单在观察起点之前，不回填旧单。", file=sys.stderr)
        return 2
    trades = {r["target_tx"]: r for r in read_trades()}
    if args.tx not in trades:
        print(f"拒绝：在 copy_trades.csv 里找不到 target_tx={args.tx}。", file=sys.stderr)
        return 2
    row = trades[args.tx]
    if parse_time(row["time_utc"]) < parse_time(state["started_at"]):
        print("拒绝：该单时间早于观察起点，不回填旧单。", file=sys.stderr)
        return 2
    if args.verdict not in VERDICTS:
        print(f"拒绝：verdict 必须是 {sorted(VERDICTS)} 之一。", file=sys.stderr)
        return 2
    if not args.reason:
        print("拒绝：v7_reason 必填。", file=sys.stderr)
        return 2

    kind, sport, sport_name, mt, hint = classify(row)
    exec_price = float(row["exec_price"])
    odds = 1.0 / exec_price if exec_price > 0 else 0.0
    odds_note = (f"赔率 {odds:.2f}（{'窗内' if 1.30 <= odds <= 1.80 else '窗外，仅参考'})")

    prob_low = prob_high = edge = conf_total = None
    if args.verdict in ("would_pass", "would_fail"):
        if args.prob_low is None or args.prob_mid is None or args.prob_high is None:
            print("拒绝：would_pass / would_fail 必须给概率区间（--prob-low/mid/high）。",
                  file=sys.stderr)
            return 2
        if not (0 <= args.prob_low <= args.prob_mid <= args.prob_high <= 100):
            print("拒绝：须满足 0 <= prob_low <= prob_mid <= prob_high <= 100。",
                  file=sys.stderr)
            return 2
        conf = {}
        for fld in CONF_FIELDS:
            v = getattr(args, fld)
            if v is None:
                print(f"拒绝：would_pass / would_fail 必须给 7 项 Confidence 子分（缺 {fld}）。",
                      file=sys.stderr)
                return 2
            if not (0 <= v <= CONF_MAX[fld]):
                print(f"拒绝：{fld} 超出 0-{CONF_MAX[fld]}。", file=sys.stderr)
                return 2
            conf[fld] = v
        opp = {}
        for fld in OPP_FIELDS:
            v = getattr(args, fld)
            if v is None:
                print(f"拒绝：would_pass / would_fail 必须给 10 项 Opportunity 子分（缺 {fld}）。",
                      file=sys.stderr)
                return 2
            if not (0 <= v <= OPP_MAX[fld]):
                print(f"拒绝：{fld} 超出 0-{OPP_MAX[fld]}。", file=sys.stderr)
                return 2
            opp[fld] = v
        if not args.red_team:
            print("拒绝：U7 要求至少一条针对性红队理由（--red-team）。", file=sys.stderr)
            return 2
        conf_total = sum(conf.values())
        opp_total = sum(opp.values())
        edge = round(args.prob_low - exec_price * 100, 2)
        prob_low, prob_high = args.prob_low, args.prob_high
        # U3 硬拦截：同日同目标同盘口类型已记分 >= 2 场，第 3 场不许 would_pass
        date_str = row["time_utc"][:10]
        n3 = u3_count(obs, row["target"], date_str, mt)
        if args.verdict == "would_pass" and n3 >= 2:
            print(f"拒绝：U3——{date_str} {row['target']} {mt} 已记分 {n3} 场，"
                  "第 3 场即使单独过关也不推，请记 would_fail 并写明 U3。",
                  file=sys.stderr)
            return 2
        if args.verdict == "would_pass":
            if edge <= 0:
                print(f"拒绝：U2——Conservative Edge {edge} <= 0，不能判 would_pass。",
                      file=sys.stderr)
                return 2
            if conf_total < 70:
                print(f"拒绝：Confidence {conf_total} < 70，不能判 would_pass。",
                      file=sys.stderr)
                return 2
            if opp_total < 70:
                print(f"拒绝：Opportunity {opp_total} < 70，不能判 would_pass。",
                      file=sys.stderr)
                return 2
        reason = args.reason
        if f"[{mt}]" not in reason:
            reason = f"[{sport_name or '未知运动'}·{mt}] " + reason
        reason = f"{reason} | Opportunity {opp_total} | {odds_note} | U3 当日同类型已记分 {n3} 场 | 校准：Dota 2 0 样本"
    else:
        # cannot_score / not_applicable：不许带概率数字（防硬编）
        if args.prob_low is not None or args.prob_mid is not None or args.prob_high is not None:
            print("拒绝：cannot_score / not_applicable 不许填概率区间。", file=sys.stderr)
            return 2
        reason = args.reason
        if kind == "not_applicable" and "非体育市场" not in reason:
            reason = "非体育市场，V7 无模型；" + reason
        if f"[{mt}]" not in reason:
            reason = f"[{sport_name or '未知运动'}·{mt}] " + reason
        reason = f"{reason} | {odds_note}"

    new_row = {
        "time_utc": row["time_utc"],
        "target": row["target"],
        "target_tx": row["target_tx"],
        "title": row["title"],
        "outcome": row["outcome"],
        "target_price": row["target_price"],
        "exec_price": row["exec_price"],
        "amount_usd": row["amount_usd"],
        "v7_prob_low": "" if prob_low is None else prob_low,
        "v7_prob_high": "" if prob_high is None else prob_high,
        "v7_edge": "" if edge is None else edge,
        "v7_confidence": "" if conf_total is None else conf_total,
        "v7_verdict": args.verdict,
        "v7_reason": reason,
        "settle_price": "",
        "settled_value": "",
        "pnl": "",
        "settled_time_utc": "",
    }
    obs.append(new_row)
    write_obs(obs)
    print(json.dumps({"scored": new_row}, ensure_ascii=False, indent=2))
    return 0


def settled_key(s):
    return f"{s_time(s)}|{s['title']}|{s['outcome']}"


def s_time(s):
    # 实测表头是 time_utc；兼容旧表头 time
    return s.get("time_utc") or s.get("time")


def cmd_settle(args):
    state = get_state()
    ensure_obs_file()
    obs = read_obs()
    applied = set(state.get("applied_settlements", []))
    settled_rows = sorted(read_settled(), key=lambda s: s_time(s))
    n_applied = 0
    n_skipped = 0
    for s in settled_rows:
        key = settled_key(s)
        if key in applied:
            continue
        # 候选：未结算、title/outcome 相同；结算行 target 非空时须与观察行 target 相同
        cands = []
        for r in obs:
            if r["settled_time_utc"]:
                continue
            if r["title"] != s["title"] or r["outcome"] != s["outcome"]:
                continue
            if s["target"] and r["target"] != s["target"]:
                continue
            cands.append(r)
        if not cands:
            # 无候选：可能是观察起点前的仓位（永不匹配），也可能是记分
            # 尚未入表（瞬时）。结算时间超过 1 小时的才标记已处理，
            # 1 小时内的下轮再试，避免记分晚到造成永久漏对。
            from datetime import timedelta
            if parse_time(s_time(s)) < datetime.now(timezone.utc) - timedelta(hours=1):
                applied.add(key)
                save_state({**state, "applied_settlements": sorted(applied)})
            else:
                n_skipped += 1
            continue
        total_amount = sum(float(r["amount_usd"]) for r in cands)
        settled_cost = float(s["cost"])
        settled_value = float(s["settled_value"])
        settle_price = s["settle_price"]
        # 按 amount_usd 占比分摊；最大的一行吃舍入残差
        cands_sorted = sorted(cands, key=lambda r: float(r["amount_usd"]), reverse=True)
        allocs = []
        for r in cands_sorted:
            share = float(r["amount_usd"]) / total_amount if total_amount else 0
            allocs.append((r, round(settled_value * share, 2),
                           round(settled_cost * share, 2)))
        v_sum = round(sum(a[1] for a in allocs), 2)
        c_sum = round(sum(a[2] for a in allocs), 2)
        if allocs:
            r0, v0, c0 = allocs[0]
            allocs[0] = (r0, round(v0 + (settled_value - v_sum), 2),
                         round(c0 + (settled_cost - c_sum), 2))
        for r, v, c in allocs:
            r["settle_price"] = settle_price
            r["settled_value"] = v
            r["pnl"] = round(v - c, 2)
            r["settled_time_utc"] = s_time(s)
        applied.add(key)
        n_applied += 1
    write_obs(obs)
    state["applied_settlements"] = sorted(applied)
    save_state(state)
    print(json.dumps({"settlements_applied": n_applied,
                      "settlements_skipped_no_match": n_skipped},
                     ensure_ascii=False, indent=2))
    return 0


def load_veto():
    """读否决清单；文件不存在/损坏时当作空清单（机器人侧同样 fail-open）。"""
    if VETO_FILE.exists():
        try:
            d = json.loads(VETO_FILE.read_text())
            if isinstance(d, dict):
                return {"strikes": d.get("strikes", {}) or {},
                        "vetoes": d.get("vetoes", []) or []}
        except Exception:
            pass
    return {"strikes": {}, "vetoes": []}


def save_veto(veto):
    VETO_FILE.write_text(json.dumps(veto, indent=2, ensure_ascii=False))


def cmd_update_vetoes(args):
    """按观察表更新否决清单（幂等，可反复跑）：
    每笔 would_fail 且 Edge <= -10% 给 (target, market-type) 记 1 次 strike
    （按 target_tx 去重，重复跑不重复记）；同一 key 7 天内满 2 次就进 vetoes
    （已进 veto 的 key 不重复进）。veto 一直有效到 2026-11-22 复盘或手动清除。
    market-type 用 market_type_of(标题) 算，与机器人 v7_gate 用的同一套分类。"""
    obs = read_obs()
    veto = load_veto()
    strikes = veto.setdefault("strikes", {})
    vetoes = veto.setdefault("vetoes", [])
    new_strikes = 0
    now = datetime.now(timezone.utc)
    for r in obs:
        if r.get("v7_verdict") != "would_fail":
            continue
        try:
            edge = float(r.get("v7_edge") or "")
        except (TypeError, ValueError):
            continue
        if edge > VETO_STRIKE_EDGE:
            continue
        key = f"{r['target']}|{market_type_of(r['title'])}"
        lst = strikes.setdefault(key, [])
        if any(s.get("tx") == r["target_tx"] for s in lst):
            continue
        lst.append({"tx": r["target_tx"], "edge": edge,
                    "time_utc": r["time_utc"]})
        new_strikes += 1
    new_vetoes = []
    veto_keys = {(v.get("target"), v.get("market_type")) for v in vetoes}
    for key, lst in strikes.items():
        target, _, mt = key.partition("|")
        if (target, mt) in veto_keys:
            continue
        recent = [s for s in lst
                  if (now - parse_time(s["time_utc"])).total_seconds()
                  <= VETO_STRIKE_DAYS * 86400]
        if len(recent) >= VETO_STRIKE_COUNT:
            vetoes.append({"target": target, "market_type": mt,
                           "since": now_iso()})
            new_vetoes.append(key)
    save_veto(veto)
    print(json.dumps({"new_strikes": new_strikes, "new_vetoes": new_vetoes,
                      "strikes": strikes, "vetoes": vetoes},
                     ensure_ascii=False, indent=2))
    return 0


def cmd_report(args):
    obs = read_obs()
    groups = {}
    for v in ("would_pass", "would_fail", "cannot_score", "not_applicable"):
        rows = [r for r in obs if r["v7_verdict"] == v]
        settled = [r for r in rows if r["settled_time_utc"]]
        pnls = [float(r["pnl"]) for r in settled]
        wins = sum(1 for p in pnls if p > 0)
        groups[v] = {
            "rows": len(rows),
            "settled": len(settled),
            "unsettled": len(rows) - len(settled),
            "total_pnl": round(sum(pnls), 2),
            "avg_pnl": round(sum(pnls) / len(pnls), 2) if pnls else None,
            "win_rate": round(wins / len(pnls), 4) if pnls else None,
        }
    scored = groups["would_pass"]["rows"] + groups["would_fail"]["rows"]
    total = len(obs)
    all_settled = [r for r in obs if r["settled_time_utc"]]
    all_pnls = [float(r["pnl"]) for r in all_settled]
    out = {
        "total_rows": total,
        "settled_rows": len(all_settled),
        "unsettled_rows": total - len(all_settled),
        "total_settled_pnl": round(sum(all_pnls), 2),
        "scorable_coverage": round(scored / total, 4) if total else None,
        "groups": groups,
        "note": "未结算行可能尚未完赛，也可能已由卖出跟随提前离场（不在 settled_trades.csv 里），复盘时按未结算计。",
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def main():
    p = argparse.ArgumentParser(description="V7 观察模式 watcher（只读机器人文件）")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("scan", help="找新跟单并分类，输出 JSON")
    sp = sub.add_parser("score", help="给一笔新跟单记分并追加进观察表")
    sp.add_argument("--tx", required=True, help="target_tx（键）")
    sp.add_argument("--verdict", required=True,
                    choices=sorted(VERDICTS))
    sp.add_argument("--reason", required=True, help="判定理由")
    sp.add_argument("--prob-low", type=float, default=None)
    sp.add_argument("--prob-mid", type=float, default=None)
    sp.add_argument("--prob-high", type=float, default=None)
    sp.add_argument("--red-team", default=None, help="U7 红队理由")
    for fld in CONF_FIELDS:
        sp.add_argument(f"--{fld.replace('_', '-')}", type=int, default=None)
    for fld in OPP_FIELDS:
        sp.add_argument(f"--{fld.replace('_', '-')}", type=int, default=None)
    sub.add_parser("settle", help="结算对账")
    sub.add_parser("report", help="聚合统计")
    sub.add_parser("update_vetoes", help="按 would_fail 强失败更新否决清单 v7_veto.json")
    args = p.parse_args()
    if args.cmd == "scan":
        return cmd_scan(args)
    if args.cmd == "score":
        return cmd_score(args)
    if args.cmd == "settle":
        return cmd_settle(args)
    if args.cmd == "report":
        return cmd_report(args)
    if args.cmd == "update_vetoes":
        return cmd_update_vetoes(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
