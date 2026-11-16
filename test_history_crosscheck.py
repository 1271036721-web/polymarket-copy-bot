#!/usr/bin/env python3
"""Regression tests for the buy-side prices-history cross-check (5th layer).

2026-11-16 evening, TWO stale-snapshot sim buys in one night, both voided:
  * Nardi (analysis 41): Gamma 0.5005, true price 0.58-0.675. The 4-layer
    Gamma-only guard (a7f014a) caught it in replay via eval divergence
    (eval 0.5835 vs snapshot 0.5005 = 0.083), but the live buy had already
    executed before that guard existed.
  * Hemery (analysis 42): Gamma 0.64, real trades already ~0.91 at buy
    time. The 4-layer guard would NOT have caught it: eval price 0.635
    and buy price 0.64 came from the SAME lagging Gamma feed, so they
    agreed. Only real trade data exposes the lag.

Real prices-history data (data-api v2, fetched 2026-11-16 ~20:20, 1-min
points; buy times in UTC):
  * Hemery Yes token, buy 11:56:44 UTC @ Gamma 0.64:
    11:52 0.665, 11:53 0.655, 11:54 0.875, 11:55 0.915, 11:56:44 0.945
    -> last trade 0.945, deviation 0.305.
  * Nardi No token, buy 11:42:53 UTC @ Gamma 0.5005:
    11:38 0.605, 11:39 0.6255, 11:40 0.616, 11:41 0.5815, 11:42:53 0.677
    -> last trade 0.677, deviation 0.177 (smallest deviation vs ANY
    5-min point: 0.081 vs 11:41's 0.5815).

The check (history_cross_check, called by fresh_ask_price after the eval
divergence check): Gamma price vs last trade within 300s; deviation >
0.05 -> refuse ('history-deviated'). 0.05 is chosen to match
EVAL_DIVERGE_TOL (the system's own "price has run" tolerance) and sits
1.6x below the smallest incident deviation (0.081). No trade in 5 min,
or a failed history fetch, FAILS OPEN with a warning: illiquid markets
legitimately have no recent trade, and blocking those buys would kill
the sim; the four Gamma layers still apply. History is a veto only --
it is NEVER used as the buy price.

Run: ./.venv/bin/python test_history_crosscheck.py
Tests use temp state dirs only; the live ledger is never touched.
"""
import json
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, ".")
import sim_trade as st


def _iso_now_minus(seconds=0):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def market_dict(slug, prices, best_ask=None, updated_at="now"):
    d = {
        "slug": slug,
        "outcomes": ["A", "B"],
        "outcomePrices": json.dumps([str(p) for p in prices]),
    }
    if best_ask is not None:
        d["bestAsk"] = best_ask
    if updated_at == "now":
        d["updatedAt"] = _iso_now_minus(0)
    elif updated_at is None:
        pass
    else:
        d["updatedAt"] = _iso_now_minus(updated_at)
    return d


def hist_points(prices, age_s=30):
    """prices-history points ending `age_s` seconds ago (inside the 300s
    window), one per minute like the real API."""
    now = int(time.time())
    return [{"price": p, "timestamp": now - age_s - 60 * (len(prices) - 1 - i)}
            for i, p in enumerate(prices)]


class FakeAll:
    """Fake st.get_json: CLOB /book -> Invalid token id; /markets pops
    successive snapshots; /events returns a fixed nested market;
    prices-history returns configured points (or raises to simulate a
    fetch failure)."""

    def __init__(self, market_reads, event_prices, slug, history=None,
                 history_raises=False):
        self.market_reads = list(market_reads)
        self.event_prices = event_prices
        self.slug = slug
        self.history = history
        self.history_raises = history_raises

    def __call__(self, url, timeout=10):
        if "clob.polymarket.com" in url:
            return {"error": "Invalid token id"}
        if "prices-history" in url:
            if self.history_raises:
                raise ConnectionError("simulated prices-history outage")
            return {"data": list(self.history) if self.history is not None else []}
        if "/events/slug/" in url:
            return {"markets": [{
                "slug": self.slug,
                "outcomePrices": json.dumps([str(p) for p in self.event_prices]),
            }]}
        if "/markets?slug=" in url:
            if self.market_reads:
                return [self.market_reads.pop(0)]
            return []
        raise AssertionError(f"unexpected URL in test: {url}")


SLUG_HEMERY = "atp-hemery-donald-2026-11-16"
SLUG_NARDI = "atp-nardi-latinov-2026-11-16"
N = 0


def check(name, cond):
    global N
    N += 1
    assert cond, f"FAIL: {name}"
    print(f"  [{N}] ok: {name}")


orig_get_json = st.get_json


def run_fresh_ask(fake, slug, idx, eval_price, delay=0):
    st.get_json = fake
    try:
        return st.fresh_ask_price("tok-test", slug, idx,
                                  eval_price=eval_price, delay=delay)
    finally:
        st.get_json = orig_get_json


def main():
    m = lambda slug, prices, **kw: market_dict(slug, prices, **kw)

    # 1. HEMERY REPLAY (real data): Gamma 0.64, eval 0.635 (both lagging,
    #    so the eval check passes), last trade 0.945 -> refused by the
    #    history layer (deviation 0.305 > 0.05).
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_HEMERY, [0.64, 0.36], best_ask=0.64),
                 m(SLUG_HEMERY, [0.64, 0.36], best_ask=0.64)],
                [0.64, 0.36], SLUG_HEMERY,
                history=hist_points([0.665, 0.655, 0.875, 0.915, 0.945])),
        SLUG_HEMERY, 0, eval_price=0.635)
    check("Hemery replay refused (history-deviated)",
          p is None and src == "history-deviated")

    # 2. NARDI REPLAY (real data), eval isolated: eval 0.5005 agrees with
    #    the Gamma snapshot (as if eval had also come from the lagging
    #    feed), last trade 0.677 -> refused by the history layer
    #    (deviation 0.177 > 0.05).
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.4995, 0.5005]),
                 m(SLUG_NARDI, [0.4995, 0.5005])],
                [0.4995, 0.5005], SLUG_NARDI,
                history=hist_points([0.605, 0.6255, 0.616, 0.5815, 0.677])),
        SLUG_NARDI, 1, eval_price=0.5005)
    check("Nardi replay refused (history-deviated, eval isolated)",
          p is None and src == "history-deviated")

    # 3. Nardi with the real eval 0.5835: the eval-divergence layer fires
    #    first (0.083 > 0.05) -- still refused, layer order documented.
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.4995, 0.5005]),
                 m(SLUG_NARDI, [0.4995, 0.5005])],
                [0.4995, 0.5005], SLUG_NARDI,
                history=hist_points([0.605, 0.6255, 0.616, 0.5815, 0.677])),
        SLUG_NARDI, 1, eval_price=0.5835)
    check("Nardi real-eval refused (eval-diverged first)",
          p is None and src == "eval-diverged")

    # 4. Normal case: Gamma 0.445, eval 0.45, last trade 0.44 -> accepted.
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445),
                 m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445)],
                [0.445, 0.555], SLUG_NARDI,
                history=hist_points([0.44, 0.44])),
        SLUG_NARDI, 0, eval_price=0.45)
    check("normal buy accepted (Gamma ~= last trade)",
          p == 0.445 and src == "gamma-fresh")

    # 5. Boundary: deviation 0.0499... (0.40 vs 0.45) -> accepted;
    #    deviation 0.06 (0.40 vs 0.46) -> refused.
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.60, 0.40], best_ask=0.60),
                 m(SLUG_NARDI, [0.60, 0.40], best_ask=0.60)],
                [0.60, 0.40], SLUG_NARDI,
                history=hist_points([0.45])),
        SLUG_NARDI, 1, eval_price=0.40)
    check("boundary 0.0499 deviation accepted", p == 0.40 and src == "gamma-fresh")
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.60, 0.40], best_ask=0.60),
                 m(SLUG_NARDI, [0.60, 0.40], best_ask=0.60)],
                [0.60, 0.40], SLUG_NARDI,
                history=hist_points([0.46])),
        SLUG_NARDI, 1, eval_price=0.40)
    check("boundary 0.06 deviation refused",
          p is None and src == "history-deviated")

    # 6. No trades in 5 min -> FAIL OPEN with warning, buy accepted.
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445),
                 m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445)],
                [0.445, 0.555], SLUG_NARDI, history=[]),
        SLUG_NARDI, 0, eval_price=0.45)
    check("no recent trades fails open (accepted with warning)",
          p == 0.445 and src == "gamma-fresh")

    # 7. prices-history fetch failure -> FAIL OPEN with warning, buy accepted.
    p, src = run_fresh_ask(
        FakeAll([m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445),
                 m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445)],
                [0.445, 0.555], SLUG_NARDI, history_raises=True),
        SLUG_NARDI, 0, eval_price=0.45)
    check("history fetch failure fails open (accepted with warning)",
          p == 0.445 and src == "gamma-fresh")

    # ---- end-to-end cmd_buy with temp state (live ledger untouched) ----
    def buy_args(**over):
        d = dict(state_file="", sim_dir="", token_id="tok-test",
                 slug=SLUG_HEMERY,
                 title="Metz: Calvin Hemery vs Matthew William Donald",
                 outcome="Calvin Hemery", outcome_side="yes",
                 outcome_index=0, price=0.635, no_chase_cap=0.70,
                 prob_low=0.70, kelly_frac=99.51 / 1126.76, sport="tennis",
                 market_type="胜负", phase="盘中",
                 polymarket_url="https://polymarket.com/event/" + SLUG_HEMERY,
                 reversal="", rev_lead_min=None, rev_trailing=False,
                 rev_team="", espn_league="tennis/atp", home_team="",
                 away_team="", line=None, total_side="", analysis_id="42-replay")
        d.update(over)
        return SimpleNamespace(**d)

    def temp_state(tmp):
        sp = Path(tmp) / "state.json"
        sp.write_text(json.dumps({"cash": 1126.76, "positions": {},
                                  "trades": 0, "sells": 0, "seen_tx": [],
                                  "pending": {}}))
        return sp

    # 8. cmd_buy Hemery replay (real history 0.945) -> refused, ledger clean.
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp)
        st.get_json = FakeAll(
            [m(SLUG_HEMERY, [0.64, 0.36], best_ask=0.64),
             m(SLUG_HEMERY, [0.64, 0.36], best_ask=0.64)],
            [0.64, 0.36], SLUG_HEMERY,
            history=hist_points([0.665, 0.655, 0.875, 0.915, 0.945]))
        try:
            rc = st.cmd_buy(buy_args(state_file=str(sp), sim_dir=tmp))
        finally:
            st.get_json = orig_get_json
        s = json.loads(sp.read_text())
        check("cmd_buy Hemery replay refused",
              rc == 1 and s["cash"] == 1126.76 and not s["positions"]
              and s["trades"] == 0)

    # 9. cmd_buy normal (history 0.44 ~= Gamma 0.445) -> bought.
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp)
        st.get_json = FakeAll(
            [m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445),
             m(SLUG_NARDI, [0.445, 0.555], best_ask=0.445)],
            [0.445, 0.555], SLUG_NARDI,
            history=hist_points([0.44, 0.44]))
        try:
            rc = st.cmd_buy(buy_args(
                state_file=str(sp), sim_dir=tmp, slug=SLUG_NARDI,
                outcome="Trail Blazers", outcome_side="yes",
                outcome_index=0, price=0.45, no_chase_cap=0.70,
                prob_low=0.70, kelly_frac=10.0 / 1126.76,
                title="Trail Blazers vs. Timberwolves",
                analysis_id="live-test-replay"))
        finally:
            st.get_json = orig_get_json
        s = json.loads(sp.read_text())
        pos = list(s["positions"].values())
        check("cmd_buy normal accepted at fresh price",
              rc == 0 and len(pos) == 1 and pos[0]["entry_price"] == 0.445
              and abs(s["cash"] - (1126.76 - 10.0)) < 0.01)

    print(f"history cross-check regression tests: {N}/{N} PASS")


if __name__ == "__main__":
    main()
