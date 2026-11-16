#!/usr/bin/env python3
"""Regression tests for the finished-match settle fix (2026-11-16).

2026-11-16 20:09 incident: Hemery beat Donald 6-3, 3-6, 6-3 (Gamma live:
False, final score, period FT). The sim position (analysis 42, bought
$99.51 @ 0.64) sat open while Gamma outcomePrices showed 0.9995. The exit
monitor ran clean twice with sells [] / settles [] -- no trigger fired.
Root cause: settle_one() and check_position()'s settle branch both required
last_price in (0.0, 1.0) exactly, where last_price comes from
history_last_price() (prices-history last trade). A finished market's last
trade is typically 0.9995/0.0005, never exactly 1.0/0.0, so the settle branch
never fired. The position was sold manually at 0.9995 (+$55.90).

Fix (is_settle_price + gamma_event_finished):
  - exactly 0.0/1.0: settle as before (no event lookup).
  - near-settle (>=0.99/<=0.01): settle ONLY when the Gamma event for the
    position's slug confirms the match is finished (ended=True, or
    finishedTimestamp set, or live=False with a final score, or this
    market closed=True).
  - live match at 0.995 (match point) must NOT auto-settle; it can still
    be sold by the price/probability triggers, which is a different path.
  - pre-match at 0.9995 must NOT settle either (live=None, no score,
    period NS) -- a near-certain pre-match price is not a settlement.

Real Gamma field values verified live 2026-11-16 20:15:
  - finished (Hemery): live=False, score='6-3, 3-6, 6-3', period='FT',
    ended=True, finishedTimestamp set; main market closed=False (it stays
    open until official resolution -- market closed alone is NOT a
    reliable finished signal for the match-winner market).
  - pre-match (Celtics 11-17): live=None, score=None, period='NS',
    ended=None, finishedTimestamp=None.

Run: ./.venv/bin/python test_settle_finished.py
Tests use temp state dirs only; the live ledger is never touched.
"""
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, ".")
import sim_trade as st

SLUG = "atp-hemery-donald-2026-11-16"
TOKEN = "tok-hemery-test"
N = 0


def check(name, cond):
    global N
    N += 1
    assert cond, f"FAIL: {name}"
    print(f"  [{N}] ok: {name}")


def hemery_pos():
    """Replay of the real analysis-42 position (pre-manual-sell state)."""
    return {
        "target": "V7-SELF", "analysis_id": "42",
        "slug": SLUG, "title": "Metz: Calvin Hemery vs Matthew William Donald",
        "outcome": "Calvin Hemery", "outcome_side": "yes", "outcome_index": 0,
        "entry_price": 0.64, "cost": 99.51, "size": 155.4911,
        "sport": "tennis", "market_type": "胜负",
    }


def temp_state(tmp, cash=927.74, pos=None):
    sp = Path(tmp) / "state.json"
    sp.write_text(json.dumps({
        "cash": cash, "positions": {TOKEN: pos} if pos else {},
        "trades": 0, "sells": 0, "seen_tx": [], "pending": {}}))
    return sp


def event(live=None, score=None, period=None, ended=None, finished=None,
          market_closed=False):
    return {"slug": SLUG, "live": live, "score": score, "period": period,
            "ended": ended, "finishedTimestamp": finished,
            "markets": [{"slug": SLUG, "closed": market_closed,
                         "outcomePrices": '["0.9995", "0.0005"]'}]}


class FakeAPI:
    """Fake st.get_json: prices-history returns a fixed last trade,
    /events/slug/ returns a fixed event, /markets returns a fixed market,
    CLOB /book returns the Invalid-token-id error (as in production)."""

    def __init__(self, last_price, last_ts, ev, market_prices=None,
                 forbid_event=False):
        self.last_price = last_price
        self.last_ts = last_ts
        self.ev = ev
        self.market_prices = market_prices
        self.forbid_event = forbid_event
        self.event_calls = 0

    def __call__(self, url, timeout=10):
        if "clob.polymarket.com" in url:
            return {"error": "Invalid token id"}
        if "/prices-history" in url:
            if self.last_price is None:
                return {"data": []}
            return {"data": [{"price": self.last_price,
                              "timestamp": self.last_ts}]}
        if "/events/slug/" in url:
            if self.forbid_event:
                raise AssertionError("event lookup must not happen for exact 0/1")
            self.event_calls += 1
            return self.ev
        if "/markets?slug=" in url:
            if self.market_prices is None:
                return []
            return [{"slug": SLUG, "outcomePrices": json.dumps(
                [str(p) for p in self.market_prices])}]
        raise AssertionError(f"unexpected URL in test: {url}")


orig_get_json = st.get_json
OLD_TS = int(time.time()) - 3600   # > 5 min old, passes SETTLE_MIN_AGE
FRESH_TS = int(time.time()) - 60   # < 5 min old, must NOT settle yet


def run_settle_one(last_price, last_ts, ev, **fake_kw):
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp, pos=hemery_pos())
        st.use_paths(str(sp), tmp)
        st.get_json = FakeAPI(last_price, last_ts, ev, **fake_kw)
        try:
            with st.LockedState(str(sp)) as state:
                r = st.settle_one(state, TOKEN)
        finally:
            st.get_json = orig_get_json
        s = json.loads(sp.read_text())
        return r, s


def main():
    finished_ev = event(live=False, score="6-3, 3-6, 6-3", period="FT",
                        ended=True, finished="2026-11-16T12:02:37.925819Z")
    live_ev = event(live=True, score="6-3, 3-6, 2-1", period="S3",
                    ended=False)
    prematch_ev = event(live=None, score=None, period="NS", ended=None)

    # 1. Hemery replay: last trade 0.9995 + finished event -> settles
    r, s = run_settle_one(0.9995, OLD_TS, finished_ev)
    check("Hemery replay settles at 0.9995",
          r is not None and r["settle_price"] == 0.9995
          and abs(r["settled_value"] - 155.41) < 0.01
          and abs(r["pnl_usd"] - 55.90) < 0.01)
    check("Hemery replay cash credited, position closed",
          abs(s["cash"] - (927.74 + 155.41)) < 0.01 and not s["positions"])

    # 2. Hemery replay via check_position -> 'settle' action
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp, pos=hemery_pos())
        st.use_paths(str(sp), tmp)
        st.get_json = FakeAPI(0.9995, OLD_TS, finished_ev)
        try:
            with st.LockedState(str(sp)) as state:
                action, reason, detail = st.check_position(
                    state, TOKEN, state["positions"][TOKEN])
        finally:
            st.get_json = orig_get_json
        check("check_position Hemery replay returns settle",
              action == "settle" and reason == "市场已结算")

    # 3. Live match at 0.995 (match point) -> does NOT settle
    r, s = run_settle_one(0.995, OLD_TS, live_ev,
                          market_prices=[0.995, 0.005])
    check("live match at 0.995 does not settle",
          r is None and TOKEN in s["positions"]
          and abs(s["cash"] - 927.74) < 0.01)
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp, pos=hemery_pos())
        st.use_paths(str(sp), tmp)
        st.get_json = FakeAPI(0.995, OLD_TS, live_ev,
                              market_prices=[0.995, 0.005])
        try:
            with st.LockedState(str(sp)) as state:
                action, _, _ = st.check_position(
                    state, TOKEN, state["positions"][TOKEN])
        finally:
            st.get_json = orig_get_json
        check("check_position live 0.995 action is None (no settle)",
              action is None)

    # 4. Exact 1.0 still settles, and does NOT fetch the event
    r, s = run_settle_one(1.0, OLD_TS, finished_ev, forbid_event=True)
    check("exact 1.0 settles without event lookup",
          r is not None and r["settle_price"] == 1.0
          and abs(r["settled_value"] - 155.49) < 0.01
          and not s["positions"])

    # 5. Exact 0.0 still settles (losing side), no event lookup
    r, s = run_settle_one(0.0, OLD_TS, finished_ev, forbid_event=True)
    check("exact 0.0 settles without event lookup",
          r is not None and r["settle_price"] == 0.0
          and r["settled_value"] == 0.0
          and abs(r["pnl_usd"] - (-99.51)) < 0.01
          and not s["positions"])

    # 6. Pre-match at 0.9995 -> does NOT settle (not finished yet)
    r, s = run_settle_one(0.9995, OLD_TS, prematch_ev)
    check("pre-match at 0.9995 does not settle",
          r is None and TOKEN in s["positions"])

    # 7. Finished match but last trade < 5 min old -> waits (min-age guard)
    r, s = run_settle_one(0.9995, FRESH_TS, finished_ev)
    check("finished match with fresh last trade waits a round",
          r is None and TOKEN in s["positions"])

    # 8. Near-settle with event fetch failing -> does NOT settle (fail closed)
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp, pos=hemery_pos())
        st.use_paths(str(sp), tmp)

        def boom(url, timeout=10):
            if "/events/slug/" in url:
                raise ConnectionError("network down")
            if "/prices-history" in url:
                return {"data": [{"price": 0.9995, "timestamp": OLD_TS}]}
            raise AssertionError(f"unexpected URL: {url}")

        st.get_json = boom
        try:
            with st.LockedState(str(sp)) as state:
                r = st.settle_one(state, TOKEN)
        finally:
            st.get_json = orig_get_json
        check("event lookup failure -> no settle (fail closed)", r is None)

    # 9. Finished via live=False + final score alone (no ended flag)
    ev2 = event(live=False, score="6-3, 3-6, 6-3", period="FT",
                ended=None, finished=None)
    r, s = run_settle_one(0.9995, OLD_TS, ev2)
    check("live=False + final score settles even without ended flag",
          r is not None and not s["positions"])

    # 10. No last trade at all -> no settle, no crash
    r, s = run_settle_one(None, 0, finished_ev)
    check("no last trade -> no settle", r is None and TOKEN in s["positions"])

    print(f"settle-finished regression tests: {N}/{N} PASS")


if __name__ == "__main__":
    main()
