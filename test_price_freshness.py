#!/usr/bin/env python3
"""Regression tests for the buy-side price-freshness guard (2026-11-16).

2026-11-16 19:42 incident: sim buy for V7 analysis 41 (Metz Nardi vs
Latinovic, buy Latinovic/No) executed at $84.96 @ 0.5005 because
live_ask_price() trusted a single Gamma snapshot. The true price at that
moment was ~0.58-0.675 (the push-step recheck minutes later saw 0.675,
Edge negative, so the pick was correctly NOT pushed -- but the buy had
already executed). The exit monitor sold at $0.201 (-$50.93); Dylan voided
the whole round-trip. Root cause: CLOB /book,/price,/midpoint return
{"error":"Invalid token id"} for every token, so Gamma is the only live
price source, and there was no freshness check on the Gamma price.

Guard (fresh_gamma_ask + fresh_ask_price, used by cmd_buy):
  (1) snapshot updatedAt older than 10 min -> refuse (gamma-stale);
      NOTE: live markets' updatedAt lags 2-4 min in practice (Hemery's was
      107s old at its legitimate buy), so the threshold is 10 min, NOT 60s.
  (2) two reads 2s apart differing > 0.03 -> refuse (gamma-jump);
  (3) /markets vs /events same-market price differing > 0.03 -> refuse
      (gamma-mismatch);
  (4) fresh price vs V7 eval price (--price) differing > 0.05 -> refuse
      (eval-diverged). This is the check that deterministically replays the
      Nardi incident: eval 0.5835 vs snapshot 0.5005 = 0.083 divergence.

Run: ./.venv/bin/python test_price_freshness.py
Tests use temp state dirs only; the live ledger is never touched.
"""
import json
import sys
import tempfile
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
        pass  # no updatedAt key at all
    else:
        d["updatedAt"] = _iso_now_minus(updated_at)
    return d


class FakeGamma:
    """Fake st.get_json: /markets pops successive snapshots, /events returns
    a fixed nested market, CLOB /book returns the Invalid-token-id error."""

    def __init__(self, market_reads, event_prices, slug):
        self.market_reads = list(market_reads)
        self.event_prices = event_prices
        self.slug = slug
        self.calls = []

    def __call__(self, url, timeout=10):
        self.calls.append(url)
        if "clob.polymarket.com" in url:
            return {"error": "Invalid token id"}
        if "prices-history" in url:
            # This suite predates the 5th (history) layer: no trades in the
            # window -> fail open with warning, as on illiquid markets.
            return {"data": []}
        if "/events/slug/" in url:
            return {"markets": [{
                "slug": self.slug,
                "outcomePrices": json.dumps([str(p) for p in self.event_prices]),
            }]}
        if "/markets?slug=" in url:
            if self.market_reads:
                return [self.market_reads.pop(0)]
            return [self.market_reads[-1]] if self.market_reads else []
        raise AssertionError(f"unexpected URL in test: {url}")


SLUG = "atp-nardi-latinov-2026-11-16"
N = 0


def check(name, cond):
    global N
    N += 1
    assert cond, f"FAIL: {name}"
    print(f"  [{N}] ok: {name}")


orig_get_json = st.get_json


def run_fresh(market_reads, event_prices, idx=1, delay=0):
    st.get_json = FakeGamma(market_reads, event_prices, SLUG)
    try:
        return st.fresh_gamma_ask(SLUG, idx, delay=delay)
    finally:
        st.get_json = orig_get_json


def run_fresh_ask(market_reads, event_prices, idx, eval_price, delay=0):
    st.get_json = FakeGamma(market_reads, event_prices, SLUG)
    try:
        return st.fresh_ask_price("tok-test", SLUG, idx,
                                  eval_price=eval_price, delay=delay)
    finally:
        st.get_json = orig_get_json


# fresh_gamma_ask needs the delay kwarg threaded through fresh_ask_price;
# default delay is 2s (fine for the two cmd_buy tests below, skipped here
# by calling fresh_gamma_ask directly with delay=0).
def main():
    m = lambda prices, **kw: market_dict(SLUG, prices, **kw)

    # 1. fresh + stable + endpoints agree -> accepted, second read's price
    p, src = run_fresh([m([0.4995, 0.5005]), m([0.4995, 0.5005])],
                       [0.4995, 0.5005])
    check("fresh stable price accepted", p == 0.5005 and src == "gamma-fresh")

    # 2. snapshot updatedAt 15 min old -> refused (gamma-stale)
    p, src = run_fresh([m([0.4995, 0.5005], updated_at=900),
                        m([0.4995, 0.5005], updated_at=900)],
                       [0.4995, 0.5005])
    check("stale snapshot refused", p is None and src == "gamma-stale")

    # 3. two reads jump 0.5005 -> 0.58 -> refused (gamma-jump)
    p, src = run_fresh([m([0.4995, 0.5005]), m([0.42, 0.58])],
                       [0.42, 0.58])
    check("jumping price refused", p is None and src == "gamma-jump")

    # 4. /markets 0.50 twice but /events says 0.66 -> refused (mismatch)
    p, src = run_fresh([m([0.4995, 0.5005]), m([0.4995, 0.5005])],
                       [0.34, 0.66])
    check("cross-endpoint mismatch refused",
          p is None and src == "gamma-mismatch")

    # 5. missing updatedAt + stable prices -> accepted (warn only, not blocking)
    p, src = run_fresh([m([0.4995, 0.5005], updated_at=None),
                        m([0.4995, 0.5005], updated_at=None)],
                       [0.4995, 0.5005])
    check("missing updatedAt accepted with warning",
          p == 0.5005 and src == "gamma-fresh")

    # 6. NARDI REPLAY: eval 0.5835, Gamma fresh+stable at 0.5005,
    #    endpoints agree -> refused on eval divergence (0.083 > 0.05)
    p, src = run_fresh_ask([m([0.4995, 0.5005]), m([0.4995, 0.5005])],
                           [0.4995, 0.5005], idx=1, eval_price=0.5835)
    check("Nardi replay refused (eval-diverged)",
          p is None and src == "eval-diverged")

    # 7. Hemery-like: eval 0.635, fresh ask 0.64 (idx 0, bestAsk) -> accepted
    p, src = run_fresh_ask(
        [m([0.64, 0.36], best_ask=0.64), m([0.64, 0.36], best_ask=0.64)],
        [0.64, 0.36], idx=0, eval_price=0.635)
    check("Hemery-like fresh buy accepted", p == 0.64 and src == "gamma-fresh")

    # 8. Trail Blazers E2E-like: eval 0.45, ask 0.445 (idx 0) -> accepted
    p, src = run_fresh_ask(
        [m([0.445, 0.555], best_ask=0.445), m([0.445, 0.555], best_ask=0.445)],
        [0.445, 0.555], idx=0, eval_price=0.45)
    check("Trail Blazers-like fresh buy accepted",
          p == 0.445 and src == "gamma-fresh")

    # ---- end-to-end cmd_buy with temp state (live ledger untouched) ----
    def buy_args(**over):
        d = dict(state_file="", sim_dir="", token_id="tok-test", slug=SLUG,
                 title="Metz: Luca Nardi vs Stefan Latinovic",
                 outcome="Stefan Latinovic", outcome_side="no",
                 outcome_index=1, price=0.5835, no_chase_cap=0.62,
                 prob_low=0.65, kelly_frac=0.0754, sport="tennis",
                 market_type="胜负", phase="盘中",
                 polymarket_url="https://polymarket.com/event/" + SLUG,
                 reversal="", rev_lead_min=None, rev_trailing=False,
                 rev_team="", espn_league="tennis/atp", home_team="",
                 away_team="", line=None, total_side="", analysis_id="41-replay")
        d.update(over)
        return SimpleNamespace(**d)

    def temp_state(tmp):
        sp = Path(tmp) / "state.json"
        sp.write_text(json.dumps({"cash": 1126.76, "positions": {},
                                  "trades": 0, "sells": 0, "seen_tx": [],
                                  "pending": {}}))
        return sp

    # 9. cmd_buy Nardi replay -> refused, cash untouched, no position
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp)
        st.get_json = FakeGamma(
            [m([0.4995, 0.5005]), m([0.4995, 0.5005])],
            [0.4995, 0.5005], SLUG)
        try:
            rc = st.cmd_buy(buy_args(state_file=str(sp), sim_dir=tmp))
        finally:
            st.get_json = orig_get_json
        s = json.loads(sp.read_text())
        check("cmd_buy Nardi replay refused",
              rc == 1 and s["cash"] == 1126.76 and not s["positions"]
              and s["trades"] == 0)

    # 10. cmd_buy Hemery-like -> bought at 0.64, cash reduced by stake
    with tempfile.TemporaryDirectory() as tmp:
        sp = temp_state(tmp)
        st.get_json = FakeGamma(
            [m([0.64, 0.36], best_ask=0.64), m([0.64, 0.36], best_ask=0.64)],
            [0.64, 0.36], SLUG)
        try:
            rc = st.cmd_buy(buy_args(
                state_file=str(sp), sim_dir=tmp, outcome="Calvin Hemery",
                outcome_side="yes", outcome_index=0, price=0.635,
                no_chase_cap=0.70, prob_low=0.70,
                kelly_frac=99.51 / 1126.76,
                title="Metz: Calvin Hemery vs Matthew William Donald",
                analysis_id="42-replay"))
        finally:
            st.get_json = orig_get_json
        s = json.loads(sp.read_text())
        pos = list(s["positions"].values())
        check("cmd_buy Hemery-like accepted at fresh price",
              rc == 0 and len(pos) == 1 and pos[0]["entry_price"] == 0.64
              and abs(s["cash"] - (1126.76 - 99.51)) < 0.01)

    print(f"price-freshness regression tests: {N}/{N} PASS")


if __name__ == "__main__":
    main()
