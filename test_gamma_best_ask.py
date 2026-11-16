#!/usr/bin/env python3
"""Regression test for the gamma_best_ask side bug (2026-11-16).

Gamma /markets bestAsk/bestBid are market-level fields belonging to
outcome 0 only. The old gamma_best_ask returned bestAsk for any
outcome_index, so outcome_index=1 reads got the opposite side's price
(e.g. Celtics 0.765 read as 0.240) and the multi-watch fired 3 false
triggers. Fixed: bestAsk is used only for outcome 0; other sides use
outcomePrices[outcome_index].
Run: ./.venv/bin/python test_gamma_best_ask.py
"""
import sys

sys.path.insert(0, ".")
import sim_trade as st


def fake_markets(best_ask, prices):
    def _get(url, timeout=10):
        return [{
            "slug": "nba-orl-bos-2026-11-16",
            "outcomes": ["Magic", "Celtics"],
            "outcomePrices": prices,
            "bestAsk": best_ask,
        }]
    return _get


orig = st.get_json
try:
    st.get_json = fake_markets(0.24, '["0.235", "0.765"]')
    assert st.gamma_best_ask("nba-orl-bos-2026-11-16", 0) == 0.24, "idx0 uses bestAsk"
    assert st.gamma_best_ask("nba-orl-bos-2026-11-16", 1) == 0.765, "idx1 uses outcomePrices[1]"
    st.get_json = fake_markets(None, '["0.235", "0.765"]')
    assert st.gamma_best_ask("nba-orl-bos-2026-11-16", 0) == 0.235, "idx0 falls back to outcomePrices[0]"
    assert st.gamma_best_ask("nba-orl-bos-2026-11-16", 1) == 0.765, "idx1 falls back to outcomePrices[1]"
    st.get_json = fake_markets(0.24, "[]")
    assert st.gamma_best_ask("nba-orl-bos-2026-11-16", 1) is None, "idx1 with no prices -> None, no crash"
    print("gamma_best_ask regression tests: 5/5 PASS")
finally:
    st.get_json = orig
