import os
import sys
from types import SimpleNamespace


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

import exit_engine  # noqa: E402
from exit_engine import (  # noqa: E402
    _ath_no_kline_fast_fail_detail,
    _quote_freshness_detail,
    _quote_primary_exit_confirmation,
    _quote_stop_exit_confirmation,
    process_guardian_exits,
)


def test_ath_no_kline_fast_fail_triggers_on_low_peak_red_probe():
    pos = SimpleNamespace(
        peak_pnl=0.03,
        monitor_state={"entryMode": "ath_no_kline_tiny_probe"},
    )

    detail = _ath_no_kline_fast_fail_detail(pos, -0.04, 60)

    assert detail["pass"] is True
    assert detail["reason"] == "guardian_ath_no_kline_no_follow_fast_fail"


def test_ath_no_kline_emergency_fast_fail_triggers_before_standard_timer():
    pos = SimpleNamespace(
        peak_pnl=0.0,
        monitor_state={"entryMode": "ath_no_kline_tiny_probe"},
    )

    detail = _ath_no_kline_fast_fail_detail(pos, -0.09, 22)

    assert detail["pass"] is True
    assert detail["reason"] == "guardian_ath_no_kline_emergency_fast_fail"


def test_ath_no_kline_fast_fail_does_not_touch_other_modes():
    pos = SimpleNamespace(
        peak_pnl=0.0,
        monitor_state={"entryMode": "ath_uncertainty_tiny_scout"},
    )

    detail = _ath_no_kline_fast_fail_detail(pos, -0.10, 60)

    assert detail["pass"] is False
    assert detail["reason"] == "not_ath_no_kline_tiny_probe"


def test_ath_no_kline_fast_fail_keeps_probe_with_real_peak():
    pos = SimpleNamespace(
        peak_pnl=0.08,
        monitor_state={"entryMode": "ath_no_kline_tiny_probe"},
    )

    detail = _ath_no_kline_fast_fail_detail(pos, -0.03, 60)

    assert detail["pass"] is False
    assert detail["reason"] == "ath_no_kline_fast_fail_peak_ok"


def test_quote_primary_exit_requires_profit_floor_breach():
    keep = _quote_primary_exit_confirmation(
        "guardian_trail_stop (pnl=16.3% < floor=18.3%, peak=16.4%)",
        quote_pnl=0.20,
    )
    exit_now = _quote_primary_exit_confirmation(
        "guardian_trail_stop (pnl=16.3% < floor=18.3%, peak=16.4%)",
        quote_pnl=0.12,
    )

    assert keep["pass"] is False
    assert keep["reason"] == "quote_primary_floor_not_breached"
    assert exit_now["pass"] is True
    assert exit_now["reason"] == "quote_primary_exit_confirmed"


def test_exit_quote_freshness_rejects_stale_quote():
    detail = _quote_freshness_detail(
        {"success": True, "quoteTs": 1_778_459_930_000},
        now_ts=1_778_460_000,
        max_age_sec=10,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "stale_exit_quote"
    assert detail["quote_age_sec"] == 70


def test_quote_stop_exit_requires_quote_floor_breach():
    keep = _quote_stop_exit_confirmation(
        "guardian_hard_sl (-31.0% <= -30.0%)",
        quote_pnl=-0.115,
        trigger_pnl=-0.31,
    )
    exit_now = _quote_stop_exit_confirmation(
        "guardian_hard_sl (-31.0% <= -30.0%)",
        quote_pnl=-0.32,
        trigger_pnl=-0.31,
    )

    assert keep["pass"] is False
    assert keep["reason"] == "quote_stop_floor_not_breached"
    assert exit_now["pass"] is True
    assert exit_now["reason"] == "quote_stop_exit_confirmed"


def test_guardian_hard_sl_discards_stale_quote_and_keeps_when_fresh_quote_above_floor(monkeypatch):
    monkeypatch.setattr(exit_engine.time, "time", lambda: 1_778_460_000.0)
    calls = []

    class _Guardian:
        def get_pending_exits(self):
            return [
                {
                    "trade_id": 1,
                    "symbol": "TINY",
                    "reason": "guardian_hard_sl (-31.0% <= -30.0%)",
                    "trigger_price": 0.69,
                    "trigger_pnl": -0.31,
                    "_instant_sim": {
                        "success": True,
                        "effectivePrice": 0.885,
                        "quoteTs": 1_778_459_930_000,
                    },
                }
            ]

    pos = SimpleNamespace(
        token_ca="tiny-ca",
        symbol="TINY",
        lifecycle_id="tiny-life",
        signal_ts=900,
        premium_signal_id=None,
        signal_type="LOTTO",
        entry_price=1.0,
        token_amount_raw=1000,
        token_decimals=0,
        strategy_stage="lotto",
        peak_pnl=0,
        price_ring=[],
    )

    def simulate_exit(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "success": True,
            "effectivePrice": 0.885,
            "quoteTs": 1_778_460_000_000,
        }

    result = process_guardian_exits(
        _Guardian(),
        {1: pos},
        {},
        "strategy",
        lambda *args: {},
        simulate_exit,
    )

    assert calls
    assert result == []
