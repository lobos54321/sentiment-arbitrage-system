import os
import sys
from types import SimpleNamespace


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from exit_engine import _ath_no_kline_fast_fail_detail, _quote_primary_exit_confirmation  # noqa: E402


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
