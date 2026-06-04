import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_exit_policy import evaluate_a_class_exit_policy


def pos(**state):
    return SimpleNamespace(
        entry_mode=state.pop("entry_mode", "a_class_fastlane"),
        peak_pnl=state.pop("peak_pnl", 0.0),
        monitor_state=state,
    )


def test_a_class_exit_policy_holds_noise_during_grace_but_not_flash_crash():
    noisy = evaluate_a_class_exit_policy(pos(), current_pnl=-0.12, peak_pnl=0.0, held_sec=20)
    crash = evaluate_a_class_exit_policy(pos(), current_pnl=-0.21, peak_pnl=0.0, held_sec=20)

    assert noisy["action"] == "hold"
    assert noisy["reason"] == "a_class_grace_period_noise"
    assert crash["action"] == "exit"
    assert crash["reason"] == "a_class_flash_crash_loss_cap"


def test_a_class_exit_policy_fast_stops_after_grace_and_on_route_failure():
    stopped = evaluate_a_class_exit_policy(pos(), current_pnl=-0.16, peak_pnl=0.0, held_sec=90)
    no_route = evaluate_a_class_exit_policy(pos(), current_pnl=0.40, peak_pnl=0.80, held_sec=20, route_available=False)

    assert stopped["action"] == "exit"
    assert stopped["reason"] == "a_class_fast_stop_loss"
    assert no_route["action"] == "exit"
    assert no_route["reason"] == "a_class_route_or_quote_failed"


def test_a_class_exit_policy_locks_profit_and_keeps_moonbag():
    partial = evaluate_a_class_exit_policy(pos(), current_pnl=0.55, peak_pnl=0.55, held_sec=80)
    recover = evaluate_a_class_exit_policy(pos(), current_pnl=1.10, peak_pnl=1.10, held_sec=120)
    moon = evaluate_a_class_exit_policy(pos(soldPct=0.60), current_pnl=3.50, peak_pnl=3.50, held_sec=180)

    assert partial["action"] == "partial_sell"
    assert partial["partial_type"] == "A_CLASS_PARTIAL_LOCK"
    assert recover["action"] == "partial_sell"
    assert recover["partial_type"] == "A_CLASS_PRINCIPAL_RECOVERY"
    assert moon["action"] == "partial_sell"
    assert moon["partial_type"] == "A_CLASS_MOONBAG"
    assert round(moon["sell_pct"], 2) == 0.10


def test_a_class_exit_policy_does_not_apply_to_normal_modes():
    decision = evaluate_a_class_exit_policy(
        SimpleNamespace(entry_mode="smart_entry_pullback_bounce", peak_pnl=1.0, monitor_state={}),
        current_pnl=-0.50,
        held_sec=90,
    )

    assert decision["applies"] is False
    assert decision["action"] == "hold"


def test_a_class_exit_policy_detects_monitor_normalized_entry_mode_field():
    decision = evaluate_a_class_exit_policy(
        SimpleNamespace(entry_mode="raw_legacy_mode", peak_pnl=0.0, monitor_state={"normalizedEntryMode": "A_CLASS_FASTLANE"}),
        current_pnl=-0.16,
        peak_pnl=0.0,
        held_sec=90,
    )

    assert decision["applies"] is True
    assert decision["reason"] == "a_class_fast_stop_loss"
