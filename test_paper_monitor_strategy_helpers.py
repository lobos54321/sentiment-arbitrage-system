import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

import paper_trade_monitor as monitor  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    ATH_HIGH_MC_TINY_PROBE_MODE,
    ATH_NO_KLINE_TINY_PROBE_MODE,
    ATH_UNCERTAINTY_TINY_SCOUT_MODE,
    MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
    PAPER_TINY_SCOUT_SIZE_SOL,
    PRIMARY_PROVING_CAP_SIZE_SOL,
    SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL,
    SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL,
    _apply_actual_tiny_trigger_mode,
    _apply_primary_proving_cap,
    _discovery_hard_block,
    _entry_mode_for_ath_uncertainty_reason,
    _entry_mode_quality_high_quality_tiny_override,
    _matrix_micro_momentum_reason,
)


def test_micro_momentum_reasons_are_split_from_broad_matrix_reclaim():
    assert _matrix_micro_momentum_reason("momentum check failed: declining 0.00%")
    assert _matrix_micro_momentum_reason("momentum check failed: noise < 0.8%")
    assert _matrix_micro_momentum_reason("momentum check waiting: flat_no_fresh_tick")
    assert not _matrix_micro_momentum_reason("momentum check failed: declining 1.20%")

    assert (
        _entry_mode_for_ath_uncertainty_reason("momentum check failed: declining 0.00%")
        == MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE
    )
    assert (
        _entry_mode_for_ath_uncertainty_reason("ath_uncertainty_mc_gate")
        == ATH_UNCERTAINTY_TINY_SCOUT_MODE
    )


def test_actual_tiny_trigger_mode_preserves_parent_scout_attribution():
    pending = {
        "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        "scout_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
    }

    actual_mode = _apply_actual_tiny_trigger_mode(pending, "pullback_tiny_scout")

    assert actual_mode == "pullback_tiny_scout"
    assert pending["entry_mode"] == "pullback_tiny_scout"
    assert pending["entry_trigger_mode"] == "pullback_tiny_scout"
    assert pending["parent_scout_mode"] == ATH_UNCERTAINTY_TINY_SCOUT_MODE
    assert pending["scout_mode"] == ATH_UNCERTAINTY_TINY_SCOUT_MODE


def test_matrix_micro_momentum_probe_uses_ath_discovery_hard_gates():
    reason = _discovery_hard_block(
        MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
        current_mc=5_000_000,
        liquidity_usd=20000,
        top10_pct=20,
    )

    assert reason == "discovery_ath_mc_gate"


def test_primary_proving_cap_limits_momentum_direct_without_touching_tiny_scout():
    pending = {"entry_mode": "momentum_direct_entry", "kelly_position_sol": 0.1}

    size, detail = _apply_primary_proving_cap(pending, 0.1)

    assert size == PRIMARY_PROVING_CAP_SIZE_SOL
    assert detail["reason"] == "primary_proving_cap"
    assert pending["kelly_position_sol"] == PRIMARY_PROVING_CAP_SIZE_SOL

    tiny_pending = {
        "entry_mode": "pullback_tiny_scout",
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
    }

    size, detail = _apply_primary_proving_cap(tiny_pending, PAPER_TINY_SCOUT_SIZE_SOL)

    assert size == PAPER_TINY_SCOUT_SIZE_SOL
    assert detail is None


def test_smart_entry_pullback_bounce_uses_smaller_proving_cap():
    pending = {"entry_mode": "smart_entry_pullback_bounce", "kelly_position_sol": 0.1}

    size, detail = _apply_primary_proving_cap(pending, 0.1)

    assert size == SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL
    assert detail["reason"] == "smart_pullback_bounce_proving_cap"
    assert pending["kelly_position_sol"] == SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL


def test_smart_entry_pullback_bounce_degraded_force_live_caps_to_tiny():
    pending = {
        "entry_mode": "smart_entry_pullback_bounce",
        "kelly_position_sol": 0.1,
        "entry_mode_quality_force_live": {"reason": "entry_mode_quality_high_quality_tiny_override"},
    }

    size, detail = _apply_primary_proving_cap(pending, 0.1)

    assert size == SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL
    assert detail["cap_sol"] == SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL


def test_new_ath_probe_modes_are_tiny_scouts_and_get_capped():
    for mode in (ATH_NO_KLINE_TINY_PROBE_MODE, ATH_HIGH_MC_TINY_PROBE_MODE):
        pending = {"entry_mode": mode, "kelly_position_sol": 0.1}

        assert monitor.pending_is_paper_tiny_scout(pending)
        detail = monitor.apply_paper_tiny_scout_size_cap(pending)

        assert pending["kelly_position_sol"] == PAPER_TINY_SCOUT_SIZE_SOL
        assert detail["capped"] is True


def test_entry_mode_quality_high_quality_tiny_override_allows_strong_ath():
    pending = {
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 60},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is True
    assert decision["reason"] == "entry_mode_quality_high_quality_tiny_override"


def test_entry_mode_quality_high_quality_tiny_override_keeps_weak_scores_shadowed():
    pending = {
        "entry_mode": "pullback_tiny_scout",
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 50, "volume": 70, "price": 80, "signal": 100, "momentum": 60},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "matrix_not_strong_enough"


def test_unknown_data_live_gate_shadows_weak_activity_without_quote(monkeypatch):
    called = False

    def fake_quote(*args, **kwargs):
        nonlocal called
        called = True
        return {"success": True}

    monkeypatch.setattr(monitor, "_discovery_quote_probe", fake_quote)

    gate = monitor._discovery_unknown_data_live_gate(
        "TokenCA",
        mode="unknown_data_activity_tiny_scout",
        activity={
            "buy_sell_ratio": 1.0,
            "vol_m5": 5000,
            "tx_m5": 60,
            "price_change_m5": -2,
        },
    )

    assert gate["pass"] is False
    assert gate["reason"] == "unknown_data_activity_not_enough"
    assert called is False


def test_unknown_data_live_gate_allows_base_activity_only_when_quote_executes(monkeypatch):
    monkeypatch.setattr(
        monitor,
        "_discovery_quote_probe",
        lambda *args, **kwargs: {"success": True, "reason": "quote_executable"},
    )

    gate = monitor._discovery_unknown_data_live_gate(
        "TokenCA",
        mode="unknown_data_activity_tiny_scout",
        activity={
            "buy_sell_ratio": 1.11,
            "vol_m5": 13000,
            "tx_m5": 110,
            "price_change_m5": -1,
        },
    )

    assert gate["pass"] is True
    assert gate["reason"] == "unknown_data_quote_executable"
    assert gate["quote_probe"]["success"] is True


def test_unknown_data_live_gate_allows_extreme_activity_without_quote(monkeypatch):
    called = False

    def fake_quote(*args, **kwargs):
        nonlocal called
        called = True
        return {"success": False}

    monkeypatch.setattr(monitor, "_discovery_quote_probe", fake_quote)

    gate = monitor._discovery_unknown_data_live_gate(
        "TokenCA",
        mode="unknown_data_activity_tiny_scout",
        activity={
            "buy_sell_ratio": 1.25,
            "vol_m5": 25000,
            "tx_m5": 240,
            "price_change_m5": 2,
        },
    )

    assert gate["pass"] is True
    assert gate["reason"] == "unknown_data_extreme_activity"
    assert called is False
