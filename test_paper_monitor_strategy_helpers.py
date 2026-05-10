import os
import sqlite3
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

import paper_trade_monitor as monitor  # noqa: E402
from paper_decision_audit import init_decision_audit  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    ATH_HIGH_MC_TINY_PROBE_MODE,
    ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
    ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
    ATH_NO_KLINE_TINY_PROBE_MODE,
    ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
    ATH_UNCERTAINTY_TINY_SCOUT_MODE,
    LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
    LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
    MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
    PAPER_TINY_SCOUT_SIZE_SOL,
    PRIMARY_PROVING_CAP_SIZE_SOL,
    SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL,
    SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL,
    _apply_actual_tiny_trigger_mode,
    _apply_primary_proving_cap,
    _ath_reentry_block_cooldown_sec,
    _ath_no_kline_followthrough_guard,
    _ath_no_kline_reentry_guard,
    _ath_no_kline_scout_quality_soft_override,
    _ath_dynamic_ttl_extension_detail,
    _ath_recovery_eligibility,
    _ath_recovery_mode_for_candidate,
    _defer_ath_reentry_block,
    _discovery_hard_block,
    _discovery_mode_for_lotto_reason,
    _lotto_dynamic_ttl_extension_detail,
    _lotto_recovery_activity_gate,
    _lotto_recovery_mode_for_blocker,
    _retarget_discovery_candidate,
    _build_discovery_pending,
    _entry_mode_for_ath_uncertainty_reason,
    _entry_mode_quality_high_quality_tiny_override,
    _matrix_micro_momentum_reason,
    _pending_watchlist_fire_block_detail,
    _select_structure_stop_loss,
)


def _paper_trade_db(rows=()):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT,
            entry_mode TEXT,
            entry_ts INTEGER,
            exit_ts INTEGER,
            exit_reason TEXT,
            pnl_pct REAL,
            peak_pnl REAL,
            entry_price REAL,
            exit_price REAL
        )
        """
    )
    for row in rows:
        db.execute(
            """
            INSERT INTO paper_trades (
                token_ca, entry_mode, entry_ts, exit_ts, exit_reason,
                pnl_pct, peak_pnl, entry_price, exit_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("token_ca", "TokenCA"),
                row.get("entry_mode", ATH_NO_KLINE_TINY_PROBE_MODE),
                row.get("entry_ts", 1_778_220_000),
                row.get("exit_ts", 1_778_220_300),
                row.get("exit_reason"),
                row.get("pnl_pct"),
                row.get("peak_pnl"),
                row.get("entry_price", 1.0),
                row.get("exit_price", 1.0),
            ),
        )
    db.commit()
    return db


def _ath_no_kline_pending():
    return {
        "token_ca": "TokenCA",
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 60},
    }


class _FakeWatchlist:
    def __init__(self, entry=None):
        self.entry = dict(entry or {"id": 7})
        self.deferred = None

    def defer_fire(self, entry_id, reason, cooldown_sec=300):
        self.deferred = {
            "entry_id": entry_id,
            "reason": reason,
            "cooldown_sec": cooldown_sec,
        }
        until = 1_778_223_700
        self.entry.update({
            "fire_block_until": until,
            "fire_block_reason": reason,
        })
        return until

    def get_by_id(self, entry_id):
        if entry_id == self.entry.get("id"):
            return dict(self.entry)
        return None


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
    for mode in (
        ATH_NO_KLINE_TINY_PROBE_MODE,
        ATH_HIGH_MC_TINY_PROBE_MODE,
        ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
        ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
        ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
    ):
        pending = {"entry_mode": mode, "kelly_position_sol": 0.1}

        assert monitor.pending_is_paper_tiny_scout(pending)
        detail = monitor.apply_paper_tiny_scout_size_cap(pending)

        assert pending["kelly_position_sol"] == PAPER_TINY_SCOUT_SIZE_SOL
        assert detail["capped"] is True


def test_lotto_recovery_probe_modes_are_tiny_scouts_and_get_capped():
    for mode in (
        LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    ):
        pending = {"entry_mode": mode, "kelly_position_sol": 0.1}

        assert monitor.pending_is_paper_tiny_scout(pending)
        detail = monitor.apply_paper_tiny_scout_size_cap(pending)

        assert pending["kelly_position_sol"] == PAPER_TINY_SCOUT_SIZE_SOL
        assert detail["capped"] is True


def test_lotto_recovery_reason_mapping_splits_missed_blockers():
    assert _discovery_mode_for_lotto_reason("not_ath_v17") == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("tracking_ttl_expired") == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("upstream_probe_mc_gate") == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    assert (
        _discovery_mode_for_lotto_reason("upstream_realtime_liquidity_too_low")
        == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    )
    assert _discovery_mode_for_lotto_reason("scout_quality_liquidity_low") == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("discovery_lotto_recovery_liquidity_too_low") == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("weak_buying_pressure") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("chasing_top") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("dead_cat_below_high_37.5pct_gt_10.0pct") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("lotto_live_top1_38pct") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("scout_quality_volume_low") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert _discovery_mode_for_lotto_reason("score_too_low") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE


def test_lotto_recovery_mode_uses_latest_blocker_over_original_reason():
    assert (
        _lotto_recovery_mode_for_blocker(
            primary_reason="not_ath_v17",
            secondary_reason="scout_quality_liquidity_low",
            current_mode=LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        )
        == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    )
    assert (
        _lotto_recovery_mode_for_blocker(
            primary_reason="not_ath_v17",
            secondary_reason="scout_quality_buy_pressure_weak",
            current_mode=LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        )
        == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    )


def test_retarget_discovery_candidate_rekeys_and_audits_latest_blocker():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    old_key = "TokenCA:1000:lotto_not_ath_reclaim_tiny_probe"
    candidates = {
        old_key: {
            "key": old_key,
            "mode": LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
            "route": "LOTTO",
            "token_ca": "TokenCA",
            "symbol": "DOG",
            "lifecycle_id": "TokenCA:1000",
            "signal_ts": 1000,
            "source_component": "upstream_gate",
            "source_reject_reason": "not_ath_v17",
            "first_seen_ts": 1000,
        }
    }

    new_key = _retarget_discovery_candidate(
        db,
        candidates,
        old_key,
        candidates[old_key],
        new_mode=LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        reason="lotto_latest_scout_quality_retarget",
        now_ts=1100,
        detail={"scout_quality_reason": "scout_quality_liquidity_low"},
    )

    assert old_key not in candidates
    assert new_key in candidates
    assert candidates[new_key]["mode"] == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    row = db.execute(
        "SELECT component, event_type, decision, reason FROM paper_decision_events"
    ).fetchone()
    assert dict(row) == {
        "component": "discovery_tracking",
        "event_type": "candidate_retarget",
        "decision": "track",
        "reason": "lotto_latest_scout_quality_retarget",
    }


def test_lotto_discovery_pending_records_recovery_attribution():
    pending = _build_discovery_pending(
        {
            "id": 9,
            "ca": "TokenCA",
            "symbol": "DOG",
            "type": "LOTTO",
            "signal_ts": 1000,
            "premium_signal_id": 123,
            "pool_address": "Pool",
            "signal_mc": 42000,
        },
        {
            "route": "LOTTO",
            "token_ca": "TokenCA",
            "symbol": "DOG",
            "signal_ts": 1000,
            "source_component": "scout_quality",
            "source_reject_reason": "scout_quality_liquidity_low",
            "first_seen_ts": 1010,
            "attempts": 2,
        },
        "TokenCA:1000",
        LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        {
            "current_mc": 43000,
            "lotto_recovery_gate": {"reason": "lotto_low_liquidity_reclaim_live_reclaim_pass"},
        },
    )

    assert pending["lotto_recovery_family"] == "lotto_low_liquidity_reclaim"
    assert pending["parent_block_reason"] == "scout_quality_liquidity_low"
    assert pending["recovery_probe_reason"] == "lotto_low_liquidity_reclaim_live_reclaim_pass"
    assert pending["lotto_state"]["lottoRecoveryFamily"] == "lotto_low_liquidity_reclaim"


def test_lotto_recovery_gate_requires_quote_and_current_reclaim():
    weak = _lotto_recovery_activity_gate(
        LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.3, "vol_m5": 12000, "tx_m5": 100, "price_change_m5": 4},
        quote_probe={"success": False, "reason": "quote_not_executable"},
        current_mc=42000,
        liquidity_usd=12000,
        top1_pct=35,
        top10_pct=60,
        now_ts=1060,
    )
    assert weak["pass"] is False
    assert "quote_not_executable" in weak["failures"]

    strong = _lotto_recovery_activity_gate(
        LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.3, "vol_m5": 12000, "tx_m5": 100, "price_change_m5": 4},
        quote_probe={"success": True},
        current_mc=42000,
        liquidity_usd=12000,
        top1_pct=35,
        top10_pct=60,
        now_ts=1060,
    )
    assert strong["pass"] is True
    assert strong["reason"] == "lotto_not_ath_reclaim_live_reclaim_pass"


def test_lotto_micro_reclaim_expires_after_short_watch():
    detail = _lotto_recovery_activity_gate(
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.4, "vol_m5": 9000, "tx_m5": 90, "price_change_m5": 9},
        quote_probe={"success": True},
        current_mc=52000,
        liquidity_usd=9000,
        top1_pct=30,
        top10_pct=65,
        now_ts=1701,
    )

    assert detail["pass"] is False
    assert "max_watch_sec_expired" in detail["failures"]


def test_lotto_dynamic_ttl_extends_only_strong_quote_executable_recovery():
    candidate = {
        "mode": LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        "first_seen_ts": 1000,
        "ttl_extend_count": 0,
    }
    prelim = _lotto_dynamic_ttl_extension_detail(
        candidate,
        dex_snapshot={"market_cap": 40000, "liquidity_usd": 2500, "vol_m5": 4000, "buys_m5": 60, "sells_m5": 40, "price_change_m5": 3},
        activity={"buy_sell_ratio": 1.5, "vol_m5": 4000, "tx_m5": 100, "price_change_m5": 3},
        now_ts=1200,
    )
    assert prelim["pass"] is True

    final = _lotto_dynamic_ttl_extension_detail(
        candidate,
        dex_snapshot={"market_cap": 40000, "liquidity_usd": 2500, "vol_m5": 4000, "buys_m5": 60, "sells_m5": 40, "price_change_m5": 3},
        activity={"buy_sell_ratio": 1.5, "vol_m5": 4000, "tx_m5": 100, "price_change_m5": 3},
        quote_probe={"success": True},
        require_quote=True,
        now_ts=1200,
    )
    assert final["pass"] is True
    assert final["reason"] == "lotto_tracking_ttl_extended"


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
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 50, "volume": 70, "price": 80, "signal": 100, "momentum": 60},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "matrix_not_strong_enough"


def test_entry_mode_quality_high_quality_tiny_override_keeps_pullback_shadowed_by_default():
    pending = {
        "entry_mode": "pullback_tiny_scout",
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 100, "volume": 100, "price": 100, "signal": 100, "momentum": 80},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "mode_not_overrideable"


def test_ath_no_kline_reentry_guard_allows_first_entry():
    decision = _ath_no_kline_reentry_guard(
        _paper_trade_db(),
        _ath_no_kline_pending(),
        current_price=1.0,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_no_kline_first_entry"


def test_ath_no_kline_reentry_guard_blocks_recent_hard_loss():
    db = _paper_trade_db([
        {"exit_reason": "hard_sl", "pnl_pct": -0.3296, "peak_pnl": 0.04, "exit_price": 1.0},
    ])

    decision = _ath_no_kline_reentry_guard(
        db,
        _ath_no_kline_pending(),
        current_price=1.12,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_no_kline_reentry_hard_loss_cooldown"


def test_ath_reentry_block_is_written_to_watchlist_fire_block():
    guard = {
        "pass": False,
        "reason": "ath_no_kline_reentry_matrix_not_strong",
    }
    watchlist_entry = {"id": 7}
    watchlist = _FakeWatchlist(watchlist_entry)

    detail = _defer_ath_reentry_block(watchlist, watchlist_entry, guard)

    assert detail["pass"] is False
    assert detail["reason"] == "ath_no_kline_reentry_matrix_not_strong"
    assert detail["cooldown_sec"] == _ath_reentry_block_cooldown_sec(
        "ath_no_kline_reentry_matrix_not_strong"
    )
    assert watchlist.deferred["entry_id"] == 7
    assert watchlist_entry["fire_block_reason"] == "ath_no_kline_reentry_matrix_not_strong"


def test_final_entry_guard_blocks_stale_pending_when_watchlist_fire_block_active():
    watchlist = _FakeWatchlist({
        "id": 7,
        "fire_block_until": 1_778_223_700,
        "fire_block_reason": "ath_no_kline_reentry_matrix_not_strong",
    })
    pending = {"watchlist_id": 7, "w_entry": {"id": 7}}

    detail = _pending_watchlist_fire_block_detail(
        watchlist,
        pending,
        now_ts=1_778_223_600,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "ath_no_kline_reentry_matrix_not_strong"
    assert detail["remaining_sec"] == 100


def test_final_entry_guard_allows_when_watchlist_fire_block_expired():
    watchlist = _FakeWatchlist({
        "id": 7,
        "fire_block_until": 1_778_223_500,
        "fire_block_reason": "ath_no_kline_reentry_matrix_not_strong",
    })

    detail = _pending_watchlist_fire_block_detail(
        watchlist,
        {"watchlist_id": 7, "w_entry": {"id": 7}},
        now_ts=1_778_223_600,
    )

    assert detail["pass"] is True
    assert detail["reason"] == "no_watchlist_fire_block"


def test_ath_no_kline_reentry_guard_blocks_low_followthrough():
    db = _paper_trade_db([
        {"exit_reason": "timeout", "pnl_pct": -0.02, "peak_pnl": 0.05, "exit_price": 1.0},
    ])

    decision = _ath_no_kline_reentry_guard(
        db,
        _ath_no_kline_pending(),
        current_price=1.12,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_no_kline_reentry_low_followthrough"


def test_ath_no_kline_outcome_guard_blocks_cross_mode_low_followthrough():
    db = _paper_trade_db([
        {
            "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
            "exit_reason": "guardian_hard_sl",
            "pnl_pct": -0.12,
            "peak_pnl": 0.03,
            "exit_price": 1.0,
        },
    ])

    decision = _ath_no_kline_reentry_guard(
        db,
        _ath_no_kline_pending(),
        current_price=1.12,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_no_kline_reentry_hard_loss_cooldown"
    assert decision["latest_trade"]["entry_mode"] == ATH_UNCERTAINTY_TINY_SCOUT_MODE


def test_ath_no_kline_reentry_guard_allows_recovered_prior_winner():
    db = _paper_trade_db([
        {"exit_reason": "tp", "pnl_pct": 0.18, "peak_pnl": 0.42, "exit_price": 1.0},
    ])

    decision = _ath_no_kline_reentry_guard(
        db,
        _ath_no_kline_pending(),
        current_price=1.09,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_no_kline_reentry_allowed"


def test_ath_no_kline_followthrough_blocks_weak_late_attempt():
    decision = _ath_no_kline_followthrough_guard(
        _ath_no_kline_pending(),
        {"buys_m5": 10, "sells_m5": 10, "price_change_m5": -1.0, "vol_m5": 4000},
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_no_kline_followthrough_buy_pressure_weak"


def test_ath_no_kline_followthrough_allows_confirmed_activity():
    decision = _ath_no_kline_followthrough_guard(
        _ath_no_kline_pending(),
        {"buys_m5": 24, "sells_m5": 16, "price_change_m5": 4.0, "vol_m5": 7000},
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_no_kline_followthrough_confirmed"
    assert decision["checks"]["tx_ok"] is True


def test_ath_no_kline_followthrough_blocks_matrix_only_without_live_activity():
    decision = _ath_no_kline_followthrough_guard(
        _ath_no_kline_pending(),
        {"buys_m5": 3, "sells_m5": 2, "price_change_m5": 1.0, "vol_m5": 2500},
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_no_kline_no_followthrough_block"
    assert decision["checks"]["matrix_strong"] is True
    assert decision["checks"]["tx_ok"] is False


def test_tiny_probe_structure_sl_selects_tighter_stop():
    sl, reason = _select_structure_stop_loss(
        -0.30,
        -0.121,
        _ath_no_kline_pending(),
    )

    assert sl == -0.121
    assert reason == "tiny_probe_tight_structure_sl"


def test_primary_structure_sl_keeps_legacy_wider_stop():
    sl, reason = _select_structure_stop_loss(
        -0.30,
        -0.121,
        {"entry_mode": "stage1", "paper_only_scout": False},
    )

    assert sl == -0.30
    assert reason == "primary_wide_structure_sl"


def test_ath_no_kline_volume_low_soft_override_turns_block_into_warn():
    scout_quality = {
        "pass": False,
        "reason": "scout_quality_volume_low",
        "observed": {
            "buy_sell_ratio": 1.35,
            "tx_m5": 80,
            "price_change_m5": 2.0,
            "top1_pct": 18,
            "top10_pct": 42,
            "liquidity_usd": 18000,
            "vol_m5": 1200,
        },
        "thresholds": {
            "min_buy_sell_ratio": 1.10,
            "min_tx_m5": 30,
            "max_negative_m5": -8.0,
            "max_top1_pct": 50,
            "max_top10_pct": 75,
            "min_liquidity_usd": 5000,
        },
    }

    decision = _ath_no_kline_scout_quality_soft_override(
        _ath_no_kline_pending(),
        scout_quality,
        route="ATH",
        scout_size={"actual_size_sol": PAPER_TINY_SCOUT_SIZE_SOL},
    )

    assert decision["pass"] is True
    assert decision["decision"] == "warn"
    assert decision["reason"] == "scout_quality_volume_low_warn_ath_no_kline_override"
    assert decision["original_reason"] == "scout_quality_volume_low"


def test_ath_no_kline_volume_low_soft_override_does_not_hide_tx_weakness():
    scout_quality = {
        "pass": False,
        "reason": "scout_quality_volume_low",
        "observed": {
            "buy_sell_ratio": 1.35,
            "tx_m5": 5,
            "price_change_m5": 2.0,
            "top1_pct": 18,
            "top10_pct": 42,
            "liquidity_usd": 18000,
            "vol_m5": 1200,
        },
        "thresholds": {
            "min_buy_sell_ratio": 1.10,
            "min_tx_m5": 30,
            "max_negative_m5": -8.0,
            "max_top1_pct": 50,
            "max_top10_pct": 75,
            "min_liquidity_usd": 5000,
        },
    }

    decision = _ath_no_kline_scout_quality_soft_override(
        _ath_no_kline_pending(),
        scout_quality,
        route="ATH",
        scout_size={"actual_size_sol": PAPER_TINY_SCOUT_SIZE_SOL},
    )

    assert decision["pass"] is False
    assert decision["reason"] == "scout_quality_volume_low"
    assert decision["volume_low_soft_override"]["checks"]["tx_ok"] is False


def test_ath_recovery_mode_mapping_does_not_pollute_no_kline():
    mode = _ath_recovery_mode_for_candidate(
        ATH_NO_KLINE_TINY_PROBE_MODE,
        route="ATH",
        source_detail={"scout_quality": {"reason": "scout_quality_recent_token_failure"}},
    )

    assert mode == ATH_NO_KLINE_TINY_PROBE_MODE


def test_ath_uncertainty_mc_shadow_tracks_micro_reclaim_watch():
    mode = _ath_recovery_mode_for_candidate(
        ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        route="ATH",
        source_detail={
            "ath_uncertainty_reject_reason": "ath_uncertainty_mc_shadow_only",
            "scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100},
        },
    )

    assert mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE


def test_ath_uncertainty_soft_quality_tracks_micro_reclaim_even_when_initial_matrix_is_weak():
    mode = _ath_recovery_mode_for_candidate(
        ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        route="ATH",
        source_reject_reason="matrices not yet aligned",
        source_detail={
            "ath_uncertainty_reject_reason": "scout_quality_negative_trend",
            "scores": {"trend": 20, "volume": 25, "price": 40, "signal": 35, "momentum": 10},
        },
    )

    assert mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE


def test_ath_uncertainty_matrix_dissonance_tracks_watch_even_when_initial_matrix_is_weak():
    mode = _ath_recovery_mode_for_candidate(
        ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        route="ATH",
        source_reject_reason="matrices not yet aligned",
        source_detail={
            "scores": {"trend": 20, "volume": 25, "price": 40, "signal": 35, "momentum": 10},
        },
    )

    assert mode == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE


def test_ath_reclaim_after_failure_passes_only_after_reclaim():
    db = _paper_trade_db([
        {
            "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
            "exit_reason": "timeout",
            "pnl_pct": -0.10,
            "peak_pnl": 0.04,
            "exit_price": 1.0,
        },
    ])
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_detail": {
            "scout_quality": {"reason": "scout_quality_recent_token_failure"},
            "scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 60},
        },
    }

    decision = _ath_recovery_eligibility(
        db,
        entry_mode=ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": True, "cooldown_expired": True},
        dex_snapshot={"price": 1.13},
        activity={"buy_sell_ratio": 1.40, "tx_m5": 60, "price_change_m5": 13.0},
        liquidity_usd=12000,
        top10_pct=20,
        quote_probe={"success": True, "reason": "quote_executable"},
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_reclaim_after_failure_pass"
    assert decision["family"] == "recent_failure_reclaim"


def test_ath_reclaim_after_failure_does_not_blanket_block_moderate_prior_hard_sl():
    db = _paper_trade_db([
        {
            "entry_mode": "momentum_direct_entry",
            "exit_reason": "hard_sl",
            "pnl_pct": -0.18,
            "peak_pnl": 0.08,
            "exit_price": 1.0,
        },
    ])
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_detail": {
            "scout_quality": {"reason": "scout_quality_recent_token_failure"},
            "scores": {"trend": 55, "volume": 70, "price": 100, "signal": 100},
        },
    }

    decision = _ath_recovery_eligibility(
        db,
        entry_mode=ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": True, "cooldown_expired": True},
        dex_snapshot={"price": 1.14},
        activity={"buy_sell_ratio": 1.40, "tx_m5": 60, "price_change_m5": 14.0},
        liquidity_usd=12000,
        top10_pct=20,
        quote_probe={"success": True, "reason": "quote_executable"},
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_reclaim_after_failure_pass"


def test_ath_reclaim_after_failure_blocks_recent_hard_sl():
    db = _paper_trade_db([
        {
            "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
            "exit_reason": "hard_sl",
            "pnl_pct": -0.31,
            "peak_pnl": 0.01,
            "exit_price": 1.0,
        },
    ])
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_detail": {
            "scout_quality": {"reason": "scout_quality_recent_token_failure"},
            "scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100},
        },
    }

    decision = _ath_recovery_eligibility(
        db,
        entry_mode=ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": True, "cooldown_expired": True},
        dex_snapshot={"price": 1.20},
        activity={"buy_sell_ratio": 1.40, "tx_m5": 120, "price_change_m5": 20.0},
        liquidity_usd=12000,
        top10_pct=20,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_recovery_recent_hard_loss"


def test_ath_matrix_dissonance_requires_quote_executable():
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_reject_reason": "matrices not yet aligned",
        "source_detail": {"scores": {"trend": 40, "volume": 70, "price": 100, "signal": 80}},
    }

    decision = _ath_recovery_eligibility(
        _paper_trade_db(),
        entry_mode=ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": False},
        activity={"buy_sell_ratio": 1.25, "tx_m5": 100, "price_change_m5": 2.0},
        liquidity_usd=12000,
        top10_pct=20,
        quote_probe={"success": False, "reason": "no_route"},
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_matrix_dissonance_quote_not_executable"


def test_ath_matrix_dissonance_waits_for_live_price_confirmation():
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_reject_reason": "matrices not yet aligned",
        "source_detail": {"scores": {"trend": 40, "volume": 70, "price": 100, "signal": 80}},
    }

    decision = _ath_recovery_eligibility(
        _paper_trade_db(),
        entry_mode=ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": False},
        activity={"buy_sell_ratio": 1.25, "tx_m5": 100, "price_change_m5": -0.1},
        liquidity_usd=12000,
        top10_pct=20,
        quote_probe={"success": True, "reason": "quote_executable"},
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_matrix_dissonance_not_live_confirmed"


def test_ath_micro_reclaim_blocks_weak_buy_pressure():
    candidate = {
        "token_ca": "TokenCA",
        "route": "ATH",
        "source_detail": {
            "scout_quality": {"reason": "scout_quality_negative_trend"},
            "scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100},
        },
    }

    decision = _ath_recovery_eligibility(
        _paper_trade_db(),
        entry_mode=ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": False},
        current_reclaim={"bounce_from_low_pct": 7.0},
        activity={"buy_sell_ratio": 1.05, "tx_m5": 100, "price_change_m5": 1.0},
        liquidity_usd=12000,
        top10_pct=20,
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "ath_micro_reclaim_buy_pressure_weak"


def test_dynamic_ath_ttl_only_extends_strong_quote_executable_candidate():
    strong = _ath_dynamic_ttl_extension_detail(
        {
            "route": "ATH",
            "mode": ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
            "source_detail": {"scores": {"trend": 40, "price": 100, "signal": 80}},
        },
        activity={"buy_sell_ratio": 1.25, "price_change_m5": 1.0},
        quote_probe={"success": True},
    )
    weak = _ath_dynamic_ttl_extension_detail(
        {
            "route": "ATH",
            "mode": ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
            "source_detail": {"scores": {"trend": 40, "price": 100, "signal": 80}},
            "last_wait_reason": "scout_quality_negative_trend",
        },
        activity={"buy_sell_ratio": 1.25, "price_change_m5": 1.0},
        quote_probe={"success": True},
    )

    assert strong["pass"] is True
    assert strong["reason"] == "ath_tracking_ttl_extended"
    assert weak["pass"] is False
    assert weak["reason"] == "ath_dynamic_ttl_recent_quality_weak"


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
