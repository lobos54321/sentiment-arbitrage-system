import json
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
    HARD_GATE_PASS_TINY_PROBE_MODE,
    LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
    LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
    MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
    PAPER_TINY_SCOUT_SIZE_SOL,
    PRE_PASS_RESONANCE_TINY_PROBE_MODE,
    PRIMARY_PROVING_CAP_SIZE_SOL,
    QUOTE_GUARD_POLICY_VERSION,
    REVIVAL_CANARY_POLICY_VERSION,
    SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL,
    SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL,
    SOURCE_RESONANCE_TINY_PROBE_MODE,
    _apply_hard_gate_pass_probe_to_pending,
    _apply_actual_tiny_trigger_mode,
    _apply_ath_soft_reject_canary_to_pending,
    _apply_primary_proving_cap,
    _apply_source_resonance_probe_to_pending,
    _ath_soft_reject_canary_detail,
    _ath_reentry_block_cooldown_sec,
    _ath_no_kline_followthrough_guard,
    _ath_no_kline_reentry_guard,
    _ath_no_kline_scout_quality_soft_override,
    _hard_gate_pass_probe_scout_quality_soft_override,
    _hard_gate_pass_probe_entry_mode_quality_soft_override,
    _dog_catcher_branch_scout_quality_soft_override,
    _dog_catcher_branch_entry_mode_quality_override,
    _dog_catcher_quote_anchor_detail,
    _dog_catcher_trail_quote_confirmation,
    _ath_dynamic_ttl_extension_detail,
    _ath_recovery_eligibility,
    _ath_recovery_mode_for_candidate,
    _defer_ath_reentry_block,
    _discovery_hard_block,
    _discovery_mode_for_lotto_reason,
    _lotto_dynamic_ttl_extension_detail,
    _lotto_recovery_activity_gate,
    _lotto_recovery_mode_for_blocker,
    _lotto_not_ath_watch_shadow_decision,
    _retarget_discovery_candidate,
    _build_discovery_pending,
    _entry_mode_for_ath_uncertainty_reason,
    _entry_mode_quality_high_quality_tiny_override,
    _entry_mode_quality_allows_live,
    _exit_stop_quote_gap_protection,
    _late_smart_pullback_abort_detail,
    _phase_policy_live_exit_detail,
    _probe_hold_quote_monitor_exit_detail,
    _probe_quote_primary_profit_exit_confirmation,
    dog_catcher_late_entry_guard_detail,
    dog_catcher_fast_lane_pending_ready,
    _matrix_micro_momentum_reason,
    _maybe_upgrade_pending_to_source_resonance_probe,
    _pending_entry_exists_for_token,
    _pending_watchlist_fire_block_detail,
    _select_structure_stop_loss,
    _ttl_final_reclaim_quote_override_detail,
    _post_exit_runner_watch_detail,
    _post_exit_reclaim_entry_mode_force_live,
    _watchlist_hard_loss_reentry_bypass_detail,
    arm_ath_uncertainty_tiny_scout,
    arm_hard_gate_pass_tiny_probe,
    arm_pre_pass_resonance_tiny_probe,
    apply_revival_canary_to_pending,
    build_paper_observation_probe_synthetic_exit_execution,
    build_lifecycle_id,
    build_paper_tiny_scout_dex_fallback_entry_execution,
    evaluate_entry_edge_budget,
    evaluate_hard_gate_pass_tiny_probe,
    evaluate_pre_pass_resonance_tiny_probe,
    evaluate_revival_canary_gate,
    evaluate_source_resonance_tiny_probe,
    position_is_observation_probe,
    _update_candidate_quote_confirmation,
    record_trade_path_sample,
    record_lotto_not_ath_watch_shadow_candidates,
    track_discovery_candidate,
    track_post_exit_runner_candidate,
)


def _gmgn_resonance_context(cohort="telegram_gmgn", lead=180, quote_clean=False):
    return {
        "resonance_cohort": "telegram_gmgn_quote_clean" if quote_clean else cohort,
        "cohort_priority": 100 if quote_clean else 80,
        "gmgn_pre_seen": True,
        "gmgn_pre_seen_raw": True,
        "gmgn_lead_time_sec": lead,
        "lead_time_sec": lead,
        "timestamp_valid": True,
        "timestamp_anomaly_reason": None,
        "last_seen_age_sec": 30,
        "quote_clean_seen": quote_clean,
        "external_alpha_available": True,
        "external_alpha": {
            "available": True,
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": lead,
            "last_seen_age_sec": 30,
            "timestamp_valid": True,
            "quote_clean_seen": quote_clean,
        },
    }


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


class _DummyPos:
    def __init__(self, *, entry_mode="lotto_micro_reclaim_tiny_probe", size_sol=0.003, exit_rules=None, route="LOTTO"):
        self.monitor_state = {"entryMode": entry_mode, "entrySol": size_sol, "signalRoute": route}
        self.entry_mode = entry_mode
        self.position_size_sol = size_sol
        self.exit_rules = exit_rules or {"stopLossPct": 18}
        self.token_ca = "TokenCA"
        self.symbol = "DUMMY"
        self.trade_id = 42
        self.lifecycle_id = "life-parent"
        self.signal_ts = 1_778_220_000
        self.premium_signal_id = None
        self.strategy_stage = "paper"
        self.signal_type = route
        self.pool_address = "PoolCA"
        self.peak_pnl = 0.0


def _ath_no_kline_pending():
    return {
        "token_ca": "TokenCA",
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 60},
    }


def test_pending_entry_exists_for_token_matches_different_lifecycle():
    pending_entries = {
        "TokenCA:100": {"token_ca": "TokenCA"},
        "OtherCA:101": {"token_ca": "OtherCA"},
    }

    assert _pending_entry_exists_for_token(pending_entries, "TokenCA") is True
    assert _pending_entry_exists_for_token(pending_entries, "MissingCA") is False


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
    assert _discovery_mode_for_lotto_reason("ema_extreme") == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
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


def _insert_not_ath_missed(
    db,
    *,
    token_ca="TokenCA",
    symbol="DOG",
    signal_ts=1000,
    created_event_ts=1000,
    pnl_5m=0.07,
    pnl_15m=0.12,
    pnl_60m=0.30,
    max_pnl_recorded=0.30,
):
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution (
            decision_event_id, created_event_ts, token_ca, symbol, lifecycle_id,
            signal_id, signal_ts, route, component, decision, reject_reason,
            baseline_price, baseline_source, baseline_ts, pnl_5m, pnl_15m,
            pnl_60m, max_pnl_recorded, tradable_missed, tradability_status,
            tradability_reason, first_tradable_pnl, tradable_peak_pnl,
            would_stop_before_peak
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            None,
            created_event_ts,
            token_ca,
            symbol,
            f"{token_ca}:{signal_ts}",
            123,
            signal_ts,
            "LOTTO",
            "upstream_gate",
            "skip",
            "not_ath_v17",
            1.0,
            "test",
            signal_ts,
            pnl_5m,
            pnl_15m,
            pnl_60m,
            max_pnl_recorded,
            1,
            "tradable_reclaim",
            "test",
            pnl_15m,
            max_pnl_recorded,
            0,
        ),
    )
    db.commit()


def _patch_not_ath_shadow_snapshot_sources(
    monkeypatch,
    *,
    quote_gap_pct=0.0,
    liquidity_usd=12000.0,
    vol_m5=8000.0,
    buys_m5=90,
    sells_m5=20,
    price_change_m5=3.0,
    quote_success=True,
):
    price_usd = 0.001
    sol_usd = 100.0
    mark_price = price_usd / sol_usd
    quote_price = mark_price * (1.0 + quote_gap_pct / 100.0)

    monkeypatch.setattr(monitor, "get_sol_price", lambda: sol_usd)
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {
            "price_usd": price_usd,
            "liquidity_usd": liquidity_usd,
            "vol_m5": vol_m5,
            "buys_m5": buys_m5,
            "sells_m5": sells_m5,
            "price_change_m5": price_change_m5,
            "dex_id": "raydium",
            "pair_address": "PairA",
        },
    )
    monkeypatch.setattr(
        monitor,
        "simulate_entry_execution",
        lambda *_args, **_kwargs: {
            "success": quote_success,
            "effectivePrice": quote_price if quote_success else None,
            "failureReason": None if quote_success else "quote_not_executable",
            "quotedOutAmountRaw": "1000" if quote_success else None,
            "routeAvailable": quote_success,
        },
    )


def test_lotto_not_ath_watch_shadow_confirms_without_live_pending(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    _insert_not_ath_missed(db)
    _patch_not_ath_shadow_snapshot_sources(monkeypatch)

    recorded = record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000, limit=10)

    assert recorded == 2
    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1300, limit=10) == 1
    events = db.execute(
        """
        SELECT event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE component = 'lotto_not_ath_watch_shadow'
        ORDER BY id
        """
    ).fetchall()
    assert [row["event_type"] for row in events] == ["watch_opened", "watch_wait", "would_enter"]
    assert events[2]["decision"] == "WOULD_ENTER"
    assert events[2]["reason"] == "not_ath_two_snapshot_quote_clean_reclaim_confirmed"
    payload = json.loads(events[2]["payload_json"])
    assert payload["live_entry_enabled"] is False
    assert payload["watch_ledger_mvp"]["parent_blocker"] == "not_ath_v17"
    assert payload["suggested_entry_mode"] == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    assert payload["confirmation"]["quote_clean_samples"] == 2
    assert payload["confirmation"]["snapshot_pass_samples"] == 2
    assert payload["historical_proxy_confirmation"]["reason"] == "not_ath_two_horizon_reclaim_confirmed"

    snapshots = db.execute("SELECT horizon_sec, quote_clean, snapshot_pass FROM lotto_not_ath_watch_shadow_snapshots ORDER BY id").fetchall()
    assert [row["horizon_sec"] for row in snapshots] == [0, 300]
    assert all(row["quote_clean"] == 1 and row["snapshot_pass"] == 1 for row in snapshots)

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1360, limit=10) == 0


def test_lotto_not_ath_watch_historical_proxy_rejects_weak_first_reclaim():
    detail = _lotto_not_ath_watch_shadow_decision(
        {"created_event_ts": 1000, "pnl_5m": 0.02, "pnl_15m": 0.16},
        now_ts=1300,
    )

    assert detail == {
        "age_sec": 300.0,
        "pnl_5m": 0.02,
        "pnl_15m": 0.16,
        "max_recovery_pnl": 0.16,
        "min_5m_pnl": 0.05,
        "min_15m_pnl": 0.10,
        "min_retention": 0.50,
        "confirm_by_sec": 1800,
        "event_type": "watch_rejected",
        "decision": "SHADOW_REJECT",
        "reason": "not_ath_watch_5m_reclaim_weak",
    }


def test_lotto_not_ath_watch_shadow_wait_is_throttled(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    _insert_not_ath_missed(db, created_event_ts=1200, pnl_5m=0.08, pnl_15m=None)
    _patch_not_ath_shadow_snapshot_sources(monkeypatch)

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1300, limit=10) == 2
    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1360, limit=10) == 0
    events = db.execute(
        """
        SELECT event_type
        FROM paper_decision_events
        WHERE component = 'lotto_not_ath_watch_shadow'
        ORDER BY id
        """
    ).fetchall()
    assert [row["event_type"] for row in events] == ["watch_opened", "watch_wait"]
    snapshots = db.execute("SELECT COUNT(*) AS n FROM lotto_not_ath_watch_shadow_snapshots").fetchone()
    assert snapshots["n"] == 1


def test_lotto_not_ath_watch_shadow_expires_without_two_snapshot_confirmation(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    _insert_not_ath_missed(db, created_event_ts=1000, pnl_5m=None, pnl_15m=None)
    _patch_not_ath_shadow_snapshot_sources(
        monkeypatch,
        quote_success=False,
        liquidity_usd=1000.0,
        vol_m5=500.0,
        buys_m5=5,
        sells_m5=20,
        price_change_m5=-3.0,
    )

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000 + 31 * 60, limit=10) == 2
    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000 + 32 * 60, limit=10) == 0

    _insert_not_ath_missed(
        db,
        created_event_ts=1000,
        pnl_5m=0.08,
        pnl_15m=0.14,
        pnl_60m=0.30,
        max_pnl_recorded=0.30,
    )

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000 + 33 * 60, limit=10) == 0
    events = db.execute(
        """
        SELECT event_type, decision, reason
        FROM paper_decision_events
        WHERE component = 'lotto_not_ath_watch_shadow'
        ORDER BY id
        """
    ).fetchall()
    assert [row["event_type"] for row in events] == [
        "watch_opened",
        "watch_expired",
    ]
    assert events[-1]["decision"] == "SHADOW_EXPIRE"
    assert events[-1]["reason"] == "not_ath_watch_missing_two_quote_clean_reclaim_snapshots"


def test_lotto_not_ath_watch_shadow_keeps_relaxed_snapshots_after_strict_expire(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    _insert_not_ath_missed(db, created_event_ts=1000, pnl_5m=None, pnl_15m=None)
    _patch_not_ath_shadow_snapshot_sources(
        monkeypatch,
        quote_success=False,
        liquidity_usd=1000.0,
        vol_m5=500.0,
        buys_m5=5,
        sells_m5=20,
        price_change_m5=-3.0,
    )

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000 + 31 * 60, limit=10) == 2
    first = db.execute(
        """
        SELECT horizon_sec, quote_clean, snapshot_pass
        FROM lotto_not_ath_watch_shadow_snapshots
        ORDER BY id
        """
    ).fetchall()
    assert [(row["horizon_sec"], row["quote_clean"], row["snapshot_pass"]) for row in first] == [(1800, 0, 0)]

    _patch_not_ath_shadow_snapshot_sources(monkeypatch)

    assert record_lotto_not_ath_watch_shadow_candidates(db, now_ts=1000 + 37 * 60, limit=10) == 0
    snapshots = db.execute(
        """
        SELECT horizon_sec, quote_clean, snapshot_pass
        FROM lotto_not_ath_watch_shadow_snapshots
        ORDER BY id
        """
    ).fetchall()
    assert [(row["horizon_sec"], row["quote_clean"], row["snapshot_pass"]) for row in snapshots] == [
        (1800, 0, 0),
        (2100, 1, 1),
    ]
    terminal_events = db.execute(
        """
        SELECT event_type
        FROM paper_decision_events
        WHERE component = 'lotto_not_ath_watch_shadow'
        ORDER BY id
        """
    ).fetchall()
    assert [row["event_type"] for row in terminal_events] == ["watch_opened", "watch_expired"]


def test_lotto_not_ath_watch_shadow_decision_expires_missing_samples():
    detail = _lotto_not_ath_watch_shadow_decision(
        {"created_event_ts": 1000, "pnl_5m": 0.08, "pnl_15m": None},
        now_ts=1000 + 31 * 60,
    )

    assert detail["event_type"] == "watch_expired"
    assert detail["decision"] == "SHADOW_EXPIRE"


def test_smart_entry_reject_tracks_lotto_micro_recovery_candidate():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_route": "LOTTO",
        "is_lotto": True,
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "not_ath_v17",
        "pool": "PoolA",
    }

    tracked = monitor._track_smart_entry_reject_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="TokenCA:1000",
        timing_reason="weak_buying_pressure",
        timing_detail="bs=0.92 < 1.05",
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["route"] == "LOTTO"
    assert candidate["source_component"] == "smart_entry"
    assert candidate["source_reject_reason"] == "weak_buying_pressure"


def test_smart_entry_reject_routes_not_ath_to_lotto_recovery_tracking():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_route": "NOT_ATH",
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "not_ath_v17",
        "pool": "PoolA",
    }

    tracked = monitor._track_smart_entry_reject_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="TokenCA:1000",
        timing_reason="trend_bearish_timeout",
        timing_detail="phase bearish timeout",
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    assert candidate["route"] == "LOTTO"
    assert candidate["source_component"] == "smart_entry"
    assert candidate["source_reject_reason"] == "trend_bearish_timeout"


def test_smart_entry_reject_tracks_ath_micro_recovery_candidate():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "AthToken",
        "symbol": "ATHDOG",
        "signal_ts": 1000,
        "signal_route": "ATH",
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "matrices not yet aligned",
        "pool": "PoolA",
    }

    tracked = monitor._track_smart_entry_reject_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="AthToken:1000",
        timing_reason="chasing_top",
        timing_detail="near local high",
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == ATH_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["route"] == "ATH"
    assert candidate["source_component"] == "smart_entry"
    assert candidate["source_reject_reason"] == "chasing_top"


def test_smart_entry_ema_extreme_tracks_ath_micro_recovery_candidate():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "AthToken",
        "symbol": "ATHDOG",
        "signal_ts": 1000,
        "signal_route": "ATH",
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "matrices not yet aligned",
        "pool": "PoolA",
    }

    tracked = monitor._track_smart_entry_reject_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="AthToken:1000",
        timing_reason="ema_extreme",
        timing_detail="ema deviation too high",
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == ATH_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["source_component"] == "smart_entry"
    assert candidate["source_reject_reason"] == "ema_extreme"


def test_ath_micro_reclaim_allows_dead_cat_smart_entry_source_after_live_reclaim():
    candidate = {
        "token_ca": "AthToken",
        "route": "ATH",
        "source_reject_reason": "dead_cat_below_high_23.7pct_gt_15.0pct",
        "source_detail": {
            "scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100},
        },
    }

    decision = _ath_recovery_eligibility(
        _paper_trade_db(),
        entry_mode=ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate=candidate,
        route="ATH",
        token_risk={"blocked": False},
        current_reclaim={"bounce_from_low_pct": 8.0},
        activity={"buy_sell_ratio": 1.40, "tx_m5": 120, "price_change_m5": 4.0},
        liquidity_usd=12000,
        top10_pct=20,
        quote_probe={"success": True, "reason": "quote_executable"},
        now_ts=1_778_223_600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "ath_micro_reclaim_probe_pass"


def test_entry_contract_reject_tracks_ath_micro_recovery_candidate():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "AthToken",
        "symbol": "ATHDOG",
        "signal_ts": 1000,
        "signal_route": "ATH",
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "matrices not yet aligned",
        "pool": "PoolA",
    }
    contract = {
        "decision": "reject",
        "reason": "odds_after_cost_below_policy",
        "entry_mode": "smart_entry_pullback_bounce",
    }

    tracked = monitor._track_entry_contract_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="AthToken:1000",
        entry_decision_contract=contract,
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == ATH_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["route"] == "ATH"
    assert candidate["source_component"] == "entry_decision_contract"
    assert candidate["source_reject_reason"] == "odds_after_cost_below_policy"


def test_entry_contract_reject_tracks_lotto_micro_recovery_candidate():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pending = {
        "token_ca": "LottoToken",
        "symbol": "LDOG",
        "signal_ts": 1000,
        "signal_route": "LOTTO",
        "is_lotto": True,
        "entry_mode": "smart_entry_pullback_bounce",
        "source_reject_reason": "not_ath_v17",
        "pool": "PoolA",
    }
    contract = {
        "decision": "reject",
        "reason": "odds_after_cost_below_policy",
        "entry_mode": "smart_entry_pullback_bounce",
    }

    tracked = monitor._track_entry_contract_recovery_candidate(
        db,
        candidates,
        pending,
        {"id": 9, "pool_address": "PoolA"},
        lifecycle_id="LottoToken:1000",
        entry_decision_contract=contract,
        now_ts=1200,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["mode"] == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["route"] == "LOTTO"
    assert candidate["source_component"] == "entry_decision_contract"
    assert candidate["source_reject_reason"] == "odds_after_cost_below_policy"


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
        quote_probe={"success": True, "consecutive_successes": 2},
        current_mc=42000,
        liquidity_usd=12000,
        top1_pct=35,
        top10_pct=60,
        now_ts=1060,
    )
    assert strong["pass"] is True
    assert strong["reason"] == "lotto_not_ath_reclaim_live_reclaim_pass"


def test_lotto_micro_reclaim_expires_after_extended_watch():
    detail = _lotto_recovery_activity_gate(
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.4, "vol_m5": 9000, "tx_m5": 90, "price_change_m5": 9},
        quote_probe={"success": True, "consecutive_successes": 2},
        current_mc=52000,
        liquidity_usd=9000,
        top1_pct=30,
        top10_pct=65,
        now_ts=1000 + 31 * 60,
    )

    assert detail["pass"] is False
    assert "max_watch_sec_expired" in detail["failures"]


def test_lotto_micro_reclaim_strict_sources_need_stronger_followthrough():
    weak = _lotto_recovery_activity_gate(
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000, "source_reject_reason": "entry_mode_quality_shadow"},
        activity={"buy_sell_ratio": 1.25, "vol_m5": 5000, "tx_m5": 50, "price_change_m5": 6.5},
        quote_probe={"success": True},
        current_mc=52000,
        liquidity_usd=9000,
        top1_pct=30,
        top10_pct=65,
        now_ts=1060,
    )

    assert weak["pass"] is False
    assert "strict_buy_sell_ratio_low" in weak["failures"]
    assert "strict_tx_m5_low" in weak["failures"]

    strong = _lotto_recovery_activity_gate(
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000, "source_reject_reason": "entry_mode_quality_shadow"},
        activity={"buy_sell_ratio": 1.45, "vol_m5": 12000, "tx_m5": 95, "price_change_m5": 10},
        quote_probe={"success": True},
        current_mc=52000,
        liquidity_usd=9000,
        top1_pct=30,
        top10_pct=65,
        now_ts=1060,
    )

    assert strong["pass"] is True
    assert strong["strict_followthrough"]["reason"] == "strict_followthrough_pass"


def test_phase_policy_live_exit_allows_tiny_probe_no_follow_and_rug_defense():
    pos = _DummyPos(entry_mode=LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE)

    no_follow = _phase_policy_live_exit_detail(
        pos,
        {"shadow_action": "EXIT", "phase_state": "NO_FOLLOW", "reason": "no_follow_60s_shadow"},
        policy_route="ATH",
    )
    rug = _phase_policy_live_exit_detail(
        pos,
        {"shadow_action": "EXIT", "phase_state": "RUG_DEFENSE", "reason": "rug_risk_78"},
        policy_route="ATH",
    )

    assert no_follow["pass"] is True
    assert no_follow["live_reason"] == "phase_probe_no_follow_60s_shadow"
    assert rug["pass"] is True
    assert rug["live_reason"] == "phase_probe_rug_defense_exit (rug_risk_78)"


def test_phase_policy_live_exit_allows_lotto_primary_no_follow_decay():
    pos = _DummyPos(entry_mode="momentum_direct_entry", size_sol=0.01)

    detail = _phase_policy_live_exit_detail(
        pos,
        {"shadow_action": "EXIT", "phase_state": "NO_FOLLOW", "reason": "no_follow_decay_30s"},
        policy_route="LOTTO",
    )

    assert detail["pass"] is True
    assert detail["live_reason"] == "phase_no_follow_decay_30s"


def test_lotto_proving_cap_position_is_observation_probe():
    pos = _DummyPos(entry_mode="momentum_direct_entry", size_sol=0.005, route="LOTTO")

    assert position_is_observation_probe(pos) is True


def test_post_exit_runner_watch_allows_lotto_stop_as_tiny_reclaim_watch():
    pos = _DummyPos(entry_mode="momentum_direct_entry", size_sol=0.01, route="LOTTO")
    pos.peak_pnl = 0.018

    detail = _post_exit_runner_watch_detail(
        pos,
        "lotto_sl (-20.3% <= -18.0%)",
        realized_pnl=-0.2095,
        trigger_pnl=-0.203,
        exit_quote_pnl=-0.2095,
    )

    assert detail["pass"] is True
    assert detail["mode"] == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert detail["source_reject_reason"] == "volatile_runner_watch"
    assert detail["watch_family"] == "volatile_runner"
    assert detail["ttl_sec"] >= 75 * 60


def test_post_exit_runner_watch_keeps_high_peak_as_runner_watch():
    pos = _DummyPos(entry_mode="momentum_direct_entry", size_sol=0.01, route="LOTTO")
    pos.peak_pnl = 0.25

    detail = _post_exit_runner_watch_detail(
        pos,
        "guardian_trail_stop (pnl=12.0% < floor=15.0%, peak=25.0%)",
        realized_pnl=0.12,
        trigger_pnl=0.12,
        exit_quote_pnl=0.12,
    )

    assert detail["pass"] is True
    assert detail["source_reject_reason"] == "post_exit_runner_watch"
    assert detail["watch_family"] == "post_exit_runner"
    assert detail["ttl_sec"] >= 90 * 60


def test_post_exit_runner_watch_rejects_toxic_rug_exit():
    pos = _DummyPos(entry_mode="lotto_micro_reclaim_tiny_probe", size_sol=0.003, route="LOTTO")

    detail = _post_exit_runner_watch_detail(
        pos,
        "phase_probe_rug_defense_exit (rug_risk_78)",
        realized_pnl=-0.09,
        trigger_pnl=-0.08,
        exit_quote_pnl=-0.09,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "toxic_exit_not_runner_watchable"


def test_track_post_exit_runner_candidate_uses_new_exit_lifecycle_and_ttl():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}
    pos = _DummyPos(entry_mode="momentum_direct_entry", size_sol=0.01, route="LOTTO")
    pos.peak_pnl = 0.25

    tracked, detail = track_post_exit_runner_candidate(
        db,
        candidates,
        pos=pos,
        reason="trail_stop_capture",
        realized_pnl=0.18,
        trigger_pnl=0.18,
        exit_quote_pnl=0.18,
        watchlist_entry={"id": 7, "pool_address": "PoolCA"},
        now_ts=1_778_221_800,
    )

    assert tracked is True
    assert detail["tracked"] is True
    assert len(candidates) == 1
    candidate = next(iter(candidates.values()))
    assert candidate["source_reject_reason"] == "post_exit_runner_watch"
    assert candidate["mode"] == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    assert candidate["signal_ts"] == 1_778_221_800
    assert candidate["expires_at"] >= 1_778_221_800 + 90 * 60


def test_post_exit_reclaim_force_live_requires_passed_recovery_and_quote():
    candidate = {
        "source_component": "post_exit_runner_watch",
        "source_reject_reason": "volatile_runner_watch",
    }
    detail = {
        "lotto_recovery_gate": {"pass": True, "reason": "lotto_micro_reclaim_ready"},
        "lotto_recovery_quote_probe": {"success": True},
    }

    decision = _post_exit_reclaim_entry_mode_force_live(
        candidate,
        detail,
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "post_exit_lotto_reclaim_quote_confirmed_force_live"


def test_post_exit_reclaim_force_live_rejects_missing_quote():
    candidate = {
        "source_component": "post_exit_runner_watch",
        "source_reject_reason": "volatile_runner_watch",
    }
    detail = {
        "lotto_recovery_gate": {"pass": True, "reason": "lotto_micro_reclaim_ready"},
        "lotto_recovery_quote_probe": {"success": False, "reason": "quote_not_executable"},
    }

    decision = _post_exit_reclaim_entry_mode_force_live(
        candidate,
        detail,
        LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "lotto_recovery_quote_not_executable"


def test_lotto_micro_reclaim_tracking_keeps_longer_reclaim_window():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    candidates = {}

    tracked = track_discovery_candidate(
        db,
        candidates,
        mode=LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
        route="LOTTO",
        token_ca="TokenCA",
        symbol="DUMMY",
        lifecycle_id="TokenCA:1000",
        signal_ts=1000,
        source_component="scout_quality",
        source_reject_reason="scout_quality_buy_pressure_weak",
        now_ts=2000,
    )

    assert tracked is True
    candidate = next(iter(candidates.values()))
    assert candidate["expires_at"] >= 2000 + 30 * 60


def test_low_liq_quote_confirmation_requires_two_consecutive_successes():
    candidate = {}
    first_probe = {"success": True, "effective_price": 1.0}
    first = _update_candidate_quote_confirmation(
        candidate,
        first_probe,
        key="low_liq_quote",
        min_successes=2,
        now_ts=1000,
    )
    second_probe = {"success": True, "effective_price": 1.01}
    second = _update_candidate_quote_confirmation(
        candidate,
        second_probe,
        key="low_liq_quote",
        min_successes=2,
        now_ts=1010,
    )

    assert first["pass"] is False
    assert first_probe["confirmed"] is False
    assert second["pass"] is True
    assert second_probe["confirmed"] is True
    assert candidate["low_liq_quote_success_count"] == 2


def test_lotto_low_liq_reclaim_gate_waits_for_quote_confirmation():
    weak = _lotto_recovery_activity_gate(
        LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.5, "vol_m5": 5000, "tx_m5": 100, "price_change_m5": 3},
        quote_probe={"success": True, "consecutive_successes": 1},
        current_mc=40000,
        liquidity_usd=2500,
        top1_pct=30,
        top10_pct=60,
        now_ts=1010,
    )
    strong = _lotto_recovery_activity_gate(
        LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        candidate={"first_seen_ts": 1000},
        activity={"buy_sell_ratio": 1.5, "vol_m5": 5000, "tx_m5": 100, "price_change_m5": 3},
        quote_probe={"success": True, "consecutive_successes": 2},
        current_mc=40000,
        liquidity_usd=2500,
        top1_pct=30,
        top10_pct=60,
        now_ts=1010,
    )

    assert weak["pass"] is False
    assert "low_liq_quote_confirmation_pending" in weak["failures"]
    assert strong["pass"] is True


def test_lotto_sl_quote_gap_protection_cancels_when_quote_above_stop_floor():
    detail = _exit_stop_quote_gap_protection(
        "lotto_sl (-19.0% <= -18.0%)",
        quote_pnl=-0.0827,
        trigger_pnl=-0.19,
        pos=_DummyPos(exit_rules={"stopLossPct": 18}),
    )

    assert detail["cancel"] is True
    assert detail["reason"] == "stop_quote_above_floor"
    assert round(detail["stop_threshold"], 2) == -0.18


def test_lotto_sl_quote_gap_protection_confirms_when_quote_breaches_stop_floor():
    detail = _exit_stop_quote_gap_protection(
        "lotto_sl (-19.0% <= -18.0%)",
        quote_pnl=-0.25,
        trigger_pnl=-0.19,
        pos=_DummyPos(exit_rules={"stopLossPct": 18}),
    )

    assert detail["cancel"] is False
    assert detail["reason"] == "quote_confirms_stop"


def test_probe_quote_primary_cancels_mark_profit_when_quote_cannot_confirm():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    pos.peak_pnl = 3.41
    exit_matrix = {
        "action": "exit",
        "reason": "guardian_ath_phase1_trail_25",
        "current_pnl": 0.305,
        "peak_pnl": 3.41,
    }

    detail = _probe_quote_primary_profit_exit_confirmation(
        pos,
        exit_matrix,
        quote_pnl=-0.5209,
        trigger_pnl=0.305,
    )

    assert detail["cancel"] is True
    assert detail["reason"] == "quote_primary_negative_quote_gap"
    assert detail["quote_mark_gap"] < -0.80


def test_probe_quote_primary_confirms_when_real_quote_keeps_profit_floor():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    exit_matrix = {
        "action": "exit",
        "reason": "hard_gate_baseline_peak50_floor",
        "current_pnl": 0.30,
        "trail_floor": 0.18,
    }

    detail = _probe_quote_primary_profit_exit_confirmation(
        pos,
        exit_matrix,
        quote_pnl=0.22,
        trigger_pnl=0.30,
    )

    assert detail["cancel"] is False
    assert detail["reason"] == "quote_primary_profit_confirmed"


def test_probe_quote_primary_cancels_small_mark_profit_with_negative_quote_gap():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    pos.peak_pnl = 0.052
    exit_matrix = {
        "action": "exit",
        "reason": "hard_gate_baseline_peak20_floor",
        "current_pnl": 0.014,
    }

    detail = _probe_quote_primary_profit_exit_confirmation(
        pos,
        exit_matrix,
        quote_pnl=-0.5685,
        trigger_pnl=0.014,
    )

    assert detail["cancel"] is True
    assert detail["reason"] == "quote_primary_negative_quote_gap"
    assert detail["quote_mark_gap"] < -0.50


def test_dog_catcher_trail_quote_cancels_when_quote_below_floor():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    exit_matrix = {
        "action": "exit",
        "reason": "dog_catcher_hard_gate_trail_floor (pnl=-25.6% <= floor=16.2%)",
        "current_pnl": -0.256,
        "trail_floor": 0.162,
    }

    detail = _dog_catcher_trail_quote_confirmation(
        pos,
        exit_matrix,
        quote_pnl=0.0038,
        trigger_pnl=-0.256,
    )

    assert detail["cancel"] is True
    assert detail["reason"] == "dog_catcher_trail_quote_not_confirmed"
    assert detail["required_quote_floor"] > 0.13


def test_dog_catcher_trail_quote_confirms_when_quote_holds_floor():
    pos = _DummyPos(entry_mode=SOURCE_RESONANCE_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    exit_matrix = {
        "action": "exit",
        "reason": "dog_catcher_resonance_trail_floor",
        "current_pnl": 0.10,
        "trail_floor": 0.15,
    }

    detail = _dog_catcher_trail_quote_confirmation(
        pos,
        exit_matrix,
        quote_pnl=0.135,
        trigger_pnl=0.10,
    )

    assert detail["cancel"] is False
    assert detail["reason"] == "dog_catcher_trail_quote_confirmed"


def test_dog_catcher_trail_quote_covers_guardian_floor_reason():
    pos = _DummyPos(entry_mode=SOURCE_RESONANCE_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    exit_matrix = {
        "action": "exit",
        "reason": "dog_catcher_guardian_trail_floor (pnl=-58.2% < floor=7.4%, peak=10.6%)",
        "current_pnl": -0.582,
        "trail_floor": 0.074,
    }

    detail = _dog_catcher_trail_quote_confirmation(
        pos,
        exit_matrix,
        quote_pnl=0.081,
        trigger_pnl=-0.582,
    )

    assert detail["cancel"] is False
    assert detail["reason"] == "dog_catcher_trail_quote_confirmed"


def test_late_smart_pullback_abort_blocks_chasing_stale_main_entry():
    detail = _late_smart_pullback_abort_detail(
        {
            "entry_mode": "smart_entry_pullback_bounce",
            "signal_ts": 1_000,
            "kelly_position_sol": 0.005,
        },
        now_ts=1_000 + monitor.SMART_PULLBACK_BOUNCE_MAX_SIGNAL_AGE_SEC + 1,
    )

    assert detail["abort"] is True
    assert detail["reason"] == "smart_pullback_signal_too_stale"


def test_probe_hold_quote_monitor_exits_when_quote_collapses():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    pos.peak_pnl = 0.12

    detail = _probe_hold_quote_monitor_exit_detail(
        pos,
        quote_pnl=-0.42,
        trigger_pnl=0.03,
        held_sec=90,
    )

    assert detail["exit"] is True
    assert detail["reason"] == "probe_quote_guard_stop"
    assert detail["quote_mark_gap_abs"] > monitor.PROBE_HOLD_QUOTE_GAP_STOP_PCT


def test_probe_hold_quote_monitor_locks_profit_when_quote_lags_mark():
    pos = _DummyPos(entry_mode=HARD_GATE_PASS_TINY_PROBE_MODE, size_sol=0.002, route="LOTTO")
    pos.peak_pnl = 0.14

    detail = _probe_hold_quote_monitor_exit_detail(
        pos,
        quote_pnl=0.05,
        trigger_pnl=0.14,
        held_sec=90,
    )

    assert detail["exit"] is True
    assert detail["reason"] == "probe_quote_profit_gap_lock"
    assert detail["quote_pnl"] > 0
    assert detail["quote_mark_gap_abs"] >= monitor.PROBE_HOLD_QUOTE_PROFIT_GAP_MIN_GAP


def test_dog_catcher_branch_quality_override_allows_soft_source_resonance():
    pending = {
        "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "entry_branch": "source_resonance_soft_override",
        "source_reject_reason": "scout_quality_buy_pressure_weak",
    }
    scout_quality = {
        "pass": False,
        "decision": "block",
        "reason": "scout_quality_buy_pressure_weak",
    }

    override = _dog_catcher_branch_scout_quality_soft_override(pending, scout_quality)

    assert override["pass"] is True
    assert override["decision"] == "warn"
    assert override["reason"] == "dog_catcher_soft_quality_warn"
    assert override["original_reason"] == "scout_quality_buy_pressure_weak"


def test_dog_catcher_branch_entry_mode_override_allows_ttl_rescue_canary():
    pending = {
        "entry_mode": LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "ttl_rescue_used": True,
        "source_reject_reason": "tracking_ttl_expired",
        "intervention_flags": ["discovery_tracking", "ttl_rescue"],
    }

    decision = _dog_catcher_branch_entry_mode_quality_override(pending)

    assert decision["pass"] is True
    assert decision["reason"] == "dog_catcher_branch_entry_mode_quality_override"


def test_dog_catcher_branch_entry_mode_override_allows_source_and_prepass_branches():
    for entry_mode, branch in [
        (SOURCE_RESONANCE_TINY_PROBE_MODE, "source_resonance_tiny_probe"),
        (PRE_PASS_RESONANCE_TINY_PROBE_MODE, "pre_pass_resonance"),
    ]:
        decision = _dog_catcher_branch_entry_mode_quality_override({
            "entry_mode": entry_mode,
            "paper_only_scout": True,
            "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
            "entry_branch": branch,
            "source_reject_reason": "matrices not yet aligned",
        })

        assert decision["pass"] is True
        assert decision["reason"] == "dog_catcher_branch_entry_mode_quality_override"


def test_ath_soft_reject_canary_converts_ath_weak_buying_pressure_to_tiny_probe():
    pending = {
        "token_ca": "TokenCA",
        "symbol": "ALGOAT",
        "signal_route": "ATH",
        "market_cap": 84000,
        "entry_mode": ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
    }

    detail = _ath_soft_reject_canary_detail(
        pending,
        "weak_buying_pressure",
        route="ATH",
        dex_snapshot={"market_cap": 84000},
    )
    _apply_ath_soft_reject_canary_to_pending(pending, detail)

    assert detail["pass"] is True
    assert detail["reason"] == "ath_soft_reject_canary"
    assert pending["entry_mode"] == ATH_UNCERTAINTY_TINY_SCOUT_MODE
    assert pending["ath_soft_reject_canary_used"] is True
    assert pending["entry_branch"] == "ath_soft_reject_canary"
    assert "ath_soft_reject_canary" in pending["intervention_flags"]


def test_dog_catcher_quote_anchor_allows_missing_trigger_for_retry_canary():
    pending = {
        "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "retry_watch_used": True,
    }

    detail = _dog_catcher_quote_anchor_detail(
        pending,
        trigger_price=None,
        quote_price=0.000001,
    )

    assert detail["pass"] is True
    assert detail["reason"] == "dog_catcher_quote_anchored_entry"


def test_build_lifecycle_id_normalizes_millisecond_signal_timestamps():
    assert build_lifecycle_id("TokenCA", 1_778_834_470_421) == "TokenCA:1778834470"
    assert build_lifecycle_id("TokenCA", 1_778_834_470) == "TokenCA:1778834470"


def test_ath_uncertainty_soft_quality_arms_dog_catcher_canary(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor, "ATH_UNCERTAINTY_TINY_SCOUT_ENABLED", True)
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {
            "market_cap": 48000,
            "fdv": 48000,
            "liquidity_usd": 25000,
            "price_usd": 0.000001,
            "price_change_m5": -4,
            "buys_m5": 4,
            "sells_m5": 10,
            "tx_m5": 14,
        },
    )
    monkeypatch.setattr(
        monitor,
        "evaluate_token_reclaim",
        lambda **_kwargs: {"status": "test_reclaim"},
    )
    monkeypatch.setattr(
        monitor,
        "token_quarantine_state",
        lambda *_args, **_kwargs: {"blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        monitor,
        "evaluate_scout_quality",
        lambda **_kwargs: {
            "pass": False,
            "decision": "block",
            "reason": "scout_quality_volume_low",
        },
    )
    monkeypatch.setattr(
        monitor,
        "_maybe_upgrade_pending_to_source_resonance_probe",
        lambda *_args, **_kwargs: {"pass": False, "reason": "test_disabled"},
    )

    pending_entries = {}
    w_entry = {
        "id": 7,
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "ATH",
        "pool_address": "Pool",
        "signal_ts": 1000,
        "premium_signal_id": 11,
        "signal_price": 0.000001,
        "signal_mc": 48000,
        "signal_top10": 20,
    }

    armed = arm_ath_uncertainty_tiny_scout(
        db,
        pending_entries,
        {},
        w_entry=w_entry,
        lifecycle_id="TokenCA:1000",
        eval_res={
            "action_reason": "matrices not yet aligned",
            "current_price": 0.000001,
            "scores": {"trend": 55, "volume": 30},
            "reasons": ["matrices not yet aligned"],
        },
        now_ts=1200,
        discovery_candidates={},
    )

    assert armed is True
    pending = pending_entries["TokenCA:1000"]
    assert pending["paper_only_scout"] is True
    assert pending["entry_branch"] == "ath_recovery_soft_quality_canary"
    assert pending["ath_recovery_soft_quality_override_used"] is True
    assert "soft_quality_canary" in pending["intervention_flags"]
    events = db.execute(
        "SELECT component, event_type, decision, reason FROM paper_decision_events ORDER BY id"
    ).fetchall()
    assert any(row["component"] == "ath_uncertainty_scout" and row["event_type"] == "pending_entry" for row in events)
    assert not any(row["component"] == "ath_recovery" and row["event_type"] == "candidate_block" for row in events)


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
        quote_probe={"success": True, "consecutive_successes": 2},
        require_quote=True,
        now_ts=1200,
    )
    assert final["pass"] is True
    assert final["reason"] == "lotto_tracking_ttl_extended"
    assert final["thresholds"]["max_extensions"] == 4


def test_entry_mode_quality_high_quality_tiny_override_allows_strong_ath():
    pending = {
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 70},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is True
    assert decision["reason"] == "entry_mode_quality_high_quality_tiny_override"


def test_entry_mode_quality_high_quality_tiny_override_blocks_no_kline_weak_momentum():
    pending = {
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 60, "volume": 70, "price": 100, "signal": 100, "momentum": 60},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "matrix_not_strong_enough"


def test_entry_mode_quality_high_quality_tiny_override_blocks_no_kline_zero_momentum_matrix():
    pending = {
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 40, "price": 100, "signal": 100, "momentum": 0},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "matrix_not_strong_enough"


def test_entry_mode_quality_high_quality_tiny_override_blocks_uncertainty_zero_momentum():
    pending = {
        "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 0},
    }

    decision = _entry_mode_quality_high_quality_tiny_override(pending, route="ATH")

    assert decision["pass"] is False
    assert decision["reason"] == "matrix_not_strong_enough"


def test_entry_mode_quality_high_quality_tiny_override_allows_uncertainty_with_live_activity():
    pending = {
        "entry_mode": ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
        "signal_type": "ATH",
        "matrix_scores": {"trend": 80, "volume": 70, "price": 100, "signal": 100, "momentum": 70},
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


def test_hard_loss_reentry_bypass_allows_strong_reclaim_above_last_entry():
    decision = _watchlist_hard_loss_reentry_bypass_detail(
        {"last_exit_price": 1.0},
        {
            "current_price": 1.03,
            "scores": {"trend": 80, "volume": 70, "price": 70, "signal": 100, "momentum": 100},
        },
    )

    assert decision["pass"] is True
    assert decision["reason"] == "hard_loss_reentry_strong_reclaim_bypass"


def test_hard_loss_reentry_bypass_rejects_weak_momentum_or_no_recovery():
    weak_momentum = _watchlist_hard_loss_reentry_bypass_detail(
        {"last_exit_price": 1.0},
        {
            "current_price": 1.10,
            "scores": {"trend": 80, "volume": 70, "price": 70, "signal": 100, "momentum": 60},
        },
    )
    below_entry = _watchlist_hard_loss_reentry_bypass_detail(
        {"last_exit_price": 1.0},
        {
            "current_price": 0.99,
            "scores": {"trend": 80, "volume": 70, "price": 70, "signal": 100, "momentum": 100},
        },
    )

    assert weak_momentum["pass"] is False
    assert weak_momentum["reason"] == "hard_loss_bypass_momentum_too_low"
    assert below_entry["pass"] is False
    assert below_entry["reason"] == "hard_loss_bypass_recovery_too_low"


def test_hard_loss_reentry_bypass_allows_flat_reclaim_after_large_recovery():
    decision = _watchlist_hard_loss_reentry_bypass_detail(
        {"last_exit_price": 1.0},
        {
            "current_price": 1.35,
            "scores": {"trend": 80, "volume": 40, "price": 30, "signal": 100, "momentum": 100},
        },
    )

    assert decision["pass"] is True
    assert decision["reason"] == "hard_loss_reentry_flat_reclaim_bypass"


def test_hard_loss_reentry_bypass_blocks_flat_reclaim_without_large_recovery():
    decision = _watchlist_hard_loss_reentry_bypass_detail(
        {"last_exit_price": 1.0},
        {
            "current_price": 1.10,
            "scores": {"trend": 80, "volume": 40, "price": 30, "signal": 100, "momentum": 100},
        },
    )

    assert decision["pass"] is False
    assert decision["reason"] == "hard_loss_bypass_volume_too_low"


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


def test_ath_tracking_ttl_expiry_maps_to_micro_reclaim_watch():
    assert monitor._ath_recovery_mode_for_reason("tracking_ttl_expired") == ATH_MICRO_RECLAIM_TINY_PROBE_MODE


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
    still_negative = _ath_dynamic_ttl_extension_detail(
        {
            "route": "ATH",
            "mode": ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
            "source_detail": {"scores": {"trend": 40, "price": 100, "signal": 80}},
            "last_wait_reason": "scout_quality_negative_trend",
        },
        activity={"buy_sell_ratio": 1.25, "price_change_m5": -1.0},
        quote_probe={"success": True},
    )

    assert strong["pass"] is True
    assert strong["reason"] == "ath_tracking_ttl_extended"
    assert strong["thresholds"]["max_extensions"] == 4
    assert weak["pass"] is True
    assert weak["reason"] == "ath_tracking_ttl_extended"
    assert weak["observed"]["last_wait_reason"] == "scout_quality_negative_trend"
    assert still_negative["pass"] is False
    assert still_negative["reason"] == "ath_tracking_ttl_not_strong"


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


def test_source_resonance_tiny_probe_requires_gmgn_lead_and_activity():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 180,
            "last_seen_age_sec": 30,
            "gmgn_momentum_rounds": 2,
            "gmgn_momentum_gain_pct": 4,
            "gmgn_momentum_confirmed": False,
            "gmgn_volume_confirmed": False,
            "last_market_cap": 42000,
        },
        route="LOTTO",
        hard_gate_status="NOT_ATH_V17",
        dex_snapshot={"vol_m5": 5000, "tx_m5": 50, "buy_sell_ratio": 1.1},
    )

    assert decision["pass"] is True
    assert decision["entry_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert decision["timing_passed"] is True


def test_source_resonance_tiny_probe_blocks_late_gmgn_seen():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": -20,
            "last_seen_age_sec": 30,
            "gmgn_momentum_rounds": 3,
            "gmgn_momentum_gain_pct": 10,
        },
        route="LOTTO",
        hard_gate_status="NOT_ATH_V17",
    )

    assert decision["pass"] is False
    assert decision["reason"] == "source_resonance_lead_time_too_short"


def test_source_resonance_tiny_probe_soft_override_allows_canary_on_soft_quality_noise():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 10,
            "last_seen_age_sec": 30,
            "gmgn_momentum_rounds": 0,
            "gmgn_momentum_gain_pct": 0,
            "gmgn_momentum_confirmed": False,
            "gmgn_volume_confirmed": False,
            "last_market_cap": 42000,
        },
        route="LOTTO",
        hard_gate_status="scout_quality_buy_pressure_weak",
        dex_snapshot={"vol_m5": 300, "tx_m5": 4, "buy_sell_ratio": 0.7},
    )

    assert decision["pass"] is True
    assert decision["reason"] == "source_resonance_soft_override"
    assert decision["entry_branch"] == "source_resonance_soft_override"
    assert decision["source_resonance_soft_override_used"] is True
    assert decision["observed"]["soft_override"]["parent_reason"] == "scout_quality_buy_pressure_weak"


def test_source_resonance_tiny_probe_rejects_stale_signal_anchor():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 180,
            "last_seen_age_sec": 30,
            "gmgn_momentum_rounds": 3,
            "gmgn_momentum_gain_pct": 10,
            "last_market_cap": 42000,
        },
        signal={"timestamp": 1_000, "signal_type": "LOTTO"},
        route="LOTTO",
        hard_gate_status="scout_quality_buy_pressure_weak",
        now_ts=1_000 + monitor.SOURCE_RESONANCE_TINY_PROBE_MAX_SIGNAL_AGE_SEC + 1,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "source_resonance_signal_too_stale"
    assert decision["observed"]["signal_age_sec"] > monitor.SOURCE_RESONANCE_TINY_PROBE_MAX_SIGNAL_AGE_SEC


def test_source_resonance_stale_lotto_canary_allows_quote_confirmed_stale_gate():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 600,
            "last_seen_age_sec": 30,
            "quote_clean": True,
            "quote_executable": True,
            "gmgn_momentum_rounds": 0,
            "gmgn_momentum_gain_pct": 0,
            "last_market_cap": 42000,
        },
        signal={"timestamp": 1_000, "signal_type": "LOTTO"},
        route="LOTTO",
        hard_gate_status="lotto_stale_2614s",
        now_ts=1_000 + monitor.SOURCE_RESONANCE_TINY_PROBE_MAX_SIGNAL_AGE_SEC + 600,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "source_resonance_soft_override"
    assert decision["source_resonance_stale_canary_used"] is True
    assert "source_resonance_stale_canary" in decision["intervention_flags"]


def test_source_resonance_risky_newborn_pullback_soft_override_allows_canary():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 90,
            "last_seen_age_sec": 30,
            "quote_clean": True,
            "quote_executable": True,
            "last_market_cap": 42000,
        },
        route="LOTTO",
        hard_gate_status="risky_newborn_pullback_m9s_zero",
    )

    assert decision["pass"] is True
    assert decision["reason"] == "source_resonance_soft_override"
    assert decision["observed"]["soft_override"]["parent_reason"] == "risky_newborn_pullback_m9s_zero"


def test_source_resonance_tiny_probe_rejects_timestamp_anomalies():
    decision = evaluate_source_resonance_tiny_probe(
        {
            "available": True,
            "source": "external_alpha_shadow",
            "gmgn_pre_seen": True,
            "gmgn_lead_time_sec": 1_776_857_007_527,
            "last_seen_age_sec": -48,
            "timestamp_valid": False,
            "timestamp_anomaly_reason": "gmgn_lead_time_unreasonable,external_alpha_future_seen",
            "gmgn_momentum_rounds": 3,
            "gmgn_momentum_gain_pct": 10,
        },
        route="LOTTO",
        hard_gate_status="NOT_ATH_V17",
    )

    assert decision["pass"] is False
    assert decision["reason"] == "gmgn_lead_time_unreasonable,external_alpha_future_seen"
    assert decision["observed"]["timestamp_valid"] is False


def test_apply_source_resonance_probe_to_pending_caps_size_and_marks_probe():
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_type": "ATH",
        "kelly_position_sol": 0.08,
        "trigger_price": 0.00001,
    }
    detail = {
        "pass": True,
        "reason": "source_resonance_telegram_gmgn_probe",
        "source_reject_reason": "no_kline_low_volume",
        "external_alpha": {"available": True},
        "timing_passed": True,
    }

    _apply_source_resonance_probe_to_pending(pending, detail, route="ATH")

    assert pending["entry_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert pending["scout_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert pending["paper_only_scout"] is True
    assert pending["execution_scope"] == "paper_only"
    assert pending["timing_passed"] is True
    assert pending["kelly_position_sol"] == monitor.SOURCE_RESONANCE_TINY_PROBE_SIZE_SOL
    assert pending["replay_source"] == "live_monitor_source_resonance_probe"


def test_hard_gate_pass_tiny_probe_allows_pass_quote_executable(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)

    decision = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        signal={"signal_type": "LOTTO", "market_cap": 48000},
        watchlist_entry={
            "type": "LOTTO",
            "signal_price": 0.000001,
            "signal_mc": 48000,
            "signal_top10": 32,
        },
        hard_gate_status="PASS",
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(quote_clean=True),
        now_ts=1000,
    )

    assert decision["pass"] is True
    assert decision["entry_mode"] == HARD_GATE_PASS_TINY_PROBE_MODE
    assert decision["paper_only_scout"] is True
    assert decision["execution_scope"] == "paper_only"
    assert decision["observed"]["quote_executable"] is True
    assert decision["resonance_cohort"] == "telegram_gmgn_quote_clean"
    assert decision["gmgn_pre_seen"] is True
    assert decision["observed"]["activity_confirmation"]["pass"] is True


def test_hard_gate_pass_tiny_probe_rejects_unconfirmed_activity(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_REQUIRE_ACTIVITY_CONFIRMATION", True)

    decision = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        signal={"signal_type": "LOTTO", "market_cap": 48000},
        watchlist_entry={
            "type": "LOTTO",
            "signal_price": 0.000001,
            "signal_mc": 48000,
            "signal_top10": 32,
        },
        hard_gate_status="PASS",
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(quote_clean=False),
        now_ts=1000,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "hard_gate_activity_not_confirmed"
    assert decision["observed"]["activity_confirmation"]["pass"] is False


def test_hard_gate_pass_tiny_probe_rejects_stale_signal(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_MAX_SIGNAL_AGE_SEC", 120)

    decision = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        signal={"signal_type": "LOTTO", "market_cap": 48000, "timestamp": 800},
        watchlist_entry={
            "type": "LOTTO",
            "signal_price": 0.000001,
            "signal_mc": 48000,
            "signal_top10": 32,
        },
        hard_gate_status="PASS",
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(quote_clean=True),
        now_ts=1000,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "hard_gate_signal_too_stale"
    assert decision["observed"]["signal_age_sec"] == 200


def test_dog_catcher_fast_lane_pending_ready_detects_timing_passed_probe():
    pending_entries = {
        "TokenCA:1000": {
            "token_ca": "TokenCA",
            "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
            "timing_passed": True,
        }
    }

    assert dog_catcher_fast_lane_pending_ready(pending_entries) is True


def test_hard_gate_pass_tiny_probe_requires_gmgn_pre_seen(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_REQUIRE_GMGN_PRE_SEEN", True)

    decision = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        signal={"signal_type": "ATH", "market_cap": 48000},
        watchlist_entry={"type": "ATH", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="PASS",
        resonance_context={
            **_gmgn_resonance_context(cohort="telegram_only"),
            "gmgn_pre_seen": False,
            "gmgn_pre_seen_raw": False,
            "resonance_cohort": "telegram_only",
            "cohort_priority": 10,
        },
        now_ts=1000,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "gmgn_pre_seen_required"


def test_hard_gate_pass_tiny_probe_requires_pass_and_quote(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)

    no_quote = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        watchlist_entry={"type": "ATH", "signal_mc": 48000, "signal_top10": 32},
        hard_gate_status="PASS",
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(quote_clean=True),
        now_ts=1000,
    )
    blocked_gate = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        watchlist_entry={"type": "ATH", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="NOT_ATH_V17",
        resonance_context=_gmgn_resonance_context(),
        now_ts=1000,
    )

    assert no_quote["pass"] is False
    assert no_quote["reason"] == "quote_not_executable"
    assert blocked_gate["pass"] is False
    assert blocked_gate["reason"] == "hard_gate_not_pass"


def test_hard_gate_pass_tiny_probe_dedupes_existing_token_probe(monkeypatch):
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)

    decision = evaluate_hard_gate_pass_tiny_probe(
        "TokenCA",
        signal={"signal_type": "ATH", "market_cap": 48000},
        watchlist_entry={"type": "ATH", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="PASS",
        pending_entries={
            "OtherLifecycle": {
                "token_ca": "TokenCA",
                "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
                "paper_only_scout": True,
                "execution_scope": "paper_only",
            }
        },
        resonance_context=_gmgn_resonance_context(quote_clean=True),
        now_ts=1000,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "probe_deduped_existing_mode"
    assert decision["observed"]["existing_probe"]["existing_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE


def test_pre_pass_resonance_tiny_probe_allows_upstream_gmgn_quote(monkeypatch):
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_cooldown", {})
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_REQUIRE_QUOTE_CLEAN", False)

    decision = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        signal={"signal_type": "NEW_TRENDING", "market_cap": 48000, "timestamp": 820},
        watchlist_entry={
            "type": "LOTTO",
            "signal_price": 0.000001,
            "signal_mc": 48000,
            "signal_top10": 32,
        },
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        dex_snapshot={"price_change_m5": 4.0, "buys_m5": 18, "sells_m5": 8, "tx_m5": 26},
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(lead=180),
        gmgn_policy={"action": "observe", "reason": "test"},
        now_ts=1000,
    )

    assert decision["pass"] is True
    assert decision["entry_mode"] == PRE_PASS_RESONANCE_TINY_PROBE_MODE
    assert decision["paper_only_scout"] is True
    assert decision["execution_scope"] == "paper_only"
    assert decision["source_reject_reason"] == "not_ath_prebuy_kline_block"
    assert decision["gmgn_pre_seen"] is True
    assert decision["followthrough"]["reason"] == "pre_pass_followthrough_confirmed"


def test_pre_pass_resonance_tiny_probe_relaxes_missing_followthrough_to_canary(monkeypatch):
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_cooldown", {})
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_REQUIRE_QUOTE_CLEAN", False)

    decision = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        signal={"signal_type": "NEW_TRENDING", "market_cap": 48000, "timestamp": 820},
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        dex_snapshot={"price_change_m5": 0.5, "buys_m5": 4, "sells_m5": 10, "tx_m5": 14},
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(lead=180, quote_clean=True),
        now_ts=1000,
    )

    assert decision["pass"] is True
    assert decision["reason"] == "pre_pass_relaxed_canary"
    assert decision["entry_branch"] == "pre_pass_relaxed_canary"
    assert decision["pre_pass_relaxed_used"] is True
    assert decision["position_size_sol"] == monitor.PRE_PASS_RELAXED_CANARY_SIZE_SOL
    assert decision["followthrough"]["pass"] is False
    assert decision["relaxed_canary"]["parent_reason"] == "pre_pass_followthrough_m5_too_low"
    assert decision["relaxed_canary"]["activity"]["quote_clean_seen"] is True


def test_pre_pass_relaxed_canary_requires_activity_confirmation(monkeypatch):
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_cooldown", {})
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_TINY_PROBE_ENABLED", True)

    decision = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        signal={"signal_type": "NEW_TRENDING", "market_cap": 48000, "timestamp": 820},
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        dex_snapshot={"price_change_m5": 0.5, "buys_m5": 4, "sells_m5": 10, "tx_m5": 14},
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(lead=180, quote_clean=False),
        now_ts=1000,
    )

    assert decision["pass"] is False
    assert decision["reason"] == "pre_pass_followthrough_m5_too_low"
    assert decision["relaxed_canary"]["reason"] == "pre_pass_relaxed_activity_not_confirmed"


def test_pre_pass_resonance_tiny_probe_keeps_pass_and_safety_owned_elsewhere(monkeypatch):
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_cooldown", {})
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_TINY_PROBE_ENABLED", True)

    pass_gate = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="PASS",
        resonance_context=_gmgn_resonance_context(),
        now_ts=1000,
    )
    reject_gate = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="GREYLIST",
        resonance_context=_gmgn_resonance_context(),
        now_ts=1000,
    )
    top_heavy = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        signal={"timestamp": 820},
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        dex_snapshot={"price_change_m5": 4, "buys_m5": 18, "sells_m5": 8, "tx_m5": 26},
        live_concentration={"top1_pct": 88, "top10_pct": 92},
        resonance_context=_gmgn_resonance_context(),
        now_ts=1000,
    )
    gmgn_reject = evaluate_pre_pass_resonance_tiny_probe(
        "TokenCA",
        signal={"timestamp": 820},
        watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        dex_snapshot={"price_change_m5": 4, "buys_m5": 18, "sells_m5": 8, "tx_m5": 26},
        live_concentration={"top1_pct": 18, "top10_pct": 32},
        resonance_context=_gmgn_resonance_context(),
        gmgn_policy={"action": "shadow_reject", "reason": "gmgn_bundler_too_high"},
        now_ts=1000,
    )

    assert pass_gate["pass"] is False
    assert pass_gate["reason"] == "pre_pass_resonance_pass_owned_by_hard_gate_baseline"
    assert reject_gate["pass"] is False
    assert reject_gate["reason"] == "pre_pass_resonance_reason_not_allowed"
    assert top_heavy["pass"] is False
    assert top_heavy["reason"] == "top1_too_high"
    assert gmgn_reject["pass"] is False
    assert gmgn_reject["reason"] == "gmgn_bundler_too_high"


def test_arm_pre_pass_resonance_tiny_probe_builds_paper_only_pending(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_pre_pass_resonance_probe_cooldown", {})
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "PRE_PASS_RESONANCE_REQUIRE_QUOTE_CLEAN", False)
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {"market_cap": 48000, "price_change_m5": 12, "buys_m5": 25, "sells_m5": 10, "tx_m5": 35},
    )
    monkeypatch.setattr(
        monitor,
        "helius_token_concentration",
        lambda _token_ca: {"top1_pct": 18, "top10_pct": 32},
    )
    monkeypatch.setattr(monitor, "fetch_gmgn_token_enrichment", lambda _token_ca: {})
    monkeypatch.setattr(
        monitor,
        "evaluate_gmgn_lotto_policy",
        lambda *_args, **_kwargs: {"action": "observe", "reason": "test"},
    )
    monkeypatch.setattr(
        monitor,
        "evaluate_scout_quality",
        lambda **_kwargs: {"pass": True, "reason": "scout_quality_pass"},
    )

    pending_entries = {}
    registered_entry = {
        "id": 7,
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "LOTTO",
        "pool_address": "Pool",
        "signal_ts": 1000,
        "premium_signal_id": 11,
        "signal_price": 0.000001,
        "signal_mc": 48000,
        "signal_top10": 32,
    }
    sig = {
        "id": 11,
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "timestamp": 1000,
        "signal_type": "NEW_TRENDING",
        "market_cap": 48000,
    }

    armed = arm_pre_pass_resonance_tiny_probe(
        db,
        pending_entries,
        {},
        sig=sig,
        registered_entry=registered_entry,
        pool="Pool",
        lifecycle_id="TokenCA:1000",
        signal_lifecycle={},
        signal_audit_payload={},
        hard_gate_status="NOT_ATH_PREBUY_KLINE_BLOCK",
        external_alpha=_gmgn_resonance_context()["external_alpha"],
        now_ts=1200,
    )

    assert armed is True
    pending = pending_entries["TokenCA:1000"]
    assert pending["entry_mode"] == PRE_PASS_RESONANCE_TINY_PROBE_MODE
    assert pending["paper_only_scout"] is True
    assert pending["execution_scope"] == "paper_only"
    assert pending["replay_source"] == "live_monitor_pre_pass_resonance_probe"
    assert pending["lotto_state"]["executionScope"] == "paper_only"
    event = db.execute(
        "SELECT component, event_type, reason FROM paper_decision_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event["component"] == "pre_pass_resonance_probe"
    assert event["event_type"] == "pending_entry"


def test_apply_hard_gate_pass_probe_to_pending_marks_paper_only():
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_type": "ATH",
        "kelly_position_sol": 0.08,
        "signal_price": 0.00001,
        "lotto_state": {},
    }
    detail = {
        "pass": True,
        "reason": "hard_gate_pass_baseline_probe",
        "timing_passed": True,
    }

    _apply_hard_gate_pass_probe_to_pending(pending, detail, route="ATH")

    assert pending["entry_mode"] == HARD_GATE_PASS_TINY_PROBE_MODE
    assert pending["paper_only_scout"] is True
    assert pending["execution_scope"] == "paper_only"
    assert pending["timing_passed"] is True
    assert pending["kelly_position_sol"] == monitor.HARD_GATE_PASS_TINY_PROBE_SIZE_SOL
    assert pending["replay_source"] == "live_monitor_hard_gate_pass_probe"
    assert pending["lotto_state"]["executionScope"] == "paper_only"


def test_hard_gate_pass_quote_retry_schedules_and_arms(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "_hard_gate_pass_quote_retry", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_QUOTE_RETRY_ENABLED", True)
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {"market_cap": 48000, "price_change_m5": 12},
    )
    monkeypatch.setattr(
        monitor,
        "helius_token_concentration",
        lambda _token_ca: {"top1_pct": 18, "top10_pct": 32},
    )
    monkeypatch.setattr(
        monitor,
        "safe_external_alpha_lookup",
        lambda *_args, **_kwargs: _gmgn_resonance_context()["external_alpha"],
    )

    registered_entry = {
        "id": 7,
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "ATH",
        "pool_address": "Pool",
        "signal_ts": 1000,
        "premium_signal_id": 11,
        "signal_price": None,
        "signal_mc": 48000,
        "signal_top10": 32,
    }
    sig = {
        "id": 11,
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "timestamp": 1000,
        "signal_type": "ATH",
        "market_cap": 48000,
    }
    pending_entries = {}

    armed = arm_hard_gate_pass_tiny_probe(
        db,
        pending_entries,
        {},
        sig=sig,
        registered_entry=registered_entry,
        pool="Pool",
        lifecycle_id="TokenCA:1000",
        signal_lifecycle={},
        signal_audit_payload={},
        hard_gate_status="PASS",
        now_ts=1200,
        route="ATH",
    )

    assert armed is False
    assert "TokenCA:1000" in monitor._hard_gate_pass_quote_retry
    first_event = db.execute(
        "SELECT event_type, decision, reason FROM paper_decision_events ORDER BY id LIMIT 1"
    ).fetchone()
    assert first_event["event_type"] == "quote_retry_scheduled"
    assert first_event["decision"] == "wait"

    monkeypatch.setattr(
        monitor,
        "fetch_realtime_price",
        lambda _token_ca, _pool, max_age_ms=15000: (0.000001, None, None),
    )

    retry_armed = monitor.process_hard_gate_pass_quote_retries(
        db,
        None,
        pending_entries,
        {},
        now_ts=1210,
        max_positions=10,
    )

    assert retry_armed == 1
    assert "TokenCA:1000" not in monitor._hard_gate_pass_quote_retry
    assert pending_entries["TokenCA:1000"]["entry_mode"] == HARD_GATE_PASS_TINY_PROBE_MODE
    assert pending_entries["TokenCA:1000"]["paper_only_scout"] is True
    assert pending_entries["TokenCA:1000"]["execution_scope"] == "paper_only"
    assert pending_entries["TokenCA:1000"]["resonance_cohort"] == "telegram_gmgn"
    assert pending_entries["TokenCA:1000"]["gmgn_pre_seen"] is True
    reasons = [
        row["reason"]
        for row in db.execute("SELECT reason FROM paper_decision_events ORDER BY id").fetchall()
    ]
    assert "quote_executable_after_retry" in reasons
    assert "hard_gate_pass_baseline_probe" in reasons


def test_hard_gate_pass_quote_retry_records_final_failure(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor, "_hard_gate_pass_quote_retry", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_QUOTE_RETRY_WINDOW_SEC", 60)
    monitor._hard_gate_pass_quote_retry["TokenCA:1000"] = {
        "sig": {"id": 11, "token_ca": "TokenCA", "symbol": "DOG", "timestamp": 1000},
        "registered_entry": {"id": 7, "type": "ATH"},
        "pool": "Pool",
        "lifecycle_id": "TokenCA:1000",
        "signal_lifecycle": {},
        "signal_audit_payload": {},
        "hard_gate_status": "PASS",
        "route": "ATH",
        "first_seen_ts": 1200,
        "next_attempt_ts": 1261,
        "attempts": 2,
    }

    retry_armed = monitor.process_hard_gate_pass_quote_retries(
        db,
        None,
        {},
        {},
        now_ts=1261,
        max_positions=10,
    )

    assert retry_armed == 0
    assert "TokenCA:1000" not in monitor._hard_gate_pass_quote_retry
    event = db.execute(
        "SELECT event_type, decision, reason FROM paper_decision_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event["event_type"] == "probe_reject"
    assert event["decision"] == "reject"
    assert event["reason"] == "quote_not_executable_after_retry"


def test_legacy_new_trending_statuses_are_observable_but_not_pass_probes():
    for status in {
        "NOT_ATH_V14",
        "NOT_ATH_V13",
        "NOT_ATH_V16",
        "INSUFFICIENT_KLINE",
        "NO_MC_DATA",
        "NOT_ATH_PREBUY_KLINE_BLOCK",
    }:
        assert status in monitor.LOTTO_OBSERVE_UPSTREAM_STATUSES
        assert monitor._is_paper_trade_signal({
            "signal_type": "NEW_TRENDING",
            "description": "New Trending token",
            "hard_gate_status": status,
        }) is True
        decision = evaluate_hard_gate_pass_tiny_probe(
            "TokenCA",
            watchlist_entry={"type": "LOTTO", "signal_price": 0.000001, "signal_mc": 48000},
            hard_gate_status=status,
            resonance_context=_gmgn_resonance_context(),
            now_ts=1000,
        )
        assert decision["pass"] is False
        assert decision["reason"] == "hard_gate_not_pass"


def test_normalize_pending_entry_adds_identity_without_changing_route():
    pending = {
        "token_ca": "TokenCA",
        "signal_type": "ATH",
        "paper_only_scout": True,
    }

    normalized = monitor.normalize_pending_entry(pending, "TokenCA:1000")

    assert normalized is pending
    assert pending["lifecycle_id"] == "TokenCA:1000"
    assert pending["is_lotto"] is False
    assert pending["execution_scope"] == "paper_only"


def test_normalize_pending_entry_backfills_defaults_and_watchlist_identity():
    pending = {
        "w_entry": {
            "ca": "TokenCA",
            "signal_ts": 1000,
            "premium_signal_id": 11,
            "type": "LOTTO",
        },
        "scout_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
        "paper_only_scout": True,
    }

    normalized = monitor.normalize_pending_entry(
        pending,
        "TokenCA:1000",
        defaults={"signal_type": "LOTTO"},
    )

    assert normalized["token_ca"] == "TokenCA"
    assert normalized["signal_ts"] == 1000
    assert normalized["premium_signal_id"] == 11
    assert normalized["signal_type"] == "LOTTO"
    assert normalized["signal_route"] == "LOTTO"
    assert normalized["entry_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert normalized["paper_only_scout"] is True
    assert normalized["execution_scope"] == "paper_only"
    assert normalized["is_lotto"] is True


def test_normalize_pending_entry_normalizes_millisecond_signal_ts():
    pending = {
        "w_entry": {
            "ca": "TokenCA",
            "signal_ts": 1778842042123,
            "type": "ATH",
        },
        "paper_only_scout": True,
    }

    normalized = monitor.normalize_pending_entry(pending, "TokenCA:1778842042")

    assert normalized["signal_ts"] == 1778842042
    assert normalized["w_entry"]["signal_ts"] == 1778842042


def _revival_canary_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT,
            entry_mode TEXT,
            entry_ts INTEGER,
            exit_ts INTEGER,
            exit_reason TEXT,
            loss_attribution TEXT,
            pnl_pct REAL,
            position_size_sol REAL,
            monitor_state_json TEXT,
            entry_execution_audit_json TEXT,
            exit_quote_mark_gap_pct REAL,
            max_path_quote_gap_pct REAL
        )
        """
    )
    return db


def test_revival_canary_pending_tags_policy_and_caps_size():
    pending = {
        "token_ca": "TokenCA",
        "symbol": "CANARY",
        "signal_type": "ATH",
        "entry_mode": ATH_NO_KLINE_TINY_PROBE_MODE,
        "kelly_position_sol": 0.05,
    }

    detail = apply_revival_canary_to_pending(
        pending,
        {"tier": "revival_canary", "reason": "test"},
        now_ts=1000,
    )

    assert detail["pass"] is True
    assert ATH_NO_KLINE_TINY_PROBE_MODE in monitor.PAPER_TINY_SCOUT_ENTRY_MODES
    assert pending["revival_canary"] is True
    assert pending["paper_only_scout"] is True
    assert pending["execution_scope"] == "paper_only"
    assert pending["policy_version"] == REVIVAL_CANARY_POLICY_VERSION
    assert pending["quote_guard_version"] == QUOTE_GUARD_POLICY_VERSION
    assert pending["registry_tier_at_entry"] == "revival_canary"
    assert pending["parent_entry_mode"] == ATH_NO_KLINE_TINY_PROBE_MODE
    assert pending["kelly_position_sol"] == PAPER_TINY_SCOUT_SIZE_SOL


def test_revival_canary_gate_ignores_mixed_policy_history_and_kills_tagged_loss_budget():
    db = _revival_canary_db()
    monitor._REVIVAL_CANARY_ARM_TS.clear()
    db.execute(
        """
        INSERT INTO paper_trades(
            token_ca, entry_mode, entry_ts, exit_ts, exit_reason, pnl_pct,
            position_size_sol, monitor_state_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "OldToken",
            ATH_NO_KLINE_TINY_PROBE_MODE,
            900,
            960,
            "old_policy_loss",
            -0.99,
            0.5,
            json.dumps({"policyVersion": "pre_quote_guard"}),
        ),
    )

    first = evaluate_revival_canary_gate(db, ATH_NO_KLINE_TINY_PROBE_MODE, now_ts=1000)
    assert first["pass"] is True
    assert first["policy_health"]["closed_trades"] == 0

    db.execute(
        """
        INSERT INTO paper_trades(
            token_ca, entry_mode, entry_ts, exit_ts, exit_reason, pnl_pct,
            position_size_sol, monitor_state_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "NewToken",
            ATH_NO_KLINE_TINY_PROBE_MODE,
            1001,
            1060,
            "guardian_hard_sl",
            -0.90,
            PAPER_TINY_SCOUT_SIZE_SOL,
            json.dumps({
                "revivalCanary": True,
                "policyVersion": REVIVAL_CANARY_POLICY_VERSION,
            }),
        ),
    )
    second = evaluate_revival_canary_gate(db, ATH_NO_KLINE_TINY_PROBE_MODE, now_ts=1100)
    assert second["pass"] is False
    assert second["reason"] == "revival_canary_loss_budget_hit"
    assert second["policy_health"]["closed_trades"] == 1


def test_entry_mode_quality_uses_revival_canary_policy_gate_for_selected_modes():
    db = _revival_canary_db()
    monitor._REVIVAL_CANARY_ARM_TS.clear()

    allowed, decision = _entry_mode_quality_allows_live(
        db,
        entry_mode=ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        token_ca="TokenCA",
        symbol="CANARY",
        lifecycle_id="TokenCA:1000",
        signal_ts=1000,
        signal_id=7,
        route="ATH",
        event_ts=1200,
        data_source="pending_entry+paper_trades",
    )

    assert allowed is True
    assert decision["reason"] == "revival_canary_policy_sampling"
    assert decision["paper_only"] is True
    assert decision["revival_canary"]["policy_version"] == REVIVAL_CANARY_POLICY_VERSION
    row = db.execute(
        """
        SELECT component, event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE component = 'revival_canary'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    payload = json.loads(row["payload_json"])
    assert row["event_type"] == "entry_allow"
    assert row["decision"] == "allow"
    assert payload["entry_mode"] == ATH_UNCERTAINTY_TINY_SCOUT_MODE
    assert payload["paper_only"] is True


def test_hard_gate_pass_probe_soft_quality_blocks_become_warnings():
    pending = {
        "entry_mode": HARD_GATE_PASS_TINY_PROBE_MODE,
        "paper_only_scout": True,
    }
    scout_quality = {
        "pass": False,
        "reason": "scout_quality_buy_pressure_weak",
        "observed": {"buy_sell_ratio": 0.8},
    }

    decision = _hard_gate_pass_probe_scout_quality_soft_override(pending, scout_quality)

    assert decision["pass"] is True
    assert decision["decision"] == "warn"
    assert decision["reason"] == "hard_gate_pass_baseline_quality_warn"
    assert decision["original_reason"] == "scout_quality_buy_pressure_weak"


def test_hard_gate_pass_probe_keeps_hard_quality_blocks():
    pending = {
        "entry_mode": HARD_GATE_PASS_TINY_PROBE_MODE,
        "paper_only_scout": True,
    }
    scout_quality = {
        "pass": False,
        "reason": "scout_quality_top1_high",
        "observed": {"top1_pct": 80},
    }

    decision = _hard_gate_pass_probe_scout_quality_soft_override(pending, scout_quality)

    assert decision is scout_quality
    assert decision["pass"] is False
    assert decision["reason"] == "scout_quality_top1_high"


def test_hard_gate_pass_probe_entry_mode_quality_degraded_becomes_warning():
    decision = {
        "decision": "shadow",
        "reason": "entry_mode_quality_degraded",
        "remaining_sec": 7200,
    }

    override = _hard_gate_pass_probe_entry_mode_quality_soft_override(
        HARD_GATE_PASS_TINY_PROBE_MODE,
        decision,
    )

    assert override["decision"] == "warn"
    assert override["reason"] == "hard_gate_pass_baseline_entry_mode_quality_warn"
    assert override["original_reason"] == "entry_mode_quality_degraded"
    assert override["paper_only_baseline_override"] is True
    assert override["execution_scope"] == "paper_only"


def test_hard_gate_pass_probe_keeps_shadow_only_entry_mode_quality_block():
    decision = {
        "decision": "shadow",
        "reason": "entry_mode_quality_shadow_only_mode",
        "shadow_only_mode": True,
    }

    override = _hard_gate_pass_probe_entry_mode_quality_soft_override(
        HARD_GATE_PASS_TINY_PROBE_MODE,
        decision,
    )

    assert override is None


def test_arm_hard_gate_pass_tiny_probe_builds_non_lotto_pending(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_arm_ts", [])
    monkeypatch.setattr(monitor, "_hard_gate_pass_probe_cooldown", {})
    monkeypatch.setattr(monitor, "HARD_GATE_PASS_TINY_PROBE_ENABLED", True)
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {"market_cap": 48000, "price_change_m5": 12},
    )
    monkeypatch.setattr(
        monitor,
        "helius_token_concentration",
        lambda _token_ca: {"top1_pct": 18, "top10_pct": 32},
    )
    monkeypatch.setattr(
        monitor,
        "safe_external_alpha_lookup",
        lambda *_args, **_kwargs: _gmgn_resonance_context()["external_alpha"],
    )
    pending_entries = {}
    registered_entry = {
        "id": 7,
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "ATH",
        "pool_address": "Pool",
        "signal_ts": 1000,
        "premium_signal_id": 11,
        "signal_price": 0.000001,
        "signal_mc": 48000,
        "signal_top10": 32,
    }

    armed = arm_hard_gate_pass_tiny_probe(
        db,
        pending_entries,
        {},
        sig={
            "id": 11,
            "token_ca": "TokenCA",
            "symbol": "DOG",
            "timestamp": 1000,
            "signal_type": "ATH",
            "market_cap": 48000,
        },
        registered_entry=registered_entry,
        pool="Pool",
        lifecycle_id="TokenCA:1000",
        signal_lifecycle={},
        signal_audit_payload={},
        hard_gate_status="PASS",
        now_ts=1200,
        route="ATH",
    )

    assert armed is True
    assert pending_entries["TokenCA:1000"]["signal_route"] == "ATH"
    assert pending_entries["TokenCA:1000"]["entry_mode"] == HARD_GATE_PASS_TINY_PROBE_MODE
    assert pending_entries["TokenCA:1000"]["paper_only_scout"] is True
    assert pending_entries["TokenCA:1000"]["execution_scope"] == "paper_only"
    assert pending_entries["TokenCA:1000"]["timing_passed"] is True
    assert pending_entries["TokenCA:1000"]["resonance_cohort"] == "telegram_gmgn"
    assert pending_entries["TokenCA:1000"]["gmgn_pre_seen"] is True


def test_source_resonance_upgrade_arms_pending_after_smart_entry_no_price(monkeypatch):
    monkeypatch.setattr(monitor, "SOURCE_RESONANCE_DIRECT_PROBE_ENABLED", True)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_type": "LOTTO",
        "signal_route": "LOTTO",
        "kelly_position_sol": 0.05,
        "trigger_price": None,
    }
    watchlist_entry = {
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "LOTTO",
        "signal_ts": 1000,
        "signal_mc": 42000,
        "signal_top10": 20,
    }
    external_alpha = {
        "available": True,
        "source": "external_alpha_shadow",
        "gmgn_pre_seen": True,
        "gmgn_lead_time_sec": 240,
        "last_seen_age_sec": 30,
        "gmgn_momentum_rounds": 2,
        "gmgn_momentum_gain_pct": 4,
        "gmgn_momentum_confirmed": False,
        "gmgn_volume_confirmed": False,
        "last_market_cap": 42000,
    }

    detail = _maybe_upgrade_pending_to_source_resonance_probe(
        db,
        pending,
        watchlist_entry,
        lifecycle_id="TokenCA:1000",
        route="LOTTO",
        parent_component="smart_entry",
        parent_decision="reject",
        parent_reason="no_price",
        dex_snapshot={"market_cap": 42000, "liquidity_usd": 15000, "vol_m5": 9000},
        live_concentration={"top1_pct": 20, "top10_pct": 40},
        external_alpha=external_alpha,
        now_ts=1300,
        data_source="smart_entry+external_alpha+dexscreener+helius",
    )

    assert detail["pass"] is True
    assert pending["entry_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert pending["paper_only_scout"] is True
    assert pending["timing_passed"] is True
    assert pending["kelly_position_sol"] == monitor.SOURCE_RESONANCE_TINY_PROBE_SIZE_SOL
    event = db.execute(
        """
        SELECT component, event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE component = 'source_resonance_probe'
        """
    ).fetchone()
    payload = json.loads(event["payload_json"])
    assert event["event_type"] == "pending_upgrade"
    assert event["decision"] == "pending"
    assert event["reason"] == "source_resonance_telegram_gmgn_probe"
    assert payload["source_resonance_parent_decision"]["component"] == "smart_entry"
    assert payload["source_resonance_parent_decision"]["reason"] == "no_price"


def test_source_resonance_upgrade_allows_smart_entry_soft_reject(monkeypatch):
    monkeypatch.setattr(monitor, "SOURCE_RESONANCE_DIRECT_PROBE_ENABLED", True)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_type": "ATH",
        "signal_route": "ATH",
        "kelly_position_sol": 0.05,
        "trigger_price": 0.000001,
    }
    watchlist_entry = {
        "ca": "TokenCA",
        "symbol": "DOG",
        "type": "ATH",
        "signal_ts": 1000,
        "signal_mc": 42000,
        "signal_price": 0.000001,
        "signal_top10": 20,
    }
    external_alpha = {
        "available": True,
        "source": "external_alpha_shadow",
        "gmgn_pre_seen": True,
        "gmgn_lead_time_sec": 30,
        "last_seen_age_sec": 30,
        "gmgn_momentum_rounds": 0,
        "gmgn_momentum_gain_pct": 0,
        "gmgn_momentum_confirmed": False,
        "gmgn_volume_confirmed": False,
        "last_market_cap": 42000,
    }

    detail = _maybe_upgrade_pending_to_source_resonance_probe(
        db,
        pending,
        watchlist_entry,
        lifecycle_id="TokenCA:1000",
        route="ATH",
        parent_component="smart_entry",
        parent_decision="reject",
        parent_reason="weak_buying_pressure",
        dex_snapshot={"market_cap": 42000, "liquidity_usd": 15000, "vol_m5": 9000},
        live_concentration={"top1_pct": 20, "top10_pct": 40},
        external_alpha=external_alpha,
        now_ts=1200,
        data_source="smart_entry+external_alpha+dexscreener+helius",
    )

    assert detail["pass"] is True
    assert detail["reason"] == "source_resonance_soft_override"
    assert pending["entry_mode"] == SOURCE_RESONANCE_TINY_PROBE_MODE
    assert pending["source_resonance_soft_override_used"] is True


def test_ttl_final_reclaim_quote_override_converts_soft_activity_gate():
    detail = _ttl_final_reclaim_quote_override_detail(
        {
            "mode": LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
            "final_reclaim_attempted": True,
            "final_reclaim_quote_executable": True,
        },
        {
            "pass": False,
            "reason": "lotto_recovery_shadow_activity_not_enough",
            "failures": ["buy_sell_ratio_low", "vol_m5_low"],
        },
        quote_probe={"success": True, "reason": "quote_executable"},
        mode=LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    )

    assert detail["pass"] is True
    assert detail["reason"] == "ttl_final_reclaim_quote_executable_canary"


def test_ttl_final_reclaim_quote_override_keeps_hard_failures_blocked():
    detail = _ttl_final_reclaim_quote_override_detail(
        {
            "mode": LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
            "final_reclaim_attempted": True,
            "final_reclaim_quote_executable": True,
        },
        {
            "pass": False,
            "reason": "lotto_recovery_shadow_activity_not_enough",
            "failures": ["top10_extreme"],
        },
        quote_probe={"success": True, "reason": "quote_executable"},
        mode=LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "ttl_final_reclaim_hard_failure"


def test_source_resonance_direct_probe_default_disabled(monkeypatch):
    monkeypatch.setattr(monitor, "SOURCE_RESONANCE_DIRECT_PROBE_ENABLED", False)
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    pending = {
        "token_ca": "TokenCA",
        "symbol": "DOG",
        "signal_ts": 1000,
        "signal_type": "LOTTO",
        "signal_route": "LOTTO",
        "kelly_position_sol": 0.05,
    }

    detail = _maybe_upgrade_pending_to_source_resonance_probe(
        db,
        pending,
        {"ca": "TokenCA", "symbol": "DOG", "type": "LOTTO", "signal_ts": 1000},
        lifecycle_id="TokenCA:1000",
        route="LOTTO",
        parent_component="smart_entry",
        parent_decision="reject",
        parent_reason="no_price",
        external_alpha=_gmgn_resonance_context()["external_alpha"],
        now_ts=1300,
    )

    assert detail["pass"] is False
    assert detail["reason"] == "source_resonance_direct_probe_disabled"
    assert pending.get("entry_mode") != SOURCE_RESONANCE_TINY_PROBE_MODE


def test_source_resonance_tiny_probe_uses_tiny_scout_spread_budget():
    decision = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.029,
        pending={
            "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
            "replay_source": "live_monitor_source_resonance_probe",
        },
    )

    assert decision["pass"] is True
    assert decision["profile"] == "lotto_probe"
    assert decision["max_spread_pct"] == monitor.ENTRY_EDGE_SOURCE_RESONANCE_MAX_SPREAD_PCT
    assert decision["spread_pct"] > monitor.ENTRY_EDGE_LOTTO_PROBE_MAX_SPREAD_PCT


def test_hard_gate_pass_tiny_probe_uses_tiny_scout_spread_budget():
    decision = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.0,
        quote_price=1.049,
        pending={
            "entry_mode": HARD_GATE_PASS_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.HARD_GATE_PASS_TINY_PROBE_SIZE_SOL,
            "replay_source": "live_monitor_hard_gate_pass_probe",
        },
    )

    assert decision["pass"] is True
    assert decision["max_spread_pct"] == monitor.ENTRY_EDGE_HARD_GATE_PASS_MAX_SPREAD_PCT
    assert decision["spread_pct"] > monitor.ENTRY_EDGE_ATH_MAX_SPREAD_PCT

    rejected = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.0,
        quote_price=1.061,
        pending={
            "entry_mode": HARD_GATE_PASS_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.HARD_GATE_PASS_TINY_PROBE_SIZE_SOL,
            "replay_source": "live_monitor_hard_gate_pass_probe",
        },
    )

    assert rejected["pass"] is False
    assert rejected["reason"] == "entry_edge_spread_too_high"


def test_pre_pass_resonance_tiny_probe_uses_probe_spread_budget():
    decision = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.044,
        pending={
            "entry_mode": PRE_PASS_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.PRE_PASS_RESONANCE_TINY_PROBE_SIZE_SOL,
            "replay_source": "live_monitor_pre_pass_resonance_probe",
        },
    )

    assert decision["pass"] is True
    assert decision["profile"] == "lotto_probe"
    assert decision["max_spread_pct"] == monitor.ENTRY_EDGE_PRE_PASS_RESONANCE_MAX_SPREAD_PCT
    assert decision["spread_pct"] > monitor.ENTRY_EDGE_LOTTO_PROBE_MAX_SPREAD_PCT

    rejected = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.052,
        pending={
            "entry_mode": PRE_PASS_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.PRE_PASS_RESONANCE_TINY_PROBE_SIZE_SOL,
            "replay_source": "live_monitor_pre_pass_resonance_probe",
        },
    )

    assert rejected["pass"] is False
    assert rejected["reason"] == "entry_edge_spread_too_high"


def test_entry_edge_blocks_probe_when_trigger_missing():
    decision = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=None,
        quote_price=1.0,
        pending={
            "entry_mode": HARD_GATE_PASS_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.HARD_GATE_PASS_TINY_PROBE_SIZE_SOL,
            "replay_source": "live_monitor_hard_gate_pass_probe",
        },
    )

    assert decision["pass"] is False
    assert decision["reason"] == "entry_edge_probe_missing_trigger_or_quote"


def test_late_entry_guard_blocks_stale_source_resonance_without_clean_edge():
    decision = dog_catcher_late_entry_guard_detail(
        {
            "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.SOURCE_RESONANCE_TINY_PROBE_SIZE_SOL,
            "external_alpha": {
                "gmgn_pre_seen": True,
                "gmgn_momentum_rounds": 0,
                "gmgn_momentum_gain_pct": 0,
            },
        },
        entry_latency_audit={
            "signal_to_quote_latency_ms": 386_131,
            "quote_spread_pct": 3.63,
        },
        entry_edge_budget={"pass": True, "spread_pct": 3.63},
    )

    assert decision["pass"] is False
    assert decision["reason"] == "dog_catcher_late_entry_latency_edge"


def test_late_entry_guard_allows_pre_pass_inside_latency_budget():
    decision = dog_catcher_late_entry_guard_detail(
        {
            "entry_mode": PRE_PASS_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.PRE_PASS_RESONANCE_TINY_PROBE_SIZE_SOL,
        },
        entry_latency_audit={
            "signal_to_quote_latency_ms": 156_263,
            "quote_spread_pct": 3.28,
        },
        entry_edge_budget={"pass": True, "spread_pct": 3.28},
    )

    assert decision["pass"] is True
    assert decision["reason"] == "late_entry_guard_ok"


def test_late_entry_guard_allows_late_source_only_with_strong_current_edge():
    decision = dog_catcher_late_entry_guard_detail(
        {
            "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
            "paper_only_scout": True,
            "kelly_position_sol": monitor.SOURCE_RESONANCE_TINY_PROBE_SIZE_SOL,
            "external_alpha": {
                "gmgn_pre_seen": True,
                "gmgn_momentum_confirmed": True,
            },
        },
        entry_latency_audit={
            "signal_to_quote_latency_ms": 300_000,
            "quote_spread_pct": 2.0,
        },
        entry_edge_budget={"pass": True, "spread_pct": 2.0},
    )

    assert decision["pass"] is True
    assert decision["reason"] == "late_entry_strong_source_confirmed"


def test_late_entry_guard_blocks_stale_smart_pullback_tiny_probe():
    decision = dog_catcher_late_entry_guard_detail(
        {
            "entry_mode": "smart_entry_pullback_bounce",
            "paper_only_scout": True,
            "kelly_position_sol": monitor.PAPER_TINY_SCOUT_SIZE_SOL,
        },
        entry_latency_audit={
            "signal_to_quote_latency_ms": 1_650_344,
            "quote_spread_pct": 3.7,
        },
        entry_edge_budget={"pass": True, "spread_pct": 3.7},
    )

    assert decision["pass"] is False
    assert decision["reason"] == "dog_catcher_late_entry_latency_edge"


def test_tiny_scout_dex_fallback_builds_synthetic_paper_entry(monkeypatch):
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {
            "price_usd": 0.001,
            "liquidity_usd": 12000,
            "vol_m5": 9000,
            "buys_m5": 70,
            "sells_m5": 35,
            "price_change_m5": 4.0,
            "dex_id": "pumpswap",
            "pair_address": "PairA",
        },
    )
    pending = {
        "token_ca": "TokenCA",
        "entry_mode": SOURCE_RESONANCE_TINY_PROBE_MODE,
        "paper_only_scout": True,
        "kelly_position_sol": PAPER_TINY_SCOUT_SIZE_SOL,
    }

    execution = build_paper_tiny_scout_dex_fallback_entry_execution(
        pending,
        PAPER_TINY_SCOUT_SIZE_SOL,
        failed_execution={"success": False, "failureReason": "no_route", "routeAvailable": False},
        sol_price=100,
    )

    assert execution["success"] is True
    assert execution["routeAvailable"] is False
    assert execution["syntheticPaperEntry"] is True
    assert execution["effectivePrice"] == 0.00001
    assert execution["quotedOutAmountRaw"] == "300000000"
    assert execution["outputDecimals"] == 6


def test_tiny_scout_dex_fallback_ignores_non_probe(monkeypatch):
    monkeypatch.setattr(
        monitor,
        "fetch_dexscreener_trend_snapshot",
        lambda _token_ca: {"price_usd": 0.001},
    )
    execution = build_paper_tiny_scout_dex_fallback_entry_execution(
        {"token_ca": "TokenCA", "entry_mode": "stage1", "kelly_position_sol": 0.06},
        0.06,
        failed_execution={"success": False, "failureReason": "no_route"},
        sol_price=100,
    )

    assert execution is None


def test_observation_probe_synthetic_exit_uses_mark_price_when_quote_blocked():
    class Pos:
        trade_id = 3003
        lifecycle_id = "lc-3003"
        token_ca = "TokenCA"
        symbol = "UNKNOWN"
        strategy_stage = "lotto"
        entry_mode = SOURCE_RESONANCE_TINY_PROBE_MODE
        position_size_sol = PAPER_TINY_SCOUT_SIZE_SOL
        token_amount_raw = "300000000"
        token_decimals = 6
        monitor_state = {
            "entryMode": SOURCE_RESONANCE_TINY_PROBE_MODE,
            "entrySol": PAPER_TINY_SCOUT_SIZE_SOL,
            "signalRoute": "LOTTO",
        }

    execution = build_paper_observation_probe_synthetic_exit_execution(
        Pos(),
        0.00000079,
        failed_execution={"success": False, "failureReason": "RATE_LIMITED"},
        sell_pct=1.0,
        reason="RATE_LIMITED",
    )

    assert execution["success"] is True
    assert execution["syntheticPaperExit"] is True
    assert execution["effectivePrice"] == 0.00000079
    assert execution["quotedOutAmount"] == 0.00000079 * 300
    assert execution["originalFailureReason"] == "RATE_LIMITED"


def test_path_sample_database_lock_does_not_block_close_path():
    class LockedDb:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.OperationalError("database is locked")

    class Pos:
        trade_id = 3003
        lifecycle_id = "lc-3003"
        token_ca = "TokenCA"
        symbol = "UNKNOWN"
        strategy_stage = "lotto"
        entry_price = 0.0000006
        peak_pnl = 3.488
        token_amount_raw = "300000000"
        monitor_state = {"entryMode": SOURCE_RESONANCE_TINY_PROBE_MODE}

    assert (
        record_trade_path_sample(
            LockedDb(),
            Pos(),
            sample_ts=1778620714,
            action="exit",
            reason="timeout",
            mark_price=0.00000079,
            mark_pnl=0.319,
            mark_source="force_timeout",
            quote_execution={"success": True, "effectivePrice": 0.00000079},
        )
        is False
    )
