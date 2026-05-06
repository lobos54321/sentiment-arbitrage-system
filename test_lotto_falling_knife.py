import sys
import sqlite3
import json

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit, record_decision_event  # noqa: E402
from lotto_engine import build_lotto_pending, evaluate_lotto_entry, evaluate_lotto_exit  # noqa: E402
from exit_engine import (  # noqa: E402
    _partial_delta_from_command,
    _raw_amount_for_absolute_partial,
)
from matrix_evaluator import (  # noqa: E402
    ExitMatrixEvaluator,
    ath_flat_momentum_allowed,
    ath_flat_structure_tiny_scout_allowed,
    ath_structural_reentry_allowed,
)
from entry_readiness_policy import evaluate_entry_readiness_policy, entry_mode_allowed  # noqa: E402
from analyze_trade_failure_attribution import classify_system_stage  # noqa: E402
from profit_protect_policy import profit_protect_floor  # noqa: E402
from entry_engine import evaluate_entry_position, evaluate_smart_entry, smart_entry_bounce_reject_reason  # noqa: E402
from entry_decision_contract import build_entry_decision_contract  # noqa: E402
from gmgn_policy import evaluate_gmgn_tiny_scout_rescue  # noqa: E402
from scout_quality import evaluate_scout_quality  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    apply_paper_tiny_scout_size_cap,
    apply_probe_profit_capture,
    arm_ath_uncertainty_tiny_scout,
    evaluate_entry_edge_budget,
    evaluate_spread_abort_memory,
    evaluate_token_reclaim,
    find_ath_real_probe_candidates,
    find_lotto_real_probe_candidates,
    find_lotto_upstream_miss_tiny_scout_candidates,
    normalize_price_age_ms,
    process_discovery_tracking_candidates,
    record_scout_funnel_summary,
    record_explosive_continuation_shadow_candidates,
    record_scout_quality_decision,
    record_upstream_miss_chain_summary,
    should_block_lotto_falling_knife,
    should_block_lotto_lifecycle_entry,
    token_quarantine_state,
    track_discovery_candidate,
)


def test_blocks_newborn_low_liq_m5_down_falling_knife():
    blocked, detail = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "NEWBORN_LAUNCH",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": -43.04,
            },
        },
    )
    assert blocked is True
    assert detail["liquidity_usd"] == 9654.68
    assert detail["price_change_m5"] == -43.04


def test_allows_newborn_low_liq_without_m5_downtrend():
    blocked, _ = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "NEWBORN_LAUNCH",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": 12.0,
            },
        },
    )
    assert blocked is False


def test_allows_non_newborn_even_when_low_liq_m5_down():
    blocked, _ = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "FIRST_PUMP",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": -43.04,
            },
        },
    )
    assert blocked is False


def test_real_probe_requires_tradable_reclaim_not_reason_text():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    rows = [
        ("BlueBuck", "BlueToken", "lotto_stale_1825s", 0.4653, 0.3439, 0.4653, 1, "tradable_reclaim", 0),
        ("Saviour", "SaviourToken", "lotto_stale_1825s", 0.6854, -0.0329, 0.6854, 0, "would_stop_before_peak", 1),
    ]
    for symbol, token, reason, pnl5, pnl15, max_pnl, tradable, status, would_stop in rows:
        db.execute(
            """
            INSERT INTO paper_missed_signal_attribution
                (created_event_ts, token_ca, symbol, signal_ts, route, component,
                 decision, reject_reason, baseline_price, baseline_ts,
                 pnl_5m, pnl_15m, max_pnl_recorded, status,
                 tradable_missed, tradability_status, would_stop_before_peak,
                 first_tradable_pnl, tradable_peak_pnl)
            VALUES (?, ?, ?, ?, 'LOTTO', 'lotto_entry_gate',
                    'expire', ?, 1.0, ?, ?, ?, ?, 'pending',
                    ?, ?, ?, ?, ?)
            """,
            (
                1000,
                token,
                symbol,
                900,
                reason,
                900,
                pnl5,
                pnl15,
                max_pnl,
                tradable,
                status,
                would_stop,
                pnl15,
                max_pnl,
            ),
        )
    db.commit()
    candidates = find_lotto_real_probe_candidates(db, now_ts=1200, limit=5)
    assert [row["symbol"] for row in candidates] == ["BlueBuck"]


def test_real_probe_rejects_stale_missed_opportunity():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, signal_ts, route, component,
             decision, reject_reason, baseline_price, baseline_ts,
             pnl_5m, pnl_15m, max_pnl_recorded, status,
             tradable_missed, tradability_status, would_stop_before_peak,
             first_tradable_pnl, tradable_peak_pnl)
        VALUES (1000, 'OldToken', 'OLD', 900, 'LOTTO', 'lotto_entry_gate',
                'expire', 'lotto_stale_3600s', 1.0, 900,
                0.70, 0.60, 0.70, 'pending',
                1, 'tradable_reclaim', 0, 0.60, 0.70)
        """
    )
    db.commit()
    candidates = find_lotto_real_probe_candidates(db, now_ts=5000, limit=5)
    assert candidates == []


def test_ath_real_probe_uses_only_uncertainty_gate_counterfactuals():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    rows = [
        ("ALIGN", "AlignToken", "matrix_evaluator", "matrices not yet aligned", 1.20, 0.40),
        ("SMART", "SmartToken", "smart_entry", "no_kline_low_volume", 0.80, 0.30),
        ("CRASH", "CrashToken", "matrix_evaluator", "price_collapse", 5.00, 1.00),
    ]
    for symbol, token, component, reason, peak_pnl, reclaim_pnl in rows:
        db.execute(
            """
            INSERT INTO paper_missed_signal_attribution
                (created_event_ts, token_ca, symbol, signal_ts, route, component,
                 decision, reject_reason, baseline_price, baseline_ts,
                 pnl_5m, pnl_15m, max_pnl_recorded, status,
                 tradable_missed, tradability_status, would_stop_before_peak,
                 first_tradable_pnl, tradable_peak_pnl)
            VALUES (?, ?, ?, ?, 'ATH', ?,
                    'wait', ?, 1.0, ?,
                    ?, ?, ?, 'pending',
                    1, 'tradable_reclaim', 0, ?, ?)
            """,
            (
                1000,
                token,
                symbol,
                900,
                component,
                reason,
                900,
                reclaim_pnl,
                peak_pnl,
                peak_pnl,
                reclaim_pnl,
                peak_pnl,
            ),
        )
    db.commit()

    candidates = find_ath_real_probe_candidates(db, now_ts=1200, limit=5)

    assert [row["symbol"] for row in candidates] == ["ALIGN", "SMART"]


def test_ath_real_probe_skips_already_armed_token():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, signal_ts, route, component,
             decision, reject_reason, baseline_price, baseline_ts,
             pnl_5m, pnl_15m, max_pnl_recorded, status,
             tradable_missed, tradability_status, would_stop_before_peak,
             first_tradable_pnl, tradable_peak_pnl)
        VALUES (1000, 'ArmedToken', 'ARMED', 900, 'ATH', 'matrix_evaluator',
                'wait', 'momentum check failed: noise +0.17% < 0.8%', 1.0, 900,
                0.40, 0.90, 0.90, 'pending',
                1, 'tradable_reclaim', 0, 0.40, 0.90)
        """
    )
    record_decision_event(
        db,
        component="ath_probe_live",
        event_type="pending_entry",
        decision="pending",
        reason="ath_flat_structure_tiny_scout",
        token_ca="ArmedToken",
        symbol="ARMED",
        route="ATH",
        event_ts=1100,
    )

    candidates = find_ath_real_probe_candidates(db, now_ts=1200, limit=5)

    assert candidates == []


def test_lotto_upstream_miss_probe_targets_only_clean_upstream_blockers():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    rows = [
        ("NATH", "NotAthToken", "upstream_gate", "not_ath_v17", 1.30, 0.35, 1, "tradable_reclaim", 0),
        ("UNK", "UnknownKlineToken", "upstream_gate", "not_ath_prebuy_kline_unknown_data_blocked", 0.80, 0.25, 1, "tradable_reclaim", 0),
        ("ENTRY", "EntryGateToken", "lotto_entry_gate", "lotto_stale_1825s", 2.00, 0.50, 1, "tradable_reclaim", 0),
        ("STOP", "StopToken", "upstream_gate", "not_ath_v17", 3.00, 0.50, 1, "would_stop_before_peak", 1),
        ("LOW", "LowPnlToken", "upstream_gate", "not_ath_v17", 0.20, 0.19, 1, "tradable_reclaim", 0),
    ]
    for symbol, token, component, reason, peak_pnl, reclaim_pnl, tradable, status, would_stop in rows:
        db.execute(
            """
            INSERT INTO paper_missed_signal_attribution
                (created_event_ts, token_ca, symbol, signal_ts, route, component,
                 decision, reject_reason, baseline_price, baseline_ts,
                 pnl_5m, pnl_15m, max_pnl_recorded, status,
                 tradable_missed, tradability_status, would_stop_before_peak,
                 first_tradable_pnl, tradable_peak_pnl)
            VALUES (1000, ?, ?, 900, 'LOTTO', ?,
                    'wait', ?, 1.0, 900,
                    ?, ?, ?, 'pending',
                    ?, ?, ?, ?, ?)
            """,
            (
                token,
                symbol,
                component,
                reason,
                reclaim_pnl,
                peak_pnl,
                peak_pnl,
                tradable,
                status,
                would_stop,
                reclaim_pnl,
                peak_pnl,
            ),
        )
    db.commit()

    candidates = find_lotto_upstream_miss_tiny_scout_candidates(db, now_ts=1200, limit=10)

    assert [row["symbol"] for row in candidates] == ["NATH", "UNK"]


def test_lotto_upstream_miss_probe_skips_already_armed_token():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, signal_ts, route, component,
             decision, reject_reason, baseline_price, baseline_ts,
             pnl_5m, pnl_15m, max_pnl_recorded, status,
             tradable_missed, tradability_status, would_stop_before_peak,
             first_tradable_pnl, tradable_peak_pnl)
        VALUES (1000, 'ArmedUpstreamToken', 'ARMUP', 900, 'LOTTO', 'upstream_gate',
                'wait', 'not_ath_v17', 1.0, 900,
                0.35, 0.80, 0.80, 'pending',
                1, 'tradable_reclaim', 0, 0.35, 0.80)
        """
    )
    record_decision_event(
        db,
        component="lotto_upstream_probe_live",
        event_type="pending_entry",
        decision="pending",
        reason="lotto_upstream_miss_tiny_scout",
        token_ca="ArmedUpstreamToken",
        symbol="ARMUP",
        route="LOTTO",
        event_ts=1100,
    )
    db.commit()

    candidates = find_lotto_upstream_miss_tiny_scout_candidates(db, now_ts=1200, limit=5)

    assert candidates == []


def test_paper_tiny_scout_size_cap_precedes_lotto_fixed_size():
    pending = {
        "is_lotto": True,
        "entry_mode": "pullback_tiny_scout",
        "paper_only_scout": True,
        "kelly_position_sol": 0.05,
        "lotto_state": {
            "positionSizeSol": 0.05,
            "entryDecision": {
                "entry_mode": "pullback_tiny_scout",
                "position_size_sol": 0.05,
                "paper_only_scout": True,
            },
        },
    }

    detail = apply_paper_tiny_scout_size_cap(pending)

    assert detail["capped"] is True
    assert pending["kelly_position_sol"] == 0.003
    assert pending["lotto_state"]["positionSizeSol"] == 0.003
    assert pending["lotto_state"]["entryDecision"]["position_size_sol"] == 0.003


def test_probe_profit_capture_locks_small_scout_at_ten_percent():
    class Pos:
        position_size_sol = 0.004
        peak_pnl = 0.02
        signal_type = "LOTTO"
        monitor_state = {
            "entryMode": "explosive_newborn_direct_scout",
            "signalRoute": "LOTTO",
            "entrySol": 0.004,
        }

    exit_matrix = apply_probe_profit_capture(
        Pos(),
        {},
        {"action": "hold", "reason": "lotto_verify", "current_pnl": 0.105, "peak_pnl": 0.105},
    )

    assert exit_matrix["action"] == "lock_profit"
    assert exit_matrix["sell_pct"] == 0.75
    assert exit_matrix["reason"].startswith("probe_profit_lock")


def test_observation_probe_late_locks_when_ten_percent_peak_gives_back_to_three():
    class Pos:
        position_size_sol = 0.003
        peak_pnl = 0.109
        signal_type = "ATH"
        monitor_state = {
            "entryMode": "ath_uncertainty_tiny_scout",
            "signalRoute": "ATH",
            "entrySol": 0.003,
        }

    exit_matrix = apply_probe_profit_capture(
        Pos(),
        {"peak_pnl": 0.109},
        {"action": "hold", "reason": "hold", "current_pnl": 0.025},
    )

    assert exit_matrix["action"] == "lock_profit"
    assert exit_matrix["sell_pct"] == 0.75
    assert exit_matrix["reason"].startswith("probe_profit_late_lock")


def test_probe_profit_capture_does_not_touch_main_size_position():
    class Pos:
        position_size_sol = 0.05
        peak_pnl = 0.11
        signal_type = "LOTTO"
        monitor_state = {
            "entryMode": "lotto_fast_lane",
            "signalRoute": "LOTTO",
            "entrySol": 0.05,
        }

    original = {"action": "hold", "reason": "lotto_verify", "current_pnl": 0.02, "peak_pnl": 0.11}
    exit_matrix = apply_probe_profit_capture(Pos(), {}, original)

    assert exit_matrix == original


def test_shared_scout_quality_blocks_weak_midcap_near_miss():
    quality = evaluate_scout_quality(
        mode="gmgn_midcap_near_miss_scout",
        route="LOTTO",
        trend={
            "liquidity_usd": 7000,
            "price_change_m5": -20,
            "vol_m5": 6000,
            "buys_m5": 50,
            "sells_m5": 40,
        },
        position_size_sol=0.003,
    )

    assert quality["pass"] is False
    assert quality["reason"] == "scout_quality_volume_low"


def test_gmgn_midcap_rescue_uses_shared_quality_gate():
    clean_policy = {
        "action": "allow",
        "toxic_score": 0,
        "edge_score": 5,
        "features": {
            "rat_trader_amount_rate": 0.01,
            "entrapment_ratio": 0.01,
            "bundler_rate": 0.01,
            "creator_hold_rate": 0.0,
            "dev_team_hold_rate": 0.0,
        },
    }

    weak = evaluate_gmgn_tiny_scout_rescue(
        "lotto_midcap_activity_unconfirmed",
        clean_policy,
        {
            "liquidity_usd": 7000,
            "vol_m5": 6000,
            "tx_m5": 120,
            "buy_sell_ratio": 1.3,
            "price_change_m5": -5,
        },
    )
    strong = evaluate_gmgn_tiny_scout_rescue(
        "lotto_midcap_activity_unconfirmed",
        clean_policy,
        {
            "liquidity_usd": 9000,
            "vol_m5": 18000,
            "tx_m5": 240,
            "buy_sell_ratio": 1.35,
            "price_change_m5": -5,
        },
    )

    assert weak["allow"] is False
    assert weak["reason"] == "scout_quality_volume_low"
    assert strong["allow"] is True
    assert strong["position_size_sol"] == 0.003


def test_explosive_continuation_shadow_records_chasing_top_without_entry():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, signal_ts, route, component,
             decision, reject_reason, baseline_price, baseline_ts,
             pnl_5m, pnl_15m, max_pnl_recorded, status)
        VALUES (1000, 'ChaseToken', 'CHASE', 900, 'LOTTO', 'smart_entry',
                'block', 'chasing_top', 1.0, 900,
                0.05, 0.30, 0.50, 'pending')
        """
    )
    db.commit()

    recorded = record_explosive_continuation_shadow_candidates(db, now_ts=1200, limit=5)

    assert recorded == 1
    row = db.execute(
        "SELECT component, event_type, decision, reason FROM paper_decision_events WHERE token_ca = 'ChaseToken'"
    ).fetchone()
    assert row["component"] == "explosive_continuation_shadow"
    assert row["event_type"] == "shadow_candidate"
    assert row["decision"] == "SHADOW_ONLY"
    assert row["reason"] == "chasing_top"


def test_lotto_lifecycle_blocks_deep_reset_reject():
    blocked, reason, detail = should_block_lotto_lifecycle_entry({
        "lifecycle_state": "ATH_DEEP_RESET",
        "entry_bias": "REJECT",
        "lifecycle_features": {"price_change_m5": -63.79},
    })
    assert blocked is True
    assert reason == "lotto_lifecycle_entry_bias_reject"
    assert detail["lifecycle_state"] == "ATH_DEEP_RESET"


def test_lotto_lifecycle_blocks_negative_m5():
    blocked, reason, detail = should_block_lotto_lifecycle_entry({
        "lifecycle_state": "FIRST_PUMP",
        "entry_bias": "PROBE",
        "lifecycle_features": {"price_change_m5": -12.0},
    })
    assert blocked is True
    assert reason == "lotto_timing_negative_m5"
    assert detail["price_change_m5"] == -12.0


def test_token_quarantine_blocks_recent_same_ca_failure():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            exit_ts REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route)
        VALUES ('TokenA', 'TOKA', 1000, -0.12, 0.0, 'guardian_lotto_fast_fail_20s',
                'live_monitor_lotto', 'LOTTO')
        """
    )
    db.commit()
    state = token_quarantine_state(db, "TokenA", now_ts=1100)
    assert state["blocked"] is True
    assert state["reason"] == "token_quarantine_recent_failure"
    assert state["severe_failure_count"] == 1


def test_token_quarantine_requires_reclaim_after_cooldown():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            exit_ts REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route)
        VALUES ('TokenA', 'TOKA', 1000, -0.12, 0.0, 'guardian_lotto_fast_fail_20s',
                'live_monitor_lotto', 'LOTTO')
        """
    )
    db.commit()
    blocked = token_quarantine_state(
        db,
        "TokenA",
        now_ts=5000,
        reclaim={"reclaim_confirmed": False, "reason": "reclaim_m5_too_low"},
    )
    assert blocked["blocked"] is True
    assert blocked["reason"] == "token_quarantine_reclaim_required"
    assert blocked["cooldown_expired"] is True

    unlocked = token_quarantine_state(
        db,
        "TokenA",
        now_ts=5000,
        reclaim={"reclaim_confirmed": True, "reason": "reclaim_confirmed"},
    )
    assert unlocked["blocked"] is False
    assert unlocked["reclaim_unlocked"] is True


def test_token_quarantine_ignores_observation_probe_loss():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            exit_ts REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT,
            position_size_sol REAL,
            entry_mode TEXT,
            monitor_state_json TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source,
             signal_route, position_size_sol, entry_mode, monitor_state_json)
        VALUES ('TokenA', 'TOKA', 1000, -0.18, 0.05, 'hard_sl',
                'live_monitor_discovery_probe', 'ATH', 0.003,
                'matrix_reclaim_tiny_probe', '{"entryMode":"matrix_reclaim_tiny_probe","entrySol":0.003}')
        """
    )
    db.commit()

    state = token_quarantine_state(db, "TokenA", now_ts=1100)

    assert state["blocked"] is False
    assert state["severe_failure_count"] == 0
    assert state["risk_memory_count"] == 0


def test_evaluate_token_reclaim_requires_current_strength():
    strong = evaluate_token_reclaim(
        dex_snapshot={
            "price_change_m5": 18,
            "buys_m5": 50,
            "sells_m5": 30,
            "liquidity_usd": 10000,
        },
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {"tx_m5": 80},
        },
        route="LOTTO",
    )
    assert strong["reclaim_confirmed"] is True

    weak = evaluate_token_reclaim(
        dex_snapshot={
            "price_change_m5": 5,
            "buys_m5": 50,
            "sells_m5": 30,
            "liquidity_usd": 10000,
        },
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {"tx_m5": 80},
        },
        route="LOTTO",
    )
    assert weak["reclaim_confirmed"] is False
    assert weak["reason"] == "reclaim_m5_too_low"


def test_entry_edge_budget_blocks_lotto_spread_over_budget():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.93e-7,
        quote_price=1.9883383588344e-7,
        lifecycle={"lifecycle_features": {"liquidity_unknown": True, "live_top1_pct": 31.6}},
        pending={"is_lotto": True},
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["profile"] == "lotto_risky"


def test_entry_edge_budget_allows_favorable_lotto_fill():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=3.72e-7,
        quote_price=3.4071349202664e-7,
        lifecycle={"lifecycle_features": {"liquidity_unknown": True, "live_top1_pct": 44.4}},
        pending={"is_lotto": True},
    )
    assert budget["pass"] is True
    assert budget["spread_pct"] < 0


def test_entry_edge_budget_uses_readiness_policy_spread_cap():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.018,
        lifecycle={"lifecycle_features": {"liquidity_unknown": False, "live_top1_pct": 10}},
        pending={
            "is_lotto": True,
            "entry_readiness_policy": {
                "lifecycle_profile": "LOTTO_NORMAL",
                "max_spread_pct": 1.5,
            },
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["max_spread_pct"] == 1.5
    assert budget["readiness_max_spread_pct"] == 1.5


def test_entry_edge_budget_keeps_upstream_miss_tiny_scout_spread_strict():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.02,
        lifecycle={"lifecycle_features": {"liquidity_unknown": False, "live_top1_pct": 10}},
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_upstream_miss_tiny_scout",
            "entry_readiness_policy": {
                "lifecycle_profile": "LOTTO_REAL_PROBE",
                "max_spread_pct": 1.0,
            },
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["max_spread_pct"] == 1.0
    assert budget["tiny_scout_spread_cap_pct"] == 1.0


def test_entry_edge_budget_keeps_upstream_realtime_tiny_scout_spread_strict():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.02,
        lifecycle={"lifecycle_features": {"liquidity_unknown": False, "live_top1_pct": 10}},
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_upstream_realtime_tiny_scout",
            "replay_source": "live_monitor_lotto_upstream_realtime",
            "entry_readiness_policy": {
                "lifecycle_profile": "LOTTO_REAL_PROBE",
                "max_spread_pct": 1.0,
            },
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["max_spread_pct"] == 1.0
    assert budget["tiny_scout_spread_cap_pct"] == 1.0


def test_entry_edge_budget_keeps_lotto_discovery_probe_spread_strict():
    budget = evaluate_entry_edge_budget(
        route="LOTTO",
        trigger_price=1.0,
        quote_price=1.02,
        lifecycle={"lifecycle_features": {"liquidity_unknown": False, "live_top1_pct": 55}},
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_high_risk_discovery_probe",
            "replay_source": "live_monitor_discovery_probe",
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["profile"] == "lotto_probe"
    assert budget["max_spread_pct"] == 1.0
    assert budget["tiny_scout_spread_cap_pct"] == 1.0


def test_entry_edge_budget_keeps_ath_uncertainty_tiny_scout_spread_capped():
    budget = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.0,
        quote_price=1.025,
        lifecycle={},
        pending={
            "paper_only_scout": True,
            "entry_mode": "ath_uncertainty_tiny_scout",
            "replay_source": "live_monitor_ath_uncertainty",
            "entry_readiness_policy": {
                "lifecycle_profile": "ATH_CONTINUATION",
                "max_spread_pct": 2.0,
            },
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["max_spread_pct"] == 2.0


def test_entry_edge_budget_keeps_ath_discovery_probe_spread_capped():
    budget = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.0,
        quote_price=1.025,
        lifecycle={},
        pending={
            "paper_only_scout": True,
            "entry_mode": "matrix_reclaim_tiny_probe",
            "replay_source": "live_monitor_discovery_probe",
        },
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["max_spread_pct"] == 2.0


def test_entry_edge_budget_blocks_ath_spread_over_budget():
    budget = evaluate_entry_edge_budget(
        route="ATH",
        trigger_price=1.713e-6,
        quote_price=1.773307e-6,
        lifecycle={},
        pending={},
    )
    assert budget["pass"] is False
    assert budget["reason"] == "entry_edge_spread_too_high"
    assert budget["profile"] == "ath"
    assert budget["max_spread_pct"] == 2.5


def test_positive_gap_crash_is_waterfall_memory_not_loss_failure():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            exit_ts REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route)
        VALUES ('LifeCA', 'Life', 1000, 0.581, 0.970,
                'guardian_gap_crash (28.7% drop in 1 tick, 96.5%→67.8%)',
                'live_monitor_lotto', 'LOTTO')
        """
    )
    db.commit()

    state = token_quarantine_state(db, "LifeCA", now_ts=1100)
    assert state["blocked"] is True
    assert state["reason"] == "token_quarantine_waterfall_memory"
    assert state["severe_failure_count"] == 0
    assert state["risk_memory_count"] == 1
    assert state["risk_profile"] == "waterfall_memory"


def test_waterfall_memory_requires_stronger_reclaim_after_cooldown():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            exit_ts REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route)
        VALUES ('LifeCA', 'Life', 1000, 0.581, 0.970,
                'guardian_gap_crash (28.7% drop in 1 tick, 96.5%→67.8%)',
                'live_monitor_lotto', 'LOTTO')
        """
    )
    db.commit()

    weak = token_quarantine_state(
        db,
        "LifeCA",
        now_ts=5000,
        reclaim={
            "reclaim_confirmed": True,
            "route": "LOTTO",
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "price_change_m5": 20,
            "buy_sell_ratio": 1.33,
            "tx_m5": 100,
            "liquidity_usd": 10000,
            "relative_volume": 0.2,
        },
    )
    assert weak["blocked"] is True
    assert weak["reason"] == "token_quarantine_reclaim_required"
    assert weak["risk_profile"] == "waterfall_memory"

    strong = token_quarantine_state(
        db,
        "LifeCA",
        now_ts=5000,
        reclaim={
            "reclaim_confirmed": True,
            "route": "LOTTO",
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "price_change_m5": 40,
            "buy_sell_ratio": 1.7,
            "tx_m5": 100,
            "liquidity_usd": 10000,
            "relative_volume": 2.0,
        },
    )
    assert strong["blocked"] is False
    assert strong["reclaim_unlocked"] is True


def test_guardian_partial_command_skips_when_target_already_reached():
    sell_delta, target = _partial_delta_from_command(
        {
            "action": "partial_sell",
            "sell_pct": 0.25,
            "queued_sold_pct": 0.0,
            "target_sold_pct": 0.25,
            "partial_type": "LOTTO_LOCK",
        },
        current_sold_pct=0.25,
    )
    assert sell_delta == 0.0
    assert target == 0.25


def test_guardian_partial_command_sells_only_remaining_delta_to_target():
    sell_delta, target = _partial_delta_from_command(
        {
            "action": "partial_sell",
            "sell_pct": 0.25,
            "queued_sold_pct": 0.0,
            "target_sold_pct": 0.25,
            "partial_type": "LOTTO_LOCK",
        },
        current_sold_pct=0.10,
    )
    assert round(sell_delta, 6) == 0.15
    assert target == 0.25


def test_guardian_partial_raw_amount_uses_remaining_position_fraction():
    raw_amount = _raw_amount_for_absolute_partial(
        900,
        current_sold_pct=0.10,
        sell_pct_delta=0.15,
    )
    assert raw_amount == 150


def test_price_age_clamps_small_clock_skew_and_rejects_future_quote():
    clamped_age, clamped_status = normalize_price_age_ms(
        10_000,
        10_900,
        max_future_ms=1500,
    )
    assert clamped_age == 0
    assert clamped_status == "clock_skew_clamped"

    future_age, future_status = normalize_price_age_ms(
        10_000,
        12_000,
        max_future_ms=1500,
    )
    assert future_age == -2000
    assert future_status == "future_quote"


def test_lotto_pending_defaults_to_timing_gate():
    pending = build_lotto_pending(
        {
            "ca": "TokenA",
            "symbol": "TOKA",
            "signal_ts": 1000,
            "premium_signal_id": 1,
            "pool_address": "PoolA",
            "id": 7,
        },
        "TokenA:1000",
        detail={"price_change_m5": 18.0},
    )
    assert pending["timing_passed"] is False
    assert pending["entry_mode"] == "lotto_fast_arm"
    assert pending["first_fire_pc_m5"] == 18.0


def test_lotto_blocks_pumpfun_liquidity_unknown_live_top10_over_risky_limit():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 10000,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 45,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 2000,
            "buys_m5": 30,
            "sells_m5": 10,
        },
        live_concentration={"top1_pct": 25, "top10_pct": 54.6},
        now=1100,
    )
    assert decision.expire is True
    assert decision.reason == "lotto_live_top10_55pct"
    assert decision.detail["live_top10_max_pct"] == 50.0


def test_lotto_explosive_direct_scout_allows_dog_like_newborn_with_tiny_size():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 13230,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 28,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 23658,
            "buys_m5": 215,
            "sells_m5": 187,
            "price_change_m5": 515,
        },
        live_concentration={"top1_pct": 34.2, "top10_pct": 59.7},
        now=1004,
    )
    assert decision.allow is True
    assert decision.reason == "lotto_explosive_direct_scout_ok"
    assert decision.detail["entry_mode"] == "explosive_newborn_direct_scout"
    assert decision.detail["position_size_sol"] == 0.008

    pending = build_lotto_pending(
        {
            "ca": "DogToken",
            "symbol": "Dog",
            "signal_ts": 1000,
            "premium_signal_id": 1,
            "pool_address": "Pool",
            "id": 7,
        },
        "DogToken:1000",
        decision.detail,
    )
    assert pending["kelly_position_sol"] == 0.008
    assert pending["entry_mode"] == "explosive_newborn_direct_scout"
    assert pending["lotto_state"]["positionSizeSol"] == 0.008


def test_lotto_newborn_momentum_tiny_scout_arms_high_super_high_volume_token():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 37920,
            "signal_super": 98,
            "signal_holders": 80,
            "signal_vol24h": 40000,
            "signal_top10": 26,
        },
        dex_snapshot={
            "liquidity_usd": 15070,
            "vol_m5": 12000,
            "buys_m5": 138,
            "sells_m5": 100,
            "price_change_m5": 8.8,
        },
        live_concentration={"top1_pct": 19, "top10_pct": 43},
        now=1004,
    )

    assert decision.allow is True
    assert decision.reason == "lotto_newborn_momentum_tiny_scout_ok"
    assert decision.detail["entry_mode"] == "newborn_momentum_tiny_scout"
    assert decision.detail["position_size_sol"] == 0.005

    pending = build_lotto_pending(
        {
            "ca": "ILHAMRToken",
            "symbol": "ILHAMR",
            "signal_ts": 1000,
            "premium_signal_id": 1,
            "pool_address": "Pool",
            "id": 7,
        },
        "ILHAMRToken:1000",
        decision.detail,
    )
    assert pending["kelly_position_sol"] == 0.005
    assert pending["entry_mode"] == "newborn_momentum_tiny_scout"


def test_lotto_concentrated_scout_still_uses_small_size_when_not_explosive():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 13230,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 28,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 18000,
            "buys_m5": 170,
            "sells_m5": 160,
            "price_change_m5": 180,
        },
        live_concentration={"top1_pct": 34.2, "top10_pct": 59.7},
        now=1004,
    )
    assert decision.allow is True
    assert decision.reason == "lotto_concentrated_scout_ok"
    assert decision.detail["entry_mode"] == "lotto_concentrated_scout"
    assert decision.detail["position_size_sol"] == 0.015


def test_lotto_concentrated_scout_still_blocks_top1_too_high():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 11820,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 28,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 23658,
            "buys_m5": 215,
            "sells_m5": 187,
            "price_change_m5": 515,
        },
        live_concentration={"top1_pct": 41.5, "top10_pct": 63.0},
        now=1004,
    )
    assert decision.expire is True
    assert decision.reason == "lotto_live_top1_42pct"


def test_lotto_concentrated_scout_blocks_top10_above_tighter_default():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 13230,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 28,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 29800,
            "buys_m5": 350,
            "sells_m5": 283,
            "price_change_m5": 349,
        },
        live_concentration={"top1_pct": 34.2, "top10_pct": 62.5},
        now=1004,
    )
    assert decision.expire is True
    assert decision.reason == "lotto_live_top10_62pct"
    assert decision.detail["concentrated_scout_top10_max_pct"] == 60.0


def test_lotto_allows_pumpfun_liquidity_unknown_live_top10_under_risky_limit():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 10000,
            "signal_holders": 80,
            "signal_vol24h": 20000,
            "signal_top10": 45,
        },
        dex_snapshot={
            "liquidity_unknown": True,
            "dex_id": "pumpfun",
            "vol_m5": 2000,
            "buys_m5": 30,
            "sells_m5": 10,
        },
        live_concentration={"top1_pct": 25, "top10_pct": 46.8},
        now=1100,
    )
    assert decision.allow is True


def test_lotto_midcap_with_activity_is_observable_not_hard_expired():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 51520,
            "signal_holders": 414,
            "signal_vol24h": 208110,
            "signal_top10": 24,
        },
        dex_snapshot={
            "liquidity_usd": 16008,
            "vol_m5": 22891,
            "buys_m5": 122,
            "sells_m5": 134,
            "dex_id": "pumpswap",
        },
        live_concentration={"top1_pct": 17.8, "top10_pct": 42.3},
        now=1008,
    )
    assert decision.allow is True
    assert decision.detail["mc_tier"] == "newborn_midcap"


def test_lotto_midcap_uses_smaller_position_size():
    pending = build_lotto_pending(
        {
            "ca": "midcap_ca",
            "symbol": "MID",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "pool_address": "pool",
            "id": 7,
        },
        "life-1",
        {"mc_tier": "newborn_midcap"},
    )
    assert pending["kelly_position_sol"] == 0.03
    assert pending["lotto_state"]["positionSizeSol"] == 0.03


def test_lotto_midcap_without_activity_waits_instead_of_fast_lane():
    decision = evaluate_lotto_entry(
        {
            "added_at": 1000,
            "signal_mc": 60000,
            "signal_holders": 100,
            "signal_vol24h": 50000,
            "signal_top10": 25,
        },
        dex_snapshot={
            "liquidity_usd": 8000,
            "vol_m5": 1000,
            "buys_m5": 10,
            "sells_m5": 8,
            "dex_id": "pumpswap",
        },
        live_concentration={"top1_pct": 20, "top10_pct": 40},
        now=1008,
    )
    assert decision.action == "wait"
    assert decision.reason == "lotto_midcap_activity_unconfirmed"


def test_smart_entry_rejects_dead_cat_below_high():
    history = [
        (0, 1.00),
        (10, 0.761),
        (20, 0.83),
    ]
    position, detail = evaluate_entry_position(history, 0.83)
    assert position == "GOOD_ENTRY"
    reason = smart_entry_bounce_reject_reason(
        detail,
        entry_readiness_policy={
            "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
            "detail": {"route": "LOTTO"},
        },
        momentum_pct=1.0,
    )
    assert reason == "dead_cat_below_high_17.0pct_gt_10.0pct"


def test_smart_entry_rejects_risky_newborn_pullback_with_zero_m9s():
    reason = smart_entry_bounce_reject_reason(
        {"below_high_pct": 6.0},
        entry_readiness_policy={
            "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
            "detail": {"route": "LOTTO"},
        },
        momentum_pct=0.0,
    )
    assert reason == "risky_newborn_pullback_m9s_zero"


def test_entry_decision_contract_accounts_for_cost_after_odds():
    contract = build_entry_decision_contract(
        entry_readiness_policy={
            "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
            "min_p_follow": 0.68,
            "expected_upside_pct": 36.0,
            "expected_loss_pct": 12.0,
            "min_odds_r": 3.0,
        },
        entry_mode="smart_entry_pullback_bounce",
        p_follow=0.68,
        spread_cost_pct=3.0,
        exit_cost_buffer_pct=1.5,
    )
    assert contract.decision == "reject"
    assert contract.reason == "odds_after_cost_below_policy"


def test_profit_protect_floor_includes_exit_slippage_buffer():
    assert profit_protect_floor(0.079) is None
    assert round(profit_protect_floor(0.08), 4) == 0.067
    assert round(profit_protect_floor(0.141), 4) == 0.106
    assert round(profit_protect_floor(0.25), 4) == 0.165
    assert profit_protect_floor(0.50) is None


def test_lotto_profit_protect_fires_before_breakeven_floor():
    class Pos:
        entry_price = 1.0
        entry_ts = 1000
        peak_pnl = 0.141

    decision = evaluate_lotto_exit(Pos(), {"peak_pnl": 0.141}, 1.09, now=1030)
    assert decision["action"] == "exit"
    assert decision["reason"].startswith("lotto_profit_protect")
    assert round(decision["trail_floor"], 4) == 0.106


def test_matrix_phase0_uses_profit_protect_floor():
    evaluator = ExitMatrixEvaluator()
    entry = {
        "type": "MATRIX",
        "symbol": "TEST",
        "entry_price": 1.0,
        "peak_pnl": 0.141,
        "entry_time": 1000,
    }
    decision = evaluator.evaluate_exit(entry, 1.09)
    assert decision["action"] == "exit"
    assert decision["reason"].startswith("phase0_trail")
    assert round(decision["trail_floor"], 4) == 0.106


def test_ath_structural_reentry_can_bypass_last_exit_price_gate():
    allowed, reason = ath_structural_reentry_allowed(
        "ATH",
        {"trend": 100, "volume": 80, "signal": 60},
        current_price=0.90,
        last_exit_price=1.00,
    )
    assert allowed is True
    assert reason == "ath_structural_reentry_below_last_exit"


def test_ath_structural_reentry_rejects_weak_structure_below_last_exit():
    allowed, reason = ath_structural_reentry_allowed(
        "ATH",
        {"trend": 60, "volume": 80, "signal": 60},
        current_price=0.90,
        last_exit_price=1.00,
    )
    assert allowed is False
    assert reason == "reentry_trend_too_low"


def test_ath_flat_momentum_allowed_only_with_strong_structure():
    allowed, pct_move = ath_flat_momentum_allowed(
        "ATH",
        {"trend": 100, "volume": 80, "signal": 60},
        [1.0, 1.0001],
    )
    assert allowed is True
    assert abs(pct_move) <= 0.05

    weak_allowed, _ = ath_flat_momentum_allowed(
        "ATH",
        {"trend": 60, "volume": 80, "signal": 60},
        [1.0, 1.0001],
    )
    assert weak_allowed is False


def test_ath_flat_structure_tiny_scout_rescues_flat_high_price_structure():
    allowed, pct_move = ath_flat_structure_tiny_scout_allowed(
        "ATH",
        {"trend": 60, "volume": 70, "price": 80, "signal": 100},
        [1.0, 1.0001],
    )
    assert allowed is True
    assert abs(pct_move) <= 0.05

    flat_allowed, _ = ath_flat_momentum_allowed(
        "ATH",
        {"trend": 60, "volume": 70, "price": 80, "signal": 100},
        [1.0, 1.0001],
    )
    assert flat_allowed is True


def test_ath_flat_structure_tiny_scout_rescues_flat_seventy_price_structure():
    allowed, pct_move = ath_flat_structure_tiny_scout_allowed(
        "ATH",
        {"trend": 60, "volume": 70, "price": 70, "signal": 100},
        [1.0, 1.0],
    )
    assert allowed is True
    assert pct_move == 0


def test_ath_flat_structure_tiny_scout_rescues_low_price_when_volume_signal_are_maxed():
    allowed, pct_move = ath_flat_structure_tiny_scout_allowed(
        "ATH",
        {"trend": 60, "volume": 100, "price": 30, "signal": 100},
        [1.0, 1.0],
    )
    assert allowed is True
    assert pct_move == 0

    weak_allowed, _ = ath_flat_structure_tiny_scout_allowed(
        "ATH",
        {"trend": 60, "volume": 70, "price": 30, "signal": 100},
        [1.0, 1.0],
    )
    assert weak_allowed is False


def test_ath_readiness_allows_flat_structure_tiny_scout_mode():
    policy = evaluate_entry_readiness_policy(
        route="ATH",
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {"age_sec": 240},
        },
        pending={"entry_mode": "ath_flat_structure_tiny_scout"},
    )

    assert entry_mode_allowed("ath_flat_structure_tiny_scout", policy) is True
    assert policy.max_spread_pct == 1.0


def test_entry_readiness_sets_higher_odds_for_lotto_risky_newborn():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "NEWBORN_LAUNCH",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 90,
                "liquidity_unknown": True,
                "dex_id": "pumpfun",
                "price_change_m5": 45,
                "buy_sell_ratio": 1.5,
            },
        },
        pending={"is_lotto": True},
    )
    assert policy.decision == "ARM"
    assert policy.lifecycle_profile == "LOTTO_NEWBORN_RISKY"
    assert policy.min_odds_r == 3.0
    assert policy.min_p_follow >= 0.68
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True
    assert entry_mode_allowed("smart_entry", policy) is False


def test_entry_readiness_allows_only_tiny_explosive_direct_for_explicit_scout():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "NEWBORN_LAUNCH",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 4,
                "liquidity_unknown": True,
                "dex_id": "pumpfun",
                "price_change_m5": 515,
                "buy_sell_ratio": 1.15,
            },
        },
        pending={"is_lotto": True, "entry_mode": "explosive_newborn_direct_scout"},
    )
    assert policy.lifecycle_profile == "LOTTO_NEWBORN_RISKY"
    assert policy.min_p_follow >= 0.74
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("explosive_newborn_direct_scout", policy) is True
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_entry_readiness_allows_gmgn_tiny_scout_only_when_explicit():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "NEWBORN_LAUNCH",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 25,
                "liquidity_unknown": False,
                "dex_id": "pumpswap",
                "price_change_m5": 13.7,
                "buy_sell_ratio": 1.24,
            },
        },
        pending={"is_lotto": True, "entry_mode": "gmgn_concentration_tiny_scout"},
    )
    assert policy.lifecycle_profile == "LOTTO_NEWBORN_RISKY"
    assert policy.min_p_follow >= 0.72
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("gmgn_concentration_tiny_scout", policy) is True
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_entry_readiness_allows_newborn_momentum_tiny_scout_when_explicit():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "NEWBORN_LAUNCH",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 25,
                "liquidity_unknown": False,
                "dex_id": "pumpswap",
                "price_change_m5": 8.8,
                "buy_sell_ratio": 1.38,
            },
        },
        pending={"is_lotto": True, "entry_mode": "newborn_momentum_tiny_scout"},
    )
    assert policy.lifecycle_profile == "LOTTO_NEWBORN_RISKY"
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("newborn_momentum_tiny_scout", policy) is True
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_smart_entry_explosive_direct_scout_bypasses_chasing_top(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["explosive_newborn_direct_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.74,
        "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
        "detail": {"route": "LOTTO"},
    }
    trend = {
        "buys_m5": 215,
        "sells_m5": 187,
        "price_change_m5": 515,
        "vol_m5": 23658,
        "vol_h1": 12000,
        "fdv": 13230,
        "market_cap": 13230,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (True, "near_local_high"))
    monkeypatch.setattr(entry_engine_module, "evaluate_trend_phase", lambda *_args, **_kwargs: ("BULLISH", "explosive"))
    monkeypatch.setattr(entry_engine_module, "calculate_ema_deviation", lambda *_args, **_kwargs: (10.0, 0.9))
    monkeypatch.setattr(entry_engine_module, "get_recent_synthetic_bars", lambda *_args, **_kwargs: [])

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "DogToken",
        symbol="Dog",
        pool_address="Pool",
        entry_readiness_policy=policy,
    )
    assert should_enter is True
    assert reason == "explosive_newborn_direct_scout"
    assert "node=explosive_direct_scout" in detail
    assert trigger == 1.0


def test_smart_entry_newborn_momentum_tiny_scout_enters_without_waiting_for_pullback(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["newborn_momentum_tiny_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.72,
        "lifecycle_profile": "LOTTO_NEWBORN_RISKY",
        "detail": {"route": "LOTTO"},
        "gmgn_policy": {
            "action": "boost",
            "reason": "gmgn_clean_smart_money_boost",
            "toxic_score": 0,
            "edge_score": 6,
            "features": {
                "bundler_rate": 0.01,
                "rat_trader_amount_rate": 0.01,
                "entrapment_ratio": 0.01,
                "creator_hold_rate": 0.0,
                "dev_team_hold_rate": 0.0,
            },
        },
    }
    trend = {
        "buys_m5": 138,
        "sells_m5": 100,
        "price_change_m5": 8.8,
        "vol_m5": 12000,
        "vol_h1": 24000,
        "liquidity_usd": 15070,
        "fdv": 37920,
        "market_cap": 37920,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (False, ""))
    monkeypatch.setattr(entry_engine_module, "evaluate_trend_phase", lambda *_args, **_kwargs: ("BULLISH", "strong"))
    monkeypatch.setattr(entry_engine_module, "calculate_ema_deviation", lambda *_args, **_kwargs: (10.0, 0.9))
    monkeypatch.setattr(entry_engine_module, "get_recent_synthetic_bars", lambda *_args, **_kwargs: [])

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "ILHAMRToken",
        symbol="ILHAMR",
        pool_address="Pool",
        entry_readiness_policy=policy,
    )

    assert should_enter is True
    assert reason == "newborn_momentum_tiny_scout"
    assert "node=newborn_momentum_tiny_scout" in detail
    assert trigger == 1.0


def test_smart_entry_ath_flat_tiny_scout_bypasses_chasing_top_for_paper_probe(monkeypatch):
    import entry_engine as entry_engine_module
    import paper_trade_monitor as monitor_module

    policy = {
        "allowed_entry_modes": ["ath_flat_structure_tiny_scout", "smart_entry_pullback_bounce"],
        "min_p_follow": 0.62,
        "lifecycle_profile": "ATH_CONTINUATION",
        "detail": {"route": "ATH"},
    }
    trend = {
        "buys_m5": 137,
        "sells_m5": 100,
        "price_change_m5": 65.9,
        "vol_m5": 12000,
        "vol_h1": 24000,
        "liquidity_usd": 15070,
        "fdv": 54000,
        "market_cap": 54000,
    }

    monkeypatch.setattr(monitor_module, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "test", 0))
    monkeypatch.setattr(entry_engine_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: trend)
    monkeypatch.setattr(entry_engine_module, "is_chasing_top", lambda *_args, **_kwargs: (True, "extreme_chase"))
    monkeypatch.setattr(entry_engine_module, "evaluate_trend_phase", lambda *_args, **_kwargs: ("BULLISH", "strong"))
    monkeypatch.setattr(entry_engine_module, "calculate_ema_deviation", lambda *_args, **_kwargs: (10.0, 0.9))
    monkeypatch.setattr(entry_engine_module, "get_recent_synthetic_bars", lambda *_args, **_kwargs: [])

    should_enter, reason, detail, trigger = evaluate_smart_entry(
        "OOOToken",
        symbol="OOO",
        pool_address="Pool",
        entry_readiness_policy=policy,
    )

    assert should_enter is True
    assert reason == "ath_flat_structure_tiny_scout"
    assert "node=ath_flat_structure_tiny_scout" in detail
    assert trigger == 1.0


def test_entry_readiness_real_probe_disallows_momentum_direct():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 600,
                "price_change_m5": 18,
                "buy_sell_ratio": 1.7,
            },
        },
        pending={
            "is_lotto": True,
            "entry_mode": "lotto_real_probe_reentry_arm",
        },
    )
    assert policy.lifecycle_profile == "LOTTO_REAL_PROBE"
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_entry_readiness_upstream_miss_tiny_scout_uses_real_probe_profile():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 480,
                "price_change_m5": 22,
                "buy_sell_ratio": 1.8,
            },
        },
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_upstream_miss_tiny_scout",
            "replay_source": "live_monitor_lotto_upstream_probe",
        },
    )
    assert policy.lifecycle_profile == "LOTTO_REAL_PROBE"
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_entry_readiness_upstream_realtime_tiny_scout_uses_real_probe_profile():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 120,
                "price_change_m5": 18,
                "buy_sell_ratio": 1.5,
            },
        },
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_upstream_realtime_tiny_scout",
            "replay_source": "live_monitor_lotto_upstream_realtime",
        },
    )
    assert policy.lifecycle_profile == "LOTTO_REAL_PROBE"
    assert entry_mode_allowed("momentum_direct_entry", policy) is False
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_entry_readiness_discovery_lotto_probe_uses_real_probe_profile():
    policy = evaluate_entry_readiness_policy(
        route="LOTTO",
        lifecycle={
            "lifecycle_state": "FIRST_PUMP",
            "entry_bias": "PROBE",
            "lifecycle_features": {
                "age_sec": 180,
                "price_change_m5": 12,
                "buy_sell_ratio": 1.3,
            },
        },
        pending={
            "is_lotto": True,
            "paper_only_scout": True,
            "entry_mode": "lotto_high_risk_discovery_probe",
        },
    )
    assert policy.lifecycle_profile == "LOTTO_REAL_PROBE"
    assert entry_mode_allowed("lotto_high_risk_discovery_probe", policy) is False
    assert entry_mode_allowed("smart_entry_pullback_bounce", policy) is True


def test_ath_uncertainty_scout_arms_tiny_pending(monkeypatch):
    import paper_trade_monitor as monitor_module

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: {
        "market_cap": 55000,
        "liquidity_usd": 12000,
        "price_change_m5": 8,
        "vol_m5": 12000,
        "buys_m5": 72,
        "sells_m5": 48,
    })
    monkeypatch.setattr(monitor_module, "get_pool_address", lambda *_args, **_kwargs: "PoolA")

    pending_entries = {}
    armed = arm_ath_uncertainty_tiny_scout(
        db,
        pending_entries,
        {},
        w_entry={
            "id": 1,
            "ca": "AthToken",
            "symbol": "ATHX",
            "type": "ATH",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "signal_mc": 55000,
            "signal_top10": 20,
            "signal_price": 1.0,
        },
        lifecycle_id="AthToken:1000",
        eval_res={
            "action": "wait",
            "action_reason": "matrices not yet aligned",
            "current_price": 1.01,
            "scores": {"trend": 0, "volume": 70},
            "reasons": {},
        },
        now_ts=1200,
    )

    assert armed is True
    assert pending_entries["AthToken:1000"]["entry_mode"] == "ath_uncertainty_tiny_scout"
    assert pending_entries["AthToken:1000"]["kelly_position_sol"] == 0.003
    row = db.execute(
        "SELECT component, event_type, reason FROM paper_decision_events WHERE component = 'ath_uncertainty_scout'"
    ).fetchone()
    assert row["event_type"] == "pending_entry"


def test_ath_uncertainty_scout_rejects_low_quality_under_mc_cap(monkeypatch):
    import paper_trade_monitor as monitor_module

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: {
        "market_cap": 55000,
        "liquidity_usd": 12000,
        "price_change_m5": -4,
        "vol_m5": 3000,
        "buys_m5": 40,
        "sells_m5": 38,
    })
    monkeypatch.setattr(monitor_module, "get_pool_address", lambda *_args, **_kwargs: "PoolA")

    pending_entries = {}
    armed = arm_ath_uncertainty_tiny_scout(
        db,
        pending_entries,
        {},
        w_entry={
            "id": 1,
            "ca": "WeakAthToken",
            "symbol": "WATH",
            "type": "ATH",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "signal_mc": 55000,
            "signal_top10": 20,
            "signal_price": 1.0,
        },
        lifecycle_id="WeakAthToken:1000",
        eval_res={
            "action": "wait",
            "action_reason": "matrices not yet aligned",
            "current_price": 1.01,
            "scores": {"trend": 0, "volume": 40},
            "reasons": {},
        },
        now_ts=1200,
    )

    assert armed is False
    assert pending_entries == {}
    row = db.execute(
        "SELECT reason FROM paper_decision_events WHERE component = 'ath_uncertainty_scout'"
    ).fetchone()
    assert row["reason"] == "scout_quality_buy_pressure_weak"


def test_ath_uncertainty_soft_quality_reject_enters_discovery_tracking(monkeypatch):
    import paper_trade_monitor as monitor_module

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: {
        "market_cap": 55000,
        "liquidity_usd": 12000,
        "price_change_m5": -4,
        "vol_m5": 3000,
        "buys_m5": 40,
        "sells_m5": 38,
    })
    monkeypatch.setattr(monitor_module, "get_pool_address", lambda *_args, **_kwargs: "PoolA")

    discovery_candidates = {}
    armed = arm_ath_uncertainty_tiny_scout(
        db,
        {},
        {},
        w_entry={
            "id": 1,
            "ca": "WeakAthToken",
            "symbol": "WATH",
            "type": "ATH",
            "pool_address": "PoolA",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "signal_mc": 55000,
            "signal_top10": 20,
            "signal_price": 1.0,
        },
        lifecycle_id="WeakAthToken:1000",
        eval_res={
            "action": "wait",
            "action_reason": "matrices not yet aligned",
            "current_price": 1.01,
            "scores": {"trend": 0, "volume": 40},
            "reasons": {},
        },
        now_ts=1200,
        discovery_candidates=discovery_candidates,
    )

    assert armed is False
    assert len(discovery_candidates) == 1
    candidate = next(iter(discovery_candidates.values()))
    assert candidate["mode"] == "matrix_reclaim_tiny_probe"
    row = db.execute(
        "SELECT component, event_type, reason FROM paper_decision_events WHERE component = 'discovery_tracking'"
    ).fetchone()
    assert row["event_type"] == "candidate_tracked"
    assert row["reason"] == "matrices not yet aligned"


def test_discovery_tracking_arms_lotto_high_risk_probe(monkeypatch):
    import paper_trade_monitor as monitor_module

    class FakeWatchlist:
        def __init__(self):
            self.entry = {
                "id": 7,
                "ca": "BossToken",
                "symbol": "BOSS",
                "type": "LOTTO",
                "pool_address": "PoolB",
                "signal_ts": 1000,
                "premium_signal_id": 99,
                "signal_price": 1.0,
                "signal_mc": 42000,
                "signal_top10": 72,
                "added_at": 1000,
            }

        def get_by_id(self, entry_id):
            return dict(self.entry) if entry_id == self.entry["id"] else None

        def get_by_ca(self, ca):
            return dict(self.entry) if ca == self.entry["ca"] else None

        def register(self, **kwargs):
            self.entry.update({
                "ca": kwargs["ca"],
                "symbol": kwargs["symbol"],
                "type": kwargs["signal_type"],
                "pool_address": kwargs["pool_address"],
                "signal_ts": kwargs["signal_ts"],
                "premium_signal_id": kwargs.get("premium_signal_id"),
                "signal_mc": kwargs.get("signal_mc"),
                "signal_top10": kwargs.get("signal_top10"),
            })
            return dict(self.entry)

        def update_position_state(self, entry_id, **kwargs):
            self.entry.update(kwargs)

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: {
        "market_cap": 42000,
        "liquidity_usd": 12000,
        "price_change_m5": -3,
        "vol_m5": 6500,
        "buys_m5": 70,
        "sells_m5": 35,
        "pair_address": "PoolB",
    })
    monkeypatch.setattr(monitor_module, "get_pool_address", lambda *_args, **_kwargs: "PoolB")
    monkeypatch.setattr(monitor_module, "helius_token_concentration", lambda *_args, **_kwargs: {
        "top1_pct": 55,
        "top10_pct": 72,
    })
    monkeypatch.setattr(monitor_module, "fetch_gmgn_token_enrichment", lambda *_args, **_kwargs: None)

    watchlist = FakeWatchlist()
    discovery_candidates = {}
    track_discovery_candidate(
        db,
        discovery_candidates,
        mode="lotto_high_risk_discovery_probe",
        route="LOTTO",
        token_ca="BossToken",
        symbol="BOSS",
        lifecycle_id="BossToken:1000",
        signal_ts=1000,
        signal_id=99,
        pool="PoolB",
        watchlist_id=7,
        watchlist_entry=watchlist.entry,
        source_component="lotto_entry_gate",
        source_reject_reason="lotto_live_top1_55pct",
        now_ts=1200,
    )

    pending_entries = {}
    armed = process_discovery_tracking_candidates(
        db,
        watchlist,
        discovery_candidates,
        pending_entries,
        {},
        now_ts=1211,
        max_positions=10,
    )

    assert armed == 1
    assert discovery_candidates == {}
    pending = pending_entries["BossToken:1000"]
    assert pending["entry_mode"] == "lotto_high_risk_discovery_probe"
    assert pending["timing_passed"] is True
    assert pending["kelly_position_sol"] == 0.003
    assert pending["paper_only_scout"] is True


def test_ath_uncertainty_scout_allows_experimental_midcap_probe(monkeypatch):
    import paper_trade_monitor as monitor_module

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    monkeypatch.setattr(monitor_module, "fetch_dexscreener_trend_snapshot", lambda *_args, **_kwargs: {
        "market_cap": 350000,
        "liquidity_usd": 32000,
        "price_change_m5": 18,
        "vol_m5": 26000,
        "buys_m5": 180,
        "sells_m5": 110,
    })
    monkeypatch.setattr(monitor_module, "get_pool_address", lambda *_args, **_kwargs: "PoolA")

    pending_entries = {}
    armed = arm_ath_uncertainty_tiny_scout(
        db,
        pending_entries,
        {},
        w_entry={
            "id": 1,
            "ca": "MidAthToken",
            "symbol": "MATH",
            "type": "ATH",
            "signal_ts": 1000,
            "premium_signal_id": 42,
            "signal_mc": 350000,
            "signal_top10": 30,
            "signal_price": 1.0,
        },
        lifecycle_id="MidAthToken:1000",
        eval_res={
            "action": "wait",
            "action_reason": "matrices not yet aligned",
            "current_price": 1.01,
            "scores": {"trend": 0, "volume": 80},
            "reasons": {},
        },
        now_ts=1200,
    )

    assert armed is True
    assert pending_entries["MidAthToken:1000"]["entry_mode"] == "ath_uncertainty_tiny_scout"
    assert pending_entries["MidAthToken:1000"]["kelly_position_sol"] == 0.003


def test_spread_abort_memory_blocks_until_reclaim():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    record_decision_event(
        db,
        component="execution_guard",
        event_type="entry_abort",
        decision="abort",
        reason="entry_edge_spread_too_high",
        token_ca="TokenA",
        symbol="TKA",
        payload={"spread_pct": 3.2, "max_spread_pct": 1.5},
        event_ts=1000,
    )
    blocked = evaluate_spread_abort_memory(
        db,
        "TokenA",
        lifecycle={
            "lifecycle_features": {
                "price_change_m5": 2.0,
                "buy_sell_ratio": 1.1,
            }
        },
        current_spread_pct=0.4,
        max_spread_pct=1.0,
        now_ts=1060,
    )
    assert blocked["blocked"] is True
    assert blocked["abort_count"] == 1
    assert blocked["reason"] == "spread_abort_memory_wait_reclaim"

    reclaimed = evaluate_spread_abort_memory(
        db,
        "TokenA",
        lifecycle={
            "lifecycle_features": {
                "price_change_m5": 8.0,
                "buy_sell_ratio": 1.6,
            }
        },
        current_spread_pct=0.4,
        max_spread_pct=1.0,
        now_ts=1060,
    )
    assert reclaimed["blocked"] is False
    assert reclaimed["reason"] == "spread_abort_memory_reclaimed"


def test_entry_readiness_marks_stale_ath_as_wait_for_fresh_high():
    policy = evaluate_entry_readiness_policy(
        route="ATH",
        lifecycle={
            "lifecycle_state": "UNKNOWN",
            "entry_bias": "WAIT",
            "lifecycle_features": {
                "age_sec": 13 * 60 * 60,
                "ath_distance_pct": -0.25,
                "price_change_m5": 8,
                "buy_sell_ratio": 1.2,
            },
        },
        pending={"signal_ts": 1000},
        now_ts=1000 + 13 * 60 * 60,
    )
    assert policy.decision == "WAIT"
    assert policy.lifecycle_profile == "ATH_STALE"
    assert policy.reason == "entry_readiness_stale_ath_requires_fresh_high"
    assert policy.min_odds_r == 3.0


def test_entry_readiness_expires_distribution_lifecycle():
    policy = evaluate_entry_readiness_policy(
        route="ATH",
        lifecycle={
            "lifecycle_state": "DISTRIBUTION",
            "entry_bias": "REJECT",
            "lifecycle_features": {"age_sec": 600},
        },
        pending={},
    )
    assert policy.decision == "EXPIRE"
    assert policy.reason == "entry_readiness_bad_lifecycle"


def test_stage_diagnosis_separates_timing_and_execution_cost():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE rows (
            id INTEGER,
            symbol TEXT,
            signal_ts REAL,
            entry_ts REAL,
            exit_ts REAL,
            entry_price REAL,
            trigger_price REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            exit_reason TEXT,
            signal_type TEXT,
            signal_route TEXT,
            lifecycle_state TEXT,
            monitor_state_json TEXT,
            entry_execution_audit_json TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO rows
        VALUES (
            1, 'BAD', 1000, 1030, 1090, 1.03, 1.0, -0.12, 0.0,
            'guardian_lotto_no_follow_60s', 'LOTTO', 'LOTTO', 'FIRST_PUMP',
            '{"entrySpreadPct":3.0,"entryReadinessPolicy":{"decision":"ARM","lifecycle_profile":"LOTTO_NEWBORN_RISKY","max_spread_pct":1.5},"smartEntryReason":"legacy_score_pass"}',
            '{}'
        )
        """
    )
    row = db.execute("SELECT * FROM rows").fetchone()
    tags = classify_system_stage(row)
    assert "execution_cost_over_readiness_budget" in tags
    assert "selection_high_risk_low_follow" in tags
    assert "timing_zero_peak_entry" in tags
    assert "timing_not_confirmed_node" in tags


def test_records_scout_quality_pass_and_block_events():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)

    record_scout_quality_decision(
        db,
        scout_quality={
            "pass": True,
            "reason": "scout_quality_pass",
            "mode": "pullback_tiny_scout",
        },
        token_ca="TokenPass",
        symbol="PASS",
        lifecycle_id="TokenPass:1000",
        signal_ts=1000,
        route="LOTTO",
        event_ts=1100,
    )
    record_scout_quality_decision(
        db,
        scout_quality={
            "pass": False,
            "reason": "scout_quality_volume_low",
            "mode": "pullback_tiny_scout",
        },
        token_ca="TokenBlock",
        symbol="BLOCK",
        lifecycle_id="TokenBlock:1000",
        signal_ts=1000,
        route="LOTTO",
        event_ts=1101,
    )

    rows = db.execute(
        """
        SELECT decision, reason, event_type, payload_json
        FROM paper_decision_events
        WHERE component = 'scout_quality'
        ORDER BY event_ts
        """
    ).fetchall()
    assert [row["decision"] for row in rows] == ["pass", "block"]
    assert rows[0]["event_type"] == "quality_gate"
    assert rows[1]["reason"] == "scout_quality_volume_low"
    assert json.loads(rows[0]["payload_json"])["entry_mode"] == "pullback_tiny_scout"


def test_scout_funnel_summary_counts_candidate_to_fill_layers():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    payload = {"entry_mode": "gmgn_midcap_near_miss_scout"}
    common = {
        "token_ca": "TokenA",
        "symbol": "AAA",
        "lifecycle_id": "TokenA:1000",
        "signal_ts": 1000,
        "route": "LOTTO",
    }

    record_decision_event(
        db,
        component="lotto_entry_gate",
        event_type="scout_candidate",
        decision="candidate",
        reason="gmgn_midcap_near_miss_scout_ok",
        payload=payload,
        event_ts=1100,
        **common,
    )
    record_decision_event(
        db,
        component="lotto_entry_gate",
        event_type="pending_entry",
        decision="pending",
        reason="gmgn_midcap_near_miss_scout_ok",
        payload=payload,
        event_ts=1101,
        **common,
    )
    record_scout_quality_decision(
        db,
        scout_quality={
            "pass": True,
            "reason": "scout_quality_pass",
            "mode": "gmgn_midcap_near_miss_scout",
        },
        event_ts=1102,
        **common,
    )
    record_decision_event(
        db,
        component="smart_entry",
        event_type="timing_decision",
        decision="pass",
        reason="legacy_score_pass",
        payload={"detail": {}},
        event_ts=1103,
        **common,
    )
    record_decision_event(
        db,
        component="execution_api",
        event_type="entry_quote",
        decision="filled_paper",
        reason="entry_quote_success",
        payload={"position_size_sol": 0.003},
        event_ts=1104,
        **common,
    )

    summary = record_scout_funnel_summary(db, now_ts=1200, lookback_sec=1000)
    mode_summary = summary["by_mode"][0]
    assert mode_summary["entry_mode"] == "gmgn_midcap_near_miss_scout"
    assert mode_summary["candidate_n"] == 1
    assert mode_summary["quality_pass_n"] == 1
    assert mode_summary["pending_n"] == 1
    assert mode_summary["smart_entry_pass_n"] == 1
    assert mode_summary["quote_success_n"] == 1
    assert mode_summary["fill_per_candidate_pct"] == 100.0


def test_upstream_miss_chain_summary_links_not_ath_to_downstream_block():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
             component, decision, reject_reason, baseline_price, baseline_ts,
             tradable_missed, tradability_status)
        VALUES (?, ?, ?, ?, ?, 'LOTTO', 'upstream_gate', 'reject',
                'not_ath_v17', 1.0, ?, 1, 'tradable_reclaim')
        """,
        (1000, "TokenNA", "NATH", "TokenNA:900", 900, 900),
    )
    db.commit()
    record_decision_event(
        db,
        component="scout_quality",
        event_type="quality_gate",
        decision="block",
        reason="scout_quality_volume_low",
        token_ca="TokenNA",
        symbol="NATH",
        lifecycle_id="TokenNA:900",
        signal_ts=900,
        route="LOTTO",
        payload={"entry_mode": "lotto_upstream_miss_tiny_scout"},
        event_ts=1030,
    )

    summary = record_upstream_miss_chain_summary(db, now_ts=1200, lookback_sec=1000)
    assert summary["source_count"] == 1
    assert summary["source_reasons"]["not_ath_v17"]["scout_quality:scout_quality_volume_low"] == 1
    assert summary["samples"][0]["terminal"] == "scout_quality:scout_quality_volume_low"


def run_tests():
    tests = [
        obj for name, obj in sorted(globals().items())
        if name.startswith("test_") and callable(obj)
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
