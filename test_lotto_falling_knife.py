import sys
import sqlite3

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
    ath_structural_reentry_allowed,
)
from entry_readiness_policy import evaluate_entry_readiness_policy, entry_mode_allowed  # noqa: E402
from analyze_trade_failure_attribution import classify_system_stage  # noqa: E402
from profit_protect_policy import profit_protect_floor  # noqa: E402
from entry_engine import evaluate_entry_position, smart_entry_bounce_reject_reason  # noqa: E402
from entry_decision_contract import build_entry_decision_contract  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    evaluate_entry_edge_budget,
    evaluate_spread_abort_memory,
    evaluate_token_reclaim,
    find_lotto_real_probe_candidates,
    normalize_price_age_ms,
    should_block_lotto_falling_knife,
    should_block_lotto_lifecycle_entry,
    token_quarantine_state,
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
