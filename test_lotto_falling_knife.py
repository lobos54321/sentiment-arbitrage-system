import sys
import sqlite3

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit  # noqa: E402
from lotto_engine import build_lotto_pending  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    evaluate_token_reclaim,
    find_lotto_real_probe_candidates,
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
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, exit_reason, replay_source, signal_route)
        VALUES ('TokenA', 'TOKA', 1000, -0.12, 'guardian_lotto_fast_fail_20s',
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
            exit_reason TEXT,
            replay_source TEXT,
            signal_route TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (token_ca, symbol, exit_ts, pnl_pct, exit_reason, replay_source, signal_route)
        VALUES ('TokenA', 'TOKA', 1000, -0.12, 'guardian_lotto_fast_fail_20s',
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


def run_tests():
    tests = [
        test_blocks_newborn_low_liq_m5_down_falling_knife,
        test_allows_newborn_low_liq_without_m5_downtrend,
        test_allows_non_newborn_even_when_low_liq_m5_down,
        test_real_probe_requires_tradable_reclaim_not_reason_text,
        test_lotto_lifecycle_blocks_deep_reset_reject,
        test_lotto_lifecycle_blocks_negative_m5,
        test_token_quarantine_blocks_recent_same_ca_failure,
        test_token_quarantine_requires_reclaim_after_cooldown,
        test_evaluate_token_reclaim_requires_current_strength,
        test_lotto_pending_defaults_to_timing_gate,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
