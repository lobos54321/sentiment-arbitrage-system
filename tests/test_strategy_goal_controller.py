import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from canonical_ledger import record_canonical_trade_entry, record_canonical_trade_exit
from strategy_goal_controller import build_strategy_goal_control_snapshot


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def closed_trade(db, trade_id, mode, pnl_pct, *, entry_ts=1_000, size=0.001):
    record_canonical_trade_entry(
        db,
        {
            "trade_id": trade_id,
            "token_ca": f"Token{trade_id}",
            "symbol": trade_id,
            "entry_ts": entry_ts,
            "entry_size_sol": size,
            "normalized_mode": mode,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
        },
    )
    record_canonical_trade_exit(
        db,
        trade_id,
        {
            "exit_ts": entry_ts + 60,
            "realized_exit_sol": size * (1 + pnl_pct),
            "exit_quote_source": "gmgn",
            "exit_route_available": True,
            "exit_quote_executable": True,
            "exit_reason": "unit",
        },
    )


def test_control_snapshot_disables_mode_after_single_trade_loss_breach():
    db = memory_db()
    closed_trade(db, "loss", "LOTTO_TINY_SCOUT", -0.25, entry_ts=1_700_000_000)

    snapshot = build_strategy_goal_control_snapshot(
        db,
        hours=24,
        min_sample_to_live=1,
        bandit_min_samples=99,
        generated_at=1_700_000_120,
    )
    actions = snapshot["controller_actions"]["actions"]

    assert any(
        action["mode"] == "LOTTO_TINY_SCOUT"
        and action["action"] == "DISABLE"
        and action["reason"] == "scorecard_single_trade_loss_breached"
        for action in actions
    )
    assert snapshot["runtime_overlay"]["can_trigger_trade"] is False


def test_control_snapshot_keeps_missed_security_hard_and_flags_soft_blockers():
    db = memory_db()
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            tradable_missed INTEGER,
            created_event_ts REAL,
            max_pnl_recorded REAL
        )
        """
    )
    db.executemany(
        """
        INSERT INTO paper_missed_signal_attribution (
            route, component, reject_reason, tradable_missed, created_event_ts, max_pnl_recorded
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("ATH", "scout_quality", "buy_pressure_weak", 1, 1_700_000_000, 1.2),
            ("LOTTO", "security", "creator_dump_red", 1, 1_700_000_000, 2.0),
        ],
    )

    snapshot = build_strategy_goal_control_snapshot(db, hours=24, generated_at=1_700_000_120)
    rows = snapshot["missed_dog_blocker_ranking"]["rows"]
    actions = snapshot["controller_actions"]["actions"]

    assert snapshot["counterfactual_replay"]["quote_clean_gold_silver_seen_count"] == 2
    assert any(row["recommendation"] == "allow_a_class_only" for row in rows)
    assert any(row["recommendation"] == "keep_hard_block" for row in rows)
    assert any(action["action"] == "ALLOW_A_CLASS_ONLY" for action in actions)


def test_control_snapshot_includes_shadow_only_bandit_allocation():
    db = memory_db()
    for idx in range(3):
        closed_trade(
            db,
            f"win-{idx}",
            "ATH_CONTINUATION",
            0.40,
            entry_ts=1_700_000_000 + idx,
        )

    snapshot = build_strategy_goal_control_snapshot(
        db,
        hours=24,
        min_sample_to_live=1,
        bandit_min_samples=1,
        generated_at=1_700_000_120,
    )
    bandit = snapshot["bandit_allocation"]
    actions = snapshot["controller_actions"]["actions"]

    assert bandit["advisory_only"] is True
    assert bandit["can_trigger_trade"] is False
    assert any(arm["arm"] == "ATH_CONTINUATION" and arm["recommended_max_size_sol"] > 0 for arm in bandit["arms"])
    assert any(
        action["mode"] == "ATH_CONTINUATION"
        and action["action"] == "TINY_CANARY"
        and action["reason"] == "bandit_advisory_positive_winsorized_reward"
        for action in actions
    )
