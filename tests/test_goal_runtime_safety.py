import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from entry_mode_quality import evaluate_entry_mode_quality
from exit_engine import _observation_probe_hard_sl
from v27_runtime_mode_gate import evaluate_runtime_mode_gate


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            entry_mode TEXT,
            entry_ts REAL,
            exit_ts REAL,
            peak_pnl REAL,
            pnl_pct REAL,
            replay_source TEXT
        )
        """
    )
    return db


def insert_closed_trade(db, *, entry_mode, pnl_pct, peak_pnl=0.0, entry_ts=1_700_000_000):
    db.execute(
        """
        INSERT INTO paper_trades (entry_mode, entry_ts, exit_ts, peak_pnl, pnl_pct, replay_source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (entry_mode, entry_ts, entry_ts + 60, peak_pnl, pnl_pct, "live_monitor"),
    )


def write_json(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_observation_probe_hard_sl_never_loosens_existing_stop_or_goal_cap():
    assert _observation_probe_hard_sl(-0.10, is_lotto_entry=False) == -0.10
    assert _observation_probe_hard_sl(-0.18, is_lotto_entry=True) == -0.18
    assert _observation_probe_hard_sl(-0.30, is_lotto_entry=True) == -0.20


def test_entry_mode_tail_loss_kill_switch_wins_over_force_live():
    db = memory_db()
    insert_closed_trade(db, entry_mode="unit_force_live_probe", pnl_pct=-0.25)

    decision = evaluate_entry_mode_quality(
        db,
        "unit_force_live_probe",
        now_ts=1_700_000_120,
        force_live=True,
    )

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_catastrophic_loss"
    assert decision["kill_switch"]["status"] == "tripped"


def test_strategy_goal_tiny_canary_requires_human_approval(tmp_path):
    readiness_path = tmp_path / "mode_readiness.json"
    controller_path = tmp_path / "strategy_goal_controller_actions.json"
    write_json(
        readiness_path,
        {
            "matrix_schema_version": "v2.7.0.mode_readiness.v1",
            "highest_allowed_mode": "ultra_tiny",
            "health": {"status": "ok", "ultra_tiny_ready": True},
            "modes": {"ultra_tiny": {"status": "allowed"}},
        },
    )
    write_json(
        controller_path,
        {
            "schema_version": "v1.strategy_goal_controller.runtime_overlay",
            "enforcement_enabled": True,
            "actions": [
                {
                    "mode": "A_CLASS_FASTLANE",
                    "action": "TINY_CANARY",
                    "size_sol": 0.001,
                    "requires_human_approval": True,
                }
            ],
        },
    )

    blocked = evaluate_runtime_mode_gate(
        entry_mode="a_class_fastlane",
        position_size_sol=0.001,
        mode_readiness_path=readiness_path,
        controller_actions_path=controller_path,
        env={
            "V27_RUNTIME_MODE_GATE_ENABLED": "true",
            "STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true",
        },
    )
    allowed = evaluate_runtime_mode_gate(
        entry_mode="a_class_fastlane",
        position_size_sol=0.001,
        mode_readiness_path=readiness_path,
        controller_actions_path=controller_path,
        env={
            "V27_RUNTIME_MODE_GATE_ENABLED": "true",
            "STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true",
            "STRATEGY_GOAL_CONTROLLER_HUMAN_APPROVED": "true",
        },
    )

    assert blocked["pass"] is False
    assert blocked["reason"] == "strategy_goal_controller_human_approval_required"
    assert allowed["pass"] is True
