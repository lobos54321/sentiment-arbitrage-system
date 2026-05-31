import json
import sqlite3

import pytest

from scripts.strategy_reflection_runner import (
    build_single_variable_recommendation,
    run_reflection,
    validate_one_variable_change,
)
from scripts.strategy_reflection_score import evaluate_strategy_window, load_simple_yaml


def _goal_file(tmp_path):
    path = tmp_path / "strategy-goal.yaml"
    path.write_text(
        """
schema_version: v1.strategy_reflection_goal
business_goal:
  target_capture_rate_gold_silver: 0.55
  target_winner_clean_quote_recall: 0.60
  target_peak30_capture: 0.30
risk_limits:
  max_drawdown_pct: 8
  min_sample_n_before_promotion: 30
reflection:
  one_variable_only: true
  shadow_first: true
candidate_scope:
  default_entry_mode: lotto_not_ath_reclaim_tiny_probe
  allowed_variables: [markov_green_required, stale_reclaim_max_age_sec, mc0_gate_fallback_enabled, position_size_sol]
""",
        encoding="utf-8",
    )
    return path


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT,
            symbol TEXT,
            entry_ts REAL,
            exit_ts REAL,
            peak_pnl REAL,
            pnl_pct REAL,
            entry_mode TEXT,
            signal_type TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            component TEXT,
            decision TEXT,
            reject_reason TEXT,
            tradability_status TEXT,
            tradability_reason TEXT,
            tradable_peak_pnl REAL,
            max_pnl_recorded REAL,
            payload_json TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL,
            component TEXT,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            token_ca TEXT,
            symbol TEXT,
            payload_json TEXT
        )
        """
    )
    return db


def _insert_trade(db, token, peak, pnl, ts=1_000_000, mode="lotto_not_ath_reclaim_tiny_probe"):
    db.execute(
        """
        INSERT INTO paper_trades(token_ca, symbol, entry_ts, exit_ts, peak_pnl, pnl_pct, entry_mode, signal_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (token, token[:4], ts, ts + 60, peak, pnl, mode, "LOTTO"),
    )


def _insert_missed(db, token, peak, blocker="tracking_ttl_expired", ts=1_000_100, quote_clean=True):
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution(
            created_event_ts, token_ca, symbol, component, decision, reject_reason,
            tradability_status, tradability_reason, tradable_peak_pnl, max_pnl_recorded, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            token,
            token[:4],
            "lotto_entry_gate",
            "reject",
            blocker,
            "tradable" if quote_clean else "untradable",
            "quote clean" if quote_clean else "quote dirty",
            peak,
            peak,
            json.dumps({"quote_clean": quote_clean, "quote_gap_pct": 1.2 if quote_clean else 44.0}),
        ),
    )


def _insert_markov_advice(db, bucket="green", ts=1_000_150):
    db.execute(
        """
        INSERT INTO paper_decision_events(event_ts, component, event_type, decision, reason, token_ca, symbol, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            "markov_position_advice",
            "shadow_decision",
            "hold_patiently",
            "markov_position_green_runner_bias",
            "TokenAdvice",
            "DOG",
            json.dumps({"markov_bucket": bucket, "p_absorb_peak30": 0.42, "p_absorb_stop_before_peak": 0.08}),
        ),
    )


def test_goal_loader_handles_nested_mapping_and_inline_lists(tmp_path):
    goal = load_simple_yaml(_goal_file(tmp_path))

    assert goal["reflection"]["one_variable_only"] is True
    assert goal["candidate_scope"]["allowed_variables"] == [
        "markov_green_required",
        "stale_reclaim_max_age_sec",
        "mc0_gate_fallback_enabled",
        "position_size_sol",
    ]


def test_strategy_score_finds_clean_missed_medal_gap(tmp_path):
    db = _db()
    goal = load_simple_yaml(_goal_file(tmp_path))
    _insert_trade(db, "CapturedBronze", peak=0.30, pnl=0.04)
    _insert_missed(db, "MissedGold", peak=1.20)
    _insert_missed(db, "MissedSilver", peak=0.65)
    _insert_markov_advice(db)

    result = evaluate_strategy_window(db, goal, hours=24, now_ts=1_000_500)

    assert result["trades"]["captured_medal_unique"] == 1
    assert result["missed_winners"]["missed_clean_medal_unique"] == 2
    assert result["missed_winners"]["missed_clean_gold_unique"] == 1
    assert result["markov_position_advice"]["by_bucket"]["green"] == 1
    assert result["status"] == "insufficient_closed_trades"


def test_recommendation_targets_stale_reclaim_with_one_variable(tmp_path):
    db = _db()
    goal = load_simple_yaml(_goal_file(tmp_path))
    for idx in range(6):
        _insert_trade(db, f"Captured{idx}", peak=0.05, pnl=-0.01, ts=1_000_000 + idx)
    _insert_missed(db, "MissedGold", peak=1.20, blocker="tracking_ttl_expired")
    _insert_missed(db, "MissedSilver", peak=0.65, blocker="lotto_stale_2174s")

    scoreboard = evaluate_strategy_window(db, goal, hours=24, now_ts=1_000_500)
    reco = build_single_variable_recommendation(scoreboard, goal)

    assert reco["action"] == "candidate_proposed"
    assert reco["one_variable_changed"] == "markov_green_required"
    validate_one_variable_change(reco, goal)


def test_one_variable_validator_rejects_multi_variable_candidate(tmp_path):
    goal = load_simple_yaml(_goal_file(tmp_path))

    with pytest.raises(ValueError):
        validate_one_variable_change(
            {
                "action": "candidate_proposed",
                "one_variable_changed": ["markov_green_required", "position_size_sol"],
            },
            goal,
        )


def test_reflection_runner_writes_scoreboard_candidate_and_hypothesis(tmp_path):
    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    memory_db = _db()
    memory_db.backup(db)
    _insert_trade(db, "CapturedBronze", peak=0.30, pnl=0.04)
    _insert_missed(db, "MissedGold", peak=1.20)
    db.commit()
    db.close()

    out_dir = tmp_path / "reflection"
    result = run_reflection(
        db_path=db_path,
        goal_path=_goal_file(tmp_path),
        out_dir=out_dir,
        hours=24,
        now_ts=1_000_500,
        write=True,
    )

    assert result["hypothesis"]["one_variable_only"] is True
    assert (out_dir / "scoreboard-latest.json").exists()
    assert (out_dir / "current_candidate.json").exists()
    lines = (out_dir / "hypotheses.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    logged = json.loads(lines[0])
    assert logged["id"] == result["hypothesis"]["id"]
