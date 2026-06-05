import json
import os
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_fastlane import AClassCandidate, evaluate_a_class_fastlane
from canonical_ledger import (
    fetch_a_class_events,
    init_canonical_ledger,
    record_a_class_decision_event,
    record_canonical_trade_entry,
    record_canonical_trade_exit,
    record_canonical_trade_path_update,
)
from fastlane_config import load_a_class_config
from opportunity_events import record_opportunity_event


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_init_creates_ledger_tables():
    db = memory_db()

    init_canonical_ledger(db)

    tables = {
        row["name"]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert "canonical_trade_ledger" in tables
    assert "a_class_decision_events" in tables


def test_shadow_decision_event_does_not_create_trade_row():
    db = memory_db()
    candidate = AClassCandidate.from_mapping(
        {
            "token_ca": "TokenCA123",
            "symbol": "AFAST",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_source": "gmgn",
            "quote_age_sec": 5,
            "route_available": True,
            "liquidity_usd": 50_000,
            "spread_pct": 1,
            "gmgn_pre_seen": True,
            "gmgn_activity_fresh": True,
            "gmgn_last_seen_age_sec": 5,
            "source_resonance": True,
            "fresh_momentum": True,
            "fresh_ath_refresh": True,
            "ath_continuation": True,
        }
    )
    decision = evaluate_a_class_fastlane(candidate, now_ts=1_000, config=load_a_class_config({}))

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        stored_action="WOULD_ENTER",
        source_table="unit",
        source_id=1,
        now_ts=1_000,
    )

    events = fetch_a_class_events(db, limit=5)
    trade_count = db.execute("SELECT COUNT(*) AS n FROM canonical_trade_ledger").fetchone()["n"]
    opportunity = db.execute("SELECT * FROM opportunity_events").fetchone()
    path_sample = db.execute("SELECT * FROM opportunity_event_path_samples").fetchone()
    assert events[0]["action"] == "WOULD_ENTER"
    assert trade_count == 0
    assert opportunity["token_ca"] == "TokenCA123"
    assert opportunity["would_enter_a_class"] == 1
    assert opportunity["evidence_status"] == "quote_clean_executable"
    assert path_sample["quote_pnl_pct"] == 0
    assert path_sample["quote_clean"] == 1


def test_a_class_decision_event_persists_block_cause():
    db = memory_db()
    candidate = {
        "token_ca": "TokenInfra",
        "symbol": "INFRA",
        "route_bucket": "ATH",
        "quote_available": False,
        "quote_executable": False,
        "route_available": False,
        "data_confidence": "unknown",
    }
    decision = {
        "action": "BLOCK",
        "reason": "hard_prefilter_failed",
        "hard_blockers": ["quote_not_available", "route_unavailable"],
        "risk_detail": {},
    }

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        source_table="unit",
        source_id=10,
        now_ts=1_000,
    )

    row = db.execute(
        """
        SELECT block_cause, recoverability, blocker_classifications_json
        FROM a_class_decision_events
        WHERE token_ca = 'TokenInfra'
        """
    ).fetchone()
    assert row["block_cause"] == "INFRA"
    assert row["recoverability"] == "provider_or_evidence_recoverable"
    assert "quote_not_available" in row["blocker_classifications_json"]


def test_a_class_decision_event_persists_execution_evidence_columns():
    db = memory_db()
    candidate = AClassCandidate.from_mapping(
        {
            "token_ca": "TokenHydrated",
            "symbol": "HYD",
            "route_bucket": "ATH",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "quote_source": "jupiter_ultra_provider_hydrate",
            "quote_age_sec": 0,
            "route_available": True,
            "route_failure_reason": "provider_hydrated_route_ok",
            "data_confidence": "provider_hydrated_quote",
            "provider_hydrate_outcome": "success",
            "liquidity_usd": 50_000,
            "spread_pct": 1.2,
            "gmgn_pre_seen": True,
            "gmgn_activity_fresh": True,
            "gmgn_last_seen_age_sec": 5,
            "source_resonance": True,
            "fresh_momentum": True,
            "fresh_ath_refresh": True,
            "ath_continuation": True,
        }
    )
    decision = evaluate_a_class_fastlane(candidate, now_ts=1_000, config=load_a_class_config({}))

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        stored_action="WOULD_ENTER",
        source_table="unit",
        source_id=11,
        now_ts=1_000,
    )

    row = db.execute(
        """
        SELECT quote_available, quote_executable, quote_clean, route_available,
               quote_source, quote_age_sec, data_confidence, evidence_status,
               route_failure_reason, liquidity_usd, spread_pct,
               provider_hydrate_outcome, block_cause
        FROM a_class_decision_events
        WHERE token_ca = 'TokenHydrated'
        """
    ).fetchone()
    assert row["quote_available"] == 1
    assert row["quote_executable"] == 1
    assert row["quote_clean"] == 1
    assert row["route_available"] == 1
    assert row["quote_source"] == "jupiter_ultra_provider_hydrate"
    assert row["quote_age_sec"] == 0
    assert row["data_confidence"] == "provider_hydrated_quote"
    assert row["evidence_status"] == "quote_clean_executable"
    assert row["route_failure_reason"] == "provider_hydrated_route_ok"
    assert row["liquidity_usd"] == 50_000
    assert row["spread_pct"] == 1.2
    assert row["provider_hydrate_outcome"] == "success"
    assert row["block_cause"] == "UNKNOWN"


def test_entry_and_exit_use_sol_accounting():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-1",
            "token_ca": "TokenCA123",
            "symbol": "AFAST",
            "entry_ts": 1_000,
            "entry_size_sol": 0.001,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
            "is_a_class_fastlane": True,
            "a_class_grade": "A",
            "a_class_score": 75,
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-1",
        {
            "exit_ts": 1_060,
            "realized_exit_sol": 0.0018,
            "exit_quote_source": "gmgn",
            "exit_route_available": True,
            "exit_quote_executable": True,
            "exit_reason": "take_profit",
        },
    )

    row = db.execute(
        "SELECT realized_pnl_sol, realized_pnl_pct, time_held_sec FROM canonical_trade_ledger WHERE trade_id = 'trade-1'"
    ).fetchone()
    assert round(row["realized_pnl_sol"], 10) == 0.0008
    assert round(row["realized_pnl_pct"], 4) == 0.8
    assert row["time_held_sec"] == 60


def test_entry_records_soft_bypass_and_exit_flags_loss_cap_breach():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-soft",
            "token_ca": "TokenSoft",
            "entry_ts": 1_000,
            "entry_size_sol": 0.001,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
            "soft_quality_bypass_reason": "scout_quality_volume_low",
            "soft_bypass_volume_low": True,
            "soft_quality_bypass": {"reason": "scout_quality_volume_low"},
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-soft",
        {
            "exit_ts": 1_030,
            "realized_exit_sol": 0.00075,
            "exit_reason": "hard_stop",
            "loss_cap_pct": 0.20,
        },
    )

    row = db.execute(
        """
        SELECT soft_quality_bypass_reason, soft_bypass_volume_low,
               soft_quality_bypass_json, loss_cap_breach, loss_cap_pct,
               loss_cap_detail_json, outlier_reason
        FROM canonical_trade_ledger
        WHERE trade_id = 'trade-soft'
        """
    ).fetchone()
    assert row["soft_quality_bypass_reason"] == "scout_quality_volume_low"
    assert row["soft_bypass_volume_low"] == 1
    assert "scout_quality_volume_low" in row["soft_quality_bypass_json"]
    assert row["loss_cap_breach"] == 1
    assert row["loss_cap_pct"] == 0.20
    assert "realized_loss_cap_breach" in row["outlier_reason"]
    assert "realized_pnl_pct" in row["loss_cap_detail_json"]


def test_no_route_trapped_and_outlier_flags_are_recorded():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-2",
            "token_ca": "TokenCA456",
            "entry_ts": 1_000,
            "entry_size_sol": 0.002,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
        },
    )
    record_canonical_trade_exit(
        db,
        "trade-2",
        {
            "exit_ts": 1_030,
            "realized_exit_sol": 0,
            "exit_reason": "no_route",
            "no_route_flag": True,
            "trapped_flag": True,
            "outlier_flag": True,
            "outlier_reason": "route_disappeared",
            "accounting_source": "no_route_zero_exit",
        },
    )

    row = db.execute(
        "SELECT no_route_flag, trapped_flag, outlier_flag, outlier_reason, accounting_source FROM canonical_trade_ledger WHERE trade_id = 'trade-2'"
    ).fetchone()
    assert row["no_route_flag"] == 1
    assert row["trapped_flag"] == 1
    assert row["outlier_flag"] == 1
    assert "route_disappeared" in row["outlier_reason"]
    assert "realized_loss_cap_breach" in row["outlier_reason"]
    assert row["accounting_source"] == "no_route_zero_exit"


def test_path_update_keeps_best_quote_peak_and_first_positive_feedback():
    db = memory_db()

    record_canonical_trade_entry(
        db,
        {
            "trade_id": "trade-path",
            "token_ca": "TokenCA789",
            "entry_ts": 1_000,
            "entry_size_sol": 0.003,
            "entry_quote_source": "gmgn",
            "entry_route_available": True,
            "entry_quote_executable": True,
        },
    )
    record_opportunity_event(
        db,
        {
            "opportunity_key": "trade-path-opp",
            "event_ts": 1_000,
            "token_ca": "TokenCA789",
            "route_bucket": "ATH",
            "source_type": "unit",
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "route_available": True,
            "linked_trade_id": "trade-path",
        },
    )
    record_canonical_trade_path_update(
        db,
        "trade-path",
        {
            "updated_at": 1_020,
            "current_quote_pnl_pct": 0.12,
            "peak_quote_pnl_pct": 0.12,
        },
    )
    record_canonical_trade_path_update(
        db,
        "trade-path",
        {
            "updated_at": 1_050,
            "current_quote_pnl_pct": -0.04,
            "peak_quote_pnl_pct": 0.08,
            "max_drawdown_pct": -0.04,
        },
    )

    row = db.execute(
        """
        SELECT peak_quote_pnl_pct, peak_quote_pnl_sol, max_drawdown_pct,
               positive_feedback_seen, first_positive_feedback_sec,
               time_to_peak_sec
        FROM canonical_trade_ledger
        WHERE trade_id = 'trade-path'
        """
    ).fetchone()
    assert row["peak_quote_pnl_pct"] == 0.12
    assert round(row["peak_quote_pnl_sol"], 8) == 0.00036
    assert row["max_drawdown_pct"] == -0.04
    assert row["positive_feedback_seen"] == 1
    assert row["first_positive_feedback_sec"] == 20
    assert row["time_to_peak_sec"] == 20
    samples = db.execute(
        """
        SELECT sample_ts, quote_pnl_pct
        FROM opportunity_event_path_samples
        WHERE opportunity_key = 'trade-path-opp'
        ORDER BY sample_ts
        """
    ).fetchall()
    assert [(row["sample_ts"], row["quote_pnl_pct"]) for row in samples] == [
        (1_000, 0),
        (1_020, 0.12),
        (1_050, -0.04),
    ]


def test_a_class_migration_backfills_source_dedup_key_and_replaces_unique_source_index():
    db = memory_db()
    db.executescript(
        """
        CREATE TABLE a_class_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL NOT NULL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            route_bucket TEXT,
            normalized_mode TEXT,
            source_table TEXT,
            source_id INTEGER,
            source_component TEXT,
            source_reason TEXT,
            action TEXT NOT NULL,
            grade TEXT,
            size_sol REAL DEFAULT 0,
            score REAL DEFAULT 0,
            reason TEXT,
            hard_blockers_json TEXT,
            soft_notes_json TEXT,
            freshness_json TEXT,
            budget_json TEXT,
            risk_json TEXT,
            candidate_json TEXT,
            created_at REAL NOT NULL
        );
        CREATE UNIQUE INDEX idx_a_class_decision_source
          ON a_class_decision_events(source_table, source_id)
          WHERE source_table IS NOT NULL AND source_id IS NOT NULL;
        INSERT INTO a_class_decision_events (
            event_ts, token_ca, route_bucket, source_table, source_id,
            action, hard_blockers_json, created_at
        ) VALUES (1000, 'LegacyToken', 'ATH', 'legacy_source', 7, 'BLOCK', '["quote_not_executable"]', 1000);
        ALTER TABLE a_class_decision_events ADD COLUMN source_dedup_key TEXT;
        """
    )

    init_canonical_ledger(db)

    cols = {row["name"] for row in db.execute("PRAGMA table_info(a_class_decision_events)").fetchall()}
    assert {
        "source_dedup_key",
        "would_action",
        "expected_rr",
        "expected_rr_detail_json",
        "denominator_key",
        "discovery_exit_json",
    }.issubset(cols)
    row = db.execute("SELECT source_dedup_key FROM a_class_decision_events WHERE id = 1").fetchone()
    assert row["source_dedup_key"] == "legacy_source:7"
    indexes = {
        row["name"]: row["unique"]
        for row in db.execute("PRAGMA index_list(a_class_decision_events)").fetchall()
    }
    assert indexes["idx_a_class_decision_source"] == 0
    assert indexes["idx_a_class_decision_dedup"] == 1

    init_canonical_ledger(db)
    assert db.execute("SELECT COUNT(*) AS n FROM a_class_decision_events").fetchone()["n"] == 1


def test_a_class_writer_does_not_rerun_dedup_migration_after_index_exists():
    db = memory_db()
    candidate = {
        "token_ca": "TokenGuard",
        "symbol": "GUARD",
        "route_bucket": "ATH",
    }
    decision = {
        "action": "SHADOW",
        "grade": "REJECT",
        "size_sol": 0.0,
        "score": 0.0,
        "reason": "collecting_evidence",
        "hard_blockers": [],
        "soft_notes": [],
        "freshness_detail": {},
        "budget_detail": {},
        "risk_detail": {},
    }

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        source_table="guard_source",
        source_id=1,
        now_ts=1000,
    )
    assert db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_a_class_decision_dedup'"
    ).fetchone()

    statements = []
    db.set_trace_callback(lambda sql: statements.append(" ".join(sql.split()).lower()))
    try:
        record_a_class_decision_event(
            db,
            candidate=candidate,
            decision=decision,
            source_table="guard_source",
            source_id=2,
            now_ts=1001,
        )
        record_a_class_decision_event(
            db,
            candidate=candidate,
            decision=decision,
            source_table="guard_source",
            source_id=3,
            now_ts=1002,
        )
    finally:
        db.set_trace_callback(None)

    traced_sql = "\n".join(statements)
    assert "row_number() over" not in traced_sql
    assert "drop index if exists idx_a_class_decision_source" not in traced_sql
    assert "create unique index if not exists idx_a_class_decision_dedup" not in traced_sql
    assert "update a_class_decision_events set source_dedup_key = case" not in traced_sql


def test_a_class_upsert_updates_only_enrichment_and_preserves_safety_fields():
    db = memory_db()
    candidate = {
        "token_ca": "TokenSafe",
        "symbol": "SAFE",
        "route_bucket": "ATH",
    }
    first_decision = {
        "action": "BLOCK",
        "grade": "REJECT",
        "size_sol": 0.0,
        "score": 0.0,
        "reason": "hard_prefilter_failed",
        "hard_blockers": ["quote_not_executable"],
        "soft_notes": [],
        "freshness_detail": {},
        "budget_detail": {},
        "risk_detail": {"quote_executable": False},
    }
    second_decision = {
        "action": "ENTER",
        "grade": "A_PLUS",
        "size_sol": 0.003,
        "score": 95.0,
        "reason": "a_class_fastlane_pass",
        "hard_blockers": [],
        "soft_notes": [],
        "freshness_detail": {},
        "budget_detail": {},
        "risk_detail": {"quote_executable": True},
        "expected_rr_detail": {
            "denominator_key": "quote_clean_gold_silver_unique:1:2",
            "outlier_trimmed_would_rr": 3.25,
        },
    }

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=first_decision,
        stored_action="BLOCK",
        source_table="unit_source",
        source_id=42,
        now_ts=1000,
    )
    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=second_decision,
        stored_action="WOULD_ENTER",
        source_table="unit_source",
        source_id=42,
        now_ts=1001,
    )

    rows = db.execute("SELECT * FROM a_class_decision_events").fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "BLOCK"
    assert row["hard_blockers_json"] == '["quote_not_executable"]'
    assert row["risk_json"] == '{"quote_executable": false}'
    assert row["score"] == 0
    assert row["size_sol"] == 0
    assert row["would_action"] == "WOULD_ENTER"
    assert row["expected_rr"] == 3.25


def test_a_class_decision_event_records_matrix_rr_ai_and_bottom_ticket_fields():
    db = memory_db()
    candidate = SimpleNamespace(
        token_ca="TokenMatrix",
        symbol="MATRIX",
        route_bucket="ATH",
        source_component="unit",
        source_reason="matrix_test",
        opportunity_key="matrix:unit",
    )
    decision = SimpleNamespace(
        action="ENTER",
        grade="A",
        size_sol=0.001,
        score=84,
        reason="a_class_fastlane_pass",
        hard_blockers=[],
        soft_notes=["expected_rr_gate_passed"],
        freshness_detail={"freshness_sources": ["fresh_quote"]},
        budget_detail={},
        risk_detail={},
        expected_rr=3.0,
        expected_upside_pct=0.60,
        defined_risk_pct=0.20,
        bottom_ticket_size_sol=0.001,
        expected_rr_detail={"expected_rr": 3.0},
        matrix_detail={"matrix_version": "v1.a_class_18_cell", "matrix_grade": "A"},
        ai_review={"schema_version": "v1.ai_strategy_advisory.shadow_only", "ai_grade": "supportive"},
        principal_recovery_plan={"no_averaging_down": True},
        moonbag_plan={"keep_tail_after_moonbag": True},
    )

    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        stored_action="WOULD_ENTER",
        source_table="unit_source",
        source_id=10,
        now_ts=2000,
    )

    row = db.execute("SELECT * FROM a_class_decision_events").fetchone()
    assert row["expected_rr"] == 3.0
    assert row["expected_upside_pct"] == 0.60
    assert row["defined_risk_pct"] == 0.20
    assert row["bottom_ticket_size_sol"] == 0.001
    assert json.loads(row["matrix_json"])["matrix_grade"] == "A"
    assert json.loads(row["ai_review_json"])["ai_grade"] == "supportive"
    assert json.loads(row["principal_recovery_plan_json"])["no_averaging_down"] is True


def test_a_class_null_source_rows_dedup_by_token_route_time_bucket():
    db = memory_db()
    candidate = {
        "token_ca": "TokenNullSource",
        "symbol": "NULLSRC",
        "route_bucket": "LOTTO",
    }
    decision = {
        "action": "SHADOW",
        "grade": "REJECT",
        "size_sol": 0.0,
        "score": 0.0,
        "reason": "collecting_evidence",
        "hard_blockers": [],
        "soft_notes": [],
        "freshness_detail": {},
        "budget_detail": {},
        "risk_detail": {},
    }

    record_a_class_decision_event(db, candidate=candidate, decision=decision, now_ts=1500)
    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision={
            **decision,
            "expected_rr_detail": {
                "denominator_key": "quote_clean_gold_silver_unique:1500:1800",
                "outlier_trimmed_would_rr": 2.5,
            },
        },
        now_ts=1520,
    )

    rows = db.execute("SELECT source_dedup_key, expected_rr FROM a_class_decision_events").fetchall()
    assert len(rows) == 1
    assert rows[0]["source_dedup_key"] == "token:TokenNullSource:LOTTO:5"
    assert rows[0]["expected_rr"] == 2.5
