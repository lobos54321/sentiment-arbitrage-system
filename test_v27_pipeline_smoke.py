import json
import sqlite3
import sys

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit, record_decision_event  # noqa: E402
from v27_pipeline_smoke import run_pipeline_smoke  # noqa: E402


def create_signal_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            created_at TEXT,
            parse_status TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE signal_features (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            chain TEXT,
            symbol TEXT,
            entry_price REAL,
            max_gain_24h REAL,
            is_gold_dog INTEGER DEFAULT 0,
            is_silver_dog INTEGER DEFAULT 0,
            captured_at TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO premium_signals (id, token_ca, symbol, created_at, parse_status) VALUES (?, ?, ?, ?, ?)",
        (1, "TokenPipe", "PIPE", "2026-01-15 00:00:00", "parsed"),
    )
    db.execute(
        """
        INSERT INTO signal_features
            (id, token_ca, chain, symbol, entry_price, max_gain_24h, captured_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "TokenPipe", "SOL", "PIPE", 0.001, 125.0, "2026-01-15 00:01:00"),
    )
    db.commit()
    return db


def create_paper_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            premium_signal_id INTEGER,
            signal_ts INTEGER,
            entry_price REAL,
            entry_ts INTEGER,
            exit_price REAL,
            exit_ts INTEGER,
            execution_availability TEXT,
            peak_pnl REAL,
            position_size_sol REAL,
            entry_execution_audit_json TEXT,
            exit_execution_audit_json TEXT,
            monitor_state_json TEXT,
            entry_mode TEXT,
            signal_route TEXT
        )
        """
    )
    record_decision_event(
        db,
        component="unit_gate",
        event_type="decision",
        decision="shadow",
        reason="pipeline_smoke",
        token_ca="TokenPipe",
        symbol="PIPE",
        route="unit_route",
        data_source="unit",
        payload={"score": 0.5},
        event_ts=1_700_000_000,
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (decision_event_id, created_event_ts, token_ca, symbol, component,
             decision, baseline_price, tradable_peak_pnl, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (999, 1_700_000_010, "TokenPipe", "PIPE", "unit_gate", "skip", 0.001, 0.75, "resolved"),
    )
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, premium_signal_id, signal_ts, entry_price, entry_ts,
             exit_price, exit_ts, execution_availability, peak_pnl, position_size_sol,
             entry_execution_audit_json, exit_execution_audit_json, monitor_state_json,
             entry_mode, signal_route)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "TokenPipe",
            "PIPE",
            1,
            1_700_000_000_000,
            0.001,
            1_700_000_004_000,
            0.0012,
            1_700_000_300_000,
            "available",
            1.25,
            0.01,
            json.dumps(
                {
                    "success": True,
                    "quoteTs": 1_700_000_002_000,
                    "effectivePrice": 0.001,
                    "slippageBps": 25,
                    "inputMint": "SOL",
                    "outputMint": "TokenPipe",
                    "entryLatencyAudit": {
                        "signal_to_quote_latency_ms": 2000,
                        "quote_spread_pct": 0.25,
                    },
                }
            ),
            json.dumps(
                {
                    "success": True,
                    "quoteTs": 1_700_000_300_000,
                    "effectivePrice": 0.0012,
                    "slippageBps": 18,
                    "inputMint": "TokenPipe",
                    "outputMint": "SOL",
                    "quoteFreshness": {
                        "quote_ts": 1_700_000_300,
                        "now_ts": 1_700_000_301,
                        "quote_age_sec": 1,
                    },
                }
            ),
            json.dumps(
                {
                    "entryExecutionEligibility": {
                        "observed": {
                            "liquidity_usd": 12345,
                        }
                    },
                    "signalRoute": "unit_signal",
                }
            ),
            "unit_entry",
            "unit_signal",
        ),
    )
    db.commit()
    return db


def create_lifecycle_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            signal_ts REAL,
            entry_price REAL,
            entry_ts REAL,
            pool_address TEXT,
            status TEXT
        )
        """
    )
    db.execute(
        """
        INSERT INTO tracks
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "TokenPipe", "PIPE", 1_700_000_000, 0.001, 1_700_000_003, "PoolPipe", "active"),
    )
    db.commit()
    return db


def test_pipeline_smoke_runs_mirrors_and_refreshes_read_model(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=tmp_path / "read_models",
            limit=1,
            include_missed=True,
        )

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["event_log_verify"]["event_count"] == 5
    assert report["refresh"]["health"]["status"] == "read_model_refresh_ok"
    assert report["refresh"]["read_model_seq"] == report["event_log_verify"]["last_global_seq"]
    assert report["steps"]["telegram_signals"]["ok"] is True
    assert report["steps"]["source_labels"]["ok"] is True
    assert report["steps"]["paper_decisions"]["ok"] is True
    assert report["steps"]["lifecycle_tracks"]["ok"] is True


def test_pipeline_smoke_can_seed_d0_from_paper_trade_source_labels(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_paper_trade_source_labels=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["event_log_verify"]["event_count"] == 5
    assert report["steps"]["paper_trade_source_labels"]["ok"] is True
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["health"]["signal_credit_assignment_ok"] is True
    assert projection["health"]["reference_price_ok"] is True


def test_pipeline_smoke_can_seed_trade_outcome_and_standardized_stop(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_trade_outcomes=True,
            include_standardized_stops=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["trade_outcomes"]["ok"] is True
    assert report["steps"]["standardized_stops"]["ok"] is True
    assert projection["health"]["trade_outcome_label_ok"] is True
    assert projection["health"]["standardized_stop_ok"] is True
    assert "TradeOutcomeLabelContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "StandardizedStopContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]


def test_pipeline_smoke_can_seed_ex_ante_feasibility(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_trade_outcomes=True,
            include_standardized_stops=True,
            include_ex_ante_feasibility=True,
            include_earliest_actionable_times=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["ex_ante_feasibility"]["ok"] is True
    assert report["steps"]["earliest_actionable_times"]["ok"] is True
    assert projection["health"]["ex_ante_feasibility_ok"] is True
    assert projection["health"]["earliest_actionable_time_ok"] is True
    assert projection["contract_evidence"]["ExAnteFeasibility"]["future_leakage_count"] == 0
    assert projection["contract_evidence"]["EarliestActionableTime"]["invariant_violation_count"] == 0
    assert "TradeOutcomeLabelContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "StandardizedStopContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "ExAnteFeasibility" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "EarliestActionableTime" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]


def test_pipeline_smoke_can_seed_realtime_clean_detector(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_realtime_clean=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["realtime_clean"]["ok"] is True
    assert projection["realtime_clean_detector_recorded_events"] == 1
    assert projection["health"]["realtime_clean_detector_ok"] is True
    assert projection["contract_evidence"]["RealtimeCleanDetector"]["realtime_clean_observed_count"] == 1
    assert "RealtimeCleanDetector" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]


def test_pipeline_smoke_can_seed_quote_intent_binding_contract(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_realtime_clean=True,
            include_quote_intent_bindings=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["quote_intent_bindings"]["ok"] is True
    assert projection["quote_intent_binding_recorded_events"] == 1
    assert projection["health"]["quote_intent_binding_ok"] is True
    assert projection["contract_evidence"]["QuoteIntentBindingContract"]["quote_intent_bound_count"] == 1
    assert "RealtimeCleanDetector" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "QuoteIntentBindingContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]


def test_pipeline_smoke_can_seed_idempotency_contracts(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_realtime_clean=True,
            include_quote_intent_bindings=True,
            include_idempotency_contracts=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["idempotency_contracts"]["ok"] is True
    assert projection["idempotency_contract_recorded_events"] == 1
    assert projection["health"]["idempotency_contract_ok"] is True
    assert projection["health"]["idempotency_key_namespace_ok"] is True
    assert projection["contract_evidence"]["IdempotencyContract"]["eligible_idempotency_records"] == 1
    assert projection["contract_evidence"]["IdempotencyContract"]["idempotency_collision_count"] == 0
    assert projection["contract_evidence"]["IdempotencyContract"]["duplicate_action_conflict_count"] == 0
    assert projection["contract_evidence"]["IdempotencyKeyNamespaceContract"]["namespace_policy_violation_count"] == 0
    assert "QuoteIntentBindingContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "IdempotencyContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "IdempotencyKeyNamespaceContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]


def test_pipeline_smoke_can_seed_execution_control_contracts(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    signal_db = tmp_path / "signals.db"
    paper_db = tmp_path / "paper.db"
    lifecycle_db = tmp_path / "lifecycle.db"
    output_dir = tmp_path / "read_models"
    with create_signal_db(signal_db), create_paper_db(paper_db), create_lifecycle_db(lifecycle_db):
        report = run_pipeline_smoke(
            signal_db=signal_db,
            paper_db=paper_db,
            lifecycle_db=lifecycle_db,
            event_log_dir=tmp_path / "events",
            output_dir=output_dir,
            limit=1,
            include_missed=False,
            include_realtime_clean=True,
            include_quote_intent_bindings=True,
            include_idempotency_contracts=True,
            include_execution_control=True,
        )

    projection = json.loads((output_dir / "denominator_projection.json").read_text(encoding="utf-8"))

    assert report["health"]["status"] == "v27_pipeline_smoke_ok"
    assert report["blocking_reasons"] == []
    assert report["steps"]["execution_control"]["ok"] is True
    assert projection["execution_control_recorded_events"] == 1
    assert projection["health"]["execution_lease_ok"] is True
    assert projection["health"]["state_version_fencing_ok"] is True
    assert projection["health"]["entry_execution_state_machine_ok"] is True
    assert projection["contract_evidence"]["ExecutionLeaseContract"]["lease_violation_count"] == 0
    assert projection["contract_evidence"]["StateVersionFencing"]["fencing_violation_count"] == 0
    assert projection["contract_evidence"]["EntryExecutionStateMachine"]["state_machine_violation_count"] == 0
    assert "IdempotencyContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "IdempotencyKeyNamespaceContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "ExecutionLeaseContract" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "StateVersionFencing" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
    assert "EntryExecutionStateMachine" not in report["refresh"]["mode_readiness"]["blocking_contracts"]["ultra_tiny"]
