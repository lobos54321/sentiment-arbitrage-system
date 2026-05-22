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
