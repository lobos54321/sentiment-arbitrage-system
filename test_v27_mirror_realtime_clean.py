import json
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_realtime_clean import (  # noqa: E402
    DEFAULT_CLEAN_STANDARD_VERSION,
    mirror_realtime_clean_detector,
    run_mirror_once,
    verify_realtime_clean_mirror_parity,
)


def new_paper_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT NOT NULL,
            symbol TEXT,
            signal_ts INTEGER,
            entry_price REAL NOT NULL,
            entry_ts INTEGER NOT NULL,
            exit_price REAL,
            exit_ts INTEGER,
            premium_signal_id INTEGER,
            lifecycle_id TEXT,
            execution_availability TEXT,
            entry_mode TEXT,
            signal_route TEXT,
            position_size_sol REAL,
            entry_execution_audit_json TEXT,
            exit_execution_audit_json TEXT,
            monitor_state_json TEXT
        )
        """
    )
    db.commit()
    return db


def new_signal_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            chain TEXT,
            canonical_pool_group TEXT,
            lifecycle_epoch INTEGER,
            created_at INTEGER,
            parse_status TEXT
        )
        """
    )
    db.commit()
    return db


def entry_audit(success=True):
    return json.dumps(
        {
            "success": success,
            "quoteTs": 1_700_000_002_000,
            "effectivePrice": 0.001,
            "slippageBps": 25,
            "inputMint": "SOL",
            "outputMint": "TokenClean",
            "entryLatencyAudit": {
                "signal_to_quote_latency_ms": 2000,
                "quote_spread_pct": 0.25,
            },
        }
    )


def exit_audit(success=True):
    return json.dumps(
        {
            "success": success,
            "quoteTs": 1_700_000_300_000,
            "effectivePrice": 0.0012,
            "slippageBps": 18,
            "inputMint": "TokenClean",
            "outputMint": "SOL",
            "quoteFreshness": {
                "quote_ts": 1_700_000_300,
                "now_ts": 1_700_000_301.5,
                "quote_age_sec": 1.5,
            },
        }
    )


def monitor_state():
    return json.dumps(
        {
            "entryExecutionEligibility": {
                "observed": {
                    "liquidity_usd": 12345,
                }
            },
            "signalRoute": "unit_signal",
        }
    )


def insert_trade(db, trade_id=1, execution_availability="available"):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, exit_price,
             exit_ts, premium_signal_id, lifecycle_id, execution_availability,
             entry_mode, signal_route, position_size_sol, entry_execution_audit_json,
             exit_execution_audit_json, monitor_state_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenClean",
            "CLEAN",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
            0.0012,
            1_700_000_300,
            10,
            "legacy-pool",
            execution_availability,
            "unit_entry",
            "unit_signal",
            0.01,
            entry_audit(),
            exit_audit(),
            monitor_state(),
        ),
    )
    db.commit()


def insert_signal(db):
    db.execute(
        """
        INSERT INTO premium_signals
            (id, token_ca, symbol, chain, canonical_pool_group, lifecycle_epoch, created_at, parse_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (10, "TokenClean", "CLEAN", "solana", "pool-clean", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_realtime_clean_mirror_records_clean_and_dirty_round_trip_evidence(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, execution_availability="unavailable")
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_realtime_clean_detector(paper_db, signal_db, event_log_dir)
    second = mirror_realtime_clean_detector(paper_db, signal_db, event_log_dir)
    parity = verify_realtime_clean_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    clean_payload = events[0]["payload"]
    dirty_payload = events[1]["payload"]
    assert events[0]["event_type"] == "realtime_clean_detector_recorded"
    assert events[0]["idempotency_key"] == f"realtime_clean_detector:1:{DEFAULT_CLEAN_STANDARD_VERSION}"
    assert clean_payload["clean_standard_version"] == DEFAULT_CLEAN_STANDARD_VERSION
    assert clean_payload["clean_observation_type"] == "TRADABLE_CLEAN_OBSERVED"
    assert clean_payload["realtime_clean"] is True
    assert clean_payload["quote_source"] == "paper_trade_round_trip_quote"
    assert clean_payload["quote_age_sec"] == 2.0
    assert clean_payload["entry_quote_available"] is True
    assert clean_payload["exit_quote_available"] is True
    assert clean_payload["decision_available_at"].endswith("Z")
    assert dirty_payload["clean_observation_type"] == "QUOTE_DIRTY_OBSERVED"
    assert dirty_payload["realtime_clean"] is False
    assert dirty_payload["execution_availability"] == "unavailable"


def test_realtime_clean_new_only_uses_versioned_event_log_cursor(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    args = SimpleNamespace(
        paper_db=str(paper_db),
        signal_db=str(signal_db),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        dry_run=False,
        table="paper_trades",
        signal_table="premium_signals",
        default_chain="solana",
        clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION,
        quote_source="paper_trade_round_trip_quote",
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
