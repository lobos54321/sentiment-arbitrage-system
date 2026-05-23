import json
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_execution_control import (  # noqa: E402
    DEFAULT_CONTROL_VERSION,
    mirror_execution_controls,
    run_mirror_once,
    verify_execution_control_mirror_parity,
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
            premium_signal_id INTEGER,
            lifecycle_id TEXT,
            entry_mode TEXT,
            signal_route TEXT,
            entry_execution_audit_json TEXT,
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


def insert_trade(db, trade_id=1, *, token_ca="TokenLease", lifecycle_id="life-1", route="unit_signal"):
    entry_audit = {
        "success": True,
        "quoteTs": 1_700_000_003_000,
        "effectivePrice": 0.001,
        "entryLatencyAudit": {
            "fast_lane_claim_to_quote_latency_ms": 500,
        },
    }
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts,
             premium_signal_id, lifecycle_id, entry_mode, signal_route,
             entry_execution_audit_json, monitor_state_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            token_ca,
            "LEASE",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
            10,
            lifecycle_id,
            "unit_entry",
            route,
            json.dumps(entry_audit),
            json.dumps({"paperOnly": True}),
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
        (10, "TokenLease", "LEASE", "solana", "pool-lease", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_execution_control_mirror_records_lease_fencing_and_state_machine(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, lifecycle_id="life-2")
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_execution_controls(paper_db, signal_db, event_log_dir, environment_id="unit")
    second = mirror_execution_controls(paper_db, signal_db, event_log_dir, environment_id="unit")
    parity = verify_execution_control_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    payload = events[0]["payload"]
    assert events[0]["event_type"] == "execution_control_recorded"
    assert events[0]["idempotency_key"] == f"execution_control:unit:1:{DEFAULT_CONTROL_VERSION}"
    assert payload["execution_control_version"] == DEFAULT_CONTROL_VERSION
    assert payload["execution_id"] == "paper_trade:1:entry_execution"
    assert payload["lease_id"].startswith("lease:unit:")
    assert payload["fencing_token"]
    assert payload["lease_status"] == "released"
    assert payload["lease_valid_at_execution"] is True
    assert payload["state_version_at_decision"] == 1
    assert payload["state_version_at_execution"] == 2
    assert payload["requires_revalidation_before_fill"] is True
    assert payload["revalidation_passed"] is True
    assert payload["state"] == "filled_paper"
    assert payload["state_version"] == 2
    assert payload["failure_reason"] == "none"
    assert payload["terminal_state"] is True
    assert payload["execution_control_proof_level"] == "entry_execution_audit_fast_lane_claim"


def test_execution_control_new_only_uses_versioned_event_log_cursor(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, lifecycle_id="life-2")
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
        control_version=DEFAULT_CONTROL_VERSION,
        environment_id="unit",
        lease_ttl_sec=20.0,
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
