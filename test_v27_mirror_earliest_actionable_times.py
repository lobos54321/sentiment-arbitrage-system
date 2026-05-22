import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_earliest_actionable_times import (  # noqa: E402
    DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION,
    mirror_earliest_actionable_times,
    run_mirror_once,
    verify_earliest_actionable_mirror_parity,
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
            exit_ts INTEGER,
            trigger_ts INTEGER,
            armed_ts INTEGER,
            premium_signal_id INTEGER,
            lifecycle_id TEXT,
            execution_availability TEXT,
            entry_mode TEXT,
            signal_route TEXT
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


def insert_trade(db, trade_id=1, execution_availability="available", *, exit_ts=1_700_000_120):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, exit_ts, trigger_ts,
             armed_ts, premium_signal_id, lifecycle_id, execution_availability,
             entry_mode, signal_route)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenActionable",
            "ACT",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
            exit_ts,
            1_700_000_002,
            None,
            10,
            "legacy-pool",
            execution_availability,
            "unit_route",
            "unit_signal",
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
        (10, "TokenActionable", "ACT", "solana", "pool-actionable", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_earliest_actionable_mirror_records_window_bound_proof_once(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db)
        insert_trade(db, trade_id=2, execution_availability="unavailable")
        insert_trade(db, trade_id=3, exit_ts=1_700_000_001)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_earliest_actionable_times(paper_db, signal_db, event_log_dir)
    second = mirror_earliest_actionable_times(paper_db, signal_db, event_log_dir)
    parity = verify_earliest_actionable_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 1
    assert first["skipped_invalid_time_order"] == 1
    assert first["rejected_appended"] == 1
    assert second["duplicate"] == 1
    assert second["rejected_duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    payload = event["payload"]
    assert event["event_type"] == "earliest_actionable_time_recorded"
    assert event["idempotency_key"] == f"earliest_actionable_time:1:{DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION}"
    assert payload["earliest_actionable_policy_version"] == DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION
    assert payload["earliest_actionable_ts"] == 1_700_000_003
    assert payload["counterfactual_entry_ts"] == 1_700_000_003
    assert payload["peak_ts"] == 1_700_000_120
    assert payload["peak_ts_quality"] == "legacy_outcome_window_close_proxy"
    assert payload["actionable_before_peak"] is True
    assert payload["required_inputs_available_at"]["decision_engine_available_at"] == 1_700_000_002
    assert payload["canonical_pool_group"] == "pool-actionable"


def test_earliest_actionable_new_only_uses_versioned_event_log_cursor(tmp_path):
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
        earliest_actionable_policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION,
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2


def test_earliest_actionable_new_only_advances_past_rejected_rows(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1, exit_ts=1_700_000_001)
        insert_trade(db, trade_id=2, exit_ts=1_700_000_001)
        insert_trade(db, trade_id=3)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    args = SimpleNamespace(
        paper_db=str(paper_db),
        signal_db=str(signal_db),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=2,
        dry_run=False,
        table="paper_trades",
        signal_table="premium_signals",
        default_chain="solana",
        earliest_actionable_policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION,
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 0
    assert first["mirror"]["rejected_appended"] == 2
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 2
    assert second["cursor"]["since_id"] == 3
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 3
