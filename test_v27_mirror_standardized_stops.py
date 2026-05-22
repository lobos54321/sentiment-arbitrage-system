import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_standardized_stops import (  # noqa: E402
    DEFAULT_STOP_CONTRACT_VERSION,
    mirror_standardized_stops,
    run_mirror_once,
    verify_standardized_stop_mirror_parity,
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
            lifecycle_id TEXT
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


def insert_trade(db, trade_id=1):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, premium_signal_id, lifecycle_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenStop",
            "STOP",
            1_700_000_000,
            0.001,
            1_700_000_003,
            10,
            "legacy-pool",
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
        (10, "TokenStop", "STOP", "solana", "pool-stop", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_standardized_stop_mirror_records_versioned_stop_contract_once(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_standardized_stops(paper_db, signal_db, event_log_dir)
    second = mirror_standardized_stops(paper_db, signal_db, event_log_dir)
    parity = verify_standardized_stop_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert second["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    payload = event["payload"]
    assert event["event_type"] == "standardized_stop_contract_recorded"
    assert event["idempotency_key"] == f"standardized_stop_contract:1:{DEFAULT_STOP_CONTRACT_VERSION}"
    assert payload["stop_contract_version"] == DEFAULT_STOP_CONTRACT_VERSION
    assert payload["stop_type"] == "standardized_counterfactual_stop"
    assert payload["stop_threshold_pct"] == -30.0
    assert payload["stop_window"] == "60m"
    assert payload["stop_price_type"] == "delayed_executable_exit_quote_proxy"
    assert payload["stop_executable_required"] is True
    assert payload["stop_friction_model_version"] == "legacy_round_trip_friction_v0.1"
    assert payload["stop_available_at"] == 1_700_000_003
    assert payload["canonical_pool_group"] == "pool-stop"


def test_standardized_stop_new_only_uses_versioned_event_log_cursor(tmp_path):
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
        stop_contract_version=DEFAULT_STOP_CONTRACT_VERSION,
        stop_type="standardized_counterfactual_stop",
        stop_threshold_pct=-30.0,
        stop_window="60m",
        stop_price_type="delayed_executable_exit_quote_proxy",
        stop_executable_required=True,
        stop_friction_model_version="legacy_round_trip_friction_v0.1",
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
