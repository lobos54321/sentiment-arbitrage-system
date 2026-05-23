import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_paper_ledgers import (  # noqa: E402
    DEFAULT_LEDGER_VERSION,
    mirror_paper_ledgers,
    run_mirror_once,
    verify_paper_ledger_mirror_parity,
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
            exit_reason TEXT,
            pnl_pct REAL,
            position_size_sol REAL,
            token_amount_raw TEXT,
            token_decimals INTEGER,
            premium_signal_id INTEGER,
            lifecycle_id TEXT,
            entry_mode TEXT,
            signal_route TEXT,
            accounting_outcome TEXT,
            synthetic_close INTEGER
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


def insert_trade(db, trade_id=1, *, token_ca="TokenLedger", size=0.02, exit_ts=1_700_000_300, pnl_pct=10.0, raw=None):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, exit_price, exit_ts,
             exit_reason, pnl_pct, position_size_sol, token_amount_raw, token_decimals,
             premium_signal_id, lifecycle_id, entry_mode, signal_route, accounting_outcome, synthetic_close)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            token_ca,
            "LEDGER",
            1_700_000_000,
            0.001,
            1_700_000_003,
            0.0011 if exit_ts else None,
            exit_ts,
            "take_profit" if exit_ts else None,
            pnl_pct if exit_ts else None,
            size,
            raw,
            6,
            10,
            f"life-{trade_id}",
            "unit_entry",
            "unit_signal",
            "closed" if exit_ts else "open",
            0,
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
        (10, "TokenLedger", "LEDGER", "solana", "pool-ledger", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_paper_ledger_mirror_records_position_capital_reservation_and_invariant(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1, size=0.02, exit_ts=1_700_000_300, pnl_pct=10.0)
        insert_trade(db, trade_id=2, size=0.03, exit_ts=None)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_paper_ledgers(paper_db, signal_db, event_log_dir, environment_id="unit", capital_basis_sol="1")
    second = mirror_paper_ledgers(paper_db, signal_db, event_log_dir, environment_id="unit", capital_basis_sol="1")
    parity = verify_paper_ledger_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    first_payload = events[0]["payload"]
    second_payload = events[1]["payload"]
    assert events[0]["event_type"] == "paper_ledger_recorded"
    assert first_payload["paper_ledger_version"] == DEFAULT_LEDGER_VERSION
    assert first_payload["position_id"] == "paper_trade:1:position"
    assert first_payload["execution_id"] == "paper_trade:1:entry_execution"
    assert first_payload["remaining_size"] == "0.000000000000"
    assert first_payload["release_reason"] == "position_closed"
    assert first_payload["invariant_ok"] is True
    assert first_payload["position_ledger_hash"]
    assert first_payload["capital_ledger_hash"]
    assert first_payload["ledger_hash"]
    assert second_payload["position_status"] == "open"
    assert second_payload["remaining_size"] == "0.030000000000"
    assert second_payload["release_reason"] == "entry_filled_open_position"
    assert second_payload["open_exposure"] == "0.030000000000"


def test_paper_ledger_new_only_uses_versioned_cursor(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, exit_ts=None)
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
        ledger_version=DEFAULT_LEDGER_VERSION,
        environment_id="unit",
        capital_basis_sol="1",
        default_position_size_sol="0.06",
        reservation_ttl_sec="20",
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2


def test_paper_ledger_skip_verify_keeps_hot_loop_bounded(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
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
        ledger_version=DEFAULT_LEDGER_VERSION,
        environment_id="unit",
        capital_basis_sol="1",
        default_position_size_sol="0.06",
        reservation_ttl_sec="20",
        new_only=True,
        skip_verify=True,
    )

    result = run_mirror_once(args)

    assert result["mirror"]["appended"] == 1
    assert result["verify"] is None
    assert result["cursor"]["verify_skipped"] is True
    assert result["cursor"]["max_mirrored_paper_trade_id"] == 1
