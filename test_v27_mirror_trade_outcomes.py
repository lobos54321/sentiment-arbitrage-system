import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_trade_outcomes import (  # noqa: E402
    mirror_trade_outcomes,
    run_mirror_once,
    verify_trade_outcome_mirror_parity,
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
            peak_pnl REAL DEFAULT 0,
            premium_signal_id INTEGER,
            entry_mode TEXT,
            position_size_sol REAL,
            synthetic_close INTEGER DEFAULT 0,
            accounting_outcome TEXT
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
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, exit_price,
             exit_ts, exit_reason, pnl_pct, peak_pnl, premium_signal_id,
             entry_mode, position_size_sol, synthetic_close, accounting_outcome)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenOutcome",
            "OUT",
            1_700_000_000,
            0.001,
            1_700_000_003,
            0.0014,
            1_700_000_120,
            "tp",
            0.4,
            0.75,
            10,
            "unit_route",
            0.01,
            0,
            "closed",
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
        (10, "TokenOutcome", "OUT", "solana", "pool-a", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_trade_outcome_mirror_records_fixed_entry_label_once(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_trade_outcomes(paper_db, signal_db, event_log_dir)
    second = mirror_trade_outcomes(paper_db, signal_db, event_log_dir)
    parity = verify_trade_outcome_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert second["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    payload = event["payload"]
    assert event["event_type"] == "trade_outcome_label_recorded"
    assert event["idempotency_key"] == "paper_trade_outcome_label:1"
    assert payload["trade_outcome_label_version"] == "legacy_paper_trade_outcome_v0.1"
    assert payload["counterfactual_entry_ts"] == 1_700_000_003
    assert payload["fill_time_anchor"] == "simulated_fill_ts"
    assert payload["simulated_fill_price"] == 0.001
    assert payload["net_delayed_executable_peak_3s"] == 0.75
    assert payload["exit_capture_ratio"] == 0.4 / 0.75
    assert payload["canonical_pool_group"] == "pool-a"


def test_trade_outcome_new_only_uses_event_log_cursor(tmp_path):
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
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
