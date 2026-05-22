import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_ex_ante_feasibility import (  # noqa: E402
    DEFAULT_FEASIBILITY_POLICY_VERSION,
    mirror_ex_ante_feasibility,
    run_mirror_once,
    verify_ex_ante_mirror_parity,
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


def insert_trade(db, trade_id=1, execution_availability="available"):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts, trigger_ts,
             armed_ts, premium_signal_id, lifecycle_id, execution_availability,
             entry_mode, signal_route)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenFeasible",
            "FEAS",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
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
        (10, "TokenFeasible", "FEAS", "solana", "pool-feasible", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_ex_ante_mirror_records_no_future_outcome_feasibility_once(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db)
        insert_trade(db, trade_id=2, execution_availability="unavailable")
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_ex_ante_feasibility(paper_db, signal_db, event_log_dir)
    second = mirror_ex_ante_feasibility(paper_db, signal_db, event_log_dir)
    parity = verify_ex_ante_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert second["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    payload = event["payload"]
    assert event["event_type"] == "ex_ante_feasibility_recorded"
    assert event["idempotency_key"] == f"ex_ante_feasibility:1:{DEFAULT_FEASIBILITY_POLICY_VERSION}"
    assert payload["feasibility_policy_version"] == DEFAULT_FEASIBILITY_POLICY_VERSION
    assert payload["ex_ante_feasible"] is True
    assert payload["feasibility_class"] == "legacy_actual_paper_entry"
    assert payload["decision_ts"] == 1_700_000_002
    assert payload["entry_quote_available"] is True
    assert payload["used_future_peak"] is False
    assert payload["used_future_outcome"] is False
    assert payload["used_posthoc_label"] is False
    assert payload["forbidden_future_fields_used"] == []
    assert payload["canonical_pool_group"] == "pool-feasible"


def test_ex_ante_new_only_uses_versioned_event_log_cursor(tmp_path):
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
        feasibility_policy_version=DEFAULT_FEASIBILITY_POLICY_VERSION,
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
