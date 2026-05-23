import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_idempotency_contracts import (  # noqa: E402
    DEFAULT_CONTRACT_VERSION,
    mirror_idempotency_contracts,
    run_mirror_once,
    verify_idempotency_contract_mirror_parity,
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


def insert_trade(db, trade_id=1, *, token_ca="TokenIdem", lifecycle_id="life-1", route="unit_signal"):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_price, entry_ts,
             premium_signal_id, lifecycle_id, entry_mode, signal_route)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            token_ca,
            "IDEM",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
            10,
            lifecycle_id,
            "unit_entry",
            route,
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
        (10, "TokenIdem", "IDEM", "solana", "pool-idem", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_idempotency_contract_mirror_records_namespaced_execution_keys(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, lifecycle_id="life-2")
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_idempotency_contracts(paper_db, signal_db, event_log_dir, environment_id="unit")
    second = mirror_idempotency_contracts(paper_db, signal_db, event_log_dir, environment_id="unit")
    parity = verify_idempotency_contract_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    payload = events[0]["payload"]
    assert events[0]["event_type"] == "idempotency_contract_recorded"
    assert events[0]["idempotency_key"] == f"idempotency_contract:unit:1:{DEFAULT_CONTRACT_VERSION}"
    assert payload["idempotency_contract_version"] == DEFAULT_CONTRACT_VERSION
    assert payload["decision_id"] == "paper_trade:1:entry_decision"
    assert payload["execution_id"] == "paper_trade:1:entry_execution"
    assert payload["idempotency_key"].startswith("unit:paper_entry_execution:")
    assert payload["token_lifecycle_key"] == "solana:TokenIdem:pool-idem:0:life-1"
    assert payload["action"] == "paper_entry"
    assert payload["namespace"] == "paper_entry_execution"
    assert payload["environment_id"] == "unit"
    assert payload["route"] == "unit_signal"
    assert payload["hash_algorithm"] == "sha256(canonical_json)"
    assert payload["collision_policy"] == "reject_same_namespace_key_with_different_intent_hash"
    assert payload["cross_environment_isolated"] is True


def test_idempotency_contract_new_only_uses_versioned_event_log_cursor(tmp_path):
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
        contract_version=DEFAULT_CONTRACT_VERSION,
        namespace="paper_entry_execution",
        environment_id="unit",
        collision_policy="reject_same_namespace_key_with_different_intent_hash",
        hash_algorithm="sha256(canonical_json)",
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
