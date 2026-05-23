import json
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_quote_intent_bindings import (  # noqa: E402
    DEFAULT_BINDING_POLICY_VERSION,
    mirror_quote_intent_bindings,
    run_mirror_once,
    verify_quote_intent_binding_mirror_parity,
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


def entry_audit(success=True, *, input_amount=0.01, output_mint="TokenQuote"):
    return json.dumps(
        {
            "success": success,
            "requestId": "quote-request-1",
            "quoteTs": 1_700_000_002_000,
            "effectivePrice": 0.001,
            "slippageBps": 25,
            "inputAmount": input_amount,
            "inputMint": "SOL",
            "outputMint": output_mint,
            "route": "unit_signal",
        }
    )


def monitor_state():
    return json.dumps(
        {
            "entrySol": 0.01,
            "signalRoute": "unit_signal",
            "poolAddress": "pool-quote",
        }
    )


def insert_trade(db, trade_id=1, *, audit="__default__", position_size_sol=0.01, token_ca="TokenQuote", monitor="__default__"):
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
            token_ca,
            "QUOTE",
            1_700_000_000_000,
            0.001,
            1_700_000_003,
            0.0012,
            1_700_000_300,
            10,
            "legacy-pool",
            "available",
            "unit_entry",
            "unit_signal",
            position_size_sol,
            entry_audit() if audit == "__default__" else audit,
            None,
            monitor_state() if monitor == "__default__" else monitor,
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
        (10, "TokenQuote", "QUOTE", "solana", "pool-quote", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_quote_intent_binding_mirror_records_bound_and_mismatched_evidence(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, audit=entry_audit(input_amount=0.001))
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_quote_intent_bindings(paper_db, signal_db, event_log_dir)
    second = mirror_quote_intent_bindings(paper_db, signal_db, event_log_dir)
    parity = verify_quote_intent_binding_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    bound_payload = events[0]["payload"]
    mismatch_payload = events[1]["payload"]
    assert events[0]["event_type"] == "quote_intent_binding_recorded"
    assert events[0]["idempotency_key"] == f"quote_intent_binding:1:{DEFAULT_BINDING_POLICY_VERSION}"
    assert bound_payload["binding_policy_version"] == DEFAULT_BINDING_POLICY_VERSION
    assert bound_payload["quote_intent_bound"] is True
    assert bound_payload["quote_binding_proof_level"] == "provider_quote_request_id"
    assert bound_payload["size"] == 0.01
    assert bound_payload["route"] == "unit_signal"
    assert bound_payload["pool"] == "pool-quote"
    assert bound_payload["quote_mint"] == "SOL"
    assert bound_payload["slippage_bps"] == 25
    assert bound_payload["quote_ts"] == 1_700_000_002
    assert bound_payload["mismatch_fields"] == []
    assert mismatch_payload["quote_intent_bound"] is False
    assert mismatch_payload["mismatch_fields"] == ["size"]


def test_quote_intent_binding_legacy_proxy_uses_policy_defaults_without_price_as_size(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1, audit=None, position_size_sol=None, monitor=None)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    result = mirror_quote_intent_bindings(paper_db, signal_db, event_log_dir, legacy_size_sol=0.003)

    assert result["appended"] == 1
    payload = list(V27EventLog(event_log_dir).iter_events())[0]["payload"]
    assert payload["quote_intent_bound"] is True
    assert payload["size"] == 0.003
    assert payload["field_sources"]["size"] == "legacy_policy_default_size_sol"
    assert payload["quote_binding_proof_level"] == "legacy_paper_trade_entry_price_proxy"
    assert payload["legacy_paper_trade"]["entry_price"] != payload["size"]


def test_quote_intent_binding_new_only_uses_versioned_event_log_cursor(tmp_path):
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
        binding_policy_version=DEFAULT_BINDING_POLICY_VERSION,
        quote_source="paper_trade_entry_quote_or_legacy_proxy",
        legacy_size_sol=0.003,
        legacy_slippage_bps=500,
        new_only=True,
    )

    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_paper_trade_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_paper_trade_id"] == 2
