import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_raw_provider_evidence import (  # noqa: E402
    DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    mirror_raw_provider_evidence,
    run_mirror_once,
    verify_raw_provider_evidence_mirror_parity,
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
            entry_ts INTEGER,
            exit_ts INTEGER,
            premium_signal_id INTEGER,
            lifecycle_id TEXT,
            entry_mode TEXT,
            signal_route TEXT,
            position_size_sol REAL,
            entry_execution_json TEXT,
            entry_execution_audit_json TEXT,
            exit_execution_json TEXT,
            exit_execution_audit_json TEXT
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


def trusted_execution(request_id="provider-request-1"):
    return json.dumps(
        {
            "success": True,
            "requestId": request_id,
            "provider": "jupiter_ultra",
            "endpoint": "/ultra/v1/order",
            "quoteTs": 1_700_000_002_000,
            "effectivePrice": 0.001,
            "slippageBps": 25,
            "inputAmount": 0.01,
            "inputMint": "SOL",
            "outputMint": "TokenRaw",
            "latencyMs": 123,
            "_rawOrder": {
                "requestId": request_id,
                "transaction": "base64-tx",
                "outAmount": "1000000",
                "slippageBps": 25,
            },
        }
    )


def trusted_provider_response_execution(request_id="provider-response-request-1"):
    return json.dumps(
        {
            "success": True,
            "requestId": request_id,
            "provider": "jupiter_ultra",
            "endpoint": "/ultra/v1/order",
            "quoteTs": 1_700_000_002_000,
            "effectivePrice": 0.001,
            "slippageBps": 25,
            "inputAmount": 0.01,
            "inputMint": "SOL",
            "outputMint": "TokenRaw",
            "latencyMs": 123,
            "providerResponse": {
                "requestId": request_id,
                "outAmount": "1000000",
                "routePlan": [{"swapInfo": {"ammKey": "pool-raw"}}],
                "slippageBps": 25,
            },
        }
    )


def audit_only(request_id="legacy-request-1"):
    return json.dumps(
        {
            "success": True,
            "requestId": request_id,
            "quoteTs": 1_700_000_002_000,
            "effectivePrice": 0.001,
            "slippageBps": 25,
            "inputAmount": 0.01,
            "inputMint": "SOL",
            "outputMint": "TokenRaw",
        }
    )


def insert_trade(db, trade_id=1, *, entry_execution="__trusted__", entry_audit="__audit__"):
    db.execute(
        """
        INSERT INTO paper_trades
            (id, token_ca, symbol, signal_ts, entry_ts, exit_ts, premium_signal_id,
             lifecycle_id, entry_mode, signal_route, position_size_sol,
             entry_execution_json, entry_execution_audit_json, exit_execution_json,
             exit_execution_audit_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_id,
            "TokenRaw",
            "RAW",
            1_700_000_000_000,
            1_700_000_003_000,
            1_700_000_300_000,
            10,
            "pool-raw",
            "unit_entry",
            "unit_signal",
            0.01,
            trusted_execution() if entry_execution == "__trusted__" else entry_execution,
            audit_only() if entry_audit == "__audit__" else entry_audit,
            None,
            None,
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
        (10, "TokenRaw", "RAW", "solana", "pool-raw", 0, 1_700_000_000, "parsed"),
    )
    db.commit()


def test_raw_provider_evidence_mirror_records_trusted_and_untrusted_provider_evidence(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, entry_execution=None, entry_audit=audit_only("legacy-request-2"))
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    first = mirror_raw_provider_evidence(paper_db, signal_db, event_log_dir)
    second = mirror_raw_provider_evidence(paper_db, signal_db, event_log_dir)
    parity = verify_raw_provider_evidence_mirror_parity(paper_db, event_log_dir)

    assert first["read_rows"] == 2
    assert first["candidate_provider_evidence"] == 2
    assert first["trusted_provider_evidence"] == 1
    assert first["appended"] == 2
    assert second["duplicate"] == 2
    assert parity["parity_ok"] is True

    events = list(V27EventLog(event_log_dir).iter_events())
    trusted = events[0]["payload"]
    untrusted = events[1]["payload"]
    assert events[0]["event_type"] == "raw_provider_evidence_recorded"
    assert events[0]["idempotency_key"] == f"raw_provider_evidence:1:entry:{DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION}"
    assert trusted["raw_provider_evidence_version"] == DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION
    assert trusted["provider"] == "jupiter_ultra"
    assert trusted["endpoint"] == "/ultra/v1/order"
    assert trusted["provider_request_id"] == "provider-request-1"
    assert trusted["latency_ms"] == 123
    assert trusted["raw_response_available"] is True
    assert trusted["provider_evidence_trusted"] is True
    assert len(trusted["request_hash"]) == 64
    assert len(trusted["response_hash"]) == 64
    assert untrusted["raw_response_available"] is False
    assert untrusted["provider_evidence_trusted"] is False
    assert untrusted["provider_evidence_proof_level"] == "legacy_execution_projection_without_raw_provider_response"


def test_raw_provider_evidence_mirror_trusts_runtime_provider_response_material(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1, entry_execution=trusted_provider_response_execution(), entry_audit=None)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    result = mirror_raw_provider_evidence(paper_db, signal_db, event_log_dir, trusted_only=True)

    events = list(V27EventLog(event_log_dir).iter_events())
    assert result["candidate_provider_evidence"] == 1
    assert result["trusted_provider_evidence"] == 1
    assert result["appended"] == 1
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["provider_request_id"] == "provider-response-request-1"
    assert payload["response_material_type"] == "execution.providerResponse"
    assert payload["raw_response_available"] is True
    assert payload["provider_evidence_trusted"] is True


def test_raw_provider_evidence_trusted_only_skips_untrusted_provider_evidence(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
        insert_trade(db, trade_id=2, entry_execution=None, entry_audit=audit_only("legacy-request-2"))
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    result = mirror_raw_provider_evidence(paper_db, signal_db, event_log_dir, trusted_only=True)
    parity = verify_raw_provider_evidence_mirror_parity(
        paper_db,
        event_log_dir,
        signal_db_path=signal_db,
        trusted_only=True,
    )

    events = list(V27EventLog(event_log_dir).iter_events())
    assert result["candidate_provider_evidence"] == 2
    assert result["trusted_provider_evidence"] == 1
    assert result["skipped_untrusted_provider_evidence"] == 1
    assert result["appended"] == 1
    assert parity["db_provider_evidence"] == 1
    assert parity["parity_ok"] is True
    assert len(events) == 1
    assert events[0]["payload"]["paper_trade_id"] == 1
    assert events[0]["payload"]["provider_evidence_trusted"] is True


def test_raw_provider_evidence_dry_run_cli_exits_cleanly_without_verify(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=1)
    with new_signal_db(signal_db) as db:
        insert_signal(db)

    script = Path(__file__).resolve().parent / "scripts" / "v27_mirror_raw_provider_evidence.py"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--dry-run",
            "--paper-db",
            str(paper_db),
            "--signal-db",
            str(signal_db),
            "--event-log-dir",
            str(event_log_dir),
            "--lock-file",
            str(tmp_path / "raw-provider.lock"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    payload = json.loads(completed.stdout)
    assert payload["mirror"]["dry_run"] is True
    assert payload["verify"] is None


def test_raw_provider_evidence_new_only_uses_overlap_cursor_for_late_exit_audits(tmp_path):
    paper_db = tmp_path / "paper.db"
    signal_db = tmp_path / "signal.db"
    event_log_dir = tmp_path / "events"
    with new_paper_db(paper_db) as db:
        insert_trade(db, trade_id=10)
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
        evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
        default_provider="jupiter_ultra",
        default_endpoint="/ultra/v1/order",
        cursor_overlap_ids=100,
        new_only=True,
    )

    first = run_mirror_once(args)
    with sqlite3.connect(str(paper_db)) as db:
        db.execute(
            "UPDATE paper_trades SET exit_execution_json = ?, exit_execution_audit_json = ? WHERE id = ?",
            (trusted_execution("exit-provider-request-10"), audit_only("exit-provider-request-10"), 10),
        )
        db.commit()
    second = run_mirror_once(args)

    events = list(V27EventLog(event_log_dir).iter_events())
    assert first["mirror"]["appended"] == 1
    assert second["cursor"]["since_id"] == 1
    assert second["mirror"]["duplicate"] == 1
    assert second["mirror"]["appended"] == 1
    assert [event["payload"]["side"] for event in events] == ["entry", "exit"]
