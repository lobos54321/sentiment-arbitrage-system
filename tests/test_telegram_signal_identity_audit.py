import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from telegram_signal_identity_audit import (  # noqa: E402
    DEFAULT_CONTRACT,
    EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION,
    EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION,
    SOL_MINT,
    build_audit,
)


def executable_quote_evidence(signal_id, token_ca, signal_ts, horizon_sec, return_pct):
    size_sol = 0.003
    entry_input = 3_000_000
    token_amount = 12_345_678 + int(signal_id)
    exit_output = round(entry_input * (1 + return_pct / 100))
    route_plan_json = json.dumps([{"percent": 100}], sort_keys=True, separators=(",", ":"))
    route_hash = hashlib.sha256(route_plan_json.encode()).hexdigest()

    def record(side, input_mint, output_mint, input_amount, output_amount, captured_at):
        return {
            "schema_version": EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION,
            "side": side,
            "signal_id": str(signal_id),
            "token_ca": token_ca,
            "signal_ts": signal_ts,
            "horizon_sec": horizon_sec,
            "provider": "jupiter-ultra",
            "source": "shared-quote-client",
            "input_mint": input_mint,
            "output_mint": output_mint,
            "input_amount_raw": str(input_amount),
            "output_amount_raw": str(output_amount),
            "provider_request_id": f"request-{signal_id}-{side}",
            "route_plan_json": route_plan_json,
            "route_plan_sha256": route_hash,
            "route_plan_hop_count": 1,
            "provider_fetched_at_ms": captured_at,
            "captured_at_ms": captured_at,
            "executable": True,
        }

    entry = record(
        "entry", SOL_MINT, token_ca, entry_input, token_amount, (signal_ts + 60) * 1000,
    )
    exit_quote = record(
        "exit", token_ca, SOL_MINT, token_amount, exit_output,
        (signal_ts + horizon_sec + 60) * 1000,
    )
    exact_return_pct = ((exit_output / entry_input) - 1) * 100
    return (
        EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION,
        size_sol,
        json.dumps(entry, sort_keys=True),
        json.dumps(exit_quote, sort_keys=True),
        exact_return_pct,
        "complete",
    )


def create_raw_db(path, now):
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE raw_signal_outcomes(
          id INTEGER PRIMARY KEY,
          signal_id TEXT,
          token_ca TEXT,
          signal_ts INTEGER,
          observation_status TEXT,
          right_censored INTEGER,
          horizon_sec INTEGER,
          max_wick_peak_pct REAL,
          max_sustained_peak_pct REAL,
          executable_quote_evidence_version TEXT,
          executable_quote_size_sol REAL,
          executable_entry_quote_json TEXT,
          executable_exit_quote_json TEXT,
          executable_quote_return_pct REAL,
          executable_quote_evidence_status TEXT,
          updated_at INTEGER
        )
        """
    )
    db.executemany(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "1", "TOKEN_A", now - 8300, "matured", 0, 7200, 10_500.0, 9_950.0,
             *executable_quote_evidence("1", "TOKEN_A", now - 8300, 7200, 9_100.0), now),
            (2, "2", "TOKEN_A", now - 8200, "matured", 0, 7200, 1_050.0, 920.0,
             *executable_quote_evidence("2", "TOKEN_A", now - 8200, 7200, 850.0), now),
            (3, "3", "TOKEN_B", now - 8100, "matured", 0, 7200, 120.0, 55.0,
             *executable_quote_evidence("3", "TOKEN_B", now - 8100, 7200, 52.0), now),
        ],
    )
    db.commit()
    db.close()


def test_dual_denominator_and_tier_versioning(tmp_path):
    now = int(time.time())
    signal_db = tmp_path / "signals.db"
    raw_db = tmp_path / "raw.db"
    db = sqlite3.connect(signal_db)
    db.execute(
        """
        CREATE TABLE premium_signals(
          id INTEGER PRIMARY KEY,
          telegram_message_id TEXT,
          telegram_channel_id TEXT,
          token_ca TEXT,
          source_message_ts INTEGER,
          signal_type TEXT,
          source_event_id TEXT,
          downstream_lifecycle_id TEXT
        )
        """
    )
    db.executemany(
        "INSERT INTO premium_signals VALUES (?,?,?,?,?,?,?,?)",
        [
            (1, "m1", "channel", "TOKEN_A", now - 8300, "NEW_TRENDING", "evt-1", "life-a"),
            (2, "m2", "channel", "TOKEN_A", now - 8200, "ATH", "evt-2", "life-a"),
            (3, None, None, "TOKEN_B", now - 8100, "ATH", "evt-3", "life-b"),
        ],
    )
    db.commit()
    db.close()
    create_raw_db(raw_db, now)

    report = build_audit(
        signal_db_path=str(signal_db),
        raw_db_path=str(raw_db),
        contract_path=str(DEFAULT_CONTRACT),
        hours=24,
        now_ts=now,
        limit=100,
    )

    assert report["outcome_schema_version"] == "telegram_signal_outcome.v1"
    assert report["denominators"]["signal_event"]["canonical_events"] == 3
    assert report["denominators"]["unique_token"]["unique_tokens"] == 2
    assert report["denominators"]["unique_token"]["tokens_with_multiple_events"] == 1
    assert report["raw_outcome_join"]["wick_tier_counts"]["100x"] == 1
    assert report["raw_outcome_join"]["sustained_tier_counts"]["100x"] == 1
    assert report["raw_outcome_join"]["sustained_tier_counts"]["10x"] == 1
    assert report["raw_outcome_join"]["sustained_tier_counts"]["silver"] == 1
    assert report["raw_outcome_join"]["executable_tier"]["available"] is True
    assert report["raw_outcome_join"]["executable_tier"]["calculated"] is True
    assert report["raw_outcome_join"]["executable_tier"]["tier_counts"]["10x"] == 1
    assert report["acceptance"]["passed"] is True
    assert report["promotion_allowed"] is False


def test_pending_rows_do_not_enter_outcome_tier_counts(tmp_path):
    now = int(time.time())
    signal_db = tmp_path / "signals.db"
    raw_db = tmp_path / "raw.db"
    db = sqlite3.connect(signal_db)
    db.execute(
        "CREATE TABLE premium_signals("
        "id INTEGER PRIMARY KEY, telegram_message_id TEXT, telegram_channel_id TEXT, "
        "token_ca TEXT, source_message_ts INTEGER, signal_type TEXT, source_event_id TEXT)"
    )
    db.execute(
        "INSERT INTO premium_signals VALUES (?,?,?,?,?,?,?)",
        (1, "m1", "channel", "TOKEN_A", now - 60, "ATH", "evt-1"),
    )
    db.commit()
    db.close()
    raw = sqlite3.connect(raw_db)
    raw.execute(
        "CREATE TABLE raw_signal_outcomes("
        "id INTEGER PRIMARY KEY, signal_id TEXT, token_ca TEXT, signal_ts INTEGER, "
        "observation_status TEXT, right_censored INTEGER, horizon_sec INTEGER, "
        "max_wick_peak_pct REAL, max_sustained_peak_pct REAL, "
        "executable_quote_evidence_version TEXT, executable_quote_size_sol REAL, "
        "executable_entry_quote_json TEXT, executable_exit_quote_json TEXT, "
        "executable_quote_return_pct REAL, executable_quote_evidence_status TEXT, "
        "updated_at INTEGER)"
    )
    raw.execute(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (1, "1", "TOKEN_A", now - 60, "pending", 1, 7200, 20_000.0, 15_000.0,
         None, None, None, None, None, None, now),
    )
    raw.commit()
    raw.close()

    report = build_audit(
        signal_db_path=str(signal_db),
        raw_db_path=str(raw_db),
        contract_path=str(DEFAULT_CONTRACT),
        hours=24,
        now_ts=now,
        limit=100,
    )

    assert report["raw_outcome_join"]["mature_rows"] == 0
    assert report["raw_outcome_join"]["right_censored_or_pending_rows"] == 1
    assert report["raw_outcome_join"]["wick_tier_counts"] == {}
    assert report["raw_outcome_join"]["sustained_tier_counts"] == {}
    assert report["raw_outcome_join"]["executable_tier"]["tier_counts"] == {}
    assert report["acceptance"]["passed"] is False


def test_executable_tier_rejects_bare_percent_and_inconsistent_quote_evidence(tmp_path):
    now = int(time.time())
    signal_ts = now - 8300
    signal_db = tmp_path / "signals.db"
    raw_db = tmp_path / "raw.db"
    signal = sqlite3.connect(signal_db)
    signal.execute(
        "CREATE TABLE premium_signals("
        "id INTEGER PRIMARY KEY, telegram_message_id TEXT, telegram_channel_id TEXT, "
        "token_ca TEXT, source_message_ts INTEGER, signal_type TEXT, source_event_id TEXT)"
    )
    signal.execute(
        "INSERT INTO premium_signals VALUES (?,?,?,?,?,?,?)",
        (1, "m1", "channel", "TOKEN_A", signal_ts, "ATH", "evt-1"),
    )
    signal.commit()
    signal.close()

    raw = sqlite3.connect(raw_db)
    raw.execute(
        "CREATE TABLE raw_signal_outcomes("
        "id INTEGER PRIMARY KEY, signal_id TEXT, token_ca TEXT, signal_ts INTEGER, "
        "observation_status TEXT, right_censored INTEGER, horizon_sec INTEGER, "
        "max_wick_peak_pct REAL, max_sustained_peak_pct REAL, "
        "executable_quote_evidence_version TEXT, executable_quote_size_sol REAL, "
        "executable_entry_quote_json TEXT, executable_exit_quote_json TEXT, "
        "executable_quote_return_pct REAL, executable_quote_evidence_status TEXT, "
        "updated_at INTEGER)"
    )
    evidence = executable_quote_evidence("1", "TOKEN_A", signal_ts, 7200, 52.0)
    raw.execute(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (1, "1", "TOKEN_A", signal_ts, "matured", 0, 7200, 120.0, 55.0, *evidence, now),
    )
    raw.commit()
    raw.close()

    def audit():
        return build_audit(
            signal_db_path=str(signal_db),
            raw_db_path=str(raw_db),
            contract_path=str(DEFAULT_CONTRACT),
            hours=24,
            now_ts=now,
            limit=100,
        )

    assert audit()["acceptance"]["passed"] is True

    raw = sqlite3.connect(raw_db)
    original = raw.execute(
        "SELECT executable_entry_quote_json, executable_exit_quote_json, "
        "executable_quote_return_pct FROM raw_signal_outcomes WHERE id=1"
    ).fetchone()
    raw.execute("UPDATE raw_signal_outcomes SET executable_entry_quote_json=NULL WHERE id=1")
    raw.commit()
    raw.close()
    bare_pct = audit()
    assert bare_pct["acceptance"]["passed"] is False
    assert bare_pct["raw_outcome_join"]["executable_tier"]["invalid_reason_counts"] == {
        "entry_or_exit_quote_json_invalid": 1,
    }

    tampered_exit = json.loads(original[1])
    tampered_exit["input_amount_raw"] = str(int(tampered_exit["input_amount_raw"]) + 1)
    raw = sqlite3.connect(raw_db)
    raw.execute(
        "UPDATE raw_signal_outcomes SET executable_entry_quote_json=?, "
        "executable_exit_quote_json=?, executable_quote_return_pct=? WHERE id=1",
        (original[0], json.dumps(tampered_exit, sort_keys=True), original[2]),
    )
    raw.commit()
    raw.close()
    amount_attack = audit()
    assert amount_attack["acceptance"]["passed"] is False
    assert amount_attack["raw_outcome_join"]["executable_tier"]["invalid_reason_counts"] == {
        "entry_exit_amount_chain_mismatch": 1,
    }

    raw = sqlite3.connect(raw_db)
    raw.execute(
        "UPDATE raw_signal_outcomes SET executable_exit_quote_json=?, "
        "executable_quote_return_pct=999 WHERE id=1",
        (original[1],),
    )
    raw.commit()
    raw.close()
    pct_attack = audit()
    assert pct_attack["acceptance"]["passed"] is False
    assert pct_attack["raw_outcome_join"]["executable_tier"]["invalid_reason_counts"] == {
        "stored_return_pct_mismatch": 1,
    }


def test_message_id_without_channel_is_not_exact_identity(tmp_path):
    now = int(time.time())
    signal_db = tmp_path / "signals.db"
    db = sqlite3.connect(signal_db)
    db.execute(
        "CREATE TABLE premium_signals("
        "id INTEGER PRIMARY KEY, telegram_message_id TEXT, token_ca TEXT, "
        "source_message_ts INTEGER, signal_type TEXT, source_event_id TEXT)"
    )
    db.execute(
        "INSERT INTO premium_signals VALUES (?,?,?,?,?,?)",
        (1, "message-1", "TOKEN_A", now - 60, "ATH", "event-1"),
    )
    db.commit()
    db.close()

    report = build_audit(
        signal_db_path=str(signal_db),
        raw_db_path=None,
        contract_path=str(DEFAULT_CONTRACT),
        hours=24,
        now_ts=now,
        limit=100,
    )

    assert report["identity_namespace_report"]["namespace_counts"].get("telegram_message_id", 0) == 0
    assert report["identity_namespace_report"]["namespace_counts"]["source_event_id"] == 1
    assert report["identity_namespace_report"]["telegram_message_id_without_channel_count"] == 1


def test_internal_signal_id_does_not_count_as_source_identity(tmp_path):
    now = int(time.time())
    signal_db = tmp_path / "signals.db"
    db = sqlite3.connect(signal_db)
    db.execute("CREATE TABLE premium_signals(id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO premium_signals(id) VALUES (1)")
    db.commit()
    db.close()

    report = build_audit(
        signal_db_path=str(signal_db),
        raw_db_path=None,
        contract_path=str(DEFAULT_CONTRACT),
        hours=24,
        now_ts=now,
        limit=100,
    )

    assert report["identity_namespace_report"]["namespace_counts"]["signal_id_only"] == 1
    assert report["acceptance"]["signal_id_only_is_not_source_identity"] is True
    assert report["acceptance"]["source_identity_coverage_rate"] == 0.0
    assert report["acceptance"]["passed"] is False
    assert report["promotion_allowed"] is False
