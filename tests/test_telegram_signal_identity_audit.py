import os
from pathlib import Path
import sqlite3
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from telegram_signal_identity_audit import DEFAULT_CONTRACT, build_audit  # noqa: E402


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
          executable_quote_return_pct REAL,
          updated_at INTEGER
        )
        """
    )
    db.executemany(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, "1", "TOKEN_A", now - 8300, "matured", 0, 7200, 10_500.0, 9_950.0, 9_100.0, now),
            (2, "2", "TOKEN_A", now - 8200, "matured", 0, 7200, 1_050.0, 920.0, 850.0, now),
            (3, "3", "TOKEN_B", now - 8100, "matured", 0, 7200, 120.0, 55.0, 52.0, now),
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
        "executable_quote_return_pct REAL, updated_at INTEGER)"
    )
    raw.execute(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (1, "1", "TOKEN_A", now - 60, "pending", 1, 7200, 20_000.0, 15_000.0, 12_000.0, now),
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
