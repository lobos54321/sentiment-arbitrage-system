import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_telegram_signals import (  # noqa: E402
    acquire_loop_lock,
    mirror_premium_signals,
    run_mirror_once,
    verify_signal_mirror_parity,
)


def create_signal_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE premium_signals (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            timestamp INTEGER,
            source_message_ts INTEGER,
            receive_ts INTEGER,
            signal_type TEXT,
            parse_status TEXT,
            gate_result TEXT,
            raw_message TEXT,
            signal_source TEXT,
            remote_signal_id INTEGER
        )
        """
    )
    db.commit()
    return db


def test_premium_signal_mirror_is_idempotent_and_preserves_realtime_anchor(tmp_path):
    signal_db = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db) as db:
        db.execute(
            """
            INSERT INTO premium_signals
                (id, token_ca, symbol, timestamp, source_message_ts, receive_ts,
                 signal_type, parse_status, gate_result, raw_message, signal_source, remote_signal_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "TokenSignal",
                "SIG",
                1_700_000_000_000,
                1_700_000_000_000,
                1_700_000_003_000,
                "NOT_ATH",
                "parsed",
                '{"status":"PASS"}',
                "raw signal text",
                "premium_channel",
                9001,
            ),
        )
        db.commit()

    first = mirror_premium_signals(signal_db, event_log_dir)
    duplicate = mirror_premium_signals(signal_db, event_log_dir)
    parity = verify_signal_mirror_parity(signal_db, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert duplicate["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["event_type"] == "telegram_signal_seen"
    assert event["source"] == "premium_signals"
    assert event["aggregate_id"] == "telegram_signal:solana:TokenSignal:unknown_pool:0"
    assert event["idempotency_key"] == "premium_signals:1"
    assert event["observed_at"] == "2023-11-14T22:13:20Z"
    assert event["available_at"] == "2023-11-14T22:13:23Z"
    assert event["payload"]["token_ca"] == "TokenSignal"
    assert event["payload"]["telegram_seen"] is True
    assert event["payload"]["realtime_observable"] is True
    assert event["payload"]["realtime_observable_quality"] == "realtime_seed"
    assert event["payload"]["raw_message_hash"]
    assert event["payload"]["raw_message_length"] == len("raw signal text")
    assert event["payload"]["payload_schema_valid"] is True
    assert event["payload"]["raw_text_fields_redacted"] == ["raw_message"]
    assert event["payload"]["legacy_premium_signal"]["raw_message"] == "<redacted_raw_text>"


def test_premium_signal_mirror_marks_backfilled_rows_not_realtime_observable(tmp_path):
    signal_db = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db) as db:
        db.execute(
            """
            INSERT INTO premium_signals
                (id, token_ca, symbol, timestamp, source_message_ts, receive_ts,
                 signal_type, parse_status, gate_result, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "TokenBackfill",
                "BACK",
                1_700_000_000_000,
                1_700_000_000_000,
                1_700_000_003_000,
                "NEW_TRENDING",
                "parsed",
                '{"status":"PASS","backfilled":true}',
                "backfilled signal text",
            ),
        )
        db.commit()

    summary = mirror_premium_signals(signal_db, event_log_dir)

    assert summary["appended"] == 1
    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["payload"]["telegram_seen"] is True
    assert event["payload"]["realtime_observable"] is False
    assert event["payload"]["realtime_observable_quality"] == "backfilled"


def test_premium_signal_scoped_parity_does_not_treat_previous_ids_as_orphans(tmp_path):
    signal_db = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db) as db:
        for signal_id in (1, 2):
            db.execute(
                """
                INSERT INTO premium_signals
                    (id, token_ca, symbol, timestamp, source_message_ts, receive_ts,
                     signal_type, parse_status, gate_result, raw_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    f"Token{signal_id}",
                    f"T{signal_id}",
                    1_700_000_000_000 + signal_id,
                    1_700_000_000_000 + signal_id,
                    1_700_000_003_000 + signal_id,
                    "NOT_ATH",
                    "parsed",
                    '{"status":"PASS"}',
                    f"signal {signal_id}",
                ),
            )
        db.commit()

    mirror_premium_signals(signal_db, event_log_dir)
    scoped = verify_signal_mirror_parity(signal_db, event_log_dir, since_id=2, limit=1)

    assert scoped["db_rows"] == 1
    assert scoped["mirrored_events"] == 1
    assert scoped["orphan_mirrored_signal_ids"] == []
    assert scoped["parity_ok"] is True


def test_premium_signal_mirror_new_only_cursor_advances_from_event_log(tmp_path):
    signal_db = tmp_path / "signals.db"
    event_log_dir = tmp_path / "v27"
    with create_signal_db(signal_db) as db:
        for signal_id in (1, 2):
            db.execute(
                """
                INSERT INTO premium_signals
                    (id, token_ca, symbol, timestamp, source_message_ts, receive_ts,
                     signal_type, parse_status, gate_result, raw_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    f"Token{signal_id}",
                    f"T{signal_id}",
                    1_700_000_000_000 + signal_id,
                    1_700_000_000_000 + signal_id,
                    1_700_000_003_000 + signal_id,
                    "NOT_ATH",
                    "parsed",
                    '{"status":"PASS"}',
                    f"signal {signal_id}",
                ),
            )
        db.commit()

    args = SimpleNamespace(
        signal_db=str(signal_db),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        dry_run=False,
        table="premium_signals",
        default_chain="solana",
        new_only=True,
    )
    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["cursor"]["since_id"] is None
    assert first["mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_signal_id"] == 1
    assert second["cursor"]["since_id"] == 2
    assert second["mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_signal_id"] == 2


def test_premium_signal_mirror_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_telegram.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()
