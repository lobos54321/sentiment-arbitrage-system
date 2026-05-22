import sqlite3
import sys

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit, record_decision_event  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_paper_decisions import (  # noqa: E402
    mirror_paper_decisions,
    verify_mirror_parity,
)


def new_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    return db


def insert_decision(db, token_ca, event_ts):
    record_decision_event(
        db,
        component="unit_gate",
        event_type="decision",
        decision="shadow",
        reason="backfill_test",
        token_ca=token_ca,
        symbol=token_ca[-4:],
        route="unit_route",
        data_source="unit",
        payload={"score": 0.5, "token": token_ca},
        event_ts=event_ts,
    )


def test_backfill_mirrors_existing_paper_decision_rows_once(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"

    with new_db(db_path) as db:
        insert_decision(db, "TokenOne", 1_700_000_000)
        insert_decision(db, "TokenTwo", 1_700_000_001)

    first = mirror_paper_decisions(db_path, event_log_dir)
    second = mirror_paper_decisions(db_path, event_log_dir)
    parity = verify_mirror_parity(db_path, event_log_dir)

    assert first["read_rows"] == 2
    assert first["appended"] == 2
    assert first["duplicate"] == 0
    assert first["failed"] == 0
    assert second["read_rows"] == 2
    assert second["appended"] == 0
    assert second["duplicate"] == 2
    assert second["failed"] == 0
    assert parity["parity_ok"] is True
    assert parity["db_rows"] == 2
    assert parity["mirrored_events"] == 2
    assert parity["missing_decision_event_ids"] == []
    assert parity["duplicate_decision_event_ids"] == []
    assert V27EventLog(event_log_dir).verify()["event_count"] == 2


def test_backfill_verify_detects_missing_mirrored_decisions(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"

    with new_db(db_path) as db:
        insert_decision(db, "TokenMissing", 1_700_000_002)

    parity = verify_mirror_parity(db_path, event_log_dir)

    assert parity["parity_ok"] is False
    assert parity["db_rows"] == 1
    assert parity["mirrored_events"] == 0
    assert parity["missing_decision_event_ids"] == [1]


def test_backfill_preserves_invalid_json_as_dirty_payload_evidence(tmp_path):
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"
    with new_db(db_path) as db:
        db.execute(
            """
            INSERT INTO paper_decision_events
                (event_ts, token_ca, component, event_type, decision, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1_700_000_003, "TokenDirty", "unit_gate", "decision", "observe", "{bad json"),
        )
        db.commit()

    summary = mirror_paper_decisions(db_path, event_log_dir)

    assert summary["appended"] == 1
    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["payload"]["payload"]["_json_field"] == "payload_json"
    assert "_json_parse_error" in event["payload"]["payload"]
    assert verify_mirror_parity(db_path, event_log_dir)["parity_ok"] is True
