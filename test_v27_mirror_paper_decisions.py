import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit, record_decision_event  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_paper_decisions import (  # noqa: E402
    acquire_loop_lock,
    mirror_missed_attributions,
    mirror_paper_decisions,
    run_mirror_once,
    verify_missed_mirror_parity,
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


def test_backfill_mirrors_missed_attribution_as_legacy_source_label_seed(tmp_path):
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"
    with new_db(db_path) as db:
        db.execute(
            """
            INSERT INTO paper_missed_signal_attribution
                (decision_event_id, created_event_ts, token_ca, symbol, signal_id, signal_ts,
                 route, component, decision, reject_reason, baseline_price, baseline_source,
                 baseline_ts, tradable_missed, tradable_peak_pnl, would_stop_before_peak, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                10,
                1_700_000_004,
                "TokenMiss",
                "MISS",
                77,
                1_700_000_000,
                "LOTTO",
                "upstream_gate",
                "skip",
                "tracking_ttl_expired",
                0.001,
                "legacy_baseline",
                1_700_000_000,
                1,
                0.75,
                0,
                "resolved",
            ),
        )
        db.commit()

    first = mirror_missed_attributions(db_path, event_log_dir)
    duplicate = mirror_missed_attributions(db_path, event_log_dir)
    parity = verify_missed_mirror_parity(db_path, event_log_dir)

    assert first["read_rows"] == 1
    assert first["appended"] == 1
    assert duplicate["duplicate"] == 1
    assert parity["parity_ok"] is True

    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["event_type"] == "paper_missed_signal_attribution_recorded"
    assert event["source"] == "paper_missed_signal_attribution"
    assert event["idempotency_key"] == "paper_missed_signal_attribution:1"
    assert event["payload"]["source_dog_label"] == "silver"
    assert event["payload"]["source_dog_label_version"] == "legacy_missed_attribution_seed_v0.1"
    assert event["payload"]["source_label_research_only"] is True
    assert event["payload"]["telegram_seen"] is True
    assert event["payload"]["realtime_observable"] is True


def test_decision_scoped_parity_does_not_treat_previous_ids_as_orphans(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"

    with new_db(db_path) as db:
        insert_decision(db, "TokenOne", 1_700_000_010)
        insert_decision(db, "TokenTwo", 1_700_000_011)

    mirror_paper_decisions(db_path, event_log_dir)
    scoped = verify_mirror_parity(db_path, event_log_dir, since_id=2, limit=1)

    assert scoped["db_rows"] == 1
    assert scoped["mirrored_events"] == 1
    assert scoped["orphan_mirrored_decision_event_ids"] == []
    assert scoped["parity_ok"] is True


def test_missed_scoped_parity_does_not_treat_previous_ids_as_orphans(tmp_path):
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"
    with new_db(db_path) as db:
        for missed_id in (1, 2):
            db.execute(
                """
                INSERT INTO paper_missed_signal_attribution
                    (id, decision_event_id, created_event_ts, token_ca, symbol,
                     component, decision, baseline_price, tradable_peak_pnl, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (missed_id, 100 + missed_id, 1_700_000_020 + missed_id, f"TokenMiss{missed_id}", f"M{missed_id}", "unit_gate", "skip", 0.001, 0.75, "resolved"),
            )
        db.commit()

    mirror_missed_attributions(db_path, event_log_dir)
    scoped = verify_missed_mirror_parity(db_path, event_log_dir, since_id=2, limit=1)

    assert scoped["db_rows"] == 1
    assert scoped["mirrored_events"] == 1
    assert scoped["orphan_mirrored_missed_attribution_ids"] == []
    assert scoped["parity_ok"] is True


def test_paper_decision_mirror_new_only_uses_independent_missed_cursor(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    db_path = tmp_path / "paper_trades.db"
    event_log_dir = tmp_path / "v27"
    with new_db(db_path) as db:
        insert_decision(db, "TokenOne", 1_700_000_030)
        insert_decision(db, "TokenTwo", 1_700_000_031)
        for missed_id in (10, 20):
            db.execute(
                """
                INSERT INTO paper_missed_signal_attribution
                    (id, decision_event_id, created_event_ts, token_ca, symbol,
                     component, decision, baseline_price, tradable_peak_pnl, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (missed_id, 200 + missed_id, 1_700_000_040 + missed_id, f"TokenMiss{missed_id}", f"M{missed_id}", "unit_gate", "skip", 0.001, 0.75, "resolved"),
            )
        db.commit()

    args = SimpleNamespace(
        db=str(db_path),
        event_log_dir=str(event_log_dir),
        since_id=None,
        until_id=None,
        limit=1,
        missed_since_id=None,
        missed_until_id=None,
        missed_limit=1,
        dry_run=False,
        include_missed=True,
        new_only=True,
    )
    first = run_mirror_once(args)
    second = run_mirror_once(args)

    assert first["cursor"]["decision_since_id"] is None
    assert first["cursor"]["missed_since_id"] is None
    assert first["mirror"]["appended"] == 1
    assert first["missed_mirror"]["appended"] == 1
    assert first["cursor"]["max_mirrored_decision_id"] == 1
    assert first["cursor"]["max_mirrored_missed_id"] == 10
    assert second["cursor"]["decision_since_id"] == 2
    assert second["cursor"]["missed_since_id"] == 11
    assert second["mirror"]["appended"] == 1
    assert second["missed_mirror"]["appended"] == 1
    assert second["cursor"]["max_mirrored_decision_id"] == 2
    assert second["cursor"]["max_mirrored_missed_id"] == 20


def test_paper_decision_mirror_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_paper_decision.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()
