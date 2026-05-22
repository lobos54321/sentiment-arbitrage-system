import sqlite3
import sys

sys.path.insert(0, "scripts")

from paper_decision_audit import (  # noqa: E402
    _mirror_v27_decision_event,
    init_decision_audit,
    record_decision_event,
)
from v27_event_log import V27EventLog  # noqa: E402


def new_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    return db


def test_paper_decision_event_mirror_is_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("V27_EVENT_LOG_MIRROR_ENABLED", raising=False)
    monkeypatch.setenv("V27_EVENT_LOG_DIR", str(tmp_path / "v27"))

    db = new_db()
    record_decision_event(
        db,
        component="unit_gate",
        event_type="decision",
        decision="shadow",
        reason="mirror_disabled",
        token_ca="TokenDisabled",
        symbol="DIS",
        route="unit_route",
        payload={"score": 0.1},
        event_ts=1_700_000_000,
    )

    rows = db.execute("SELECT * FROM paper_decision_events").fetchall()
    assert len(rows) == 1
    assert not (tmp_path / "v27" / "events.jsonl").exists()


def test_paper_decision_event_mirrors_to_v27_event_log(tmp_path, monkeypatch):
    monkeypatch.setenv("V27_EVENT_LOG_MIRROR_ENABLED", "true")
    monkeypatch.setenv("V27_EVENT_LOG_DIR", str(tmp_path / "v27"))

    db = new_db()
    record_decision_event(
        db,
        component="clean_dog_gate",
        event_type="decision",
        decision="shadow",
        reason="reclaim_pending",
        token_ca="TokenMirror",
        symbol="MIR",
        lifecycle_id="life-1",
        signal_id=123,
        signal_ts=1_700_000_000,
        strategy_stage="shadow",
        route="tracking_ttl_reclaim_quote_clean_tiny_probe",
        data_source="unit",
        payload={
            "score": 0.77,
            "lifecycle": {
                "lifecycle_state": "RECLAIM_FORMING",
                "vitality_score": 0.42,
                "entry_bias": "wait",
            },
        },
        event_ts=1_700_000_001,
    )

    db_rows = db.execute("SELECT * FROM paper_decision_events").fetchall()
    assert len(db_rows) == 1

    event_log = V27EventLog(tmp_path / "v27")
    events = list(event_log.iter_events())
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "paper_decision_event_recorded"
    assert event["source"] == "paper_decision_audit"
    assert event["aggregate_id"] == "paper_decision:lifecycle:life-1"
    assert event["aggregate_seq"] == 1
    assert event["idempotency_key"] == f"paper_decision_events:{db_rows[0]['id']}"
    assert event["observed_at"] == "2023-11-14T22:13:21Z"
    assert event["payload"]["decision_event_id"] == db_rows[0]["id"]
    assert event["payload"]["token_ca"] == "TokenMirror"
    assert event["payload"]["legacy_event_type"] == "decision"
    assert event["payload"]["decision"] == "shadow"
    assert event["payload"]["lifecycle"]["lifecycle_state"] == "RECLAIM_FORMING"
    assert event["payload"]["payload"]["score"] == 0.77
    assert event_log.verify()["event_count"] == 1


def test_v27_decision_mirror_is_idempotent_by_legacy_decision_id(tmp_path, monkeypatch):
    monkeypatch.setenv("V27_EVENT_LOG_MIRROR_ENABLED", "true")
    monkeypatch.setenv("V27_EVENT_LOG_DIR", str(tmp_path / "v27"))

    kwargs = {
        "decision_event_id": 42,
        "event_ts": 1_700_000_002,
        "signal_id": 7,
        "token_ca": "TokenIdem",
        "symbol": "IDEM",
        "lifecycle_id": None,
        "trade_id": None,
        "signal_ts": 1_700_000_000,
        "strategy_stage": "shadow",
        "route": "unit_route",
        "component": "unit_gate",
        "event_type": "decision",
        "decision": "skip",
        "reason": "idempotency_check",
        "data_source": "unit",
        "payload": {"score": 0.2},
        "lifecycle": {},
    }

    first = _mirror_v27_decision_event(**kwargs)
    duplicate = _mirror_v27_decision_event(**kwargs)

    assert first["status"] == "appended"
    assert duplicate["status"] == "duplicate"
    assert duplicate["event"]["event_id"] == first["event"]["event_id"]
    event_log = V27EventLog(tmp_path / "v27")
    events = list(event_log.iter_events())
    assert len(events) == 1
    assert events[0]["aggregate_id"] == "paper_decision:token:TokenIdem"
    assert event_log.verify()["event_count"] == 1
