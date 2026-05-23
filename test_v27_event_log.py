import json

import pytest

from scripts.v27_event_log import V27EventLog, V27EventLogError


def test_v27_event_log_assigns_global_and_aggregate_sequences(tmp_path):
    log = V27EventLog(tmp_path)

    first = log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"token_ca": "TOKEN1"},
        idempotency_key="sig-1",
    )["event"]
    second = log.append_event(
        event_type="quote_observed",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"price": "1.23"},
        idempotency_key="quote-1",
    )["event"]
    third = log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="sol:TOKEN2:pool:epoch0",
        payload={"token_ca": "TOKEN2"},
        idempotency_key="sig-2",
    )["event"]

    assert first["global_seq"] == 1
    assert second["global_seq"] == 2
    assert third["global_seq"] == 3
    assert first["aggregate_seq"] == 1
    assert second["aggregate_seq"] == 2
    assert third["aggregate_seq"] == 1

    assert log.verify() == {
        "event_count": 3,
        "last_global_seq": 3,
        "aggregate_count": 2,
        "idempotency_count": 3,
    }
    assert log.summary() == {
        "event_count": 3,
        "last_global_seq": 3,
        "aggregate_count": 2,
        "idempotency_count": 3,
        "verify_mode": "cached_state_metadata",
    }


def test_v27_event_log_returns_existing_event_for_duplicate_idempotency_key(tmp_path):
    log = V27EventLog(tmp_path)

    first = log.append_event(
        event_type="decision_recorded",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"action": "shadow"},
        idempotency_key="decision-1",
    )
    duplicate = log.append_event(
        event_type="decision_recorded",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"action": "shadow"},
        idempotency_key="decision-1",
    )

    assert first["status"] == "appended"
    assert duplicate["status"] == "duplicate"
    assert duplicate["event"]["event_id"] == first["event"]["event_id"]
    assert log.verify()["event_count"] == 1


def test_v27_event_log_appends_batch_with_monotonic_sequences(tmp_path):
    log = V27EventLog(tmp_path)

    results = log.append_events(
        [
            {
                "event_type": "paper_ledger_recorded",
                "aggregate_id": "paper:1",
                "payload": {"paper_trade_id": 1},
                "idempotency_key": "ledger-1",
            },
            {
                "event_type": "paper_ledger_recorded",
                "aggregate_id": "paper:1",
                "payload": {"paper_trade_id": 2},
                "idempotency_key": "ledger-2",
            },
            {
                "event_type": "paper_ledger_recorded",
                "aggregate_id": "paper:2",
                "payload": {"paper_trade_id": 3},
                "idempotency_key": "ledger-3",
            },
        ]
    )

    events = [result["event"] for result in results]
    assert [result["status"] for result in results] == ["appended", "appended", "appended"]
    assert [event["global_seq"] for event in events] == [1, 2, 3]
    assert [event["aggregate_seq"] for event in events] == [1, 2, 1]
    assert log.verify() == {
        "event_count": 3,
        "last_global_seq": 3,
        "aggregate_count": 2,
        "idempotency_count": 3,
    }


def test_v27_event_log_batch_dedupes_existing_and_in_batch_keys(tmp_path):
    log = V27EventLog(tmp_path)
    first = log.append_event(
        event_type="decision_recorded",
        aggregate_id="decision:1",
        payload={"action": "shadow"},
        idempotency_key="decision-1",
    )["event"]

    results = log.append_events(
        [
            {
                "event_type": "decision_recorded",
                "aggregate_id": "decision:1",
                "payload": {"action": "shadow"},
                "idempotency_key": "decision-1",
            },
            {
                "event_type": "decision_recorded",
                "aggregate_id": "decision:2",
                "payload": {"action": "shadow"},
                "idempotency_key": "decision-2",
            },
            {
                "event_type": "decision_recorded",
                "aggregate_id": "decision:2",
                "payload": {"action": "shadow"},
                "idempotency_key": "decision-2",
            },
        ]
    )

    assert [result["status"] for result in results] == ["duplicate", "appended", "duplicate"]
    assert results[0]["event"]["event_id"] == first["event_id"]
    assert results[2]["event"]["event_id"] == results[1]["event"]["event_id"]
    assert log.verify()["event_count"] == 2


def test_v27_event_log_rebuilds_stale_sequencer_state_before_append(tmp_path):
    log = V27EventLog(tmp_path)
    first = log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"token_ca": "TOKEN1"},
        idempotency_key="sig-1",
    )["event"]

    state_path = tmp_path / "sequencer-state.json"
    state_path.write_text(
        json.dumps(
            {
                "last_global_seq": 0,
                "aggregate_last_seq": {},
                "idempotency_index": {},
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    second = log.append_event(
        event_type="quote_observed",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"price": "1.23"},
        idempotency_key="quote-1",
    )["event"]

    assert first["global_seq"] == 1
    assert second["global_seq"] == 2
    assert second["aggregate_seq"] == 2
    assert log.verify() == {
        "event_count": 2,
        "last_global_seq": 2,
        "aggregate_count": 1,
        "idempotency_count": 2,
    }
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["last_global_seq"] == 2
    assert state["event_file_size_bytes"] == (tmp_path / "events.jsonl").stat().st_size
    assert state["event_file_mtime_ns"] == (tmp_path / "events.jsonl").stat().st_mtime_ns


def test_v27_event_log_rejects_missing_required_event_semantics(tmp_path):
    log = V27EventLog(tmp_path)

    with pytest.raises(V27EventLogError, match="event_type is required"):
        log.append_event(
            event_type="",
            aggregate_id="sol:TOKEN1:pool:epoch0",
            payload={},
        )

    with pytest.raises(V27EventLogError, match="aggregate_id is required"):
        log.append_event(
            event_type="telegram_signal_seen",
            aggregate_id="",
            payload={},
        )

    with pytest.raises(V27EventLogError, match="payload must be a dict"):
        log.append_event(
            event_type="telegram_signal_seen",
            aggregate_id="sol:TOKEN1:pool:epoch0",
            payload=[],
        )


def test_v27_event_log_verify_detects_tampered_hash(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="quote_observed",
        aggregate_id="sol:TOKEN1:pool:epoch0",
        payload={"price": "1.23"},
        idempotency_key="quote-1",
    )

    event_path = tmp_path / "events.jsonl"
    event = json.loads(event_path.read_text(encoding="utf-8").strip())
    event["payload"]["price"] = "9.99"
    event_path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(V27EventLogError, match="event_hash mismatch"):
        log.verify()
