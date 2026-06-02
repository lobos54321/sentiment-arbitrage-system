import json
import sqlite3
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from paper_decision_audit import record_decision_event  # noqa: E402
from paper_evidence_log import append_paper_evidence_event  # noqa: E402


def _read_events(root):
    rows = []
    for path in sorted(Path(root).glob("paper-events-*.jsonl")):
        rows.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return rows


def test_append_paper_evidence_event_writes_jsonl(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_EVIDENCE_LOG_DIR", str(tmp_path))

    path = append_paper_evidence_event(
        source="unit",
        event_type="probe",
        idempotency_key="unit:1",
        event_ts=1_700_000_000,
        payload={"token_ca": "TokenA", "nested": {"ok": True}},
        critical=True,
    )

    assert path is not None
    events = _read_events(tmp_path)
    assert len(events) == 1
    assert events[0]["schema_version"] == "paper_evidence_log.v1"
    assert events[0]["source"] == "unit"
    assert events[0]["event_type"] == "probe"
    assert events[0]["idempotency_key"] == "unit:1"
    assert events[0]["payload"]["token_ca"] == "TokenA"


class FailingDb:
    def execute(self, *_args, **_kwargs):
        raise sqlite3.DatabaseError("database disk image is malformed")

    def commit(self):
        raise AssertionError("commit should not be reached")


def test_record_decision_event_preserves_evidence_when_sqlite_write_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("PAPER_EVIDENCE_LOG_DIR", str(tmp_path))

    record_decision_event(
        FailingDb(),
        component="execution_api",
        event_type="entry_quote",
        decision="filled_paper",
        reason="unit_test",
        token_ca="TokenMalformed",
        symbol="BADDB",
        lifecycle_id="life-1",
        trade_id=123,
        signal_ts=1_700_000_000,
        signal_id=99,
        strategy_stage="stage1",
        route="LOTTO",
        data_source="unit",
        payload={"entry_price": 0.1},
        event_ts=1_700_000_001,
    )

    events = _read_events(tmp_path)
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "paper_decision_event_intent"
    assert event["payload"]["token_ca"] == "TokenMalformed"
    assert event["payload"]["component"] == "execution_api"
    assert event["payload"]["decision"] == "filled_paper"

