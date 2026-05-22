import json
import sys
from pathlib import Path

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402
from v27_event_log_recover import recover_event_log  # noqa: E402


def test_recovery_quarantines_invalid_event_log_and_preserves_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    recovery_dir = tmp_path / "recovery"
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenA:unknown_pool:0",
        payload={"token_ca": "TokenA"},
        idempotency_key="premium_signals:TokenA",
    )
    event_path = event_log_dir / "events.jsonl"
    original_line = event_path.read_text(encoding="utf-8").strip()
    event_path.write_text(original_line + "\n" + original_line + "\n", encoding="utf-8")

    try:
        log.verify()
    except V27EventLogError as exc:
        assert "global_seq gap" in str(exc) or "duplicate idempotency" in str(exc)
    else:
        raise AssertionError("corrupt event log unexpectedly verified")

    report = recover_event_log(
        event_log_dir=event_log_dir,
        recovery_dir=recovery_dir,
        quarantine_invalid=True,
    )

    assert report["status"] == "quarantined"
    assert report["preflight"]["ok"] is False
    assert report["post_recovery"]["ok"] is True
    assert report["post_recovery"]["verify"]["event_count"] == 0
    assert not event_path.exists()

    quarantine_path = recovery_dir / Path(report["quarantine_path"]).name
    moved_event_path = quarantine_path / "events.jsonl"
    recovery_report = json.loads((quarantine_path / "recovery-report.json").read_text(encoding="utf-8"))
    last_report = json.loads((event_log_dir / "last-recovery-report.json").read_text(encoding="utf-8"))
    assert moved_event_path.exists()
    assert moved_event_path.read_text(encoding="utf-8").count("telegram_signal_seen") == 2
    assert recovery_report["status"] == "quarantined"
    assert last_report["quarantine_path"] == report["quarantine_path"]
