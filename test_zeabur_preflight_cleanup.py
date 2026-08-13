import datetime as dt
import gzip
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import zeabur_preflight_cleanup as preflight  # noqa: E402
import offline_raw_gold_silver_funnel_audit as funnel_audit  # noqa: E402
import paper_evidence_log  # noqa: E402


def test_malformed_paper_db_is_quarantined_without_deleting_family(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paper_db = data_dir / "paper_trades.db"
    paper_wal = Path(f"{paper_db}-wal")
    paper_shm = Path(f"{paper_db}-shm")
    paper_db.write_bytes(b"this is not sqlite")
    paper_wal.write_bytes(b"wal bytes")
    paper_shm.write_bytes(b"shm bytes")

    monkeypatch.setattr(preflight, "RECOVERY_DIR", data_dir / "recovery")
    monkeypatch.setattr(preflight, "QUARANTINE_MALFORMED_PAPER_DB", True)

    preflight.checkpoint_db(paper_db)

    assert not paper_db.exists()
    assert not paper_wal.exists()
    assert not paper_shm.exists()
    recovery_dirs = list((data_dir / "recovery").glob("paper_trades_corrupt_*"))
    assert len(recovery_dirs) == 1
    recovery_dir = recovery_dirs[0]
    assert (recovery_dir / "paper_trades.db").read_bytes() == b"this is not sqlite"
    assert (recovery_dir / "paper_trades.db-wal").read_bytes() == b"wal bytes"
    assert (recovery_dir / "paper_trades.db-shm").read_bytes() == b"shm bytes"
    manifest = json.loads((recovery_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["moved"]
    assert "not a database" in manifest["reason"].lower()


def test_large_valid_db_skips_startup_quick_check_but_checkpoints(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paper_db = data_dir / "paper_trades.db"
    conn = sqlite3.connect(paper_db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO t (value) VALUES ('ok')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(preflight, "QUICK_CHECK_MAX_BYTES", 1)

    preflight.checkpoint_db(paper_db)

    assert paper_db.exists()
    assert not paper_db.with_suffix(".db.integrity_error").exists()


def test_existing_malformed_marker_quarantines_large_paper_db(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paper_db = data_dir / "paper_trades.db"
    paper_db.write_bytes(b"SQLite format 3\x00" + (b"x" * 128))
    marker = paper_db.with_suffix(".db.integrity_error")
    marker.write_text("context=pending_entry\nerror=database disk image is malformed\n", encoding="utf-8")

    monkeypatch.setattr(preflight, "RECOVERY_DIR", data_dir / "recovery")
    monkeypatch.setattr(preflight, "QUARANTINE_MALFORMED_PAPER_DB", True)
    monkeypatch.setattr(preflight, "QUICK_CHECK_MAX_BYTES", 1)

    preflight.checkpoint_db(paper_db)

    assert not paper_db.exists()
    assert not marker.exists()
    recovery_dirs = list((data_dir / "recovery").glob("paper_trades_corrupt_*"))
    assert len(recovery_dirs) == 1
    recovery_dir = recovery_dirs[0]
    assert (recovery_dir / "paper_trades.db").exists()
    assert "pending_entry" in (recovery_dir / "paper_trades.db.integrity_error").read_text(encoding="utf-8")


def test_paper_db_family_backup_copies_db_and_wal_files(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paper_db = data_dir / "paper_trades.db"
    paper_wal = Path(f"{paper_db}-wal")
    paper_shm = Path(f"{paper_db}-shm")
    conn = sqlite3.connect(paper_db)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO t (value) VALUES ('ok')")
    conn.commit()
    conn.close()
    paper_wal.write_bytes(b"wal bytes")
    paper_shm.write_bytes(b"shm bytes")

    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_ENABLED", True)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_DIR", data_dir / "backup")
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_MIN_INTERVAL_SEC", 0)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_KEEP", 3)

    preflight.backup_db_family(paper_db)

    backup_dirs = list((data_dir / "backup").glob("paper_trades_*"))
    assert len(backup_dirs) == 1
    backup_dir = backup_dirs[0]
    assert (backup_dir / "paper_trades.db").exists()
    assert (backup_dir / "paper_trades.db-wal").read_bytes() == b"wal bytes"
    assert (backup_dir / "paper_trades.db-shm").read_bytes() == b"shm bytes"
    manifest = json.loads((backup_dir / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["copied"]) == 3


def test_main_can_skip_db_checkpoint_for_partial_process_restart(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    paper_db = data_dir / "paper_trades.db"
    paper_db.write_bytes(b"this is not sqlite")

    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "LOG_NAMES", [])
    monkeypatch.setattr(preflight, "DB_NAMES", ["paper_trades.db"])
    monkeypatch.setattr(preflight, "DB_CHECK_ENABLED", False)

    assert preflight.main() == 0
    assert paper_db.exists()
    assert not (data_dir / "recovery").exists()


def test_jsonl_trim_keeps_tail_on_line_boundary(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    path = data_dir / "gmgn_candidates.jsonl"
    rows = [json.dumps({"row": i}) for i in range(20)]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    monkeypatch.setattr(preflight, "JSONL_TRIM_ENABLED", True)

    preflight.trim_jsonl_tail(path, max_bytes=64, keep_bytes=48)

    trimmed_rows = path.read_text(encoding="utf-8").splitlines()
    assert trimmed_rows
    assert trimmed_rows[-1] == json.dumps({"row": 19})
    assert all(row.startswith("{") for row in trimmed_rows)
    assert path.stat().st_size < len("\n".join(rows).encode("utf-8"))


def test_old_paper_evidence_shards_are_bounded_and_verifiable(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    payloads = {}
    for day in ("20260728", "20260729", "20260730", "20260731", "20260806"):
        payload = (json.dumps({"day": day, "value": "x" * 400}) + "\n").encode()
        path = evidence_dir / f"paper-events-{day}.jsonl"
        path.write_bytes(payload)
        payloads[day] = payload
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED", True)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_HOT_DAYS", 7)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES", 3)
    now_ts = dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc).timestamp()

    summary = preflight.archive_paper_evidence_jsonl_files(now_ts=now_ts)

    assert len(summary["archived"]) == 3
    assert summary["errors"] == []
    for day in ("20260728", "20260729", "20260730"):
        plain = evidence_dir / f"paper-events-{day}.jsonl"
        archive = evidence_dir / f"paper-events-{day}.jsonl.gz"
        assert not plain.exists()
        assert gzip.open(archive, "rb").read() == payloads[day]
    assert (evidence_dir / "paper-events-20260731.jsonl").exists()
    assert (evidence_dir / "paper-events-20260806.jsonl").exists()


def test_paper_evidence_research_retention_is_hard_capped_at_30_days():
    assert preflight.bounded_research_retention_days("999", 7) == 30
    assert preflight.bounded_research_retention_days("0", 7) == 1
    assert preflight.bounded_research_retention_days("invalid", 7) == 7


def test_expired_paper_evidence_archives_are_verified_then_deleted(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    expired = evidence_dir / "paper-events-20260713.jsonl.gz"
    boundary = evidence_dir / "paper-events-20260714.jsonl.gz"
    corrupt = evidence_dir / "paper-events-20260712.jsonl.gz"
    with gzip.open(expired, "wb") as fh:
        fh.write(b'{"event_id":"expired"}\n')
    with gzip.open(boundary, "wb") as fh:
        fh.write(b'{"event_id":"boundary"}\n')
    corrupt.write_bytes(b"not-gzip")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_RETENTION_DAYS", 30)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_GC_MAX_FILES", 4)
    now_ts = dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc).timestamp()

    summary = preflight.garbage_collect_paper_evidence_archives(now_ts=now_ts)

    assert summary["eligible"] == 2
    assert len(summary["deleted"]) == 1
    assert summary["deleted"][0]["verified"] is True
    assert not expired.exists()
    assert boundary.exists()
    assert corrupt.exists()
    assert len(summary["errors"]) == 1
    assert summary["max_total_research_retention_days"] == 30


def test_archive_gc_defers_when_plain_source_still_exists(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20260701.jsonl"
    archive = Path(f"{source}.gz")
    source.write_bytes(b'{"event_id":"late"}\n')
    with gzip.open(archive, "wb") as fh:
        fh.write(b'{"event_id":"archived"}\n')
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_GC_MAX_FILES", 4)

    summary = preflight.garbage_collect_paper_evidence_archives(
        now_ts=dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc).timestamp()
    )

    assert summary["deleted"] == []
    assert summary["deferred_source_present"] == [str(archive)]
    assert source.exists()
    assert archive.exists()


def test_archive_pass_is_idempotent(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    payload = b'{"event_id":"stable"}\n'
    source.write_bytes(payload)
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)

    first = preflight.archive_paper_evidence_jsonl_files()
    archive = Path(f"{source}.gz")
    first_archive_bytes = archive.read_bytes()
    second = preflight.archive_paper_evidence_jsonl_files()

    assert len(first["archived"]) == 1
    assert second["archived"] == []
    assert second["errors"] == []
    assert archive.read_bytes() == first_archive_bytes
    assert gzip.open(archive, "rb").read() == payload


def test_invalid_existing_gzip_never_removes_source(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20260728.jsonl"
    source.write_text('{"event_ts": 1}\n', encoding="utf-8")
    Path(f"{source}.gz").write_bytes(b"not-gzip")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED", True)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_HOT_DAYS", 7)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES", 3)

    summary = preflight.archive_paper_evidence_jsonl_files(
        now_ts=dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc).timestamp()
    )

    assert source.exists()
    assert len(summary["errors"]) == 1


def test_valid_existing_gzip_merges_late_source_without_loss(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    archived_payload = b'{"event_id":"archived"}\n'
    late_payload = b'{"event_id":"late"}\n'
    source.write_bytes(late_payload)
    archive = Path(f"{source}.gz")
    with gzip.open(archive, "wb") as fh:
        fh.write(archived_payload)
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)

    summary = preflight.archive_paper_evidence_jsonl_files()

    assert summary["errors"] == []
    assert not source.exists()
    assert gzip.open(archive, "rb").read() == archived_payload + late_payload


def test_archive_restart_after_replace_does_not_append_source_twice(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    archived_payload = b'{"event_id":"archived"}\n'
    late_payload = b'{"event_id":"late"}\n'
    source.write_bytes(late_payload)
    archive = Path(f"{source}.gz")
    with gzip.open(archive, "wb") as fh:
        fh.write(archived_payload + late_payload)
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)

    summary = preflight.archive_paper_evidence_jsonl_files()

    assert summary["errors"] == []
    assert not source.exists()
    assert gzip.open(archive, "rb").read() == archived_payload + late_payload


def test_archive_deadline_mid_copy_preserves_source_and_removes_temporary(
    tmp_path,
    monkeypatch,
):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    payload = b'x' * (preflight.ARCHIVE_COPY_CHUNK_BYTES + 1)
    source.write_bytes(payload)
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    deadline_checks = 0

    def deadline_expires_mid_copy(_deadline):
        nonlocal deadline_checks
        deadline_checks += 1
        return deadline_checks >= 4

    monkeypatch.setattr(preflight, "archive_deadline_expired", deadline_expires_mid_copy)

    summary = preflight.archive_paper_evidence_jsonl_files(deadline=100.0)

    assert source.read_bytes() == payload
    assert not Path(f"{source}.gz").exists()
    assert not list(evidence_dir.glob("*.gz.tmp.*"))
    assert len(summary["errors"]) == 1
    assert "deadline exceeded" in summary["errors"][0]["error"]


def test_stale_archive_temporary_is_removed_before_safe_retry(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    payload = b'{"event_id":"preserved"}\n'
    source.write_bytes(payload)
    stale = evidence_dir / "paper-events-20200101.jsonl.gz.tmp.999"
    stale.write_bytes(b"incomplete")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)

    summary = preflight.archive_paper_evidence_jsonl_files()

    assert summary["errors"] == []
    assert not stale.exists()
    assert not source.exists()
    assert gzip.open(Path(f"{source}.gz"), "rb").read() == payload


def test_paper_evidence_service_preflights_all_use_hard_timeout():
    service_script = (SCRIPT_DIR / "run_zeabur_services.sh").read_text(encoding="utf-8")
    invocation = "python3 scripts/zeabur_preflight_cleanup.py"
    matches = list(re.finditer(re.escape(invocation), service_script))

    assert len(matches) == 5
    for match in matches:
        wrapper_prefix = service_script[max(0, match.start() - 300) : match.start()]
        assert "scripts/run_with_timeout.py" in wrapper_prefix
        assert "--timeout-sec" in wrapper_prefix
    assert '[1-9]|[1-3][0-9]|4[0-5]' in service_script
    assert "export ZEABUR_PREFLIGHT_TIMEOUT_SEC=45" in service_script


@pytest.mark.parametrize(
    ("script_name", "expected_prefix"),
    [
        ("run_zeabur_services.sh", "export "),
        ("zeabur_prestart.sh", ""),
    ],
)
def test_zeabur_service_rejects_timeout_override_above_hard_cap(
    script_name,
    expected_prefix,
):
    service_script = (SCRIPT_DIR / script_name).read_text(encoding="utf-8")
    clamp = re.search(
        rf'{expected_prefix}ZEABUR_PREFLIGHT_TIMEOUT_SEC=.*?\n(case "\$ZEABUR_PREFLIGHT_TIMEOUT_SEC" in.*?esac)',
        service_script,
        re.DOTALL,
    )
    assert clamp is not None
    for supplied, expected in (("999", "45"), ("0", "45"), ("nan", "45"), ("44", "44")):
        probe = subprocess.run(
            [
                "bash",
                "-c",
                clamp.group(1) + "\nprintf '%s' \"$ZEABUR_PREFLIGHT_TIMEOUT_SEC\"",
            ],
            env={**os.environ, "ZEABUR_PREFLIGHT_TIMEOUT_SEC": supplied},
            text=True,
            capture_output=True,
            timeout=5,
            check=True,
        )

        assert probe.stdout == expected


def test_zeabur_service_npm_prestart_uses_clamped_preflight_timeout():
    prestart = (SCRIPT_DIR / "zeabur_prestart.sh").read_text(encoding="utf-8")

    assert '--timeout-sec "$ZEABUR_PREFLIGHT_TIMEOUT_SEC"' in prestart
    assert '--timeout-sec "${ZEABUR_PREFLIGHT_TIMEOUT_SEC:-45}"' not in prestart


def test_funnel_audit_reads_archived_paper_evidence_shard(tmp_path):
    evidence_dir = tmp_path / "paper_evidence_log"
    evidence_dir.mkdir()
    event_ts = dt.datetime(2026, 7, 28, 12, tzinfo=dt.timezone.utc).timestamp()
    record = {
        "event_ts": event_ts,
        "event_type": "paper_trade_entry_committed",
        "source": "paper_trade_monitor",
    }
    archive = evidence_dir / "paper-events-20260728.jsonl.gz"
    with gzip.open(archive, "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    result = funnel_audit.load_paper_evidence_event_counts(
        tmp_path / "paper_trades.db",
        event_ts - 60,
        event_ts + 60,
    )

    assert result["files_checked"] == 1
    assert result["events_in_window"] == 1
    assert result["parse_errors"] == 0
    assert result["event_type_counts"] == {"paper_trade_entry_committed": 1}


def test_cold_shards_beyond_archive_bound_are_not_tail_trimmed(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    payload = (json.dumps({"value": "x" * 80}) + "\n").encode() * 4
    for day in ("20200101", "20200102"):
        (evidence_dir / f"paper-events-{day}.jsonl").write_bytes(payload)
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED", True)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES", 1)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_GC_MAX_FILES", 0)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_MAX_BYTES", 64)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_KEEP_BYTES", 48)

    preflight.trim_runtime_jsonl_files()

    assert (evidence_dir / "paper-events-20200101.jsonl.gz").exists()
    deferred = evidence_dir / "paper-events-20200102.jsonl"
    assert deferred.read_bytes() == payload


def test_funnel_audit_prefers_plain_shard_without_double_counting(tmp_path):
    evidence_dir = tmp_path / "paper_evidence_log"
    evidence_dir.mkdir()
    event_ts = dt.datetime(2026, 7, 28, 12, tzinfo=dt.timezone.utc).timestamp()
    record = json.dumps(
        {
            "event_ts": event_ts,
            "event_type": "paper_trade_entry_committed",
            "source": "paper_trade_monitor",
        }
    )
    plain = evidence_dir / "paper-events-20260728.jsonl"
    plain.write_text(record + "\n", encoding="utf-8")
    with gzip.open(Path(f"{plain}.gz"), "wt", encoding="utf-8") as fh:
        fh.write(record + "\n")

    result = funnel_audit.load_paper_evidence_event_counts(
        tmp_path / "paper_trades.db",
        event_ts - 60,
        event_ts + 60,
    )

    assert result["files_checked"] == 2
    assert result["events_in_window"] == 1


def test_funnel_audit_combines_archived_shard_and_late_plain_events(tmp_path):
    evidence_dir = tmp_path / "paper_evidence_log"
    evidence_dir.mkdir()
    event_ts = dt.datetime(2026, 7, 28, 12, tzinfo=dt.timezone.utc).timestamp()
    archived_record = json.dumps(
        {
            "event_ts": event_ts,
            "event_type": "paper_trade_entry_committed",
            "source": "paper_trade_monitor",
        }
    )
    late_record = json.dumps(
        {
            "event_ts": event_ts + 1,
            "event_type": "paper_trade_exit_realized",
            "source": "paper_trade_monitor",
        }
    )
    plain = evidence_dir / "paper-events-20260728.jsonl"
    plain.write_text(late_record + "\n", encoding="utf-8")
    with gzip.open(Path(f"{plain}.gz"), "wt", encoding="utf-8") as fh:
        fh.write(archived_record + "\n")

    result = funnel_audit.load_paper_evidence_event_counts(
        tmp_path / "paper_trades.db",
        event_ts - 60,
        event_ts + 60,
    )

    assert result["files_checked"] == 2
    assert result["events_in_window"] == 2
    assert result["event_type_counts"] == {
        "paper_trade_entry_committed": 1,
        "paper_trade_exit_realized": 1,
    }


def test_funnel_audit_counts_plain_extension_of_archive_once(tmp_path):
    evidence_dir = tmp_path / "paper_evidence_log"
    evidence_dir.mkdir()
    event_ts = dt.datetime(2026, 7, 28, 12, tzinfo=dt.timezone.utc).timestamp()
    first = json.dumps(
        {"event_ts": event_ts, "event_type": "first", "source": "paper_trade_monitor"}
    )
    second = json.dumps(
        {"event_ts": event_ts + 1, "event_type": "second", "source": "paper_trade_monitor"}
    )
    plain = evidence_dir / "paper-events-20260728.jsonl"
    plain.write_text(first + "\n" + second + "\n", encoding="utf-8")
    with gzip.open(Path(f"{plain}.gz"), "wt", encoding="utf-8") as fh:
        fh.write(first + "\n")

    result = funnel_audit.load_paper_evidence_event_counts(
        tmp_path / "paper_trades.db", event_ts - 60, event_ts + 60
    )

    assert result["files_checked"] == 2
    assert result["events_in_window"] == 2
    assert result["event_type_counts"] == {"first": 1, "second": 1}


def test_funnel_audit_deduplicates_arbitrary_plain_gzip_overlap(tmp_path):
    evidence_dir = tmp_path / "paper_evidence_log"
    evidence_dir.mkdir()
    event_ts = dt.datetime(2026, 7, 28, 12, tzinfo=dt.timezone.utc).timestamp()

    def record(name, offset):
        return json.dumps(
            {
                "event_id": name,
                "event_ts": event_ts + offset,
                "event_type": name,
                "source": "paper_trade_monitor",
            }
        )

    plain = evidence_dir / "paper-events-20260728.jsonl"
    plain.write_text(record("B", 1) + "\n" + record("C", 2) + "\n", encoding="utf-8")
    with gzip.open(Path(f"{plain}.gz"), "wt", encoding="utf-8") as fh:
        fh.write(record("A", 0) + "\n" + record("B", 1) + "\n")

    result = funnel_audit.load_paper_evidence_event_counts(
        tmp_path / "paper_trades.db", event_ts - 60, event_ts + 60
    )

    assert result["events_in_window"] == 3
    assert result["event_type_counts"] == {"A": 1, "B": 1, "C": 1}


def test_archive_lock_wait_is_bounded_and_preserves_source(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    source = evidence_dir / "paper-events-20200101.jsonl"
    source.write_text('{"event_id":"preserve"}\n', encoding="utf-8")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_ARCHIVE_TIMEOUT_SEC", 0.05)

    with (evidence_dir / ".append.lock").open("a+", encoding="utf-8") as lock_fh:
        preflight.fcntl.flock(lock_fh, preflight.fcntl.LOCK_EX)
        started = time.monotonic()
        summary = preflight.archive_paper_evidence_jsonl_files(
            now_ts=dt.datetime(2026, 8, 12, tzinfo=dt.timezone.utc).timestamp()
        )
        elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert source.exists()
    assert summary["errors"]
    assert "deadline exceeded" in summary["errors"][0]["error"]


def test_hot_shard_trim_holds_append_lock(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    hot_day = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
    source = evidence_dir / f"paper-events-{hot_day}.jsonl"
    source.write_text((json.dumps({"value": "x" * 80}) + "\n") * 4, encoding="utf-8")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_MAX_BYTES", 64)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_KEEP_BYTES", 48)
    observed_locked = []
    original_trim = preflight.trim_jsonl_tail

    def assert_locked(path, *, max_bytes, keep_bytes):
        lock_probe = (evidence_dir / ".append.lock").open("a+", encoding="utf-8")
        try:
            try:
                preflight.fcntl.flock(
                    lock_probe,
                    preflight.fcntl.LOCK_EX | preflight.fcntl.LOCK_NB,
                )
                observed_locked.append(False)
                preflight.fcntl.flock(lock_probe, preflight.fcntl.LOCK_UN)
            except BlockingIOError:
                observed_locked.append(True)
        finally:
            lock_probe.close()
        original_trim(path, max_bytes=max_bytes, keep_bytes=keep_bytes)

    monkeypatch.setattr(preflight, "trim_jsonl_tail", assert_locked)
    preflight.trim_runtime_jsonl_files()

    assert observed_locked[-1] is True


def test_hot_shard_writer_appends_after_atomic_trim_without_loss(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    evidence_dir = data_dir / "paper_evidence_log"
    evidence_dir.mkdir(parents=True)
    event_ts = time.time()
    hot_day = dt.datetime.fromtimestamp(event_ts, dt.timezone.utc).strftime("%Y%m%d")
    source = evidence_dir / f"paper-events-{hot_day}.jsonl"
    source.write_text((json.dumps({"value": "x" * 80}) + "\n") * 4, encoding="utf-8")
    monkeypatch.setattr(preflight, "DATA_DIR", data_dir)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_MAX_BYTES", 64)
    monkeypatch.setattr(preflight, "PAPER_EVIDENCE_JSONL_KEEP_BYTES", 48)
    monkeypatch.setenv("PAPER_EVIDENCE_LOG_DIR", str(evidence_dir))
    writer_done = threading.Event()
    writer_result = []
    writer_thread = None

    def append_sentinel():
        writer_result.append(
            paper_evidence_log.append_paper_evidence_event(
                source="concurrency_test",
                event_type="sentinel_after_trim",
                event_ts=event_ts,
                critical=True,
            )
        )
        writer_done.set()

    def controlled_atomic_trim(path, *, max_bytes, keep_bytes):
        nonlocal writer_thread
        if not path.exists():
            return
        size = path.stat().st_size
        if size <= max_bytes:
            return
        with path.open("rb") as source_fh:
            source_fh.seek(max(0, size - keep_bytes))
            data = source_fh.read()
        first_newline = data.find(b"\n")
        if first_newline >= 0:
            data = data[first_newline + 1 :]
        temporary = path.with_suffix(path.suffix + ".trim.concurrent")
        writer_thread = threading.Thread(target=append_sentinel)
        writer_thread.start()
        assert not writer_done.wait(0.1)
        temporary.write_bytes(data)
        os.replace(temporary, path)

    monkeypatch.setattr(preflight, "trim_jsonl_tail", controlled_atomic_trim)

    preflight.trim_runtime_jsonl_files()
    assert writer_thread is not None
    writer_thread.join(timeout=2)

    assert not writer_thread.is_alive()
    assert writer_result == [str(source)]
    records = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines()]
    assert records[-1]["event_type"] == "sentinel_after_trim"
