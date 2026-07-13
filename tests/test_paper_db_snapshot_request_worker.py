import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import paper_db_snapshot_request_worker as worker


def create_source(path):
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE candidate_shadow_observations (id INTEGER PRIMARY KEY)")
    connection.execute("CREATE TABLE candidate_shadow_virtual_trades (id INTEGER PRIMARY KEY)")
    connection.execute("CREATE TABLE paper_trades (id INTEGER PRIMARY KEY)")
    connection.execute("INSERT INTO candidate_shadow_observations DEFAULT VALUES")
    connection.execute("INSERT INTO candidate_shadow_virtual_trades DEFAULT VALUES")
    connection.execute("INSERT INTO paper_trades DEFAULT VALUES")
    connection.commit()
    connection.close()


def write_request(path, request_id="cleanup-20260713"):
    path.write_text(
        json.dumps(
            {
                "schema_version": worker.REQUEST_SCHEMA_VERSION,
                "request_id": request_id,
                "reason": "approved_cleanup_after_verified_snapshot",
            }
        ),
        encoding="utf-8",
    )


def run_to_terminal(*, request, status, source, recovery, archive, max_attempts=3):
    states = []
    for _ in range(8):
        result = worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
            max_attempts=max_attempts,
        )
        states.append(result["state"])
        if result["state"] in {"completed", "completed_existing", "failed", "attempts_exhausted"}:
            return result, states
    raise AssertionError(f"snapshot did not reach a terminal state: {states}")


def test_snapshot_request_creates_verified_snapshot_and_archives_request(tmp_path):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request)

    result, states = run_to_terminal(
        request=request,
        status=status,
        source=source,
        recovery=recovery,
        archive=archive,
    )

    assert result["state"] == "completed"
    assert states == [
        "running_snapshot_copied",
        "running_quick_check_complete",
        "running_sha256_complete",
        "running_critical_counts_complete",
        "completed",
    ]
    assert result["quick_check"] == ["ok"]
    assert result["critical_table_counts"] == {
        "candidate_shadow_observations": 1,
        "candidate_shadow_virtual_trades": 1,
        "paper_trades": 1,
    }
    assert len(result["snapshot_sha256"]) == 64
    final_dir = recovery / "paper_trades_verified_cleanup-20260713"
    assert (final_dir / "paper_trades.db").is_file()
    assert (final_dir / "manifest.json").is_file()
    assert list(recovery.glob(".paper_trades_verified_*.partial")) == []
    assert not request.exists()
    assert (archive / "completed_cleanup-20260713.json").is_file()
    assert json.loads(status.read_text())["state"] == "completed"


def test_snapshot_request_stops_after_bounded_failures(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="bounded-failure")

    def fail_snapshot(*_args, **_kwargs):
        raise RuntimeError("synthetic snapshot failure")

    monkeypatch.setattr(worker, "create_consistent_sqlite_snapshot", fail_snapshot)
    for expected_attempt in (1, 2, 3):
        result = worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
            max_attempts=3,
        )
        assert result["state"] == "failed"
        assert result["attempt_count"] == expected_attempt
        assert list(recovery.glob(".paper_trades_verified_*.partial")) == []

    marker = tmp_path / "paper_trades.db.integrity_error"
    marker.write_text("synthetic marker after third failure", encoding="utf-8")
    exhausted = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        max_attempts=3,
    )
    assert exhausted["state"] == "attempts_exhausted"
    assert exhausted["attempt_count"] == 3
    marker.unlink()
    still_exhausted = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        max_attempts=3,
    )
    assert still_exhausted["state"] == "attempts_exhausted"
    assert still_exhausted["attempt_count"] == 3
    assert request.exists()


def test_snapshot_request_blocks_without_attempt_when_integrity_marker_exists(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="marked-source")
    marker = tmp_path / "paper_trades.db.integrity_error"
    marker.write_text("database disk image is malformed", encoding="utf-8")

    def must_not_snapshot(*_args, **_kwargs):
        raise AssertionError("snapshot must not run while integrity marker exists")

    monkeypatch.setattr(worker, "create_consistent_sqlite_snapshot", must_not_snapshot)
    result = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )

    assert result["state"] == "blocked_source_integrity_marker"
    assert result["attempt_count"] == 0
    assert result["integrity_marker"] == str(marker)
    assert request.exists()
    assert list(recovery.glob(".paper_trades_verified_*.partial")) == []


def test_completed_final_attempt_wins_over_request_archive_retry(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="final-attempt-archive-retry")
    real_replace = worker.os.replace
    archive_failures = 0

    def fail_first_request_archive(source_path, destination_path):
        nonlocal archive_failures
        if Path(source_path) == request and archive_failures == 0:
            archive_failures += 1
            raise OSError("synthetic request archive failure")
        return real_replace(source_path, destination_path)

    monkeypatch.setattr(worker.os, "replace", fail_first_request_archive)
    completed, _states = run_to_terminal(
        request=request,
        status=status,
        source=source,
        recovery=recovery,
        archive=archive,
        max_attempts=1,
    )
    assert completed["state"] == "completed"
    assert completed["attempt_count"] == 1
    assert completed["request_archive_error"]
    assert request.exists()

    recovered = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        max_attempts=1,
    )
    assert recovered["state"] == "completed_existing"
    assert recovered["attempt_count"] == 1
    assert recovered["request_archive_error"] is None
    assert not request.exists()


def test_snapshot_resumes_validation_without_recopy_after_external_stop(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="resume-after-stop")
    real_create = worker.create_consistent_sqlite_snapshot
    create_calls = 0

    def counted_create(*args, **kwargs):
        nonlocal create_calls
        create_calls += 1
        return real_create(*args, **kwargs)

    monkeypatch.setattr(worker, "create_consistent_sqlite_snapshot", counted_create)
    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"

    real_quick_check = worker.snapshot_quick_check

    def externally_stopped(_path):
        raise KeyboardInterrupt("synthetic external stop")

    monkeypatch.setattr(worker, "snapshot_quick_check", externally_stopped)
    try:
        worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
        )
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("synthetic external stop did not propagate")

    checkpoint = json.loads(
        (recovery / ".paper_trades_verified_resume-after-stop.partial" / "checkpoint.json").read_text()
    )
    assert checkpoint["stage"] == "snapshot_copied"
    monkeypatch.setattr(worker, "snapshot_quick_check", real_quick_check)
    completed, states = run_to_terminal(
        request=request,
        status=status,
        source=source,
        recovery=recovery,
        archive=archive,
    )
    assert completed["state"] == "completed"
    assert completed["attempt_count"] == 1
    assert states[0] == "running_quick_check_complete"
    assert create_calls == 1


def test_checkpoint_continues_after_copy_at_attempt_limit(tmp_path):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="resume-at-limit")

    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        max_attempts=3,
    )
    assert copied["state"] == "running_snapshot_copied"
    copied["attempt_count"] = 3
    status.write_text(json.dumps(copied), encoding="utf-8")

    completed, _states = run_to_terminal(
        request=request,
        status=status,
        source=source,
        recovery=recovery,
        archive=archive,
        max_attempts=3,
    )
    assert completed["state"] == "completed"
    assert completed["attempt_count"] == 3
