import json
import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

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


def add_orphan_page_corruption(path):
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE disposable_pages (payload BLOB)")
    connection.executemany(
        "INSERT INTO disposable_pages(payload) VALUES (?)",
        [(b"x" * 8192,) for _ in range(128)],
    )
    connection.commit()
    connection.execute("DELETE FROM disposable_pages")
    connection.commit()
    freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    connection.close()
    assert freelist_count > 0
    with path.open("r+b") as handle:
        handle.seek(32)
        handle.write(b"\x00" * 8)
        handle.flush()
        os.fsync(handle.fileno())
    assert worker.snapshot_quick_check_result(path) != ["ok"]


def write_request(path, request_id="cleanup-20260713", **extra):
    path.write_text(
        json.dumps(
            {
                "schema_version": worker.REQUEST_SCHEMA_VERSION,
                "request_id": request_id,
                "reason": "approved_cleanup_after_verified_snapshot",
                **extra,
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


def test_vacuum_rebuild_requires_explicit_approval_flag(tmp_path):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="repair-without-approval", repair_mode="vacuum_rebuild")

    with pytest.raises(ValueError, match="allow_rebuild_corrupt_snapshot=true"):
        worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
        )
    assert worker.sha256_file(source)
    assert not recovery.exists()


def test_vacuum_rebuild_runs_only_on_snapshot_and_preserves_required_counts(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    local_verify = tmp_path / "local-verify"
    create_source(source)
    add_orphan_page_corruption(source)
    source_sha256 = worker.sha256_file(source)
    write_request(
        request,
        request_id="approved-vacuum-rebuild",
        repair_mode="vacuum_rebuild",
        allow_rebuild_corrupt_snapshot=True,
        required_tables=list(worker.CRITICAL_TABLES),
    )
    real_rebuild = worker.vacuum_rebuild_snapshot

    def same_filesystem_rebuild(snapshot, **kwargs):
        assert snapshot != source
        return real_rebuild(snapshot, require_distinct_filesystem=False, **kwargs)

    monkeypatch.setattr(worker, "vacuum_rebuild_snapshot", same_filesystem_rebuild)
    result = None
    states = []
    for _ in range(10):
        result = worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
            local_verify_dir=local_verify if len(states) < 2 else None,
        )
        states.append(result["state"])
        if result["state"] == "completed":
            break

    assert result["state"] == "completed"
    assert states == [
        "running_snapshot_copied",
        "running_vacuum_rebuild_complete",
        "running_quick_check_complete",
        "running_sha256_complete",
        "running_critical_counts_complete",
        "completed",
    ]
    assert worker.sha256_file(source) == source_sha256
    manifest = json.loads(
        (recovery / "paper_trades_verified_approved-vacuum-rebuild" / "manifest.json").read_text()
    )
    snapshot = manifest["snapshot"]
    assert snapshot["method"] == "sqlite_online_backup_then_vacuum_rebuild"
    assert snapshot["repair"]["repair_mode"] == "vacuum_rebuild"
    assert snapshot["repair"]["source_quick_check"] != ["ok"]
    assert snapshot["repair"]["persistent_rebuilt_quick_check"] == ["ok"]
    assert snapshot["repair"]["source_required_table_counts"] == {
        "candidate_shadow_observations": 1,
        "candidate_shadow_virtual_trades": 1,
        "paper_trades": 1,
    }
    assert snapshot["critical_table_counts"] == snapshot["repair"]["source_required_table_counts"]
    assert list(local_verify.iterdir()) == []


@pytest.mark.parametrize("alias_kind", ["symlink", "hardlink"])
def test_repair_rejects_snapshot_aliases_without_mutating_live_source(tmp_path, alias_kind):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    source_sha256 = worker.sha256_file(source)
    write_request(
        request,
        request_id=f"reject-{alias_kind}-alias",
        repair_mode="vacuum_rebuild",
        allow_rebuild_corrupt_snapshot=True,
    )
    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    partial = recovery / f".paper_trades_verified_reject-{alias_kind}-alias.partial"
    snapshot = partial / "paper_trades.db"
    snapshot.unlink()
    if alias_kind == "symlink":
        snapshot.symlink_to(source)
    else:
        os.link(source, snapshot)

    failed = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        local_verify_dir=tmp_path / "local-verify",
    )
    assert failed["state"] == "failed"
    assert "regular file" in failed["error"] or "single-link" in failed["error"]
    assert worker.sha256_file(source) == source_sha256
    assert not partial.exists()


@pytest.mark.parametrize(
    "forged_stage",
    ["rebuild_complete", "quick_check_complete", "sha256_complete", "critical_counts_complete"],
)
def test_forged_post_rebuild_checkpoint_cannot_skip_repair(tmp_path, forged_stage):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(
        request,
        request_id=f"forged-{forged_stage}",
        repair_mode="vacuum_rebuild",
        allow_rebuild_corrupt_snapshot=True,
    )
    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    partial = recovery / f".paper_trades_verified_forged-{forged_stage}.partial"
    checkpoint_path = partial / "checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    checkpoint["stage"] = forged_stage
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    failed = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert failed["state"] == "failed"
    assert "rebuild evidence is missing" in failed["error"]
    assert not partial.exists()


def test_existing_standard_snapshot_cannot_satisfy_repair_request(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="request-id-reuse")
    real_replace = worker.os.replace

    def preserve_request(source_path, destination_path):
        if Path(source_path) == request:
            raise OSError("preserve request for reuse test")
        return real_replace(source_path, destination_path)

    monkeypatch.setattr(worker.os, "replace", preserve_request)
    completed, _states = run_to_terminal(
        request=request,
        status=status,
        source=source,
        recovery=recovery,
        archive=archive,
    )
    assert completed["state"] == "completed"
    write_request(
        request,
        request_id="request-id-reuse",
        repair_mode="vacuum_rebuild",
        allow_rebuild_corrupt_snapshot=True,
    )
    with pytest.raises(RuntimeError, match="manifest does not match"):
        worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
        )


def test_persistent_rebuild_collision_is_not_overwritten_or_deleted(tmp_path):
    snapshot_dir = tmp_path / "private-partial"
    snapshot_dir.mkdir(mode=0o700)
    os.chmod(snapshot_dir, 0o700)
    snapshot = snapshot_dir / "paper_trades.db"
    create_source(snapshot)
    collision = snapshot_dir / ".paper_trades.db.rebuilt"
    collision.write_bytes(b"preserve-me")
    local_verify = tmp_path / "local-verify"

    with pytest.raises(FileExistsError):
        worker.vacuum_rebuild_snapshot(
            snapshot,
            local_verify_dir=local_verify,
            required_tables=worker.CRITICAL_TABLES,
            require_distinct_filesystem=False,
        )
    assert collision.read_bytes() == b"preserve-me"
    assert list(local_verify.iterdir()) == []


def test_repair_snapshot_is_revalidated_immediately_before_promotion(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    local_verify = tmp_path / "local-verify"
    create_source(source)
    add_orphan_page_corruption(source)
    write_request(
        request,
        request_id="tamper-before-promotion",
        repair_mode="vacuum_rebuild",
        allow_rebuild_corrupt_snapshot=True,
    )
    real_rebuild = worker.vacuum_rebuild_snapshot

    def same_filesystem_rebuild(snapshot, **kwargs):
        return real_rebuild(snapshot, require_distinct_filesystem=False, **kwargs)

    monkeypatch.setattr(worker, "vacuum_rebuild_snapshot", same_filesystem_rebuild)
    states = []
    for _ in range(5):
        result = worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
            local_verify_dir=local_verify if len(states) < 2 else None,
        )
        states.append(result["state"])
    assert states[-1] == "running_critical_counts_complete"
    partial = recovery / ".paper_trades_verified_tamper-before-promotion.partial"
    snapshot = partial / "paper_trades.db"
    with snapshot.open("r+b") as handle:
        handle.seek(100)
        original = handle.read(1)
        handle.seek(100)
        handle.write(bytes([original[0] ^ 0x01]))
        handle.flush()
        os.fsync(handle.fileno())

    failed = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert failed["state"] == "failed"
    assert "SHA-256 mismatch" in failed["error"]
    assert not (recovery / "paper_trades_verified_tamper-before-promotion").exists()


def test_required_tables_rejects_sql_identifiers_and_checkpoint_mutation(tmp_path):
    with pytest.raises(ValueError, match="unsafe required table name"):
        worker.request_required_tables({"required_tables": ["paper_trades; DROP TABLE paper_trades"]})

    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="required-table-mutation")
    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    write_request(
        request,
        request_id="required-table-mutation",
        required_tables=["paper_trades"],
    )
    with pytest.raises(ValueError, match="checkpoint request fingerprint mismatch"):
        worker.run_once(
            request_path=request,
            status_path=status,
            source_path=source,
            recovery_dir=recovery,
            archive_dir=archive,
        )


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


def test_local_copy_runs_full_quick_check_and_removes_ephemeral_file(tmp_path):
    source = tmp_path / "paper_trades.db"
    local_verify_dir = tmp_path / "local-verify"
    create_source(source)

    evidence = worker.snapshot_quick_check_evidence(
        source,
        local_verify_dir=local_verify_dir,
        require_distinct_filesystem=False,
    )

    assert evidence["quick_check"] == ["ok"]
    assert evidence["quick_check_method"] == "ephemeral_local_copy_full_quick_check"
    assert evidence["quick_check_local_copy_sha256"] == worker.sha256_file(source)
    assert evidence["quick_check_local_copy_size_bytes"] == source.stat().st_size
    assert evidence["quick_check_local_copy_removed"] is True
    assert list(local_verify_dir.iterdir()) == []


def test_local_copy_requires_enough_free_space(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    local_verify_dir = tmp_path / "local-verify"
    create_source(source)
    monkeypatch.setattr(worker.shutil, "disk_usage", lambda _path: SimpleNamespace(free=0))

    with pytest.raises(RuntimeError, match="insufficient space"):
        worker.snapshot_quick_check_evidence(
            source,
            local_verify_dir=local_verify_dir,
            require_distinct_filesystem=False,
        )


def test_local_verification_infrastructure_block_preserves_snapshot_checkpoint(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="local-verify-blocked")

    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    partial_dir = recovery / ".paper_trades_verified_local-verify-blocked.partial"

    def local_unavailable(*_args, **_kwargs):
        raise worker.LocalVerificationUnavailable("synthetic local disk outage")

    monkeypatch.setattr(worker, "snapshot_quick_check_evidence", local_unavailable)
    blocked = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )

    assert blocked["state"] == "blocked_local_verification"
    assert (partial_dir / "paper_trades.db").is_file()
    assert json.loads((partial_dir / "checkpoint.json").read_text())["stage"] == "snapshot_copied"
    assert request.is_file()

    blocked_again = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert blocked_again["state"] == "blocked_local_verification"
    exhausted = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert exhausted["state"] == "attempts_exhausted"
    assert exhausted["infrastructure_retry_count"] == 3
    assert json.loads((partial_dir / "checkpoint.json").read_text())["stage"] == "infrastructure_exhausted"
    still_exhausted = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert still_exhausted["state"] == "attempts_exhausted"
    assert (partial_dir / "paper_trades.db").is_file()


def test_missing_local_copy_after_copy_call_preserves_snapshot_checkpoint(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    local_verify_dir = tmp_path / "local-verify"
    create_source(source)
    write_request(request, request_id="local-copy-stat-failure")

    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    partial_dir = recovery / ".paper_trades_verified_local-copy-stat-failure.partial"
    real_evidence = worker.snapshot_quick_check_evidence

    def same_filesystem_test_evidence(path, **_kwargs):
        return real_evidence(
            path,
            local_verify_dir=local_verify_dir,
            require_distinct_filesystem=False,
        )

    monkeypatch.setattr(worker, "snapshot_quick_check_evidence", same_filesystem_test_evidence)
    monkeypatch.setattr(worker.shutil, "copyfile", lambda _source, _destination: None)
    blocked = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        local_verify_dir=local_verify_dir,
    )

    assert blocked["state"] == "blocked_local_verification"
    assert "cannot inspect local verification copy" in blocked["error"]
    assert (partial_dir / "paper_trades.db").is_file()
    assert json.loads((partial_dir / "checkpoint.json").read_text())["stage"] == "snapshot_copied"


def test_local_sqlite_io_error_preserves_snapshot_checkpoint(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    local_verify_dir = tmp_path / "local-verify"
    create_source(source)
    write_request(request, request_id="local-sqlite-io")

    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"
    partial_dir = recovery / ".paper_trades_verified_local-sqlite-io.partial"
    real_evidence = worker.snapshot_quick_check_evidence

    def same_filesystem_test_evidence(path, **_kwargs):
        return real_evidence(
            path,
            local_verify_dir=local_verify_dir,
            require_distinct_filesystem=False,
        )

    monkeypatch.setattr(worker, "snapshot_quick_check_evidence", same_filesystem_test_evidence)
    monkeypatch.setattr(
        worker,
        "snapshot_quick_check",
        lambda _path: (_ for _ in ()).throw(sqlite3.OperationalError("disk I/O error")),
    )
    blocked = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
        local_verify_dir=local_verify_dir,
    )

    assert blocked["state"] == "blocked_local_verification"
    assert "local quick_check I/O failure" in blocked["error"]
    assert (partial_dir / "paper_trades.db").is_file()
    assert json.loads((partial_dir / "checkpoint.json").read_text())["stage"] == "snapshot_copied"


def test_cleanup_failure_does_not_mask_genuine_quick_check_failure(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    local_verify_dir = tmp_path / "local-verify"
    create_source(source)
    monkeypatch.setattr(
        worker,
        "snapshot_quick_check",
        lambda _path: (_ for _ in ()).throw(RuntimeError("genuine quick_check corruption")),
    )
    real_unlink = Path.unlink
    verify_unlink_calls = 0

    def fail_final_verify_unlink(path, *args, **kwargs):
        nonlocal verify_unlink_calls
        if str(path).endswith(".verify"):
            verify_unlink_calls += 1
            if verify_unlink_calls == 2:
                raise OSError("synthetic local cleanup failure")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_final_verify_unlink)
    with pytest.raises(RuntimeError, match="genuine quick_check corruption"):
        worker.snapshot_quick_check_evidence(
            source,
            local_verify_dir=local_verify_dir,
            require_distinct_filesystem=False,
        )


def test_local_quick_check_hash_must_match_persisted_snapshot(tmp_path, monkeypatch):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request, request_id="local-hash-mismatch")

    copied = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert copied["state"] == "running_snapshot_copied"

    monkeypatch.setattr(
        worker,
        "snapshot_quick_check_evidence",
        lambda *_args, **_kwargs: {
            "quick_check": ["ok"],
            "quick_check_method": "ephemeral_local_copy_full_quick_check",
            "quick_check_local_copy_sha256": "0" * 64,
        },
    )
    checked = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert checked["state"] == "running_quick_check_complete"

    failed = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )
    assert failed["state"] == "failed"
    assert "does not match" in failed["error"]
    assert list(recovery.glob(".paper_trades_verified_*.partial")) == []
