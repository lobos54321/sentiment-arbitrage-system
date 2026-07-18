from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from evidence_clock_audit import build_report as build_clock_report  # noqa: E402
from kline_db_health_audit import build_report as build_health_report  # noqa: E402
import kline_db_recovery as recovery  # noqa: E402
from kline_db_recovery import build_plan, execute_recovery, validate_approval, write_markers  # noqa: E402
from phase4_h1_recovery_approval_packet import (  # noqa: E402
    build_packet,
    REQUIRED_CHANGED_FILES,
    REQUIRED_TESTS,
)
from sqlite_evidence_snapshot import create_snapshot  # noqa: E402
from sqlite_evidence_utils import (  # noqa: E402
    find_process_references,
    inspect_sqlite,
    open_sqlite_readonly,
    sha256_file,
)


def make_sqlite(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE kline_1m (token_ca TEXT, timestamp INTEGER)")
    connection.execute("INSERT INTO kline_1m VALUES ('DOG', 123)")
    connection.commit()
    connection.close()


def test_health_audit_is_read_only_for_zero_header(tmp_path: Path) -> None:
    target = tmp_path / "zero.db"
    target.write_bytes(b"\x00" * 8192)
    before = sha256_file(target)
    report = build_health_report(str(target), [], proc_root=str(tmp_path / "missing-proc"))
    assert report["classification"] == "INVALID_HEADER"
    assert report["mutation_performed"] is False
    assert sha256_file(target) == before


def test_readonly_kline_open_does_not_create_a_missing_database(tmp_path: Path) -> None:
    target = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        open_sqlite_readonly(target)
    assert not target.exists()


def test_readonly_kline_open_rejects_invalid_header_without_opening_sidecars(tmp_path: Path) -> None:
    target = tmp_path / "zero.db"
    target.write_bytes(b"\x00" * 8192)
    before = sha256_file(target)
    with pytest.raises(sqlite3.DatabaseError, match="invalid SQLite header"):
        open_sqlite_readonly(target)
    assert sha256_file(target) == before
    assert not Path(f"{target}-wal").exists()
    assert not Path(f"{target}-shm").exists()


def test_paper_monitor_has_no_creating_kline_connect_calls() -> None:
    source = (SCRIPTS / "paper_trade_monitor.py").read_text(encoding="utf-8")
    assert "sqlite3.connect(KLINE_DB)" not in source
    assert "open_sqlite_readonly(KLINE_DB" in source
    assert source.count("with closing(open_sqlite_readonly(KLINE_DB") == 2


def test_consistent_snapshot_preserves_source(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    destination = tmp_path / "snapshot.db"
    make_sqlite(source)
    before = sha256_file(source)
    manifest = create_snapshot(str(source), str(destination))
    assert manifest["snapshot"]["classification"] == "HEALTHY"
    assert sha256_file(source) == before


def test_recovery_plan_defaults_to_dry_run(tmp_path: Path) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    before = sha256_file(target)
    plan = build_plan(
        str(target),
        now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        proc_root=str(tmp_path / "missing-proc"),
    )
    assert plan["mode"] == "dry_run"
    assert plan["mutation_performed"] is False
    assert not Path(plan["proposed_quarantine_path"]).exists()
    assert sha256_file(target) == before


def test_execute_recovery_requires_real_process_visibility(tmp_path: Path) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    plan = build_plan(
        str(target),
        now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        proc_root=str(tmp_path / "missing-proc"),
    )
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        sha256_file(target),
        tmp_path / "broken.db.quarantine-approved",
    )
    with pytest.raises(RuntimeError, match="process reference audit is unavailable"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(tmp_path / "missing-proc"),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )


def test_approval_rejects_quarantine_outside_target_directory(tmp_path: Path) -> None:
    target = tmp_path / "data" / "broken.db"
    target.parent.mkdir()
    target.write_bytes(b"\x00" * 4096)
    marker = tmp_path / "approval.json"
    marker.write_text(json.dumps({
        "approval_type": "phase4_kline_recovery_h1",
        "approval_id": "safe-id",
        "approved": True,
        "operator": "operator",
        "target_db": str(target),
        "expected_source_sha256": sha256_file(target),
        "quarantine_path": str(tmp_path / "outside" / "broken.db.quarantine-bad"),
        "expires_at": "2999-01-01T00:00:00Z",
    }))
    with pytest.raises(PermissionError, match="versioned sibling"):
        validate_approval(
            str(marker),
            target=target.resolve(),
            operator="operator",
            source_health=inspect_sqlite(target),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )


def test_execute_recovery_blocks_partial_process_visibility(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    before = sha256_file(target)
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(tmp_path))
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        before,
        tmp_path / "broken.db.quarantine-approved",
    )
    monkeypatch.setattr(recovery, "find_process_references", lambda *args, **kwargs: {
        "available": True,
        "errors": ["pid=999: permission denied"],
        "references": [],
    })
    with pytest.raises(RuntimeError, match="process reference audit is incomplete"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(tmp_path),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert sha256_file(target) == before


def test_process_scan_reports_readlink_permission_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    proc_root = tmp_path / "proc"
    fd_dir = proc_root / "123" / "fd"
    fd_dir.mkdir(parents=True)
    fd_path = fd_dir / "7"
    fd_path.write_text("placeholder")
    real_readlink = os.readlink

    def denied_readlink(path):
        if Path(path) == fd_path:
            raise PermissionError("permission denied")
        return real_readlink(path)

    monkeypatch.setattr(os, "readlink", denied_readlink)
    report = find_process_references(tmp_path / "kline.db", proc_root=proc_root)
    assert report["references"] == []
    assert len(report["errors"]) == 1
    assert "pid=123 fd=7 readlink_failed" in report["errors"][0]


def test_execute_recovery_does_not_overwrite_quarantined_sidecar(tmp_path: Path) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    Path(f"{target}-wal").write_bytes(b"source-wal")
    quarantine = tmp_path / "broken.db.quarantine-approved"
    quarantined_wal = Path(f"{quarantine}-wal")
    quarantined_wal.write_bytes(b"existing-evidence")
    empty_proc = tmp_path / "proc"
    empty_proc.mkdir()
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
    approval, maintenance = write_markers(tmp_path / "markers", target, sha256_file(target), quarantine)
    with pytest.raises(FileExistsError, match="sidecar quarantine"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert target.exists()
    assert Path(f"{target}-wal").read_bytes() == b"source-wal"
    assert quarantined_wal.read_bytes() == b"existing-evidence"


def test_execute_recovery_does_not_overwrite_sidecar_created_after_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    target_wal = Path(f"{target}-wal")
    target_wal.write_bytes(b"source-wal")
    quarantine = tmp_path / "broken.db.quarantine-approved"
    quarantined_wal = Path(f"{quarantine}-wal")
    empty_proc = tmp_path / "proc"
    empty_proc.mkdir()
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
    approval, maintenance = write_markers(tmp_path / "markers", target, sha256_file(target), quarantine)
    real_initialize = recovery.initialize_temp_database

    def initialize_with_racing_evidence(temp_path, schema_sql):
        health = real_initialize(temp_path, schema_sql)
        quarantined_wal.write_bytes(b"late-forensic-evidence")
        return health

    monkeypatch.setattr(recovery, "initialize_temp_database", initialize_with_racing_evidence)
    with pytest.raises(FileExistsError):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert target.exists()
    assert target_wal.read_bytes() == b"source-wal"
    assert quarantined_wal.read_bytes() == b"late-forensic-evidence"


def test_execute_recovery_refuses_missing_target_with_orphan_sidecars(tmp_path: Path) -> None:
    target = tmp_path / "missing.db"
    target_wal = Path(f"{target}-wal")
    target_wal.write_bytes(b"orphan-wal")
    empty_proc = tmp_path / "proc"
    empty_proc.mkdir()
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        None,
        tmp_path / "missing.db.quarantine-approved",
    )
    with pytest.raises(RuntimeError, match="orphan SQLite sidecars"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert not target.exists()
    assert target_wal.read_bytes() == b"orphan-wal"


def test_missing_target_is_restored_to_missing_after_post_install_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "missing.db"
    empty_proc = tmp_path / "proc"
    empty_proc.mkdir()
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        None,
        tmp_path / "missing.db.quarantine-approved",
    )
    monkeypatch.setattr(
        recovery,
        "snapshot_last_known_good",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("forced lkg failure")),
    )
    with pytest.raises(RuntimeError, match="forced lkg failure"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert not target.exists()
    failed = list(tmp_path.glob("missing.db.failed-recovery-self-test-h1-*.sqlite"))
    assert len(failed) == 0
    failed = list(tmp_path.glob("missing.db.failed-recovery-self-test-h1-*"))
    assert len(failed) == 1
    assert inspect_sqlite(failed[0])["classification"] == "HEALTHY"


def test_recovery_rescans_processes_inside_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    before = sha256_file(target)
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(tmp_path))
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        before,
        tmp_path / "broken.db.quarantine-approved",
    )
    calls = {"count": 0}

    def fake_references(*args, **kwargs):
        calls["count"] += 1
        return {
            "available": True,
            "errors": [],
            "references": [] if calls["count"] == 1 else [{"pid": 999, "command": "late-writer", "fds": []}],
        }

    monkeypatch.setattr(recovery, "find_process_references", fake_references)
    with pytest.raises(RuntimeError, match="appeared after recovery lock"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(tmp_path),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert calls["count"] == 2
    assert sha256_file(target) == before


def test_recovery_revalidates_source_hash_inside_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "broken.db"
    target.write_bytes(b"\x00" * 4096)
    before = sha256_file(target)
    empty_proc = tmp_path / "proc"
    empty_proc.mkdir()
    plan = build_plan(str(target), now=datetime(2026, 7, 10, tzinfo=timezone.utc), proc_root=str(empty_proc))
    approval, maintenance = write_markers(
        tmp_path / "markers",
        target,
        before,
        tmp_path / "broken.db.quarantine-approved",
    )
    real_inspect = recovery.inspect_sqlite
    calls = {"count": 0}

    def changed_after_lock(path):
        calls["count"] += 1
        health = real_inspect(path)
        if calls["count"] >= 2:
            health = {**health, "sha256": "changed-after-first-check"}
        return health

    monkeypatch.setattr(recovery, "inspect_sqlite", changed_after_lock)
    with pytest.raises(PermissionError, match="after recovery lock acquisition"):
        execute_recovery(
            plan,
            approval_marker=str(approval),
            maintenance_marker=str(maintenance),
            operator="self-test",
            proc_root=str(empty_proc),
            now=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
    assert calls["count"] == 2
    assert sha256_file(target) == before


def test_evidence_clock_detects_mixed_run_lineage(tmp_path: Path) -> None:
    status = tmp_path / "status.json"
    capture = tmp_path / "capture.json"
    status.write_text(json.dumps({"run_id": "a", "generated_at": "2026-07-10T00:00:00Z"}))
    capture.write_text(json.dumps({"run_id": "b", "generated_at": "2026-07-09T23:59:00Z"}))
    report = build_clock_report(
        [("latest_status", status), ("primary_capture", capture)],
        now=datetime(2026, 7, 10, tzinfo=timezone.utc),
    )
    assert report["classification"] == "MIXED_RUN_LINEAGE"


def test_h1_packet_never_grants_approval() -> None:
    packet = build_packet(
        {
            "classification": "INVALID_HEADER",
            "mutation_performed": False,
            "primary": {
                "classification": "INVALID_HEADER",
                "path": "/app/data/kline_cache.db",
                "sha256": "abc",
            },
            "process_references": {"available": True, "errors": [], "references": []},
        },
        {"classification": "STALE_INPUT"},
        {
            "mode": "dry_run",
            "mutation_performed": False,
            "target_db": "/app/data/kline_cache.db",
            "proposed_quarantine_path": "/app/data/kline_cache.db.quarantine-approved",
            "process_references": {"available": True, "errors": [], "references": []},
        },
        {
            "tests_passed": True,
            "results": [{"name": name, "exit_code": 0} for name in sorted(REQUIRED_TESTS)],
        },
        {
            "changed_files": sorted(REQUIRED_CHANGED_FILES),
            "scope_verified_against_base": True,
            "base_commit": "base",
            "candidate_commit": "candidate",
        },
    )
    assert packet["ready_for_human_review"] is True
    assert packet["approval_granted"] is False
    assert packet["promotion_allowed"] is False


def test_h1_packet_rejects_local_fixture_as_production_evidence(tmp_path: Path) -> None:
    target = tmp_path / "kline_cache.db"
    packet = build_packet(
        {
            "classification": "INVALID_HEADER",
            "mutation_performed": False,
            "primary": {"classification": "INVALID_HEADER", "path": str(target), "sha256": "abc"},
            "process_references": {"available": False, "errors": [], "references": []},
        },
        {"classification": "STALE_INPUT"},
        {
            "mode": "dry_run",
            "mutation_performed": False,
            "target_db": str(target),
            "proposed_quarantine_path": f"{target}.quarantine-approved",
            "process_references": {"available": False, "errors": [], "references": []},
        },
        {
            "tests_passed": True,
            "results": [{"name": name, "exit_code": 0} for name in sorted(REQUIRED_TESTS)],
        },
        {
            "changed_files": sorted(REQUIRED_CHANGED_FILES),
            "scope_verified_against_base": True,
            "base_commit": "base",
            "candidate_commit": "candidate",
        },
    )
    assert packet["ready_for_human_review"] is False
    assert packet["inputs"]["target_matches_production"] is False
    assert packet["inputs"]["process_visibility_complete"] is False
