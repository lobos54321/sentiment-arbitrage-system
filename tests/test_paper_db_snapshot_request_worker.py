import json
import os
import sqlite3
import sys

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


def test_snapshot_request_creates_verified_snapshot_and_archives_request(tmp_path):
    source = tmp_path / "paper_trades.db"
    request = tmp_path / "request.json"
    status = tmp_path / "status.json"
    recovery = tmp_path / "recovery"
    archive = tmp_path / "requests"
    create_source(source)
    write_request(request)

    result = worker.run_once(
        request_path=request,
        status_path=status,
        source_path=source,
        recovery_dir=recovery,
        archive_dir=archive,
    )

    assert result["state"] == "completed"
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
    assert request.exists()
