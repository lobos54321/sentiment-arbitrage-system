import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import zeabur_preflight_cleanup as preflight


def test_paper_backup_uses_atomic_consistent_snapshot(tmp_path, monkeypatch):
    paper = tmp_path / "paper_trades.db"
    connection = sqlite3.connect(paper)
    connection.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("INSERT INTO evidence(value) VALUES ('preserved')")
    connection.commit()
    connection.close()

    backup_dir = tmp_path / "backup" / "paper-db-family"
    incomplete = backup_dir / "paper_trades_20200101T000000Z"
    incomplete.mkdir(parents=True)
    (incomplete / "paper_trades.db").write_bytes(b"incomplete")
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_ENABLED", True)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_DIR", backup_dir)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_MIN_INTERVAL_SEC", 0)

    preflight.backup_db_family(paper)

    complete = preflight.complete_paper_db_backups()
    assert len(complete) == 1
    assert incomplete.exists()
    assert list(backup_dir.glob(".paper_trades_*.partial")) == []
    manifest = json.loads((complete[0] / "manifest.json").read_text())
    assert manifest["snapshot"]["method"] == "sqlite_online_backup"
    assert manifest["snapshot"]["quick_check"] == ["ok"]
    snapshot = sqlite3.connect(complete[0] / "paper_trades.db")
    assert snapshot.execute("SELECT value FROM evidence").fetchone()[0] == "preserved"
    snapshot.close()


def test_backup_retention_counts_only_manifest_complete_snapshots(tmp_path, monkeypatch):
    backup_dir = tmp_path / "backup" / "paper-db-family"
    for suffix in ("01", "02", "03"):
        directory = backup_dir / f"paper_trades_202001{suffix}T000000Z"
        directory.mkdir(parents=True)
        (directory / "paper_trades.db").write_bytes(b"SQLite format 3\x00")
        (directory / "manifest.json").write_text("{}", encoding="utf-8")
    incomplete = backup_dir / "paper_trades_20200104T000000Z"
    incomplete.mkdir()
    (incomplete / "paper_trades.db").write_bytes(b"incomplete")
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_DIR", backup_dir)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_KEEP", 2)

    preflight.prune_complete_paper_db_backups()

    assert [path.name for path in preflight.complete_paper_db_backups()] == [
        "paper_trades_20200102T000000Z",
        "paper_trades_20200103T000000Z",
    ]
    assert incomplete.exists()


def test_stale_partial_cleanup_never_touches_fresh_partial(tmp_path, monkeypatch):
    backup_dir = tmp_path / "backup" / "paper-db-family"
    stale = backup_dir / ".paper_trades_stale.partial"
    fresh = backup_dir / ".paper_trades_fresh.partial"
    stale.mkdir(parents=True)
    fresh.mkdir()
    old = time.time() - 120
    os.utime(stale, (old, old))
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_DIR", backup_dir)
    monkeypatch.setattr(preflight, "PAPER_DB_BACKUP_PARTIAL_MAX_AGE_SEC", 60)

    preflight.cleanup_stale_backup_partials()

    assert not stale.exists()
    assert fresh.exists()


def test_checkpoint_quarantines_zero_byte_paper_db(tmp_path, monkeypatch):
    paper = tmp_path / "paper_trades.db"
    paper.write_bytes(b"")
    recovery_dir = tmp_path / "recovery"
    monkeypatch.setattr(preflight, "RECOVERY_DIR", recovery_dir)

    preflight.checkpoint_db(paper)

    assert not paper.exists()
    quarantines = list(recovery_dir.glob("paper_trades_corrupt_*"))
    assert len(quarantines) == 1
    manifest = json.loads((quarantines[0] / "manifest.json").read_text())
    assert "zero-byte paper DB" in manifest["reason"]
    moved_names = {item["to"].split("/")[-1] for item in manifest["moved"]}
    assert "paper_trades.db" in moved_names
    assert "paper_trades.db.integrity_error" in moved_names


def test_checkpoint_quarantines_existing_integrity_marker(tmp_path, monkeypatch):
    paper = tmp_path / "paper_trades.db"
    paper.write_bytes(b"SQLite format 3\x00" + b"\x00" * 128)
    marker = tmp_path / "paper_trades.db.integrity_error"
    marker.write_text("context=pending_entry\nerror=database disk image is malformed\n", encoding="utf-8")
    recovery_dir = tmp_path / "recovery"
    monkeypatch.setattr(preflight, "RECOVERY_DIR", recovery_dir)

    preflight.checkpoint_db(paper)

    assert not paper.exists()
    assert not marker.exists()
    quarantines = list(recovery_dir.glob("paper_trades_corrupt_*"))
    assert len(quarantines) == 1
    manifest = json.loads((quarantines[0] / "manifest.json").read_text())
    assert "database disk image is malformed" in manifest["reason"]
    moved_names = {item["to"].split("/")[-1] for item in manifest["moved"]}
    assert "paper_trades.db" in moved_names
    assert "paper_trades.db.integrity_error" in moved_names
