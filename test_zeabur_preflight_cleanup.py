import json
import sqlite3
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import zeabur_preflight_cleanup as preflight  # noqa: E402


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
