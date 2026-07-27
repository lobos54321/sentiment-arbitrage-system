import json
from pathlib import Path
import sqlite3
import sys

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from kline_sqlite_durability import (  # noqa: E402
    configure_kline_sqlite_connection,
    kline_single_writer,
)


def test_kline_connection_uses_delete_full_and_no_mmap(tmp_path):
    db_path = tmp_path / "kline.db"
    db = sqlite3.connect(db_path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO evidence (value) VALUES ('committed')")
    db.commit()
    settings = configure_kline_sqlite_connection(db)
    assert settings == {
        "journal_mode": "DELETE",
        "synchronous": "FULL",
        "mmap_size": 0,
        "busy_timeout_ms": 30000,
    }
    assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert db.execute("SELECT value FROM evidence").fetchone()[0] == "committed"
    db.close()
    assert not Path(f"{db_path}-wal").exists()
    assert not Path(f"{db_path}-shm").exists()


def test_kline_writer_lock_blocks_concurrent_owner(tmp_path):
    lock_path = tmp_path / "writer.lock"
    with kline_single_writer("first", lock_file=lock_path):
        with pytest.raises(TimeoutError, match="timed out waiting"):
            with kline_single_writer("second", lock_file=lock_path, timeout_sec=0.02):
                pass
    assert not lock_path.exists()


def test_kline_writer_lock_recovers_dead_owner(tmp_path):
    lock_path = tmp_path / "writer.lock"
    lock_path.write_text(
        json.dumps(
            {
                "schema_version": "kline_sqlite_writer_lock.v1",
                "token": "dead-owner",
                "pid": 2_147_483_647,
                "owner": "dead-test-owner",
            }
        ),
        encoding="utf-8",
    )
    with kline_single_writer("replacement", lock_file=lock_path, timeout_sec=0.1):
        assert lock_path.exists()
    assert not lock_path.exists()


def test_kline_writer_lock_honors_environment_override(tmp_path, monkeypatch):
    lock_path = tmp_path / "environment.lock"
    monkeypatch.setenv("KLINE_SQLITE_WRITER_LOCK_FILE", str(lock_path))
    with kline_single_writer("environment-owner"):
        assert lock_path.exists()
    assert not lock_path.exists()
