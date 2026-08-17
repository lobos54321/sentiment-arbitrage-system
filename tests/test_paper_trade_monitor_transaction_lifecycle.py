import os
from pathlib import Path
import sqlite3
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from paper_trade_monitor import _settle_monitor_iteration_transaction  # noqa: E402


def _open_delete_journal_db(path):
    db = sqlite3.connect(path, timeout=1)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("CREATE TABLE IF NOT EXISTS evidence (value TEXT)")
    db.commit()
    return db


def _assert_competing_writer_can_begin(path):
    competitor = sqlite3.connect(path, timeout=1)
    try:
        competitor.execute("BEGIN IMMEDIATE")
        competitor.rollback()
    finally:
        competitor.close()


def test_successful_monitor_iteration_commits_before_sleep(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('committed')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        assert _settle_monitor_iteration_transaction(db, success=True)

        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_failed_monitor_iteration_rolls_back_before_retry_sleep(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    root_error = LookupError("iteration failed")
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute("INSERT INTO evidence VALUES ('rolled-back')")
        assert db.in_transaction
        assert Path(f"{db_path}-journal").exists()

        assert _settle_monitor_iteration_transaction(
            db,
            success=False,
            root_error=root_error,
        )

        assert not db.in_transaction
        assert not Path(f"{db_path}-journal").exists()
        assert db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 0
        _assert_competing_writer_can_begin(db_path)
    finally:
        db.close()


def test_failed_monitor_iteration_preserves_root_when_rollback_fails():
    class FailingRollback:
        in_transaction = True

        def rollback(self):
            raise RuntimeError("injected rollback failure")

    root_error = LookupError("iteration failed")
    assert not _settle_monitor_iteration_transaction(
        FailingRollback(),
        success=False,
        root_error=root_error,
    )
    assert any(
        "paper_monitor_iteration_rollback_error RuntimeError" in note
        for note in root_error.__notes__
    )
