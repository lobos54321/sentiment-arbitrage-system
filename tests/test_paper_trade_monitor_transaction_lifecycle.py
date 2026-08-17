import os
from pathlib import Path
import sqlite3
import sys

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import paper_decision_audit  # noqa: E402
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


def test_failed_best_effort_audit_commit_releases_delete_journal_before_inner_sleep(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "paper.db"
    db = _open_delete_journal_db(db_path)
    reader = sqlite3.connect(db_path, timeout=1)
    try:
        db.execute(paper_decision_audit.CREATE_DECISION_AUDIT_SQL)
        db.execute(
            """
            INSERT INTO paper_decision_events
                (event_ts, component, event_type, decision)
            VALUES (1, 'baseline', 'baseline', 'baseline')
            """
        )
        db.commit()

        reader.execute("BEGIN")
        assert reader.execute(
            "SELECT id FROM paper_decision_events ORDER BY id"
        ).fetchone()

        monkeypatch.setattr(
            paper_decision_audit,
            "append_paper_evidence_event",
            lambda **_kwargs: None,
        )
        monkeypatch.setattr(
            paper_decision_audit,
            "_maybe_record_missed_attribution",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            paper_decision_audit,
            "_mirror_v27_decision_event",
            lambda **_kwargs: None,
        )

        paper_decision_audit.record_decision_event(
            db,
            component="signal_ingest",
            event_type="signal_received",
            decision="received",
        )

        def assert_safe_inner_sleep(_seconds):
            assert not db.in_transaction
            assert not Path(f"{db_path}-journal").exists()
            _assert_competing_writer_can_begin(db_path)

        assert_safe_inner_sleep(0.1)
        assert db.execute(
            "SELECT COUNT(*) FROM paper_decision_events"
        ).fetchone()[0] == 1
    finally:
        reader.rollback()
        reader.close()
        db.close()


def test_failed_best_effort_audit_does_not_swallow_an_uncleared_transaction(
    monkeypatch,
):
    class UnclearableAuditDb:
        in_transaction = True

        def execute(self, *_args, **_kwargs):
            raise LookupError("injected audit insert failure")

        def rollback(self):
            raise RuntimeError("injected audit rollback failure")

    monkeypatch.setattr(
        paper_decision_audit,
        "append_paper_evidence_event",
        lambda **_kwargs: None,
    )

    with pytest.raises(LookupError, match="injected audit insert failure") as caught:
        paper_decision_audit.record_decision_event(
            UnclearableAuditDb(),
            component="signal_ingest",
            event_type="signal_received",
            decision="received",
        )

    assert any(
        "paper_decision_audit_rollback_error RuntimeError" in note
        for note in caught.value.__notes__
    )
