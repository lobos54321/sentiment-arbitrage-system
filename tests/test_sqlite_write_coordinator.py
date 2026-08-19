import sqlite3

import pytest

from scripts.sqlite_write_coordinator import configure_paper_sqlite_connection


def test_paper_connection_defaults_to_rollback_journal_and_full_sync(tmp_path, monkeypatch):
    monkeypatch.delenv("PAPER_SQLITE_JOURNAL_MODE", raising=False)
    monkeypatch.delenv("PAPER_SQLITE_SYNCHRONOUS", raising=False)
    db = sqlite3.connect(tmp_path / "paper.db")
    try:
        result = configure_paper_sqlite_connection(db)
        assert result["journal_mode"] == "DELETE"
        assert result["synchronous"] == 2
        assert db.execute("PRAGMA journal_mode").fetchone()[0].upper() == "DELETE"
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
    finally:
        db.close()


def test_paper_connection_allows_explicit_local_wal_override(tmp_path):
    db = sqlite3.connect(tmp_path / "paper.db")
    try:
        result = configure_paper_sqlite_connection(
            db,
            journal_mode="WAL",
            synchronous="NORMAL",
        )
        assert result["journal_mode"] == "WAL"
        assert result["synchronous"] == 1
    finally:
        db.close()


def test_explicit_local_wal_reader_does_not_block_writer_commit(tmp_path):
    path = tmp_path / "paper.db"
    setup = sqlite3.connect(path)
    try:
        configure_paper_sqlite_connection(
            setup,
            journal_mode="WAL",
            synchronous="FULL",
        )
        setup.execute("CREATE TABLE evidence(value TEXT)")
        setup.execute("INSERT INTO evidence VALUES ('before-reader')")
        setup.commit()
    finally:
        setup.close()

    reader = sqlite3.connect(path, timeout=0.2)
    writer = sqlite3.connect(path, timeout=0.2)
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        configure_paper_sqlite_connection(
            writer,
            journal_mode="WAL",
            synchronous="FULL",
            busy_timeout_ms=200,
        )
        writer.execute("INSERT INTO evidence VALUES ('during-reader')")
        writer.commit()
        assert reader.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 1
        reader.rollback()
        assert reader.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] == 2
    finally:
        if reader.in_transaction:
            reader.rollback()
        if writer.in_transaction:
            writer.rollback()
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    ("journal_mode", "synchronous"),
    [("unsafe-name", "FULL"), ("DELETE", "sometimes")],
)
def test_paper_connection_rejects_unknown_pragma_values(
    tmp_path, journal_mode, synchronous
):
    db = sqlite3.connect(tmp_path / "paper.db")
    try:
        with pytest.raises(ValueError):
            configure_paper_sqlite_connection(
                db,
                journal_mode=journal_mode,
                synchronous=synchronous,
            )
    finally:
        db.close()
