import gzip
import json
import sqlite3
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import paper_db_retention as retention  # noqa: E402


def _read_archive_rows(archive_dir):
    rows = []
    for path in archive_dir.rglob("*.jsonl.gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            rows.extend(json.loads(line) for line in fh if line.strip())
    return rows


def test_report_mode_counts_without_archiving_or_deleting(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    db = sqlite3.connect(db_path)
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL NOT NULL,
            component TEXT,
            event_type TEXT,
            decision TEXT
        )
        """
    )
    db.execute(
        "INSERT INTO paper_decision_events (event_ts, component, event_type, decision) VALUES (?, 'gate', 'decision', 'reject')",
        (1_000,),
    )
    db.commit()
    db.close()

    monkeypatch.setenv("PAPER_DB_RETENTION_DECISION_DAYS", "30")
    summary = retention.run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="report",
        now_ts=1_000 + 31 * 86400,
    )

    assert summary["status"] == "ok"
    decision = next(item for item in summary["policies"] if item["table"] == "paper_decision_events")
    assert decision["eligible"] == 1
    assert decision["deleted"] == 0
    assert not archive_dir.exists()
    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM paper_decision_events").fetchone()[0] == 1
    db.close()


def test_apply_mode_archives_then_prunes_old_rows_only(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    old_ts = 1_000
    new_ts = old_ts + 40 * 86400
    db = sqlite3.connect(db_path)
    db.execute("CREATE TABLE paper_trades (id INTEGER PRIMARY KEY, exit_reason TEXT, exit_ts INTEGER)")
    db.execute(
        """
        CREATE TABLE paper_trade_path_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER NOT NULL,
            sample_ts INTEGER NOT NULL,
            payload_json TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL NOT NULL,
            component TEXT,
            event_type TEXT,
            decision TEXT
        )
        """
    )
    db.execute("INSERT INTO paper_trades (id, exit_reason, exit_ts) VALUES (1, 'take_profit', ?)", (old_ts + 60,))
    db.execute("INSERT INTO paper_trades (id, exit_reason, exit_ts) VALUES (2, NULL, NULL)")
    db.execute("INSERT INTO paper_trade_path_samples (trade_id, sample_ts, payload_json) VALUES (1, ?, 'old closed')", (old_ts,))
    db.execute("INSERT INTO paper_trade_path_samples (trade_id, sample_ts, payload_json) VALUES (2, ?, 'old open')", (old_ts,))
    db.execute("INSERT INTO paper_trade_path_samples (trade_id, sample_ts, payload_json) VALUES (1, ?, 'new closed')", (new_ts,))
    db.execute(
        "INSERT INTO paper_decision_events (event_ts, component, event_type, decision) VALUES (?, 'gate', 'decision', 'reject')",
        (old_ts,),
    )
    db.execute(
        "INSERT INTO paper_decision_events (event_ts, component, event_type, decision) VALUES (?, 'gate', 'decision', 'pass')",
        (new_ts,),
    )
    db.commit()
    db.close()

    monkeypatch.setenv("PAPER_DB_RETENTION_DECISION_DAYS", "30")
    monkeypatch.setenv("PAPER_DB_RETENTION_PATH_SAMPLE_DAYS", "30")
    summary = retention.run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="apply",
        now_ts=old_ts + 31 * 86400,
        batch_size=2,
        max_rows_per_table=10,
        max_rows_total=20,
        max_seconds=0,
    )

    assert summary["status"] == "ok"
    assert summary["total_archived"] == 2
    assert summary["total_deleted"] == 2
    archived_rows = _read_archive_rows(archive_dir)
    archived_payloads = {row.get("payload_json") for row in archived_rows}
    assert "old closed" in archived_payloads
    assert any(row.get("decision") == "reject" for row in archived_rows)

    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    assert db.execute("SELECT COUNT(*) FROM paper_decision_events").fetchone()[0] == 1
    samples = [dict(row) for row in db.execute("SELECT trade_id, payload_json FROM paper_trade_path_samples ORDER BY id")]
    assert samples == [
        {"trade_id": 2, "payload_json": "old open"},
        {"trade_id": 1, "payload_json": "new closed"},
    ]
    assert db.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0] == 2
    db.close()


def test_archive_mode_preserves_hot_db_and_writes_manifests(tmp_path, monkeypatch):
    db_path = tmp_path / "paper_trades.db"
    archive_dir = tmp_path / "archive"
    db = sqlite3.connect(db_path)
    db.execute(
        """
        CREATE TABLE lotto_not_ath_watch_shadow_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT NOT NULL,
            parent_blocker TEXT NOT NULL,
            snapshot_ts REAL NOT NULL
        )
        """
    )
    db.execute(
        "INSERT INTO lotto_not_ath_watch_shadow_snapshots (token_ca, parent_blocker, snapshot_ts) VALUES ('TokenA', 'lotto_stale', ?)",
        (1_000,),
    )
    db.commit()
    db.close()

    monkeypatch.setenv("PAPER_DB_RETENTION_WATCH_SHADOW_DAYS", "30")
    summary = retention.run_retention(
        db_path=db_path,
        archive_dir=archive_dir,
        mode="archive",
        now_ts=1_000 + 31 * 86400,
        batch_size=1,
        max_seconds=0,
    )

    assert summary["total_archived"] == 1
    assert summary["total_deleted"] == 0
    manifests = list(archive_dir.rglob("*.manifest.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["row_count"] == 1
    db = sqlite3.connect(db_path)
    assert db.execute("SELECT COUNT(*) FROM lotto_not_ath_watch_shadow_snapshots").fetchone()[0] == 1
    db.close()
