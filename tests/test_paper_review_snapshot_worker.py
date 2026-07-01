import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for item in (ROOT, ROOT / "scripts"):
    if str(item) not in sys.path:
        sys.path.insert(0, str(item))

from scripts.paper_review_snapshot_worker import build_snapshot, connect  # noqa: E402


def test_paper_review_snapshot_worker_opens_paper_db_readonly(tmp_path):
    db_path = tmp_path / "paper_trades.db"
    db = sqlite3.connect(db_path)
    db.execute("CREATE TABLE trades(id INTEGER PRIMARY KEY)")
    db.commit()
    db.close()

    ro = connect(db_path)
    try:
        try:
            ro.execute("INSERT INTO trades(id) VALUES (1)")
        except sqlite3.OperationalError as exc:
            assert "readonly" in str(exc).lower()
        else:
            raise AssertionError("paper review snapshot worker connection allowed a write")
    finally:
        ro.close()


def test_paper_review_snapshot_defaults_to_freshness_first_p0_skip(tmp_path):
    db_path = tmp_path / "paper_trades.db"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        snapshot = build_snapshot(db, hours=24, limit=5)
    finally:
        db.close()

    assert snapshot["snapshot_freshness_policy"]["a_class_p0_discovery_mode"] == "skip"
    assert snapshot["a_class_p0_discovery"]["available"] is False
    assert snapshot["a_class_p0_discovery"]["reason"] == "p0_discovery_skipped_for_snapshot_freshness"
    assert snapshot["a_class"]["p0_discovery"]["reason"] == "p0_discovery_skipped_for_snapshot_freshness"
    assert snapshot["strategy_goal_controller"]["actions"][0]["action"] == "SHADOW"
