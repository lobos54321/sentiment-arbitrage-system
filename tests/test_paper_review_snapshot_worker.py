import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for item in (ROOT, ROOT / "scripts"):
    if str(item) not in sys.path:
        sys.path.insert(0, str(item))

from scripts.paper_review_snapshot_worker import connect  # noqa: E402


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
