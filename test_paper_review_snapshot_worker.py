import sqlite3
import time

from scripts.paper_review_snapshot_worker import build_snapshot


def test_review_snapshot_worker_handles_legacy_schema(tmp_path):
    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE paper_trades (
          id INTEGER PRIMARY KEY,
          token_ca TEXT,
          symbol TEXT,
          signal_ts INTEGER,
          entry_ts INTEGER,
          exit_ts INTEGER,
          entry_mode TEXT,
          pnl_pct REAL,
          peak_pnl REAL,
          position_size_sol REAL
        );
        CREATE TABLE paper_missed_signal_attribution (
          id INTEGER PRIMARY KEY,
          created_event_ts REAL,
          token_ca TEXT,
          symbol TEXT,
          route TEXT,
          component TEXT,
          reject_reason TEXT,
          max_pnl_recorded REAL,
          tradable_missed INTEGER,
          would_stop_before_peak INTEGER
        );
        """
    )
    now_ts = int(time.time())
    db.execute(
        """
        INSERT INTO paper_trades
          (token_ca, symbol, signal_ts, entry_ts, exit_ts, entry_mode, pnl_pct, peak_pnl, position_size_sol)
        VALUES
          ('T1', 'DOG', ?, ?, ?, 'hard_gate_pass_tiny_probe', 0.12, 0.31, 0.002)
        """,
        (now_ts - 60, now_ts - 60, now_ts - 30),
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
          (created_event_ts, token_ca, symbol, route, component, reject_reason, max_pnl_recorded, tradable_missed, would_stop_before_peak)
        VALUES
          (?, 'M1', 'MISS', 'ATH', 'entry', 'tracking_ttl_expired', 1.25, 1, 0)
        """,
        (now_ts - 60,),
    )
    db.commit()

    snapshot = build_snapshot(db, 24, 10)

    assert snapshot["missed"]["available"] is True
    assert snapshot["missed"]["overall"]["unique_tokens"] == 1
    assert snapshot["missed"]["overall"]["gold_unique"] == 1
    assert snapshot["trades"]["available"] is True
    assert snapshot["trades"]["totals"]["total"] == 1
    assert snapshot["trades"]["by_mode"][0]["entry_mode"] == "hard_gate_pass_tiny_probe"
