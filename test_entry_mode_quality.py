import sqlite3

from scripts.entry_mode_quality import evaluate_entry_mode_quality, recent_entry_mode_stats


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            entry_mode TEXT,
            replay_source TEXT,
            entry_ts INTEGER,
            exit_ts INTEGER,
            peak_pnl REAL,
            pnl_pct REAL
        )
        """
    )
    return db


def _insert(db, entry_mode, peak, pnl, ts):
    db.execute(
        """
        INSERT INTO paper_trades(entry_mode, replay_source, entry_ts, exit_ts, peak_pnl, pnl_pct)
        VALUES (?, 'live_monitor', ?, ?, ?, ?)
        """,
        (entry_mode, ts, ts + 60, peak, pnl),
    )


def test_entry_mode_quality_insufficient_samples_allows_live():
    db = _db()
    for idx in range(3):
        _insert(db, "matrix_reclaim_tiny_probe", 0.0, -0.10, idx)

    decision = evaluate_entry_mode_quality(db, "matrix_reclaim_tiny_probe", now_ts=1000)

    assert decision["decision"] == "allow_live"
    assert decision["reason"] == "entry_mode_quality_insufficient_samples"


def test_entry_mode_quality_degraded_path_shadows_future_entries():
    db = _db()
    for idx in range(8):
        _insert(db, "lotto_high_risk_discovery_probe", 0.0, -0.12, idx)

    stats = recent_entry_mode_stats(db, "lotto_high_risk_discovery_probe")
    decision = evaluate_entry_mode_quality(db, "lotto_high_risk_discovery_probe", now_ts=1000)

    assert stats["peak_lt_3_rate"] == 1.0
    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_degraded"
