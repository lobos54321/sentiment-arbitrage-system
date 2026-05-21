import sqlite3
import time

from scripts.paper_review_snapshot_worker import build_snapshot, market_session_for_ts, missed_since_predicate, since_predicate


def test_since_predicate_keeps_timestamp_columns_index_friendly():
    cols = {"created_event_ts", "signal_ts", "baseline_ts"}

    predicate = since_predicate(cols, ["created_event_ts", "signal_ts", "baseline_ts"])

    assert predicate == "(created_event_ts >= :since OR signal_ts >= :since OR baseline_ts >= :since)"
    assert "COALESCE" not in predicate


def test_since_predicate_can_include_open_rows_without_wrapping_timestamps():
    cols = {"entry_ts", "exit_ts"}

    predicate = since_predicate(cols, ["entry_ts", "signal_ts", "exit_ts"], include_null="exit_ts")

    assert predicate == "(entry_ts >= :since OR exit_ts >= :since OR exit_ts IS NULL)"
    assert "COALESCE" not in predicate


def test_missed_since_predicate_prefers_created_event_index():
    cols = {"created_event_ts", "signal_ts", "baseline_ts"}

    assert missed_since_predicate(cols) == "created_event_ts >= :since"


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
        CREATE TABLE paper_fast_entry_queue (
          id INTEGER PRIMARY KEY,
          created_at REAL,
          updated_at REAL,
          status TEXT,
          source_type TEXT,
          entry_branch TEXT,
          last_error TEXT,
          first_error TEXT,
          market_session TEXT
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
    db.execute(
        """
        INSERT INTO paper_fast_entry_queue
          (created_at, updated_at, status, source_type, entry_branch, last_error, first_error, market_session)
        VALUES
          (?, ?, 'expired', 'hard_gate_fast', 'hard_gate_fast_clean', 'fast_lane_retry_watch_expired', 'entry_quote_failed_429', 'us')
        """,
        (now_ts - 60, now_ts - 30),
    )
    db.commit()

    snapshot = build_snapshot(db, 24, 10)

    assert set(snapshot["section_query_ms"]) == {
        "missed",
        "trades",
        "fast_lane",
        "entry_mode_performance",
        "route_health",
    }
    assert snapshot["missed"]["available"] is True
    assert snapshot["missed"]["overall"]["unique_tokens"] == 1
    assert snapshot["missed"]["overall"]["gold_unique"] == 1
    assert snapshot["trades"]["available"] is True
    assert snapshot["trades"]["totals"]["total"] == 1
    assert snapshot["trades"]["by_mode"][0]["entry_mode"] == "hard_gate_pass_tiny_probe"
    assert snapshot["fast_lane"]["available"] is True
    assert snapshot["fast_lane"]["reason_summary"][0]["reason"] == "entry_quote_failed_429"
    assert snapshot["fast_lane"]["session_summary"][0]["market_session"] == "us"
    assert snapshot["entry_mode_performance"]["available"] is True
    assert snapshot["entry_mode_performance"]["by_entry_mode"][0]["entry_mode"] == "hard_gate_pass_tiny_probe"
    assert snapshot["route_health"]["available"] is True
    assert snapshot["route_health"]["routes"][0]["entry_branch"] == "hard_gate_fast_clean"


def test_review_snapshot_worker_separates_mark_only_missed_peaks(tmp_path):
    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    db.executescript(
        """
        CREATE TABLE paper_missed_signal_attribution (
          id INTEGER PRIMARY KEY,
          created_event_ts REAL,
          token_ca TEXT,
          symbol TEXT,
          route TEXT,
          component TEXT,
          reject_reason TEXT,
          executable_peak_pnl REAL,
          quote_clean_peak_pnl REAL,
          tradable_peak_pnl REAL,
          theoretical_peak_pnl REAL,
          max_pnl_recorded REAL,
          tradable_missed INTEGER,
          would_stop_before_peak INTEGER
        );
        """
    )
    now_ts = int(time.time())
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
          (created_event_ts, token_ca, symbol, route, component, reject_reason,
           executable_peak_pnl, quote_clean_peak_pnl, tradable_peak_pnl,
           theoretical_peak_pnl, max_pnl_recorded, tradable_missed, would_stop_before_peak)
        VALUES
          (?, 'MARK', 'MARKONLY', 'ATH', 'matrix_evaluator', 'mark_spike',
           NULL, NULL, NULL, 1.40, 1.40, 1, 0),
          (?, 'TRUST', 'TRUSTED', 'ATH', 'matrix_evaluator', 'trusted_quote',
           NULL, 0.60, NULL, 0.60, 0.60, 1, 0)
        """,
        (now_ts - 60, now_ts - 55),
    )
    db.commit()

    snapshot = build_snapshot(db, 24, 10)

    overall = snapshot["missed"]["overall"]
    assert overall["gold_unique"] == 0
    assert overall["silver_unique"] == 1
    assert overall["mark_only_gold_unique"] == 1
    mark_only = next(row for row in snapshot["missed"]["top_dogs"] if row["token_ca"] == "MARK")
    assert mark_only["peak_trust_status"] == "mark_only_peak_untrusted"
    trusted = next(row for row in snapshot["missed"]["top_dogs"] if row["token_ca"] == "TRUST")
    assert trusted["peak_trust_status"] == "trusted_peak"


def test_entry_mode_performance_excludes_old_open_rows_from_recent_window(tmp_path):
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
        """
    )
    now_ts = int(time.time())
    db.execute(
        """
        INSERT INTO paper_trades
          (token_ca, symbol, signal_ts, entry_ts, exit_ts, entry_mode, pnl_pct, peak_pnl, position_size_sol)
        VALUES
          ('OLD', 'OLD', ?, ?, NULL, 'lotto_unknown', 0.0, 0.0, 0.05),
          ('NEW', 'NEW', ?, ?, ?, 'pre_pass_resonance_tiny_probe', 0.10, 0.20, 0.001)
        """,
        (now_ts - 90000, now_ts - 90000, now_ts - 60, now_ts - 60, now_ts - 30),
    )
    db.commit()

    snapshot = build_snapshot(db, 24, 10)

    recent_tokens = {row["token_ca"] for row in snapshot["entry_mode_performance"]["recent"]}
    modes = {row["entry_mode"] for row in snapshot["entry_mode_performance"]["by_entry_mode"]}
    assert "NEW" in recent_tokens
    assert "OLD" not in recent_tokens
    assert "pre_pass_resonance_tiny_probe" in modes
    assert "lotto_unknown" not in modes


def test_review_snapshot_worker_outputs_branch_session_ev(tmp_path):
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
          entry_branch TEXT,
          pnl_pct REAL,
          trusted_peak_pnl REAL,
          quote_peak_pnl REAL,
          peak_pnl REAL,
          position_size_sol REAL
        );
        CREATE TABLE paper_fast_entry_queue (
          id INTEGER PRIMARY KEY,
          created_at REAL,
          updated_at REAL,
          status TEXT,
          source_type TEXT,
          entry_branch TEXT,
          first_error TEXT,
          market_session TEXT
        );
        """
    )
    ts = int(time.time()) - 3600
    db.executemany(
        """
        INSERT INTO paper_trades
          (token_ca, symbol, signal_ts, entry_ts, exit_ts, entry_mode, entry_branch,
           pnl_pct, trusted_peak_pnl, quote_peak_pnl, peak_pnl, position_size_sol)
        VALUES
          (?, 'DOG', ?, ?, ?, 'source_resonance_tiny_probe',
           'source_quote_clean_refresh_tiny_probe', ?, ?, NULL, 9.9, 0.001)
        """,
        [
            (f"T{i}", ts, ts, ts + 60, -0.06, 0.0)
            for i in range(20)
        ],
    )
    db.commit()

    snapshot = build_snapshot(db, 24, 10)
    ev = snapshot["fast_lane"]["branch_ev_summary"][0]

    assert ev["entry_branch"] == "source_quote_clean_refresh_tiny_probe"
    assert ev["market_session"] == market_session_for_ts(ts)
    assert ev["closed_n"] == 20
    assert ev["avg_pnl_pct"] == -6.0
    assert ev["auto_action"] == "downgrade_to_watch_only"
    route = snapshot["route_health"]["routes"][0]
    assert route["kill_switch"]["status"] == "tripped"
    assert route["kill_switch"]["auto_action"] == "downgrade_to_watch_only"
