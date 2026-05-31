import sqlite3
import sys


sys.path.insert(0, "scripts")

from backtest_lotto_reclaim_markov import build_backtest_report  # noqa: E402


def _db(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_ca TEXT,
            symbol TEXT,
            entry_ts INTEGER,
            exit_ts INTEGER,
            exit_reason TEXT,
            pnl_pct REAL,
            peak_pnl REAL,
            signal_route TEXT,
            signal_type TEXT,
            entry_mode TEXT,
            entry_branch TEXT,
            replay_source TEXT,
            monitor_state_json TEXT,
            entry_execution_audit_json TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            baseline_ts INTEGER,
            tradability_status TEXT,
            tradable_missed INTEGER,
            tradable_peak_pnl REAL,
            time_to_peak_sec INTEGER,
            would_stop_before_peak INTEGER,
            first_tradable_ts INTEGER,
            first_tradable_pnl REAL,
            pnl_5m REAL,
            pnl_15m REAL,
            pnl_60m REAL,
            pnl_24h REAL,
            max_pnl_recorded REAL,
            payload_json TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE paper_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            signal_ts INTEGER,
            route TEXT,
            component TEXT,
            event_type TEXT,
            decision TEXT,
            reason TEXT,
            lifecycle_state TEXT,
            payload_json TEXT
        )
        """
    )
    db.commit()
    return db


def _insert_trade(db, token, entry_ts, *, peak, pnl=None, mode="lotto_micro_reclaim_tiny_probe"):
    db.execute(
        """
        INSERT INTO paper_trades(
            token_ca, symbol, entry_ts, exit_ts, exit_reason, pnl_pct, peak_pnl,
            signal_route, entry_mode, entry_branch, replay_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'LOTTO', ?, 'not_ath_reclaim_quote_clean_tiny_probe', 'paper_fast_lane')
        """,
        (
            token,
            token[:6],
            entry_ts,
            entry_ts + 600,
            "take_profit" if peak >= 0.30 else "hard_sl",
            pnl if pnl is not None else peak,
            peak,
            mode,
        ),
    )


def _insert_missed_candidate(db, token, ts, *, peak, stop=0, reason="tracking_ttl_expired"):
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution(
            created_event_ts, token_ca, symbol, signal_ts, route, component, reject_reason,
            baseline_ts, tradability_status, tradable_missed, tradable_peak_pnl,
            would_stop_before_peak, first_tradable_ts, payload_json
        ) VALUES (?, ?, ?, ?, 'LOTTO', 'discovery_tracking', ?, ?, 'tradable_reclaim', 1, ?, ?, ?, '{}')
        """,
        (ts, token, token[:6], ts, reason, ts, peak, stop, ts),
    )


def test_lotto_reclaim_markov_backtest_uses_only_past_training_rows(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _db(db_path)
    _insert_missed_candidate(db, "CandidateA", 1_000, peak=0.50)
    _insert_trade(db, "FutureWinner", 1_100, peak=0.80)
    db.commit()
    db.close()

    report = build_backtest_report(db_path, since_ts=900, until_ts=1_050, min_sample=1, include_rows=True)

    assert report["paired_sample_n"] == 1
    row = report["rows"][0]
    assert row["token_ca"] == "CandidateA"
    assert row["forecast"]["sample_n"] == 0
    assert row["markov_bucket"] == "insufficient"


def test_lotto_reclaim_markov_backtest_buckets_green_when_past_path_wins(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _db(db_path)
    for idx in range(5):
        _insert_trade(db, f"PastWinner{idx}", 100 + idx * 20, peak=0.60)
    _insert_missed_candidate(db, "CandidateB", 1_000, peak=0.45)
    db.commit()
    db.close()

    report = build_backtest_report(db_path, since_ts=900, until_ts=1_200, min_sample=1, include_rows=True)

    assert report["paired_sample_n"] == 1
    row = report["rows"][0]
    assert row["markov_bucket"] == "green"
    assert row["forecast"]["p_absorb_peak30"] == 1.0
    assert report["by_markov_bucket"]["green"]["peak30_before_stop_rate"] == 1.0


def test_lotto_reclaim_markov_backtest_groups_blocker_families(tmp_path):
    db_path = tmp_path / "paper.db"
    db = _db(db_path)
    _insert_trade(db, "PastLoser", 100, peak=0.05, pnl=-0.20)
    _insert_missed_candidate(db, "CandidateC", 1_000, peak=0.05, stop=1, reason="lotto_stale_2253s")
    db.commit()
    db.close()

    report = build_backtest_report(db_path, since_ts=900, until_ts=1_200, min_sample=1)

    assert report["by_blocker_family"]["lotto_stale"]["sample_n"] == 1
    assert report["by_blocker_family"]["lotto_stale"]["stop_before_peak_rate"] == 1.0
