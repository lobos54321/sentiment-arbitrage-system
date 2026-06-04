import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from counterfactual_replay import build_counterfactual_replay_report
from opportunity_events import record_opportunity_event


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def test_counterfactual_replay_uses_path_samples_when_available():
    db = memory_db()
    key = record_opportunity_event(
        db,
        {
            "opportunity_key": "opp:path",
            "event_ts": 1_700_000_000,
            "token_ca": "TokenPath",
            "symbol": "PATH",
            "route_bucket": "ATH",
            "source_type": "unit",
            "quote_clean": True,
            "quote_executable": True,
            "route_available": True,
            "would_enter_a_class": True,
        },
    )
    db.execute(
        """
        CREATE TABLE opportunity_event_path_samples (
            opportunity_key TEXT,
            sample_ts REAL,
            quote_pnl_pct REAL,
            quote_clean INTEGER,
            quote_executable INTEGER,
            route_available INTEGER
        )
        """
    )
    db.executemany(
        """
        INSERT INTO opportunity_event_path_samples (
            opportunity_key, sample_ts, quote_pnl_pct, quote_clean, quote_executable, route_available
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (key, 1_700_000_010, 0.10, 1, 1, 1),
            (key, 1_700_000_020, 0.55, 1, 1, 1),
            (key, 1_700_000_030, -0.30, 1, 1, 1),
        ],
    )

    report = build_counterfactual_replay_report(db, since_ts=1_699_999_000, limit=10)
    row = report["rows"][0]

    assert row["label"] == "UPPER"
    assert row["hit_upper"] == 0.50
    assert row["data_quality"] == "quote_clean"
    assert report["quote_clean_gold_silver_seen_count"] == 1
    assert report["quote_clean_gold_silver_would_enter_count"] == 1


def test_counterfactual_replay_summary_only_blocks_stop_before_peak():
    db = memory_db()
    db.execute(
        """
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            token_ca TEXT,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            tradable_missed INTEGER,
            tradability_status TEXT,
            mae_before_peak_pnl REAL,
            time_to_peak_sec REAL,
            created_event_ts REAL,
            max_pnl_recorded REAL
        )
        """
    )
    db.executemany(
        """
        INSERT INTO paper_missed_signal_attribution (
            id, symbol, token_ca, route, component, reject_reason, tradable_missed,
            tradability_status, mae_before_peak_pnl, time_to_peak_sec, created_event_ts, max_pnl_recorded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (1, "DOG", "TokenDog", "ATH", "scout", "volume_low", 1, "tradable_reclaim", -0.05, 120, 1_700_000_000, 1.20),
            (2, "STOPDOG", "TokenStop", "ATH", "scout", "volume_low", 1, "would_stop_before_peak", -0.25, 140, 1_700_000_000, 1.40),
        ],
    )

    report = build_counterfactual_replay_report(db, since_ts=1_699_999_000, limit=10)
    by_symbol = {row["symbol"]: row for row in report["rows"]}

    assert by_symbol["DOG"]["label"] == "UPPER"
    assert by_symbol["DOG"]["data_quality"] == "summary_only"
    assert by_symbol["STOPDOG"]["label"] == "LOWER"
    assert by_symbol["STOPDOG"]["terminal_reason"] == "summary_would_stop_before_peak"
    assert report["quote_clean_gold_silver_seen_count"] == 2
    assert report["quote_clean_gold_silver_would_enter_count"] == 1
