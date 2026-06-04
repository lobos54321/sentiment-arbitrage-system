import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_expected_rr import build_a_class_p0_discovery, calculate_a_class_expected_rr, trim10_mean


NOW_TS = 10_000


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def create_rr_schema(db):
    db.executescript(
        """
        CREATE TABLE canonical_trade_ledger (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            entry_ts REAL,
            peak_quote_pnl_pct REAL,
            realized_pnl_pct REAL,
            entry_quote_executable INTEGER,
            no_route_flag INTEGER DEFAULT 0,
            trapped_flag INTEGER DEFAULT 0,
            outlier_flag INTEGER DEFAULT 0
        );
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            entry_ts REAL,
            trusted_peak_pnl REAL,
            quote_peak_pnl REAL,
            peak_pnl REAL,
            entry_quote_executable INTEGER,
            no_route_flag INTEGER DEFAULT 0,
            trapped_flag INTEGER DEFAULT 0,
            outlier_flag INTEGER DEFAULT 0
        );
        CREATE TABLE paper_missed_signal_attribution (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            created_event_ts REAL,
            route TEXT,
            component TEXT,
            reject_reason TEXT,
            executable_peak_pnl REAL,
            quote_clean_peak_pnl REAL,
            tradable_peak_pnl REAL,
            tradable_missed INTEGER,
            would_stop_before_peak INTEGER,
            outlier_flag INTEGER DEFAULT 0
        );
        CREATE TABLE a_class_decision_events (
            id INTEGER PRIMARY KEY,
            event_ts REAL,
            token_ca TEXT,
            action TEXT,
            would_action TEXT
        );
        """
    )


def seed_rr_fixture(db):
    ts = NOW_TS - 3600
    db.executemany(
        """
        INSERT INTO canonical_trade_ledger
          (token_ca, entry_ts, peak_quote_pnl_pct, realized_pnl_pct,
           entry_quote_executable, no_route_flag, trapped_flag, outlier_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("T_EXEC", ts, 1.20, 0.40, 1, 0, 0, 0),
            ("T_PRECEDENCE", ts, 0.40, 0.10, 1, 0, 0, 0),
        ],
    )
    db.executemany(
        """
        INSERT INTO paper_trades
          (token_ca, entry_ts, trusted_peak_pnl, quote_peak_pnl, peak_pnl,
           entry_quote_executable, no_route_flag, trapped_flag, outlier_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("T_PAPER", ts, 0.0, 0.75, 9.90, 1, 1, 0, 0),
            ("T_MARK_ONLY", ts, 0.0, 0.0, 9.90, 1, 0, 0, 0),
            ("T_PRECEDENCE", ts, 2.00, 2.00, 9.90, 1, 0, 0, 0),
        ],
    )
    db.executemany(
        """
        INSERT INTO paper_missed_signal_attribution
          (token_ca, created_event_ts, route, component, reject_reason,
           executable_peak_pnl, quote_clean_peak_pnl, tradable_peak_pnl,
           tradable_missed, would_stop_before_peak, outlier_flag)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("T_MISS", ts, "ATH", "entry_gate", "freshness_wait", 1.00, None, None, 1, 0, 0),
            ("T_BLOCKER", ts, "LOTTO", "matrix_gate", "weak_matrix", 1.40, None, None, 1, 0, 0),
            ("T_STOP", ts, "ATH", "entry_gate", "stop_before_peak", 2.00, None, None, 1, 1, 0),
        ],
    )
    db.executemany(
        """
        INSERT INTO a_class_decision_events (event_ts, token_ca, action, would_action)
        VALUES (?, ?, ?, ?)
        """,
        [
            (ts, "T_EXEC", "WOULD_ENTER", None),
            (ts, "T_PAPER", "SHADOW", "WOULD_ENTER"),
            (ts, "T_MISS", "WOULD_ENTER", None),
            (ts, "T_MARK_ONLY", "WOULD_ENTER", None),
            (ts, "T_PRECEDENCE", "WOULD_ENTER", None),
        ],
    )
    db.commit()


def relaxed_config(**overrides):
    config = {
        "window_hours": 24,
        "evaluation_cadence_minutes": 15,
        "consecutive_windows_required": 4,
        "min_quote_clean_gold_silver_seen_24h": 3,
        "min_quote_clean_gold_silver_would_enter_count": 3,
        "min_quote_clean_gold_silver_would_enter_count_72h": 3,
        "max_would_enter_no_route_rate": 0.50,
        "max_would_enter_trapped_rate": 0.10,
        "max_unknown_data_rate": 0.05,
        "min_outlier_trimmed_would_rr": 2.0,
        "defined_fast_stop_pct": 0.15,
        "hard_loss_cap_pct": 0.20,
    }
    config.update(overrides)
    return config


def test_denominator_precedence_haircut_mark_fallback_and_defined_risk_rr():
    db = memory_db()
    create_rr_schema(db)
    seed_rr_fixture(db)

    summary = calculate_a_class_expected_rr(
        db,
        since_ts=NOW_TS - 24 * 3600,
        until_ts=NOW_TS,
        config=relaxed_config(),
    )

    assert summary["quote_clean_gold_silver_seen_count"] == 4
    assert summary["quote_clean_gold_silver_gold_count"] == 2
    assert summary["quote_clean_gold_silver_silver_count"] == 2
    assert summary["quote_clean_gold_silver_would_enter_count"] == 3
    assert summary["source_breakdown"] == {
        "canonical_trade_ledger": 1,
        "paper_trades": 1,
        "paper_missed_signal_attribution": 2,
    }
    assert round(summary["would_enter_no_route_rate"], 6) == round(1 / 3, 6)
    assert summary["defined_risk_pct"] == 0.20
    assert round(summary["outlier_trimmed_adjusted_peak_mean"], 6) == 0.90
    assert round(summary["outlier_trimmed_would_rr"], 6) == 4.50
    assert "peak_pnl is not used" in " ".join(summary["evidence_notes"])
    assert summary["missed_blockers"][0] == {
        "route": "LOTTO",
        "component": "matrix_gate",
        "reject_reason": "weak_matrix",
        "unique_tokens": 1,
        "gold_n": 1,
        "silver_n": 0,
        "max_adjusted_peak": 1.07,
    }


def test_investigate_sourcing_when_seen_below_min():
    db = memory_db()
    create_rr_schema(db)
    seed_rr_fixture(db)

    summary = build_a_class_p0_discovery(
        db,
        since_ts=NOW_TS - 24 * 3600,
        until_ts=NOW_TS,
        config=relaxed_config(min_quote_clean_gold_silver_seen_24h=8),
    )

    assert summary["quote_clean_gold_silver_seen_count"] == 4
    assert summary["discovery_exit"]["advisory"] == "INVESTIGATE_SOURCING"
    assert summary["discovery_exit"]["reason"] == "quote_clean_gold_silver_seen_below_min"


def test_promote_advisory_only_when_thresholds_and_consecutive_windows_pass():
    db = memory_db()
    create_rr_schema(db)
    seed_rr_fixture(db)

    summary = build_a_class_p0_discovery(
        db,
        since_ts=NOW_TS - 24 * 3600,
        until_ts=NOW_TS,
        config=relaxed_config(),
    )

    assert summary["discovery_exit"]["pass"] is True
    assert summary["discovery_exit"]["advisory"] == "PROMOTE_TINY_CANARY"
    assert summary["discovery_exit"]["advisory_only"] is True
    assert summary["discovery_exit"]["requires_human_approval"] is True
    assert all(row["pass"] for row in summary["discovery_exit"]["consecutive_windows"])


def test_trim10_mean_marks_small_samples_and_trims_large_samples():
    small_mean, small_detail = trim10_mean([1, 2, 100])
    large_mean, large_detail = trim10_mean([1, 2, 3, 4, 5, 6, 7, 8, 9, 100])

    assert small_mean == 103 / 3
    assert small_detail["small_sample"] is True
    assert large_mean == 5.5
    assert large_detail["small_sample"] is False
    assert large_detail["trimmed_n"] == 2
