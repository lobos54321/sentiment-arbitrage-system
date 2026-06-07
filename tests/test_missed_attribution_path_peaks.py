import os
import sqlite3
import sys

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
os.environ.setdefault(
    "PAPER_SQLITE_WRITER_LOCK_FILE",
    os.path.join("/private/tmp", f"paper_sqlite_writer_test_{os.getpid()}.lock"),
)

from paper_decision_audit import init_decision_audit, update_due_missed_attributions


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    return db


def _insert_missed(db, *, route="NOT_ATH", baseline_price=1.0, baseline_ts=1_000):
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, signal_ts, route, component,
             decision, reject_reason, baseline_price, baseline_source, baseline_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            baseline_ts,
            "FastPumpCA",
            "FAST",
            baseline_ts,
            route,
            "matrix_evaluator",
            "wait",
            "matrices_not_yet_aligned",
            baseline_price,
            "fixture",
            baseline_ts,
        ),
    )
    db.commit()


def test_missed_attribution_uses_path_high_when_fixed_horizons_are_missing():
    db = _db()
    _insert_missed(db)

    def no_exact_horizon_price(_token_ca, _target_ts):
        return None

    def path_samples(_token_ca, start_ts, end_ts):
        assert start_ts == 1_000
        assert end_ts >= 1_120
        return [
            {"timestamp": 1_060, "low": 0.98, "high": 1.20, "close": 1.10, "source": "fixture"},
            {"timestamp": 1_120, "low": 1.15, "high": 6.00, "close": 4.50, "source": "fixture"},
        ]

    updated = update_due_missed_attributions(
        db,
        historical_price_fetcher=no_exact_horizon_price,
        historical_path_fetcher=path_samples,
        now=1_400,
        limit=10,
    )

    assert updated == 1
    row = db.execute(
        """
        SELECT pnl_5m, max_pnl_recorded, min_pnl_recorded, theoretical_peak_pnl,
               quote_clean_peak_pnl, executable_peak_pnl, executable_peak_source,
               executable_peak_horizon, tradable_missed, tradability_status,
               time_to_peak_sec, mae_before_peak_pnl, first_tradable_horizon
        FROM paper_missed_signal_attribution
        WHERE token_ca = 'FastPumpCA'
        """
    ).fetchone()

    assert row["pnl_5m"] is None
    assert row["max_pnl_recorded"] == pytest.approx(5.0)
    assert row["min_pnl_recorded"] == pytest.approx(-0.02)
    assert row["theoretical_peak_pnl"] == pytest.approx(5.0)
    assert row["quote_clean_peak_pnl"] == pytest.approx(5.0)
    assert row["executable_peak_pnl"] == pytest.approx(5.0)
    assert row["executable_peak_source"] == "path:fixture:high"
    assert row["executable_peak_horizon"] == "path_120s_high"
    assert row["tradable_missed"] == 1
    assert row["tradability_status"] == "tradable_reclaim"
    assert row["time_to_peak_sec"] == 120
    assert row["mae_before_peak_pnl"] == pytest.approx(-0.02)
    assert row["first_tradable_horizon"] == "path_60s_high"


def test_missed_attribution_marks_stop_before_path_peak_conservatively():
    db = _db()
    _insert_missed(db, route="LOTTO")

    def path_samples(_token_ca, _start_ts, _end_ts):
        return [
            {"timestamp": 1_030, "low": 0.85, "high": 1.05, "close": 0.95, "source": "fixture"},
            {"timestamp": 1_120, "low": 0.90, "high": 3.00, "close": 2.50, "source": "fixture"},
        ]

    update_due_missed_attributions(
        db,
        historical_price_fetcher=lambda *_args: None,
        historical_path_fetcher=path_samples,
        now=1_400,
        limit=10,
    )

    row = db.execute(
        """
        SELECT max_pnl_recorded, tradable_missed, tradability_status,
               would_stop_before_peak, mae_before_peak_pnl
        FROM paper_missed_signal_attribution
        WHERE token_ca = 'FastPumpCA'
        """
    ).fetchone()

    assert row["max_pnl_recorded"] == pytest.approx(2.0)
    assert row["tradable_missed"] == 0
    assert row["tradability_status"] == "would_stop_before_peak"
    assert row["would_stop_before_peak"] == 1
    assert row["mae_before_peak_pnl"] == pytest.approx(-0.15)
