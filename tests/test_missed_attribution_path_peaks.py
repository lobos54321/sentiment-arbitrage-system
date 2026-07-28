import os
import sqlite3
import sys
from contextlib import contextmanager

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
os.environ.setdefault(
    "PAPER_SQLITE_WRITER_LOCK_FILE",
    os.path.join("/private/tmp", f"paper_sqlite_writer_test_{os.getpid()}.lock"),
)

import paper_decision_audit
import paper_trade_monitor
from entry_readiness_policy import _any_ts_sec
from paper_decision_audit import (
    CREATE_MISSED_ATTRIBUTION_SQL,
    init_decision_audit,
    update_due_missed_attributions,
)
from paper_fast_lane import parse_datetime_ts


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


def test_init_adds_attribution_revision_to_legacy_table():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    legacy_sql = CREATE_MISSED_ATTRIBUTION_SQL.replace(
        "    attribution_revision INTEGER NOT NULL DEFAULT 0,\n",
        "",
    )
    db.execute(legacy_sql)

    init_decision_audit(db)

    columns = {
        row["name"]: row
        for row in db.execute(
            "PRAGMA table_info(paper_missed_signal_attribution)"
        ).fetchall()
    }
    assert "attribution_revision" in columns
    assert columns["attribution_revision"]["notnull"] == 1
    assert columns["attribution_revision"]["dflt_value"] == "0"


def test_monitor_does_not_hold_global_writer_lock_during_missed_attribution_enrichment(monkeypatch):
    lock_calls = []

    @contextmanager
    def unexpected_outer_lock(name, **_kwargs):
        lock_calls.append(name)
        yield

    monkeypatch.setattr(paper_trade_monitor, "sqlite_single_writer", unexpected_outer_lock)
    monkeypatch.setattr(
        paper_trade_monitor,
        "update_due_missed_attributions",
        lambda *_args, **_kwargs: 3,
    )
    monkeypatch.setattr(paper_trade_monitor, "_MISSED_ATTRIBUTION_BACKOFF_UNTIL", 0.0)
    monkeypatch.setattr(paper_trade_monitor, "_MISSED_ATTRIBUTION_LOCK_FAILURES", 0)

    result = paper_trade_monitor.run_due_missed_attribution_update(object(), now=2_000)

    assert result == {"updated": 3, "skipped": False, "reason": "updated"}
    assert lock_calls == []


def test_missed_attribution_fetches_before_short_writer_transaction(monkeypatch):
    db = _db()
    _insert_missed(db)
    writer_lock_active = False
    lock_calls = []

    @contextmanager
    def tracked_writer_lock(name, **_kwargs):
        nonlocal writer_lock_active
        lock_calls.append(name)
        writer_lock_active = True
        try:
            yield
        finally:
            writer_lock_active = False

    def historical_price(_token_ca, _target_ts):
        assert not writer_lock_active
        return 1.25, "fixture"

    monkeypatch.setattr(paper_decision_audit, "sqlite_single_writer", tracked_writer_lock)

    updated = paper_decision_audit.update_due_missed_attributions(
        db,
        historical_price_fetcher=historical_price,
        historical_path_fetcher=lambda *_args: [],
        now=1_000 + 24 * 60 * 60,
        limit=10,
    )

    row = db.execute(
        """
        SELECT attribution_revision, updated_at
        FROM paper_missed_signal_attribution
        WHERE token_ca = 'FastPumpCA'
        """
    ).fetchone()
    assert updated == 1
    assert lock_calls == ["paper_decision_audit:missed_attribution_update"]
    assert row["attribution_revision"] == 1
    assert len(row["updated_at"]) == 19
    assert parse_datetime_ts(row["updated_at"]) is not None
    assert _any_ts_sec(row["updated_at"]) is not None


def test_stale_missed_attribution_batch_cannot_overwrite_newer_revision(tmp_path):
    db_path = tmp_path / "paper.db"
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    _insert_missed(db)
    competing_db = sqlite3.connect(db_path)
    competing_db.row_factory = sqlite3.Row
    competing_write_done = False

    def historical_price(_token_ca, _target_ts):
        nonlocal competing_write_done
        if not competing_write_done:
            competing_db.execute(
                """
                UPDATE paper_missed_signal_attribution
                SET baseline_price = 10.0,
                    price_5m = 20.0,
                    pnl_5m = 1.0,
                    price_15m = 30.0,
                    pnl_15m = 2.0,
                    price_60m = 40.0,
                    pnl_60m = 3.0,
                    price_24h = 50.0,
                    pnl_24h = 4.0,
                    max_pnl_recorded = 4.0,
                    min_pnl_recorded = 1.0,
                    status = 'complete',
                    attribution_revision = attribution_revision + 1
                WHERE token_ca = 'FastPumpCA'
                """
            )
            competing_db.commit()
            competing_write_done = True
        return 2.0, "stale_fixture"

    updated = update_due_missed_attributions(
        db,
        historical_price_fetcher=historical_price,
        historical_path_fetcher=lambda *_args: [],
        now=1_000 + 24 * 60 * 60,
        limit=10,
    )

    row = db.execute(
        """
        SELECT baseline_price, pnl_15m, pnl_24h, max_pnl_recorded, status,
               attribution_revision, updated_at
        FROM paper_missed_signal_attribution
        WHERE token_ca = 'FastPumpCA'
        """
    ).fetchone()
    assert updated == 0
    assert row["baseline_price"] == 10.0
    assert row["pnl_15m"] == 2.0
    assert row["pnl_24h"] == 4.0
    assert row["max_pnl_recorded"] == 4.0
    assert row["status"] == "complete"
    assert row["attribution_revision"] == 1
    assert len(row["updated_at"]) == 19
