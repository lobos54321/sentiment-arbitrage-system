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
        _insert(db, "custom_probe_mode", 0.0, -0.10, idx)

    decision = evaluate_entry_mode_quality(db, "custom_probe_mode", now_ts=1000)

    assert decision["decision"] == "allow_live"
    assert decision["reason"] == "entry_mode_quality_insufficient_samples"


def test_entry_mode_quality_force_live_bypasses_degraded_shadow():
    db = _db()
    for idx in range(8):
        _insert(db, "custom_force_mode", 0.0, -0.12, idx)

    decision = evaluate_entry_mode_quality(db, "custom_force_mode", now_ts=1000, force_live=True)

    assert decision["decision"] == "allow_live"
    assert decision["reason"] == "entry_mode_quality_force_live"


def test_shadow_only_mode_overrides_force_live():
    db = _db()

    decision = evaluate_entry_mode_quality(db, "ath_micro_reclaim_tiny_probe", now_ts=1000, force_live=True)

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_shadow_only_mode"
    assert decision["shadow_only_mode"] is True


def test_lotto_micro_reclaim_is_shadow_only_by_default():
    db = _db()

    decision = evaluate_entry_mode_quality(db, "lotto_micro_reclaim_tiny_probe", now_ts=1000)

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_shadow_only_mode"


def test_observed_tiny_modes_are_shadow_only_by_default():
    db = _db()

    for mode in (
        "ath_no_kline_tiny_probe",
        "ath_uncertainty_tiny_scout",
        "ath_high_mc_tiny_probe",
        "ath_matrix_dissonance_tiny_probe",
        "ath_reclaim_after_failure_tiny_probe",
        "lotto_not_ath_reclaim_tiny_probe",
        "lotto_low_liquidity_reclaim_tiny_probe",
        "lotto_upstream_realtime_tiny_scout",
        "matrix_reclaim_tiny_probe",
        "matrix_micro_momentum_tiny_probe",
        "pullback_tiny_scout",
        "newborn_momentum_tiny_scout",
    ):
        decision = evaluate_entry_mode_quality(db, mode, now_ts=1000)
        assert decision["decision"] == "shadow", mode
        assert decision["reason"] == "entry_mode_quality_shadow_only_mode", mode


def test_entry_mode_quality_degraded_path_shadows_future_entries():
    db = _db()
    for idx in range(8):
        _insert(db, "custom_degraded_probe", 0.0, -0.12, idx)

    stats = recent_entry_mode_stats(db, "custom_degraded_probe")
    decision = evaluate_entry_mode_quality(db, "custom_degraded_probe", now_ts=1000)

    assert stats["peak_lt_3_rate"] == 1.0
    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_degraded"


def test_matrix_reclaim_shadows_when_peaks_are_given_back():
    db = _db()
    for idx in range(6):
        _insert(db, "matrix_reclaim_tiny_probe", 0.10, -0.06, idx)

    decision = evaluate_entry_mode_quality(db, "matrix_reclaim_tiny_probe", now_ts=2000)

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_shadow_only_mode"


def test_pullback_tiny_scout_can_shadow_after_two_dead_probes():
    db = _db()
    for idx in range(2):
        _insert(db, "pullback_tiny_scout", 0.0, -0.18, idx)

    decision = evaluate_entry_mode_quality(db, "pullback_tiny_scout", now_ts=3000)

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_shadow_only_mode"


def test_matrix_micro_momentum_probe_shadows_after_four_bad_givebacks():
    db = _db()
    for idx in range(4):
        _insert(db, "matrix_micro_momentum_tiny_probe", 0.08, -0.06, idx)

    decision = evaluate_entry_mode_quality(db, "matrix_micro_momentum_tiny_probe", now_ts=4000)

    assert decision["decision"] == "shadow"
    assert decision["reason"] == "entry_mode_quality_shadow_only_mode"
