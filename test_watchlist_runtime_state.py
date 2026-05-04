#!/usr/bin/env python3

import os
import sys
from concurrent.futures import Future


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from matrix_evaluator import MatrixEvaluator, _epoch_seconds, score_realtime_momentum  # noqa: E402
from paper_trade_monitor import parse_super_index, smart_entry_result_ready  # noqa: E402
from watchlist_store import WatchlistStore  # noqa: E402


def test_register_refreshes_existing_watchlist_entry_for_fresh_ath(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        first = store.register(
            ca="TokenCA",
            symbol="DOG",
            signal_type="LOTTO",
            pool_address="Pool1",
            signal_ts=1000,
            premium_signal_id=1,
            signal_price=0.000001,
            signal_mc=12000,
        )
        updated = store.register(
            ca="TokenCA",
            symbol="DOGATH",
            signal_type="ATH",
            pool_address="Pool2",
            signal_ts=2000,
            premium_signal_id=2,
            signal_price=0.000003,
            signal_mc=60000,
        )

        assert updated["id"] == first["id"]
        assert updated["type"] == "ATH"
        assert updated["symbol"] == "DOGATH"
        assert updated["pool_address"] == "Pool2"
        assert updated["signal_ts"] == 2000
        assert updated["premium_signal_id"] == 2
        assert updated["signal_price"] == 0.000001
        assert updated["latest_ath_price"] == 0.000003
        assert updated["signal_mc"] == 60000
        assert updated["last_eval_at"] == 0
        assert updated["added_at"] >= first["added_at"]
    finally:
        store.close()


def test_register_fresh_ath_preserves_real_symbol_when_parser_returns_unknown(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        first = store.register(
            ca="TokenCA",
            symbol="MIKE",
            signal_type="NOT_ATH",
            pool_address="Pool1",
            signal_ts=1000,
            premium_signal_id=1,
            signal_price=0.000001,
            signal_mc=16000,
        )
        updated = store.register(
            ca="TokenCA",
            symbol="UNKNOWN",
            signal_type="ATH",
            pool_address="Pool2",
            signal_ts=2000,
            premium_signal_id=2,
            signal_price=0.0000026,
            signal_mc=42000,
        )

        assert updated["id"] == first["id"]
        assert updated["symbol"] == "MIKE"
        assert updated["type"] == "ATH"
        assert updated["signal_ts"] == 2000
        assert updated["premium_signal_id"] == 2
        assert updated["signal_mc"] == 42000
        assert updated["last_ath_ts"] == 2000
        assert updated["last_ath_mc"] == 42000
        assert updated["signal_price"] == 0.000001
        assert updated["latest_ath_price"] == 0.0000026
    finally:
        store.close()


def test_register_reactivates_expired_ath_with_fresh_signal_anchor(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        first = store.register(
            ca="TokenCA",
            symbol="LUCY",
            signal_type="ATH",
            pool_address="Pool1",
            signal_ts=1000,
            premium_signal_id=1,
            signal_price=0.000001,
            signal_mc=30000,
        )
        store.mark_expired(first["id"], "timeout")

        reactivated = store.register(
            ca="TokenCA",
            symbol="UNKNOWN",
            signal_type="ATH",
            pool_address="Pool2",
            signal_ts=3000,
            premium_signal_id=3,
            signal_price=0.000004,
            signal_mc=80000,
        )

        assert reactivated["id"] == first["id"]
        assert reactivated["status"] == "watching"
        assert reactivated["symbol"] == "LUCY"
        assert reactivated["type"] == "ATH"
        assert reactivated["pool_address"] == "Pool2"
        assert reactivated["signal_ts"] == 3000
        assert reactivated["premium_signal_id"] == 3
        assert reactivated["signal_price"] == 0.000004
        assert reactivated["signal_mc"] == 80000
        assert reactivated["last_ath_ts"] == 3000
        assert reactivated["last_ath_mc"] == 80000
        assert reactivated["latest_ath_price"] == 0.000004
        assert reactivated["expire_reason"] is None
        assert reactivated["fire_block_until"] == 0
    finally:
        store.close()


def test_reconcile_expires_orphaned_watchlist_holding_rows(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        entry = store.register(ca="TokenCA", symbol="DOG", signal_type="ATH", signal_ts=1000)
        store.mark_holding(
            entry["id"],
            entry_price=0.000001,
            position_size_sol=0.08,
            token_amount_raw="1000",
            token_decimals=6,
            trade_id=123,
        )

        assert store.get_by_id(entry["id"])["status"] == "holding"
        assert store.expire_orphaned_position_states(set()) == 1
        assert store.get_by_id(entry["id"])["status"] == "expired"
        assert store.get_by_id(entry["id"])["expire_reason"] == "reconcile_no_open_paper_trade"
    finally:
        store.close()


def test_register_does_not_refresh_holding_signal_anchor(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        entry = store.register(
            ca="TokenCA",
            symbol="DOG",
            signal_type="LOTTO",
            signal_ts=1000,
            signal_price=0.000001,
            signal_mc=12000,
        )
        store.mark_holding(
            entry["id"],
            entry_price=0.000001,
            position_size_sol=0.08,
            token_amount_raw="1000",
            token_decimals=6,
            trade_id=123,
        )

        updated = store.register(
            ca="TokenCA",
            symbol="DOGATH",
            signal_type="ATH",
            signal_ts=2000,
            signal_price=0.000003,
            signal_mc=60000,
        )

        assert updated["status"] == "holding"
        assert updated["signal_ts"] == 1000
        assert updated["signal_price"] == 0.000001
        assert updated["latest_ath_price"] == 0.000003
        assert updated["last_ath_ts"] == 2000
        assert updated["last_ath_mc"] == 60000
    finally:
        store.close()


def test_reconcile_keeps_active_watchlist_holding_rows(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        entry = store.register(ca="TokenCA", symbol="DOG", signal_type="ATH", signal_ts=1000)
        store.mark_holding(
            entry["id"],
            entry_price=0.000001,
            position_size_sol=0.08,
            token_amount_raw="1000",
            token_decimals=6,
            trade_id=123,
        )

        assert store.expire_orphaned_position_states({123}) == 0
        assert store.get_by_id(entry["id"])["status"] == "holding"
    finally:
        store.close()


def test_defer_fire_persists_readiness_block(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        entry = store.register(ca="TokenCA", symbol="DOG", signal_type="ATH", signal_ts=1000)
        until = store.defer_fire(entry["id"], "entry_readiness_stale_ath_requires_fresh_high", cooldown_sec=120)
        updated = store.get_by_id(entry["id"])

        assert updated["fire_block_reason"] == "entry_readiness_stale_ath_requires_fresh_high"
        assert updated["fire_block_until"] >= until - 1
        assert updated["fire_block_until"] > 0
        assert updated["last_eval_at"] > 0
    finally:
        store.close()


def test_touch_eval_updates_last_eval_at(tmp_path):
    store = WatchlistStore(str(tmp_path / "watchlist.db"))
    try:
        entry = store.register(ca="TokenCA", symbol="DOG", signal_type="ATH", signal_ts=1000)
        store.touch_eval(entry["id"], eval_time=1234.5)
        updated = store.get_by_id(entry["id"])

        assert updated["last_eval_at"] == 1234.5
    finally:
        store.close()


def test_smart_entry_result_ready_detects_done_future():
    pending = {"lc": {"_smart_entry_future": Future()}}
    assert smart_entry_result_ready(pending) is False

    pending["lc"]["_smart_entry_future"].set_result((True, "momentum_direct_entry", "", 1.0))
    assert smart_entry_result_ready(pending) is True
    assert smart_entry_result_ready({"lc": {"timing_passed": True}}) is True


def test_parse_super_index_accepts_current_plain_numeric_format():
    assert parse_super_index("✡ **Super Index**： 98\n\nAI Index：15") == 98
    assert parse_super_index("✡ Super Index： 119🔮") == 119
    assert parse_super_index("✡ Super Index： ✡ x 82") == 82


def test_matrix_watchlist_timeout_normalizes_millisecond_last_ath_ts(monkeypatch):
    now = 1_777_900_000.0
    old_ath_ms = int((now - 3 * 60 * 60) * 1000)
    monkeypatch.setattr("matrix_evaluator.time.time", lambda: now)

    removal = MatrixEvaluator()._check_removal(
        {
            "added_at": now - 3 * 60 * 60,
            "last_ath_ts": old_ath_ms,
            "signal_price": 1.0,
            "lowest_price": 1.0,
        },
        {"max_obs_minutes": 120},
    )

    assert _epoch_seconds(old_ath_ms) == old_ath_ms / 1000
    assert removal == "timeout (180min >= 120min)"


def test_realtime_momentum_marks_same_snapshot_timestamp_as_wait_not_decline(monkeypatch):
    import matrix_evaluator as matrix_module

    snapshots = [
        {"price": 0.0000004544, "source": "shared-quote-cache", "age_ms": 1000, "timestamp_ms": 1777891570000},
        {"price": 0.0000004544, "source": "shared-quote-cache", "age_ms": 3000, "timestamp_ms": 1777891570000},
    ]

    monkeypatch.setattr(matrix_module, "_lazy_import", lambda: None)
    monkeypatch.setattr(matrix_module, "_price_snapshot_fn", lambda *_args, **_kwargs: snapshots.pop(0))
    monkeypatch.setattr(matrix_module.time, "sleep", lambda *_args, **_kwargs: None)

    score, reason, prices = score_realtime_momentum("TokenCA", "Pool", interval_sec=0)

    assert score == 0
    assert reason.startswith("flat_no_fresh_tick")
    assert "declining" not in reason
    assert prices == [0.0000004544, 0.0000004544]
