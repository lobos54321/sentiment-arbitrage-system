#!/usr/bin/env python3

import os
import sys
from concurrent.futures import Future


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from paper_trade_monitor import smart_entry_result_ready  # noqa: E402
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
