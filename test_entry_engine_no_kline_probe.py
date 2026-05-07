import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

import entry_engine  # noqa: E402
import paper_trade_monitor as monitor  # noqa: E402


def _trend():
    return {
        "buys_m5": 20,
        "sells_m5": 10,
        "price_change_m5": 12.0,
        "vol_m5": 8000.0,
        "vol_h1": 192000.0,
        "fdv": 119000.0,
        "market_cap": 119000.0,
        "liquidity_usd": 30000.0,
    }


def _flat_bars():
    return [
        {"open": 1.0, "close": 1.0, "high": 1.1, "complete": True},
        {"open": 1.0, "close": 1.0, "high": 1.1, "complete": True},
    ]


def _ath_policy():
    return {
        "lifecycle_profile": "ATH_CONTINUATION",
        "allowed_entry_modes": ["momentum_direct_entry", "smart_entry_pullback_bounce"],
        "detail": {"route": "ATH"},
    }


def test_ath_no_kline_compound_reject_returns_tiny_probe(monkeypatch):
    monkeypatch.setattr(monitor, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "mock", 0))
    monkeypatch.setattr(entry_engine, "fetch_dexscreener_trend_snapshot", lambda *args, **kwargs: _trend())
    monkeypatch.setattr(entry_engine, "get_recent_synthetic_bars", lambda *args, **kwargs: _flat_bars())
    monkeypatch.setattr(entry_engine, "calculate_ema_deviation", lambda *args, **kwargs: (None, None))

    should_enter, mode, detail, price = entry_engine.evaluate_smart_entry(
        "TokenCA",
        symbol="Runner",
        pool_address="Pool",
        momentum_pct=1.0,
        entry_readiness_policy=_ath_policy(),
    )

    assert should_enter is True
    assert mode == "ath_no_kline_tiny_probe"
    assert "node=ath_no_kline_tiny_probe" in detail
    assert price == 1.0


def test_no_kline_compound_reject_still_blocks_non_ath(monkeypatch):
    monkeypatch.setattr(monitor, "fetch_realtime_price", lambda *args, **kwargs: (1.0, "mock", 0))
    monkeypatch.setattr(entry_engine, "fetch_dexscreener_trend_snapshot", lambda *args, **kwargs: _trend())
    monkeypatch.setattr(entry_engine, "get_recent_synthetic_bars", lambda *args, **kwargs: _flat_bars())
    monkeypatch.setattr(entry_engine, "calculate_ema_deviation", lambda *args, **kwargs: (None, None))

    policy = {
        "lifecycle_profile": "LOTTO_NORMAL",
        "allowed_entry_modes": ["momentum_direct_entry", "smart_entry_pullback_bounce"],
        "detail": {"route": "LOTTO"},
    }

    should_enter, mode, detail, price = entry_engine.evaluate_smart_entry(
        "TokenCA",
        symbol="Runner",
        pool_address="Pool",
        momentum_pct=1.0,
        entry_readiness_policy=policy,
    )

    assert should_enter is False
    assert mode == "no_kline_low_volume"
    assert "kline_unconfirmed" in detail
    assert price is None
