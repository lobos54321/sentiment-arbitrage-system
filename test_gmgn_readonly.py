#!/usr/bin/env python3

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from gmgn_readonly import (  # noqa: E402
    clear_gmgn_readonly_cache,
    fetch_gmgn_token_enrichment,
    gmgn_readonly_runtime_status,
    gmgn_risk_flags,
    normalize_gmgn_token_info,
)


def test_normalize_gmgn_token_info_extracts_risk_fields():
    normalized = normalize_gmgn_token_info(
        {
            "address": "TokenCA",
            "symbol": "DOG",
            "price": "0.0001",
            "liquidity": "24000.5",
            "holder_count": 321,
            "creation_timestamp": 1770000000,
            "launchpad_platform": "Pump.fun",
            "pool": {"exchange": "pump_amm", "initial_liquidity": "12000"},
            "dev": {
                "creator_token_status": "creator_close",
                "top_10_holder_rate": "0.27",
                "cto_flag": 1,
                "dexscr_update_link": 1,
            },
            "stat": {
                "top_rat_trader_percentage": "0.12",
                "top_bundler_trader_percentage": "0.44",
                "top_entrapment_trader_percentage": "0.03",
                "bot_degen_rate": "0.22",
                "fresh_wallet_rate": "0.09",
                "dev_team_hold_rate": "0",
                "creator_hold_rate": "0",
            },
            "wallet_tags_stat": {
                "smart_wallets": 4,
                "renowned_wallets": 2,
                "sniper_wallets": 17,
                "bundler_wallets": 99,
            },
            "link": {"gmgn": "https://gmgn.ai/sol/token/TokenCA"},
        }
    )

    assert normalized["source"] == "gmgn"
    assert normalized["address"] == "TokenCA"
    assert normalized["price"] == 0.0001
    assert normalized["liquidity_usd"] == 24000.5
    assert normalized["initial_liquidity_usd"] == 12000
    assert normalized["top10_holder_rate"] == 0.27
    assert normalized["top10_holder_pct"] == 27.0
    assert normalized["bundler_rate"] == 0.44
    assert normalized["smart_degen_count"] == 4
    assert normalized["renowned_count"] == 2
    assert normalized["creator_close"] is True
    assert normalized["gmgn_url"].endswith("/TokenCA")


def test_gmgn_risk_flags_are_observational():
    flags = gmgn_risk_flags(
        {
            "available": True,
            "bundler_rate": 0.44,
            "rat_trader_amount_rate": 0.01,
            "entrapment_ratio": 0.02,
            "top10_holder_rate": 0.2,
            "dev_team_hold_rate": 0.0,
            "creator_hold_rate": 0.0,
        }
    )

    assert flags == ["gmgn_high_bundler_rate"]


def test_fetch_gmgn_token_enrichment_disabled_does_not_call_cli(monkeypatch):
    monkeypatch.setattr("gmgn_readonly._run_gmgn_cli", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("called")))

    result = fetch_gmgn_token_enrichment("TokenCA", enabled=False)

    assert result == {"available": False, "source": "gmgn", "reason": "disabled"}


def test_gmgn_runtime_status_strips_env_values(monkeypatch):
    monkeypatch.setenv("GMGN_API_KEY", "  gmgn_test_key  ")
    monkeypatch.setattr("gmgn_readonly.GMGN_READONLY_ENABLED", True)
    monkeypatch.setattr("gmgn_readonly.shutil.which", lambda name: "/usr/local/bin/gmgn-cli")

    status = gmgn_readonly_runtime_status()

    assert status["enabled"] is True
    assert status["api_key_present"] is True
    assert status["api_key_prefix"] == "gmgn_tes"
    assert status["gmgn_cli"].endswith("gmgn-cli")


def test_fetch_gmgn_token_enrichment_normalizes_cli_result(monkeypatch):
    clear_gmgn_readonly_cache()
    monkeypatch.setenv("GMGN_API_KEY", "test-key")
    monkeypatch.setattr("gmgn_readonly.shutil.which", lambda name: "/usr/local/bin/gmgn-cli")
    monkeypatch.setattr(
        "gmgn_readonly._run_gmgn_cli",
        lambda *_args, **_kwargs: {
            "address": "TokenCA",
            "symbol": "DOG",
            "stat": {"top_bundler_trader_percentage": "0.51"},
            "wallet_tags_stat": {"smart_wallets": 3},
        },
    )

    result = fetch_gmgn_token_enrichment("TokenCA", enabled=True, now=1000)

    assert result["available"] is True
    assert result["address"] == "TokenCA"
    assert result["smart_degen_count"] == 3
    assert result["risk_flags"] == ["gmgn_high_bundler_rate"]
