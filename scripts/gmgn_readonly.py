#!/usr/bin/env python3
"""
Read-only GMGN OpenAPI adapter.

This module deliberately exposes market/risk enrichment only. It never calls
swap/order commands, and callers must treat the result as observational data
unless a later strategy change explicitly wires it into a gate.
"""

import json
import os
import shutil
import subprocess
import time


GMGN_READONLY_ENABLED = os.environ.get("GMGN_READONLY_ENABLED", "false").lower() == "true"
GMGN_READONLY_CACHE_SEC = float(os.environ.get("GMGN_READONLY_CACHE_SEC", "60"))
GMGN_READONLY_TIMEOUT_SEC = float(os.environ.get("GMGN_READONLY_TIMEOUT_SEC", "6"))
GMGN_ALLOW_DEMO_KEY = os.environ.get("GMGN_ALLOW_DEMO_KEY", "false").lower() == "true"
GMGN_DEMO_API_KEY = "gmgn_solbscbaseethmonadtron"

_risk_cache = {}


def clear_gmgn_readonly_cache():
    _risk_cache.clear()


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _nested_get(payload, *path, default=None):
    cur = payload
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


def normalize_gmgn_token_info(raw):
    """Normalize GMGN token info into stable risk/enrichment fields."""
    raw = raw or {}
    dev = raw.get("dev") if isinstance(raw.get("dev"), dict) else {}
    stat = raw.get("stat") if isinstance(raw.get("stat"), dict) else {}
    wallet_tags = raw.get("wallet_tags_stat") if isinstance(raw.get("wallet_tags_stat"), dict) else {}
    pool = raw.get("pool") if isinstance(raw.get("pool"), dict) else {}

    top10 = _to_float(
        stat.get("top_10_holder_rate", dev.get("top_10_holder_rate", raw.get("top_10_holder_rate")))
    )
    rat_rate = _to_float(stat.get("top_rat_trader_percentage", raw.get("rat_trader_amount_rate")))
    bundler_rate = _to_float(stat.get("top_bundler_trader_percentage", raw.get("bundler_rate")))
    entrapment_rate = _to_float(stat.get("top_entrapment_trader_percentage", raw.get("entrapment_ratio")))
    bot_rate = _to_float(stat.get("bot_degen_rate", raw.get("bot_degen_rate")))
    fresh_wallet_rate = _to_float(stat.get("fresh_wallet_rate", raw.get("fresh_wallet_rate")))
    dev_hold_rate = _to_float(stat.get("dev_team_hold_rate", raw.get("dev_team_hold_rate")))
    creator_hold_rate = _to_float(stat.get("creator_hold_rate", raw.get("creator_hold_rate")))

    creator_status = (
        dev.get("creator_token_status")
        or raw.get("creator_token_status")
        or ("creator_close" if _to_float(dev.get("creator_token_balance")) <= 0 else "")
    )

    return {
        "source": "gmgn",
        "address": raw.get("address"),
        "symbol": raw.get("symbol"),
        "name": raw.get("name"),
        "price": _to_float(raw.get("price")),
        "liquidity_usd": _to_float(raw.get("liquidity", _nested_get(raw, "pool", "liquidity"))),
        "initial_liquidity_usd": _to_float(pool.get("initial_liquidity")),
        "market_cap": _to_float(raw.get("market_cap") or raw.get("usd_market_cap")),
        "holder_count": _to_int(raw.get("holder_count", stat.get("holder_count"))),
        "creation_timestamp": _to_int(raw.get("creation_timestamp")),
        "open_timestamp": _to_int(raw.get("open_timestamp")),
        "launchpad": raw.get("launchpad"),
        "launchpad_platform": raw.get("launchpad_platform"),
        "exchange": pool.get("exchange") or raw.get("exchange"),
        "top10_holder_rate": top10,
        "top10_holder_pct": top10 * 100.0 if top10 <= 1.0 else top10,
        "rat_trader_amount_rate": rat_rate,
        "bundler_rate": bundler_rate,
        "entrapment_ratio": entrapment_rate,
        "bot_degen_rate": bot_rate,
        "fresh_wallet_rate": fresh_wallet_rate,
        "dev_team_hold_rate": dev_hold_rate,
        "creator_hold_rate": creator_hold_rate,
        "creator_token_status": creator_status,
        "creator_close": bool(raw.get("creator_close")) or creator_status == "creator_close",
        "smart_degen_count": _to_int(
            raw.get("smart_degen_count", wallet_tags.get("smart_wallets"))
        ),
        "renowned_count": _to_int(raw.get("renowned_count", wallet_tags.get("renowned_wallets"))),
        "sniper_count": _to_int(raw.get("sniper_count", wallet_tags.get("sniper_wallets"))),
        "bundler_wallets": _to_int(wallet_tags.get("bundler_wallets")),
        "rat_trader_wallets": _to_int(wallet_tags.get("rat_trader_wallets")),
        "bot_degen_count": _to_int(stat.get("bot_degen_count", raw.get("bot_degen_count"))),
        "cto_flag": _to_int(dev.get("cto_flag", raw.get("cto_flag"))),
        "dexscr_update_link": _to_int(dev.get("dexscr_update_link", raw.get("dexscr_update_link"))),
        "gmgn_url": _nested_get(raw, "link", "gmgn", default=""),
    }


def gmgn_risk_flags(enrichment):
    """Return conservative flags for analysis/logging. Does not block entries."""
    if not enrichment or enrichment.get("available") is False:
        return []
    flags = []
    if _to_float(enrichment.get("bundler_rate")) > 0.3:
        flags.append("gmgn_high_bundler_rate")
    if _to_float(enrichment.get("rat_trader_amount_rate")) > 0.3:
        flags.append("gmgn_high_rat_trader_rate")
    if _to_float(enrichment.get("entrapment_ratio")) > 0.3:
        flags.append("gmgn_high_entrapment_ratio")
    if _to_float(enrichment.get("top10_holder_rate")) > 0.5:
        flags.append("gmgn_high_top10_concentration")
    if _to_float(enrichment.get("dev_team_hold_rate")) > 0.05:
        flags.append("gmgn_dev_team_holding")
    if _to_float(enrichment.get("creator_hold_rate")) > 0.05:
        flags.append("gmgn_creator_holding")
    return flags


def _api_key_available(env):
    if env.get("GMGN_API_KEY"):
        return True
    return GMGN_ALLOW_DEMO_KEY


def _run_gmgn_cli(args, timeout=GMGN_READONLY_TIMEOUT_SEC, env=None):
    env = dict(env or os.environ)
    if not env.get("GMGN_API_KEY") and GMGN_ALLOW_DEMO_KEY:
        env["GMGN_API_KEY"] = GMGN_DEMO_API_KEY
    completed = subprocess.run(
        ["gmgn-cli", *args, "--raw"],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "gmgn-cli failed").strip())
    return json.loads(completed.stdout)


def fetch_gmgn_token_enrichment(token_ca, chain="sol", enabled=None, now=None):
    """
    Fetch read-only GMGN token enrichment.

    Returns a dict. On disabled/unavailable/failure, returns a small structured
    status instead of raising, so trading flow can degrade to existing behavior.
    """
    if enabled is None:
        enabled = GMGN_READONLY_ENABLED
    if not enabled:
        return {"available": False, "source": "gmgn", "reason": "disabled"}
    if not token_ca:
        return {"available": False, "source": "gmgn", "reason": "missing_token"}
    if not shutil.which("gmgn-cli"):
        return {"available": False, "source": "gmgn", "reason": "gmgn_cli_missing"}
    if not _api_key_available(os.environ):
        return {"available": False, "source": "gmgn", "reason": "api_key_missing"}

    now = time.time() if now is None else now
    cache_key = (chain, token_ca)
    cached = _risk_cache.get(cache_key)
    if cached and now - cached["fetched_at"] < GMGN_READONLY_CACHE_SEC:
        return cached["data"]

    try:
        raw = _run_gmgn_cli(["token", "info", "--chain", chain, "--address", token_ca])
        normalized = normalize_gmgn_token_info(raw)
        normalized["available"] = True
        normalized["risk_flags"] = gmgn_risk_flags(normalized)
        _risk_cache[cache_key] = {"fetched_at": now, "data": normalized}
        return normalized
    except Exception as exc:
        fallback = {"available": False, "source": "gmgn", "reason": "fetch_failed", "error": str(exc)[:300]}
        if cached:
            stale = dict(cached["data"])
            stale["stale"] = True
            stale["last_error"] = fallback["error"]
            return stale
        return fallback
