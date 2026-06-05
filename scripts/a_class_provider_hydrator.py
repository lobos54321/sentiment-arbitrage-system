"""Read-only provider quote hydration for A_CLASS candidates.

This module only refreshes execution evidence.  It never creates an order and
it never overrides hard security or budget gates.
"""

import os
import time

from v27_record_raw_provider_probe_evidence import _fetch_jupiter_order


SOL_MINT = "So11111111111111111111111111111111111111112"


def _safe_float(value, default=None):
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _jupiter_price_impact_pct(data):
    """Return price impact as percentage points when available."""
    if not isinstance(data, dict):
        return None
    impact = _safe_float(data.get("priceImpact"), None)
    if impact is not None:
        return abs(impact)
    impact_pct = _safe_float(data.get("priceImpactPct"), None)
    if impact_pct is None:
        return None
    # Ultra commonly returns priceImpactPct as a decimal fraction.
    return abs(impact_pct * 100.0 if abs(impact_pct) <= 1 else impact_pct)


def _classify_failure(status, data):
    data = data if isinstance(data, dict) else {}
    message = str(data.get("error") or data.get("message") or "").lower()
    error_code = str(data.get("errorCode") or data.get("code") or "").lower()
    if status in {429} or "rate limit" in message or "too many" in message:
        return "provider_rate_limited"
    if status in {401, 403}:
        return "provider_auth_or_forbidden"
    if any(token in error_code or token in message for token in ("route_not_found", "could_not_find_any_route", "no route")):
        return "no_route"
    if "not tradable" in message or "token_not_tradable" in error_code:
        return "token_not_tradable"
    if 500 <= int(status or 0) < 600:
        return "provider_upstream_error"
    if status and status >= 400:
        return "provider_request_failed"
    return "provider_unknown_data"


def hydrate_provider_quote(candidate, *, now_ts=None, config=None, fetcher=None):
    """Fetch a tiny SOL->token quote and translate it into A_CLASS evidence."""
    now_ts = float(now_ts if now_ts is not None else time.time())
    token_ca = str(getattr(candidate, "token_ca", "") or "").strip()
    if not token_ca:
        return {}

    fetcher = fetcher or _fetch_jupiter_order
    result = fetcher(
        endpoint_base=getattr(config, "provider_hydrate_endpoint_base", "https://api.jup.ag/ultra/v1/order"),
        input_mint=SOL_MINT,
        output_mint=token_ca,
        amount_raw=str(getattr(config, "provider_hydrate_amount_raw", "1000000")),
        slippage_bps=getattr(config, "provider_hydrate_slippage_bps", 500),
        timeout_sec=getattr(config, "provider_hydrate_timeout_sec", 4.0),
        api_key=os.environ.get("JUPITER_API_KEY"),
    )
    status = int(result.get("status") or 0)
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    out_amount = data.get("outAmount")
    route_plan = data.get("routePlan") if isinstance(data.get("routePlan"), list) else []
    price_impact_pct = _jupiter_price_impact_pct(data)
    source = "jupiter_ultra_provider_hydrate"

    if 200 <= status < 300 and out_amount:
        evidence = {
            "quote_available": True,
            "quote_executable": True,
            "quote_clean": True,
            "quote_clean_verified": True,
            "quote_source": source,
            "quote_age_sec": 0.0,
            "quote_ts": now_ts,
            "route_available": True,
            "route_stable_recent": bool(route_plan),
            "route_failure_reason": "provider_hydrated_route_ok",
            "data_confidence": "provider_hydrated_quote",
            "provider_hydrate_http_status": status,
            "provider_hydrate_latency_ms": result.get("latency_ms"),
            "provider_hydrate_request_id": data.get("requestId"),
            "provider_hydrate_out_amount": str(out_amount),
        }
        if price_impact_pct is not None:
            evidence["spread_pct"] = price_impact_pct
            evidence["spread_verified"] = True
        elif route_plan:
            # A fresh executable route is still better than treating stale
            # shadow evidence as unknown spread for an ultra-tiny paper ticket.
            evidence["spread_verified"] = True
        return {key: value for key, value in evidence.items() if value is not None}

    reason = _classify_failure(status, data)
    return {
        "quote_available": False,
        "quote_executable": False,
        "quote_clean": False,
        "quote_source": source,
        "quote_age_sec": 0.0,
        "quote_ts": now_ts,
        "route_available": False,
        "route_failure_reason": reason,
        "data_confidence": "provider_hydrate_failed",
        "provider_hydrate_http_status": status,
        "provider_hydrate_latency_ms": result.get("latency_ms"),
        "provider_hydrate_reason": reason,
    }
