"""Read-only provider quote hydration for A_CLASS candidates.

This module only refreshes execution evidence.  It never creates an order and
it never overrides hard security or budget gates.
"""

import os
import time
from threading import RLock

from v27_record_raw_provider_probe_evidence import _fetch_jupiter_order


SOL_MINT = "So11111111111111111111111111111111111111112"
_STATE_LOCK = RLock()
_QUOTE_CACHE = {}
_INFLIGHT = set()
_PROVIDER_STATE = {
    "failures": 0,
    "backoff_until": 0.0,
    "last_reason": None,
}


def reset_hydrator_state():
    """Test helper and safe operator reset."""
    with _STATE_LOCK:
        _QUOTE_CACHE.clear()
        _INFLIGHT.clear()
        _PROVIDER_STATE.update({"failures": 0, "backoff_until": 0.0, "last_reason": None})


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


def _cache_key(candidate, config):
    return (
        str(getattr(config, "provider_hydrate_endpoint_base", "https://api.jup.ag/ultra/v1/order")),
        str(getattr(candidate, "token_ca", "") or "").strip(),
        str(getattr(config, "provider_hydrate_amount_raw", "1000000")),
        int(getattr(config, "provider_hydrate_slippage_bps", 500) or 500),
    )


def _with_outcome(evidence, *, outcome, cache_hit=False, backoff_remaining_sec=None):
    data = dict(evidence or {})
    data["provider_hydrate_outcome"] = outcome
    data["provider_hydrate_cache_hit"] = bool(cache_hit)
    if backoff_remaining_sec is not None:
        data["provider_hydrate_backoff_remaining_sec"] = max(0.0, float(backoff_remaining_sec))
    return data


def _failure_outcome(reason):
    text = str(reason or "").lower()
    if any(token in text for token in ("no_route", "token_not_tradable", "trapped", "not tradable")):
        return "market_fail"
    if any(token in text for token in ("rate", "429", "timeout", "provider", "unknown", "request_failed", "upstream")):
        return "infra_fail"
    return "infra_fail"


def _cache_ttl_for(evidence, config):
    if evidence.get("quote_executable"):
        return max(0.0, float(getattr(config, "provider_hydrate_cache_ttl_sec", 20.0) or 20.0))
    return max(0.0, float(getattr(config, "provider_hydrate_failure_cache_ttl_sec", 8.0) or 8.0))


def _set_provider_backoff(reason, now_ts, config):
    text = str(reason or "").lower()
    if not any(token in text for token in ("rate", "429", "timeout", "upstream", "provider_request_failed", "provider_unknown")):
        with _STATE_LOCK:
            _PROVIDER_STATE["failures"] = 0
            _PROVIDER_STATE["last_reason"] = reason
        return
    base = max(0.0, float(getattr(config, "provider_hydrate_rate_limit_backoff_sec", 60.0) or 60.0))
    cap = max(base, float(getattr(config, "provider_hydrate_backoff_max_sec", 300.0) or 300.0))
    with _STATE_LOCK:
        failures = int(_PROVIDER_STATE.get("failures") or 0) + 1
        delay = min(cap, base * (2 ** min(failures - 1, 4)))
        _PROVIDER_STATE.update({
            "failures": failures,
            "backoff_until": float(now_ts) + delay,
            "last_reason": reason,
        })


def hydrate_provider_quote(candidate, *, now_ts=None, config=None, fetcher=None):
    """Fetch a tiny SOL->token quote and translate it into A_CLASS evidence."""
    now_ts = float(now_ts if now_ts is not None else time.time())
    token_ca = str(getattr(candidate, "token_ca", "") or "").strip()
    if not token_ca:
        return {}
    key = _cache_key(candidate, config)
    with _STATE_LOCK:
        cached = _QUOTE_CACHE.get(key)
        if cached and float(cached.get("expires_at") or 0.0) > now_ts:
            return _with_outcome(cached.get("evidence") or {}, outcome=cached.get("outcome") or "cache_hit", cache_hit=True)
        backoff_until = float(_PROVIDER_STATE.get("backoff_until") or 0.0)
        if backoff_until > now_ts:
            return _with_outcome(
                {
                    "quote_available": False,
                    "quote_executable": False,
                    "quote_clean": False,
                    "quote_source": "jupiter_ultra_provider_hydrate",
                    "quote_age_sec": 0.0,
                    "quote_ts": now_ts,
                    "route_available": False,
                    "route_failure_reason": "provider_backoff_active",
                    "data_confidence": "provider_hydrate_failed",
                    "provider_hydrate_reason": _PROVIDER_STATE.get("last_reason") or "provider_backoff_active",
                },
                outcome="skipped_backoff",
                backoff_remaining_sec=backoff_until - now_ts,
            )
        if key in _INFLIGHT:
            return _with_outcome(
                {
                    "quote_available": False,
                    "quote_executable": False,
                    "quote_clean": False,
                    "quote_source": "jupiter_ultra_provider_hydrate",
                    "quote_age_sec": 0.0,
                    "quote_ts": now_ts,
                    "route_available": False,
                    "route_failure_reason": "provider_hydrate_inflight_duplicate",
                    "data_confidence": "provider_hydrate_failed",
                    "provider_hydrate_reason": "provider_hydrate_inflight_duplicate",
                },
                outcome="skipped_inflight_duplicate",
            )
        _INFLIGHT.add(key)

    fetcher = fetcher or _fetch_jupiter_order
    try:
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
            evidence = {key: value for key, value in evidence.items() if value is not None}
            outcome = "success"
            with _STATE_LOCK:
                _PROVIDER_STATE.update({"failures": 0, "backoff_until": 0.0, "last_reason": None})
            return _with_outcome(evidence, outcome=outcome)

        reason = _classify_failure(status, data)
        evidence = {
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
        outcome = _failure_outcome(reason)
        _set_provider_backoff(reason, now_ts, config)
        return _with_outcome(evidence, outcome=outcome)
    except Exception as exc:
        reason = "provider_hydrate_exception"
        evidence = {
            "quote_available": False,
            "quote_executable": False,
            "quote_clean": False,
            "quote_source": "jupiter_ultra_provider_hydrate",
            "quote_age_sec": 0.0,
            "quote_ts": now_ts,
            "route_available": False,
            "route_failure_reason": reason,
            "data_confidence": "provider_hydrate_failed",
            "provider_hydrate_reason": f"{reason}:{exc}",
        }
        outcome = "infra_fail"
        _set_provider_backoff(reason, now_ts, config)
        return _with_outcome(evidence, outcome=outcome)
    finally:
        try:
            evidence  # noqa: B018
        except NameError:
            evidence = {
                "quote_available": False,
                "quote_executable": False,
                "quote_clean": False,
                "quote_source": "jupiter_ultra_provider_hydrate",
                "quote_age_sec": 0.0,
                "quote_ts": now_ts,
                "route_available": False,
                "route_failure_reason": "provider_hydrate_exception",
                "data_confidence": "provider_hydrate_failed",
                "provider_hydrate_reason": "provider_hydrate_exception",
            }
            outcome = "infra_fail"
        with _STATE_LOCK:
            _INFLIGHT.discard(key)
            ttl = _cache_ttl_for(evidence, config)
            if ttl > 0:
                _QUOTE_CACHE[key] = {
                    "evidence": dict(evidence),
                    "outcome": outcome,
                    "expires_at": now_ts + ttl,
                }
