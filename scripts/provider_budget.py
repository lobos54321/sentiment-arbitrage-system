#!/usr/bin/env python3
"""Process-local provider request budgets with shared cooldown semantics.

The paper system has several independent loops that can all hit the same
upstream provider. This module gives those loops one small, conservative
budgeting API so 429s turn into cooldowns instead of retry storms.
"""

from __future__ import annotations

from collections import deque
import os
import threading
import time


def _env_flag(name: str, default: str = "true") -> bool:
    return os.environ.get(name, default).strip().lower() not in {"0", "false", "no", "off"}


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default).strip())
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: str) -> int:
    try:
        return int(float(os.environ.get(name, default).strip()))
    except (TypeError, ValueError):
        return int(float(default))


PROVIDER_BUDGET_ENABLED = _env_flag("PROVIDER_BUDGET_ENABLED", "true")
PROVIDER_BUDGET_WINDOW_SEC = _env_float("PROVIDER_BUDGET_WINDOW_SEC", "60")
PROVIDER_BUDGET_DEFAULT_PER_MIN = _env_int("PROVIDER_BUDGET_DEFAULT_PER_MIN", "120")
PROVIDER_BUDGET_COOLDOWN_SEC = _env_float("PROVIDER_BUDGET_COOLDOWN_SEC", "60")
PROVIDER_BUDGET_MAX_COOLDOWN_SEC = _env_float("PROVIDER_BUDGET_MAX_COOLDOWN_SEC", "600")

_PROVIDER_DEFAULT_LIMITS = {
    "dexscreener": 90,
    "helius": 45,
    "gmgn": 30,
    "shared_market_data": 180,
}

_LOCK = threading.RLock()
_STATE: dict[str, dict] = {}


def _provider_key(provider: str) -> str:
    return str(provider or "unknown").strip().lower().replace("-", "_")


def provider_limit_per_min(provider: str) -> int:
    key = _provider_key(provider)
    env_name = f"PROVIDER_BUDGET_{key.upper()}_PER_MIN"
    return _env_int(env_name, str(_PROVIDER_DEFAULT_LIMITS.get(key, PROVIDER_BUDGET_DEFAULT_PER_MIN)))


def _state(provider: str) -> dict:
    key = _provider_key(provider)
    return _STATE.setdefault(
        key,
        {
            "requests": deque(),
            "cooldown_until": 0.0,
            "consecutive_rate_limits": 0,
            "allowed_n": 0,
            "blocked_n": 0,
            "rate_limited_n": 0,
            "success_n": 0,
            "failure_n": 0,
            "last_reason": None,
            "last_error": None,
        },
    )


def provider_request_allowed(provider: str, *, cost: int = 1, now: float | None = None) -> dict:
    """Reserve provider budget for one request batch.

    Returns a serializable detail dict. When ``pass`` is False callers should
    use cache/stale fallback or fail closed without hitting the provider.
    """
    key = _provider_key(provider)
    now = float(time.time() if now is None else now)
    cost = max(1, int(cost or 1))
    limit = provider_limit_per_min(key)
    if not PROVIDER_BUDGET_ENABLED or limit <= 0:
        return {
            "pass": True,
            "provider": key,
            "reason": "provider_budget_disabled",
            "limit_per_min": limit,
            "remaining": None,
            "cooldown_until": 0.0,
        }

    with _LOCK:
        state = _state(key)
        while state["requests"] and now - state["requests"][0] >= PROVIDER_BUDGET_WINDOW_SEC:
            state["requests"].popleft()
        if state["cooldown_until"] > now:
            state["blocked_n"] += 1
            state["last_reason"] = "provider_budget_cooldown"
            return {
                "pass": False,
                "provider": key,
                "reason": "provider_budget_cooldown",
                "limit_per_min": limit,
                "remaining": max(0, limit - len(state["requests"])),
                "cooldown_until": state["cooldown_until"],
                "cooldown_remaining_sec": round(state["cooldown_until"] - now, 3),
            }
        if len(state["requests"]) + cost > limit:
            state["blocked_n"] += 1
            state["last_reason"] = "provider_budget_exhausted"
            return {
                "pass": False,
                "provider": key,
                "reason": "provider_budget_exhausted",
                "limit_per_min": limit,
                "remaining": max(0, limit - len(state["requests"])),
                "cooldown_until": state["cooldown_until"],
            }
        for _ in range(cost):
            state["requests"].append(now)
        state["allowed_n"] += 1
        state["last_reason"] = "provider_budget_allow"
        return {
            "pass": True,
            "provider": key,
            "reason": "provider_budget_allow",
            "limit_per_min": limit,
            "remaining": max(0, limit - len(state["requests"])),
            "cooldown_until": state["cooldown_until"],
        }


def _looks_rate_limited(error: object) -> bool:
    text = str(error or "").lower()
    return "429" in text or "rate limit" in text or "too many requests" in text


def record_provider_result(
    provider: str,
    *,
    success: bool = True,
    rate_limited: bool = False,
    error: object = None,
    now: float | None = None,
) -> dict:
    """Record provider outcome and open cooldowns on 429/rate-limit signals."""
    key = _provider_key(provider)
    now = float(time.time() if now is None else now)
    rate_limited = bool(rate_limited or _looks_rate_limited(error))
    with _LOCK:
        state = _state(key)
        if rate_limited:
            state["consecutive_rate_limits"] += 1
            state["rate_limited_n"] += 1
            backoff = min(
                PROVIDER_BUDGET_MAX_COOLDOWN_SEC,
                PROVIDER_BUDGET_COOLDOWN_SEC * (2 ** max(0, state["consecutive_rate_limits"] - 1)),
            )
            state["cooldown_until"] = max(state["cooldown_until"], now + backoff)
            state["last_reason"] = "provider_rate_limited"
        elif success:
            state["success_n"] += 1
            state["consecutive_rate_limits"] = 0
            state["last_reason"] = "provider_success"
        else:
            state["failure_n"] += 1
            state["last_reason"] = "provider_failure"
        if error is not None:
            state["last_error"] = str(error)[:300]
        return provider_budget_snapshot(provider=key, now=now)


def provider_budget_snapshot(provider: str | None = None, *, now: float | None = None) -> dict:
    now = float(time.time() if now is None else now)
    with _LOCK:
        keys = [_provider_key(provider)] if provider else sorted(_STATE.keys())
        providers = {}
        for key in keys:
            state = _state(key)
            while state["requests"] and now - state["requests"][0] >= PROVIDER_BUDGET_WINDOW_SEC:
                state["requests"].popleft()
            limit = provider_limit_per_min(key)
            providers[key] = {
                "limit_per_min": limit,
                "in_window": len(state["requests"]),
                "remaining": max(0, limit - len(state["requests"])) if limit > 0 else None,
                "cooldown_until": state["cooldown_until"],
                "cooldown_remaining_sec": max(0.0, state["cooldown_until"] - now),
                "consecutive_rate_limits": state["consecutive_rate_limits"],
                "allowed_n": state["allowed_n"],
                "blocked_n": state["blocked_n"],
                "rate_limited_n": state["rate_limited_n"],
                "success_n": state["success_n"],
                "failure_n": state["failure_n"],
                "last_reason": state["last_reason"],
                "last_error": state["last_error"],
            }
        return {
            "enabled": PROVIDER_BUDGET_ENABLED,
            "window_sec": PROVIDER_BUDGET_WINDOW_SEC,
            "providers": providers,
        }


def clear_provider_budget_state() -> None:
    with _LOCK:
        _STATE.clear()
