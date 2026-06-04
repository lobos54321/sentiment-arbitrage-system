"""Executable SOL valuation primitives.

This module is intentionally small and dependency-free.  It gives the rest of
the strategy stack one shared way to answer: "what is this position/quote worth
in executable SOL right now?"  Mark prices and display prices can still exist,
but risk contracts and ledgers should consume this result.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
import time
from typing import Any


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available", "executable"}
    return bool(value)


def _quote_ts_seconds(quote: Any) -> float | None:
    for key in ("quote_ts", "quoteTs", "ts", "timestamp", "updated_at", "created_at"):
        raw = _safe_float(_get(quote, key), None)
        if raw is None:
            continue
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    return None


def _first_float(value: Any, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        number = _safe_float(_get(value, key), None)
        if number is not None:
            return number
    return None


@dataclass(frozen=True)
class ExecutableSolValuation:
    usable: bool
    valuation_sol: float | None
    reason: str
    hard_blockers: tuple[str, ...]
    quote_source: str | None = None
    quote_age_sec: float | None = None
    quote_executable: bool = False
    route_available: bool = False
    route_clean: bool = False
    stale: bool = False
    input_sol: float | None = None
    output_sol: float | None = None
    price_sol_per_token: float | None = None
    token_amount_raw: str | None = None
    token_decimals: int | None = None
    spread_pct: float | None = None
    slippage_bps: float | None = None
    data_confidence: str = "quote_only"

    def to_dict(self) -> dict:
        return asdict(self)


def executable_sol_valuation(
    quote: Any,
    *,
    now_ts: float | None = None,
    max_quote_age_sec: float = 10.0,
    require_route: bool = True,
    require_executable: bool = True,
) -> ExecutableSolValuation:
    """Return a single executable SOL valuation for a quote-like object.

    The function accepts both normalized quote dictionaries and common fields
    from existing paper-trade/audit payloads.  It does not try to infer value
    from mark price unless a SOL amount is already present.
    """
    now_ts = float(now_ts if now_ts is not None else time.time())
    blockers: list[str] = []
    quote_source = _get(quote, "quote_source") or _get(quote, "source") or _get(quote, "provider")

    route_available = _truthy(
        _get(quote, "route_available", _get(quote, "routeAvailable", _get(quote, "success", False)))
    )
    quote_executable = _truthy(
        _get(quote, "quote_executable", _get(quote, "quoteExecutable", _get(quote, "success", False)))
    )
    quote_clean = _truthy(_get(quote, "quote_clean", _get(quote, "quoteClean", quote_executable and route_available)))

    quote_age_sec = _safe_float(_get(quote, "quote_age_sec", _get(quote, "quoteAgeSec")), None)
    quote_ts = _quote_ts_seconds(quote)
    if quote_age_sec is None and quote_ts is not None:
        quote_age_sec = max(0.0, now_ts - quote_ts)
    stale = quote_age_sec is None or quote_age_sec > max_quote_age_sec

    if require_route and not route_available:
        blockers.append("route_unavailable")
    if require_executable and not quote_executable:
        blockers.append("quote_not_executable")
    if stale:
        blockers.append("quote_stale")

    output_sol = _first_float(
        quote,
        (
            "output_sol",
            "out_sol",
            "quote_out_sol",
            "exit_quote_out_sol",
            "realized_exit_sol",
            "valuation_sol",
        ),
    )
    input_sol = _first_float(quote, ("input_sol", "in_sol", "entry_size_sol", "amount_sol"))
    valuation_sol = output_sol if output_sol is not None else input_sol
    if valuation_sol is None or valuation_sol < 0:
        blockers.append("valuation_sol_missing")

    price_sol_per_token = _first_float(
        quote,
        ("price_sol_per_token", "quote_price_sol", "entry_price", "exit_price", "price"),
    )
    token_decimals_float = _safe_float(_get(quote, "token_decimals", _get(quote, "decimals")), None)

    usable = not blockers
    return ExecutableSolValuation(
        usable=usable,
        valuation_sol=valuation_sol if valuation_sol is not None else None,
        reason="executable_sol_valuation_pass" if usable else "executable_sol_valuation_blocked",
        hard_blockers=tuple(blockers),
        quote_source=str(quote_source) if quote_source else None,
        quote_age_sec=quote_age_sec,
        quote_executable=quote_executable,
        route_available=route_available,
        route_clean=quote_clean and quote_executable and route_available,
        stale=stale,
        input_sol=input_sol,
        output_sol=output_sol,
        price_sol_per_token=price_sol_per_token,
        token_amount_raw=str(_get(quote, "token_amount_raw", _get(quote, "outAmount", "")) or "") or None,
        token_decimals=int(token_decimals_float) if token_decimals_float is not None else None,
        spread_pct=_safe_float(_get(quote, "spread_pct", _get(quote, "quote_spread_pct")), None),
        slippage_bps=_safe_float(_get(quote, "slippage_bps", _get(quote, "slippageBps")), None),
        data_confidence=str(_get(quote, "data_confidence", "quote_only") or "quote_only"),
    )


def defined_loss_pct_from_valuation(entry_sol: Any, valuation_sol: Any) -> float | None:
    """Return fractional loss from entry SOL to current executable SOL value."""
    entry = _safe_float(entry_sol, None)
    value = _safe_float(valuation_sol, None)
    if entry is None or entry <= 0 or value is None:
        return None
    return max(0.0, (entry - value) / entry)
