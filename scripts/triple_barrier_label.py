#!/usr/bin/env python3
"""Deterministic triple-barrier labels for counterfactual trade paths.

This module is intentionally model-free.  It answers one replay question:
given an executable tiny entry and a sequence of quote-clean path samples, did
the candidate hit upside, hard loss, no-route/trapped, or time first?
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any


@dataclass(frozen=True)
class TripleBarrierConfig:
    upper_barriers: tuple[float, ...] = (0.50, 1.00, 2.00)
    lower_barrier: float = -0.20
    horizon_sec: int = 3600
    require_quote_clean: bool = True


@dataclass(frozen=True)
class TripleBarrierLabel:
    label: str
    terminal_reason: str
    terminal_ts: float | None
    time_to_terminal_sec: float | None
    max_pnl_pct: float | None
    min_pnl_pct: float | None
    hit_upper: float | None
    hit_lower: float | None
    sample_count: int
    quote_clean_sample_count: int
    no_route_seen: bool = False
    trapped_seen: bool = False
    data_quality: str = "unknown"

    def to_dict(self) -> dict:
        return asdict(self)


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


def _get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _sample_ts(row: Any) -> float | None:
    ts = _safe_float(_get(row, "ts", _get(row, "event_ts", _get(row, "sample_ts"))), None)
    if ts is None:
        return None
    return ts / 1000.0 if ts > 10_000_000_000 else ts


def _sample_pnl(row: Any) -> float | None:
    return _safe_float(
        _get(row, "quote_pnl_pct", _get(row, "pnl_pct", _get(row, "mark_pnl_pct", _get(row, "pnl")))),
        None,
    )


def _quote_clean(row: Any) -> bool:
    if any(
        key in row
        for key in ("quote_clean", "quote_executable", "route_available")
    ) if isinstance(row, dict) else False:
        return (
            _truthy(_get(row, "quote_clean", True))
            and _truthy(_get(row, "quote_executable", True))
            and _truthy(_get(row, "route_available", True))
        )
    return True


def label_triple_barrier_path(
    samples: list[Any],
    *,
    entry_ts: float,
    config: TripleBarrierConfig | dict | None = None,
) -> TripleBarrierLabel:
    if isinstance(config, dict):
        config = TripleBarrierConfig(
            upper_barriers=tuple(sorted(float(value) for value in config.get("upper_barriers", (0.50, 1.00, 2.00)))),
            lower_barrier=float(config.get("lower_barrier", -0.20)),
            horizon_sec=int(config.get("horizon_sec", 3600)),
            require_quote_clean=bool(config.get("require_quote_clean", True)),
        )
    config = config or TripleBarrierConfig()
    entry_ts = float(entry_ts)
    horizon_ts = entry_ts + int(config.horizon_sec)

    sorted_samples = sorted(
        [
            sample
            for sample in (samples or [])
            if _sample_ts(sample) is not None and entry_ts <= _sample_ts(sample) <= horizon_ts
        ],
        key=lambda row: _sample_ts(row) or 0.0,
    )
    if not sorted_samples:
        return TripleBarrierLabel(
            label="DATA_MISSING",
            terminal_reason="no_samples_in_horizon",
            terminal_ts=None,
            time_to_terminal_sec=None,
            max_pnl_pct=None,
            min_pnl_pct=None,
            hit_upper=None,
            hit_lower=None,
            sample_count=0,
            quote_clean_sample_count=0,
            data_quality="missing",
        )

    max_pnl = None
    min_pnl = None
    quote_clean_count = 0
    no_route_seen = False
    trapped_seen = False

    for sample in sorted_samples:
        ts = _sample_ts(sample)
        if _truthy(_get(sample, "trapped_flag", False)) or str(_get(sample, "status", "")).lower() == "trapped":
            trapped_seen = True
            return TripleBarrierLabel(
                label="TRAPPED",
                terminal_reason="trapped_absorbing_state",
                terminal_ts=ts,
                time_to_terminal_sec=max(0.0, ts - entry_ts) if ts is not None else None,
                max_pnl_pct=max_pnl,
                min_pnl_pct=min_pnl,
                hit_upper=None,
                hit_lower=None,
                sample_count=len(sorted_samples),
                quote_clean_sample_count=quote_clean_count,
                trapped_seen=True,
                data_quality="quote_clean" if quote_clean_count else "dirty_or_missing_quote",
            )
        if _truthy(_get(sample, "no_route_flag", False)) or str(_get(sample, "status", "")).lower() in {"no_route", "route_unavailable"}:
            no_route_seen = True
            return TripleBarrierLabel(
                label="NO_ROUTE",
                terminal_reason="no_route_absorbing_state",
                terminal_ts=ts,
                time_to_terminal_sec=max(0.0, ts - entry_ts) if ts is not None else None,
                max_pnl_pct=max_pnl,
                min_pnl_pct=min_pnl,
                hit_upper=None,
                hit_lower=None,
                sample_count=len(sorted_samples),
                quote_clean_sample_count=quote_clean_count,
                no_route_seen=True,
                data_quality="quote_clean" if quote_clean_count else "dirty_or_missing_quote",
            )

        clean = _quote_clean(sample)
        if clean:
            quote_clean_count += 1
        elif config.require_quote_clean:
            continue

        pnl = _sample_pnl(sample)
        if pnl is None:
            continue
        max_pnl = pnl if max_pnl is None else max(max_pnl, pnl)
        min_pnl = pnl if min_pnl is None else min(min_pnl, pnl)

        if pnl <= config.lower_barrier:
            return TripleBarrierLabel(
                label="LOWER",
                terminal_reason="lower_barrier_hit",
                terminal_ts=ts,
                time_to_terminal_sec=max(0.0, ts - entry_ts) if ts is not None else None,
                max_pnl_pct=max_pnl,
                min_pnl_pct=min_pnl,
                hit_upper=None,
                hit_lower=config.lower_barrier,
                sample_count=len(sorted_samples),
                quote_clean_sample_count=quote_clean_count,
                no_route_seen=no_route_seen,
                trapped_seen=trapped_seen,
                data_quality="quote_clean" if quote_clean_count else "dirty_or_missing_quote",
            )
        hit = next((barrier for barrier in sorted(config.upper_barriers) if pnl >= barrier), None)
        if hit is not None:
            return TripleBarrierLabel(
                label="UPPER",
                terminal_reason=f"upper_barrier_{int(hit * 100)}pct_hit",
                terminal_ts=ts,
                time_to_terminal_sec=max(0.0, ts - entry_ts) if ts is not None else None,
                max_pnl_pct=max_pnl,
                min_pnl_pct=min_pnl,
                hit_upper=hit,
                hit_lower=None,
                sample_count=len(sorted_samples),
                quote_clean_sample_count=quote_clean_count,
                no_route_seen=no_route_seen,
                trapped_seen=trapped_seen,
                data_quality="quote_clean",
            )

    return TripleBarrierLabel(
        label="TIMEOUT",
        terminal_reason="time_barrier_hit",
        terminal_ts=horizon_ts,
        time_to_terminal_sec=config.horizon_sec,
        max_pnl_pct=max_pnl,
        min_pnl_pct=min_pnl,
        hit_upper=None,
        hit_lower=None,
        sample_count=len(sorted_samples),
        quote_clean_sample_count=quote_clean_count,
        no_route_seen=no_route_seen,
        trapped_seen=trapped_seen,
        data_quality="quote_clean" if quote_clean_count else "dirty_or_missing_quote",
    )
