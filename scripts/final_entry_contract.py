"""Final hard-gate contract shared by live/paper entry paths.

The contract is deterministic and deliberately stricter than strategy scoring:
scores, AI reviews, and soft signals can only run after this layer says the
candidate is executable and within risk limits.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import time
from typing import Any

from executable_sol_valuation import executable_sol_valuation
from fastlane_config import load_a_class_config


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    number = _safe_float(value, None)
    return default if number is None else int(number)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available", "live"}
    return bool(value)


def _risk_flags(candidate: Any) -> list[str]:
    flags = _get(candidate, "risk_flags", []) or []
    if isinstance(flags, str):
        return [flags]
    try:
        return [str(flag) for flag in flags]
    except TypeError:
        return []


@dataclass(frozen=True)
class FinalEntryDecision:
    decision: str
    reason: str
    hard_blockers: list[str] = field(default_factory=list)
    soft_notes: list[str] = field(default_factory=list)
    normalized_mode: str | None = None
    route_bucket: str | None = None
    expected_rr: float | None = None
    defined_risk_pct: float | None = None
    quote_detail: dict = field(default_factory=dict)
    liquidity_usd: float | None = None
    spread_pct: float | None = None
    mode_state: dict = field(default_factory=dict)
    budget_state: dict = field(default_factory=dict)
    evaluated_at: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def passed(self) -> bool:
        return self.decision == "PASS"


def evaluate_final_entry_contract(
    candidate: Any,
    *,
    mode_state: dict | None = None,
    budget_state: dict | None = None,
    config: Any = None,
    now_ts: float | None = None,
) -> FinalEntryDecision:
    """Evaluate the global final-entry hard gates.

    This function is side-effect free.  Callers can attach the returned
    dictionary to decision_events/ledger rows before they wire it into execution.
    """
    config = config or load_a_class_config({})
    mode_state = mode_state or {}
    budget_state = budget_state or {}
    now_ts = float(now_ts if now_ts is not None else time.time())
    blockers: list[str] = []
    notes: list[str] = []

    route = str(_get(candidate, "route_bucket", _get(candidate, "route", "ATH")) or "ATH").upper()
    normalized_mode = _get(candidate, "normalized_mode") or _get(candidate, "entry_mode")

    quote = _get(candidate, "quote", None) or candidate
    quote_detail = executable_sol_valuation(
        quote,
        now_ts=now_ts,
        max_quote_age_sec=_safe_float(_get(config, "quote_max_age_sec"), 10.0) or 10.0,
    )
    blockers.extend(quote_detail.hard_blockers)

    liquidity_usd = _safe_float(_get(candidate, "liquidity_usd"), None)
    if liquidity_usd is None:
        blockers.append("liquidity_unknown")
    elif liquidity_usd < config.min_liquidity_for_route(route):
        blockers.append("liquidity_below_min")

    spread_pct = _safe_float(_get(candidate, "spread_pct", _get(candidate, "quote_spread_pct")), None)
    if spread_pct is None:
        notes.append("spread_unknown")
    elif spread_pct > config.extreme_spread_block_pct:
        blockers.append("spread_extreme")
    elif spread_pct > config.max_spread_for_route(route):
        blockers.append("spread_above_route_limit")

    flags = {flag.lower() for flag in _risk_flags(candidate)}
    if flags.intersection({"obvious_rug", "rug", "security_red", "honeypot", "creator_dump", "creator_close"}):
        blockers.append("security_red_flag")
    for key, blocker in (
        ("creator_close", "creator_close"),
        ("creator_dump", "creator_dump"),
        ("recent_hard_loss", "recent_hard_loss"),
        ("recent_trapped_or_no_route", "recent_trapped_or_no_route"),
        ("prior_fastlane_in_lifecycle", "prior_fastlane_in_lifecycle"),
        ("prior_exposure_in_lifecycle", "prior_exposure_in_lifecycle"),
    ):
        if _truthy(_get(candidate, key, False)):
            blockers.append(blocker)

    top10 = _safe_float(_get(candidate, "top10_pct"), None)
    if top10 is not None and top10 > config.top10_hard_max_pct:
        blockers.append("top10_hard_red")
    bundler = _safe_float(_get(candidate, "bundler_rate"), None)
    if bundler is not None and bundler > config.bundler_hard_max:
        blockers.append("bundler_hard_red")
    rat = _safe_float(_get(candidate, "rat_trader_rate"), None)
    if rat is not None and rat > config.rat_trader_hard_max:
        blockers.append("rat_trader_hard_red")
    entrapment = _safe_float(_get(candidate, "entrapment_ratio"), None)
    if entrapment is not None and entrapment > config.entrapment_hard_max:
        blockers.append("entrapment_hard_red")

    expected_rr = _safe_float(_get(candidate, "expected_rr"), None)
    if expected_rr is not None and expected_rr < 2.0:
        blockers.append("expected_rr_below_2")
    elif expected_rr is None:
        notes.append("expected_rr_missing")

    defined_risk_pct = _safe_float(_get(candidate, "defined_risk_pct"), None)
    if defined_risk_pct is not None and defined_risk_pct > 0.20:
        blockers.append("defined_loss_risk_above_20pct")
    elif defined_risk_pct is None:
        notes.append("defined_risk_missing")

    status = str(mode_state.get("status") or mode_state.get("action") or "LIVE").upper()
    if status in {"DISABLED", "BLOCK", "BLOCKED", "CIRCUIT_BROKEN"}:
        blockers.append("mode_disabled")
    if _truthy(mode_state.get("circuit_broken", False)):
        blockers.append("mode_circuit_breaker")

    active_count = _safe_int(budget_state.get("active_count"), 0)
    max_concurrent = _safe_int(budget_state.get("max_concurrent"), config.max_concurrent)
    if active_count >= max_concurrent:
        blockers.append("max_concurrent_reached")
    if _truthy(budget_state.get("daily_loss_budget_hit", False)):
        blockers.append("daily_loss_budget_hit")
    if _truthy(budget_state.get("mode_loss_budget_hit", False)):
        blockers.append("mode_loss_budget_hit")

    unique_blockers = sorted(set(blockers))
    return FinalEntryDecision(
        decision="BLOCK" if unique_blockers else "PASS",
        reason="final_entry_hard_block" if unique_blockers else "final_entry_contract_pass",
        hard_blockers=unique_blockers,
        soft_notes=sorted(set(notes)),
        normalized_mode=str(normalized_mode) if normalized_mode else None,
        route_bucket=route,
        expected_rr=expected_rr,
        defined_risk_pct=defined_risk_pct,
        quote_detail=quote_detail.to_dict(),
        liquidity_usd=liquidity_usd,
        spread_pct=spread_pct,
        mode_state=dict(mode_state),
        budget_state=dict(budget_state),
        evaluated_at=now_ts,
    )
