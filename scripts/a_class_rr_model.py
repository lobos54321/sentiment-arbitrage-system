"""Per-candidate A_CLASS risk/reward and bottom-ticket model."""

from __future__ import annotations

from typing import Any

from fastlane_config import load_a_class_config


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "tradable"}
    return bool(value)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number


def _payload(candidate: Any) -> dict:
    payload = _get(candidate, "raw_payload", {}) or {}
    return payload if isinstance(payload, dict) else {}


def defined_risk_pct(config: Any = None) -> float:
    config = config or load_a_class_config()
    fast_stop = abs(_safe_float(_get(config, "fast_stop_loss_pct"), -0.15) or -0.15)
    hard_cap = abs(_safe_float(_get(config, "hard_loss_cap_pct"), 0.20) or 0.20)
    # Use hard cap as the denominator so a 50% silver-dog path must clear 2:1.
    return max(0.01, min(0.20, max(fast_stop, hard_cap)))


def _explicit_upside(candidate: Any) -> tuple[float | None, str | None]:
    payload = _payload(candidate)
    keys = (
        "expected_upside_pct",
        "executable_peak_pnl",
        "quote_clean_peak_pnl",
        "tradable_peak_pnl",
        "max_pnl_recorded",
        "peak_quote_pnl_pct",
        "quote_peak_pnl",
        "trusted_peak_pnl",
        "pnl_60m",
        "pnl_15m",
        "pnl_5m",
    )
    for key in keys:
        value = _safe_float(_get(candidate, key), None)
        if value is None:
            value = _safe_float(payload.get(key), None)
        if value is not None and value > 0:
            # Missed-attribution peaks get an execution haircut before RR.
            adjusted = max(0.0, value * 0.80 - 0.05)
            return adjusted, f"explicit_{key}_haircut"
    return None, None


def _inferred_upside(candidate: Any, matrix_detail: dict | None) -> tuple[float, str]:
    matrix_detail = matrix_detail or {}
    grade = str(matrix_detail.get("matrix_grade") or "").upper()
    if grade == "A_PLUS":
        base = 1.00
    elif grade == "STRONG_A":
        base = 0.70
    elif grade == "A":
        base = 0.50
    else:
        base = 0.35
    route = str(_get(candidate, "route_bucket", "") or "").upper()
    if route == "LOTTO":
        base += 0.20
    elif route in {"ATH", "RECLAIM", "A_GRADE", "A_GRADE_RESONANCE_FASTLANE"}:
        base += 0.10
    if _truthy(_get(candidate, "source_resonance", False)):
        base += 0.10
    if _truthy(_get(candidate, "fresh_momentum", False)):
        base += 0.10
    if _truthy(_get(candidate, "missed_dog_cohort_strong", False)):
        base += 0.20
    return min(base, 3.00), "inferred_from_matrix_source_flow"


def _rr_grade(expected_rr: float | None) -> str:
    if expected_rr is None:
        return "REJECT"
    if expected_rr >= 5.0:
        return "A_PLUS"
    if expected_rr >= 3.0:
        return "STRONG_A"
    if expected_rr >= 2.0:
        return "A"
    return "REJECT"


def _ticket_size(rr_grade: str, config: Any) -> float:
    if rr_grade == "A_PLUS":
        return min(config.size_a_plus_sol, config.max_size_sol)
    if rr_grade == "STRONG_A":
        return min(config.size_strong_a_sol, config.max_size_sol)
    if rr_grade == "A":
        return min(config.size_a_sol, config.max_size_sol)
    return 0.0


def principal_recovery_plan(config: Any = None) -> dict:
    config = config or load_a_class_config()
    return {
        "plan_version": "v1.a_class_bottom_ticket",
        "positive_feedback_peak_pct": config.breakeven_peak_pct,
        "protect_principal_peak_pct": 0.50,
        "recover_principal_peak_pct": config.recover_principal_peak_pct,
        "paper_partial_note": "Paper can simulate partials; live must check min executable partial before selling tiny tickets.",
        "no_averaging_down": True,
    }


def moonbag_plan(config: Any = None) -> dict:
    config = config or load_a_class_config()
    return {
        "plan_version": "v1.a_class_moonbag",
        "breakeven_peak_pct": config.breakeven_peak_pct,
        "recover_principal_peak_pct": config.recover_principal_peak_pct,
        "moonbag_peak_pct": config.moonbag_peak_pct,
        "keep_tail_after_moonbag": True,
        "full_exit_allowed_on_hard_failure": True,
    }


def build_a_class_rr_model(candidate: Any, matrix_detail: dict | None = None, *, config: Any = None) -> dict:
    """Build one candidate's deterministic RR contract.

    The output is advisory until the live tiny-probe rollout is explicitly
    enabled. It still provides hard blockers for controller decisions.
    """
    config = config or load_a_class_config()
    risk = defined_risk_pct(config)
    expected_upside, source = _explicit_upside(candidate)
    if expected_upside is None:
        expected_upside, source = _inferred_upside(candidate, matrix_detail)
    expected_rr = expected_upside / risk if risk > 0 else None
    grade = _rr_grade(expected_rr)
    blockers = []
    if expected_rr is None or expected_rr < 2.0:
        blockers.append("expected_rr_below_2")
    if risk > 0.20:
        blockers.append("defined_loss_risk_above_20pct")
    ticket = _ticket_size(grade, config)
    return {
        "rr_version": "v1.a_class_2_to_1_bottom_ticket",
        "expected_upside_pct": round(expected_upside, 6),
        "expected_upside_source": source,
        "defined_risk_pct": round(risk, 6),
        "expected_rr": round(expected_rr, 6) if expected_rr is not None else None,
        "rr_grade": grade,
        "bottom_ticket_size_sol": round(ticket, 6),
        "principal_recovery_plan": principal_recovery_plan(config),
        "moonbag_plan": moonbag_plan(config),
        "live_allowed_by_rr": not blockers,
        "hard_blockers": blockers,
        "advisory_only": True,
    }
