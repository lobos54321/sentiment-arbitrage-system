"""Advisory-only candidate reviewer.

This is a deterministic local reviewer with an AI-compatible schema. It keeps
the first rollout independent from external model availability while preserving
the boundary that AI cannot trade or bypass hard gates.
"""

from __future__ import annotations

from typing import Any

from ai_review_schema import ALLOWED_EFFECT_SCORE_BOOST_ONLY, ai_review_envelope


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
        return float(value)
    except (TypeError, ValueError):
        return default


def review_a_class_candidate(candidate: Any, matrix_detail: dict | None, rr_detail: dict | None) -> dict:
    matrix_detail = matrix_detail or {}
    rr_detail = rr_detail or {}
    score = 0.20
    reasons = []
    risk_notes = []

    if _truthy(_get(candidate, "source_resonance", False)):
        score += 0.18
        reasons.append("source resonance is present")
    if _truthy(_get(candidate, "gmgn_pre_seen", False)) or _truthy(_get(candidate, "gmgn_activity_fresh", False)):
        score += 0.14
        reasons.append("GMGN evidence is present")
    if _truthy(_get(candidate, "fresh_momentum", False)):
        score += 0.14
        reasons.append("fresh momentum supports timing")
    if _truthy(_get(candidate, "fresh_reclaim", False)) or _truthy(_get(candidate, "fresh_ath_refresh", False)):
        score += 0.12
        reasons.append("fresh reclaim or ATH refresh renews opportunity")
    if (rr_detail.get("expected_rr") or 0) >= 2.0:
        score += 0.14
        reasons.append("expected RR clears 2:1")
    if matrix_detail.get("red_count", 0) == 0 and matrix_detail.get("green_count", 0) >= 3:
        score += 0.12
        reasons.append("matrix has no red cells and enough green evidence")

    liquidity = _safe_float(_get(candidate, "liquidity_usd"), None)
    spread = _safe_float(_get(candidate, "spread_pct"), None)
    if liquidity is not None and liquidity < 25_000:
        risk_notes.append("thin liquidity")
        score -= 0.05
    if spread is not None and spread > 2.0:
        risk_notes.append("spread needs fast stop discipline")
        score -= 0.05
    if matrix_detail.get("red_count", 0):
        risk_notes.append("matrix red cells prevent live influence")
        score = min(score, 0.40)
    if rr_detail.get("hard_blockers"):
        risk_notes.extend(str(item) for item in rr_detail.get("hard_blockers") or [])
        score = min(score, 0.45)

    score = max(0.0, min(1.0, score))
    if score >= 0.75:
        grade = "supportive"
        boost = 8
    elif score >= 0.55:
        grade = "cautious_support"
        boost = 4
    elif score >= 0.40:
        grade = "neutral"
        boost = 0
    else:
        grade = "oppose"
        boost = 0

    narrative_strength = "high" if score >= 0.75 else ("medium" if score >= 0.55 else "low")
    return ai_review_envelope(
        reviewer="AI_CANDIDATE_RATER_LOCAL_SHADOW",
        ai_score=score,
        ai_grade=grade,
        reason="; ".join(reasons) if reasons else "insufficient semantic evidence for A_CLASS support",
        risk_notes=risk_notes,
        allowed_effect=ALLOWED_EFFECT_SCORE_BOOST_ONLY,
        score_boost_suggested=boost,
        extra={
            "narrative_strength": narrative_strength,
            "matrix_grade": matrix_detail.get("matrix_grade"),
            "expected_rr": rr_detail.get("expected_rr"),
        },
    )
