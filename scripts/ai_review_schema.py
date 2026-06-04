"""Shared schema helpers for advisory-only AI strategy reviews."""

from __future__ import annotations

from typing import Any


AI_REVIEW_SCHEMA_VERSION = "v1.ai_strategy_advisory.shadow_only"
ALLOWED_EFFECT_ADVISORY_ONLY = "advisory_only"
ALLOWED_EFFECT_SCORE_BOOST_ONLY = "score_boost_only"
MAX_DEFAULT_SCORE_BOOST = 10


def bounded_score(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    return max(0.0, min(1.0, number))


def bounded_boost(value: Any, max_boost: int = MAX_DEFAULT_SCORE_BOOST) -> int:
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        number = 0
    return max(0, min(int(max_boost), number))


def ai_review_envelope(
    *,
    reviewer: str,
    ai_score: float,
    ai_grade: str,
    reason: str,
    risk_notes: list[str] | None = None,
    allowed_effect: str = ALLOWED_EFFECT_ADVISORY_ONLY,
    score_boost_suggested: int = 0,
    extra: dict | None = None,
) -> dict:
    review = {
        "schema_version": AI_REVIEW_SCHEMA_VERSION,
        "reviewer": reviewer,
        "ai_score": round(bounded_score(ai_score), 6),
        "ai_grade": ai_grade,
        "reason": reason,
        "risk_notes": list(risk_notes or []),
        "allowed_effect": allowed_effect,
        "score_boost_suggested": bounded_boost(score_boost_suggested),
        "can_trigger_trade": False,
        "can_override_hard_gate": False,
        "advisory_only": True,
    }
    if extra:
        review.update(extra)
    return review
