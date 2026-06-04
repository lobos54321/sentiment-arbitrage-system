"""Advisory missed-dog blocker reviewer."""

from __future__ import annotations

from typing import Any

from ai_review_schema import ai_review_envelope


HARD_SECURITY_TOKENS = {
    "rug",
    "security",
    "honeypot",
    "blacklist",
    "creator",
    "bundler",
    "rat",
    "entrapment",
    "no_route",
    "trapped",
    "quote_not_executable",
    "liquidity_unknown",
}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def review_missed_dog_blockers(p0_discovery: dict | None) -> dict:
    p0 = p0_discovery or {}
    blockers = p0.get("missed_blockers") or []
    rows = []
    for row in blockers:
        reason = str(row.get("reject_reason") or row.get("blocker") or "unknown")
        reason_l = reason.lower()
        hard_security = any(token in reason_l for token in HARD_SECURITY_TOKENS)
        gold_n = int(row.get("gold_n") or 0)
        silver_n = int(row.get("silver_n") or 0)
        unique_tokens = int(row.get("unique_tokens") or 0)
        max_peak = _safe_float(row.get("max_adjusted_peak"), 0.0) or 0.0
        if hard_security:
            recommendation = "keep_hard_block"
        elif gold_n >= 1 or silver_n >= 3:
            recommendation = "allow_a_class_only"
        elif unique_tokens >= 5 and max_peak >= 0.50:
            recommendation = "investigate_data_quality"
        else:
            recommendation = "no_action"
        rows.append({
            "route": row.get("route"),
            "component": row.get("component"),
            "reject_reason": reason,
            "unique_tokens": unique_tokens,
            "gold_n": gold_n,
            "silver_n": silver_n,
            "max_adjusted_peak": row.get("max_adjusted_peak"),
            "hard_security_blocker": hard_security,
            "recommendation": recommendation,
        })
    rows.sort(key=lambda item: (item["recommendation"] == "allow_a_class_only", item["gold_n"], item["silver_n"], item["unique_tokens"]), reverse=True)
    score = 0.75 if any(row["recommendation"] == "allow_a_class_only" for row in rows) else 0.45
    return ai_review_envelope(
        reviewer="AI_MISSED_DOG_REVIEWER_LOCAL_SHADOW",
        ai_score=score,
        ai_grade="actionable" if score >= 0.70 else "observe",
        reason="Ranks missed dog blockers without downgrading hard security gates.",
        risk_notes=[],
        extra={
            "recommendations": rows[:50],
            "allow_a_class_only_count": sum(1 for row in rows if row["recommendation"] == "allow_a_class_only"),
            "keep_hard_block_count": sum(1 for row in rows if row["recommendation"] == "keep_hard_block"),
        },
    )
