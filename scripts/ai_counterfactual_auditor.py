"""Counterfactual denominator auditor for A_CLASS promotion safety."""

from __future__ import annotations

from ai_review_schema import ai_review_envelope


def audit_counterfactual_denominator(p0_discovery: dict | None) -> dict:
    p0 = p0_discovery or {}
    seen = int(p0.get("quote_clean_gold_silver_seen_count") or 0)
    would_enter = int(p0.get("quote_clean_gold_silver_would_enter_count") or 0)
    rr = p0.get("outlier_trimmed_would_rr")
    no_route_rate = float(p0.get("would_enter_no_route_rate") or 0.0)
    trapped_rate = float(p0.get("would_enter_trapped_rate") or 0.0)
    unknown_rate = float(p0.get("unknown_data_rate") or 0.0)
    blockers = []
    if seen < 8:
        blockers.append("quote_clean_gold_silver_seen_below_min")
    if would_enter < 5:
        blockers.append("quote_clean_gold_silver_would_enter_below_min")
    if rr is None or float(rr) < 2.0:
        blockers.append("outlier_trimmed_would_rr_below_2")
    if no_route_rate > 0.10:
        blockers.append("would_enter_no_route_rate_above_10pct")
    if trapped_rate > 0.10:
        blockers.append("would_enter_trapped_rate_above_10pct")
    if unknown_rate > 0.05:
        blockers.append("unknown_data_rate_above_5pct")

    pass_audit = not blockers
    return ai_review_envelope(
        reviewer="AI_COUNTERFACTUAL_AUDITOR_LOCAL_SHADOW",
        ai_score=0.82 if pass_audit else 0.38,
        ai_grade="promotion_evidence_ok" if pass_audit else "shadow_continue",
        reason="Audits denominator, outlier-adjusted RR, no-route, trapped, and unknown-data rates.",
        risk_notes=blockers,
        extra={
            "pass": pass_audit,
            "blockers": blockers,
            "candidate_count": seen,
            "would_enter_count": would_enter,
            "outlier_trimmed_would_rr": rr,
            "would_enter_no_route_rate": no_route_rate,
            "would_enter_trapped_rate": trapped_rate,
            "unknown_data_rate": unknown_rate,
        },
    )
