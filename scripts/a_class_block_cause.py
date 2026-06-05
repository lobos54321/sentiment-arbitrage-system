"""Shared A_CLASS block-cause classification.

Write paths should persist this classification once. Read paths may use this
module only as a legacy fallback for rows created before the persisted columns
existed.
"""

from __future__ import annotations

import json
import re
from typing import Any


VALID_CATEGORIES = {"INFRA", "MARKET", "POLICY", "UNKNOWN"}


def json_loads(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available", "executable", "pass"}
    return bool(value)


def norm(value: Any) -> str:
    return str(value or "").strip().lower()


def compact_text(*values: Any) -> str:
    return " ".join(str(v).lower() for v in values if v is not None and str(v).strip())


def blocker_list(value: Any) -> list[str]:
    parsed = value if isinstance(value, list) else json_loads(value, [])
    if isinstance(parsed, list):
        return [str(x).strip() for x in parsed if str(x or "").strip()]
    if parsed:
        return [str(parsed).strip()]
    return []


def infer_blockers(row: dict[str, Any]) -> list[str]:
    blockers = blocker_list(row.get("hard_blockers_json") or row.get("hard_blockers"))
    seen = {norm(x) for x in blockers}
    action = norm(row.get("action")).upper()
    blockedish = action in {"BLOCK", "SHADOW"} or "BLOCK" in action

    def push(blocker: str) -> None:
        if blocker not in seen:
            blockers.append(blocker)
            seen.add(blocker)

    if "quote_available" in row and row.get("quote_available") is not None and not truthy(row.get("quote_available")):
        push("quote_not_available")
    if "quote_executable" in row and row.get("quote_executable") is not None and not truthy(row.get("quote_executable")):
        push("quote_not_executable")
    if "route_available" in row and row.get("route_available") is not None and not truthy(row.get("route_available")):
        push("route_unavailable")
    if blockedish:
        if "liquidity_usd" in row and row.get("liquidity_usd") is None:
            push("liquidity_unknown")
        if "spread_pct" in row and row.get("spread_pct") is None:
            push("spread_unknown")
    return blockers


def classify_blocker(blocker: str, row: dict[str, Any]) -> dict[str, Any]:
    b = norm(blocker)
    risk = json_loads(row.get("risk_json"), {})
    candidate = json_loads(row.get("candidate_json"), {})
    data_confidence = norm(row.get("data_confidence") or risk.get("data_confidence") or candidate.get("data_confidence"))
    quote_source = norm(row.get("quote_source") or risk.get("quote_source") or candidate.get("quote_source"))
    evidence = compact_text(
        row.get("route_failure_reason"),
        row.get("quote_failure_reason"),
        row.get("provider_reason"),
        row.get("evidence_status"),
        row.get("reason"),
        row.get("source_reason"),
        risk.get("route_failure_reason"),
        risk.get("quote_failure_reason"),
        risk.get("provider_reason"),
        risk.get("evidence_status"),
        candidate.get("route_failure_reason"),
        candidate.get("quote_failure_reason"),
        candidate.get("provider_reason"),
        candidate.get("evidence_status"),
    )
    market_route_failure = re.search(r"\b(no[_ -]?route|trapped|token[_ -]?not[_ -]?tradable|not tradable|route[_ -]?failure[_ -]?red|honeypot|rug)\b", evidence)
    infra_context = (
        not evidence
        or data_confidence in {"unknown", "partial", "quote_only"}
        or not quote_source
        or re.search(r"\b(rate[_ -]?limited|429|timeout|provider|missing|unknown|stale|quote[_ -]?failed|unavailable)\b", evidence)
    )

    if not b:
        return {"blocker": blocker, "category": "UNKNOWN", "recoverability": "unknown", "reason": "empty_blocker"}
    if re.search(r"(creator[_ -]?(close|dump)|rug|security|honeypot|bundler|rat[_ -]?trader|entrapment|top10|mint[_ -]?authority|freeze[_ -]?authority)", b):
        return {"blocker": blocker, "category": "MARKET", "recoverability": "exclude_from_clean_denominator", "reason": "hard_security_or_structure_red_flag"}
    if re.search(r"(liquidity[_ -]?(below|min|too[_ -]?low)|spread[_ -]?(extreme|too[_ -]?high)|route[_ -]?failure[_ -]?red|trapped|token[_ -]?not[_ -]?tradable|no[_ -]?route)", b):
        return {"blocker": blocker, "category": "MARKET", "recoverability": "exclude_from_clean_denominator", "reason": "market_execution_or_liquidity_red_flag"}
    if "route_unavailable" in b:
        if market_route_failure:
            return {"blocker": blocker, "category": "MARKET", "recoverability": "exclude_from_clean_denominator", "reason": "route_unavailable_confirmed_by_no_route_or_trapped_reason"}
        return {
            "blocker": blocker,
            "category": "INFRA" if infra_context else "MARKET",
            "recoverability": "provider_or_evidence_recoverable" if infra_context else "exclude_from_clean_denominator",
            "reason": "route_unavailable_without_market_failure_evidence" if infra_context else "route_unavailable_with_market_context",
        }
    if "quote_not_executable" in b:
        if market_route_failure:
            return {"blocker": blocker, "category": "MARKET", "recoverability": "exclude_from_clean_denominator", "reason": "quote_not_executable_confirmed_by_market_route_failure"}
        return {"blocker": blocker, "category": "INFRA", "recoverability": "provider_or_evidence_recoverable", "reason": "quote_not_executable_without_market_failure_evidence"}
    if re.search(r"\b(quote[_ -]?(not[_ -]?available|source[_ -]?missing|age[_ -]?unknown|stale|missing|unknown|failed)|liquidity[_ -]?unknown|spread[_ -]?unknown|unknown[_ -]?data|data[_ -]?unknown|provider[_ -]?(missing|failed|rate[_ -]?limited)|rate[_ -]?limited|429)\b", b):
        return {"blocker": blocker, "category": "INFRA", "recoverability": "provider_or_evidence_recoverable", "reason": "provider_or_evidence_missing"}
    if re.search(r"(expected[_ -]?rr|defined[_ -]?loss|loss[_ -]?risk|cooldown|budget|circuit|mode[_ -]?(disabled|shadow|down)|max[_ -]?concurrent|duplicate|prior[_ -]?(exposure|fastlane)|already[_ -]?fastlane|counterfactual[_ -]?only|watch[_ -]?only|shadow[_ -]?only|entry[_ -]?mode[_ -]?quality|matrix|matrices|scout[_ -]?quality|buy[_ -]?pressure|volume[_ -]?low|negative[_ -]?trend|momentum)", b):
        return {"blocker": blocker, "category": "POLICY", "recoverability": "policy_or_strategy_review", "reason": "strategy_or_budget_guardrail"}
    return {"blocker": blocker, "category": "UNKNOWN", "recoverability": "needs_review", "reason": "unmapped_blocker"}


def recoverability_for_category(category: str) -> str:
    category = str(category or "UNKNOWN").upper()
    if category == "INFRA":
        return "provider_or_evidence_recoverable"
    if category == "MARKET":
        return "exclude_from_clean_denominator"
    if category == "POLICY":
        return "policy_or_strategy_review"
    return "needs_review"


def classify_event(row: dict[str, Any]) -> dict[str, Any]:
    persisted = persisted_block_cause_payload(row)
    if persisted:
        return persisted
    blockers = infer_blockers(row)
    classified = [classify_blocker(blocker, row) for blocker in blockers]
    categories = {item["category"] for item in classified}
    if "MARKET" in categories:
        category = "MARKET"
    elif "POLICY" in categories:
        category = "POLICY"
    elif "INFRA" in categories:
        category = "INFRA"
    else:
        category = "UNKNOWN"
    action = norm(row.get("action")).upper()
    would_action = norm(row.get("would_action")).upper()
    reason = next((item.get("reason") for item in classified if item.get("category") == category), None)
    return {
        "category": category,
        "recoverability": recoverability_for_category(category),
        "classification_reason": reason or "no_mapped_blocker",
        "blocked": bool(classified) or action in {"BLOCK", "SHADOW"} or "BLOCK" in action,
        "would_enter_a_class": action == "WOULD_ENTER" or would_action == "WOULD_ENTER" or truthy(row.get("would_enter_a_class")),
        "did_enter": action == "ENTER" or truthy(row.get("did_enter")),
        "blockers": blockers,
        "blocker_classifications": classified,
    }


def persisted_block_cause_payload(row: dict[str, Any]) -> dict[str, Any] | None:
    category = str(row.get("block_cause") or row.get("block_category") or "").strip().upper()
    if category not in VALID_CATEGORIES:
        return None
    raw_classifications = row.get("blocker_classifications_json")
    classifications = json_loads(raw_classifications, []) if raw_classifications else []
    if not isinstance(classifications, list):
        classifications = []
    blockers = row.get("blockers")
    if blockers is None:
        blockers = [str(item.get("blocker")) for item in classifications if isinstance(item, dict) and item.get("blocker")]
    action = norm(row.get("action")).upper()
    would_action = norm(row.get("would_action")).upper()
    return {
        "category": category,
        "recoverability": row.get("recoverability") or recoverability_for_category(category),
        "classification_reason": row.get("classification_reason") or "persisted",
        "blocked": bool(blockers) or action in {"BLOCK", "SHADOW"} or "BLOCK" in action,
        "would_enter_a_class": action == "WOULD_ENTER" or would_action == "WOULD_ENTER" or truthy(row.get("would_enter_a_class")),
        "did_enter": action == "ENTER" or truthy(row.get("did_enter")),
        "blockers": blockers,
        "blocker_classifications": classifications,
    }
