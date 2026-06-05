#!/usr/bin/env python3
"""Classify A_CLASS blocks into infra, market, and policy causes.

This is a read-only diagnostic.  It answers whether BLOCK rows are mostly
provider/evidence recoverable, genuinely unexecutable market cases, or policy
guardrails.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "v1.a_class_block_cause_breakdown"


def _json_loads(value: Any, fallback: Any) -> Any:
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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available", "executable", "pass"}
    return bool(value)


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _compact_text(*values: Any) -> str:
    return " ".join(str(v).lower() for v in values if v is not None and str(v).strip())


def _blocker_list(value: Any) -> list[str]:
    parsed = value if isinstance(value, list) else _json_loads(value, [])
    if isinstance(parsed, list):
        return [str(x).strip() for x in parsed if str(x or "").strip()]
    if parsed:
        return [str(parsed).strip()]
    return []


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)).fetchone() is not None


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {row["name"] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _optional(cols: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in cols else f"{fallback} AS {name}"


def _expr(cols: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in cols else fallback


def infer_blockers(row: dict[str, Any]) -> list[str]:
    blockers = _blocker_list(row.get("hard_blockers_json") or row.get("hard_blockers"))
    seen = {_norm(x) for x in blockers}

    def push(blocker: str) -> None:
        if blocker not in seen:
            blockers.append(blocker)
            seen.add(blocker)

    if "quote_available" in row and row.get("quote_available") is not None and not _truthy(row.get("quote_available")):
        push("quote_not_available")
    if "quote_executable" in row and row.get("quote_executable") is not None and not _truthy(row.get("quote_executable")):
        push("quote_not_executable")
    if "route_available" in row and row.get("route_available") is not None and not _truthy(row.get("route_available")):
        push("route_unavailable")
    if "liquidity_usd" in row and row.get("liquidity_usd") is None:
        push("liquidity_unknown")
    if "spread_pct" in row and row.get("spread_pct") is None:
        push("spread_unknown")
    return blockers


def classify_blocker(blocker: str, row: dict[str, Any]) -> dict[str, Any]:
    b = _norm(blocker)
    risk = _json_loads(row.get("risk_json"), {})
    candidate = _json_loads(row.get("candidate_json"), {})
    data_confidence = _norm(row.get("data_confidence") or risk.get("data_confidence") or candidate.get("data_confidence"))
    quote_source = _norm(row.get("quote_source") or risk.get("quote_source") or candidate.get("quote_source"))
    evidence = _compact_text(
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


def classify_event(row: dict[str, Any]) -> dict[str, Any]:
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
    action = _norm(row.get("action")).upper()
    would_action = _norm(row.get("would_action")).upper()
    return {
        "category": category,
        "blocked": bool(classified) or action in {"BLOCK", "SHADOW"} or "BLOCK" in action,
        "would_enter_a_class": action == "WOULD_ENTER" or would_action == "WOULD_ENTER" or _truthy(row.get("would_enter_a_class")),
        "did_enter": action == "ENTER" or _truthy(row.get("did_enter")),
        "blockers": blockers,
        "blocker_classifications": classified,
    }


def fetch_rows(db: sqlite3.Connection, since_ts: float | None, source: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    issues: list[str] = []
    params = {} if since_ts is None else {"since_ts": since_ts}
    where = "" if since_ts is None else "WHERE event_ts >= :since_ts"
    if source in {"all", "a_class", "a_class_decision_events"}:
        if _table_exists(db, "a_class_decision_events"):
            cols = _columns(db, "a_class_decision_events")
            sql = f"""
            SELECT id, 'a_class_decision_events' AS source_kind, event_ts, token_ca, symbol,
                   lifecycle_id, route_bucket, source_table, source_component, source_reason,
                   action, {_optional(cols, "would_action")}, reason, hard_blockers_json,
                   {_optional(cols, "risk_json")}, {_optional(cols, "candidate_json")},
                   {_optional(cols, "denominator_key")}, {_optional(cols, "expected_rr")},
                   {_optional(cols, "score")}, {_optional(cols, "grade")}, {_optional(cols, "size_sol")}
            FROM a_class_decision_events
            {where}
            ORDER BY event_ts DESC, id DESC
            """
            rows.extend(dict(r) for r in db.execute(sql, params).fetchall())
        else:
            issues.append("a_class_decision_events_missing")
    if source in {"all", "opportunity", "opportunity_events"}:
        if _table_exists(db, "opportunity_events"):
            cols = _columns(db, "opportunity_events")
            sql = f"""
            SELECT id, 'opportunity_events' AS source_kind, event_ts, token_ca, symbol,
                   lifecycle_id, route_bucket, source_type AS source_table,
                   source_component, source_reason,
                   CASE WHEN COALESCE({_expr(cols, "did_enter", "0")}, 0) = 1 THEN 'ENTER'
                        WHEN COALESCE({_expr(cols, "would_enter_a_class", "0")}, 0) = 1 THEN 'WOULD_ENTER'
                        ELSE 'BLOCK' END AS action,
                   NULL AS would_action,
                   {_expr(cols, "quote_failure_reason", "NULL")} AS reason,
                   hard_blockers_json, NULL AS risk_json, raw_payload_json AS candidate_json,
                   NULL AS denominator_key, expected_rr, matrix_score AS score, NULL AS grade, NULL AS size_sol,
                   {_optional(cols, "quote_available")}, {_optional(cols, "quote_executable")},
                   {_optional(cols, "quote_clean")}, {_optional(cols, "route_available")},
                   {_optional(cols, "quote_source")}, {_optional(cols, "quote_age_sec")},
                   {_optional(cols, "data_confidence")}, {_optional(cols, "provider_data_state")},
                   {_optional(cols, "provider_reason")}, {_optional(cols, "evidence_status")},
                   {_optional(cols, "quote_failure_reason")}, {_optional(cols, "liquidity_usd")},
                   {_optional(cols, "spread_pct")}, {_optional(cols, "would_enter_a_class", "0")},
                   {_optional(cols, "did_enter", "0")}
            FROM opportunity_events
            {where}
            ORDER BY event_ts DESC, id DESC
            """
            rows.extend(dict(r) for r in db.execute(sql, params).fetchall())
        else:
            issues.append("opportunity_events_missing")
    rows.sort(key=lambda r: (float(r.get("event_ts") or 0), int(r.get("id") or 0)), reverse=True)
    return rows, issues


def _new_group(extra: dict[str, Any]) -> dict[str, Any]:
    return {**extra, "n": 0, "blocked_n": 0, "unique_tokens": 0, "would_enter_n": 0, "did_enter_n": 0, "latest_event_ts": None, "_tokens": set()}


def _touch(group: dict[str, Any], row: dict[str, Any], cls: dict[str, Any]) -> None:
    group["n"] += 1
    if cls["blocked"]:
        group["blocked_n"] += 1
    if cls["would_enter_a_class"]:
        group["would_enter_n"] += 1
    if cls["did_enter"]:
        group["did_enter_n"] += 1
    if row.get("token_ca"):
        group["_tokens"].add(row["token_ca"])
    if row.get("event_ts") is not None:
        group["latest_event_ts"] = max(float(group["latest_event_ts"] or 0), float(row["event_ts"] or 0))


def _finish(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for group in groups.values():
        tokens = group.pop("_tokens", set())
        group["unique_tokens"] = len(tokens)
        group["latest_event_iso"] = (
            datetime.fromtimestamp(group["latest_event_ts"], tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if group.get("latest_event_ts") else None
        )
        out.append(group)
    return sorted(out, key=lambda r: (r.get("n", 0), r.get("latest_event_ts") or 0), reverse=True)


def build_breakdown(db: sqlite3.Connection, since_ts: float | None, source: str = "all", recent_limit: int = 50) -> dict[str, Any]:
    rows, issues = fetch_rows(db, since_ts, source)
    category_groups: dict[str, dict[str, Any]] = {}
    blocker_groups: dict[str, dict[str, Any]] = {}
    source_groups: dict[str, dict[str, Any]] = {}
    unique_tokens = set()
    recent = []
    total = blocked = would_enter = did_enter = 0
    latest_ts = None
    for row in rows:
        cls = classify_event(row)
        total += 1
        blocked += int(cls["blocked"])
        would_enter += int(cls["would_enter_a_class"])
        did_enter += int(cls["did_enter"])
        if row.get("token_ca"):
            unique_tokens.add(row["token_ca"])
        if row.get("event_ts") is not None:
            latest_ts = max(float(latest_ts or 0), float(row["event_ts"] or 0))
        category_groups.setdefault(cls["category"], _new_group({"category": cls["category"]}))
        _touch(category_groups[cls["category"]], row, cls)
        skey = f"{row.get('source_kind') or row.get('source_table') or 'unknown'}|{row.get('source_component') or 'unknown'}|{cls['category']}"
        source_groups.setdefault(skey, _new_group({"source_kind": row.get("source_kind") or row.get("source_table") or "unknown", "source_component": row.get("source_component") or "unknown", "category": cls["category"]}))
        _touch(source_groups[skey], row, cls)
        for item in cls["blocker_classifications"]:
            bkey = f"{item['category']}|{item['blocker']}"
            blocker_groups.setdefault(bkey, _new_group({"blocker": item["blocker"], "category": item["category"], "recoverability": item["recoverability"], "classification_reason": item["reason"]}))
            _touch(blocker_groups[bkey], row, cls)
        if len(recent) < recent_limit:
            recent.append({
                "id": row.get("id"),
                "source_kind": row.get("source_kind"),
                "event_ts": row.get("event_ts"),
                "token_ca": row.get("token_ca"),
                "symbol": row.get("symbol"),
                "route_bucket": row.get("route_bucket"),
                "source_component": row.get("source_component"),
                "source_reason": row.get("source_reason") or row.get("reason"),
                "action": row.get("action"),
                "category": cls["category"],
                "blockers": cls["blockers"],
                "blocker_classifications": cls["blocker_classifications"],
            })
    category_summary = _finish(category_groups)
    by_cat = {row["category"]: row for row in category_summary}

    def small(category: str) -> dict[str, Any]:
        row = by_cat.get(category, {})
        return {
            "events": row.get("n", 0),
            "blocked_events": row.get("blocked_n", 0),
            "unique_tokens": row.get("unique_tokens", 0),
            "would_enter_n": row.get("would_enter_n", 0),
            "did_enter_n": row.get("did_enter_n", 0),
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "available": bool(rows),
        "source_issues": issues,
        "source_filter": source,
        "since_ts": since_ts,
        "latest_event_ts": latest_ts,
        "total_events": total,
        "blocked_events": blocked,
        "unique_tokens": len(unique_tokens),
        "would_enter_n": would_enter,
        "did_enter_n": did_enter,
        "infra_recoverable": small("INFRA"),
        "market_unexecutable": small("MARKET"),
        "policy_guardrail": small("POLICY"),
        "category_summary": category_summary,
        "blocker_summary": _finish(blocker_groups)[:100],
        "source_component_summary": _finish(source_groups)[:100],
        "recent_events": recent,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--since-ts", type=float, default=None)
    parser.add_argument("--hours", type=float, default=None)
    parser.add_argument("--source", default="all", choices=["all", "a_class", "a_class_decision_events", "opportunity", "opportunity_events"])
    parser.add_argument("--limit-recent", type=int, default=50)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    since_ts = args.since_ts
    if since_ts is None and args.hours:
        since_ts = time.time() - args.hours * 3600
    path = Path(args.db)
    db = sqlite3.connect(str(path))
    db.row_factory = sqlite3.Row
    try:
        result = build_breakdown(db, since_ts=since_ts, source=args.source, recent_limit=max(args.limit_recent, 0))
        result["db_path"] = str(path)
    finally:
        db.close()
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=args.pretty, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
