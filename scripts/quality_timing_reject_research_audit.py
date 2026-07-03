#!/usr/bin/env python3
"""Read-only quality/timing reject research audit.

This report inspects raw gold/silver events that were blocked by quality or
timing logic before final_entry_contract. It is discovery/readiness evidence
only: it never changes strategy, gates, final_entry_contract, A_CLASS mode,
executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from a_class_fastlane_mode_readiness_audit import (
    classify_pending_gap_reason,
    classify_upstream_gap_reason,
)
from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    decision_would_enter,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    normalize_ts,
    pct,
    rate,
    safe_float,
    signal_id_key,
    table_exists,
)


SCHEMA_VERSION = "quality_timing_reject_research_audit.v3"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_EXPECTED_CANDIDATES = 84
READINESS_TARGET_RATE = 0.6
BLOCKED_CONTEXT_DIMENSIONS = ["kline", "volume"]
QUOTE_FRESH_REANCHOR_MAX_AGE_SEC = float(os.environ.get("ENTRY_QUOTE_MAX_AGE_SEC", "180"))
REJECT_DECISIONS = {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
REJECT_EVENT_TYPES = {
    "entry_block",
    "entry_abort",
    "pending_reject",
    "probe_reject",
    "quality_gate",
    "timing_decision",
}
PRICE_AT_REJECT_PATHS = [
    ("price_at_reject",),
    ("price_at_rejection",),
    ("decision_price",),
    ("current_price",),
    ("trigger_price",),
    ("signal_price",),
    ("entry_price",),
    ("quote_price",),
    ("mark_price",),
    ("momentum_final_price",),
    ("price",),
    ("candidate", "decision_price"),
    ("candidate", "current_price"),
    ("candidate", "trigger_price"),
    ("candidate", "signal_price"),
    ("candidate", "entry_price"),
    ("candidate", "quote_price"),
    ("candidate", "mark_price"),
    ("final_candidate", "decision_price"),
    ("final_candidate", "trigger_price"),
    ("final_candidate", "signal_price"),
    ("final_candidate", "entry_price"),
    ("final_candidate", "quote_price"),
    ("final_candidate", "price_sol_per_token"),
    ("raw_payload", "decision_price"),
    ("raw_payload", "trigger_price"),
    ("raw_payload", "signal_price"),
    ("raw_payload", "entry_price"),
    ("entry_decision_contract", "trigger_price"),
    ("lifecycle", "lifecycle_features", "current_price"),
    ("lifecycle", "lifecycle_features", "signal_price"),
    ("lifecycle", "lifecycle_features", "trigger_price"),
    ("lifecycle_features", "current_price"),
    ("lifecycle_features", "signal_price"),
    ("lifecycle_features", "trigger_price"),
]
QUOTE_AGE_AT_REJECT_PATHS = [
    ("quote_age_at_reject",),
    ("quote_age_sec",),
    ("entry_quote_age_sec",),
    ("quoteAgeSec",),
    ("ageSec",),
    ("age_sec",),
    ("candidate", "quote_age_sec"),
    ("candidate", "entry_quote_age_sec"),
    ("final_candidate", "quote_age_sec"),
    ("raw_payload", "quote_age_sec"),
    ("quote", "quote_age_sec"),
    ("quote", "quoteAgeSec"),
    ("execution", "quote_age_sec"),
    ("entry_execution", "quote_age_sec"),
]
QUOTE_TS_AT_REJECT_PATHS = [
    ("quote_ts",),
    ("quoteTs",),
    ("quote", "quote_ts"),
    ("quote", "quoteTs"),
    ("execution", "quote_ts"),
    ("execution", "quoteTs"),
    ("entry_execution", "quote_ts"),
    ("entry_execution", "quoteTs"),
    ("final_candidate", "quote_ts"),
    ("candidate", "quote_ts"),
]


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jdump(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def decision_value(row, key, default=None):
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key] if key in row.keys() else default
    except Exception:
        return default


def reason_row(row):
    if row is None:
        return {
            "component": "UNKNOWN",
            "event_type": "missing_decision_event",
            "decision": "UNKNOWN",
            "reason": "missing_decision_event",
        }
    return {
        "component": decision_value(row, "source_component")
        or decision_value(row, "component")
        or "UNKNOWN",
        "event_type": decision_value(row, "event_type") or "UNKNOWN",
        "decision": decision_value(row, "decision") or decision_value(row, "action") or "UNKNOWN",
        "reason": decision_value(row, "reason")
        or decision_value(row, "block_cause")
        or decision_value(row, "quote_failure_reason")
        or "UNKNOWN",
    }


def row_ts(row):
    return safe_float(decision_value(row, "event_ts_norm") or decision_value(row, "event_ts")) or 0


def lift(selected_rate, base_rate):
    if selected_rate is None or base_rate is None:
        return None
    return round(float(selected_rate) - float(base_rate), 6)


def decision_payload(row):
    payload = decision_value(row, "payload_json")
    if isinstance(payload, dict):
        return payload
    return jloads(payload)


def path_value(payload, path):
    cursor = payload
    for key in path:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def first_float_from_paths(payload, paths):
    for path in paths:
        value = path_value(payload, path)
        parsed = safe_float(value)
        if parsed is not None and parsed > 0:
            return parsed, ".".join(path)
    return None, None


def quote_age_from_payload(payload, reject_ts):
    age, source = first_float_from_paths(payload, QUOTE_AGE_AT_REJECT_PATHS)
    if age is not None:
        return age, source
    quote_ts, ts_source = first_float_from_paths(payload, QUOTE_TS_AT_REJECT_PATHS)
    if quote_ts is None or reject_ts is None:
        return None, None
    if quote_ts > 10_000_000_000:
        quote_ts = quote_ts / 1000.0
    return max(0.0, float(reject_ts) - float(quote_ts)), ts_source


def reject_event_stage(row):
    component = str(decision_value(row, "source_component") or decision_value(row, "component") or "").lower()
    event_type = str(decision_value(row, "event_type") or "").lower()
    if component == "final_entry_contract" or event_type in {"entry_block", "pending_reject"}:
        return "pending_without_final_entry_contract"
    if event_type in {"entry_abort"}:
        return "pass_or_allow_without_pending_entry"
    return "decision_no_pass_or_allow"


def is_reject_event_row(row):
    event_type = str(decision_value(row, "event_type") or "").lower()
    decision = str(decision_value(row, "decision") or decision_value(row, "action") or "").upper()
    return event_type in REJECT_EVENT_TYPES or decision in REJECT_DECISIONS


def quality_timing_event_from_decision(row, *, stage=None):
    if not is_reject_event_row(row):
        return None
    stage = stage or reject_event_stage(row)
    attribution = reason_row(row)
    category = (
        classify_pending_gap_reason(attribution)
        if stage == "pending_without_final_entry_contract"
        else classify_upstream_gap_reason(attribution, stage)
    )
    if category != "QUALITY_OR_TIMING_REJECT":
        return None
    payload = decision_payload(row)
    reject_ts = normalize_ts(decision_value(row, "event_ts_norm") or decision_value(row, "event_ts"))
    price_at_reject, price_source = first_float_from_paths(payload, PRICE_AT_REJECT_PATHS)
    quote_age_at_reject, quote_age_source = quote_age_from_payload(payload, reject_ts)
    item = {
        "decision_event_id": decision_value(row, "id"),
        "signal_id": signal_id_key(decision_value(row, "signal_id")),
        "token_ca": decision_value(row, "token_ca"),
        "stage": stage,
        "category": category,
        "reject_ts": reject_ts,
        "price_at_reject": price_at_reject,
        "price_at_reject_source": price_source,
        "quote_age_at_reject": quote_age_at_reject,
        "quote_age_at_reject_source": quote_age_source,
        "attribution": attribution,
    }
    item["reason_key"] = (
        item["stage"],
        item["attribution"]["component"],
        item["attribution"]["event_type"],
        item["attribution"]["decision"],
        item["attribution"]["reason"],
    )
    item["reason_signature"] = (
        item["attribution"]["component"],
        item["attribution"]["event_type"],
        item["attribution"]["decision"],
        item["attribution"]["reason"],
    )
    return item


def raw_peak_ts(row):
    signal_ts = row.get("signal_ts_norm") if isinstance(row, dict) else None
    if signal_ts is None and isinstance(row, dict):
        signal_ts = normalize_ts(row.get("signal_ts"))
    peak_sec = safe_float(row.get("time_to_sustained_peak_sec")) if isinstance(row, dict) else None
    if signal_ts is None or peak_sec is None:
        return None
    return float(signal_ts) + float(peak_sec)


def peak_vs_reject_ordering(raw_row, reject_ts):
    peak_ts = raw_peak_ts(raw_row)
    if peak_ts is None or reject_ts is None:
        return {
            "peak_ts": peak_ts,
            "reject_ts": reject_ts,
            "available": False,
            "ordering": "missing_peak_or_reject_ts",
            "seconds_from_reject_to_peak": None,
        }
    delta = round(float(peak_ts) - float(reject_ts), 6)
    return {
        "peak_ts": peak_ts,
        "reject_ts": reject_ts,
        "available": True,
        "ordering": "reject_before_or_at_peak" if delta >= 0 else "reject_after_peak",
        "seconds_from_reject_to_peak": delta,
    }


def choose_decision_no_pass_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    terminal_rows = [
        row
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "probe_reject", "quality_gate", "timing_decision", "entry_abort"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[-1] if terminal_rows else (sorted_rows[-1] if sorted_rows else None)


def choose_pass_without_pending_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pass_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if decision_would_enter(row)
    ]
    first_pass_ts = min(pass_ts_values, default=None)
    after_pass = [
        row
        for row in sorted_rows
        if first_pass_ts is None or row_ts(row) >= first_pass_ts
    ]
    terminal_rows = [
        row
        for row in after_pass
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "entry_abort", "pending_reject"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pass[-1] if after_pass else None), first_pass_ts


def choose_pending_without_final_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pending_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
    ]
    first_pending_ts = min(pending_ts_values, default=None)
    after_pending = [
        row
        for row in sorted_rows
        if first_pending_ts is None or row_ts(row) >= first_pending_ts
    ]
    terminal_rows = [
        row
        for row in after_pending
        if str(decision_value(row, "event_type") or "").lower() == "entry_block"
        or str(decision_value(row, "decision") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
        or str(decision_value(row, "action") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pending[-1] if after_pending else None), first_pending_ts


def is_final_entry_contract(row):
    return str(decision_value(row, "source_component") or decision_value(row, "component") or "") == "final_entry_contract"


def stage_quality_timing_events(raw_rows, decisions):
    raw_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    decisions_by_signal = defaultdict(list)
    for row in decisions:
        key = signal_id_key(decision_value(row, "signal_id"))
        if key in raw_ids:
            decisions_by_signal[key].append(row)

    events = {}
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        rows = sorted(decisions_by_signal.get(signal_id, []), key=row_ts)
        if not rows:
            continue
        pass_allow = [row for row in rows if decision_would_enter(row)]
        pending = [
            row
            for row in rows
            if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
        ]
        final_contract = [row for row in rows if is_final_entry_contract(row)]

        stage = None
        chosen = None
        first_stage_ts = None
        if not pass_allow:
            stage = "decision_no_pass_or_allow"
            chosen = choose_decision_no_pass_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not pending:
            stage = "pass_or_allow_without_pending_entry"
            chosen, first_stage_ts = choose_pass_without_pending_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not final_contract:
            stage = "pending_without_final_entry_contract"
            chosen, first_stage_ts = choose_pending_without_final_row(rows)
            category = classify_pending_gap_reason(reason_row(chosen))
        else:
            continue

        if category != "QUALITY_OR_TIMING_REJECT":
            continue
        reject_info = quality_timing_event_from_decision(chosen, stage=stage) if chosen is not None else None
        item = {
            "signal_id": signal_id,
            "stage": stage,
            "category": category,
            "first_stage_ts": first_stage_ts,
            "attribution": reason_row(chosen),
        }
        if reject_info:
            item.update({
                "decision_event_id": reject_info.get("decision_event_id"),
                "reject_ts": reject_info.get("reject_ts"),
                "price_at_reject": reject_info.get("price_at_reject"),
                "price_at_reject_source": reject_info.get("price_at_reject_source"),
                "quote_age_at_reject": reject_info.get("quote_age_at_reject"),
                "quote_age_at_reject_source": reject_info.get("quote_age_at_reject_source"),
            })
        item["reason_key"] = (
            item["stage"],
            item["attribution"]["component"],
            item["attribution"]["event_type"],
            item["attribution"]["decision"],
            item["attribution"]["reason"],
        )
        item["reason_signature"] = (
            item["attribution"]["component"],
            item["attribution"]["event_type"],
            item["attribution"]["decision"],
            item["attribution"]["reason"],
        )
        events[signal_id] = item
    return events


def compact_counter(counter, names, limit=30):
    rows = []
    for key, count in counter.most_common(limit):
        if not isinstance(key, tuple):
            key = (key,)
        item = {names[idx]: key[idx] if idx < len(key) else None for idx in range(len(names))}
        item["count"] = count
        rows.append(item)
    return rows


def compact_reason_counter(counter, limit=30):
    rows = []
    for key, count in (counter or Counter()).most_common(limit):
        if not isinstance(key, tuple):
            key = (key,)
        stage, component, event_type, decision, reason = (list(key) + [None] * 5)[:5]
        rows.append({
            "stage": stage,
            "component": component,
            "event_type": event_type,
            "decision": decision,
            "reason": reason,
            "count": count,
            "suggested_shadow_only_action": reason_level_shadow_action(
                stage=stage,
                component=component,
                event_type=event_type,
                decision=decision,
                reason=reason,
            ),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return rows


def reason_level_shadow_action(*, stage, component, event_type, decision, reason):
    text = " ".join(
        str(value or "").lower()
        for value in [stage, component, event_type, decision, reason]
    )
    if "not_ath" in text or "notath" in text:
        return "track_notath_upstream_skip_reason_shadow_only"
    if "matrix" in text or "aligned" in text:
        return "track_matrix_alignment_reason_shadow_only"
    if "negative_trend" in text or "momentum_fading" in text or "negative_m5" in text:
        return "track_momentum_fading_reason_shadow_only"
    if "dead_cat" in text or "below_high" in text:
        return "track_dead_cat_below_high_reason_shadow_only"
    if "buy_pressure" in text or "weak_buying_pressure" in text:
        return "track_buy_pressure_reason_shadow_only"
    if "chasing_top" in text:
        return "track_chasing_top_reason_shadow_only"
    if "top10" in text or "concentration" in text:
        return "track_holder_concentration_reason_shadow_only"
    if "low_volume" in text or "low_vol" in text:
        return "track_low_volume_reason_shadow_only"
    if "timeout" in text or "retry" in text:
        return "track_entry_timing_timeout_reason_shadow_only"
    if "score" in text or "quality" in text:
        return "track_score_quality_reason_shadow_only"
    if "stale" in text or "too_late" in text or "late" in text:
        return "track_timing_late_reason_shadow_only"
    return "review_unclassified_quality_timing_reason_shadow_only"


def blocked_candidate_dimensions(candidate_id, family):
    candidate_text = str(candidate_id or "").lower()
    family_text = str(family or "").lower()
    dimensions = []
    if family_text == "kline" or candidate_text.startswith("kline:"):
        dimensions.append("kline")
    volume_markers = ("volume", "lowvol", "low_vol", "vol_", "_vol")
    if family_text == "volume" or any(marker in candidate_text for marker in volume_markers):
        dimensions.append("volume")
    return sorted(set(dimensions))


def is_blocked_context_candidate(candidate_id, family):
    return bool(blocked_candidate_dimensions(candidate_id, family))


def compact_candidate_counter_with_context(counter, limit=30):
    rows = []
    for (candidate_id, family), count in counter.most_common(limit):
        dimensions = blocked_candidate_dimensions(candidate_id, family)
        rows.append({
            "candidate_id": candidate_id,
            "family": family,
            "count": count,
            "blocked_context_dimensions": dimensions,
            "context_clean_for_candidate_suggestion": not bool(dimensions),
        })
    return rows


def count_raw_signals_reaching_final_entry_contract(raw_rows, decisions):
    raw_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    reached = set()
    for row in decisions:
        key = signal_id_key(decision_value(row, "signal_id"))
        if key in raw_ids and is_final_entry_contract(row):
            reached.add(key)
    return reached


def load_all_paper_decision_events(paper_db, since_ts, until_ts):
    if not table_exists(paper_db, "paper_decision_events"):
        return []
    cols = {
        row[1]
        for row in paper_db.execute("PRAGMA table_info(paper_decision_events)").fetchall()
    }
    rows = paper_db.execute(
        f"""
        SELECT id, 'paper_decision_events' AS source_kind, event_ts, signal_id, token_ca,
               symbol, lifecycle_id, component AS source_component, reason,
               event_type, decision, route, data_source, lifecycle_state,
               {('payload_json' if 'payload_json' in cols else "'{}' AS payload_json")},
               NULL AS action
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
        """,
        (since_ts - 60, until_ts + 900),
    ).fetchall()
    return [dict(row) for row in rows]


def _event_rows_to_unique_signals(events):
    return {
        event.get("signal_id")
        for event in events
        if event.get("signal_id")
    }


def build_reject_counterfactuals(*, raw_rows, qt_events, all_decisions, limit):
    raw_by_signal = {
        row.get("signal_id_key"): row
        for row in raw_rows
        if row.get("signal_id_key")
    }
    raw_signal_ids = set(raw_by_signal)
    all_decision_signal_ids = {
        signal_id_key(decision_value(row, "signal_id"))
        for row in all_decisions
        if signal_id_key(decision_value(row, "signal_id"))
    }
    all_qt_events = []
    missing_signal_id = 0
    for row in all_decisions:
        event = quality_timing_event_from_decision(row)
        if not event:
            continue
        if not event.get("signal_id"):
            missing_signal_id += 1
        all_qt_events.append(event)

    by_reason = defaultdict(list)
    for event in all_qt_events:
        by_reason[event["reason_key"]].append(event)
    raw_qt_by_reason = defaultdict(list)
    for event in (qt_events or {}).values():
        raw_qt_by_reason[event["reason_key"]].append(event)

    base_gs_signal_count = len(raw_signal_ids & all_decision_signal_ids)
    base_signal_count = len(all_decision_signal_ids)
    base_rate = rate(base_gs_signal_count, base_signal_count)
    rows = []
    for reason_key, events in sorted(by_reason.items(), key=lambda item: len(item[1]), reverse=True):
        unique_signals = _event_rows_to_unique_signals(events)
        gs_signals = unique_signals & raw_signal_ids
        raw_reason_events = raw_qt_by_reason.get(reason_key) or []
        stale_events = [
            event
            for event in events
            if "stale" in str((event.get("attribution") or {}).get("reason") or "").lower()
        ]
        quote_age_available = [
            event for event in events if event.get("quote_age_at_reject") is not None
        ]
        quote_fresh = [
            event
            for event in quote_age_available
            if safe_float(event.get("quote_age_at_reject")) is not None
            and safe_float(event.get("quote_age_at_reject")) <= QUOTE_FRESH_REANCHOR_MAX_AGE_SEC
        ]
        stage, component, event_type, decision, reason = (list(reason_key) + [None] * 5)[:5]
        p_gs = rate(len(gs_signals), len(unique_signals))
        rows.append({
            "stage": stage,
            "component": component,
            "event_type": event_type,
            "decision": decision,
            "reason": reason,
            "all_reject_events": len(events),
            "all_reject_signal_count": len(unique_signals),
            "raw_gs_reject_events": len(raw_reason_events),
            "raw_gs_reject_signal_count": len(gs_signals),
            "dud_kills": max(0, len(unique_signals) - len(gs_signals)),
            "p_gold_silver_given_reject_by_reason": p_gs,
            "base_gold_silver_rate": base_rate,
            "lift_vs_base_rate": lift(p_gs, base_rate),
            "quote_age_at_reject_available_rate": rate(len(quote_age_available), len(events)),
            "stale_reject_events": len(stale_events),
            "shadow_quote_fresh_reanchor_watch_only": {
                "enabled": bool(stale_events),
                "max_quote_age_sec": QUOTE_FRESH_REANCHOR_MAX_AGE_SEC,
                "quote_age_available_events": len(quote_age_available),
                "would_be_quote_fresh_events": len(quote_fresh),
                "would_be_quote_fresh_rate": rate(len(quote_fresh), len(events)),
                "allowed_use": "watch_only",
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
        if len(rows) >= limit:
            break

    stale_all = [
        event
        for event in all_qt_events
        if "stale" in str((event.get("attribution") or {}).get("reason") or "").lower()
    ]
    stale_quote_age = [event for event in stale_all if event.get("quote_age_at_reject") is not None]
    stale_quote_fresh = [
        event
        for event in stale_quote_age
        if safe_float(event.get("quote_age_at_reject")) is not None
        and safe_float(event.get("quote_age_at_reject")) <= QUOTE_FRESH_REANCHOR_MAX_AGE_SEC
    ]
    return {
        "schema_version": "quality_timing_reject_counterfactuals.v1",
        "available": bool(all_decisions),
        "evidence_level": EVIDENCE_LEVEL,
        "usage": "read_only_shadow_research",
        "dud_inclusive_denominator": {
            "available": bool(all_qt_events),
            "all_decision_signal_count": base_signal_count,
            "raw_gold_silver_signal_count_in_decision_denominator": base_gs_signal_count,
            "base_gold_silver_rate": base_rate,
            "all_quality_timing_reject_events": len(all_qt_events),
            "all_quality_timing_reject_signal_count": len(_event_rows_to_unique_signals(all_qt_events)),
            "raw_gold_silver_quality_timing_reject_events": len(qt_events or {}),
            "missing_signal_id_quality_timing_reject_events": missing_signal_id,
        },
        "per_reason_denominators": rows,
        "shadow_quote_fresh_reanchor_variant": {
            "reason_filter": "entry_execution_signal_stale_or_reason_contains_stale",
            "allowed_use": "watch_only",
            "max_quote_age_sec": QUOTE_FRESH_REANCHOR_MAX_AGE_SEC,
            "stale_reject_events": len(stale_all),
            "quote_age_available_events": len(stale_quote_age),
            "would_be_quote_fresh_events": len(stale_quote_fresh),
            "would_be_quote_fresh_rate": rate(len(stale_quote_fresh), len(stale_all)),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
        },
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_reject_instrumentation_summary(*, qt_events, raw_by_signal):
    qt_count = len(qt_events or {})
    reject_ts_count = 0
    price_count = 0
    quote_age_count = 0
    peak_ordering_count = 0
    ordering_counts = Counter()
    for signal_id, event in (qt_events or {}).items():
        if event.get("reject_ts") is not None:
            reject_ts_count += 1
        if event.get("price_at_reject") is not None:
            price_count += 1
        if event.get("quote_age_at_reject") is not None:
            quote_age_count += 1
        ordering = peak_vs_reject_ordering(raw_by_signal.get(signal_id), event.get("reject_ts"))
        event["peak_vs_reject_ordering"] = ordering
        if ordering.get("available"):
            peak_ordering_count += 1
        ordering_counts[ordering.get("ordering")] += 1
    blockers = []
    if qt_count and reject_ts_count < qt_count:
        blockers.append("reject_ts_coverage_below_100pct")
    if qt_count and price_count < qt_count:
        blockers.append("price_at_reject_coverage_below_100pct")
    if qt_count and quote_age_count < qt_count:
        blockers.append("quote_age_at_reject_coverage_below_100pct")
    if qt_count and peak_ordering_count < qt_count:
        blockers.append("peak_vs_reject_ordering_coverage_below_100pct")
    return {
        "schema_version": "quality_timing_reject_instrumentation.v1",
        "quality_timing_reject_event_rows": qt_count,
        "joined_exact_reject_ts_count": reject_ts_count,
        "price_at_reject_count": price_count,
        "quote_age_at_reject_count": quote_age_count,
        "peak_vs_reject_ordering_count": peak_ordering_count,
        "reject_ts_coverage_rate": rate(reject_ts_count, qt_count),
        "price_at_reject_coverage_rate": rate(price_count, qt_count),
        "quote_age_at_reject_coverage_rate": rate(quote_age_count, qt_count),
        "peak_vs_reject_ordering_coverage_rate": rate(peak_ordering_count, qt_count),
        "peak_vs_reject_ordering_counts": compact_counter(ordering_counts, ["ordering"], 20),
        "blockers": blockers,
        "acceptance_target": {
            "reject_ts_coverage_rate": 1.0,
            "price_at_reject_coverage_rate": 1.0,
            "quote_age_at_reject_coverage_rate": 1.0,
            "peak_vs_reject_ordering_coverage_rate": 1.0,
        },
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_readiness_impact_upper_bound(raw_rows, qt_count, reached_final_signal_ids, cluster_counts):
    raw_count = len(raw_rows)
    current_final_count = len(reached_final_signal_ids or set())
    target_count = int((READINESS_TARGET_RATE * raw_count) + 0.999999) if raw_count else 0
    current_gap = max(0, target_count - current_final_count)
    potential_final_count = min(raw_count, current_final_count + int(qt_count or 0))
    residual_gap_after_all_qt = max(0, target_count - potential_final_count)
    cluster_rows = []
    for cluster, count in (cluster_counts or Counter()).most_common():
        potential_count = min(raw_count, current_final_count + int(count or 0))
        cluster_rows.append({
            "cluster": cluster,
            "event_count": count,
            "current_final_eligibility_count": current_final_count,
            "upper_bound_final_eligibility_count_if_cluster_resolved": potential_count,
            "upper_bound_final_eligibility_rate_if_cluster_resolved": rate(potential_count, raw_count),
            "events_contributing_to_60pct_gap_upper_bound": min(int(count or 0), current_gap),
            "share_of_current_60pct_gap_upper_bound": rate(min(int(count or 0), current_gap), current_gap),
            "residual_gap_to_60pct_after_cluster_upper_bound": max(0, target_count - potential_count),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return {
        "target_final_eligibility_rate": READINESS_TARGET_RATE,
        "raw_all_gold_silver_event_rows": raw_count,
        "target_final_eligibility_event_count": target_count,
        "current_final_entry_contract_signal_count": current_final_count,
        "current_final_entry_contract_rate": rate(current_final_count, raw_count),
        "quality_timing_reject_event_rows": qt_count,
        "current_gap_to_60pct_event_count": current_gap,
        "quality_timing_rejects_share_of_current_60pct_gap_upper_bound": rate(min(int(qt_count or 0), current_gap), current_gap),
        "upper_bound_final_eligibility_count_if_all_quality_timing_resolved": potential_final_count,
        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": rate(potential_final_count, raw_count),
        "residual_gap_to_60pct_after_all_quality_timing_upper_bound": residual_gap_after_all_qt,
        "would_all_quality_timing_resolution_reach_60pct_upper_bound": potential_final_count >= target_count if raw_count else False,
        "cluster_upper_bounds": cluster_rows,
        "interpretation": (
            "Upper bound only: assumes every quality/timing reject could safely reach final_entry_contract. "
            "It does not prove the rejects were wrong, safe, or eligible for runtime changes."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def top_examples(rows, limit):
    rows = sorted(
        rows,
        key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
        reverse=True,
    )
    return rows[:limit]


def classify_shadow_review_cluster(stage, attribution):
    component = str((attribution or {}).get("component") or "").lower()
    event_type = str((attribution or {}).get("event_type") or "").lower()
    reason = str((attribution or {}).get("reason") or "").lower()
    decision = str((attribution or {}).get("decision") or "").lower()
    text = " ".join([stage or "", component, event_type, decision, reason])
    if "matrix" in component or "matrices not yet aligned" in reason:
        return "matrix_alignment_wait"
    if "lotto_observe_low_mc_vol" in reason or "low_volume" in reason or "low_vol" in reason:
        return "low_volume_observe"
    if "risky_newborn_pullback" in reason:
        return "newborn_pullback_timing_reject"
    if "not_ath" in reason:
        return "notath_upstream_skip"
    if "buy_pressure" in reason or "weak_buying_pressure" in reason:
        return "buy_pressure_weak"
    if "chasing_top" in reason:
        return "chasing_top_timing_reject"
    if "score_too_low" in reason or "quality_score" in reason:
        return "score_or_quality_too_low"
    if "negative_trend" in reason or "momentum_fading" in reason or "negative_m5" in reason:
        return "momentum_fading_or_negative_trend"
    if "dead_cat" in reason or "below_high" in reason:
        return "dead_cat_bounce_timing_reject"
    if "stale" in reason or "too_late" in reason or "late" in reason:
        return "entry_signal_stale_before_final"
    if "entry_node_timeout" in reason or "retry_watch_scheduled" in reason:
        return "entry_timing_timeout_or_retry"
    if "final_entry" in component or "final_entry" in reason:
        return "final_entry_contract_research_block"
    if "top10_pct" in reason or "concentration" in reason:
        return "holder_concentration_quality_reject"
    if text.strip():
        return "other_quality_timing_reject"
    return "unknown_quality_timing_reject"


SHADOW_REVIEW_CLUSTER_DETAILS = {
    "matrix_alignment_wait": {
        "description": "Raw gold/silver reached a matrix wait state before pass/pending/final eligibility.",
        "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing matrix alignment thresholds or treating OBSERVE/WAIT as entry-eligible",
    },
    "low_volume_observe": {
        "description": "Raw gold/silver was skipped or delayed by low-volume observation logic.",
        "suggested_shadow_only_action": "track_low_volume_gold_silver_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing low-volume, liquidity, or market-quality gates",
    },
    "newborn_pullback_timing_reject": {
        "description": "Raw gold/silver was rejected by newborn pullback timing logic.",
        "suggested_shadow_only_action": "track_newborn_pullback_timing_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing newborn timing rules or pullback rejection thresholds",
    },
    "notath_upstream_skip": {
        "description": "Raw gold/silver was skipped by upstream NOT_ATH routing or classification.",
        "suggested_shadow_only_action": "track_notath_upstream_skip_shadow_probe",
        "human_approval_required_if_fix_requires": "changing upstream ATH/NOT_ATH routing policy",
    },
    "buy_pressure_weak": {
        "description": "Raw gold/silver was rejected by weak buy-pressure or scout-quality logic.",
        "suggested_shadow_only_action": "track_buy_pressure_weak_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing scout quality or buy-pressure gates",
    },
    "chasing_top_timing_reject": {
        "description": "Raw gold/silver was rejected as chasing top.",
        "suggested_shadow_only_action": "track_chasing_top_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing anti-chase timing policy",
    },
    "score_or_quality_too_low": {
        "description": "Raw gold/silver was rejected by a score or quality threshold.",
        "suggested_shadow_only_action": "track_score_quality_threshold_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing score or quality thresholds",
    },
    "momentum_fading_or_negative_trend": {
        "description": "Raw gold/silver was rejected for fading momentum or negative trend.",
        "suggested_shadow_only_action": "track_momentum_fading_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing momentum/trend rejection policy",
    },
    "dead_cat_bounce_timing_reject": {
        "description": "Raw gold/silver was rejected as a dead-cat or too-far-below-high timing setup.",
        "suggested_shadow_only_action": "track_dead_cat_below_high_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing dead-cat, pullback, or below-high timing policy",
    },
    "entry_signal_stale_before_final": {
        "description": "Raw gold/silver became stale before final-entry eligibility.",
        "suggested_shadow_only_action": "track_entry_signal_stale_before_final_shadow_probe",
        "human_approval_required_if_fix_requires": "changing freshness, stale-entry, final_entry_contract, or execution timing policy",
    },
    "entry_timing_timeout_or_retry": {
        "description": "Raw gold/silver was delayed or timed out around entry retry/timing logic.",
        "suggested_shadow_only_action": "track_entry_timing_timeout_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing entry retry, timeout, or scheduling policy",
    },
    "final_entry_contract_research_block": {
        "description": "Raw gold/silver reached final-entry-related block evidence inside the quality/timing audit scope.",
        "suggested_shadow_only_action": "decompose_final_entry_hard_block_shadow_only",
        "human_approval_required_if_fix_requires": "changing final_entry_contract, A_CLASS mode, or paper/live enablement",
    },
    "holder_concentration_quality_reject": {
        "description": "Raw gold/silver was rejected by holder concentration or related quality logic.",
        "suggested_shadow_only_action": "track_holder_concentration_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing holder concentration or quality gates",
    },
    "other_quality_timing_reject": {
        "description": "Raw gold/silver was rejected by a less frequent quality/timing reason.",
        "suggested_shadow_only_action": "continue_reason_level_shadow_review",
        "human_approval_required_if_fix_requires": "changing strategy, entry policy, gate, final_entry, or runtime behavior",
    },
    "unknown_quality_timing_reject": {
        "description": "Raw gold/silver was rejected but the reason payload was not classifiable.",
        "suggested_shadow_only_action": "improve_quality_timing_reason_instrumentation",
        "human_approval_required_if_fix_requires": "changing strategy, entry policy, gate, final_entry, or runtime behavior",
    },
}


def median_or_none(values):
    parsed = [safe_float(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    return None if not parsed else round(float(median(parsed)), 6)


def max_or_none(values):
    parsed = [safe_float(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    return None if not parsed else round(max(parsed), 6)


def build_shadow_only_review(
    *,
    raw_rows,
    qt_count,
    stage_counts,
    cluster_counts,
    cluster_reason_counts,
    cluster_stage_counts,
    cluster_candidate_counts,
    cluster_family_counts,
    cluster_clean_candidate_counts,
    cluster_clean_family_counts,
    cluster_blocked_candidate_counts,
    cluster_blocked_family_counts,
    cluster_context_counts,
    cluster_tokens,
    cluster_matched_any,
    cluster_clean_matched_any,
    cluster_blocked_matched_any,
    cluster_peak_pct,
    cluster_time_to_peak,
    readiness_impact_upper_bound,
    limit,
):
    cluster_impact = {
        row.get("cluster"): row
        for row in (readiness_impact_upper_bound or {}).get("cluster_upper_bounds") or []
    }
    opportunities = []
    for cluster, count in cluster_counts.most_common(limit):
        details = SHADOW_REVIEW_CLUSTER_DETAILS.get(
            cluster,
            SHADOW_REVIEW_CLUSTER_DETAILS["other_quality_timing_reject"],
        )
        reason_level_breakout = compact_reason_counter(
            cluster_reason_counts.get(cluster) or Counter(),
            limit,
        )
        opportunities.append({
            "cluster": cluster,
            "description": details["description"],
            "event_count": count,
            "share_of_quality_timing_rejects": rate(count, qt_count),
            "share_of_raw_all_gold_silver": rate(count, len(raw_rows)),
            "unique_tokens": len(cluster_tokens.get(cluster) or set()),
            "candidate_matched_any_rate": rate(cluster_matched_any.get(cluster, 0), count),
            "clean_candidate_matched_any_rate": rate(cluster_clean_matched_any.get(cluster, 0), count),
            "blocked_candidate_matched_any_rate": rate(cluster_blocked_matched_any.get(cluster, 0), count),
            "max_sustained_peak_pct_max": max_or_none(cluster_peak_pct.get(cluster) or []),
            "time_to_sustained_peak_sec_median": median_or_none(cluster_time_to_peak.get(cluster) or []),
            "readiness_impact_upper_bound": cluster_impact.get(cluster) or {},
            "reason_level_breakout": reason_level_breakout,
            "reason_level_breakout_count": len(reason_level_breakout),
            "reason_level_review_status": (
                "REASON_LEVEL_READY"
                if reason_level_breakout
                else "REASON_LEVEL_EMPTY"
            ),
            "stage_counts": compact_counter(
                cluster_stage_counts.get(cluster) or Counter(),
                ["stage"],
                limit,
            ),
            "top_candidates": compact_counter(
                cluster_candidate_counts.get(cluster) or Counter(),
                ["candidate_id", "family"],
                limit,
            ),
            "top_clean_candidates": compact_candidate_counter_with_context(
                cluster_clean_candidate_counts.get(cluster) or Counter(),
                limit,
            ),
            "top_blocked_candidates": compact_candidate_counter_with_context(
                cluster_blocked_candidate_counts.get(cluster) or Counter(),
                limit,
            ),
            "top_families": compact_counter(
                cluster_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_clean_families": compact_counter(
                cluster_clean_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_blocked_families": compact_counter(
                cluster_blocked_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_lifecycle_source_contexts": compact_counter(
                cluster_context_counts.get(cluster) or Counter(),
                ["lifecycle_profile", "source_component"],
                limit,
            ),
            "suggested_shadow_only_action": details["suggested_shadow_only_action"],
            "evidence_level": EVIDENCE_LEVEL,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "human_approval_required_if_fix_requires": details["human_approval_required_if_fix_requires"],
            "next_validation": "repeat_same_cluster_in_clean_window_then_oos_if_it_generates_shadow_candidate_lift",
        })
    dominant_stage = stage_counts.most_common(1)[0][0] if stage_counts else None
    return {
        "classification": (
            "QUALITY_TIMING_SHADOW_REVIEW_READY"
            if qt_count
            else "QUALITY_TIMING_SHADOW_REVIEW_EMPTY"
        ),
        "quality_timing_false_negative_upper_bound": {
            "event_count": qt_count,
            "raw_all_gold_silver_event_rows": len(raw_rows),
            "rate": rate(qt_count, len(raw_rows)),
            "interpretation": (
                "Upper bound only: these raw gold/silver events were rejected by quality/timing logic, "
                "but this does not prove the reject was wrong or safe to trade."
            ),
        },
        "readiness_impact_upper_bound": readiness_impact_upper_bound,
        "dominant_cluster": opportunities[0]["cluster"] if opportunities else None,
        "dominant_stage": dominant_stage,
        "research_opportunity_count": len(opportunities),
        "top_research_opportunities": opportunities,
        "allowed_scope": [
            "read-only evaluator/report improvements",
            "shadow-only candidate or context instrumentation",
            "hypothesis registry entries for future clean-window/OOS validation",
        ],
        "forbidden_scope": [
            "strategy change",
            "entry policy change",
            "hard gate relaxation",
            "exit gate change",
            "final_entry_contract change",
            "A_CLASS mode reset or enablement",
            "paper/live executor enablement",
            "canary or risk increase",
        ],
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Use these clusters to decide what shadow-only probes to add or watch next.",
            "Prefer top_clean_candidates when generating shadow probes while kline/volume dimensions are blocked.",
            "Any change to thresholds, gates, final_entry_contract, runtime mode, executor, or risk requires human approval.",
        ],
    }


def build_reason_level_breakout(*, cluster_counts, cluster_reason_counts, limit):
    dominant_cluster = cluster_counts.most_common(1)[0][0] if cluster_counts else None
    dominant_reasons = compact_reason_counter(
        cluster_reason_counts.get(dominant_cluster) or Counter(),
        limit,
    ) if dominant_cluster else []
    other_reasons = compact_reason_counter(
        cluster_reason_counts.get("other_quality_timing_reject") or Counter(),
        limit,
    )
    all_cluster_rows = []
    for cluster, count in (cluster_counts or Counter()).most_common(limit):
        reason_rows = compact_reason_counter(
            cluster_reason_counts.get(cluster) or Counter(),
            min(5, limit),
        )
        all_cluster_rows.append({
            "cluster": cluster,
            "event_count": count,
            "reason_count": len(reason_rows),
            "top_reasons": reason_rows,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    if other_reasons:
        next_action = "review_other_quality_timing_reason_breakout_shadow_only"
    elif dominant_reasons:
        next_action = "review_dominant_quality_timing_reason_breakout_shadow_only"
    else:
        next_action = "continue_collecting_quality_timing_reason_evidence"
    return {
        "classification": (
            "QUALITY_TIMING_REASON_LEVEL_READY"
            if (dominant_reasons or other_reasons)
            else "QUALITY_TIMING_REASON_LEVEL_EMPTY"
        ),
        "dominant_cluster": dominant_cluster,
        "dominant_cluster_top_reasons": dominant_reasons,
        "other_quality_timing_top_reasons": other_reasons,
        "cluster_reason_breakouts": all_cluster_rows,
        "next_action": next_action,
        "interpretation": (
            "Read-only reason-level decomposition of quality/timing rejects. "
            "This narrows broad clusters such as other_quality_timing_reject into exact "
            "stage/component/event_type/decision/reason signatures for shadow-only review."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_blocked_context_excluded_view(
    *,
    qt_count,
    clean_candidate_counts,
    clean_family_counts,
    blocked_candidate_counts,
    blocked_family_counts,
    clean_matched_any,
    blocked_matched_any,
    clean_observation_count,
    blocked_observation_count,
    limit,
):
    return {
        "classification": (
            "CLEAN_CANDIDATE_ATTRIBUTION_READY"
            if qt_count
            else "CLEAN_CANDIDATE_ATTRIBUTION_EMPTY"
        ),
        "blocked_dimensions": BLOCKED_CONTEXT_DIMENSIONS,
        "interpretation": (
            "Read-only candidate attribution that excludes candidates depending on blocked kline/volume dimensions. "
            "Blocked candidates remain visible for diagnostics but must not drive shadow probe suggestions until the dimensions are clean."
        ),
        "quality_timing_reject_event_rows": qt_count,
        "clean_candidate_matched_any_events": clean_matched_any,
        "clean_candidate_matched_any_rate": rate(clean_matched_any, qt_count),
        "blocked_candidate_matched_any_events": blocked_matched_any,
        "blocked_candidate_matched_any_rate": rate(blocked_matched_any, qt_count),
        "clean_candidate_observation_count": clean_observation_count,
        "blocked_candidate_observation_count": blocked_observation_count,
        "top_clean_candidates": compact_candidate_counter_with_context(clean_candidate_counts, limit),
        "top_blocked_candidates": compact_candidate_counter_with_context(blocked_candidate_counts, limit),
        "top_clean_families": compact_counter(clean_family_counts, ["family"], limit),
        "top_blocked_families": compact_counter(blocked_family_counts, ["family"], limit),
        "candidate_suggestion_policy": (
            "Use top_clean_candidates for shadow-only quality/timing probe generation. "
            "Do not use top_blocked_candidates while kline/volume context coverage is blocked."
        ),
        "evidence_level": EVIDENCE_LEVEL,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_top_level_summary(
    *,
    verdict,
    blockers,
    denominator,
    readiness_impact,
    shadow_review,
    reason_level_breakout=None,
):
    opportunities = (shadow_review or {}).get("top_research_opportunities") or []
    dominant = opportunities[0] if opportunities else {}
    if blockers:
        next_action = "fix_quality_timing_audit_data_blockers"
    elif (reason_level_breakout or {}).get("other_quality_timing_top_reasons"):
        next_action = "review_other_quality_timing_reason_breakout_shadow_only"
    elif dominant:
        next_action = dominant.get("suggested_shadow_only_action") or "review_shadow_candidates_for_quality_timing_rejects"
    else:
        next_action = "continue_collecting_quality_timing_reject_evidence"
    top_clusters = []
    for row in opportunities[:8]:
        top_clusters.append({
            "cluster": row.get("cluster"),
            "event_count": row.get("event_count"),
            "share_of_quality_timing_rejects": row.get("share_of_quality_timing_rejects"),
            "share_of_raw_all_gold_silver": row.get("share_of_raw_all_gold_silver"),
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
            "next_validation": row.get("next_validation"),
            "reason_level_breakout": (row.get("reason_level_breakout") or [])[:5],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return {
        "classification": verdict,
        "next_action": next_action,
        "dominant_cluster": dominant.get("cluster"),
        "dominant_stage": (shadow_review or {}).get("dominant_stage"),
        "quality_timing_reject_event_rows": (denominator or {}).get("quality_timing_reject_event_rows"),
        "quality_timing_reject_share_of_raw_all": (denominator or {}).get("quality_timing_reject_share_of_raw_all"),
        "raw_all_gold_silver_event_rows": (denominator or {}).get("raw_all_gold_silver_event_rows"),
        "current_final_entry_contract_rate": (readiness_impact or {}).get("current_final_entry_contract_rate"),
        "current_final_entry_contract_signal_count": (
            readiness_impact or {}
        ).get("current_final_entry_contract_signal_count"),
        "target_final_eligibility_event_count": (readiness_impact or {}).get("target_final_eligibility_event_count"),
        "current_gap_to_60pct_event_count": (readiness_impact or {}).get("current_gap_to_60pct_event_count"),
        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": (
            readiness_impact or {}
        ).get("upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"),
        "residual_gap_to_60pct_after_all_quality_timing_upper_bound": (
            readiness_impact or {}
        ).get("residual_gap_to_60pct_after_all_quality_timing_upper_bound"),
        "would_all_quality_timing_resolution_reach_60pct_upper_bound": (
            readiness_impact or {}
        ).get("would_all_quality_timing_resolution_reach_60pct_upper_bound"),
        "top_quality_timing_clusters": top_clusters,
        "reason_level_breakout": reason_level_breakout or {},
        "allowed_scope": (shadow_review or {}).get("allowed_scope") or [
            "read-only evaluator/report improvements",
            "shadow-only candidate or context instrumentation",
        ],
        "human_approval_required_if_fix_requires": (
            dominant.get("human_approval_required_if_fix_requires")
            or "changing strategy, entry policy, gate, final_entry, runtime mode, executor, or risk"
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "interpretation": (
            "Top-level read-only summary for the quality/timing reject audit. "
            "It ranks shadow-only research targets and does not authorize runtime, strategy, gate, "
            "final_entry_contract, paper, executor, canary, or risk changes."
        ),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        if not table_exists(raw_db, "raw_signal_outcomes"):
            raise SystemExit("raw_signal_outcomes table missing")
        raw_rows = load_raw_dogs(raw_db, since_ts)
        raw_signal_ids, raw_tokens, _, _ = make_raw_indexes(raw_rows)
        until_ts = max([row.get("signal_ts_norm") or since_ts for row in raw_rows] + [now_ts])
        observations, obs_meta = load_candidate_observations(paper_db, raw_signal_ids, since_ts)
        decisions = load_paper_decisions(paper_db, raw_tokens, since_ts, until_ts)
        all_decisions = load_all_paper_decision_events(paper_db, since_ts, until_ts)
        trades = load_paper_trades(paper_db, raw_tokens, since_ts, until_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, args.expected_candidates)
        audits_by_signal = {row.get("signal_id"): row for row in audits if row.get("signal_id")}
        raw_by_signal = {row.get("signal_id_key"): row for row in raw_rows if row.get("signal_id_key")}
        observations_by_signal = defaultdict(list)
        for obs in observations:
            if obs.get("signal_id_key"):
                observations_by_signal[obs["signal_id_key"]].append(obs)

        qt_events = stage_quality_timing_events(raw_rows, decisions)
        reject_instrumentation = build_reject_instrumentation_summary(
            qt_events=qt_events,
            raw_by_signal=raw_by_signal,
        )
        reject_counterfactuals = build_reject_counterfactuals(
            raw_rows=raw_rows,
            qt_events=qt_events,
            all_decisions=all_decisions,
            limit=args.limit,
        )
        reached_final_signal_ids = count_raw_signals_reaching_final_entry_contract(raw_rows, decisions)
        qt_rows = []
        stage_counts = Counter()
        reason_counts = Counter()
        candidate_counts = Counter()
        family_counts = Counter()
        clean_candidate_counts = Counter()
        clean_family_counts = Counter()
        blocked_candidate_counts = Counter()
        blocked_family_counts = Counter()
        context_counts = Counter()
        lifecycle_counts = Counter()
        source_counts = Counter()
        markov_counts = Counter()
        schema_counts = Counter()
        quote_clean_counts = Counter()
        quote_exec_counts = Counter()
        cluster_counts = Counter()
        cluster_reason_counts = defaultdict(Counter)
        cluster_stage_counts = defaultdict(Counter)
        cluster_candidate_counts = defaultdict(Counter)
        cluster_family_counts = defaultdict(Counter)
        cluster_clean_candidate_counts = defaultdict(Counter)
        cluster_clean_family_counts = defaultdict(Counter)
        cluster_blocked_candidate_counts = defaultdict(Counter)
        cluster_blocked_family_counts = defaultdict(Counter)
        cluster_context_counts = defaultdict(Counter)
        cluster_tokens = defaultdict(set)
        cluster_matched_any = Counter()
        cluster_clean_matched_any = Counter()
        cluster_blocked_matched_any = Counter()
        cluster_peak_pct = defaultdict(list)
        cluster_time_to_peak = defaultdict(list)
        coverage_ok = 0
        matched_any = 0
        clean_matched_any = 0
        blocked_matched_any = 0
        total_obs_rows = 0
        clean_observation_count = 0
        blocked_observation_count = 0

        for signal_id, event in qt_events.items():
            audit = audits_by_signal.get(signal_id) or {}
            signal_obs = observations_by_signal.get(signal_id, [])
            matched_obs = [obs for obs in signal_obs if obs.get("matched")]
            candidate_coverage_ok = len({obs.get("candidate_id") for obs in signal_obs}) == args.expected_candidates
            stage_counts[event["stage"]] += 1
            reason_counts[event["reason_key"]] += 1
            if candidate_coverage_ok:
                coverage_ok += 1
            if matched_obs:
                matched_any += 1
            total_obs_rows += len(signal_obs)
            lifecycle = audit.get("lifecycle_profile") or "UNKNOWN"
            source = audit.get("source_component") or "UNKNOWN"
            lifecycle_counts[lifecycle] += 1
            source_counts[source] += 1
            context_counts[(lifecycle, source)] += 1
            markov_counts[str(audit.get("markov_bucket") or "UNKNOWN")] += 1
            schema_counts[str(audit.get("context_schema_version") or "legacy_or_missing")] += 1
            quote_clean_counts[str(audit.get("source_quote_clean"))] += 1
            quote_exec_counts[str(audit.get("source_quote_executable"))] += 1
            cluster = classify_shadow_review_cluster(event["stage"], event["attribution"])
            cluster_counts[cluster] += 1
            cluster_reason_counts[cluster][event["reason_key"]] += 1
            cluster_stage_counts[cluster][event["stage"]] += 1
            cluster_context_counts[cluster][(lifecycle, source)] += 1
            if audit.get("token_ca"):
                cluster_tokens[cluster].add(audit.get("token_ca"))
            if matched_obs:
                cluster_matched_any[cluster] += 1
            cluster_peak_pct[cluster].append(audit.get("max_sustained_peak_pct"))
            cluster_time_to_peak[cluster].append(audit.get("time_to_sustained_peak_sec"))
            event_has_clean_match = False
            event_has_blocked_match = False
            for obs in matched_obs:
                candidate_id = obs.get("candidate_id") or "UNKNOWN"
                family = obs.get("family") or "UNKNOWN"
                candidate_counts[(candidate_id, family)] += 1
                family_counts[family] += 1
                cluster_candidate_counts[cluster][
                    (candidate_id, family)
                ] += 1
                cluster_family_counts[cluster][family] += 1
                if is_blocked_context_candidate(candidate_id, family):
                    blocked_candidate_counts[(candidate_id, family)] += 1
                    blocked_family_counts[family] += 1
                    cluster_blocked_candidate_counts[cluster][(candidate_id, family)] += 1
                    cluster_blocked_family_counts[cluster][family] += 1
                    blocked_observation_count += 1
                    event_has_blocked_match = True
                else:
                    clean_candidate_counts[(candidate_id, family)] += 1
                    clean_family_counts[family] += 1
                    cluster_clean_candidate_counts[cluster][(candidate_id, family)] += 1
                    cluster_clean_family_counts[cluster][family] += 1
                    clean_observation_count += 1
                    event_has_clean_match = True
            if event_has_clean_match:
                clean_matched_any += 1
                cluster_clean_matched_any[cluster] += 1
            if event_has_blocked_match:
                blocked_matched_any += 1
                cluster_blocked_matched_any[cluster] += 1
            qt_rows.append(
                {
                    "signal_id": signal_id,
                    "token_ca": audit.get("token_ca"),
                    "symbol": audit.get("symbol"),
                    "tier": audit.get("tier"),
                    "stage": event["stage"],
                    "decision_event_id": event.get("decision_event_id"),
                    "reject_ts": event.get("reject_ts"),
                    "price_at_reject": event.get("price_at_reject"),
                    "price_at_reject_source": event.get("price_at_reject_source"),
                    "quote_age_at_reject": event.get("quote_age_at_reject"),
                    "quote_age_at_reject_source": event.get("quote_age_at_reject_source"),
                    "peak_vs_reject_ordering": event.get("peak_vs_reject_ordering"),
                    "shadow_review_cluster": cluster,
                    "attribution": event["attribution"],
                    "candidate_observation_count": len(signal_obs),
                    "candidate_coverage_ok": candidate_coverage_ok,
                    "matched_candidate_count": len(matched_obs),
                    "top_matched_candidates": [
                        {
                            "candidate_id": obs.get("candidate_id"),
                            "family": obs.get("family"),
                            "reason": obs.get("reason"),
                            "blocked_context_dimensions": blocked_candidate_dimensions(
                                obs.get("candidate_id"),
                                obs.get("family"),
                            ),
                        }
                        for obs in matched_obs[:20]
                    ],
                    "clean_matched_candidate_count": sum(
                        1
                        for obs in matched_obs
                        if not is_blocked_context_candidate(obs.get("candidate_id"), obs.get("family"))
                    ),
                    "blocked_matched_candidate_count": sum(
                        1
                        for obs in matched_obs
                        if is_blocked_context_candidate(obs.get("candidate_id"), obs.get("family"))
                    ),
                    "lifecycle_profile": lifecycle,
                    "source_component": source,
                    "markov_bucket": audit.get("markov_bucket"),
                    "source_quote_clean": audit.get("source_quote_clean"),
                    "source_quote_executable": audit.get("source_quote_executable"),
                    "context_schema_version": audit.get("context_schema_version"),
                    "max_sustained_peak_pct": audit.get("max_sustained_peak_pct"),
                    "time_to_sustained_peak_sec": audit.get("time_to_sustained_peak_sec"),
                }
            )

        qt_count = len(qt_events)
        blockers = []
        if not raw_rows:
            blockers.append("raw_gold_silver_denominator_empty")
        if qt_count and rate(coverage_ok, qt_count) is not None and rate(coverage_ok, qt_count) < 0.99:
            blockers.append("quality_timing_candidate_coverage_incomplete")
        verdict = "QUALITY_TIMING_REJECT_RESEARCH_READY"
        if not qt_count:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_EMPTY"
        elif blockers:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_BLOCKED_DATA"
        readiness_impact_upper_bound = build_readiness_impact_upper_bound(
            raw_rows,
            qt_count,
            reached_final_signal_ids,
            cluster_counts,
        )
        blocked_context_excluded_view = build_blocked_context_excluded_view(
            qt_count=qt_count,
            clean_candidate_counts=clean_candidate_counts,
            clean_family_counts=clean_family_counts,
            blocked_candidate_counts=blocked_candidate_counts,
            blocked_family_counts=blocked_family_counts,
            clean_matched_any=clean_matched_any,
            blocked_matched_any=blocked_matched_any,
            clean_observation_count=clean_observation_count,
            blocked_observation_count=blocked_observation_count,
            limit=args.limit,
        )
        shadow_only_review = build_shadow_only_review(
            raw_rows=raw_rows,
            qt_count=qt_count,
            stage_counts=stage_counts,
            cluster_counts=cluster_counts,
            cluster_reason_counts=cluster_reason_counts,
            cluster_stage_counts=cluster_stage_counts,
            cluster_candidate_counts=cluster_candidate_counts,
            cluster_family_counts=cluster_family_counts,
            cluster_clean_candidate_counts=cluster_clean_candidate_counts,
            cluster_clean_family_counts=cluster_clean_family_counts,
            cluster_blocked_candidate_counts=cluster_blocked_candidate_counts,
            cluster_blocked_family_counts=cluster_blocked_family_counts,
            cluster_context_counts=cluster_context_counts,
            cluster_tokens=cluster_tokens,
            cluster_matched_any=cluster_matched_any,
            cluster_clean_matched_any=cluster_clean_matched_any,
            cluster_blocked_matched_any=cluster_blocked_matched_any,
            cluster_peak_pct=cluster_peak_pct,
            cluster_time_to_peak=cluster_time_to_peak,
            readiness_impact_upper_bound=readiness_impact_upper_bound,
            limit=args.limit,
        )
        reason_level_breakout = build_reason_level_breakout(
            cluster_counts=cluster_counts,
            cluster_reason_counts=cluster_reason_counts,
            limit=args.limit,
        )
        denominator = {
            "raw_all_gold_silver_event_rows": len(raw_rows),
            "raw_all_gold_silver_unique_tokens": len({row.get("token_ca") for row in raw_rows if row.get("token_ca")}),
            "quality_timing_reject_event_rows": qt_count,
            "quality_timing_reject_unique_tokens": len({
                (audits_by_signal.get(signal_id) or {}).get("token_ca")
                for signal_id in qt_events
                if (audits_by_signal.get(signal_id) or {}).get("token_ca")
            }),
            "quality_timing_reject_share_of_raw_all": rate(qt_count, len(raw_rows)),
            "quality_timing_reject_share_of_raw_all_pct": pct(qt_count, len(raw_rows)),
            "dud_inclusive_reject_denominator_available": (
                reject_counterfactuals.get("dud_inclusive_denominator") or {}
            ).get("available"),
            "all_quality_timing_reject_events": (
                reject_counterfactuals.get("dud_inclusive_denominator") or {}
            ).get("all_quality_timing_reject_events"),
            "all_quality_timing_reject_signal_count": (
                reject_counterfactuals.get("dud_inclusive_denominator") or {}
            ).get("all_quality_timing_reject_signal_count"),
            "base_gold_silver_rate": (
                reject_counterfactuals.get("dud_inclusive_denominator") or {}
            ).get("base_gold_silver_rate"),
        }
        summary = build_top_level_summary(
            verdict=verdict,
            blockers=blockers,
            denominator=denominator,
            readiness_impact=readiness_impact_upper_bound,
            shadow_review=shadow_only_review,
            reason_level_breakout=reason_level_breakout,
        )

        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "quality_timing_reject_research_audit",
            "generated_at": utc_now(),
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {"paper_db": args.db, "raw_db": args.raw_db, "raw_funnel": args.raw_funnel},
            "evidence_level": EVIDENCE_LEVEL,
            "usage": "read_only_shadow_research_quality_timing_rejects",
            "promotion_allowed": False,
            "can_promote_live": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "final_entry_contract_change_allowed": False,
            "classification": summary["classification"],
            "next_action": summary["next_action"],
            "dominant_cluster": summary["dominant_cluster"],
            "dominant_stage": summary["dominant_stage"],
            "quality_timing_reject_event_rows": summary["quality_timing_reject_event_rows"],
            "quality_timing_reject_share_of_raw_all": summary["quality_timing_reject_share_of_raw_all"],
            "current_final_entry_contract_rate": summary["current_final_entry_contract_rate"],
            "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": (
                summary["upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"]
            ),
            "would_all_quality_timing_resolution_reach_60pct_upper_bound": (
                summary["would_all_quality_timing_resolution_reach_60pct_upper_bound"]
            ),
            "residual_gap_to_60pct_after_all_quality_timing_upper_bound": (
                summary["residual_gap_to_60pct_after_all_quality_timing_upper_bound"]
            ),
            "top_quality_timing_clusters": summary["top_quality_timing_clusters"],
            "reason_level_breakout": reason_level_breakout,
            "allowed_scope": summary["allowed_scope"],
            "human_approval_required_if_fix_requires": summary["human_approval_required_if_fix_requires"],
            "verdict": verdict,
            "blockers": blockers,
            "summary": summary,
            "denominator": denominator,
            "reject_instrumentation": reject_instrumentation,
            "reject_counterfactuals": reject_counterfactuals,
            "observation_load": obs_meta,
            "candidate_match_attribution": {
                "expected_candidates": args.expected_candidates,
                "candidate_observation_rows": total_obs_rows,
                "events_with_full_candidate_coverage": coverage_ok,
                "full_candidate_coverage_rate": rate(coverage_ok, qt_count),
                "full_candidate_coverage_pct": pct(coverage_ok, qt_count),
                "candidate_matched_any_events": matched_any,
                "candidate_matched_any_rate": rate(matched_any, qt_count),
                "candidate_matched_any_pct": pct(matched_any, qt_count),
                "top_candidates": compact_counter(candidate_counts, ["candidate_id", "family"], args.limit),
                "top_families": compact_counter(family_counts, ["family"], args.limit),
            },
            "blocked_context_dimensions_excluded_view": blocked_context_excluded_view,
            "readiness_impact_upper_bound": readiness_impact_upper_bound,
            "stage_attribution": {
                "stage_counts": compact_counter(stage_counts, ["stage"], args.limit),
                "reason_counts": compact_counter(
                    reason_counts,
                    ["stage", "component", "event_type", "decision", "reason"],
                    args.limit,
                ),
                "cluster_reason_counts": {
                    cluster: compact_reason_counter(counter, args.limit)
                    for cluster, counter in cluster_reason_counts.items()
                },
            },
            "context_attribution": {
                "lifecycle_profile_counts": compact_counter(lifecycle_counts, ["lifecycle_profile"], args.limit),
                "source_component_counts": compact_counter(source_counts, ["source_component"], args.limit),
                "lifecycle_source_counts": compact_counter(
                    context_counts,
                    ["lifecycle_profile", "source_component"],
                    args.limit,
                ),
                "markov_bucket_counts": compact_counter(markov_counts, ["markov_bucket"], args.limit),
                "context_schema_version_counts": compact_counter(schema_counts, ["context_schema_version"], args.limit),
                "source_quote_clean_counts": compact_counter(quote_clean_counts, ["source_quote_clean"], args.limit),
                "source_quote_executable_counts": compact_counter(
                    quote_exec_counts,
                    ["source_quote_executable"],
                    args.limit,
                ),
            },
            "shadow_only_review": shadow_only_review,
            "shadow_only_next_actions": [
                "review_shadow_candidates_for_quality_timing_rejects",
                "compare quality/timing reject candidate families against pending/final eligibility lifts",
                "do not relax timing or quality thresholds without human approval",
            ],
            "top_examples": top_examples(qt_rows, args.limit),
            "notes": [
                "Research-only upper-bound audit. This report explains quality/timing rejects; it does not authorize strategy, gate, final_entry_contract, A_CLASS, executor, or risk changes.",
                "promotion_allowed remains false.",
            ],
        }
    finally:
        raw_db.close()
        paper_db.close()


def compact_summary(report):
    return {
        "verdict": report.get("verdict"),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "denominator": report.get("denominator") or {},
        "candidate_match_attribution": report.get("candidate_match_attribution") or {},
        "blocked_context_dimensions_excluded_view": (
            report.get("blocked_context_dimensions_excluded_view") or {}
        ),
        "readiness_impact_upper_bound": report.get("readiness_impact_upper_bound") or {},
        "reject_instrumentation": report.get("reject_instrumentation") or {},
        "reject_counterfactuals": {
            "dud_inclusive_denominator": (
                (report.get("reject_counterfactuals") or {}).get("dud_inclusive_denominator") or {}
            ),
            "top_per_reason_denominators": (
                (report.get("reject_counterfactuals") or {}).get("per_reason_denominators") or []
            )[:10],
            "shadow_quote_fresh_reanchor_variant": (
                (report.get("reject_counterfactuals") or {}).get("shadow_quote_fresh_reanchor_variant") or {}
            ),
        },
        "reason_level_breakout": report.get("reason_level_breakout") or {},
        "top_stage_counts": ((report.get("stage_attribution") or {}).get("stage_counts") or [])[:8],
        "top_reason_counts": ((report.get("stage_attribution") or {}).get("reason_counts") or [])[:8],
        "top_candidates": ((report.get("candidate_match_attribution") or {}).get("top_candidates") or [])[:10],
        "top_contexts": ((report.get("context_attribution") or {}).get("lifecycle_source_counts") or [])[:10],
        "shadow_only_review": report.get("shadow_only_review") or {},
        "blockers": report.get("blockers") or [],
    }


def self_test():
    assert classify_shadow_review_cluster(
        "pending_without_final_entry_contract",
        {
            "component": "entry_execution_eligibility",
            "event_type": "entry_block",
            "decision": "watch_only",
            "reason": "entry_execution_signal_stale",
        },
    ) == "entry_signal_stale_before_final"
    assert classify_shadow_review_cluster(
        "pass_or_allow_without_pending_entry",
        {
            "component": "lotto_entry_gate",
            "event_type": "entry_gate",
            "decision": "wait",
            "reason": "lotto_timing_negative_m5",
        },
    ) == "momentum_fading_or_negative_trend"
    assert classify_shadow_review_cluster(
        "pending_without_final_entry_contract",
        {
            "component": "smart_entry",
            "event_type": "timing_decision",
            "decision": "reject",
            "reason": "dead_cat_below_high_15.7pct_gt_10.0pct",
        },
    ) == "dead_cat_bounce_timing_reject"
    assert reason_level_shadow_action(
        stage="pending_without_final_entry_contract",
        component="smart_entry",
        event_type="timing_decision",
        decision="reject",
        reason="dead_cat_below_high_15.7pct_gt_10.0pct",
    ) == "track_dead_cat_below_high_reason_shadow_only"
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper_path = root / "paper.db"
        raw_path = root / "raw.db"
        paper = sqlite3.connect(paper_path)
        paper.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        payload = json.dumps({
            "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
            "source_component": "matrix_evaluator",
            "markov_bucket": "insufficient",
            "source_quote_clean": True,
            "source_quote_executable": False,
            "context_schema_version": "candidate-shadow-context-v2.no_signal_price_quote_inference",
        })
        for sig, token, cand, family, matched in [
            ("101", "QT1", "current_all", "baseline", 1),
            ("101", "QT1", "kline:active_mom20_first3", "kline", 1),
            ("102", "QT2", "current_all", "baseline", 1),
            ("102", "QT2", "entry_mode_registry:pullback_tiny_scout", "entry_mode_registry", 1),
            ("103", "OK", "current_all", "baseline", 1),
            ("103", "OK", "kline:active_mom20_first3", "kline", 1),
        ]:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (sig, token, now - 100, cand, family, matched, "self_test", now - 50, payload if cand == "current_all" else "{}"),
            )
        paper.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER, event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT,
              lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT, decision TEXT,
              route TEXT, data_source TEXT, lifecycle_state TEXT, payload_json TEXT
            )
            """
        )
        paper.executemany(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    1,
                    now - 80,
                    "101",
                    "QT1",
                    "QT1",
                    "lc1",
                    "smart_entry",
                    "quality_score_low",
                    "quality_gate",
                    "REJECT",
                    None,
                    None,
                    None,
                    json.dumps({"signal_price": 0.00000101, "quote_age_sec": 12}),
                ),
                (2, now - 70, "102", "QT2", "QT2", "lc2", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (
                    3,
                    now - 65,
                    "102",
                    "QT2",
                    "QT2",
                    "lc2",
                    "scout_quality",
                    "timing_too_late",
                    "timing_decision",
                    "REJECT",
                    None,
                    None,
                    None,
                    json.dumps({"current_price": 0.00000102, "quote_age_sec": 18}),
                ),
                (4, now - 60, "103", "OK", "OK", "lc3", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (5, now - 55, "103", "OK", "OK", "lc3", "entry_engine", "pending", "pending_entry", "PENDING", None, None, None, "{}"),
                (6, now - 50, "103", "OK", "OK", "lc3", "final_entry_contract", "mode_disabled", "entry_block", "BLOCK", None, None, None, "{}"),
                (
                    7,
                    now - 75,
                    "201",
                    "DUD1",
                    "DUD1",
                    "lc4",
                    "smart_entry",
                    "quality_score_low",
                    "quality_gate",
                    "REJECT",
                    None,
                    None,
                    None,
                    json.dumps({"signal_price": 0.00000201, "quote_age_sec": 7}),
                ),
                (
                    8,
                    now - 74,
                    "202",
                    "DUD2",
                    "DUD2",
                    "lc5",
                    "entry_execution_eligibility",
                    "entry_execution_signal_stale",
                    "entry_block",
                    "WATCH_ONLY",
                    None,
                    None,
                    None,
                    json.dumps({"trigger_price": 0.00000202, "quote_age_sec": 9}),
                ),
            ],
        )
        paper.commit()
        paper.close()

        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, source TEXT, observation_status TEXT,
              kline_covered INTEGER, coverage_reason TEXT, baseline_confidence TEXT,
              same_source_path INTEGER, outlier_flag INTEGER, outlier_reason TEXT,
              sustained_evaluable INTEGER, sustained_reason TEXT, raw_sustained_tier TEXT,
              raw_primary_tier TEXT, max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
              time_to_sustained_peak_sec REAL, did_enter INTEGER, entered_before_peak INTEGER,
              held_to_silver INTEGER, held_to_gold INTEGER, raw_dog_entered INTEGER,
              raw_dog_realized INTEGER, sold_before_silver INTEGER, sold_before_gold INTEGER,
              exit_reason TEXT, payload_json TEXT, source_kind TEXT, source_family TEXT
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("101", "QT1", "QT1", now - 100, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 90, 95, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("102", "QT2", "QT2", now - 90, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "gold", "gold", 300, 320, 120, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("103", "OK", "OK", now - 80, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 110, 120, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
            ],
        )
        raw.commit()
        raw.close()

        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            raw_funnel=None,
            hours=1,
            expected_candidates=2,
            now_ts=now,
            limit=10,
            out=None,
            compact=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["strategy_change_allowed"] is False
        assert report["classification"] == "QUALITY_TIMING_REJECT_RESEARCH_READY"
        assert report["verdict"] == "QUALITY_TIMING_REJECT_RESEARCH_READY"
        assert report["next_action"] in {
            "track_score_quality_threshold_false_negative_shadow_probe",
            "track_newborn_pullback_timing_false_negative_shadow_probe",
            "continue_reason_level_shadow_review",
            "review_other_quality_timing_reason_breakout_shadow_only",
            "review_dominant_quality_timing_reason_breakout_shadow_only",
        }
        assert report["dominant_cluster"] is not None
        assert report["quality_timing_reject_event_rows"] == 2
        assert report["upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"] == 1.0
        assert report["would_all_quality_timing_resolution_reach_60pct_upper_bound"] is True
        assert report["top_quality_timing_clusters"]
        reason_breakout = report["reason_level_breakout"]
        assert reason_breakout["classification"] == "QUALITY_TIMING_REASON_LEVEL_READY"
        assert reason_breakout["dominant_cluster_top_reasons"]
        assert reason_breakout["promotion_allowed"] is False
        assert report["summary"]["reason_level_breakout"]["classification"] == "QUALITY_TIMING_REASON_LEVEL_READY"
        assert report["allowed_scope"]
        assert report["summary"]["promotion_allowed"] is False
        assert report["summary"]["automatic_runtime_change_allowed"] is False
        assert report["summary"]["paper_enablement_allowed"] is False
        assert report["denominator"]["quality_timing_reject_event_rows"] == 2
        assert report["denominator"]["dud_inclusive_reject_denominator_available"] is True
        reject_instrumentation = report["reject_instrumentation"]
        assert reject_instrumentation["reject_ts_coverage_rate"] == 1.0
        assert reject_instrumentation["price_at_reject_coverage_rate"] == 1.0
        assert reject_instrumentation["quote_age_at_reject_coverage_rate"] == 1.0
        assert reject_instrumentation["peak_vs_reject_ordering_coverage_rate"] == 1.0
        assert reject_instrumentation["peak_vs_reject_ordering_counts"][0]["count"] == 2
        counterfactuals = report["reject_counterfactuals"]
        assert counterfactuals["dud_inclusive_denominator"]["all_quality_timing_reject_events"] >= 4
        assert counterfactuals["per_reason_denominators"]
        quality_reason = next(
            row
            for row in counterfactuals["per_reason_denominators"]
            if row["reason"] == "quality_score_low"
        )
        assert quality_reason["raw_gs_reject_signal_count"] == 1
        assert quality_reason["dud_kills"] == 1
        stale_variant = counterfactuals["shadow_quote_fresh_reanchor_variant"]
        assert stale_variant["allowed_use"] == "watch_only"
        assert stale_variant["would_be_quote_fresh_events"] == 1
        assert stale_variant["promotion_allowed"] is False
        assert report["candidate_match_attribution"]["candidate_matched_any_events"] == 2
        assert report["candidate_match_attribution"]["full_candidate_coverage_rate"] == 1.0
        clean_view = report["blocked_context_dimensions_excluded_view"]
        assert clean_view["classification"] == "CLEAN_CANDIDATE_ATTRIBUTION_READY"
        assert clean_view["blocked_dimensions"] == ["kline", "volume"]
        assert clean_view["clean_candidate_matched_any_events"] == 2
        assert clean_view["blocked_candidate_matched_any_events"] == 1
        assert clean_view["clean_candidate_observation_count"] == 3
        assert clean_view["blocked_candidate_observation_count"] == 1
        assert any(
            row["candidate_id"] == "entry_mode_registry:pullback_tiny_scout"
            for row in clean_view["top_clean_candidates"]
        )
        assert all(
            not row["blocked_context_dimensions"]
            for row in clean_view["top_clean_candidates"]
        )
        assert any(
            row["candidate_id"] == "kline:active_mom20_first3"
            and "kline" in row["blocked_context_dimensions"]
            for row in clean_view["top_blocked_candidates"]
        )
        impact = report["readiness_impact_upper_bound"]
        assert impact["current_final_entry_contract_signal_count"] == 1
        assert impact["quality_timing_reject_event_rows"] == 2
        assert impact["upper_bound_final_eligibility_count_if_all_quality_timing_resolved"] == 3
        assert impact["would_all_quality_timing_resolution_reach_60pct_upper_bound"] is True
        review = report["shadow_only_review"]
        assert review["classification"] == "QUALITY_TIMING_SHADOW_REVIEW_READY"
        assert review["promotion_allowed"] is False
        assert review["strategy_change_allowed"] is False
        assert review["automatic_runtime_change_allowed"] is False
        assert review["paper_enablement_allowed"] is False
        assert review["research_opportunity_count"] >= 1
        assert review["top_research_opportunities"][0]["promotion_allowed"] is False
        assert review["top_research_opportunities"][0]["readiness_impact_upper_bound"]["promotion_allowed"] is False
        assert review["top_research_opportunities"][0]["reason_level_breakout"]
        assert review["top_research_opportunities"][0]["reason_level_review_status"] == "REASON_LEVEL_READY"
        assert "top_clean_candidates" in review["top_research_opportunities"][0]
        assert "top_blocked_candidates" in review["top_research_opportunities"][0]
        stages = {row["stage"]: row["count"] for row in report["stage_attribution"]["stage_counts"]}
        assert stages["decision_no_pass_or_allow"] == 1
        assert stages["pass_or_allow_without_pending_entry"] == 1
        assert "pending_without_final_entry_contract" not in stages
        compact = compact_summary(report)
        assert compact["promotion_allowed"] is False
        assert compact["reject_instrumentation"]["peak_vs_reject_ordering_coverage_rate"] == 1.0
        assert compact["reject_counterfactuals"]["dud_inclusive_denominator"]["available"] is True
        assert compact["reason_level_breakout"]["classification"] == "QUALITY_TIMING_REASON_LEVEL_READY"
        assert compact["shadow_only_review"]["classification"] == "QUALITY_TIMING_SHADOW_REVIEW_READY"
        assert compact["blocked_context_dimensions_excluded_view"]["classification"] == "CLEAN_CANDIDATE_ATTRIBUTION_READY"
    print("SELF_TEST_PASS quality_timing_reject_research_audit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--raw-funnel", default=None)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--out")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    payload = compact_summary(report) if args.compact else report
    if args.out:
        jdump(args.out, payload)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
