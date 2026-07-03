#!/usr/bin/env python3
"""Read-only post-freeze OOS validation for capture-first 2D cross definitions.

This report validates frozen same-window capture-cross definitions against raw
gold/silver rows that arrived after the freeze timestamp. It is discovery /
readiness evidence only: it never changes strategy, entry policy, gates,
final_entry_contract, A_CLASS, executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    rate,
    signal_id_key,
)
from pass_allow_60_post_freeze_oos_validation import (
    build_post_freeze_source_activity,
    context_value,
    load_json,
    norm_value,
    parse_utc_ts,
    table_exists,
    write_json,
)
from quality_timing_reject_research_audit import (
    classify_shadow_review_cluster,
    stage_quality_timing_events,
)


SCHEMA_VERSION = "capture_cross_post_freeze_oos_validation.v2"
DEFAULT_EXPECTED_CANDIDATES = 84
DEFAULT_MIN_RAW_EVENTS = 10
DEFAULT_MIN_SELECTED_EVENTS = 3
DEFAULT_MIN_UNIQUE_TOKENS = 10
DEFAULT_FDR_Q = 0.1
DEFAULT_NULL_REPLICATES = 64
DEFAULT_SAFETY_SEC = 120

SUPPORTED_STAGES = {
    "detector_capture",
    "decision_capture",
    "pass_allow_capture",
    "pending_capture",
    "final_eligibility",
    "mode_disabled_adjusted_final_eligibility",
    "paper_capture",
    "realized_capture",
}
CAPTURE_CROSS_FREEZE_SOURCES = {
    "capture_first_2d_cross",
    "clean_dimension_2d_capture_cross",
    "quality_timing_reason_cross",
    "shadow_matured_volume_cross",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_from_ts(value):
    if value is None:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(value)))
    except Exception:
        return None


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enter", "would_enter"}


def stable_hash(parts):
    body = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]


def token_key(row):
    token = row.get("token_ca") if isinstance(row, dict) else None
    if token not in (None, ""):
        return f"token:{str(token)}"
    signal_id = row.get("signal_id_key") if isinstance(row, dict) else None
    return f"signal:{signal_id}"


def log_choose(n, k):
    if k < 0 or k > n:
        return float("-inf")
    return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)


def hypergeom_sf(k, population_n, success_n, draw_n):
    """One-sided Fisher/hypergeometric tail P[X >= k]."""
    try:
        k = int(k)
        population_n = int(population_n)
        success_n = int(success_n)
        draw_n = int(draw_n)
    except Exception:
        return None
    if population_n <= 0 or draw_n < 0 or success_n < 0:
        return None
    if draw_n > population_n or success_n > population_n:
        return None
    lo = max(0, draw_n - (population_n - success_n))
    hi = min(draw_n, success_n)
    if k <= lo:
        return 1.0
    if k > hi:
        return 0.0
    denom = log_choose(population_n, draw_n)
    probs = []
    for x in range(k, hi + 1):
        probs.append(math.exp(log_choose(success_n, x) + log_choose(population_n - success_n, draw_n - x) - denom))
    return min(1.0, max(0.0, sum(probs)))


def benjamini_hochberg(p_values):
    indexed = [(idx, p) for idx, p in enumerate(p_values) if p is not None]
    m = len(indexed)
    out = [None for _ in p_values]
    if m <= 0:
        return out
    ranked = sorted(indexed, key=lambda item: item[1])
    prev = 1.0
    for rank_from_end, (idx, p) in enumerate(reversed(ranked), start=1):
        rank = m - rank_from_end + 1
        q = min(prev, float(p) * m / rank)
        q = min(1.0, max(0.0, q))
        out[idx] = q
        prev = q
    return out


def jloads(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    except Exception:
        return {}


def extract_hard_blockers(payload):
    payload = payload if isinstance(payload, dict) else {}
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if isinstance(blockers, str):
        try:
            decoded = json.loads(blockers)
            blockers = decoded
        except Exception:
            blockers = [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if item is not None and str(item)]
    return []


def build_observation_indexes(observations):
    by_signal = defaultdict(list)
    matched_candidates = defaultdict(set)
    context_by_signal = {}
    full_coverage_signals = set()
    candidate_sets = defaultdict(set)
    for row in observations or []:
        key = row.get("signal_id_key")
        if not key:
            continue
        by_signal[key].append(row)
        candidate_id = row.get("candidate_id")
        if candidate_id:
            candidate_sets[key].add(candidate_id)
        if row.get("matched"):
            matched_candidates[key].add(candidate_id)
        if candidate_id == "current_all":
            context_by_signal[key] = row.get("payload") or {}
    return by_signal, matched_candidates, context_by_signal, candidate_sets, full_coverage_signals


def build_stage_sets(paper_db, raw_rows, audits, trades, eval_start_ts, now_ts):
    raw_signal_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    stages = {
        "detector_capture": set(),
        "decision_capture": set(),
        "pass_allow_capture": set(),
        "pending_capture": set(),
        "final_eligibility": set(),
        "mode_disabled_adjusted_final_eligibility": set(),
        "paper_capture": set(),
        "realized_capture": set(),
    }
    for audit in audits or []:
        signal_id = audit.get("signal_id")
        if not signal_id:
            continue
        if int(audit.get("matched_candidate_count") or 0) > 0:
            stages["detector_capture"].add(signal_id)
        if int(audit.get("decision_record_count") or 0) > 0:
            stages["decision_capture"].add(signal_id)
        if int(audit.get("would_enter_count") or 0) > 0:
            stages["pass_allow_capture"].add(signal_id)
        if audit.get("entered") or int(audit.get("paper_trade_count") or 0) > 0:
            stages["paper_capture"].add(signal_id)
        if audit.get("raw_dog_realized"):
            stages["realized_capture"].add(signal_id)

    for trade in trades or []:
        signal_id = trade.get("signal_id_key")
        if signal_id:
            stages["paper_capture"].add(signal_id)

    if table_exists(paper_db, "paper_decision_events") and raw_signal_ids:
        rows = paper_db.execute(
            """
            SELECT event_ts, signal_id, component, event_type, decision, reason, payload_json
            FROM paper_decision_events
            WHERE event_ts >= ? AND event_ts <= ?
              AND signal_id IS NOT NULL
            """,
            (eval_start_ts - 60, now_ts + 900),
        ).fetchall()
        for row in rows:
            signal_id = signal_id_key(row["signal_id"])
            if signal_id not in raw_signal_ids:
                continue
            event_type = str(row["event_type"] or "").lower()
            decision = str(row["decision"] or "").upper()
            component = str(row["component"] or "")
            stages["decision_capture"].add(signal_id)
            if decision in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"} or event_type in {"would_enter", "enter"}:
                stages["pass_allow_capture"].add(signal_id)
            if event_type == "pending_entry":
                stages["pending_capture"].add(signal_id)
            if component == "final_entry_contract":
                stages["final_eligibility"].add(signal_id)
                blockers = extract_hard_blockers(jloads(row["payload_json"]))
                non_mode_blockers = [blocker for blocker in blockers if blocker != "mode_disabled"]
                if not non_mode_blockers:
                    stages["mode_disabled_adjusted_final_eligibility"].add(signal_id)
    return stages


def build_oos_data_availability(raw_count, min_raw_events, observation_meta, source_activity):
    all_raw_since_freeze = (source_activity or {}).get("all_raw_rows_since_eval_start")
    root_causes = []
    if all_raw_since_freeze == 0:
        root_causes.append("no_post_freeze_raw_signal_rows")
    if raw_count == 0:
        root_causes.append("no_post_freeze_raw_gold_silver_events")
    elif raw_count < min_raw_events:
        root_causes.append("post_freeze_raw_gold_silver_event_rows_below_min")
    if raw_count > 0 and not (observation_meta or {}).get("available"):
        root_causes.append("candidate_observations_unavailable_for_post_freeze_signal_ids")
    candidate_observation_effective_status = (
        "not_applicable_no_raw_signal_ids"
        if raw_count == 0
        else "available"
        if (observation_meta or {}).get("available")
        else "unavailable"
    )
    classification = (
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS"
        if all_raw_since_freeze == 0
        else "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        if raw_count == 0
        else "OOS_DATA_BELOW_MIN_RAW_EVENTS"
        if raw_count < min_raw_events
        else "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        if candidate_observation_effective_status == "unavailable"
        else "OOS_DATA_AVAILABLE_FOR_JUDGMENT"
    )
    next_action = (
        "wait_for_post_freeze_raw_signal_rows"
        if classification == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS"
        else "continue_collecting_post_freeze_raw_gold_silver_events"
        if classification == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        else "continue_collecting_until_min_oos_raw_events"
        if classification == "OOS_DATA_BELOW_MIN_RAW_EVENTS"
        else "inspect_post_freeze_candidate_observation_join"
        if classification == "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        else "judge_capture_cross_post_freeze_oos_repeat_evidence"
    )
    return {
        "classification": classification,
        "root_causes": root_causes,
        "raw_gold_silver_event_rows": raw_count,
        "min_raw_events_for_oos_judgment": min_raw_events,
        "minimum_raw_gold_silver_event_rows": min_raw_events,
        "minimum_raw_gold_silver_event_rows_for_oos_judgment": min_raw_events,
        "raw_gold_silver_event_rows_needed_for_min": max(0, int(min_raw_events) - int(raw_count or 0)),
        "raw_gold_silver_event_rows_needed_to_minimum": max(0, int(min_raw_events) - int(raw_count or 0)),
        "all_raw_rows_since_eval_start": all_raw_since_freeze,
        "raw_signal_rows_seen_after_freeze": (
            None if all_raw_since_freeze is None else int(all_raw_since_freeze or 0)
        ),
        "post_freeze_source_activity": source_activity or {},
        "candidate_observation_meta": observation_meta or {},
        "candidate_observation_effective_status": candidate_observation_effective_status,
        "next_action": next_action,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def signal_stage_rates(signal_ids, stages):
    selected = set(signal_ids or [])
    denom = len(selected)
    out = {}
    for stage in sorted(SUPPORTED_STAGES):
        count = len(selected & (stages.get(stage) or set()))
        out[f"{stage}_count"] = count
        out[f"{stage}_rate"] = rate(count, denom)
    return out


def quality_reason_signature(event):
    if not event:
        return None
    stage, component, event_type, decision, reason = (list(event.get("reason_key") or []) + [None] * 5)[:5]
    return "|".join(str(part or "UNKNOWN") for part in [stage, component, event_type, decision, reason])


def quality_timing_value(event, dimension):
    if not event:
        return None
    if dimension == "quality_timing_cluster":
        return classify_shadow_review_cluster(event.get("stage"), event.get("attribution"))
    if dimension == "quality_timing_reason":
        return quality_reason_signature(event)
    return None


def self_cross_reason(candidate_id, dimension):
    """Detect mechanically-defined candidate x context crosses.

    This is intentionally conservative. It only excludes dimensions that are
    visibly part of the candidate definition, so the statistics panel does not
    treat tautological gates as independent evidence.
    """
    candidate = str(candidate_id or "").lower()
    dim = str(dimension or "").lower()
    if not candidate or not dim:
        return None
    if dim in {"source_quote_clean", "source_quote_executable", "quote_clean", "quote_executable"}:
        if "quote" in candidate or "executable" in candidate:
            return "candidate_definition_contains_quote_dimension"
    if dim in {"market_cap_bucket", "mc_bucket", "market_cap"}:
        if "mc_" in candidate or "market_cap" in candidate or "_mc" in candidate:
            return "candidate_definition_contains_market_cap_dimension"
    if dim in {"volume_profile", "matured_volume_profile"}:
        if "volume" in candidate or "lowvol" in candidate:
            return "candidate_definition_contains_volume_dimension"
    if dim in {"candle_pattern", "kline_profile", "fbr_time_legal"}:
        if candidate.startswith("kline:") or "fbr" in candidate or "first_bar" in candidate:
            return "candidate_definition_contains_kline_dimension"
    if dim == "lifecycle_profile":
        if candidate.startswith("lifecycle:") or "lifecycle" in candidate:
            return "candidate_definition_contains_lifecycle_dimension"
    if dim == "source_component":
        if candidate.startswith("source:") or candidate.startswith("source_"):
            return "candidate_definition_contains_source_dimension"
    return None


def matured_volume_profile(bars):
    vols = [float(bar.get("volume") or 0) for bar in bars]
    if len(vols) < 3:
        return "unknown"
    if vols[-1] > max(vols[:-1]) * 1.8:
        return "climax"
    if all(vols[i] <= vols[i + 1] for i in range(len(vols) - 1)):
        return "building"
    if all(vols[i] >= vols[i + 1] for i in range(len(vols) - 1)):
        return "declining"
    if max(vols) <= 0:
        return "flat"
    if (max(vols) - min(vols)) / max(vols) < 0.2:
        return "flat"
    return "mixed"


def load_matured_volume_contexts(kline_db_path, raw_rows, limit=125):
    path = Path(kline_db_path or "")
    if not path.exists() or not raw_rows:
        return {}, {
            "available": False,
            "reason": "kline_db_missing" if not path.exists() else "no_raw_rows",
        }
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    contexts = {}
    try:
        if not table_exists(db, "kline_1m"):
            return {}, {"available": False, "reason": "kline_1m_table_missing"}
        for row in raw_rows:
            signal_id = row.get("signal_id_key")
            token = row.get("token_ca")
            signal_ts = row.get("signal_ts_norm") or row.get("signal_ts")
            if not signal_id or not token or signal_ts is None:
                continue
            try:
                bars = db.execute(
                    """
                    SELECT timestamp, open, high, low, close, volume
                    FROM kline_1m
                    WHERE token_ca = ? AND timestamp >= ?
                    ORDER BY timestamp ASC
                    LIMIT ?
                    """,
                    (token, int(signal_ts), int(limit)),
                ).fetchall()
            except sqlite3.Error:
                bars = []
            first = [dict(item) for item in bars[:5]]
            contexts[signal_id] = {
                "matured_volume_profile": matured_volume_profile(first),
                "matured_kline_bar_count": len(bars),
                "matured_volume_available": len(first) >= 3,
            }
    finally:
        db.close()
    known = sum(1 for row in contexts.values() if row.get("matured_volume_profile") != "unknown")
    return contexts, {
        "available": True,
        "raw_signal_count": len({row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}),
        "context_count": len(contexts),
        "known_count": known,
        "known_rate": rate(known, len(contexts)),
    }


def selected_status(selected_count, selected_stage_rate, global_stage_rate, min_selected_events):
    lift = (
        None
        if selected_stage_rate is None or global_stage_rate is None
        else round(float(selected_stage_rate) - float(global_stage_rate), 6)
    )
    if selected_count < min_selected_events:
        verdict = "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"
    elif lift is not None and lift > 0:
        verdict = "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
    else:
        verdict = "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT"
    return lift, verdict


def validate_capture_cross_item(
    item,
    raw_rows,
    matched_candidates,
    context_by_signal,
    matured_volume_by_signal,
    quality_timing_by_signal,
    stages,
    global_stage_rates,
    min_selected_events,
    safety_sec,
):
    definition = item.get("freeze_definition") or {}
    candidate_id = definition.get("candidate_id")
    dimension = definition.get("dimension")
    slice_value = definition.get("slice_value")
    expected_stage = (
        item.get("expected_capture_stage_improved")
        or definition.get("expected_capture_stage_improved")
        or "detector_capture"
    )
    item_frozen_ts = parse_utc_ts(item.get("frozen_at") or definition.get("frozen_at"))
    item_eval_start_ts = None if item_frozen_ts is None else int(item_frozen_ts) + int(safety_sec)
    selected = []
    selected_tokens = set()
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id:
            continue
        if item_eval_start_ts is not None:
            signal_ts = row.get("signal_ts_norm") or row.get("signal_ts")
            if signal_ts is None or int(signal_ts) < item_eval_start_ts:
                continue
        if candidate_id and candidate_id not in matched_candidates.get(signal_id, set()):
            continue
        if dimension:
            if dimension in {"quality_timing_cluster", "quality_timing_reason"}:
                observed_value = quality_timing_value(
                    (quality_timing_by_signal or {}).get(signal_id),
                    dimension,
                )
            elif dimension == "matured_volume_profile":
                observed_value = context_value(
                    (matured_volume_by_signal or {}).get(signal_id) or {},
                    dimension,
                )
            else:
                observed_value = context_value(context_by_signal.get(signal_id) or {}, dimension)
            if norm_value(observed_value) != norm_value(slice_value):
                continue
        selected.append(signal_id)
        selected_tokens.add(token_key(row))
    selected_rates = signal_stage_rates(selected, stages)
    selected_count = len(selected)
    success_count = (
        selected_rates.get(f"{expected_stage}_count")
        if expected_stage in SUPPORTED_STAGES
        else None
    )
    self_cross = self_cross_reason(candidate_id, dimension)
    if expected_stage not in SUPPORTED_STAGES:
        stage_lift = None
        verdict = "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
    else:
        selected_stage_rate = selected_rates.get(f"{expected_stage}_rate")
        global_stage_rate = global_stage_rates.get(f"{expected_stage}_rate")
        stage_lift, verdict = selected_status(
            selected_count,
            selected_stage_rate,
            global_stage_rate,
            min_selected_events,
        )
    return {
        "freeze_id": item.get("freeze_id"),
        "source": item.get("source"),
        "definition_fingerprint": item.get("definition_fingerprint"),
        "frozen_at": item.get("frozen_at"),
        "validation_role": "capture_first_2d_cross_post_freeze_oos",
        "candidate_id": candidate_id,
        "dimension": dimension,
        "slice_value": slice_value,
        "expected_capture_stage_improved": expected_stage,
        "selected_raw_gold_silver_events": selected_count,
        "expected_stage_rate": (
            selected_rates.get(f"{expected_stage}_rate")
            if expected_stage in SUPPORTED_STAGES
            else None
        ),
        "expected_stage_success_count": success_count,
        "global_expected_stage_rate": (
            global_stage_rates.get(f"{expected_stage}_rate")
            if expected_stage in SUPPORTED_STAGES
            else None
        ),
        "expected_stage_lift_vs_post_freeze_global": stage_lift,
        "selected_stage_rates": selected_rates,
        "global_stage_rates": global_stage_rates,
        "selected_signal_id_count": len(set(selected)),
        "selected_signal_ids_hash": stable_hash(sorted(set(selected))),
        "selected_unique_token_count": len(selected_tokens),
        "selected_token_hash": stable_hash(sorted(selected_tokens)),
        "_selected_signal_ids": sorted(set(selected)),
        "_selected_token_keys": sorted(selected_tokens),
        "item_frozen_at": item.get("frozen_at"),
        "item_eval_start_ts": item_eval_start_ts,
        "item_eval_start_iso": iso_from_ts(item_eval_start_ts),
        "self_cross_excluded": bool(self_cross),
        "self_cross_reason": self_cross,
        "current_window_evidence": item.get("current_window_evidence") or {},
        "verdict": verdict,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def stage_success_tokens(raw_rows, stages, stage):
    successes = set(stages.get(stage) or set())
    tokens = set()
    for row in raw_rows or []:
        signal_id = row.get("signal_id_key")
        if signal_id in successes:
            tokens.add(token_key(row))
    return tokens


def raw_tokens(raw_rows):
    return {token_key(row) for row in (raw_rows or []) if row.get("signal_id_key")}


def family_key_for_item(item):
    return stable_hash({
        "expected_stage": item.get("expected_capture_stage_improved"),
        "selected_signal_ids_hash": item.get("selected_signal_ids_hash"),
        "selected_token_hash": item.get("selected_token_hash"),
    })


def build_family_statistics(
    items,
    raw_rows,
    stages,
    *,
    min_unique_tokens,
    fdr_q,
    null_replicates,
):
    """Collapse definitions into event-set families and run exact/FDR stats.

    The family key intentionally ignores candidate/dimension labels after the
    selected event set has been formed. Identical selected events for the same
    expected stage are one hypothesis, not multiple independent tests.
    """
    global_tokens = raw_tokens(raw_rows)
    family_map = {}
    definition_rows = []
    for item in items or []:
        fid = family_key_for_item(item)
        stage = item.get("expected_capture_stage_improved")
        family = family_map.setdefault(
            fid,
            {
                "family_id": fid,
                "expected_capture_stage_improved": stage,
                "selected_signal_ids": set(item.get("_selected_signal_ids") or []),
                "selected_token_keys": set(item.get("_selected_token_keys") or []),
                "definition_count": 0,
                "definition_ids": [],
                "definition_fingerprints": [],
                "candidate_ids": set(),
                "dimensions": set(),
                "sources": set(),
                "self_cross_definition_count": 0,
                "self_cross_reasons": Counter(),
                "min_item_eval_start_ts": item.get("item_eval_start_ts"),
                "max_item_eval_start_ts": item.get("item_eval_start_ts"),
                "frozen_at_values": set(),
            },
        )
        family["selected_signal_ids"].update(item.get("_selected_signal_ids") or [])
        family["selected_token_keys"].update(item.get("_selected_token_keys") or [])
        family["definition_count"] += 1
        family["definition_ids"].append(item.get("freeze_id"))
        family["definition_fingerprints"].append(item.get("definition_fingerprint"))
        if item.get("candidate_id"):
            family["candidate_ids"].add(item.get("candidate_id"))
        if item.get("dimension"):
            family["dimensions"].add(item.get("dimension"))
        if item.get("source"):
            family["sources"].add(item.get("source"))
        if item.get("self_cross_excluded"):
            family["self_cross_definition_count"] += 1
            family["self_cross_reasons"][item.get("self_cross_reason") or "self_cross"] += 1
        if item.get("item_eval_start_ts") is not None:
            current_min = family.get("min_item_eval_start_ts")
            current_max = family.get("max_item_eval_start_ts")
            ts = int(item.get("item_eval_start_ts"))
            family["min_item_eval_start_ts"] = ts if current_min is None else min(int(current_min), ts)
            family["max_item_eval_start_ts"] = ts if current_max is None else max(int(current_max), ts)
        if item.get("item_frozen_at"):
            family["frozen_at_values"].add(item.get("item_frozen_at"))
        definition_rows.append({
            "freeze_id": item.get("freeze_id"),
            "definition_fingerprint": item.get("definition_fingerprint"),
            "family_id": fid,
            "candidate_id": item.get("candidate_id"),
            "dimension": item.get("dimension"),
            "slice_value": item.get("slice_value"),
            "source": item.get("source"),
            "expected_capture_stage_improved": stage,
            "self_cross_excluded": bool(item.get("self_cross_excluded")),
            "self_cross_reason": item.get("self_cross_reason"),
            "selected_raw_gold_silver_events": item.get("selected_raw_gold_silver_events"),
            "selected_unique_token_count": item.get("selected_unique_token_count"),
            "item_frozen_at": item.get("item_frozen_at"),
            "item_eval_start_iso": item.get("item_eval_start_iso"),
        })

    families = []
    for family in family_map.values():
        stage = family.get("expected_capture_stage_improved")
        selected_tokens = set(family.get("selected_token_keys") or set())
        success_tokens = stage_success_tokens(raw_rows, stages, stage) if stage in SUPPORTED_STAGES else set()
        selected_success_tokens = selected_tokens & success_tokens
        n = len(selected_tokens)
        k = len(selected_success_tokens)
        N = len(global_tokens)
        K = len(success_tokens)
        selected_rate = rate(k, n)
        global_rate = rate(K, N)
        lift = None if selected_rate is None or global_rate is None else round(selected_rate - global_rate, 6)
        all_defs_self_cross = (
            family["definition_count"] > 0
            and family["self_cross_definition_count"] == family["definition_count"]
        )
        p_value = None
        testable = (
            stage in SUPPORTED_STAGES
            and not all_defs_self_cross
            and n >= int(min_unique_tokens)
            and N > 0
            and K > 0
        )
        if testable:
            p_value = hypergeom_sf(k, N, K, n)
        status = (
            "SELF_CROSS_EXCLUDED"
            if all_defs_self_cross
            else "TOO_SMALL_UNIQUE_TOKENS"
            if n < int(min_unique_tokens)
            else "UNSUPPORTED_STAGE"
            if stage not in SUPPORTED_STAGES
            else "TESTABLE"
        )
        families.append({
            "family_id": family["family_id"],
            "expected_capture_stage_improved": stage,
            "definition_count": family["definition_count"],
            "definition_ids": family["definition_ids"],
            "definition_fingerprints": family["definition_fingerprints"],
            "candidate_ids": sorted(family["candidate_ids"]),
            "dimensions": sorted(family["dimensions"]),
            "sources": sorted(family["sources"]),
            "selected_signal_count": len(family["selected_signal_ids"]),
            "selected_event_set_hash": stable_hash(sorted(family["selected_signal_ids"])),
            "unique_token_n": n,
            "selected_success_unique_tokens": k,
            "global_unique_token_n": N,
            "global_success_unique_tokens": K,
            "selected_stage_rate_unique_token": selected_rate,
            "global_stage_rate_unique_token": global_rate,
            "stage_lift_vs_post_freeze_global_unique_token": lift,
            "p_value_one_sided_exact": p_value,
            "q_value_bh_fdr": None,
            "fdr_q_threshold": float(fdr_q),
            "testable": testable,
            "family_status": status,
            "self_cross_definition_count": family["self_cross_definition_count"],
            "self_cross_reasons": dict(family["self_cross_reasons"]),
            "min_item_eval_start_ts": family.get("min_item_eval_start_ts"),
            "min_item_eval_start_iso": iso_from_ts(family.get("min_item_eval_start_ts")),
            "max_item_eval_start_ts": family.get("max_item_eval_start_ts"),
            "max_item_eval_start_iso": iso_from_ts(family.get("max_item_eval_start_ts")),
            "frozen_at_values": sorted(family["frozen_at_values"]),
            "two_window_status": "SECOND_DISJOINT_WINDOW_REQUIRED",
            "oos_confirmed_family": False,
            "promotion_allowed": False,
        })

    p_values = [row.get("p_value_one_sided_exact") if row.get("testable") else None for row in families]
    q_values = benjamini_hochberg(p_values)
    for row, q in zip(families, q_values):
        row["q_value_bh_fdr"] = q
        row["same_window_statistical_hit"] = bool(
            row.get("testable")
            and q is not None
            and q <= float(fdr_q)
            and (row.get("stage_lift_vs_post_freeze_global_unique_token") or 0) > 0
        )
        row["family_verdict"] = (
            "OOS_STAT_HIT_SECOND_WINDOW_REQUIRED"
            if row["same_window_statistical_hit"]
            else row["family_status"]
            if row["family_status"] != "TESTABLE"
            else "NO_STATISTICAL_REPEAT"
        )

    null_panel = build_null_panel(families, global_tokens, stages, raw_rows, fdr_q, null_replicates)
    tested = [row for row in families if row.get("testable")]
    observed_hits = sum(1 for row in tested if row.get("same_window_statistical_hit"))
    return {
        "schema_version": "capture_cross_oos_statistics.v1",
        "family_table": sorted(families, key=lambda row: (not row.get("same_window_statistical_hit"), row.get("q_value_bh_fdr") is None, row.get("q_value_bh_fdr") or 1, row.get("family_id"))),
        "definition_family_map": definition_rows,
        "tested_family_count": len(tested),
        "raw_definition_count": len(items or []),
        "family_count": len(families),
        "deduped_definition_count": max(0, len(items or []) - len(families)),
        "self_cross_excluded_family_count": sum(1 for row in families if row.get("family_status") == "SELF_CROSS_EXCLUDED"),
        "too_small_family_count": sum(1 for row in families if row.get("family_status") == "TOO_SMALL_UNIQUE_TOKENS"),
        "observed_statistical_hit_count": observed_hits,
        "fdr_q_threshold": float(fdr_q),
        "minimum_unique_tokens_per_family": int(min_unique_tokens),
        "multiplicity_budget": {
            "raw_cells_searched": len(items or []),
            "families_after_event_set_dedupe": len(families),
            "families_tested_after_self_cross_and_min_n": len(tested),
            "expected_false_hits_at_q_threshold": round(float(fdr_q) * len(tested), 6),
            "observed_statistical_hits": observed_hits,
        },
        "null_panel": null_panel,
        "two_window_rule": {
            "required": True,
            "current_window_can_only_create_watch": True,
            "confirmed_family_count": 0,
            "reason": "Second disjoint post-freeze OOS window not yet attached to this artifact.",
        },
        "promotion_allowed": False,
    }


def build_null_panel(families, global_tokens, stages, raw_rows, fdr_q, null_replicates):
    testable = [row for row in families if row.get("testable")]
    if not testable:
        return {
            "available": True,
            "method": "deterministic_label_shuffle",
            "replicates": 0,
            "null_repeat_rate": 0.0,
            "repeat_counts": [],
            "promotion_allowed": False,
        }
    token_list = sorted(global_tokens)
    token_count = len(token_list)
    by_stage_success_count = {
        stage: len(stage_success_tokens(raw_rows, stages, stage))
        for stage in SUPPORTED_STAGES
    }
    repeat_counts = []
    for replicate in range(int(null_replicates)):
        p_values = []
        rows = []
        rng = random.Random(f"capture-cross-null-v1:{replicate}:{token_count}")
        shuffled_success = {}
        for stage, success_count in by_stage_success_count.items():
            if success_count <= 0:
                shuffled_success[stage] = set()
            else:
                shuffled_success[stage] = set(rng.sample(token_list, min(success_count, token_count)))
        for row in testable:
            stage = row.get("expected_capture_stage_improved")
            n = int(row.get("unique_token_n") or 0)
            # Random-candidate null: keep the observed family size and stage
            # prevalence, but assign a deterministic random token set.
            selected_rng = random.Random(f"capture-cross-null-family:{replicate}:{row.get('family_id')}")
            selected_tokens = set(selected_rng.sample(token_list, min(n, token_count))) if n > 0 else set()
            success_tokens = shuffled_success.get(stage) or set()
            k = len(selected_tokens & success_tokens)
            p_values.append(hypergeom_sf(k, token_count, len(success_tokens), len(selected_tokens)))
            rows.append({
                "stage_lift": (rate(k, len(selected_tokens)) or 0) - (rate(len(success_tokens), token_count) or 0),
            })
        q_values = benjamini_hochberg(p_values)
        repeat_count = 0
        for row, q in zip(rows, q_values):
            if q is not None and q <= float(fdr_q) and row.get("stage_lift", 0) > 0:
                repeat_count += 1
        repeat_counts.append(repeat_count)
    denominator = max(1, len(testable) * int(null_replicates))
    return {
        "available": True,
        "method": "deterministic_label_shuffle_holdout_negative_controls_compatible",
        "holdout_negative_controls_reused_as": "discovery_null_panel_shape",
        "replicates": int(null_replicates),
        "tested_family_count": len(testable),
        "repeat_counts": repeat_counts,
        "null_repeat_rate": round(sum(repeat_counts) / denominator, 6),
        "max_null_repeat_count": max(repeat_counts) if repeat_counts else 0,
        "mean_null_repeat_count": round(sum(repeat_counts) / max(1, len(repeat_counts)), 6),
        "promotion_allowed": False,
    }


def classify_items(raw_count, items, min_raw_events):
    if raw_count < min_raw_events:
        return "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"
    supported = [
        row for row in items
        if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
    ]
    if not supported:
        return "CAPTURE_CROSS_POST_FREEZE_NO_SUPPORTED_DEFINITIONS"
    repeated = [row for row in supported if row.get("verdict") == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"]
    sufficient = [row for row in supported if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"]
    if repeated:
        return "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
    if sufficient:
        return "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT"
    return "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"


def refine_classification(base_classification, oos_data_availability):
    if base_classification != "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL":
        return base_classification
    mapping = {
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS"
        ),
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
        ),
        "OOS_DATA_BELOW_MIN_RAW_EVENTS": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS"
        ),
        "OOS_DATA_OBSERVATION_JOIN_BLOCKED": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED"
        ),
    }
    return mapping.get((oos_data_availability or {}).get("classification"), base_classification)


def next_action_for_classification(classification, oos_data_availability):
    if classification in {
        "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS",
        "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER",
        "CAPTURE_CROSS_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS",
        "CAPTURE_CROSS_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED",
    }:
        return (oos_data_availability or {}).get("next_action") or "continue_collecting_capture_cross_oos_window"
    if classification == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH":
        return "review_repeated_capture_cross_oos_evidence_without_promotion"
    if classification == "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT":
        return "keep_frozen_capture_cross_definitions_watch_only_or_retire_if_repeated_no_repeat"
    return "continue_collecting_capture_cross_oos_window"


def build_report(args):
    registry = load_json(args.freeze_registry, {})
    frozen_at = registry.get("definition_set_frozen_at") or registry.get("generated_at")
    frozen_ts = parse_utc_ts(frozen_at)
    if frozen_ts is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "capture_cross_post_freeze_oos_validation",
            "generated_at": utc_now(),
            "classification": "CAPTURE_CROSS_POST_FREEZE_FREEZE_TS_MISSING",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": [],
        }
    registry_items = [
        item for item in (registry.get("items") or [])
        if isinstance(item, dict) and item.get("source") in CAPTURE_CROSS_FREEZE_SOURCES
    ]
    item_eval_starts = []
    for item in registry_items:
        item_frozen_ts = parse_utc_ts(item.get("frozen_at") or (item.get("freeze_definition") or {}).get("frozen_at"))
        if item_frozen_ts is not None:
            item_eval_starts.append(int(item_frozen_ts) + int(args.safety_sec))
    definition_set_eval_start_ts = int(frozen_ts) + int(args.safety_sec)
    eval_start_ts = min(item_eval_starts or [definition_set_eval_start_ts])
    now_ts = int(time.time())
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        post_freeze_source_activity = build_post_freeze_source_activity(raw_db, eval_start_ts, now_ts)
        raw_rows = load_raw_dogs(raw_db, eval_start_ts)
        raw_rows = [
            row for row in raw_rows
            if row.get("signal_ts_norm") is not None
            and row.get("signal_ts_norm") >= eval_start_ts
            and row.get("signal_ts_norm") <= now_ts
        ]
        raw_signal_ids, tokens, _by_signal, _by_token = make_raw_indexes(raw_rows)
        observations, observation_meta = load_candidate_observations(paper_db, raw_signal_ids, eval_start_ts)
        decisions = load_paper_decisions(paper_db, tokens, eval_start_ts, now_ts)
        trades = load_paper_trades(paper_db, tokens, eval_start_ts, now_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, int(args.expected_candidates))
        stages = build_stage_sets(paper_db, raw_rows, audits, trades, eval_start_ts, now_ts)
    finally:
        raw_db.close()
        paper_db.close()

    _obs_by_signal, matched_candidates, context_by_signal, candidate_sets, _full = build_observation_indexes(observations)
    quality_timing_by_signal = stage_quality_timing_events(raw_rows, decisions)
    matured_volume_by_signal, matured_volume_meta = load_matured_volume_contexts(
        getattr(args, "kline_db", None),
        raw_rows,
    )
    raw_signal_set = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    observed_signal_set = set(candidate_sets)
    full_coverage_signal_count = sum(
        1
        for signal_id in raw_signal_set
        if len(candidate_sets.get(signal_id) or set()) >= int(args.expected_candidates)
    )
    raw_count = len(raw_rows)
    global_stage_rates = signal_stage_rates(raw_signal_set, stages)
    items = [
        validate_capture_cross_item(
            item,
            raw_rows,
            matched_candidates,
            context_by_signal,
            matured_volume_by_signal,
            quality_timing_by_signal,
            stages,
            global_stage_rates,
            int(args.min_selected_events),
            int(args.safety_sec),
        )
        for item in registry_items
    ]
    oos_statistics = build_family_statistics(
        items,
        raw_rows,
        stages,
        min_unique_tokens=int(args.min_unique_tokens),
        fdr_q=float(args.fdr_q),
        null_replicates=int(args.null_replicates),
    )
    status_counts = Counter(row.get("verdict") for row in items)
    repeat_watch = [row for row in items if row.get("verdict") == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"]
    positive_lift = [
        row for row in items
        if row.get("expected_stage_lift_vs_post_freeze_global") is not None
        and row.get("expected_stage_lift_vs_post_freeze_global") > 0
    ]
    post_freeze_usable_hours = round(max(0, now_ts - eval_start_ts) / 3600.0, 4)
    oos_data_availability = build_oos_data_availability(
        raw_count,
        int(args.min_raw_events),
        observation_meta,
        post_freeze_source_activity,
    )
    legacy_classification = classify_items(raw_count, items, int(args.min_raw_events))
    classification = refine_classification(legacy_classification, oos_data_availability)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "capture_cross_post_freeze_oos_validation",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "evidence_level": "post_freeze_oos_readiness_probe",
        "usage": "read_only_validation_only",
        "classification": classification,
        "legacy_classification": legacy_classification,
        "oos_data_availability_classification": oos_data_availability.get("classification"),
        "oos_data_root_causes": oos_data_availability.get("root_causes") or [],
        "next_action": next_action_for_classification(classification, oos_data_availability),
        "freeze_registry_available": bool(registry),
        "definition_set_frozen_at": registry.get("definition_set_frozen_at"),
        "freeze_generated_at": registry.get("generated_at"),
        "definition_set_eval_start_ts": definition_set_eval_start_ts,
        "definition_set_eval_start_iso": iso_from_ts(definition_set_eval_start_ts),
        "earliest_item_eval_start_ts": eval_start_ts,
        "earliest_item_eval_start_iso": iso_from_ts(eval_start_ts),
        "eval_start_ts": eval_start_ts,
        "eval_start_iso": iso_from_ts(eval_start_ts),
        "now_ts": now_ts,
        "post_freeze_usable_hours": post_freeze_usable_hours,
        "post_freeze_safety_sec": int(args.safety_sec),
        "raw_gold_silver_event_rows": raw_count,
        "raw_gold_silver_rows_since_eval_start_unfiltered": (
            post_freeze_source_activity.get("raw_gold_silver_rows_since_eval_start_unfiltered")
        ),
        "all_raw_rows_since_eval_start": post_freeze_source_activity.get("all_raw_rows_since_eval_start"),
        "latest_raw_signal_age_sec": post_freeze_source_activity.get("latest_raw_signal_age_sec"),
        "latest_raw_gold_silver_age_sec": (
            post_freeze_source_activity.get("latest_raw_gold_silver_age_sec")
        ),
        "latest_raw_gold_silver_lag_sec_before_eval_start": (
            post_freeze_source_activity.get("latest_raw_gold_silver_lag_sec_before_eval_start")
        ),
        "min_raw_events_for_oos_judgment": int(args.min_raw_events),
        "minimum_raw_gold_silver_event_rows": int(args.min_raw_events),
        "minimum_raw_gold_silver_event_rows_for_oos_judgment": int(args.min_raw_events),
        "raw_gold_silver_event_rows_needed_for_min": (
            oos_data_availability.get("raw_gold_silver_event_rows_needed_for_min")
        ),
        "raw_gold_silver_event_rows_needed_to_minimum": (
            oos_data_availability.get("raw_gold_silver_event_rows_needed_for_min")
        ),
        "raw_signal_rows_seen_after_freeze": oos_data_availability.get("raw_signal_rows_seen_after_freeze"),
        "candidate_observation_meta": observation_meta,
        "candidate_observation_effective_status": (
            oos_data_availability.get("candidate_observation_effective_status")
        ),
        "candidate_observation_join_blocked": (
            oos_data_availability.get("classification") == "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        ),
        "post_freeze_oos_wait_reason": oos_data_availability.get("classification"),
        "oos_data_next_action": oos_data_availability.get("next_action"),
        "oos_data_availability": oos_data_availability,
        "post_freeze_source_activity": post_freeze_source_activity,
        "post_freeze_matured_volume_context": matured_volume_meta,
        "post_freeze_global_stage_rates": global_stage_rates,
        "post_freeze_signal_observation_coverage": {
            "raw_signal_count": len(raw_signal_set),
            "observed_signal_count": len(observed_signal_set & raw_signal_set),
            "observed_signal_rate": rate(len(observed_signal_set & raw_signal_set), len(raw_signal_set)),
            "full_candidate_coverage_signal_count": full_coverage_signal_count,
            "full_candidate_coverage_signal_rate": rate(full_coverage_signal_count, len(raw_signal_set)),
            "expected_candidates": int(args.expected_candidates),
        },
        "frozen_definition_count": len(registry.get("items") or []),
        "validated_definition_count": len(items),
        "supported_definition_count": sum(
            1 for row in items
            if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
        ),
        "repeat_watch_count": len(repeat_watch),
        "positive_lift_count": len(positive_lift),
        "too_small_definition_count": status_counts.get("CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL", 0),
        "status_counts": dict(status_counts),
        "source_counts": dict(Counter(row.get("source") for row in items)),
        "stage_counts": dict(Counter(row.get("expected_capture_stage_improved") for row in items)),
        "oos_statistics": oos_statistics,
        "family_table": oos_statistics.get("family_table"),
        "definition_family_map": oos_statistics.get("definition_family_map"),
        "null_panel_repeat_rate": (oos_statistics.get("null_panel") or {}).get("null_repeat_rate"),
        "multiplicity_budget": oos_statistics.get("multiplicity_budget"),
        "window_lineage": {
            "definition_set_frozen_at": registry.get("definition_set_frozen_at"),
            "definition_set_eval_start_iso": iso_from_ts(definition_set_eval_start_ts),
            "earliest_item_eval_start_iso": iso_from_ts(eval_start_ts),
            "item_eval_start_policy": "item.frozen_at + safety_sec",
            "post_freeze_safety_sec": int(args.safety_sec),
        },
        "top_repeat_watch_items": sorted(
            repeat_watch,
            key=lambda row: (
                row.get("selected_raw_gold_silver_events") or 0,
                row.get("expected_stage_lift_vs_post_freeze_global") or 0,
            ),
            reverse=True,
        )[:20],
        "items": items,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "This report validates frozen capture-first 2D cross definitions only on post-freeze raw gold/silver rows.",
            "Repeat evidence does not authorize promotion or runtime behavior changes.",
        ],
    }


def create_self_test_inputs(root):
    now = int(time.time())
    frozen_ts = now - 600
    registry_path = root / "capture_cross_freeze.json"
    write_json(registry_path, {
        "schema_version": "capture_cross_oos_freeze_registry.v1",
        "generated_at": iso_from_ts(frozen_ts),
        "definition_set_frozen_at": iso_from_ts(frozen_ts),
        "items": [
            {
                "freeze_id": "capture-cross-1",
                "source": "capture_first_2d_cross",
                "definition_fingerprint": "abc",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "pending_capture",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "source_component",
                    "slice_value": "matrix_evaluator",
                    "expected_capture_stage_improved": "pending_capture",
                },
            },
            {
                "freeze_id": "capture-cross-duplicate-event-set",
                "source": "capture_first_2d_cross",
                "definition_fingerprint": "abc-duplicate",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "pending_capture",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "source_component",
                    "slice_value": "matrix_evaluator",
                    "expected_capture_stage_improved": "pending_capture",
                },
            },
            {
                "freeze_id": "quality-cross-1",
                "source": "quality_timing_reason_cross",
                "definition_fingerprint": "def",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "decision_capture",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "quality_timing_cluster",
                    "slice_value": "matrix_alignment_wait",
                    "expected_capture_stage_improved": "decision_capture",
                },
            },
            {
                "freeze_id": "matured-volume-cross-1",
                "source": "shadow_matured_volume_cross",
                "definition_fingerprint": "ghi",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "pending_capture",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "matured_volume_profile",
                    "slice_value": "building",
                    "expected_capture_stage_improved": "pending_capture",
                },
            }
        ],
    })
    raw_path = root / "raw.db"
    paper_path = root / "paper.db"
    kline_path = root / "kline.db"
    raw = sqlite3.connect(raw_path)
    raw.execute(
        """
        CREATE TABLE raw_signal_outcomes (
          signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
          signal_type TEXT, source TEXT, observation_status TEXT,
          kline_covered INTEGER, coverage_reason TEXT, baseline_confidence TEXT,
          same_source_path INTEGER, outlier_flag INTEGER, outlier_reason TEXT,
          sustained_evaluable INTEGER, sustained_reason TEXT,
          raw_sustained_tier TEXT, raw_primary_tier TEXT,
          max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
          time_to_sustained_peak_sec REAL, did_enter INTEGER,
          entered_before_peak INTEGER, held_to_silver INTEGER, held_to_gold INTEGER,
          raw_dog_entered INTEGER, raw_dog_realized INTEGER,
          sold_before_silver INTEGER, sold_before_gold INTEGER,
          exit_reason TEXT, payload_json TEXT, source_kind TEXT, source_family TEXT
        )
        """
    )
    raw.executemany(
        """
        INSERT INTO raw_signal_outcomes VALUES (
          ?, ?, ?, ?, 'premium', 'selftest', 'matured', 1, NULL, 'high', 1, 0,
          NULL, 1, NULL, ?, ?, 150.0, 160.0, 300, 0, 0, 0, 0, 0, 0, 0, 0,
          NULL, '{}', 'selftest', 'selftest'
        )
        """,
        [
            (101, "token-a", "A", now - 200, "silver", "silver"),
            (102, "token-b", "B", now - 190, "gold", "gold"),
        ],
    )
    raw.commit()
    raw.close()

    paper = sqlite3.connect(paper_path)
    paper.execute(
        """
        CREATE TABLE candidate_shadow_observations (
          signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT,
          family TEXT, matched INTEGER, reason TEXT, observed_at INTEGER,
          payload_json TEXT
        )
        """
    )
    rows = []
    for signal_id, token, component in [
        (101, "token-a", "matrix_evaluator"),
        (102, "token-b", "other_component"),
    ]:
        rows.append((
            signal_id, token, now - 200, "current_all", "base", 1, "baseline",
            now - 180, json.dumps({"source_component": component}),
        ))
        rows.append((
            signal_id, token, now - 200, "notath_quote_clean", "base", 1,
            "matched", now - 180, None,
        ))
    paper.executemany(
        "INSERT INTO candidate_shadow_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    paper.execute(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY, event_ts INTEGER, signal_id INTEGER, token_ca TEXT,
          symbol TEXT, lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT,
          decision TEXT, route TEXT, data_source TEXT, lifecycle_state TEXT,
          payload_json TEXT
        )
        """
    )
    paper.executemany(
        """
        INSERT INTO paper_decision_events
        (event_ts, signal_id, token_ca, symbol, lifecycle_id, component, reason,
         event_type, decision, route, data_source, lifecycle_state, payload_json)
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, 'selftest', NULL, ?)
        """,
        [
            (now - 180, 101, "token-a", "A", "entry_engine", "selftest pass", "decision", "PASS", "{}"),
            (now - 170, 101, "token-a", "A", "entry_engine", "pending", "pending_entry", "PENDING", "{}"),
            (
                now - 160, 101, "token-a", "A", "final_entry_contract",
                "mode disabled", "entry_block", "BLOCK", json.dumps({"hard_blockers": ["mode_disabled"]}),
            ),
            (
                now - 165, 102, "token-b", "B", "matrix_evaluator",
                "lotto_timing_negative_m5", "timing_decision", "REJECT", "{}",
            ),
        ],
    )
    paper.commit()
    paper.close()
    kline = sqlite3.connect(kline_path)
    kline.execute(
        """
        CREATE TABLE kline_1m (
          token_ca TEXT, timestamp INTEGER, open REAL, high REAL, low REAL,
          close REAL, volume REAL
        )
        """
    )
    kline.executemany(
        "INSERT INTO kline_1m VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("token-a", now - 200, 1.0, 1.1, 0.9, 1.0, 10.0),
            ("token-a", now - 140, 1.0, 1.1, 0.9, 1.0, 20.0),
            ("token-a", now - 80, 1.0, 1.1, 0.9, 1.0, 30.0),
        ],
    )
    kline.commit()
    kline.close()
    return raw_path, paper_path, registry_path, kline_path


def run_self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path, paper_path, registry_path, kline_path = create_self_test_inputs(root)
        out = root / "out.json"
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            kline_db=str(kline_path),
            freeze_registry=str(registry_path),
            out=str(out),
            expected_candidates=2,
            safety_sec=120,
            min_raw_events=1,
            min_selected_events=1,
            min_unique_tokens=1,
            fdr_q=0.1,
            null_replicates=8,
        )
        payload = build_report(args)
        write_json(out, payload)
        assert payload["schema_version"] == SCHEMA_VERSION
        assert payload["promotion_allowed"] is False
        assert payload["raw_gold_silver_event_rows"] == 2
        assert payload["oos_data_availability_classification"] == "OOS_DATA_AVAILABLE_FOR_JUDGMENT"
        assert payload["validated_definition_count"] == 4
        assert payload["repeat_watch_count"] == 3
        assert payload["source_counts"]["capture_first_2d_cross"] == 2
        assert payload["source_counts"]["quality_timing_reason_cross"] == 1
        assert payload["source_counts"]["shadow_matured_volume_cross"] == 1
        assert payload["oos_statistics"]["schema_version"] == "capture_cross_oos_statistics.v1"
        assert payload["oos_statistics"]["raw_definition_count"] == 4
        assert payload["oos_statistics"]["family_count"] == 2
        assert payload["oos_statistics"]["deduped_definition_count"] == 2
        assert payload["oos_statistics"]["tested_family_count"] >= 1
        assert payload["oos_statistics"]["null_panel"]["available"] is True
        assert payload["oos_statistics"]["null_panel"]["replicates"] == 8
        assert payload["multiplicity_budget"]["families_after_event_set_dedupe"] == 2
        assert payload["window_lineage"]["item_eval_start_policy"] == "item.frozen_at + safety_sec"
        assert payload["post_freeze_matured_volume_context"]["known_count"] == 1
        assert payload["classification"] == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
        assert payload["post_freeze_global_stage_rates"]["decision_capture_rate"] == 1.0
        assert payload["post_freeze_global_stage_rates"]["pending_capture_rate"] == 0.5
        item = payload["items"][0]
        assert item["selected_raw_gold_silver_events"] == 1
        assert item["selected_stage_rates"]["pending_capture_rate"] == 1.0
        assert item["expected_stage_lift_vs_post_freeze_global"] == 0.5
        assert item["verdict"] == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
        quality_item = [
            row for row in payload["items"]
            if row.get("source") == "quality_timing_reason_cross"
        ][0]
        assert quality_item["selected_raw_gold_silver_events"] == 1
        assert quality_item["dimension"] == "quality_timing_cluster"
        matured_volume_item = [
            row for row in payload["items"]
            if row.get("source") == "shadow_matured_volume_cross"
        ][0]
        assert matured_volume_item["selected_raw_gold_silver_events"] == 1
        assert matured_volume_item["dimension"] == "matured_volume_profile"
        assert matured_volume_item["expected_stage_lift_vs_post_freeze_global"] == 0.5
        assert matured_volume_item["verdict"] == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
        assert out.exists()
        availability = build_oos_data_availability(
            0,
            1,
            {"available": False},
            {"available": True, "all_raw_rows_since_eval_start": 7},
        )
        assert availability["classification"] == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        assert refine_classification(
            "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL",
            availability,
        ) == "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
    print("self-test passed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument(
        "--freeze-registry",
        default="/app/data/agent_runs/latest/capture_cross_oos_freeze_registry.json",
    )
    parser.add_argument(
        "--out",
        default="/app/data/agent_runs/latest/capture_cross_post_freeze_oos_validation.json",
    )
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--safety-sec", type=int, default=DEFAULT_SAFETY_SEC)
    parser.add_argument("--min-raw-events", type=int, default=DEFAULT_MIN_RAW_EVENTS)
    parser.add_argument("--min-selected-events", type=int, default=DEFAULT_MIN_SELECTED_EVENTS)
    parser.add_argument("--min-unique-tokens", type=int, default=DEFAULT_MIN_UNIQUE_TOKENS)
    parser.add_argument("--fdr-q", type=float, default=DEFAULT_FDR_Q)
    parser.add_argument("--null-replicates", type=int, default=DEFAULT_NULL_REPLICATES)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    payload = build_report(args)
    write_json(args.out, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
