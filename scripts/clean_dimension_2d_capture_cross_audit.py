#!/usr/bin/env python3
"""Read-only clean-dimension 2D capture cross audit.

This report compares raw gold/silver events that reached decision/pass/pending
/final eligibility against those that did not, using only clean or explicitly
shadow-only dimensions. It is discovery evidence only and never changes
strategy, gates, final_entry_contract, A_CLASS, executor, wallet, canary, or
risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from capture_cross_post_freeze_oos_validation import (
    SUPPORTED_STAGES,
    build_observation_indexes,
    build_stage_sets,
    context_value,
    signal_stage_rates,
)
from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    rate,
    signal_id_key,
    table_exists,
)
from quality_timing_reject_research_audit import (
    classify_shadow_review_cluster,
    stage_quality_timing_events,
)


SCHEMA_VERSION = "clean_dimension_2d_capture_cross_audit.v1"
DEFAULT_EXPECTED_CANDIDATES = 84
DEFAULT_MIN_RAW_EVENTS = 3
DEFAULT_TOP_LIMIT = 120
BLOCKED_DIMENSIONS = {"volume", "kline"}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path, default=None):
    if not path:
        return default if default is not None else {}
    target = Path(path)
    if not target.exists():
        return default if default is not None else {}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return default if default is not None else {}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def norm_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None or value == "":
        return None
    return str(value).strip()


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if parsed == parsed else default
    except Exception:
        return default


def lift(selected_rate, global_rate):
    if selected_rate is None or global_rate is None:
        return None
    return round(float(selected_rate) - float(global_rate), 6)


def slug(value):
    text = str(value or "unknown").strip()
    cleaned = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in text)
    return cleaned[:120] or "unknown"


def dimension_status(context_eligibility, dimension_group):
    dimensions = (
        (context_eligibility or {}).get("dimension_eligibility")
        or (context_eligibility or {}).get("dimensions")
        or {}
    )
    row = dimensions.get(dimension_group) or {}
    return row.get("status") or "UNKNOWN"


def candidate_blockers(candidate_id, family):
    text = f"{candidate_id or ''} {family or ''}".lower()
    blockers = []
    if "kline:" in text or "first_bar" in text or "fbr_" in text or "candle" in text:
        blockers.append("kline")
    if "volume" in text or "lowvol" in text or "low_vol" in text:
        blockers.append("volume")
    return sorted(set(blockers))


def quality_reason_signature(event):
    if not event:
        return None
    stage, component, event_type, decision, reason = (list(event.get("reason_key") or []) + [None] * 5)[:5]
    return "|".join(str(part or "UNKNOWN") for part in [stage, component, event_type, decision, reason])


def raw_signal_sets(raw_rows):
    return [row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")]


def stage_metric_payload(selected_signals, stages, global_rates):
    selected_set = set(selected_signals or [])
    selected_rates = signal_stage_rates(selected_set, stages)
    payload = {
        "selected_raw_gs_unique_signals": len(selected_set),
    }
    for stage in sorted(SUPPORTED_STAGES):
        selected_rate = selected_rates.get(f"{stage}_rate")
        payload[f"{stage}_count"] = selected_rates.get(f"{stage}_count")
        payload[f"{stage}_rate"] = selected_rate
        payload[f"{stage}_lift_vs_global"] = lift(selected_rate, global_rates.get(f"{stage}_rate"))
    return payload


def classify_cross(row, min_raw_events):
    if row.get("data_blockers"):
        return "BLOCKED_VOLUME_KLINE_CONTEXT"
    if int(row.get("selected_raw_gs_events") or 0) < int(min_raw_events):
        return "TOO_SMALL"
    material_lifts = [
        safe_float(row.get("decision_capture_lift_vs_global"), 0),
        safe_float(row.get("pass_allow_capture_lift_vs_global"), 0),
        safe_float(row.get("pending_capture_lift_vs_global"), 0),
        safe_float(row.get("final_eligibility_lift_vs_global"), 0),
        safe_float(row.get("mode_disabled_adjusted_final_eligibility_lift_vs_global"), 0),
    ]
    if any(value > 0 for value in material_lifts):
        return "SHADOW_IMPROVEMENT_CANDIDATE"
    return "DISCOVERY_WATCH"


def all_window_observations(paper_db, since_ts):
    if not table_exists(paper_db, "candidate_shadow_observations"):
        return []
    rows = paper_db.execute(
        """
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched,
               observed_at,
               CASE WHEN candidate_id = 'current_all' THEN payload_json ELSE NULL END AS payload_json
        FROM candidate_shadow_observations
        WHERE observed_at >= ?
        """,
        (since_ts - 3600,),
    ).fetchall()
    out = []
    for row in rows:
        payload = {}
        if row["candidate_id"] == "current_all" and row["payload_json"]:
            try:
                payload = json.loads(row["payload_json"]) or {}
            except Exception:
                payload = {}
        out.append({
            "signal_id_key": signal_id_key(row["signal_id"]),
            "candidate_id": row["candidate_id"],
            "family": row["family"],
            "matched": bool(row["matched"]),
            "payload": payload,
        })
    return out


def build_all_signal_indexes(observations):
    matched_by_candidate = defaultdict(set)
    context_by_signal = {}
    family_by_candidate = {}
    for row in observations or []:
        signal_id = row.get("signal_id_key")
        candidate_id = row.get("candidate_id")
        if not signal_id or not candidate_id:
            continue
        if row.get("matched"):
            matched_by_candidate[candidate_id].add(signal_id)
            family_by_candidate.setdefault(candidate_id, row.get("family"))
        if candidate_id == "current_all":
            context_by_signal[signal_id] = row.get("payload") or {}
    return matched_by_candidate, context_by_signal, family_by_candidate


def count_candidate_context_matches(candidate_id, dimension, slice_value, matched_all, context_all):
    total = 0
    for signal_id in matched_all.get(candidate_id, set()):
        observed_value = context_value(context_all.get(signal_id) or {}, dimension)
        if norm_value(observed_value) == norm_value(slice_value):
            total += 1
    return total


def build_candidate_dimension_rows(
    *,
    raw_rows,
    matched_candidates,
    context_by_signal,
    stages,
    global_rates,
    matched_all,
    context_all,
    family_by_candidate,
    context_eligibility,
    min_raw_events,
    top_limit,
):
    raw_event_count = len(raw_rows)
    groups = defaultdict(list)
    candidate_family = {}
    dimensions = [
        ("source_component", "source_component"),
        ("lifecycle_profile", "lifecycle"),
        ("markov_bucket", "Markov"),
        ("source_quote_clean", "quote-sensitive"),
        ("source_quote_executable", "quote-sensitive"),
    ]
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id:
            continue
        payload = context_by_signal.get(signal_id) or {}
        for candidate_id in matched_candidates.get(signal_id, set()):
            candidate_family.setdefault(candidate_id, family_by_candidate.get(candidate_id))
            for dimension, group in dimensions:
                value = norm_value(context_value(payload, dimension))
                if value is None:
                    continue
                groups[(candidate_id, dimension, group, value)].append(signal_id)
    out = []
    for (candidate_id, dimension, group, value), signals in groups.items():
        family = candidate_family.get(candidate_id) or family_by_candidate.get(candidate_id)
        blockers = candidate_blockers(candidate_id, family)
        status = dimension_status(context_eligibility, group)
        if group in {"volume", "kline"} or any(blocker in BLOCKED_DIMENSIONS for blocker in blockers):
            data_blockers = [f"{blocker}_context_blocked" for blocker in blockers]
        elif status not in {"CLEAN", "CORE_METADATA_ALLOWED"} and group not in {"quality_timing_reason"}:
            data_blockers = [f"{group}_not_clean"]
        else:
            data_blockers = []
        unique_signals = sorted(set(signals))
        total_candidate_match_signals = count_candidate_context_matches(
            candidate_id,
            dimension,
            value,
            matched_all,
            context_all,
        )
        row = {
            "cross_type": "candidate_x_clean_context",
            "candidate_id": candidate_id,
            "family": family,
            "dimension": dimension,
            "dimension_group": group,
            "slice_value": value,
            "dimension_status": status,
            "selected_raw_gs_events": len(signals),
            "raw_gs_recall": rate(len(signals), raw_event_count),
            "candidate_match_total_signals": total_candidate_match_signals,
            "precision_signal_proxy": rate(len(unique_signals), total_candidate_match_signals),
            "precision_scope": "candidate_match_signals_in_same_context_slice",
            "data_blockers": sorted(set(data_blockers)),
            "same_window_only": True,
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
        }
        row.update(stage_metric_payload(unique_signals, stages, global_rates))
        row["verdict"] = classify_cross(row, min_raw_events)
        out.append(row)
    out.sort(
        key=lambda item: (
            item.get("verdict") == "SHADOW_IMPROVEMENT_CANDIDATE",
            safe_float(item.get("decision_capture_lift_vs_global"), 0),
            safe_float(item.get("pending_capture_lift_vs_global"), 0),
            item.get("selected_raw_gs_events") or 0,
        ),
        reverse=True,
    )
    return out[:top_limit]


def build_quality_timing_rows(
    *,
    raw_rows,
    qt_events,
    matched_candidates,
    stages,
    global_rates,
    family_by_candidate,
    min_raw_events,
    top_limit,
):
    raw_event_count = len(raw_rows)
    cluster_groups = defaultdict(list)
    reason_groups = defaultdict(list)
    candidate_family = {}
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id or signal_id not in qt_events:
            continue
        event = qt_events[signal_id]
        cluster = classify_shadow_review_cluster(event.get("stage"), event.get("attribution"))
        reason_sig = quality_reason_signature(event)
        for candidate_id in matched_candidates.get(signal_id, set()):
            candidate_family.setdefault(candidate_id, family_by_candidate.get(candidate_id))
            cluster_groups[(candidate_id, cluster)].append(signal_id)
            if reason_sig:
                reason_groups[(candidate_id, reason_sig)].append(signal_id)

    def make_row(kind, key, signals):
        candidate_id, value = key
        family = candidate_family.get(candidate_id) or family_by_candidate.get(candidate_id)
        blockers = candidate_blockers(candidate_id, family)
        data_blockers = [f"{blocker}_context_blocked" for blocker in blockers if blocker in BLOCKED_DIMENSIONS]
        row = {
            "cross_type": f"candidate_x_{kind}",
            "candidate_id": candidate_id,
            "family": family,
            "dimension": kind,
            "dimension_group": "quality_timing_reason",
            "slice_value": value,
            "dimension_status": "CLEAN",
            "selected_raw_gs_events": len(signals),
            "raw_gs_recall": rate(len(signals), raw_event_count),
            "candidate_match_total_signals": None,
            "precision_signal_proxy": rate(len(set(signals)), len(set(signals))),
            "precision_scope": "raw_gold_silver_quality_timing_scope_only",
            "data_blockers": sorted(set(data_blockers)),
            "same_window_only": True,
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
        }
        row.update(stage_metric_payload(sorted(set(signals)), stages, global_rates))
        row["verdict"] = classify_cross(row, min_raw_events)
        return row

    rows = [make_row("quality_timing_cluster", key, val) for key, val in cluster_groups.items()]
    rows.extend(make_row("quality_timing_reason", key, val) for key, val in reason_groups.items())
    rows.sort(
        key=lambda item: (
            item.get("verdict") == "SHADOW_IMPROVEMENT_CANDIDATE",
            item.get("selected_raw_gs_events") or 0,
            safe_float(item.get("pending_capture_lift_vs_global"), 0),
        ),
        reverse=True,
    )
    return rows[:top_limit]


def strategy_hypotheses(strategy_memory):
    rows = []
    for row in (strategy_memory or {}).get("hypotheses") or []:
        if not isinstance(row, dict):
            continue
        rows.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "name": row.get("name"),
            "mapped_candidate_ids": row.get("mapped_candidate_ids") or row.get("mapped_existing_candidate_ids") or [],
            "future_data_rejected": bool(row.get("future_data_rejected")),
            "verdict": row.get("verdict"),
            "context_blockers": row.get("context_blockers") or [],
            "promotion_allowed": False,
        })
    return rows


def build_strategy_memory_rows(
    *,
    raw_rows,
    qt_events,
    matched_candidates,
    stages,
    global_rates,
    strategy_memory,
    min_raw_events,
    top_limit,
):
    raw_event_count = len(raw_rows)
    rows = []
    for hyp in strategy_hypotheses(strategy_memory):
        mapped = set(hyp.get("mapped_candidate_ids") or [])
        if not mapped:
            continue
        selected = []
        matched_by_candidate = defaultdict(list)
        qt_selected = defaultdict(list)
        for raw in raw_rows:
            signal_id = raw.get("signal_id_key")
            if not signal_id:
                continue
            matched = set(matched_candidates.get(signal_id, set())) & mapped
            if not matched:
                continue
            selected.append(signal_id)
            for candidate_id in matched:
                matched_by_candidate[candidate_id].append(signal_id)
            if signal_id in qt_events:
                event = qt_events[signal_id]
                cluster = classify_shadow_review_cluster(event.get("stage"), event.get("attribution"))
                qt_selected[cluster].append(signal_id)
        base_blockers = []
        if hyp.get("future_data_rejected"):
            base_blockers.append("strategy_memory_rejected_future_data")
        if hyp.get("context_blockers"):
            base_blockers.extend(str(item) for item in hyp.get("context_blockers") or [])
        row = {
            "cross_type": "strategy_memory_hypothesis_x_current_candidates",
            "hypothesis_id": hyp.get("hypothesis_id"),
            "name": hyp.get("name"),
            "mapped_candidate_count": len(mapped),
            "dimension": "strategy_memory_hypothesis",
            "dimension_group": "Strategy Memory",
            "slice_value": hyp.get("hypothesis_id"),
            "selected_raw_gs_events": len(selected),
            "raw_gs_recall": rate(len(selected), raw_event_count),
            "precision_signal_proxy": None,
            "precision_scope": "historical_memory_candidate_map_raw_gold_silver_scope",
            "data_blockers": sorted(set(base_blockers)),
            "same_window_only": True,
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "top_mapped_candidates_current_window": [
                {"candidate_id": key, "selected_raw_gs_events": len(value)}
                for key, value in sorted(matched_by_candidate.items(), key=lambda item: len(item[1]), reverse=True)[:12]
            ],
        }
        row.update(stage_metric_payload(sorted(set(selected)), stages, global_rates))
        row["verdict"] = (
            "STRATEGY_MEMORY_REJECTED_FUTURE_DATA"
            if hyp.get("future_data_rejected")
            else classify_cross(row, min_raw_events)
        )
        rows.append(row)
        for cluster, signals in qt_selected.items():
            cross = {
                "cross_type": "quality_timing_cluster_x_strategy_memory_hypothesis",
                "hypothesis_id": hyp.get("hypothesis_id"),
                "name": hyp.get("name"),
                "dimension": "quality_timing_cluster",
                "dimension_group": "quality_timing_reason",
                "slice_value": cluster,
                "selected_raw_gs_events": len(signals),
                "raw_gs_recall": rate(len(signals), raw_event_count),
                "precision_signal_proxy": None,
                "precision_scope": "quality_timing_strategy_memory_raw_gold_silver_scope",
                "data_blockers": sorted(set(base_blockers)),
                "same_window_only": True,
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
            }
            cross.update(stage_metric_payload(sorted(set(signals)), stages, global_rates))
            cross["verdict"] = (
                "STRATEGY_MEMORY_REJECTED_FUTURE_DATA"
                if hyp.get("future_data_rejected")
                else classify_cross(cross, min_raw_events)
            )
            rows.append(cross)
    rows.sort(
        key=lambda item: (
            item.get("verdict") == "SHADOW_IMPROVEMENT_CANDIDATE",
            item.get("selected_raw_gs_events") or 0,
            safe_float(item.get("decision_capture_lift_vs_global"), 0),
        ),
        reverse=True,
    )
    return rows[:top_limit]


def top_level_payload(*, report_type, rows, raw_count, global_rates, min_raw_events, extra=None):
    counts = Counter(row.get("verdict") for row in rows)
    improvement = counts.get("SHADOW_IMPROVEMENT_CANDIDATE", 0)
    blocked = sum(count for verdict, count in counts.items() if str(verdict or "").startswith("BLOCKED"))
    too_small = counts.get("TOO_SMALL", 0)
    rejected = sum(count for verdict, count in counts.items() if "REJECTED" in str(verdict or ""))
    watch = counts.get("DISCOVERY_WATCH", 0)
    valid = max(0, len(rows) - blocked - too_small - rejected)

    def max_lift(field):
        values = [safe_float(row.get(field)) for row in rows if safe_float(row.get(field)) is not None]
        return round(max(values), 6) if values else None

    if improvement:
        classification = "CLEAN_DIMENSION_2D_SHADOW_IMPROVEMENT_CANDIDATES"
        next_action = "freeze_repeated_clean_dimension_crosses_for_oos_if_context_remains_clean"
    elif rows:
        classification = "CLEAN_DIMENSION_2D_DISCOVERY_WATCH"
        next_action = "continue_collecting_clean_dimension_cross_evidence"
    else:
        classification = "CLEAN_DIMENSION_2D_NO_SIGNAL"
        next_action = "continue_collecting_raw_gold_silver_and_decision_gap_evidence"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "evidence_level": "discovery_same_window",
        "allowed_use": "shadow_only",
        "raw_gold_silver_event_rows": raw_count,
        "min_raw_events": min_raw_events,
        "global_stage_rates": global_rates,
        "row_count": len(rows),
        "cross_count": len(rows),
        "valid_cross_count": valid,
        "invalid_cross_count": len(rows) - valid,
        "shadow_improvement_candidate_count": improvement,
        "discovery_hit_count": improvement,
        "watch_count": watch,
        "blocked_cross_count": blocked,
        "too_small_cross_count": too_small,
        "rejected_cross_count": rejected,
        "status_counts": dict(counts),
        "same_window_discovery_only": True,
        "oos_required_before_promotion": True,
        "oos_status": "OOS_PENDING",
        "best_lifts_vs_global": {
            "decision_capture_lift": max_lift("decision_capture_lift_vs_global"),
            "pass_allow_capture_lift": max_lift("pass_allow_capture_lift_vs_global"),
            "pending_capture_lift": max_lift("pending_capture_lift_vs_global"),
            "final_eligibility_lift": max_lift("final_eligibility_lift_vs_global"),
            "mode_disabled_adjusted_final_eligibility_lift": max_lift(
                "mode_disabled_adjusted_final_eligibility_lift_vs_global"
            ),
        },
        "top_rows": rows[:40],
        "top_crosses": rows[:40],
        "rows": rows,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Capture-first 2D cross report. PnL is not used as the primary metric.",
            "Same-window evidence may generate shadow-only hypotheses but cannot promote strategy.",
            "Volume/kline-dependent rows remain blocked or research-only while coverage is below threshold.",
        ],
    }
    if extra:
        payload.update(extra)
    return payload


def build_reports(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    context_eligibility = load_json(args.context_eligibility, {})
    strategy_memory = load_json(args.strategy_memory, {})
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        raw_rows = load_raw_dogs(raw_db, since_ts)
        raw_signal_ids, tokens, _by_signal, _by_token = make_raw_indexes(raw_rows)
        observations, observation_meta = load_candidate_observations(paper_db, raw_signal_ids, since_ts)
        all_observations = all_window_observations(paper_db, since_ts)
        decisions = load_paper_decisions(paper_db, tokens, since_ts, now_ts)
        trades = load_paper_trades(paper_db, tokens, since_ts, now_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, int(args.expected_candidates))
        stages = build_stage_sets(paper_db, raw_rows, audits, trades, since_ts, now_ts)
    finally:
        raw_db.close()
        paper_db.close()

    _obs_by_signal, matched_candidates, context_by_signal, _candidate_sets, _full = build_observation_indexes(observations)
    matched_all, context_all, family_by_candidate = build_all_signal_indexes(all_observations)
    qt_events = stage_quality_timing_events(raw_rows, decisions)
    global_rates = signal_stage_rates(raw_signal_sets(raw_rows), stages)
    clean_rows = build_candidate_dimension_rows(
        raw_rows=raw_rows,
        matched_candidates=matched_candidates,
        context_by_signal=context_by_signal,
        stages=stages,
        global_rates=global_rates,
        matched_all=matched_all,
        context_all=context_all,
        family_by_candidate=family_by_candidate,
        context_eligibility=context_eligibility,
        min_raw_events=int(args.min_raw_events),
        top_limit=int(args.top_limit),
    )
    quality_rows = build_quality_timing_rows(
        raw_rows=raw_rows,
        qt_events=qt_events,
        matched_candidates=matched_candidates,
        stages=stages,
        global_rates=global_rates,
        family_by_candidate=family_by_candidate,
        min_raw_events=int(args.min_raw_events),
        top_limit=int(args.top_limit),
    )
    strategy_rows = build_strategy_memory_rows(
        raw_rows=raw_rows,
        qt_events=qt_events,
        matched_candidates=matched_candidates,
        stages=stages,
        global_rates=global_rates,
        strategy_memory=strategy_memory,
        min_raw_events=int(args.min_raw_events),
        top_limit=int(args.top_limit),
    )
    common_extra = {
        "window": {"hours": float(args.hours), "since_ts": since_ts, "now_ts": now_ts},
        "observation_load": {
            "raw_scoped": observation_meta,
            "all_window_observation_rows": len(all_observations),
        },
        "context_dimension_eligibility_classification": context_eligibility.get("classification"),
    }
    clean_payload = top_level_payload(
        report_type="clean_dimension_2d_capture_cross",
        rows=clean_rows,
        raw_count=len(raw_rows),
        global_rates=global_rates,
        min_raw_events=int(args.min_raw_events),
        extra={
            **common_extra,
            "clean_dimensions_used": [
                "source_component",
                "lifecycle_profile",
                "markov_bucket",
                "source_quote_clean",
                "source_quote_executable",
            ],
            "blocked_dimensions": ["volume", "kline"],
        },
    )
    quality_payload = top_level_payload(
        report_type="quality_timing_reason_cross",
        rows=quality_rows,
        raw_count=len(raw_rows),
        global_rates=global_rates,
        min_raw_events=int(args.min_raw_events),
        extra={
            **common_extra,
            "quality_timing_event_count": len(qt_events),
            "dimensions_used": ["quality_timing_cluster", "quality_timing_reason"],
        },
    )
    strategy_payload = top_level_payload(
        report_type="strategy_memory_reason_cross",
        rows=strategy_rows,
        raw_count=len(raw_rows),
        global_rates=global_rates,
        min_raw_events=int(args.min_raw_events),
        extra={
            **common_extra,
            "strategy_memory_hypotheses_count": len(strategy_hypotheses(strategy_memory)),
            "dimensions_used": [
                "strategy_memory_hypothesis",
                "quality_timing_cluster_x_strategy_memory_hypothesis",
            ],
        },
    )
    return clean_payload, quality_payload, strategy_payload


def create_self_test_inputs(root):
    now = int(time.time())
    raw_path = root / "raw.db"
    paper_path = root / "paper.db"
    strategy_path = root / "strategy.json"
    context_path = root / "context.json"
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
            (103, "token-c", "C", now - 180, "gold", "gold"),
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
    obs = []
    for signal_id, token, component, lifecycle in [
        (101, "token-a", "matrix_evaluator", "ATH_SHALLOW_PULLBACK"),
        (102, "token-b", "other_component", "OTHER"),
        (103, "token-c", "matrix_evaluator", "ATH_SHALLOW_PULLBACK"),
        (201, "token-x", "matrix_evaluator", "ATH_SHALLOW_PULLBACK"),
    ]:
        obs.append((
            signal_id,
            token,
            now - 200,
            "current_all",
            "base",
            1,
            "baseline",
            now - 180,
            json.dumps({
                "source_component": component,
                "lifecycle_profile": lifecycle,
                "markov_bucket": "yellow",
                "source_quote_clean": True,
                "source_quote_executable": True,
            }),
        ))
        obs.append((signal_id, token, now - 200, "notath_quote_clean", "base", 1, "matched", now - 180, None))
    paper.executemany(
        "INSERT INTO candidate_shadow_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        obs,
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
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, 'selftest', NULL, '{}')
        """,
        [
            (now - 170, 101, "token-a", "A", "entry_engine", "pass", "decision", "PASS"),
            (now - 160, 101, "token-a", "A", "entry_engine", "pending", "pending_entry", "PENDING"),
            (now - 150, 102, "token-b", "B", "smart_entry", "chasing_top", "timing_decision", "reject"),
        ],
    )
    paper.commit()
    paper.close()
    write_json(strategy_path, {
        "hypotheses": [
            {
                "hypothesis_id": "SM-TEST",
                "name": "self test",
                "mapped_candidate_ids": ["notath_quote_clean"],
                "future_data_rejected": False,
                "promotion_allowed": False,
            }
        ],
        "promotion_allowed": False,
    })
    write_json(context_path, {
        "classification": "BLOCKED_CONTEXT_COVERAGE",
        "dimension_eligibility": {
            "source_component": {"status": "CLEAN"},
            "lifecycle": {"status": "CLEAN"},
            "Markov": {"status": "CLEAN"},
            "quote-sensitive": {"status": "CLEAN"},
        },
        "promotion_allowed": False,
    })
    return raw_path, paper_path, strategy_path, context_path


def run_self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path, paper_path, strategy_path, context_path = create_self_test_inputs(root)
        clean_out = root / "clean.json"
        quality_out = root / "quality.json"
        strategy_out = root / "strategy_cross.json"
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            strategy_memory=str(strategy_path),
            context_eligibility=str(context_path),
            hours=24,
            now_ts=int(time.time()),
            expected_candidates=2,
            min_raw_events=1,
            top_limit=100,
            out=str(clean_out),
            quality_out=str(quality_out),
            strategy_out=str(strategy_out),
        )
        clean, quality, strategy = build_reports(args)
        write_json(clean_out, clean)
        write_json(quality_out, quality)
        write_json(strategy_out, strategy)
        assert clean["promotion_allowed"] is False
        assert clean["raw_gold_silver_event_rows"] == 3
        assert clean["row_count"] > 0
        assert any(row["dimension"] == "source_component" for row in clean["rows"])
        assert quality["quality_timing_event_count"] == 1
        assert quality["row_count"] > 0
        assert strategy["strategy_memory_hypotheses_count"] == 1
        assert strategy["row_count"] > 0
        assert clean_out.exists() and quality_out.exists() and strategy_out.exists()
    print("self-test passed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--strategy-memory", default="/app/data/agent_runs/latest/strategy_memory_capture_validation.json")
    parser.add_argument("--context-eligibility", default="/app/data/agent_runs/latest/context_dimension_eligibility.json")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--min-raw-events", type=int, default=DEFAULT_MIN_RAW_EVENTS)
    parser.add_argument("--top-limit", type=int, default=DEFAULT_TOP_LIMIT)
    parser.add_argument("--out", default="/app/data/agent_runs/latest/clean_dimension_2d_capture_cross_24h.json")
    parser.add_argument("--quality-out", default="/app/data/agent_runs/latest/quality_timing_reason_cross_24h.json")
    parser.add_argument("--strategy-out", default="/app/data/agent_runs/latest/strategy_memory_reason_cross_24h.json")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    clean, quality, strategy = build_reports(args)
    write_json(args.out, clean)
    write_json(args.quality_out, quality)
    write_json(args.strategy_out, strategy)
    print(json.dumps({
        "classification": clean.get("classification"),
        "clean_rows": clean.get("row_count"),
        "quality_rows": quality.get("row_count"),
        "strategy_rows": strategy.get("row_count"),
        "promotion_allowed": False,
        "out": args.out,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
