#!/usr/bin/env python3
"""Read-only post-freeze OOS validation for pass_allow 60% closure definitions.

This report validates frozen pass_allow closure definitions against data that
arrived after the freeze timestamp. It is discovery/readiness evidence only:
it never changes strategy, entry policy, gates, final_entry_contract, A_CLASS,
executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
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
from quality_timing_reject_research_audit import (
    classify_shadow_review_cluster,
    stage_quality_timing_events,
)


SCHEMA_VERSION = "pass_allow_60_post_freeze_oos_validation.v1"
DEFAULT_EXPECTED_CANDIDATES = 84
DEFAULT_MIN_RAW_EVENTS = 10
DEFAULT_MIN_SELECTED_EVENTS = 3
DEFAULT_SAFETY_SEC = 120


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parse_utc_ts(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value / 1000) if value > 1_000_000_000_000 else int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = _dt.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


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


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enter", "would_enter"}


def norm_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value).strip().lower()


def context_value(payload, dimension):
    if not isinstance(payload, dict) or not dimension:
        return None
    if dimension in payload:
        return payload.get(dimension)
    aliases = {
        "source_quote_clean": ("source_quote_clean_seen", "quote_clean_seen"),
        "source_quote_executable": ("source_quote_executable_proxy", "quote_executable"),
        "lifecycle_profile": ("lifecycle_state",),
    }
    for alias in aliases.get(str(dimension), ()):
        if alias in payload:
            return payload.get(alias)
    return None


def audit_pass_allow(audit):
    return bool((audit or {}).get("would_enter_count", 0) > 0)


def index_by_signal(rows):
    out = defaultdict(list)
    for row in rows or []:
        key = row.get("signal_id") or row.get("signal_id_key")
        if key is not None:
            out[signal_id_key(key)].append(row)
    return out


def build_observation_indexes(observations):
    by_signal = defaultdict(list)
    matched_candidates = defaultdict(set)
    context_by_signal = {}
    for row in observations or []:
        key = row.get("signal_id_key")
        if not key:
            continue
        by_signal[key].append(row)
        if row.get("matched"):
            matched_candidates[key].add(row.get("candidate_id"))
        if row.get("candidate_id") == "current_all":
            context_by_signal[key] = row.get("payload") or {}
    return by_signal, matched_candidates, context_by_signal


def quality_cluster_by_signal(raw_rows, decisions):
    events = stage_quality_timing_events(raw_rows, decisions)
    out = {}
    for signal_id, event in events.items():
        out[signal_id] = {
            **event,
            "cluster": classify_shadow_review_cluster(
                event.get("stage"),
                event.get("attribution") or {},
            ),
        }
    return out


def selected_stats(selected, audit_by_signal, global_pass_allow_rate, min_selected_events):
    selected_count = len(selected)
    pass_allow_count = sum(1 for signal_id in selected if audit_pass_allow(audit_by_signal.get(signal_id)))
    pass_allow_rate = rate(pass_allow_count, selected_count)
    lift = None if pass_allow_rate is None or global_pass_allow_rate is None else round(pass_allow_rate - global_pass_allow_rate, 6)
    if selected_count < min_selected_events:
        verdict = "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"
    elif lift is not None and lift > 0:
        verdict = "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"
    else:
        verdict = "PASS_ALLOW_60_POST_FREEZE_NO_REPEAT"
    return {
        "selected_raw_gold_silver_events": selected_count,
        "selected_pass_allow_count": pass_allow_count,
        "selected_pass_allow_rate": pass_allow_rate,
        "pass_allow_lift_vs_post_freeze_global": lift,
        "verdict": verdict,
    }


def validate_clean_2d(item, raw_rows, audit_by_signal, matched_candidates, context_by_signal, global_rate, min_selected):
    definition = item.get("freeze_definition") or {}
    candidate_id = definition.get("candidate_id")
    dimension = definition.get("dimension")
    slice_value = definition.get("slice_value")
    selected = []
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id:
            continue
        if candidate_id and candidate_id not in matched_candidates.get(signal_id, set()):
            continue
        observed_value = context_value(context_by_signal.get(signal_id) or {}, dimension)
        if norm_value(observed_value) != norm_value(slice_value):
            continue
        selected.append(signal_id)
    stats = selected_stats(selected, audit_by_signal, global_rate, min_selected)
    return {
        "validation_role": "candidate_context_pass_allow_oos_probe",
        "candidate_id": candidate_id,
        "dimension": dimension,
        "slice_value": slice_value,
        **stats,
    }


def validate_candidate_only(item, raw_rows, audit_by_signal, matched_candidates, global_rate, min_selected):
    definition = item.get("freeze_definition") or {}
    candidate_id = definition.get("candidate_id")
    if not candidate_id:
        return {
            "validation_role": "unsupported_shadow_queue_item",
            "verdict": "VALIDATION_UNSUPPORTED_SHADOW_QUEUE_ITEM",
            "selected_raw_gold_silver_events": 0,
            "selected_pass_allow_count": 0,
            "selected_pass_allow_rate": None,
            "pass_allow_lift_vs_post_freeze_global": None,
        }
    selected = [
        row.get("signal_id_key")
        for row in raw_rows
        if row.get("signal_id_key") and candidate_id in matched_candidates.get(row.get("signal_id_key"), set())
    ]
    stats = selected_stats(selected, audit_by_signal, global_rate, min_selected)
    return {
        "validation_role": "candidate_only_pass_allow_oos_probe",
        "candidate_id": candidate_id,
        **stats,
    }


def validate_quality_cluster(item, raw_rows, audit_by_signal, cluster_by_signal, min_selected):
    definition = item.get("freeze_definition") or {}
    cluster = definition.get("cluster") or ((definition.get("required_match") or {}).get("quality_timing_cluster"))
    selected = []
    selected_stages = Counter()
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        event = cluster_by_signal.get(signal_id) or {}
        if event.get("cluster") != cluster:
            continue
        selected.append(signal_id)
        selected_stages[event.get("stage") or "UNKNOWN"] += 1
    selected_count = len(selected)
    pass_allow_count = sum(1 for signal_id in selected if audit_pass_allow(audit_by_signal.get(signal_id)))
    if selected_count < min_selected:
        verdict = "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"
    else:
        verdict = "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"
    return {
        "validation_role": "quality_timing_cluster_repeat_evidence_not_effect",
        "cluster": cluster,
        "selected_raw_gold_silver_events": selected_count,
        "selected_pass_allow_count": pass_allow_count,
        "selected_pass_allow_rate": rate(pass_allow_count, selected_count),
        "pass_allow_lift_vs_post_freeze_global": None,
        "stage_counts": dict(selected_stages),
        "verdict": verdict,
    }


def validate_item(item, raw_rows, audit_by_signal, matched_candidates, context_by_signal, cluster_by_signal, global_rate, min_selected):
    source = item.get("source")
    if source == "clean_2d_pass_allow_lift_slice":
        result = validate_clean_2d(
            item,
            raw_rows,
            audit_by_signal,
            matched_candidates,
            context_by_signal,
            global_rate,
            min_selected,
        )
    elif source == "decision_no_pass_quality_timing_cluster":
        result = validate_quality_cluster(item, raw_rows, audit_by_signal, cluster_by_signal, min_selected)
    elif source == "shadow_queue_pass_allow_item":
        result = validate_candidate_only(item, raw_rows, audit_by_signal, matched_candidates, global_rate, min_selected)
    else:
        result = {
            "validation_role": "unknown_frozen_definition_source",
            "verdict": "VALIDATION_UNSUPPORTED_FROZEN_DEFINITION_SOURCE",
            "selected_raw_gold_silver_events": 0,
            "selected_pass_allow_count": 0,
            "selected_pass_allow_rate": None,
            "pass_allow_lift_vs_post_freeze_global": None,
        }
    return {
        "freeze_id": item.get("freeze_id"),
        "source": source,
        "definition_fingerprint": item.get("definition_fingerprint"),
        "frozen_at": item.get("frozen_at"),
        "current_window_evidence": item.get("current_window_evidence") or {},
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        **result,
    }


def classify_report(raw_count, items):
    if raw_count < DEFAULT_MIN_RAW_EVENTS:
        return "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"
    supported = [
        row for row in items
        if row.get("verdict") not in {
            "VALIDATION_UNSUPPORTED_SHADOW_QUEUE_ITEM",
            "VALIDATION_UNSUPPORTED_FROZEN_DEFINITION_SOURCE",
        }
    ]
    if not supported:
        return "PASS_ALLOW_60_POST_FREEZE_OOS_NO_SUPPORTED_DEFINITIONS"
    repeated = [row for row in supported if row.get("verdict") == "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"]
    sufficient = [row for row in supported if row.get("verdict") != "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"]
    if repeated:
        return "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"
    if sufficient:
        return "PASS_ALLOW_60_POST_FREEZE_NO_REPEAT"
    return "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"


def build_report(args):
    registry = load_json(args.freeze_registry, {})
    frozen_at = registry.get("definition_set_frozen_at") or registry.get("generated_at")
    frozen_ts = parse_utc_ts(frozen_at)
    if frozen_ts is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "pass_allow_60_post_freeze_oos_validation",
            "generated_at": utc_now(),
            "classification": "PASS_ALLOW_60_POST_FREEZE_FREEZE_TS_MISSING",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": [],
        }
    eval_start_ts = int(frozen_ts) + int(args.safety_sec)
    now_ts = int(time.time())
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
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
    finally:
        raw_db.close()
        paper_db.close()
    audit_by_signal = {
        row.get("signal_id"): row for row in audits if row.get("signal_id")
    }
    _obs_by_signal, matched_candidates, context_by_signal = build_observation_indexes(observations)
    cluster_by_signal = quality_cluster_by_signal(raw_rows, decisions)
    raw_count = len(raw_rows)
    global_pass_allow_count = sum(1 for row in audits if audit_pass_allow(row))
    global_pass_allow_rate = rate(global_pass_allow_count, raw_count)
    items = [
        validate_item(
            item,
            raw_rows,
            audit_by_signal,
            matched_candidates,
            context_by_signal,
            cluster_by_signal,
            global_pass_allow_rate,
            int(args.min_selected_events),
        )
        for item in (registry.get("items") or [])
        if isinstance(item, dict)
    ]
    status_counts = Counter(row.get("verdict") for row in items)
    source_counts = Counter(row.get("source") for row in items)
    positive_lift = [
        row for row in items
        if row.get("pass_allow_lift_vs_post_freeze_global") is not None
        and row.get("pass_allow_lift_vs_post_freeze_global") > 0
    ]
    repeated = [row for row in items if row.get("verdict") == "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"]
    classification = classify_report(raw_count, items)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "pass_allow_60_post_freeze_oos_validation",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "evidence_level": "post_freeze_oos_readiness_probe",
        "usage": "read_only_validation_only",
        "classification": classification,
        "next_action": (
            "continue_collecting_post_freeze_oos_window"
            if classification == "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL"
            else "review_repeated_post_freeze_pass_allow_evidence_without_promotion"
            if classification == "PASS_ALLOW_60_POST_FREEZE_REPEAT_WATCH"
            else "continue_shadow_oos_watch"
        ),
        "freeze_registry_available": bool(registry),
        "definition_set_frozen_at": registry.get("definition_set_frozen_at"),
        "freeze_generated_at": registry.get("generated_at"),
        "eval_start_ts": eval_start_ts,
        "eval_start_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(eval_start_ts)),
        "now_ts": now_ts,
        "post_freeze_usable_hours": round(max(0, now_ts - eval_start_ts) / 3600.0, 4),
        "post_freeze_safety_sec": int(args.safety_sec),
        "raw_gold_silver_event_rows": raw_count,
        "min_raw_events_for_oos_judgment": int(args.min_raw_events),
        "global_pass_allow_count": global_pass_allow_count,
        "global_pass_allow_rate": global_pass_allow_rate,
        "candidate_observation_meta": observation_meta,
        "frozen_definition_count": len(registry.get("items") or []),
        "validated_definition_count": len(items),
        "supported_definition_count": sum(
            1 for row in items
            if row.get("verdict") not in {
                "VALIDATION_UNSUPPORTED_SHADOW_QUEUE_ITEM",
                "VALIDATION_UNSUPPORTED_FROZEN_DEFINITION_SOURCE",
            }
        ),
        "repeat_watch_count": len(repeated),
        "positive_lift_count": len(positive_lift),
        "too_small_definition_count": status_counts.get("PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL", 0),
        "status_counts": dict(status_counts),
        "source_counts": dict(source_counts),
        "top_repeat_watch_items": sorted(
            repeated,
            key=lambda row: (
                row.get("selected_raw_gold_silver_events") or 0,
                row.get("pass_allow_lift_vs_post_freeze_global") or 0,
            ),
            reverse=True,
        )[:20],
        "items": items,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "This report validates frozen definitions only on post-freeze raw gold/silver rows.",
            "Repeat evidence does not authorize promotion or runtime behavior changes.",
        ],
    }


def create_self_test_inputs(root):
    now = int(time.time())
    frozen_ts = now - 600
    registry_path = root / "freeze.json"
    write_json(registry_path, {
        "schema_version": "pass_allow_60_oos_freeze_registry.v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(frozen_ts)),
        "definition_set_frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(frozen_ts)),
        "items": [
            {
                "freeze_id": "slice-1",
                "source": "clean_2d_pass_allow_lift_slice",
                "definition_fingerprint": "slice1",
                "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(frozen_ts)),
                "freeze_definition": {
                    "candidate_id": "notath_executable_quote_clean",
                    "dimension": "source_quote_executable",
                    "slice_value": True,
                },
            },
            {
                "freeze_id": "cluster-1",
                "source": "decision_no_pass_quality_timing_cluster",
                "definition_fingerprint": "cluster1",
                "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(frozen_ts)),
                "freeze_definition": {
                    "cluster": "matrix_alignment_wait",
                    "required_match": {"quality_timing_cluster": "matrix_alignment_wait"},
                },
            },
        ],
    })
    raw_path = root / "raw.db"
    paper_path = root / "paper.db"
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
    paper.executemany(
        "INSERT INTO candidate_shadow_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                101, "token-a", now - 200, "current_all", "base", 1, "baseline",
                now - 190,
                json.dumps({"source_quote_executable": True, "source_component": "selftest"}),
            ),
            (
                101, "token-a", now - 200, "notath_executable_quote_clean", "entry", 1,
                "matched", now - 190, None,
            ),
            (
                102, "token-b", now - 190, "current_all", "base", 1, "baseline",
                now - 180,
                json.dumps({"source_quote_executable": False, "source_component": "matrix_evaluator"}),
            ),
        ],
    )
    paper.execute(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY, event_ts INTEGER, signal_id INTEGER, token_ca TEXT,
          symbol TEXT, lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT,
          decision TEXT, route TEXT, data_source TEXT, lifecycle_state TEXT
        )
        """
    )
    paper.executemany(
        """
        INSERT INTO paper_decision_events
        (event_ts, signal_id, token_ca, symbol, lifecycle_id, component, reason,
         event_type, decision, route, data_source, lifecycle_state)
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, 'selftest', NULL)
        """,
        [
            (now - 180, 101, "token-a", "A", "entry_engine", "selftest pass", "decision", "PASS"),
            (
                now - 170, 102, "token-b", "B", "matrix_evaluator",
                "matrices not yet aligned", "timing_decision", "WAIT",
            ),
        ],
    )
    paper.commit()
    paper.close()
    return raw_path, paper_path, registry_path


def run_self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path, paper_path, registry_path = create_self_test_inputs(root)
        out = root / "out.json"
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            freeze_registry=str(registry_path),
            out=str(out),
            expected_candidates=2,
            safety_sec=120,
            min_raw_events=1,
            min_selected_events=1,
        )
        payload = build_report(args)
        write_json(out, payload)
        assert payload["promotion_allowed"] is False
        assert payload["raw_gold_silver_event_rows"] == 2
        assert payload["validated_definition_count"] == 2
        assert payload["repeat_watch_count"] >= 1
        assert out.exists()
    print("self-test passed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--freeze-registry", default="/app/data/agent_runs/latest/pass_allow_60_oos_freeze_registry.json")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/pass_allow_60_post_freeze_oos_validation.json")
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--safety-sec", type=int, default=DEFAULT_SAFETY_SEC)
    parser.add_argument("--min-raw-events", type=int, default=DEFAULT_MIN_RAW_EVENTS)
    parser.add_argument("--min-selected-events", type=int, default=DEFAULT_MIN_SELECTED_EVENTS)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    payload = build_report(args)
    write_json(args.out, payload)
    print(json.dumps({
        "classification": payload.get("classification"),
        "raw_gold_silver_event_rows": payload.get("raw_gold_silver_event_rows"),
        "validated_definition_count": payload.get("validated_definition_count"),
        "repeat_watch_count": payload.get("repeat_watch_count"),
        "promotion_allowed": payload.get("promotion_allowed"),
        "out": str(args.out),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
