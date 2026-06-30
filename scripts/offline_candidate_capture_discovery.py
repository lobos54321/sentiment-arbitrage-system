#!/usr/bin/env python3
"""Capture-first discovery report for the 84-candidate shadow mesh.

Read-only. This report evaluates gold/silver dog capture using
candidate_shadow_observations as the primary table, so non-matches count.
PnL is intentionally secondary and is not used for promotion decisions here.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path


EXPECTED_CANDIDATE_COUNT = 84
EXPECTED_CONTEXT_SCHEMA_VERSION = "candidate-shadow-context-v2.no_signal_price_quote_inference"
EXPECTED_QUOTE_CLEAN_DEFINITION = "source_or_executable_quote_only_no_signal_price"
SCHEMA_VERSION = "offline_candidate_capture_discovery.v4"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_MAX_SCAN_ROWS = 2_000_000
QUOTE_CLEAN_KEYS = ("source_quote_clean", "source_quote_clean_seen")
QUOTE_EXECUTABLE_KEYS = ("source_quote_executable", "source_quote_executable_proxy")
QUOTE_UNKNOWN_VALUES = {"unknown", "unk", "null", "none", "unavailable"}
QUOTE_NOT_APPLICABLE_VALUES = {"not_applicable", "not-applicable", "n/a", "na", "not applicable"}
MATURE_CONTEXT_MIN_AGE_SEC = 6 * 3600
MATURE_CONTEXT_MIN_ROWS = 50

DIMENSIONS = (
    "source_quote_clean",
    "source_quote_executable",
    "source_quote_executable_proxy",
    "source_component",
    "source_resonance_state",
    "signal_type",
    "hard_gate_status",
    "market_cap_bucket",
    "volume_profile",
    "candle_pattern",
    "lifecycle_profile",
    "markov_bucket",
    "fbr_time_legal",
    "fbr_lookahead_warning",
)


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        number = int(float(value))
        return number if number > 0 else None
    except Exception:
        return None


def signal_id_key(value):
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
        if math.isfinite(number) and number.is_integer():
            return str(int(number))
    except Exception:
        pass
    return text


def safe_float(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def pct(numerator, denominator):
    if not denominator:
        return None
    return round(float(numerator) / float(denominator) * 100.0, 4)


def rate(numerator, denominator):
    if not denominator:
        return None
    return round(float(numerator) / float(denominator), 6)


def row_signal_age_sec(row, now_ts):
    ts = safe_int(row.get("signal_ts") or row.get("observed_at"))
    if ts is None:
        return None
    return max(0, int(now_ts) - int(ts))


def mature_context_rows(rows, now_ts, min_age_sec=MATURE_CONTEXT_MIN_AGE_SEC):
    return [
        row for row in rows
        if (row_signal_age_sec(row, now_ts) is not None and row_signal_age_sec(row, now_ts) >= min_age_sec)
    ]


def compact_rate_counts(prefix, counts, denominator):
    true_n = int(counts.get("true") or 0)
    false_n = int(counts.get("false") or 0)
    missing_n = int(counts.get("missing") or 0)
    unknown_n = int(counts.get("unknown") or 0)
    not_applicable_n = int(counts.get("not_applicable") or 0)
    present_n = true_n + false_n + not_applicable_n
    return {
        f"{prefix}_present_rows": present_n,
        f"{prefix}_true_rows": true_n,
        f"{prefix}_false_rows": false_n,
        f"{prefix}_missing_rows": missing_n,
        f"{prefix}_unknown_rows": unknown_n,
        f"{prefix}_not_applicable_rows": not_applicable_n,
        f"{prefix}_present_rate": rate(present_n, denominator),
        f"{prefix}_true_rate": rate(true_n, denominator),
        f"{prefix}_false_rate": rate(false_n, denominator),
        f"{prefix}_missing_rate": rate(missing_n, denominator),
        f"{prefix}_unknown_rate": rate(unknown_n, denominator),
        f"{prefix}_not_applicable_rate": rate(not_applicable_n, denominator),
    }


def table_exists(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def recent_rowid_floor(db, table, max_scan_rows):
    if not max_scan_rows or max_scan_rows <= 0:
        return None
    try:
        row = db.execute(f"SELECT MAX(rowid) FROM {table}").fetchone()
        max_rowid = int(row[0] or 0) if row else 0
    except Exception:
        return None
    if max_rowid <= 0:
        return None
    return max(1, max_rowid - int(max_scan_rows) + 1)


def cols(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def bucket_market_cap(value):
    mc = safe_float(value)
    if mc is None or mc <= 0:
        return "UNKNOWN"
    if mc < 5_000:
        return "lt5k"
    if mc < 10_000:
        return "5k_10k"
    if mc < 30_000:
        return "10k_30k"
    if mc < 100_000:
        return "30k_100k"
    return "gte100k"


def dim_value(payload, name):
    if name == "market_cap_bucket":
        return bucket_market_cap(payload.get("market_cap"))
    value = payload.get(name)
    if value in (None, ""):
        if name == "source_quote_clean":
            value = payload.get("source_quote_clean_seen")
        elif name == "source_quote_executable":
            value = payload.get("source_quote_executable_proxy")
    if value in (None, ""):
        return "UNKNOWN"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def dog_tier(row):
    for key in ("raw_primary_tier", "raw_sustained_tier", "tier", "dog_tier"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value).lower()
    return None


def is_gold_silver_row(row):
    return dog_tier(row) in {"gold", "silver"}


def normalize_raw_dog_row(row):
    payload = jloads(row.get("payload_json"))
    signal_id = signal_id_key(row.get("signal_id"))
    token_ca = row.get("token_ca") or row.get("token") or row.get("ca")
    signal_ts = safe_int(row.get("signal_ts") or row.get("timestamp"))
    alias_ids = []
    for key in ("premium_signal_id", "source_signal_id", "raw_signal_id", "signal_id"):
        value = payload.get(key)
        normalized = signal_id_key(value)
        if normalized and normalized != signal_id:
            alias_ids.append(normalized)
    return {
        "raw_event_id": row.get("id"),
        "raw_signal_id_raw": row.get("signal_id"),
        "signal_id": signal_id,
        "signal_alias_ids": sorted(set(alias_ids)),
        "token_ca": str(token_ca) if token_ca not in (None, "") else None,
        "symbol": row.get("symbol"),
        "signal_ts": signal_ts,
        "signal_type": row.get("signal_type"),
        "route": row.get("route"),
        "hard_gate_status": row.get("hard_gate_status"),
        "source": row.get("source"),
        "source_kind": row.get("source_kind"),
        "source_family": row.get("source_family"),
        "observation_status": row.get("observation_status"),
        "kline_covered": row.get("kline_covered"),
        "baseline_confidence": row.get("baseline_confidence"),
        "same_source_path": row.get("same_source_path"),
        "outlier_flag": row.get("outlier_flag"),
        "sustained_evaluable": row.get("sustained_evaluable"),
        "lifecycle_id": row.get("lifecycle_id") or payload.get("lifecycle_id"),
        "tier": dog_tier(row),
        "max_sustained_peak_pct": safe_float(row.get("max_sustained_peak_pct")),
        "time_to_sustained_peak_sec": safe_float(row.get("time_to_sustained_peak_sec")),
        "raw_dog_entered": safe_bool(row.get("raw_dog_entered") or row.get("did_enter")),
        "raw_dog_realized": safe_bool(row.get("raw_dog_realized") or row.get("held_to_silver") or row.get("held_to_gold")),
        "exit_reason": row.get("exit_reason"),
    }


def load_raw_dogs_from_json(path, since_ts):
    if not path:
        return [], {"source": "none", "available": False}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = []
    for key in ("top_raw_dogs", "missed_raw_dogs", "raw_dogs", "dogs"):
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(value)
    out = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or not is_gold_silver_row(row):
            continue
        dog = normalize_raw_dog_row(row)
        if since_ts and dog["signal_ts"] and dog["signal_ts"] < since_ts:
            continue
        key = (dog["signal_id"], dog["token_ca"], dog["tier"])
        if key in seen:
            continue
        seen.add(key)
        out.append(dog)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    expected_unique = safe_int(summary.get("raw_sustained_gold_silver_unique"))
    expected_events = safe_int(summary.get("raw_sustained_gold_silver_event_rows"))
    loaded_unique = len({row.get("token_ca") for row in out if row.get("token_ca")})
    loaded_events = len(out)
    rows_complete = True
    if expected_unique is not None and loaded_unique < expected_unique:
        rows_complete = False
    if expected_events is not None and loaded_events < expected_events:
        rows_complete = False
    meta = {
        "source": "raw_dog_json",
        "available": True,
        "path": path,
        "_raw_all_dogs": out,
        "summary": summary or None,
        "expected_unique_from_summary": expected_unique,
        "expected_event_rows_from_summary": expected_events,
        "loaded_unique_rows": loaded_unique,
        "loaded_event_rows": loaded_events,
        "rows_complete_against_summary": rows_complete,
        "denominator_audit": {
            "mode": "json_rows_vs_summary",
            "raw_all_gold_silver_event_rows": expected_events if expected_events is not None else loaded_events,
            "raw_all_gold_silver_unique_tokens": expected_unique if expected_unique is not None else loaded_unique,
            "evaluable_gold_silver_event_rows": loaded_events,
            "evaluable_gold_silver_unique_tokens": loaded_unique,
            "filtered_out_event_rows": None,
            "filter_drop_breakdown_non_exclusive": {},
            "eligibility_filters_applied": [],
            "business_denominator": "raw_all_gold_silver_from_summary",
            "primary_report_denominator": "loaded_raw_dog_rows",
        },
        "note": payload.get("note"),
    }
    return out, meta


def load_raw_dogs_from_db(db, since_ts):
    if not table_exists(db, "raw_signal_outcomes"):
        return [], {"source": "raw_signal_outcomes", "available": False}
    columns = cols(db, "raw_signal_outcomes")
    if "raw_primary_tier" not in columns and "raw_sustained_tier" not in columns:
        return [], {
            "source": "raw_signal_outcomes",
            "available": False,
            "reason": "missing_raw_dog_tier_columns",
        }
    select = []
    for name in (
        "id",
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
        "signal_type",
        "route",
        "hard_gate_status",
        "source",
        "observation_status",
        "kline_covered",
        "baseline_confidence",
        "same_source_path",
        "outlier_flag",
        "sustained_evaluable",
        "raw_primary_tier",
        "raw_sustained_tier",
        "max_sustained_peak_pct",
        "time_to_sustained_peak_sec",
        "raw_dog_entered",
        "raw_dog_realized",
        "did_enter",
        "held_to_silver",
        "held_to_gold",
        "exit_reason",
        "payload_json",
        "source_kind",
        "source_family",
    ):
        select.append(name if name in columns else f"NULL AS {name}")
    filters = ["COALESCE(signal_ts, 0) >= ?"]
    eligibility_filter_names = []
    if "observation_status" in columns:
        filters.append("observation_status = 'matured'")
        eligibility_filter_names.append("matured")
    if "kline_covered" in columns:
        filters.append("COALESCE(kline_covered, 0) = 1")
        eligibility_filter_names.append("kline_covered")
    if "baseline_confidence" in columns:
        filters.append("baseline_confidence IN ('high', 'medium')")
        eligibility_filter_names.append("baseline_confidence_high_or_medium")
    if "same_source_path" in columns:
        filters.append("COALESCE(same_source_path, 0) = 1")
        eligibility_filter_names.append("same_source_path")
    if "outlier_flag" in columns:
        filters.append("COALESCE(outlier_flag, 0) = 0")
        eligibility_filter_names.append("not_outlier")
    if "sustained_evaluable" in columns:
        filters.append("COALESCE(sustained_evaluable, 0) = 1")
        eligibility_filter_names.append("sustained_evaluable")
    tier_exprs = []
    if "raw_primary_tier" in columns:
        tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if "raw_sustained_tier" in columns:
        tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    all_rows = db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE COALESCE(signal_ts, 0) >= ?
          AND ({' OR '.join(tier_exprs)})
        """,
        (since_ts,),
    ).fetchall()
    rows = db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE {' AND '.join(filters)}
          AND ({' OR '.join(tier_exprs)})
        """,
        (since_ts,),
    ).fetchall()
    all_out = [normalize_raw_dog_row(dict(row)) for row in all_rows]
    out = [normalize_raw_dog_row(dict(row)) for row in rows]
    all_unique = len({row.get("token_ca") for row in all_out if row.get("token_ca")})
    loaded_unique = len({row.get("token_ca") for row in out if row.get("token_ca")})
    def fails(row, reason):
        if reason == "dropped_not_matured":
            return "observation_status" in columns and row["observation_status"] != "matured"
        if reason == "dropped_kline_uncovered":
            return "kline_covered" in columns and not safe_bool(row["kline_covered"])
        if reason == "dropped_low_confidence":
            return "baseline_confidence" in columns and row["baseline_confidence"] not in ("high", "medium")
        if reason == "dropped_not_same_source_path":
            return "same_source_path" in columns and not safe_bool(row["same_source_path"])
        if reason == "dropped_outlier":
            return "outlier_flag" in columns and safe_bool(row["outlier_flag"])
        if reason == "dropped_not_sustained_evaluable":
            return "sustained_evaluable" in columns and not safe_bool(row["sustained_evaluable"])
        return False
    drop_reasons = (
        "dropped_not_matured",
        "dropped_kline_uncovered",
        "dropped_low_confidence",
        "dropped_not_same_source_path",
        "dropped_outlier",
        "dropped_not_sustained_evaluable",
    )
    drop_breakdown = {
        reason: sum(1 for row in all_rows if fails(row, reason))
        for reason in drop_reasons
    }
    return out, {
        "source": "raw_signal_outcomes",
        "available": True,
        "_raw_all_dogs": all_out,
        "loaded_unique_rows": loaded_unique,
        "loaded_event_rows": len(out),
        "rows_complete_against_summary": True,
        "filters": ["signal_ts_window", *eligibility_filter_names],
        "denominator_audit": {
            "mode": "raw_all_vs_evaluable",
            "raw_all_gold_silver_event_rows": len(all_out),
            "raw_all_gold_silver_unique_tokens": all_unique,
            "evaluable_gold_silver_event_rows": len(out),
            "evaluable_gold_silver_unique_tokens": loaded_unique,
            "filtered_out_event_rows": max(0, len(all_out) - len(out)),
            "filter_drop_breakdown_non_exclusive": drop_breakdown,
            "eligibility_filters_applied": eligibility_filter_names,
            "business_denominator": "raw_all_gold_silver",
            "primary_report_denominator": "evaluable_gold_silver",
            "note": "Drop breakdown is non-exclusive; one row can fail multiple eligibility filters.",
        },
    }


def load_raw_dogs_from_db_path(path, since_ts):
    if not path:
        return [], {"source": "raw_signal_outcomes_db", "available": False}
    raw_db = sqlite3.connect(path)
    raw_db.row_factory = sqlite3.Row
    try:
        rows, meta = load_raw_dogs_from_db(raw_db, since_ts)
        return rows, {
            **meta,
            "source": "raw_signal_outcomes_db",
            "path": path,
        }
    finally:
        raw_db.close()


def load_observations(db, since_ts, max_scan_rows=DEFAULT_MAX_SCAN_ROWS):
    rowid_floor = recent_rowid_floor(db, "candidate_shadow_observations", max_scan_rows)
    filters = ["observed_at >= ?"]
    params = [since_ts]
    if rowid_floor is not None:
        filters.append("rowid >= ?")
        params.append(rowid_floor)
    rows = db.execute(
        f"""
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason,
               observed_at, payload_json
        FROM candidate_shadow_observations
        WHERE {' AND '.join(filters)}
        """,
        tuple(params),
    ).fetchall()
    out = []
    for row in rows:
        payload = jloads(row["payload_json"])
        out.append(
            {
                "signal_id": signal_id_key(row["signal_id"]),
                "token_ca": row["token_ca"],
                "signal_ts": safe_int(row["signal_ts"]),
                "candidate_id": row["candidate_id"],
                "family": row["family"],
                "matched": safe_bool(row["matched"]),
                "reason": row["reason"],
                "observed_at": safe_int(row["observed_at"]),
                "payload": payload,
            }
        )
    observed_values = [row["observed_at"] for row in out if row.get("observed_at")]
    scan_meta = {
        "table": "candidate_shadow_observations",
        "max_scan_rows": max_scan_rows,
        "rowid_floor": rowid_floor,
        "loaded_rows": len(out),
        "earliest_observed_at": min(observed_values) if observed_values else None,
        "latest_observed_at": max(observed_values) if observed_values else None,
        "may_be_rowid_truncated": bool(
            rowid_floor is not None
            and observed_values
            and min(observed_values) > since_ts + 300
        ),
    }
    return out, scan_meta


def raw_dog_index(raw_dogs):
    signal_ids = {row["signal_id"] for row in raw_dogs if row.get("signal_id")}
    tokens = {row["token_ca"] for row in raw_dogs if row.get("token_ca")}
    unique_by_token = {}
    for row in raw_dogs:
        token = row.get("token_ca")
        if token and token not in unique_by_token:
            unique_by_token[token] = row
    return {
        "signal_ids": signal_ids,
        "tokens": tokens,
        "unique_by_token": unique_by_token,
        "event_count": len(raw_dogs),
        "unique_count": len(unique_by_token),
    }


def obs_is_raw_dog(obs, dog_idx):
    if obs["signal_id"] in dog_idx["signal_ids"]:
        return True
    if not dog_idx["signal_ids"] and obs["token_ca"] in dog_idx["tokens"]:
        return True
    return False


def summarize_group(rows, dog_idx, raw_all_dog_idx=None):
    raw_all_dog_idx = raw_all_dog_idx or dog_idx
    signals = {row["signal_id"] for row in rows}
    tokens = {row["token_ca"] for row in rows if row["token_ca"]}
    matched = [row for row in rows if row["matched"]]
    dog_rows = [row for row in rows if obs_is_raw_dog(row, dog_idx)]
    matched_dogs = [row for row in matched if obs_is_raw_dog(row, dog_idx)]
    raw_all_dog_rows = [row for row in rows if obs_is_raw_dog(row, raw_all_dog_idx)]
    matched_raw_all_dogs = [row for row in matched if obs_is_raw_dog(row, raw_all_dog_idx)]
    matched_dog_tokens = {row["token_ca"] for row in matched_dogs if row["token_ca"]}
    matched_raw_all_dog_tokens = {row["token_ca"] for row in matched_raw_all_dogs if row["token_ca"]}
    matched_tokens = {row["token_ca"] for row in matched if row["token_ca"]}
    dog_tokens = {row["token_ca"] for row in dog_rows if row["token_ca"]}
    raw_all_dog_tokens = {row["token_ca"] for row in raw_all_dog_rows if row["token_ca"]}
    return {
        "observation_rows": len(rows),
        "signal_count": len(signals),
        "unique_tokens": len(tokens),
        "match_count": len(matched),
        "non_match_count": len(rows) - len(matched),
        "matched_unique_tokens": len(matched_tokens),
        "gold_silver_event_denominator": len(dog_rows),
        "gold_silver_unique_denominator": len(dog_tokens),
        "matched_gold_silver_events": len(matched_dogs),
        "matched_gold_silver_unique": len(matched_dog_tokens),
        "match_recall_event": rate(len(matched_dogs), len(dog_rows)),
        "match_recall_unique": rate(len(matched_dog_tokens), len(dog_tokens)),
        "raw_all_gold_silver_event_denominator": len(raw_all_dog_rows),
        "raw_all_gold_silver_unique_denominator": len(raw_all_dog_tokens),
        "matched_raw_all_gold_silver_events": len(matched_raw_all_dogs),
        "matched_raw_all_gold_silver_unique": len(matched_raw_all_dog_tokens),
        "business_match_recall_event": rate(len(matched_raw_all_dogs), len(raw_all_dog_rows)),
        "business_match_recall_unique": rate(len(matched_raw_all_dog_tokens), len(raw_all_dog_tokens)),
        "match_precision_event": rate(len(matched_dogs), len(matched)),
        "match_precision_unique": rate(len(matched_dog_tokens), len(matched_tokens)),
        "match_rate": rate(len(matched), len(rows)),
    }


def judge_slice(row):
    dogs = row.get("gold_silver_event_denominator") or 0
    recall = row.get("match_recall_event")
    precision = row.get("match_precision_event")
    recall_lift = row.get("recall_lift_vs_candidate_baseline")
    if dogs < 3 or row.get("signal_count", 0) < 20:
        return "TOO_SMALL"
    if recall is not None and precision is not None and recall_lift is not None:
        if dogs >= 10 and recall >= 0.5 and precision > 0 and recall_lift >= 0.2:
            return "DISCOVERY_HIT"
        if recall_lift > 0 and precision > 0:
            return "WATCH"
    return "NO_SIGNAL"


def build_coverage(observations, expected_count):
    by_signal = defaultdict(set)
    by_candidate = defaultdict(int)
    for row in observations:
        by_signal[row["signal_id"]].add(row["candidate_id"])
        by_candidate[row["candidate_id"]] += 1
    signal_count = len(by_signal)
    expected_rows = signal_count * expected_count
    bad = [
        {"signal_id": signal_id, "candidate_count": len(candidates)}
        for signal_id, candidates in by_signal.items()
        if len(candidates) != expected_count
    ]
    candidate_rows = [
        {
            "candidate_id": candidate_id,
            "observation_count": count,
            "coverage_pct": pct(count, signal_count),
        }
        for candidate_id, count in sorted(by_candidate.items())
    ]
    return {
        "candidate_count_expected": expected_count,
        "candidate_count_observed": len(by_candidate),
        "signal_count": signal_count,
        "observation_rows": len(observations),
        "expected_observation_rows": expected_rows,
        "coverage_pct": pct(len(observations), expected_rows),
        "bad_signal_count": len(bad),
        "bad_signal_sample": bad[:50],
        "candidate_coverage": candidate_rows,
    }


def quote_context_status(payload, keys):
    for applicable_key in ("quote_context_applicable", "source_quote_context_applicable"):
        if applicable_key in payload:
            value = payload.get(applicable_key)
            if isinstance(value, bool) and value is False:
                return "not_applicable"
            if isinstance(value, str) and value.strip().lower() in {"0", "false", "no", "n", "off"}:
                return "not_applicable"
    for key in keys:
        if key not in payload:
            continue
        value = payload.get(key)
        if value == "":
            continue
        if value is None:
            return "unknown"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return "true" if value != 0 else "false"
        text = str(value).strip().lower()
        if text in QUOTE_NOT_APPLICABLE_VALUES:
            return "not_applicable"
        if text in QUOTE_UNKNOWN_VALUES:
            return "unknown"
        if text in {"1", "true", "yes", "y", "on"}:
            return "true"
        if text in {"0", "false", "no", "n", "off"}:
            return "false"
        return "unknown"
    return "missing"


def context_carrier_score(row):
    payload = row["payload"]
    candidate_id = row.get("candidate_id") or ""
    priority = {
        "current_all": 10_000,
        "current_would_enter_all": 9_000,
    }.get(candidate_id, 0)
    context_fields = (
        "context_schema_version",
        "quote_clean_definition",
        "source_quote_clean",
        "source_quote_clean_seen",
        "source_quote_executable",
        "source_quote_executable_proxy",
        "source_component",
        "source_resonance_state",
        "signal_type",
        "lifecycle_profile",
        "lifecycle_state",
        "markov_bucket",
        "volume_profile",
        "candle_pattern",
    )
    populated = sum(1 for field in context_fields if payload.get(field) not in (None, ""))
    return (priority + populated, populated, safe_int(row.get("observed_at")) or 0)


def select_context_carrier_rows(observations):
    by_signal = defaultdict(list)
    for row in observations:
        by_signal[row["signal_id"]].append(row)
    carriers = []
    for rows in by_signal.values():
        carriers.append(max(rows, key=context_carrier_score))
    return carriers


def writer_path(payload):
    for key in (
        "quote_context_writer_path",
        "context_writer_path",
        "writer_path",
        "writer",
        "context_source",
        "source_writer_path",
    ):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return "candidate_shadow_observer:inferred"


def quote_group_key(row, dimension):
    payload = row["payload"]
    if dimension == "by_context_schema_version":
        return str(payload.get("context_schema_version") or "legacy_or_missing")
    if dimension == "by_source_component":
        return str(payload.get("source_component") or "UNKNOWN")
    if dimension == "by_signal_type":
        return str(payload.get("signal_type") or "UNKNOWN")
    if dimension == "by_writer_path":
        return writer_path(payload)
    if dimension == "by_candidate_family":
        return str(row.get("family") or "UNKNOWN")
    if dimension == "by_lifecycle_profile":
        return str(payload.get("lifecycle_profile") or payload.get("lifecycle_state") or "UNKNOWN")
    if dimension == "by_context_carrier_candidate_id":
        return str(row.get("candidate_id") or "UNKNOWN")
    return "UNKNOWN"


def quote_coverage_summary(rows):
    denominator = len(rows)
    clean_counts = defaultdict(int)
    executable_counts = defaultdict(int)
    for row in rows:
        payload = row["payload"]
        clean_counts[quote_context_status(payload, QUOTE_CLEAN_KEYS)] += 1
        executable_counts[quote_context_status(payload, QUOTE_EXECUTABLE_KEYS)] += 1
    return {
        "coverage_denominator_rows": denominator,
        **compact_rate_counts("source_quote_clean", clean_counts, denominator),
        **compact_rate_counts("source_quote_executable", executable_counts, denominator),
    }


def context_field_group_key(row, dimension):
    payload = row["payload"]
    if dimension == "by_context_schema_version":
        return str(payload.get("context_schema_version") or "legacy_or_missing")
    if dimension == "by_writer_path":
        return writer_path(payload)
    if dimension == "by_source_component":
        return str(payload.get("source_component") or "MISSING")
    if dimension == "by_signal_type":
        return str(payload.get("signal_type") or "UNKNOWN")
    if dimension == "by_candidate_family":
        return str(row.get("family") or "UNKNOWN")
    if dimension == "by_context_carrier_candidate_id":
        return str(row.get("candidate_id") or "UNKNOWN")
    if dimension == "by_lifecycle_profile":
        return str(payload.get("lifecycle_profile") or payload.get("lifecycle_state") or "MISSING")
    return "UNKNOWN"


def context_field_status(payload, field, fallback_fields=()):
    value = payload.get(field)
    if value not in (None, ""):
        return "present"
    for fallback in fallback_fields:
        if payload.get(fallback) not in (None, ""):
            return "fallback_present"
    return "missing"


def context_field_coverage_summary(rows, field, fallback_fields=()):
    denominator = len(rows)
    counts = Counter(context_field_status(row["payload"], field, fallback_fields) for row in rows)
    present = counts.get("present", 0)
    fallback_present = counts.get("fallback_present", 0)
    missing = counts.get("missing", 0)
    return {
        "coverage_denominator_rows": denominator,
        "field": field,
        "fallback_fields": list(fallback_fields),
        "present_rows": present,
        "present_rate": rate(present, denominator),
        "fallback_present_rows": fallback_present,
        "fallback_present_rate": rate(fallback_present, denominator),
        "effective_present_rows": present + fallback_present,
        "effective_present_rate": rate(present + fallback_present, denominator),
        "missing_rows": missing,
        "missing_rate": rate(missing, denominator),
    }


def build_context_field_coverage_audit(observations, field, fallback_fields=()):
    carriers = select_context_carrier_rows(observations)
    now_ts = int(time.time())
    top = context_field_coverage_summary(carriers, field, fallback_fields)
    mature_carriers = mature_context_rows(carriers, now_ts)
    mature = context_field_coverage_summary(mature_carriers, field, fallback_fields)
    missing_rows = [
        row for row in carriers
        if context_field_status(row["payload"], field, fallback_fields) == "missing"
    ]
    breakdowns = {}
    for dimension in (
        "by_context_schema_version",
        "by_writer_path",
        "by_source_component",
        "by_signal_type",
        "by_candidate_family",
        "by_context_carrier_candidate_id",
        "by_lifecycle_profile",
    ):
        grouped = defaultdict(list)
        for row in carriers:
            grouped[context_field_group_key(row, dimension)].append(row)
        breakdowns[dimension] = {
            key: context_field_coverage_summary(rows, field, fallback_fields)
            for key, rows in sorted(grouped.items(), key=lambda item: str(item[0]))
        }
    missing_breakdowns = {
        "by_context_schema_version": Counter(),
        "by_writer_path": Counter(),
        "by_source_component": Counter(),
        "by_signal_type": Counter(),
        "by_candidate_family": Counter(),
        "by_context_carrier_candidate_id": Counter(),
        "by_payload_key_presence": Counter(),
    }
    samples = []
    for row in missing_rows:
        payload = row["payload"]
        missing_breakdowns["by_context_schema_version"][str(payload.get("context_schema_version") or "legacy_or_missing")] += 1
        missing_breakdowns["by_writer_path"][writer_path(payload)] += 1
        missing_breakdowns["by_source_component"][str(payload.get("source_component") or "MISSING")] += 1
        missing_breakdowns["by_signal_type"][str(payload.get("signal_type") or "UNKNOWN")] += 1
        missing_breakdowns["by_candidate_family"][str(row.get("family") or "UNKNOWN")] += 1
        missing_breakdowns["by_context_carrier_candidate_id"][str(row.get("candidate_id") or "UNKNOWN")] += 1
        missing_breakdowns["by_payload_key_presence"][payload_key_presence_signature(payload)] += 1
        if len(samples) < 25:
            samples.append(
                {
                    "signal_id": row.get("signal_id"),
                    "token_ca": row.get("token_ca"),
                    "candidate_id": row.get("candidate_id"),
                    "family": row.get("family"),
                    "signal_ts": row.get("signal_ts"),
                    "observed_at": row.get("observed_at"),
                    "context_schema_version": payload.get("context_schema_version"),
                    "writer_path": writer_path(payload),
                    "source_component": payload.get("source_component"),
                    "signal_type": payload.get("signal_type"),
                    "lifecycle_profile": payload.get("lifecycle_profile"),
                    "lifecycle_state": payload.get("lifecycle_state"),
                    "volume_profile": payload.get("volume_profile"),
                    "payload_key_presence": payload_key_presence_signature(payload),
                }
            )
    dominant_missing = None
    if missing_rows:
        writer_counts = missing_breakdowns["by_writer_path"]
        dominant_writer, dominant_count = writer_counts.most_common(1)[0]
        dominant_missing = {
            "dimension": "writer_path",
            "value": dominant_writer,
            "count": dominant_count,
            "share": rate(dominant_count, len(missing_rows)),
        }
    rolling_rate = top.get("effective_present_rate") or 0
    rolling_blocker = f"{field}_coverage_below_80pct" if rolling_rate < 0.8 else None
    mature_rate = mature.get("effective_present_rate") or 0
    mature_enough_rows = (mature.get("coverage_denominator_rows") or 0) >= MATURE_CONTEXT_MIN_ROWS
    maturity_adjusted_blocker = rolling_blocker
    warnings = []
    if field == "lifecycle_profile" and rolling_blocker and mature_enough_rows and mature_rate >= 0.8:
        maturity_adjusted_blocker = None
        warnings.append("lifecycle_profile_rolling_below_80_mature_context_ok")

    top.update(
        {
            "schema_version": "context_field_coverage_audit.v1",
            "coverage_denominator_type": "signal_context_carrier_rows",
            "context_carrier_candidate_ids": sorted({row.get("candidate_id") for row in carriers if row.get("candidate_id")}),
            "rolling_blocker": rolling_blocker,
            "blocker": maturity_adjusted_blocker,
            "mature_context_min_age_sec": MATURE_CONTEXT_MIN_AGE_SEC,
            "mature_context_min_rows": MATURE_CONTEXT_MIN_ROWS,
            "mature_context_enough_rows": mature_enough_rows,
            "mature_context": mature,
            "warnings": warnings,
            "missing_breakdowns": {
                key: dict(counter.most_common())
                for key, counter in missing_breakdowns.items()
            },
            "dominant_missing_bucket": dominant_missing,
            "breakdowns": breakdowns,
            "samples": samples,
            "notes": [
                "Read-only context field coverage audit; it does not alter candidate matching, entry policy, gates, or runtime mode.",
                "effective_present includes the primary field plus explicitly configured fallback fields.",
                "mature_context is a diagnostic window for runtime fields that can arrive after the initial signal; it never changes candidate matching.",
            ],
        }
    )
    return top


def payload_key_presence_signature(payload):
    keys = (
        "source_quote_clean",
        "source_quote_clean_seen",
        "source_quote_executable",
        "source_quote_executable_proxy",
        "quote_context_applicable",
        "source_quote_context_applicable",
        "context_schema_version",
        "quote_clean_definition",
        "source_component",
        "signal_type",
        "lifecycle_profile",
    )
    present = [key for key in keys if payload.get(key) not in (None, "")]
    missing = [key for key in keys if payload.get(key) in (None, "")]
    return "present=" + ",".join(present or ["none"]) + "|missing=" + ",".join(missing or ["none"])


def row_should_be_quote_not_applicable(row):
    payload = row["payload"]
    for key in ("quote_context_applicable", "source_quote_context_applicable"):
        if key not in payload:
            continue
        value = payload.get(key)
        if isinstance(value, bool):
            return value is False
        if isinstance(value, str) and value.strip().lower() in {"0", "false", "no", "n", "off", *QUOTE_NOT_APPLICABLE_VALUES}:
            return True
    signal_type = str(payload.get("signal_type") or "").strip().lower()
    source_component = str(payload.get("source_component") or "").strip().lower()
    writer = writer_path(payload).lower()
    text = " ".join([signal_type, source_component, writer])
    not_applicable_markers = (
        "watch_only",
        "observer_only",
        "no_quote_required",
        "quote_not_applicable",
        "not_applicable",
    )
    return any(marker in text for marker in not_applicable_markers)


def increment_breakdown(target, key):
    target[str(key or "UNKNOWN")] += 1


def build_quote_missing_root_cause_audit(observations):
    carriers = select_context_carrier_rows(observations)
    missing_rows = []
    for row in carriers:
        payload = row["payload"]
        clean_status = quote_context_status(payload, QUOTE_CLEAN_KEYS)
        executable_status = quote_context_status(payload, QUOTE_EXECUTABLE_KEYS)
        if clean_status == "missing" or executable_status == "missing":
            missing_rows.append((row, clean_status, executable_status))

    by_schema = defaultdict(int)
    by_source_component = defaultdict(int)
    by_signal_type = defaultdict(int)
    by_writer_path = defaultdict(int)
    by_lifecycle = defaultdict(int)
    by_payload_presence = defaultdict(int)
    legacy_count = 0
    writer_path_count = 0
    should_be_not_applicable_count = 0
    unknown_count = 0
    samples = []

    for row, clean_status, executable_status in missing_rows:
        payload = row["payload"]
        schema = str(payload.get("context_schema_version") or "legacy_or_missing")
        writer = writer_path(payload)
        source_component = str(payload.get("source_component") or "UNKNOWN")
        signal_type = str(payload.get("signal_type") or "UNKNOWN")
        lifecycle = str(payload.get("lifecycle_profile") or payload.get("lifecycle_state") or "UNKNOWN")
        signature = payload_key_presence_signature(payload)
        increment_breakdown(by_schema, schema)
        increment_breakdown(by_source_component, source_component)
        increment_breakdown(by_signal_type, signal_type)
        increment_breakdown(by_writer_path, writer)
        increment_breakdown(by_lifecycle, lifecycle)
        increment_breakdown(by_payload_presence, signature)

        if schema != EXPECTED_CONTEXT_SCHEMA_VERSION:
            legacy_count += 1
            reason = "legacy_schema"
        elif row_should_be_quote_not_applicable(row):
            should_be_not_applicable_count += 1
            reason = "should_be_not_applicable"
        elif writer:
            writer_path_count += 1
            reason = "v2_writer_path_missing_quote_fields"
        else:
            unknown_count += 1
            reason = "unknown"
        if len(samples) < 25:
            samples.append(
                {
                    "signal_id": row.get("signal_id"),
                    "token_ca": row.get("token_ca"),
                    "candidate_id": row.get("candidate_id"),
                    "family": row.get("family"),
                    "signal_ts": row.get("signal_ts"),
                    "observed_at": row.get("observed_at"),
                    "context_schema_version": schema,
                    "source_component": source_component,
                    "signal_type": signal_type,
                    "writer_path": writer,
                    "lifecycle_profile": lifecycle,
                    "source_quote_clean_status": clean_status,
                    "source_quote_executable_status": executable_status,
                    "payload_key_presence": signature,
                    "root_cause": reason,
                }
            )

    total = len(missing_rows)
    if total <= 0:
        dominant_root_cause = "none"
    elif legacy_count / total >= 0.5:
        dominant_root_cause = "legacy_schema"
    elif should_be_not_applicable_count / total >= 0.5:
        dominant_root_cause = "should_be_not_applicable"
    elif writer_path_count / total >= 0.5:
        dominant_root_cause = "v2_writer_path_missing_quote_fields"
    elif unknown_count:
        dominant_root_cause = "unknown"
    else:
        dominant_root_cause = "mixed"
    return {
        "schema_version": "quote_missing_root_cause_audit.v1",
        "coverage_denominator_type": "signal_context_carrier_rows",
        "coverage_denominator_rows": len(carriers),
        "quote_missing_rows_total": total,
        "missing_by_context_schema_version": dict(sorted(by_schema.items())),
        "missing_by_source_component": dict(sorted(by_source_component.items())),
        "missing_by_signal_type": dict(sorted(by_signal_type.items())),
        "missing_by_writer_path": dict(sorted(by_writer_path.items())),
        "missing_by_lifecycle_profile": dict(sorted(by_lifecycle.items())),
        "missing_by_payload_key_presence": dict(sorted(by_payload_presence.items())),
        "missing_due_to_legacy_schema_count": legacy_count,
        "missing_due_to_writer_path_count": writer_path_count,
        "missing_should_be_not_applicable_count": should_be_not_applicable_count,
        "missing_unknown_count": unknown_count,
        "dominant_root_cause": dominant_root_cause,
        "samples": samples,
        "notes": [
            "Read-only audit; it classifies missing quote context fields but does not alter entry policy or runtime behavior.",
            "source_quote_clean=false and source_quote_executable=false are not counted as missing.",
            "not_applicable is not counted as missing when explicitly encoded.",
        ],
    }


def build_quote_context_coverage_audit(observations):
    carriers = select_context_carrier_rows(observations)
    breakdowns = {}
    for dimension in (
        "by_context_schema_version",
        "by_source_component",
        "by_signal_type",
        "by_writer_path",
        "by_candidate_family",
        "by_lifecycle_profile",
        "by_context_carrier_candidate_id",
    ):
        grouped = defaultdict(list)
        for row in carriers:
            grouped[quote_group_key(row, dimension)].append(row)
        breakdowns[dimension] = {
            key: quote_coverage_summary(rows)
            for key, rows in sorted(grouped.items(), key=lambda item: str(item[0]))
        }
    top = quote_coverage_summary(carriers)
    top.update(
        {
            "schema_version": "quote_context_coverage_audit.v1",
            "coverage_denominator_type": "signal_context_carrier_rows",
            "context_carrier_candidate_ids": sorted({row.get("candidate_id") for row in carriers if row.get("candidate_id")}),
            "breakdowns": breakdowns,
            "definitions": {
                "present_rate": "true + false + not_applicable over signal_context_carrier_rows; unknown and missing are not treated as known coverage.",
                "false_is_present": True,
                "not_applicable_is_present": True,
                "coverage_is_signal_level": True,
                "all_84_candidate_rows_used_as_denominator": False,
            },
        }
    )
    return top


def build_context_health(observations):
    carrier_rows = select_context_carrier_rows(observations)
    rows = [row["payload"] for row in carrier_rows]
    signal_count = len(rows)
    quote_context_coverage = build_quote_context_coverage_audit(observations)
    quote_missing_root_cause = build_quote_missing_root_cause_audit(observations)
    context_field_coverage = {
        "lifecycle_profile": build_context_field_coverage_audit(
            observations, "lifecycle_profile", ("lifecycle_state",)
        ),
        "source_component": build_context_field_coverage_audit(observations, "source_component"),
        "markov_bucket": build_context_field_coverage_audit(observations, "markov_bucket"),
        "volume_profile": build_context_field_coverage_audit(observations, "volume_profile"),
    }
    fields = (
        "source_quote_clean",
        "source_quote_clean_seen",
        "source_quote_executable",
        "source_quote_executable_proxy",
        "source_component",
        "source_resonance_state",
        "lifecycle_profile",
        "lifecycle_state",
        "markov_bucket",
        "volume_profile",
        "candle_pattern",
        "market_cap",
        "fbr_time_legal",
        "fbr_lookahead_warning",
        "signal_price_seen",
        "signal_price_positive",
        "context_schema_version",
        "quote_clean_definition",
    )
    field_coverage = {}
    for field in fields:
        present = sum(1 for payload in rows if payload.get(field) not in (None, ""))
        field_coverage[field] = {
            "signals_present": present,
            "coverage_pct": pct(present, signal_count),
        }
    quote_clean = sum(
        1
        for payload in rows
        if safe_bool(payload.get("source_quote_clean") or payload.get("source_quote_clean_seen"))
    )
    quote_executable = sum(
        1
        for payload in rows
        if safe_bool(payload.get("source_quote_executable") or payload.get("source_quote_executable_proxy"))
    )
    signal_price_only = sum(
        1
        for payload in rows
        if safe_bool(payload.get("signal_price_seen") or payload.get("signal_price_positive"))
        and not safe_bool(payload.get("source_quote_clean") or payload.get("source_quote_clean_seen"))
        and not safe_bool(payload.get("source_quote_executable") or payload.get("source_quote_executable_proxy"))
    )
    schema_versions = defaultdict(int)
    quote_clean_definitions = defaultdict(int)
    for payload in rows:
        schema_versions[str(payload.get("context_schema_version") or "legacy_or_missing")] += 1
        quote_clean_definitions[str(payload.get("quote_clean_definition") or "legacy_or_missing")] += 1
    expected_schema_rows = schema_versions.get(EXPECTED_CONTEXT_SCHEMA_VERSION, 0)
    expected_quote_definition_rows = quote_clean_definitions.get(EXPECTED_QUOTE_CLEAN_DEFINITION, 0)
    expected_schema_coverage_pct = pct(expected_schema_rows, signal_count)
    expected_quote_definition_coverage_pct = pct(expected_quote_definition_rows, signal_count)
    gaps = []
    quote_clean_present_rate = quote_context_coverage.get("source_quote_clean_present_rate")
    quote_executable_present_rate = quote_context_coverage.get("source_quote_executable_present_rate")
    if quote_clean_present_rate is None or quote_clean_present_rate < 0.8:
        gaps.append("source_quote_clean_coverage_below_80pct")
    if quote_executable_present_rate is None or quote_executable_present_rate < 0.8:
        gaps.append("source_quote_executable_coverage_below_80pct")
    for field in ("lifecycle_profile", "markov_bucket", "volume_profile"):
        field_audit = context_field_coverage.get(field) or {}
        blocker = field_audit.get("blocker")
        if blocker:
            gaps.append(blocker)
    if signal_price_only:
        gaps.append("signal_price_seen_without_quote_context_present")
    if expected_schema_coverage_pct is not None and expected_schema_coverage_pct < 95:
        gaps.append("context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked")
    if expected_quote_definition_coverage_pct is not None and expected_quote_definition_coverage_pct < 95:
        gaps.append("quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked")
    return {
        "signal_count": signal_count,
        "field_coverage": field_coverage,
        "quote_context": {
            "source_quote_clean_signals": quote_clean,
            "source_quote_executable_signals": quote_executable,
            "signal_price_seen_without_quote_context_signals": signal_price_only,
        },
        "quote_context_coverage": quote_context_coverage,
        "quote_missing_root_cause": quote_missing_root_cause,
        "context_field_coverage": context_field_coverage,
        "context_schema_versions": dict(sorted(schema_versions.items())),
        "context_schema_version_counts": dict(sorted(schema_versions.items())),
        "expected_context_schema_version": EXPECTED_CONTEXT_SCHEMA_VERSION,
        "expected_context_schema_version_rows": expected_schema_rows,
        "expected_context_schema_version_coverage_pct": expected_schema_coverage_pct,
        "quote_clean_definition_counts": dict(sorted(quote_clean_definitions.items())),
        "expected_quote_clean_definition": EXPECTED_QUOTE_CLEAN_DEFINITION,
        "expected_quote_clean_definition_rows": expected_quote_definition_rows,
        "expected_quote_clean_definition_coverage_pct": expected_quote_definition_coverage_pct,
        "quote_sensitive_slices_evaluable": (
            (expected_schema_coverage_pct is not None and expected_schema_coverage_pct >= 95)
            and (expected_quote_definition_coverage_pct is not None and expected_quote_definition_coverage_pct >= 95)
        ),
        "gaps": gaps,
    }


def build_candidate_baseline(observations, dog_idx, raw_all_dog_idx=None):
    groups = defaultdict(list)
    for row in observations:
        groups[row["candidate_id"]].append(row)
    out = []
    for candidate_id, rows in groups.items():
        summary = summarize_group(rows, dog_idx, raw_all_dog_idx)
        out.append({"candidate_id": candidate_id, "family": rows[0]["family"], **summary})
    out.sort(
        key=lambda row: (
            row.get("match_recall_event") if row.get("match_recall_event") is not None else -1,
            row.get("match_precision_event") if row.get("match_precision_event") is not None else -1,
            row.get("matched_gold_silver_events") or 0,
        ),
        reverse=True,
    )
    return out


def build_context_slices(observations, dog_idx, raw_all_dog_idx, baseline_by_candidate, min_slice_signals):
    buckets = defaultdict(list)
    for row in observations:
        payload = row["payload"]
        for dim in DIMENSIONS:
            buckets[(row["candidate_id"], row["family"], dim, dim_value(payload, dim))].append(row)
    out = []
    for (candidate_id, family, dimension, slice_value), rows in buckets.items():
        if len({row["signal_id"] for row in rows}) < min_slice_signals:
            continue
        summary = summarize_group(rows, dog_idx, raw_all_dog_idx)
        base = baseline_by_candidate.get(candidate_id, {})
        recall = summary.get("match_recall_event")
        precision = summary.get("match_precision_event")
        base_recall = base.get("match_recall_event")
        base_precision = base.get("match_precision_event")
        item = {
            "candidate_id": candidate_id,
            "family": family,
            "dimension": dimension,
            "slice_value": slice_value,
            **summary,
            "baseline_match_recall_event": base_recall,
            "baseline_match_precision_event": base_precision,
            "recall_lift_vs_candidate_baseline": (
                round(recall - base_recall, 6)
                if recall is not None and base_recall is not None
                else None
            ),
            "precision_lift_vs_candidate_baseline": (
                round(precision - base_precision, 6)
                if precision is not None and base_precision is not None
                else None
            ),
        }
        item["judgment"] = judge_slice(item)
        out.append(item)
    order = {"DISCOVERY_HIT": 3, "WATCH": 2, "TOO_SMALL": 1, "NO_SIGNAL": 0}
    out.sort(
        key=lambda row: (
            order.get(row["judgment"], 0),
            row.get("recall_lift_vs_candidate_baseline") if row.get("recall_lift_vs_candidate_baseline") is not None else -999,
            row.get("matched_gold_silver_events") or 0,
            row.get("match_precision_event") if row.get("match_precision_event") is not None else -999,
        ),
        reverse=True,
    )
    return out


def build_missed_attribution(raw_dogs, observations, limit):
    by_signal = defaultdict(list)
    by_token = defaultdict(list)
    for row in observations:
        by_signal[row["signal_id"]].append(row)
        by_token[row["token_ca"]].append(row)
    out = []
    for dog in raw_dogs[:limit]:
        rows = by_signal.get(dog.get("signal_id")) or by_token.get(dog.get("token_ca")) or []
        matched = [row for row in rows if row["matched"]]
        payload = rows[0]["payload"] if rows else {}
        quote_clean = safe_bool(payload.get("source_quote_clean") or payload.get("source_quote_clean_seen"))
        quote_exec = safe_bool(payload.get("source_quote_executable") or payload.get("source_quote_executable_proxy"))
        if not rows:
            miss_stage = "no_candidate_observations"
        elif not matched:
            miss_stage = "no_candidate_match"
        elif not quote_clean and not quote_exec:
            miss_stage = "candidate_match_but_quote_not_clean"
        elif not dog.get("raw_dog_entered"):
            miss_stage = "candidate_match_not_entered"
        elif not dog.get("raw_dog_realized"):
            miss_stage = "entered_not_realized"
        else:
            miss_stage = "captured"
        out.append(
            {
                **dog,
                "observation_rows": len(rows),
                "matched_candidate_count": len(matched),
                "matched_candidates": [row["candidate_id"] for row in matched[:25]],
                "source_quote_clean": quote_clean,
                "source_quote_executable": quote_exec,
                "signal_price_seen": safe_bool(payload.get("signal_price_seen") or payload.get("signal_price_positive")),
                "lifecycle_profile": payload.get("lifecycle_profile"),
                "markov_bucket": payload.get("markov_bucket"),
                "source_component": payload.get("source_component"),
                "source_resonance_state": payload.get("source_resonance_state"),
                "miss_stage": miss_stage,
                "match_confidence": "signal_id" if dog.get("signal_id") in by_signal else ("token" if dog.get("token_ca") in by_token else "none"),
            }
        )
    return out


def build_raw_dog_observation_join(raw_dogs, observations):
    obs_signal_ids = {row["signal_id"] for row in observations if row.get("signal_id")}
    obs_tokens = {row["token_ca"] for row in observations if row.get("token_ca")}
    joined_by_signal = 0
    joined_by_token_fallback = 0
    missing = []
    for dog in raw_dogs:
        signal_id = dog.get("signal_id")
        token = dog.get("token_ca")
        if signal_id and signal_id in obs_signal_ids:
            joined_by_signal += 1
        elif token and token in obs_tokens:
            joined_by_token_fallback += 1
        else:
            missing.append(dog)
    total = len(raw_dogs)
    joined = joined_by_signal + joined_by_token_fallback
    return {
        "raw_dog_event_rows": total,
        "joined_event_rows": joined,
        "joined_by_signal_id": joined_by_signal,
        "joined_by_token_fallback": joined_by_token_fallback,
        "missing_observation_event_rows": len(missing),
        "join_rate": rate(joined, total),
        "missing_sample": missing[:25],
    }


def signal_namespace(value):
    if value in (None, ""):
        return "missing"
    text = str(value).strip()
    if not text:
        return "missing"
    try:
        number = float(text)
        if math.isfinite(number) and number.is_integer():
            return "numeric"
    except Exception:
        pass
    return "string"


def raw_event_identity_key(dog):
    return (
        dog.get("signal_id"),
        dog.get("token_ca"),
        dog.get("signal_ts"),
        dog.get("tier"),
    )


def raw_event_mesh_eligible(dog):
    if not dog.get("signal_id") or not dog.get("token_ca") or not dog.get("signal_ts"):
        return False
    source = str(dog.get("source") or "").strip().lower()
    if source and source not in {"premium_signals", "premium_channel", "local"}:
        return False
    return True


def load_identity_observation_rows(db, raw_dogs, since_ts, expected_candidates):
    signal_ids = sorted({dog.get("signal_id") for dog in raw_dogs if dog.get("signal_id")})
    rows = []
    int_ids = []
    text_ids = []
    for value in signal_ids:
        try:
            number = int(value)
            if str(number) == str(value):
                int_ids.append(number)
            else:
                text_ids.append(value)
        except Exception:
            text_ids.append(value)
    for chunk_values in (int_ids, text_ids):
        for chunk in [chunk_values[i : i + 400] for i in range(0, len(chunk_values), 400)]:
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            if chunk_values is int_ids:
                where = f"signal_id IN ({placeholders})"
            else:
                where = f"CAST(signal_id AS TEXT) IN ({placeholders})"
            rows.extend(
                db.execute(
                    f"""
                    SELECT signal_id, token_ca, signal_ts, candidate_id, observed_at,
                           CASE WHEN candidate_id = 'current_all' THEN payload_json ELSE NULL END AS payload_json
                    FROM candidate_shadow_observations
                    WHERE observed_at >= ?
                      AND {where}
                    """,
                    [since_ts - 3600, *chunk],
                ).fetchall()
            )

    by_signal = defaultdict(list)
    by_token = defaultdict(list)
    lifecycle_to_signal = defaultdict(set)
    for row in rows:
        item = {
            "signal_id": signal_id_key(row["signal_id"]),
            "token_ca": row["token_ca"],
            "signal_ts": safe_int(row["signal_ts"]),
            "candidate_id": row["candidate_id"],
            "observed_at": safe_int(row["observed_at"]),
            "payload": jloads(row["payload_json"]) if row["payload_json"] else {},
        }
        if item["signal_id"]:
            by_signal[item["signal_id"]].append(item)
        if item["token_ca"]:
            by_token[item["token_ca"]].append(item)
        lifecycle_id = item["payload"].get("lifecycle_id")
        if lifecycle_id and item["signal_id"]:
            lifecycle_to_signal[str(lifecycle_id)].add(item["signal_id"])

    signal_candidate_counts = {
        signal_id: len({row["candidate_id"] for row in signal_rows})
        for signal_id, signal_rows in by_signal.items()
    }
    full_coverage_signals = {
        signal_id
        for signal_id, count in signal_candidate_counts.items()
        if count == expected_candidates
    }
    return {
        "by_signal": by_signal,
        "by_token": by_token,
        "lifecycle_to_signal": lifecycle_to_signal,
        "signal_candidate_counts": signal_candidate_counts,
        "full_coverage_signals": full_coverage_signals,
    }


def classify_signal_identity(dog, loaded_obs_by_signal, identity_lookup, seen_keys, expected_candidates):
    key = raw_event_identity_key(dog)
    if key in seen_keys:
        return "raw_event_duplicate", None
    seen_keys.add(key)
    if not dog.get("signal_id"):
        return "raw_event_derived_no_signal", None
    if not raw_event_mesh_eligible(dog):
        return "not_mesh_eligible", None
    signal_id = dog.get("signal_id")
    if signal_id in loaded_obs_by_signal:
        return "joined_exact_signal_id", signal_id
    if signal_id in identity_lookup["by_signal"]:
        return "outside_candidate_observer_window", signal_id
    for alias_id in dog.get("signal_alias_ids") or []:
        if alias_id in loaded_obs_by_signal or alias_id in identity_lookup["by_signal"]:
            return "joined_by_signal_alias", alias_id
    lifecycle_id = dog.get("lifecycle_id")
    if lifecycle_id:
        signals = identity_lookup["lifecycle_to_signal"].get(str(lifecycle_id)) or set()
        if signals:
            return "joined_by_lifecycle_id", sorted(signals)[0]
    token = dog.get("token_ca")
    signal_ts = dog.get("signal_ts")
    if token and signal_ts:
        best = None
        best_dt = None
        for obs in identity_lookup["by_token"].get(token, []):
            obs_ts = obs.get("signal_ts")
            obs_signal_id = obs.get("signal_id")
            if not obs_ts or not obs_signal_id:
                continue
            dt = abs(int(obs_ts) - int(signal_ts))
            if dt <= 900 and obs_signal_id in identity_lookup["full_coverage_signals"]:
                if best_dt is None or dt < best_dt:
                    best = obs_signal_id
                    best_dt = dt
        if best:
            return "joined_by_token_time_high_confidence", best
    if signal_id not in identity_lookup["by_signal"]:
        return "missing_candidate_observation", None
    return "unknown_unjoined", None


def build_signal_identity_reconciliation(db, raw_all_dogs, evaluable_dogs, observations, since_ts, expected_candidates, observation_scan):
    loaded_obs_by_signal = defaultdict(list)
    for obs in observations:
        if obs.get("signal_id"):
            loaded_obs_by_signal[obs["signal_id"]].append(obs)
    identity_lookup = load_identity_observation_rows(db, raw_all_dogs, since_ts, expected_candidates)

    counts = defaultdict(int)
    samples = defaultdict(list)
    seen_keys = set()
    row_results = []
    for dog in raw_all_dogs:
        category, resolved_signal_id = classify_signal_identity(
            dog,
            loaded_obs_by_signal,
            identity_lookup,
            seen_keys,
            expected_candidates,
        )
        counts[category] += 1
        result = {
            "category": category,
            "resolved_signal_id": resolved_signal_id,
            "signal_id": dog.get("signal_id"),
            "raw_signal_id_raw": dog.get("raw_signal_id_raw"),
            "token_ca": dog.get("token_ca"),
            "symbol": dog.get("symbol"),
            "signal_ts": dog.get("signal_ts"),
            "tier": dog.get("tier"),
            "source": dog.get("source"),
            "observation_status": dog.get("observation_status"),
            "evaluable": dog in evaluable_dogs,
        }
        row_results.append(result)
        if len(samples[category]) < 10:
            samples[category].append(result)

    joined_categories = {
        "joined_exact_signal_id",
        "joined_by_signal_alias",
        "joined_by_lifecycle_id",
        "joined_by_token_time_high_confidence",
        "outside_candidate_observer_window",
    }
    unjoined_categories = {
        "missing_candidate_observation",
        "unknown_unjoined",
    }
    deterministic_non_mesh = {
        "not_mesh_eligible",
        "raw_event_duplicate",
        "raw_event_derived_no_signal",
    }
    raw_all_count = len(raw_all_dogs)
    mesh_eligible_count = sum(
        count
        for category, count in counts.items()
        if category not in deterministic_non_mesh
    )
    reconciled_joined = sum(counts[category] for category in joined_categories)
    loaded_exact_joined = counts["joined_exact_signal_id"]
    unjoined = sum(counts[category] for category in unjoined_categories)

    raw_namespaces = defaultdict(int)
    for dog in raw_all_dogs:
        raw_namespaces[signal_namespace(dog.get("raw_signal_id_raw") or dog.get("signal_id"))] += 1
    obs_namespaces = defaultdict(int)
    for obs in observations:
        obs_namespaces[signal_namespace(obs.get("signal_id"))] += 1

    denominator_split = {
        "raw_all_gold_silver": {
            "event_rows": raw_all_count,
            "unique_tokens": len({dog.get("token_ca") for dog in raw_all_dogs if dog.get("token_ca")}),
        },
        "evaluable_gold_silver": {
            "event_rows": len(evaluable_dogs),
            "unique_tokens": len({dog.get("token_ca") for dog in evaluable_dogs if dog.get("token_ca")}),
        },
        "mesh_eligible_gold_silver": {
            "event_rows": mesh_eligible_count,
            "unique_tokens": len({
                row.get("token_ca")
                for row in row_results
                if row["category"] not in deterministic_non_mesh and row.get("token_ca")
            }),
        },
        "joined_gold_silver": {
            "event_rows": reconciled_joined,
            "unique_tokens": len({
                row.get("token_ca")
                for row in row_results
                if row["category"] in joined_categories and row.get("token_ca")
            }),
        },
        "unjoined_gold_silver": {
            "event_rows": unjoined,
            "unique_tokens": len({
                row.get("token_ca")
                for row in row_results
                if row["category"] in unjoined_categories and row.get("token_ca")
            }),
            "by_reason": {category: counts[category] for category in sorted(unjoined_categories)},
        },
    }
    raw_all_signal_id_join_rate = rate(loaded_exact_joined, raw_all_count)
    mesh_eligible_signal_id_join_rate = rate(reconciled_joined, mesh_eligible_count)
    return {
        "schema_version": "signal_identity_reconciliation.v1",
        "joined_exact_signal_id": counts["joined_exact_signal_id"],
        "joined_by_signal_alias": counts["joined_by_signal_alias"],
        "joined_by_lifecycle_id": counts["joined_by_lifecycle_id"],
        "joined_by_token_time_high_confidence": counts["joined_by_token_time_high_confidence"],
        "outside_candidate_observer_window": counts["outside_candidate_observer_window"],
        "not_mesh_eligible": counts["not_mesh_eligible"],
        "missing_candidate_observation": counts["missing_candidate_observation"],
        "raw_event_duplicate": counts["raw_event_duplicate"],
        "raw_event_derived_no_signal": counts["raw_event_derived_no_signal"],
        "unknown_unjoined": counts["unknown_unjoined"],
        "raw_all_signal_id_join_rate": raw_all_signal_id_join_rate,
        "mesh_eligible_signal_id_join_rate": mesh_eligible_signal_id_join_rate,
        "reconciled_joined_event_rows": reconciled_joined,
        "mesh_eligible_event_rows": mesh_eligible_count,
        "signal_id_namespace_report": {
            "raw_signal_id_namespaces": dict(sorted(raw_namespaces.items())),
            "loaded_observation_signal_id_namespaces": dict(sorted(obs_namespaces.items())),
            "full_observation_exact_signal_count": len(identity_lookup["by_signal"]),
            "full_observation_full_coverage_signal_count": len(identity_lookup["full_coverage_signals"]),
            "loaded_observation_signal_count": len(loaded_obs_by_signal),
        },
        "denominator_split": denominator_split,
        "samples_by_reason": {category: rows for category, rows in sorted(samples.items())},
        "raw_all_unjoined_fully_attributed": counts["unknown_unjoined"] == 0,
        "v4_funnel_scope_vs_autoloop_scope_reconciliation": {
            "v4_funnel_scope": "raw dog scoped query: exact raw signal_id IN (...) against candidate_shadow_observations with no rowid scan cap",
            "autoloop_capture_scope": "global candidate_shadow_observations scan constrained by observed_at and max_scan_rows/rowid_floor",
            "difference": "A raw dog can have complete 84-row candidate observations in the DB while being absent from the AutoLoop loaded scan when rowid_floor truncates older observations.",
            "observation_scan_rowid_truncated": bool(observation_scan.get("may_be_rowid_truncated")),
            "observation_scan_rowid_floor": observation_scan.get("rowid_floor"),
            "outside_candidate_observer_window_count": counts["outside_candidate_observer_window"],
            "raw_all_signal_id_join_rate_before_reconciliation": raw_all_signal_id_join_rate,
            "mesh_eligible_signal_id_join_rate_after_reconciliation": mesh_eligible_signal_id_join_rate,
        },
    }


def summarize(
    db_path,
    raw_dog_json,
    raw_db_path,
    hours,
    expected_candidates,
    min_slice_signals,
    limit,
    missed_limit,
    max_scan_rows=DEFAULT_MAX_SCAN_ROWS,
):
    since_ts = int(time.time()) - hours * 3600
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        observations, observation_scan = load_observations(db, since_ts, max_scan_rows=max_scan_rows)
        if raw_dog_json:
            raw_dogs, raw_meta = load_raw_dogs_from_json(raw_dog_json, since_ts)
        elif raw_db_path:
            raw_dogs, raw_meta = load_raw_dogs_from_db_path(raw_db_path, since_ts)
        else:
            raw_dogs, raw_meta = load_raw_dogs_from_db(db, since_ts)
        raw_all_dogs = raw_meta.get("_raw_all_dogs") or raw_dogs
        signal_identity_reconciliation = build_signal_identity_reconciliation(
            db,
            raw_all_dogs,
            raw_dogs,
            observations,
            since_ts,
            expected_candidates,
            observation_scan,
        )
    finally:
        db.close()
    raw_meta_report = {key: value for key, value in raw_meta.items() if key != "_raw_all_dogs"}
    dog_idx = raw_dog_index(raw_dogs)
    raw_all_dog_idx = raw_dog_index(raw_all_dogs)
    coverage = build_coverage(observations, expected_candidates)
    context_health = build_context_health(observations)
    baseline = build_candidate_baseline(observations, dog_idx, raw_all_dog_idx)
    baseline_by_candidate = {row["candidate_id"]: row for row in baseline}
    slices = build_context_slices(observations, dog_idx, raw_all_dog_idx, baseline_by_candidate, min_slice_signals)
    missed = build_missed_attribution(raw_dogs, observations, missed_limit)
    raw_dog_observation_join = build_raw_dog_observation_join(raw_dogs, observations)
    raw_all_observation_join = build_raw_dog_observation_join(raw_all_dogs, observations)
    promotion_blockers = []
    reconciliation_resolves_join = (
        (signal_identity_reconciliation.get("mesh_eligible_signal_id_join_rate") or 0) >= 0.99
        or signal_identity_reconciliation.get("unknown_unjoined", 0) == 0
    )
    raw_all_reconciled = signal_identity_reconciliation.get("raw_all_unjoined_fully_attributed") is True
    if observation_scan.get("may_be_rowid_truncated") and not reconciliation_resolves_join:
        promotion_blockers.append("observation_scan_rowid_truncated")
    if coverage["candidate_count_observed"] != expected_candidates:
        promotion_blockers.append("candidate_count_mismatch")
    if coverage["bad_signal_count"]:
        promotion_blockers.append("per_signal_candidate_coverage_incomplete")
    if not raw_dogs:
        promotion_blockers.append("raw_gold_silver_denominator_unavailable")
    if raw_meta_report.get("rows_complete_against_summary") is False:
        promotion_blockers.append("raw_gold_silver_denominator_rows_truncated")
    if raw_dog_observation_join["missing_observation_event_rows"] and not reconciliation_resolves_join:
        promotion_blockers.append("raw_dog_candidate_observation_join_incomplete")
    if raw_all_observation_join["missing_observation_event_rows"] and not raw_all_reconciled:
        promotion_blockers.append("raw_all_dog_candidate_observation_join_incomplete")
    promotion_blockers.extend(context_health["gaps"])
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "capture_first_candidate_discovery",
        "evidence_level": EVIDENCE_LEVEL,
        "evidence_role": "primary_gold_silver_capture_discovery",
        "can_promote_live": False,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "db": db_path,
        "raw_dog_source": raw_meta_report,
        "hours": hours,
        "since_ts": since_ts,
        "candidate_count_expected": expected_candidates,
        "observation_scan": observation_scan,
        "report_health": {
            "promotion_allowed": False,
            "promotion_blockers": promotion_blockers or ["discovery_same_window_not_promotion_evidence"],
        },
        "definitions": {
            "primary_table": "candidate_shadow_observations",
            "non_match_rows_counted": True,
            "capture_recall": "matched_gold_silver / raw_gold_silver_denominator",
            "business_capture_recall": "matched_raw_all_gold_silver / raw_all_gold_silver_denominator",
            "evaluable_capture_recall": "matched_gold_silver / evaluable_gold_silver_denominator",
            "capture_precision": "matched_gold_silver / candidate_matches",
            "pnl_is_secondary": True,
        },
        "coverage": coverage,
        "context_health": context_health,
        "quote_context_coverage": context_health.get("quote_context_coverage"),
        "quote_missing_root_cause": context_health.get("quote_missing_root_cause"),
        "raw_gold_silver_denominator": {
            "available": bool(raw_dogs),
            "mode": "evaluable_gold_silver",
            "unique_tokens": dog_idx["unique_count"],
            "event_rows": dog_idx["event_count"],
            "expected_unique_from_summary": raw_meta_report.get("expected_unique_from_summary"),
            "expected_event_rows_from_summary": raw_meta_report.get("expected_event_rows_from_summary"),
            "rows_complete_against_summary": raw_meta_report.get("rows_complete_against_summary"),
            "signal_id_denominator_available": bool(dog_idx["signal_ids"]),
            "note": None if raw_dogs else "raw_dog_denominator_unavailable",
        },
        "denominator_audit": raw_meta_report.get("denominator_audit") or {
            "mode": "unavailable",
            "raw_all_gold_silver_event_rows": raw_all_dog_idx["event_count"],
            "raw_all_gold_silver_unique_tokens": raw_all_dog_idx["unique_count"],
            "evaluable_gold_silver_event_rows": dog_idx["event_count"],
            "evaluable_gold_silver_unique_tokens": dog_idx["unique_count"],
        },
        "denominator_split": signal_identity_reconciliation.get("denominator_split"),
        "signal_identity_reconciliation": signal_identity_reconciliation,
        "v4_funnel_scope_vs_autoloop_scope_reconciliation": signal_identity_reconciliation.get(
            "v4_funnel_scope_vs_autoloop_scope_reconciliation"
        ),
        "raw_dog_observation_join": raw_dog_observation_join,
        "raw_all_dog_observation_join": raw_all_observation_join,
        "candidate_baseline": baseline[:limit],
        "context_slices": slices[:limit],
        "judgment_counts": {
            name: sum(1 for row in slices if row.get("judgment") == name)
            for name in ("DISCOVERY_HIT", "WATCH", "TOO_SMALL", "NO_SIGNAL")
        },
        "missed_dog_attribution": missed,
        "watchlist_hypotheses": [
            row
            for row in slices
            if row.get("judgment") in {"DISCOVERY_HIT", "WATCH"}
        ][: min(25, limit)],
        "notes": {
            "promotion": "Same-window discovery can generate hypotheses only; out-of-sample validation is required before policy changes.",
            "markov": "markov_bucket is treated as a context slice when present, not as promotion evidence.",
        },
    }


def self_test():
    now = int(time.time())
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "paper.db"
        raw_path = root / "raw.json"
        db = sqlite3.connect(db_path)
        db.executescript(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            );
            """
        )
        rows = [
            (1, "DOG", now - 300, "cand_a", "base", 1, "hit", now, {"source_quote_clean": True, "volume_profile": "building"}),
            (1, "DOG", now - 300, "cand_b", "base", 0, "miss", now, {"source_quote_clean": True, "volume_profile": "building"}),
            (2, "NORM", now - 240, "cand_a", "base", 0, "miss", now, {"source_quote_clean": False, "volume_profile": "unknown"}),
            (2, "NORM", now - 240, "cand_b", "base", 1, "hit", now, {"source_quote_clean": False, "volume_profile": "unknown"}),
        ]
        for row in rows:
            db.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (*row[:8], json.dumps(row[8])),
            )
        db.commit()
        db.close()
        raw_path.write_text(
            json.dumps({"top_raw_dogs": [{"signal_id": 1, "token_ca": "DOG", "raw_primary_tier": "silver", "signal_ts": now - 300}]}),
            encoding="utf-8",
        )
        out = summarize(str(db_path), str(raw_path), None, 1, 2, 1, 50, 10)
        assert out["coverage"]["coverage_pct"] == 100.0
        assert out["raw_gold_silver_denominator"]["event_rows"] == 1
        cand_a = next(row for row in out["candidate_baseline"] if row["candidate_id"] == "cand_a")
        cand_b = next(row for row in out["candidate_baseline"] if row["candidate_id"] == "cand_b")
        assert cand_a["match_recall_event"] == 1.0
        assert cand_a["match_precision_event"] == 1.0
        assert cand_b["match_recall_event"] == 0.0
        raw_path.write_text(
            json.dumps(
                {
                    "summary": {
                        "raw_sustained_gold_silver_unique": 2,
                        "raw_sustained_gold_silver_event_rows": 2,
                    },
                    "top_raw_dogs": [
                        {"signal_id": 1, "token_ca": "DOG", "raw_primary_tier": "silver", "signal_ts": now - 300}
                    ],
                }
            ),
            encoding="utf-8",
        )
        truncated = summarize(str(db_path), str(raw_path), None, 1, 2, 1, 50, 10)
        assert truncated["raw_gold_silver_denominator"]["rows_complete_against_summary"] is False
        assert "raw_gold_silver_denominator_rows_truncated" in truncated["report_health"]["promotion_blockers"]
        raw_db_path = root / "raw.db"
        raw_db = sqlite3.connect(raw_db_path)
        raw_db.executescript(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              observation_status TEXT, kline_covered INTEGER, baseline_confidence TEXT,
              same_source_path INTEGER, outlier_flag INTEGER, sustained_evaluable INTEGER,
              raw_primary_tier TEXT, raw_sustained_tier TEXT,
              max_sustained_peak_pct REAL, time_to_sustained_peak_sec INTEGER,
              raw_dog_entered INTEGER, raw_dog_realized INTEGER, did_enter INTEGER,
              held_to_silver INTEGER, held_to_gold INTEGER, exit_reason TEXT
            );
            """
        )
        raw_db.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("1", "DOG", "DOG", now - 300, "matured", 1, "high", 1, 0, 1, "silver", "silver", 80.0, 900, 0, 0, 0, 0, 0, None),
                ("3", "BAD", "BAD", now - 300, "matured", 0, "high", 1, 0, 1, "gold", "gold", 150.0, 600, 0, 0, 0, 0, 0, None),
            ],
        )
        raw_db.commit()
        raw_db.close()
        raw_db_out = summarize(str(db_path), None, str(raw_db_path), 1, 2, 1, 50, 10)
        assert raw_db_out["raw_dog_source"]["source"] == "raw_signal_outcomes_db"
        assert raw_db_out["raw_gold_silver_denominator"]["event_rows"] == 1
        assert raw_db_out["raw_gold_silver_denominator"]["rows_complete_against_summary"] is True
        assert raw_db_out["denominator_audit"]["raw_all_gold_silver_event_rows"] == 2
        assert raw_db_out["denominator_audit"]["evaluable_gold_silver_event_rows"] == 1
        assert raw_db_out["denominator_audit"]["filter_drop_breakdown_non_exclusive"]["dropped_kline_uncovered"] == 1
        assert raw_db_out["context_health"]["quote_clean_definition_counts"]["legacy_or_missing"] == 2
        assert "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked" in raw_db_out["report_health"]["promotion_blockers"]
        assert raw_db_out["signal_identity_reconciliation"]["joined_exact_signal_id"] == 1
        assert raw_db_out["signal_identity_reconciliation"]["missing_candidate_observation"] == 1
        assert raw_db_out["signal_identity_reconciliation"]["unknown_unjoined"] == 0
        assert raw_db_out["signal_identity_reconciliation"]["raw_all_unjoined_fully_attributed"] is True
        assert "raw_all_dog_candidate_observation_join_incomplete" not in raw_db_out["report_health"]["promotion_blockers"]
    print("SELF_TEST_PASS offline_candidate_capture_discovery")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="data/paper_trades.db")
    ap.add_argument("--raw-dog-json", default=None)
    ap.add_argument("--raw-db", default=None)
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--expected-candidates", type=int, default=EXPECTED_CANDIDATE_COUNT)
    ap.add_argument("--min-slice-signals", type=int, default=20)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--missed-limit", type=int, default=100)
    ap.add_argument("--max-scan-rows", type=int, default=DEFAULT_MAX_SCAN_ROWS)
    ap.add_argument("--out", default="data/offline_candidate_capture_discovery.json")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return
    result = summarize(
        args.db,
        args.raw_dog_json,
        args.raw_db,
        args.hours,
        args.expected_candidates,
        args.min_slice_signals,
        args.limit,
        args.missed_limit,
        args.max_scan_rows,
    )
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "out": args.out,
                "coverage": result["coverage"],
                "raw_gold_silver_denominator": result["raw_gold_silver_denominator"],
                "judgment_counts": result["judgment_counts"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
