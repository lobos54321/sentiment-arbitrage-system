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
from collections import defaultdict
from pathlib import Path


EXPECTED_CANDIDATE_COUNT = 84
SCHEMA_VERSION = "offline_candidate_capture_discovery.v1"
EVIDENCE_LEVEL = "discovery_same_window"

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


def table_exists(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


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
    signal_id = signal_id_key(row.get("signal_id"))
    token_ca = row.get("token_ca") or row.get("token") or row.get("ca")
    signal_ts = safe_int(row.get("signal_ts") or row.get("timestamp"))
    return {
        "signal_id": signal_id,
        "token_ca": str(token_ca) if token_ca not in (None, "") else None,
        "symbol": row.get("symbol"),
        "signal_ts": signal_ts,
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
        "summary": summary or None,
        "expected_unique_from_summary": expected_unique,
        "expected_event_rows_from_summary": expected_events,
        "loaded_unique_rows": loaded_unique,
        "loaded_event_rows": loaded_events,
        "rows_complete_against_summary": rows_complete,
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
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
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
    ):
        select.append(name if name in columns else f"NULL AS {name}")
    filters = ["COALESCE(signal_ts, 0) >= ?"]
    if "observation_status" in columns:
        filters.append("observation_status = 'matured'")
    if "kline_covered" in columns:
        filters.append("COALESCE(kline_covered, 0) = 1")
    if "baseline_confidence" in columns:
        filters.append("baseline_confidence IN ('high', 'medium')")
    if "same_source_path" in columns:
        filters.append("COALESCE(same_source_path, 0) = 1")
    if "outlier_flag" in columns:
        filters.append("COALESCE(outlier_flag, 0) = 0")
    if "sustained_evaluable" in columns:
        filters.append("COALESCE(sustained_evaluable, 0) = 1")
    tier_exprs = []
    if "raw_primary_tier" in columns:
        tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if "raw_sustained_tier" in columns:
        tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    rows = db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE {' AND '.join(filters)}
          AND ({' OR '.join(tier_exprs)})
        """,
        (since_ts,),
    ).fetchall()
    out = [normalize_raw_dog_row(dict(row)) for row in rows]
    loaded_unique = len({row.get("token_ca") for row in out if row.get("token_ca")})
    return out, {
        "source": "raw_signal_outcomes",
        "available": True,
        "loaded_unique_rows": loaded_unique,
        "loaded_event_rows": len(out),
        "rows_complete_against_summary": True,
        "filters": [
            "signal_ts_window",
            *[
                name
                for name, present in (
                    ("matured", "observation_status" in columns),
                    ("kline_covered", "kline_covered" in columns),
                    ("baseline_confidence_high_or_medium", "baseline_confidence" in columns),
                    ("same_source_path", "same_source_path" in columns),
                    ("not_outlier", "outlier_flag" in columns),
                    ("sustained_evaluable", "sustained_evaluable" in columns),
                )
                if present
            ],
        ],
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


def load_observations(db, since_ts):
    rows = db.execute(
        """
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason,
               observed_at, payload_json
        FROM candidate_shadow_observations
        WHERE observed_at >= ?
        """,
        (since_ts,),
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
    return out


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


def summarize_group(rows, dog_idx):
    signals = {row["signal_id"] for row in rows}
    tokens = {row["token_ca"] for row in rows if row["token_ca"]}
    matched = [row for row in rows if row["matched"]]
    dog_rows = [row for row in rows if obs_is_raw_dog(row, dog_idx)]
    matched_dogs = [row for row in matched if obs_is_raw_dog(row, dog_idx)]
    matched_dog_tokens = {row["token_ca"] for row in matched_dogs if row["token_ca"]}
    matched_tokens = {row["token_ca"] for row in matched if row["token_ca"]}
    dog_tokens = {row["token_ca"] for row in dog_rows if row["token_ca"]}
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


def build_context_health(observations):
    signal_payloads = {}
    for row in observations:
        signal_payloads.setdefault(row["signal_id"], row["payload"])
    rows = list(signal_payloads.values())
    signal_count = len(rows)
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
    for payload in rows:
        schema_versions[str(payload.get("context_schema_version") or "legacy_or_missing")] += 1
    gaps = []
    for field in ("source_quote_clean", "source_quote_executable", "lifecycle_profile", "markov_bucket", "volume_profile"):
        coverage = field_coverage[field]["coverage_pct"]
        if coverage is None or coverage < 80:
            gaps.append(f"{field}_coverage_below_80pct")
    if signal_price_only:
        gaps.append("signal_price_seen_without_quote_context_present")
    return {
        "signal_count": signal_count,
        "field_coverage": field_coverage,
        "quote_context": {
            "source_quote_clean_signals": quote_clean,
            "source_quote_executable_signals": quote_executable,
            "signal_price_seen_without_quote_context_signals": signal_price_only,
        },
        "context_schema_versions": dict(sorted(schema_versions.items())),
        "gaps": gaps,
    }


def build_candidate_baseline(observations, dog_idx):
    groups = defaultdict(list)
    for row in observations:
        groups[row["candidate_id"]].append(row)
    out = []
    for candidate_id, rows in groups.items():
        summary = summarize_group(rows, dog_idx)
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


def build_context_slices(observations, dog_idx, baseline_by_candidate, min_slice_signals):
    buckets = defaultdict(list)
    for row in observations:
        payload = row["payload"]
        for dim in DIMENSIONS:
            buckets[(row["candidate_id"], row["family"], dim, dim_value(payload, dim))].append(row)
    out = []
    for (candidate_id, family, dimension, slice_value), rows in buckets.items():
        if len({row["signal_id"] for row in rows}) < min_slice_signals:
            continue
        summary = summarize_group(rows, dog_idx)
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


def summarize(db_path, raw_dog_json, raw_db_path, hours, expected_candidates, min_slice_signals, limit, missed_limit):
    since_ts = int(time.time()) - hours * 3600
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        observations = load_observations(db, since_ts)
        if raw_dog_json:
            raw_dogs, raw_meta = load_raw_dogs_from_json(raw_dog_json, since_ts)
        elif raw_db_path:
            raw_dogs, raw_meta = load_raw_dogs_from_db_path(raw_db_path, since_ts)
        else:
            raw_dogs, raw_meta = load_raw_dogs_from_db(db, since_ts)
    finally:
        db.close()
    dog_idx = raw_dog_index(raw_dogs)
    coverage = build_coverage(observations, expected_candidates)
    context_health = build_context_health(observations)
    baseline = build_candidate_baseline(observations, dog_idx)
    baseline_by_candidate = {row["candidate_id"]: row for row in baseline}
    slices = build_context_slices(observations, dog_idx, baseline_by_candidate, min_slice_signals)
    missed = build_missed_attribution(raw_dogs, observations, missed_limit)
    promotion_blockers = []
    if coverage["candidate_count_observed"] != expected_candidates:
        promotion_blockers.append("candidate_count_mismatch")
    if coverage["bad_signal_count"]:
        promotion_blockers.append("per_signal_candidate_coverage_incomplete")
    if not raw_dogs:
        promotion_blockers.append("raw_gold_silver_denominator_unavailable")
    if raw_meta.get("rows_complete_against_summary") is False:
        promotion_blockers.append("raw_gold_silver_denominator_rows_truncated")
    promotion_blockers.extend(context_health["gaps"])
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "capture_first_candidate_discovery",
        "evidence_level": EVIDENCE_LEVEL,
        "evidence_role": "primary_gold_silver_capture_discovery",
        "can_promote_live": False,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "db": db_path,
        "raw_dog_source": raw_meta,
        "hours": hours,
        "since_ts": since_ts,
        "candidate_count_expected": expected_candidates,
        "report_health": {
            "promotion_allowed": False,
            "promotion_blockers": promotion_blockers or ["discovery_same_window_not_promotion_evidence"],
        },
        "definitions": {
            "primary_table": "candidate_shadow_observations",
            "non_match_rows_counted": True,
            "capture_recall": "matched_gold_silver / raw_gold_silver_denominator",
            "capture_precision": "matched_gold_silver / candidate_matches",
            "pnl_is_secondary": True,
        },
        "coverage": coverage,
        "context_health": context_health,
        "raw_gold_silver_denominator": {
            "available": bool(raw_dogs),
            "unique_tokens": dog_idx["unique_count"],
            "event_rows": dog_idx["event_count"],
            "expected_unique_from_summary": raw_meta.get("expected_unique_from_summary"),
            "expected_event_rows_from_summary": raw_meta.get("expected_event_rows_from_summary"),
            "rows_complete_against_summary": raw_meta.get("rows_complete_against_summary"),
            "signal_id_denominator_available": bool(dog_idx["signal_ids"]),
            "note": None if raw_dogs else "raw_dog_denominator_unavailable",
        },
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
