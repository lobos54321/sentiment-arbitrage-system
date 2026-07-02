#!/usr/bin/env python3
"""Read-only volume/kline coverage root-cause audit.

This audit supports the Gold/Silver 60% Capture Loop in discovery/readiness
mode. It only reads candidate shadow observations and raw signal outcomes. It
never backfills kline data, changes strategy, gates, entry policy, execution
mode, canary size, wallet, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter
from pathlib import Path


SCHEMA_VERSION = "volume_kline_coverage_audit.v2.p1_matured_confidence_adjusted"
DEFAULT_CONTEXT_CARRIER = "current_all"
UNKNOWN_VALUES = {"", "unknown", "unk", "null", "none"}
NOT_APPLICABLE_VALUES = {"not_applicable", "not-applicable", "n/a", "na", "not applicable"}
P1_VOLUME_METHOD_VERSION = "p1_matured_recompute_v2"
P1_KLINE_METHOD_VERSION = "p1_confidence_time_legal_v2"


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def pct(num, den):
    value = rate(num, den)
    return None if value is None else round(value * 100.0, 4)


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def cols(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def number(value):
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed


def norm_text(value):
    if value is None:
        return "MISSING"
    text = str(value).strip()
    return text if text else "UNKNOWN"


def payload_dim(payload, key):
    if key not in payload:
        return "MISSING"
    value = payload.get(key)
    if value is None:
        return "UNKNOWN"
    text = str(value).strip()
    return text if text else "UNKNOWN"


def value_status(payload, key):
    if key not in payload:
        return "missing"
    value = payload.get(key)
    if value is None:
        return "unknown"
    text = str(value).strip().lower()
    if text in UNKNOWN_VALUES:
        return "unknown"
    if text in NOT_APPLICABLE_VALUES:
        return "not_applicable"
    return "known"


def load_context_rows(db, since_ts, context_carrier):
    if not table_exists(db, "candidate_shadow_observations"):
        return []
    rows = db.execute(
        """
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json
        FROM candidate_shadow_observations
        WHERE candidate_id = ?
          AND COALESCE(observed_at, 0) >= ?
        ORDER BY observed_at ASC
        """,
        (context_carrier, int(since_ts)),
    ).fetchall()
    return [(dict(row), jloads(row["payload_json"])) for row in rows]


def breakdown(rows, predicate):
    dims = {
        "by_context_schema_version": "context_schema_version",
        "by_source_component": "source_component",
        "by_writer_path": "quote_context_writer_path",
        "by_volume_profile_reason": "volume_profile_reason",
        "by_lifecycle_profile": "lifecycle_profile",
        "by_signal_type": "signal_type",
        "by_candidate_family": "candidate_family",
    }
    out = {}
    selected = [(row, payload) for row, payload in rows if predicate(row, payload)]
    for label, key in dims.items():
        out[label] = dict(Counter(payload_dim(payload, key) for _row, payload in selected).most_common())
    return out


def bucket_bar_count(value):
    parsed = number(value)
    if parsed is None:
        return "missing"
    count = int(parsed)
    if count <= 0:
        return "0"
    if count == 1:
        return "1"
    if count == 2:
        return "2"
    if count < 5:
        return "3_4"
    if count < 10:
        return "5_9"
    return "gte_10"


def bucket_signal_age(row, payload):
    observed = number(row.get("observed_at"))
    signal_ts = number(row.get("signal_ts"))
    if signal_ts is None:
        signal_ts = number(payload.get("signal_ts"))
    if observed is None or signal_ts is None:
        return "missing"
    age = observed - signal_ts
    if age < 0:
        return "invalid_negative"
    if age < 60:
        return "lt_60s"
    if age < 180:
        return "60_180s"
    if age < 300:
        return "180_300s"
    if age < 900:
        return "300_900s"
    return "gte_900s"


def volume_profile_from_bars(bars):
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


def volume_profile_reason_from_bars(bars):
    if not bars:
        return "kline_bars_unavailable"
    if len(bars[:5]) < 3:
        return "insufficient_kline_bars_lt_3"
    return "classified_from_first_5_bars"


def load_kline_bars(kline_db, token_ca, signal_ts, limit):
    if kline_db is None or not token_ca or signal_ts is None or not table_exists(kline_db, "kline_1m"):
        return []
    floor_ts = int(float(signal_ts) // 60 * 60)
    try:
        rows = kline_db.execute(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM kline_1m
            WHERE token_ca = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (token_ca, floor_ts, int(limit)),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


def matured_volume_recompute(rows, kline_db, kline_limit):
    status_counts = Counter(value_status(payload, "volume_profile") for _row, payload in rows)
    strict_known = status_counts.get("known", 0)
    target_rows = [
        (row, payload)
        for row, payload in rows
        if value_status(payload, "volume_profile") in {"missing", "unknown"}
    ]
    profile_counts = Counter()
    reason_counts = Counter()
    recovered = 0
    still_unknown = 0
    kline_available = bool(kline_db is not None and table_exists(kline_db, "kline_1m"))
    samples = []
    if kline_available:
        for row, payload in target_rows:
            signal_ts = number(row.get("signal_ts"))
            if signal_ts is None:
                signal_ts = number(payload.get("signal_ts"))
            bars = load_kline_bars(kline_db, row.get("token_ca"), signal_ts, kline_limit)
            first = bars[:5]
            profile = volume_profile_from_bars(first)
            reason = volume_profile_reason_from_bars(first)
            profile_counts[profile] += 1
            reason_counts[reason] += 1
            if profile == "unknown":
                still_unknown += 1
            else:
                recovered += 1
                if len(samples) < 20:
                    samples.append({
                        "signal_id": row.get("signal_id"),
                        "token_ca": row.get("token_ca"),
                        "original_volume_profile": payload.get("volume_profile"),
                        "original_volume_profile_reason": payload.get("volume_profile_reason"),
                        "matured_volume_profile": profile,
                        "matured_volume_profile_reason": reason,
                        "matured_kline_bar_count": len(bars),
                    })
    canonical_known = strict_known + recovered
    return {
        "available": kline_available,
        "method_version": P1_VOLUME_METHOD_VERSION,
        "method_change": "matured_volume_recompute_canonical_known_rate_raw_first_look_secondary",
        "target_rows": len(target_rows),
        "strict_first_look_known_rows": strict_known,
        "strict_first_look_known_rate": rate(strict_known, len(rows)),
        "matured_recomputed_known_rows": recovered,
        "matured_recomputed_unknown_rows": still_unknown,
        "matured_recomputed_profile_counts": dict(profile_counts.most_common()),
        "matured_recomputed_reason_counts": dict(reason_counts.most_common()),
        "canonical_known_rows": canonical_known,
        "canonical_known_rate": rate(canonical_known, len(rows)),
        "strict_first_look_preserved": True,
        "canonical_backfill_performed": False,
        "samples": samples,
    }


def volume_context_audit(rows, matured_recompute=None):
    den = len(rows)
    status_counts = Counter(value_status(payload, "volume_profile") for _row, payload in rows)
    value_counts = Counter()
    for _row, payload in rows:
        if value_status(payload, "volume_profile") == "known":
            value_counts[str(payload.get("volume_profile")).strip().lower()] += 1
    known = status_counts.get("known", 0)
    missing = status_counts.get("missing", 0)
    unknown = status_counts.get("unknown", 0)
    not_applicable = status_counts.get("not_applicable", 0)
    field_present = den - missing
    strict_known = known
    strict_unknown = unknown
    strict_missing = missing
    strict_blocker = known < den * 0.8
    if matured_recompute and matured_recompute.get("available"):
        known = int(matured_recompute.get("canonical_known_rows") or known)
        unknown = max(0, den - known - not_applicable)
    blocker = known < den * 0.8
    missing_or_unknown = lambda _row, payload: value_status(payload, "volume_profile") in {"missing", "unknown"}
    root_causes = []
    missing_writer = breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "missing")["by_writer_path"]
    if missing_writer.get("MISSING", 0) or missing_writer.get("candidate_shadow_observer:inferred", 0):
        root_causes.append("volume_profile_missing_in_context_carrier_payload")
    if strict_unknown:
        root_causes.append("volume_profile_unknown_from_insufficient_or_unclassified_kline")
    if not root_causes and blocker:
        root_causes.append("volume_profile_coverage_below_threshold")
    if not blocker and strict_blocker and matured_recompute:
        root_causes.append("p1_matured_volume_recompute_cleared_strict_first_look_blocker")
    unknown_rows = [(row, payload) for row, payload in rows if value_status(payload, "volume_profile") == "unknown"]
    return {
        "coverage_denominator_type": "signal_context_carrier_rows",
        "coverage_method_version": P1_VOLUME_METHOD_VERSION if matured_recompute else "strict_first_look_v1",
        "coverage_method_change": (
            "matured_volume_recompute_canonical_known_rate_raw_first_look_secondary"
            if matured_recompute
            else "none"
        ),
        "strict_first_look_preserved": bool(matured_recompute),
        "context_carrier_candidate_id": DEFAULT_CONTEXT_CARRIER,
        "rows_scanned": den,
        "field_present_rows": field_present,
        "field_present_rate": rate(field_present, den),
        "known_rows": known,
        "known_rate": rate(known, den),
        "strict_first_look_known_rows": strict_known,
        "strict_first_look_known_rate": rate(strict_known, den),
        "strict_first_look_unknown_rows": strict_unknown,
        "strict_first_look_unknown_rate": rate(strict_unknown, den),
        "strict_first_look_missing_rows": strict_missing,
        "strict_first_look_missing_rate": rate(strict_missing, den),
        "strict_first_look_blocker": "volume_profile_coverage_below_80pct" if strict_blocker else None,
        "missing_rows": missing,
        "missing_rate": rate(missing, den),
        "unknown_rows": unknown,
        "unknown_rate": rate(unknown, den),
        "not_applicable_rows": not_applicable,
        "not_applicable_rate": rate(not_applicable, den),
        "value_counts": dict(value_counts.most_common()),
        "blocker": "volume_profile_coverage_below_80pct" if blocker else None,
        "matured_recompute": matured_recompute or {},
        "root_causes": root_causes,
        "missing_or_unknown_breakdown": breakdown(rows, missing_or_unknown),
        "missing_breakdown": breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "missing"),
        "unknown_breakdown": breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "unknown"),
        "unknown_diagnostics": {
            "kline_bar_count_bucket_counts": dict(
                Counter(bucket_bar_count(payload.get("kline_bar_count")) for _row, payload in unknown_rows).most_common()
            ),
            "signal_age_bucket_counts": dict(
                Counter(bucket_signal_age(row, payload) for row, payload in unknown_rows).most_common()
            ),
            "volume_profile_reason_counts": dict(
                Counter(payload_dim(payload, "volume_profile_reason") for _row, payload in unknown_rows).most_common()
            ),
            "kline_missing_reason_counts": dict(
                Counter(payload_dim(payload, "kline_missing_reason") for _row, payload in unknown_rows).most_common()
            ),
            "unknown_samples": [
                {
                    "signal_id": row.get("signal_id"),
                    "token_ca": row.get("token_ca"),
                    "observed_at": row.get("observed_at"),
                    "signal_ts": row.get("signal_ts"),
                    "signal_age_bucket": bucket_signal_age(row, payload),
                    "kline_bar_count": payload.get("kline_bar_count"),
                    "volume_profile_reason": payload.get("volume_profile_reason"),
                    "kline_missing_reason": payload.get("kline_missing_reason"),
                }
                for row, payload in unknown_rows[:20]
            ],
        },
    }


def compact_volume_context_audit(audit):
    diagnostics = audit.get("unknown_diagnostics") or {}
    return {
        "rows_scanned": audit.get("rows_scanned"),
        "field_present_rate": audit.get("field_present_rate"),
        "known_rate": audit.get("known_rate"),
        "missing_rate": audit.get("missing_rate"),
        "unknown_rate": audit.get("unknown_rate"),
        "blocker": audit.get("blocker"),
        "root_causes": audit.get("root_causes") or [],
        "unknown_volume_profile_reason_counts": diagnostics.get("volume_profile_reason_counts") or {},
        "unknown_kline_missing_reason_counts": diagnostics.get("kline_missing_reason_counts") or {},
        "unknown_kline_bar_count_bucket_counts": diagnostics.get("kline_bar_count_bucket_counts") or {},
    }


def volume_context_recent_windows(rows, now_ts, windows_hours=(1, 2, 4, 6, 12, 24)):
    out = {}
    for hours in windows_hours:
        since = int(now_ts) - int(hours * 3600)
        subset = [
            (row, payload) for row, payload in rows
            if int(row.get("observed_at") or 0) >= since
        ]
        out[f"{hours}h"] = compact_volume_context_audit(volume_context_audit(subset))
    return out


def first_counter_key(counter):
    if not counter:
        return None
    return next(iter(counter.keys()))


def volume_context_resolution(volume):
    diagnostics = volume.get("unknown_diagnostics") or {}
    reason_counts = diagnostics.get("volume_profile_reason_counts") or {}
    missing_reason_counts = diagnostics.get("kline_missing_reason_counts") or {}
    bar_count_buckets = diagnostics.get("kline_bar_count_bucket_counts") or {}
    field_present_rate = volume.get("field_present_rate")
    known_rate = volume.get("known_rate")
    missing_rate = volume.get("missing_rate")
    unknown_rate = volume.get("unknown_rate")
    writer_field_present = (
        field_present_rate is not None
        and float(field_present_rate) >= 0.99
        and float(missing_rate or 0) == 0.0
    )
    formal_clean = known_rate is not None and float(known_rate) >= 0.8
    unknown_from_kline_maturity = bool(
        reason_counts.get("insufficient_kline_bars_lt_3")
        or reason_counts.get("kline_bars_unavailable")
    )
    if formal_clean:
        classification = "VOLUME_FORMAL_CONTEXT_CLEAN"
        next_action = "allow_discovery_only_volume_profile_slices"
    elif not writer_field_present:
        classification = "VOLUME_CONTEXT_WRITER_OR_CARRIER_INCOMPLETE"
        next_action = "inspect_volume_context_writer_or_carrier_payload"
    elif unknown_from_kline_maturity:
        classification = "VOLUME_FORMAL_CONTEXT_BLOCKED_SHADOW_MATURED_RECHECK_AVAILABLE"
        next_action = "review_matured_volume_shadow_recheck_and_kline_resolution_before_formal_volume_promotion"
    else:
        classification = "VOLUME_FORMAL_CONTEXT_BLOCKED_UNKNOWN_CLASSIFICATION"
        next_action = "decompose_volume_profile_unknown_classification_before_using_volume_slices"
    return {
        "classification": classification,
        "next_action": next_action,
        "formal_volume_profile_known_rate": known_rate,
        "formal_volume_profile_known_rows": volume.get("known_rows"),
        "formal_volume_profile_unknown_rate": unknown_rate,
        "formal_volume_profile_unknown_rows": volume.get("unknown_rows"),
        "field_present_rate": field_present_rate,
        "missing_rate": missing_rate,
        "writer_field_present": writer_field_present,
        "primary_unknown_reason": first_counter_key(reason_counts),
        "unknown_volume_profile_reason_counts": reason_counts,
        "unknown_kline_missing_reason_counts": missing_reason_counts,
        "unknown_kline_bar_count_bucket_counts": bar_count_buckets,
        "matured_volume_shadow_recheck_recommended": bool(
            not formal_clean and writer_field_present and unknown_from_kline_maturity
        ),
        "formal_volume_slices_blocked": not formal_clean,
        "allowed_use": (
            "formal_discovery_context_evidence"
            if formal_clean
            else "shadow_only_matured_volume_recheck"
            if writer_field_present and unknown_from_kline_maturity
            else "data_quality_audit_only"
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def bucket_lag(value):
    if value is None:
        return "missing"
    try:
        number = float(value)
    except Exception:
        return "invalid"
    if number <= 60:
        return "le_60s"
    if number <= 300:
        return "le_300s"
    if number <= 900:
        return "le_900s"
    return "gt_900s"


def bucket_baseline_lag(value):
    parsed = number(value)
    if parsed is None:
        return "missing"
    if parsed < 0:
        return "invalid_negative"
    if parsed <= 10:
        return "high_le_10s"
    if parsed <= 30:
        return "medium_10_30s"
    if parsed <= 60:
        return "low_30_60s"
    if parsed <= 120:
        return "low_60_120s"
    if parsed <= 300:
        return "low_120_300s"
    return "not_evaluable_gt_300s"


def bucket_pct(value):
    if value is None:
        return "missing"
    try:
        number = float(value)
    except Exception:
        return "invalid"
    if number >= 80:
        return "gte_80"
    if number >= 50:
        return "50_80"
    if number >= 20:
        return "20_50"
    return "lt_20"


def kline_uncovered_root_cause(row):
    if truthy(row.get("kline_covered")):
        return "covered"
    if truthy(row.get("outlier_flag")):
        return "outlier_price"
    if not truthy(row.get("same_source_path")):
        return "not_same_source_path"
    confidence = norm_text(row.get("baseline_confidence")).lower()
    if confidence not in {"high", "medium"}:
        if confidence == "low":
            return f"baseline_confidence_low_{bucket_baseline_lag(row.get('baseline_lag_sec'))}"
        return f"baseline_confidence_{confidence}"
    reason = norm_text(row.get("coverage_reason")).lower()
    if reason not in {"missing", "unknown", "covered"}:
        return f"coverage_reason_{reason}"
    if row.get("first_bar_lag_sec") is None:
        return "missing_first_bar"
    return "kline_covered_false_composite_unknown"


def select_expr(columns, names):
    return [name if name in columns else f"NULL AS {name}" for name in names]


def is_gold_silver_sql(columns):
    exprs = []
    if "raw_primary_tier" in columns:
        exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if "raw_sustained_tier" in columns:
        exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    return " OR ".join(exprs) if exprs else None


def raw_kline_audit(raw_db, since_ts):
    empty = {
        "available": False,
        "source": "raw_signal_outcomes",
        "raw_all_gold_silver_event_rows": 0,
        "raw_all_gold_silver_unique_tokens": 0,
        "kline_covered_rows": 0,
        "kline_uncovered_rows": 0,
        "kline_coverage_rate": None,
        "blocker": "kline_coverage_unavailable",
        "reason": None,
    }
    if raw_db is None or not table_exists(raw_db, "raw_signal_outcomes"):
        return {**empty, "reason": "raw_signal_outcomes_unavailable"}
    columns = cols(raw_db, "raw_signal_outcomes")
    tier_expr = is_gold_silver_sql(columns)
    if not tier_expr:
        return {**empty, "reason": "missing_gold_silver_tier_columns"}
    names = (
        "id",
        "signal_id",
        "token_ca",
        "signal_ts",
        "signal_type",
        "route",
        "hard_gate_status",
        "observation_status",
        "kline_covered",
        "coverage_reason",
        "pool_found",
        "provider",
        "baseline_lag_sec",
        "baseline_confidence",
        "same_source_path",
        "source_kind",
        "source_family",
        "path_source_kind",
        "path_source_family",
        "first_bar_lag_sec",
        "early_15m_bar_count",
        "early_15m_expected_minutes",
        "early_15m_bar_coverage_pct",
        "early_15m_complete",
        "outlier_flag",
        "sustained_evaluable",
        "time_to_sustained_peak_sec",
        "raw_primary_tier",
        "raw_sustained_tier",
    )
    rows = raw_db.execute(
        f"""
        SELECT {", ".join(select_expr(columns, names))}
        FROM raw_signal_outcomes
        WHERE COALESCE(signal_ts, 0) >= ?
          AND ({tier_expr})
        """,
        (int(since_ts),),
    ).fetchall()
    if not rows:
        return {**empty, "available": True, "reason": "raw_gold_silver_empty", "blocker": None}
    rows = [dict(row) for row in rows]
    covered_rows = [row for row in rows if truthy(row.get("kline_covered"))]
    uncovered_rows = [row for row in rows if not truthy(row.get("kline_covered"))]
    unique_tokens = len({row.get("token_ca") for row in rows if row.get("token_ca")})
    coverage_rate = rate(len(covered_rows), len(rows))

    def count(key, source_rows=rows):
        return dict(Counter(norm_text(row.get(key)) for row in source_rows).most_common())

    primary_drop_order = (
        ("not_matured", lambda row: row.get("observation_status") != "matured"),
        ("kline_uncovered", lambda row: not truthy(row.get("kline_covered"))),
        ("low_confidence", lambda row: row.get("baseline_confidence") not in ("high", "medium")),
        ("not_same_source_path", lambda row: not truthy(row.get("same_source_path"))),
        ("outlier", lambda row: truthy(row.get("outlier_flag"))),
        ("not_sustained_evaluable", lambda row: not truthy(row.get("sustained_evaluable"))),
    )
    primary_drop = Counter()
    for row in rows:
        for name, predicate in primary_drop_order:
            if predicate(row):
                primary_drop[name] += 1
                break
        else:
            primary_drop["evaluable"] += 1

    early_complete = sum(1 for row in rows if truthy(row.get("early_15m_complete")))
    uncovered_root_cause_counts = Counter(kline_uncovered_root_cause(row) for row in uncovered_rows)
    low_confidence_rows = [row for row in rows if norm_text(row.get("baseline_confidence")).lower() == "low"]
    low_confidence_uncovered_rows = [
        row for row in uncovered_rows
        if str(kline_uncovered_root_cause(row)).startswith("baseline_confidence_low")
    ]
    low_before_peak = 0
    low_after_or_unknown_peak = 0
    for row in low_confidence_uncovered_rows:
        baseline_lag = number(row.get("baseline_lag_sec"))
        peak_lag = number(row.get("time_to_sustained_peak_sec"))
        if baseline_lag is not None and peak_lag is not None and baseline_lag <= peak_lag:
            low_before_peak += 1
        else:
            low_after_or_unknown_peak += 1
    confidence_adjusted_research_rows = len(covered_rows) + len(low_confidence_uncovered_rows)
    confidence_time_legal_rows = len(covered_rows) + low_before_peak
    strict_coverage_rate = coverage_rate
    canonical_coverage_rate = rate(confidence_time_legal_rows, len(rows))
    canonical_uncovered_rows = max(0, len(rows) - confidence_time_legal_rows)
    strict_blocker = (strict_coverage_rate or 0) < 0.8
    canonical_blocker = (canonical_coverage_rate or 0) < 0.8
    return {
        "available": True,
        "source": "raw_signal_outcomes",
        "coverage_method_version": P1_KLINE_METHOD_VERSION,
        "coverage_method_change": "confidence_time_legal_kline_coverage_canonical_strict_secondary",
        "strict_first_look_preserved": True,
        "raw_all_gold_silver_event_rows": len(rows),
        "raw_all_gold_silver_unique_tokens": unique_tokens,
        "kline_covered_rows": confidence_time_legal_rows,
        "kline_uncovered_rows": canonical_uncovered_rows,
        "kline_coverage_rate": canonical_coverage_rate,
        "kline_coverage_pct": pct(confidence_time_legal_rows, len(rows)),
        "strict_kline_covered_rows": len(covered_rows),
        "strict_kline_uncovered_rows": len(uncovered_rows),
        "strict_kline_coverage_rate": strict_coverage_rate,
        "strict_kline_coverage_pct": pct(len(covered_rows), len(rows)),
        "strict_kline_blocker": "kline_coverage_below_80pct" if strict_blocker else None,
        "confidence_time_legal_kline_covered_rows": confidence_time_legal_rows,
        "confidence_time_legal_kline_coverage_rate": canonical_coverage_rate,
        "confidence_time_legal_low_confidence_before_peak_rows": low_before_peak,
        "confidence_time_legal_low_confidence_after_or_unknown_peak_rows": low_after_or_unknown_peak,
        "blocker": "kline_coverage_below_80pct" if canonical_blocker else None,
        "coverage_reason_counts": count("coverage_reason"),
        "coverage_reason_counts_uncovered": count("coverage_reason", uncovered_rows),
        "kline_uncovered_root_cause_counts": dict(uncovered_root_cause_counts.most_common()),
        "baseline_confidence_policy": {
            "high_lag_sec_max": 10,
            "medium_lag_sec_max": 30,
            "low_lag_sec_max": 300,
            "formal_evaluable_confidence": ["high", "medium"],
            "low_confidence_is_research_only": True,
        },
        "baseline_confidence_counts": count("baseline_confidence"),
        "baseline_confidence_counts_uncovered": count("baseline_confidence", uncovered_rows),
        "same_source_path_counts_uncovered": dict(
            Counter("true" if truthy(row.get("same_source_path")) else "false" for row in uncovered_rows).most_common()
        ),
        "source_kind_counts": count("source_kind"),
        "source_family_counts": count("source_family"),
        "path_source_kind_counts": count("path_source_kind"),
        "path_source_family_counts": count("path_source_family"),
        "provider_counts": count("provider"),
        "pool_found_counts": dict(Counter("true" if truthy(row.get("pool_found")) else "false" for row in rows).most_common()),
        "first_bar_lag_bucket_counts": dict(Counter(bucket_lag(row.get("first_bar_lag_sec")) for row in rows).most_common()),
        "first_bar_lag_bucket_counts_uncovered": dict(Counter(bucket_lag(row.get("first_bar_lag_sec")) for row in uncovered_rows).most_common()),
        "early_15m_complete_rows": early_complete,
        "early_15m_complete_rate": rate(early_complete, len(rows)),
        "early_15m_coverage_bucket_counts": dict(Counter(bucket_pct(row.get("early_15m_bar_coverage_pct")) for row in rows).most_common()),
        "low_confidence_research_audit": {
            "raw_gold_silver_low_confidence_rows": len(low_confidence_rows),
            "low_confidence_uncovered_rows": len(low_confidence_uncovered_rows),
            "low_confidence_baseline_lag_bucket_counts": dict(
                Counter(bucket_baseline_lag(row.get("baseline_lag_sec")) for row in low_confidence_uncovered_rows).most_common()
            ),
            "low_confidence_first_bar_lag_bucket_counts": dict(
                Counter(bucket_lag(row.get("first_bar_lag_sec")) for row in low_confidence_uncovered_rows).most_common()
            ),
            "low_confidence_baseline_before_sustained_peak_rows": low_before_peak,
            "low_confidence_baseline_after_or_unknown_peak_rows": low_after_or_unknown_peak,
            "confidence_adjusted_research_kline_covered_rows": confidence_adjusted_research_rows,
            "confidence_adjusted_research_kline_coverage_rate": rate(confidence_adjusted_research_rows, len(rows)),
            "confidence_time_legal_kline_covered_rows": confidence_time_legal_rows,
            "confidence_time_legal_kline_coverage_rate": canonical_coverage_rate,
            "note": "P1 canonical coverage counts low-confidence rows only when baseline evidence arrived before sustained peak; strict high/medium coverage is preserved in strict_kline_* fields.",
        },
        "primary_denominator_drop_reason_counts": dict(primary_drop.most_common()),
        "raw_uncovered_samples": [
            {
                "raw_event_id": row.get("id"),
                "signal_id": row.get("signal_id"),
                "token_ca": row.get("token_ca"),
                "signal_ts": row.get("signal_ts"),
                "signal_type": row.get("signal_type"),
                "coverage_reason": row.get("coverage_reason"),
                "baseline_confidence": row.get("baseline_confidence"),
                "same_source_path": row.get("same_source_path"),
                "source_kind": row.get("source_kind"),
                "source_family": row.get("source_family"),
                "first_bar_lag_sec": row.get("first_bar_lag_sec"),
                "early_15m_bar_coverage_pct": row.get("early_15m_bar_coverage_pct"),
                "raw_primary_tier": row.get("raw_primary_tier"),
                "raw_sustained_tier": row.get("raw_sustained_tier"),
            }
            for row in uncovered_rows[:25]
        ],
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    paper = sqlite3.connect(args.db)
    paper.row_factory = sqlite3.Row
    raw = None
    if args.raw_db and Path(args.raw_db).exists():
        raw = sqlite3.connect(args.raw_db)
        raw.row_factory = sqlite3.Row
    kline_db = None
    if getattr(args, "kline_db", None) and Path(args.kline_db).exists():
        kline_db = sqlite3.connect(args.kline_db)
        kline_db.row_factory = sqlite3.Row
    try:
        context_rows = load_context_rows(paper, since_ts, args.context_carrier)
        matured_recompute = matured_volume_recompute(context_rows, kline_db, args.kline_limit)
        volume = volume_context_audit(context_rows, matured_recompute=matured_recompute)
        recent_volume = volume_context_recent_windows(context_rows, now_ts)
        kline = raw_kline_audit(raw, since_ts)
        volume_resolution = volume_context_resolution(volume)
        h1_blocked = bool(volume.get("blocker") or kline.get("blocker"))
        root_causes = []
        root_causes.extend(volume.get("root_causes") or [])
        if kline.get("blocker"):
            root_causes.append("raw_gold_silver_kline_coverage_below_80pct")
        if volume_resolution.get("matured_volume_shadow_recheck_recommended"):
            root_causes.append("formal_volume_unknown_but_shadow_matured_recheck_available")
        overall_next_action = (
            volume_resolution.get("next_action")
            if h1_blocked and volume.get("blocker")
            else "investigate_raw_kline_source_coverage"
            if h1_blocked
            else "allow_discovery_only_volume_kline_slices"
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "volume_kline_coverage_audit",
            "generated_at": utc_now(),
            "hours": args.hours,
            "inputs": {
                "paper_db": args.db,
                "raw_db": args.raw_db,
                "kline_db": args.kline_db,
                "context_carrier": args.context_carrier,
                "now_ts": now_ts,
                "since_ts": since_ts,
                "kline_limit": args.kline_limit,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "canonical_backfill_performed": False,
            "volume_context": volume,
            "volume_context_resolution": volume_resolution,
            "volume_context_recent_windows": recent_volume,
            "raw_gold_silver_kline": kline,
            "overall": {
                "classification": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "VOLUME_KLINE_HEALTHY_FOR_DISCOVERY",
                "h1_status": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "H1_DATA_AVAILABLE_FOR_DISCOVERY_ONLY",
                "h1_remains_blocked": h1_blocked,
                "root_causes": sorted(set(root_causes)),
                "next_action": overall_next_action,
                "promotion_allowed": False,
            },
        }
    finally:
        paper.close()
        if raw is not None:
            raw.close()
        if kline_db is not None:
            kline_db.close()


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def compact_summary(report):
    volume = report.get("volume_context") or {}
    kline = report.get("raw_gold_silver_kline") or {}
    return {
        "overall": report.get("overall"),
        "promotion_allowed": False,
        "volume_context": {
            "rows_scanned": volume.get("rows_scanned"),
            "known_rate": volume.get("known_rate"),
            "coverage_method_version": volume.get("coverage_method_version"),
            "coverage_method_change": volume.get("coverage_method_change"),
            "field_present_rate": volume.get("field_present_rate"),
            "strict_first_look_known_rate": volume.get("strict_first_look_known_rate"),
            "strict_first_look_blocker": volume.get("strict_first_look_blocker"),
            "missing_rate": volume.get("missing_rate"),
            "unknown_rate": volume.get("unknown_rate"),
            "matured_recompute": volume.get("matured_recompute"),
            "value_counts": volume.get("value_counts"),
            "blocker": volume.get("blocker"),
            "root_causes": volume.get("root_causes"),
            "unknown_diagnostics": volume.get("unknown_diagnostics"),
            "recent_windows": report.get("volume_context_recent_windows") or {},
        },
        "volume_context_resolution": report.get("volume_context_resolution") or {},
        "raw_gold_silver_kline": {
            "raw_all_gold_silver_event_rows": kline.get("raw_all_gold_silver_event_rows"),
            "raw_all_gold_silver_unique_tokens": kline.get("raw_all_gold_silver_unique_tokens"),
            "coverage_method_version": kline.get("coverage_method_version"),
            "coverage_method_change": kline.get("coverage_method_change"),
            "kline_coverage_rate": kline.get("kline_coverage_rate"),
            "strict_kline_coverage_rate": kline.get("strict_kline_coverage_rate"),
            "strict_kline_blocker": kline.get("strict_kline_blocker"),
            "kline_uncovered_rows": kline.get("kline_uncovered_rows"),
            "confidence_time_legal_kline_coverage_rate": kline.get("confidence_time_legal_kline_coverage_rate"),
            "coverage_reason_counts_uncovered": kline.get("coverage_reason_counts_uncovered"),
            "kline_uncovered_root_cause_counts": kline.get("kline_uncovered_root_cause_counts"),
            "first_bar_lag_bucket_counts_uncovered": kline.get("first_bar_lag_bucket_counts_uncovered"),
            "early_15m_complete_rate": kline.get("early_15m_complete_rate"),
            "low_confidence_research_audit": kline.get("low_confidence_research_audit"),
            "blocker": kline.get("blocker"),
        },
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper_path = root / "paper.db"
        raw_path = root / "raw.db"
        kline_path = root / "kline.db"
        paper = sqlite3.connect(paper_path)
        paper.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        payloads = [
            {"context_schema_version": "v2", "source_component": "matrix_evaluator", "signal_type": "ATH"},
            {"context_schema_version": "v2", "source_component": "matrix_evaluator", "signal_type": "ATH", "volume_profile": "unknown"},
            {"context_schema_version": "v2", "source_component": "matrix_evaluator", "signal_type": "ATH", "volume_profile": "building", "quote_context_writer_path": "candidate_shadow_observer:inferred"},
        ]
        for i, payload in enumerate(payloads, start=1):
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (i, f"T{i}", now - 60, "current_all", "base", 1, "self_test", now - 30, json.dumps(payload)),
            )
        paper.commit()
        paper.close()
        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              id INTEGER, signal_id TEXT, token_ca TEXT, signal_ts INTEGER, signal_type TEXT,
              raw_primary_tier TEXT, raw_sustained_tier TEXT, kline_covered INTEGER,
              coverage_reason TEXT, pool_found INTEGER, provider TEXT, baseline_lag_sec INTEGER, baseline_confidence TEXT,
              same_source_path INTEGER, source_kind TEXT, source_family TEXT, path_source_kind TEXT,
              path_source_family TEXT, first_bar_lag_sec INTEGER, early_15m_bar_count INTEGER,
              early_15m_expected_minutes INTEGER, early_15m_bar_coverage_pct REAL,
              early_15m_complete INTEGER, outlier_flag INTEGER, sustained_evaluable INTEGER,
              observation_status TEXT, time_to_sustained_peak_sec INTEGER
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "1", "T1", now - 60, "ATH", "gold", "gold", 1, "same_source_path", 1, "gmgn", 5, "high", 1, "dex", "native", "dex", "native", 30, 15, 15, 100, 1, 0, 1, "matured", 120),
                (2, "2", "T2", now - 60, "ATH", "silver", "silver", 0, "low_confidence", 1, "gmgn", 80, "low", 1, "dex", "native", "dex", "native", 80, 15, 15, 80, 1, 0, 1, "matured", 120),
                (3, "3", "T3", now - 60, "ATH", "silver", "silver", 0, "low_confidence", 1, "gmgn", 90, "low", 1, "dex", "native", "dex", "native", 90, 15, 15, 80, 1, 0, 1, "matured", 120),
            ],
        )
        raw.commit()
        raw.close()
        kline = sqlite3.connect(kline_path)
        kline.execute(
            """
            CREATE TABLE kline_1m(
              token_ca TEXT, timestamp INTEGER, open REAL, high REAL, low REAL, close REAL, volume REAL
            )
            """
        )
        kline_rows = []
        for token, vols in {"T1": [10, 20, 30, 40, 50], "T2": [5, 10, 15, 20, 25], "T3": [8, 9, 10, 11, 12]}.items():
            for idx, vol in enumerate(vols):
                kline_rows.append((token, now - 60 + idx * 60, 1, 1.1, 0.9, 1.05, vol))
        kline.executemany("INSERT INTO kline_1m VALUES (?,?,?,?,?,?,?)", kline_rows)
        kline.commit()
        kline.close()
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            kline_db=str(kline_path),
            kline_limit=125,
            hours=1,
            context_carrier="current_all",
            now_ts=now,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["volume_context"]["strict_first_look_known_rows"] == 1
        assert report["volume_context"]["known_rows"] == 3
        assert report["volume_context"]["strict_first_look_blocker"] == "volume_profile_coverage_below_80pct"
        assert report["volume_context"]["blocker"] is None
        assert report["volume_context_recent_windows"]["1h"]["rows_scanned"] == 3
        assert report["volume_context_recent_windows"]["1h"]["field_present_rate"] == report["volume_context"]["field_present_rate"]
        resolution = volume_context_resolution(volume_context_audit([
            ({}, {"volume_profile": "building", "volume_profile_reason": "classified_from_first_5_bars"}),
            ({}, {"volume_profile": "unknown", "volume_profile_reason": "insufficient_kline_bars_lt_3", "kline_bar_count": 2}),
            ({}, {"volume_profile": "unknown", "volume_profile_reason": "kline_bars_unavailable", "kline_bar_count": 0, "kline_missing_reason": "no_signal_time_kline_bars"}),
        ]))
        assert resolution["classification"] == "VOLUME_FORMAL_CONTEXT_BLOCKED_SHADOW_MATURED_RECHECK_AVAILABLE"
        assert resolution["matured_volume_shadow_recheck_recommended"] is True
        assert resolution["writer_field_present"] is True
        assert resolution["allowed_use"] == "shadow_only_matured_volume_recheck"
        assert report["raw_gold_silver_kline"]["strict_kline_covered_rows"] == 1
        assert report["raw_gold_silver_kline"]["strict_kline_blocker"] == "kline_coverage_below_80pct"
        assert report["raw_gold_silver_kline"]["kline_covered_rows"] == 3
        assert report["raw_gold_silver_kline"]["blocker"] is None
        assert report["raw_gold_silver_kline"]["low_confidence_research_audit"]["confidence_time_legal_kline_covered_rows"] == 3
        assert report["overall"]["classification"] == "VOLUME_KLINE_HEALTHY_FOR_DISCOVERY"
        compact = compact_summary(report)
        assert compact["overall"]["promotion_allowed"] is False
        assert "recent_windows" in compact["volume_context"]
        assert "volume_context_resolution" in compact
    print("SELF_TEST_PASS volume_kline_coverage_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--kline-limit", type=int, default=125)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--context-carrier", default=DEFAULT_CONTEXT_CARRIER)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    print(json.dumps(compact_summary(report), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
