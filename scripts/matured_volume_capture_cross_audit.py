#!/usr/bin/env python3
"""Read-only capture cross audit using matured volume profiles.

This report does not change candidate observations, denominators, strategy,
gates, A_CLASS mode, final_entry_contract, paper execution, or risk. It asks a
research-only question: if the early `volume_profile=unknown` context is
recomputed from matured cached kline bars, do volume-sensitive candidate slices
show gold/silver capture lift?
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


SCHEMA_VERSION = "matured_volume_capture_cross_audit.v1"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_CONTEXT_CARRIER = "current_all"
H1_CANDIDATES = {
    "kline:active_mom20_first3",
    "kline:lowvol_active20_support",
}
DEFAULT_MAX_SCAN_ROWS = 300_000


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    if parsed is None:
        return default
    return int(parsed)


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def numeric_signal_id(value):
    key = signal_id_key(value)
    if key is None:
        return None
    try:
        parsed = float(key)
        if math.isfinite(parsed) and parsed.is_integer():
            return int(parsed)
    except Exception:
        return None
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


def signal_id_reconciliation(raw_rows, scanned_signal_ids):
    scanned_signal_ids = {signal_id_key(value) for value in scanned_signal_ids if signal_id_key(value)}
    scanned_numeric = [numeric_signal_id(value) for value in scanned_signal_ids]
    scanned_numeric = [value for value in scanned_numeric if value is not None]
    min_scanned = min(scanned_numeric) if scanned_numeric else None
    max_scanned = max(scanned_numeric) if scanned_numeric else None
    event_rows = len(raw_rows)
    raw_signal_ids = [signal_id_key(row.get("signal_id")) for row in raw_rows]
    present_signal_ids = [sid for sid in raw_signal_ids if sid]
    unique_signal_ids = set(present_signal_ids)
    joined_event_rows = sum(1 for sid in raw_signal_ids if sid and sid in scanned_signal_ids)
    joined_unique_signal_ids = len(unique_signal_ids & scanned_signal_ids)
    reason_counts = Counter()
    samples = []
    seen = Counter(present_signal_ids)
    duplicate_event_rows = sum(max(0, count - 1) for count in seen.values())
    for row, sid in zip(raw_rows, raw_signal_ids):
        if not sid:
            reason = "missing_signal_id"
        elif sid in scanned_signal_ids:
            continue
        else:
            numeric = numeric_signal_id(sid)
            if numeric is not None and min_scanned is not None and numeric < min_scanned:
                reason = "outside_candidate_observer_window_before"
            elif numeric is not None and max_scanned is not None and numeric > max_scanned:
                reason = "outside_candidate_observer_window_after"
            elif numeric is None:
                reason = "non_numeric_signal_id_unjoined"
            else:
                reason = "missing_context_carrier_observation"
        reason_counts[reason] += 1
        if len(samples) < 25:
            samples.append(
                {
                    "raw_event_id": row.get("id"),
                    "signal_id": sid,
                    "token_ca": row.get("token_ca"),
                    "signal_ts": row.get("signal_ts"),
                    "reason": reason,
                }
            )
    return {
        "event_rows": event_rows,
        "unique_signal_ids": len(unique_signal_ids),
        "duplicate_event_rows": duplicate_event_rows,
        "scanned_context_signal_ids": len(scanned_signal_ids),
        "scanned_context_signal_id_min": min_scanned,
        "scanned_context_signal_id_max": max_scanned,
        "joined_event_rows": joined_event_rows,
        "joined_event_rate": rate(joined_event_rows, event_rows),
        "joined_unique_signal_ids": joined_unique_signal_ids,
        "joined_unique_signal_id_rate": rate(joined_unique_signal_ids, len(unique_signal_ids)),
        "unjoined_event_rows": event_rows - joined_event_rows,
        "unjoined_reason_counts": dict(reason_counts.most_common()),
        "unjoined_samples": samples,
        "promotion_allowed": False,
    }


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def cols(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


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


def volume_profile(bars):
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


def profile_reason(bars):
    if not bars:
        return "kline_bars_unavailable"
    if len(bars[:5]) < 3:
        return "insufficient_kline_bars_lt_3"
    return "classified_from_first_5_bars"


def dog_tier(row):
    fallback = None
    for key in ("raw_primary_tier", "raw_sustained_tier", "tier", "dog_tier"):
        value = row.get(key)
        if value not in (None, ""):
            tier = str(value).lower()
            if tier in {"gold", "silver"}:
                return tier
            if fallback is None:
                fallback = tier
    return fallback


def is_gold_silver_row(row):
    return dog_tier(row) in {"gold", "silver"}


def row_fails_eligibility(row):
    if row.get("_has_observation_status") and row.get("observation_status") != "matured":
        return True
    if row.get("_has_kline_covered") and not safe_bool(row.get("kline_covered")):
        return True
    if row.get("_has_baseline_confidence") and row.get("baseline_confidence") not in ("high", "medium"):
        return True
    if row.get("_has_same_source_path") and not safe_bool(row.get("same_source_path")):
        return True
    if row.get("_has_outlier_flag") and safe_bool(row.get("outlier_flag")):
        return True
    if row.get("_has_sustained_evaluable") and not safe_bool(row.get("sustained_evaluable")):
        return True
    return False


def normalize_raw_row(row, columns):
    out = dict(row)
    for name in (
        "observation_status",
        "kline_covered",
        "baseline_confidence",
        "same_source_path",
        "outlier_flag",
        "sustained_evaluable",
    ):
        out[f"_has_{name}"] = name in columns
    out["signal_id"] = signal_id_key(out.get("signal_id"))
    out["signal_ts"] = safe_int(out.get("signal_ts"))
    out["token_ca"] = out.get("token_ca") or out.get("token") or out.get("ca")
    out["tier"] = dog_tier(out)
    return out


def load_raw_gold_silver(path, since_ts):
    if not path or not Path(path).exists():
        return [], [], {"source": "raw_signal_outcomes_db", "available": False}
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    try:
        if not table_exists(db, "raw_signal_outcomes"):
            return [], [], {"source": "raw_signal_outcomes_db", "available": False, "reason": "missing_table"}
        columns = cols(db, "raw_signal_outcomes")
        if "raw_primary_tier" not in columns and "raw_sustained_tier" not in columns:
            return [], [], {"source": "raw_signal_outcomes_db", "available": False, "reason": "missing_tier_columns"}
        select = []
        for name in (
            "id",
            "signal_id",
            "token_ca",
            "token",
            "ca",
            "signal_ts",
            "observation_status",
            "kline_covered",
            "baseline_confidence",
            "same_source_path",
            "outlier_flag",
            "sustained_evaluable",
            "raw_primary_tier",
            "raw_sustained_tier",
        ):
            select.append(name if name in columns else f"NULL AS {name}")
        tier_terms = []
        if "raw_primary_tier" in columns:
            tier_terms.append("raw_primary_tier IN ('gold', 'silver')")
        if "raw_sustained_tier" in columns:
            tier_terms.append("raw_sustained_tier IN ('gold', 'silver')")
        rows = db.execute(
            f"""
            SELECT {", ".join(select)}
            FROM raw_signal_outcomes
            WHERE COALESCE(signal_ts, 0) >= ?
              AND ({' OR '.join(tier_terms)})
            """,
            (int(since_ts),),
        ).fetchall()
    finally:
        db.close()
    raw_all = [normalize_raw_row(row, columns) for row in rows]
    raw_all = [row for row in raw_all if is_gold_silver_row(row)]
    evaluable = [row for row in raw_all if not row_fails_eligibility(row)]
    meta = {
        "source": "raw_signal_outcomes_db",
        "available": True,
        "path": path,
        "raw_all_event_rows": len(raw_all),
        "raw_all_unique_tokens": len({row.get("token_ca") for row in raw_all if row.get("token_ca")}),
        "evaluable_event_rows": len(evaluable),
        "evaluable_unique_tokens": len({row.get("token_ca") for row in evaluable if row.get("token_ca")}),
        "primary_report_denominator": "evaluable_gold_silver",
        "business_denominator": "raw_all_gold_silver",
    }
    return raw_all, evaluable, meta


def normalize_observation_row(row):
    payload = jloads(row["payload_json"])
    return {
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


def load_context_rows(path, since_ts, context_carrier, max_scan_rows):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    try:
        if not table_exists(db, "candidate_shadow_observations"):
            return []
        filters = ["COALESCE(observed_at, 0) >= ?", "candidate_id = ?"]
        params = [int(since_ts), context_carrier]
        rows = db.execute(
            f"""
            SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched,
                   reason, observed_at, payload_json
            FROM candidate_shadow_observations
            WHERE {' AND '.join(filters)}
            """,
            tuple(params),
        ).fetchall()
        if not rows:
            filters = ["COALESCE(observed_at, 0) >= ?"]
            params = [int(since_ts)]
            rows = db.execute(
                f"""
                SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched,
                       reason, observed_at, '{{}}' AS payload_json
                FROM candidate_shadow_observations
                WHERE {' AND '.join(filters)}
                """,
                tuple(params),
            ).fetchall()
    finally:
        db.close()
    return [normalize_observation_row(row) for row in rows]


def load_candidate_rows(path, since_ts, signal_ids, max_scan_rows, chunk_size=500):
    if not signal_ids:
        return []
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    out = []
    try:
        if not table_exists(db, "candidate_shadow_observations"):
            return []
        signal_ids = sorted(signal_ids)
        for index in range(0, len(signal_ids), int(chunk_size)):
            chunk = signal_ids[index:index + int(chunk_size)]
            placeholders = ",".join("?" for _ in chunk)
            filters = [f"signal_id IN ({placeholders})", "COALESCE(observed_at, 0) >= ?"]
            params = list(chunk) + [int(since_ts)]
            rows = db.execute(
                f"""
                SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched,
                       reason, observed_at, payload_json
                FROM candidate_shadow_observations
                WHERE {' AND '.join(filters)}
                """,
                tuple(params),
            ).fetchall()
            out.extend(normalize_observation_row(row) for row in rows)
    finally:
        db.close()
    return out


def pick_signal_contexts(observations, context_carrier):
    contexts = {}
    for row in observations:
        sid = row.get("signal_id")
        if not sid:
            continue
        current = contexts.get(sid)
        row_is_carrier = row.get("candidate_id") == context_carrier
        current_is_carrier = current and current.get("candidate_id") == context_carrier
        newer = (row.get("observed_at") or 0) >= ((current or {}).get("observed_at") or 0)
        if current is None or (row_is_carrier and not current_is_carrier) or (row_is_carrier == current_is_carrier and newer):
            contexts[sid] = row
    return contexts


def load_kline_bars(db, token_ca, signal_ts, limit):
    if not token_ca or signal_ts is None or not table_exists(db, "kline_1m"):
        return []
    floor_ts = int(float(signal_ts) // 60 * 60)
    try:
        rows = db.execute(
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


def matured_volume_contexts(kline_path, signal_contexts, kline_limit):
    if not kline_path or not Path(kline_path).exists():
        return {}, False
    db = sqlite3.connect(kline_path)
    db.row_factory = sqlite3.Row
    try:
        available = table_exists(db, "kline_1m")
        contexts = {}
        if not available:
            return {}, False
        for sid, row in signal_contexts.items():
            bars = load_kline_bars(db, row.get("token_ca"), row.get("signal_ts"), kline_limit)
            first = bars[:5]
            contexts[sid] = {
                "matured_volume_profile": volume_profile(first),
                "matured_volume_profile_reason": profile_reason(first),
                "matured_kline_bar_count": len(bars),
                "original_volume_profile": row.get("payload", {}).get("volume_profile"),
                "original_volume_profile_reason": row.get("payload", {}).get("volume_profile_reason"),
                "context_carrier_candidate_id": row.get("candidate_id"),
            }
    finally:
        db.close()
    return contexts, True


def aggregate_slice(row, raw_signal_ids, slice_stats, baseline_stats, slice_key):
    candidate_id = row.get("candidate_id")
    if not candidate_id:
        return
    matched = bool(row.get("matched"))
    is_gs = row.get("signal_id") in raw_signal_ids
    base = baseline_stats[candidate_id]
    base["signal_count"] += 1
    if is_gs:
        base["raw_gs_count"] += 1
    if matched:
        base["candidate_match_count"] += 1
        if is_gs:
            base["matched_gs_count"] += 1
    stat = slice_stats[(candidate_id, slice_key)]
    stat["candidate_id"] = candidate_id
    stat["family"] = row.get("family")
    stat["dimension"] = "matured_volume_profile"
    stat["slice_value"] = slice_key
    stat["slice_signal_count"] += 1
    if is_gs:
        stat["slice_raw_gs_count"] += 1
    if matched:
        stat["candidate_match_count"] += 1
        if is_gs:
            stat["matched_gs_count"] += 1


def finalize_stats(slice_stats, baseline_stats):
    rows = []
    for (_candidate_id, _slice), stat in slice_stats.items():
        base = baseline_stats[stat["candidate_id"]]
        recall = rate(stat["matched_gs_count"], stat["slice_raw_gs_count"])
        precision = rate(stat["matched_gs_count"], stat["candidate_match_count"])
        base_recall = rate(base["matched_gs_count"], base["raw_gs_count"])
        base_precision = rate(base["matched_gs_count"], base["candidate_match_count"])
        recall_lift = None if recall is None or base_recall is None else round(recall - base_recall, 6)
        precision_lift = None if precision is None or base_precision is None else round(precision - base_precision, 6)
        if stat["slice_signal_count"] < 20 or stat["slice_raw_gs_count"] < 3 or stat["candidate_match_count"] < 3:
            verdict = "TOO_SMALL"
        elif (recall_lift or 0) > 0 and (precision_lift or 0) >= 0 and precision is not None and precision > 0:
            verdict = "MATURED_VOLUME_DISCOVERY_WATCH"
        else:
            verdict = "NO_SIGNAL"
        row = {
            **stat,
            "match_recall_event": recall,
            "match_precision_event": precision,
            "candidate_baseline_recall_event": base_recall,
            "candidate_baseline_precision_event": base_precision,
            "recall_lift_vs_candidate_baseline": recall_lift,
            "precision_lift_vs_candidate_baseline": precision_lift,
            "verdict": verdict,
            "promotion_allowed": False,
        }
        rows.append(row)
    rows.sort(
        key=lambda row: (
            row.get("verdict") != "MATURED_VOLUME_DISCOVERY_WATCH",
            -(row.get("matched_gs_count") or 0),
            -(row.get("candidate_match_count") or 0),
            row.get("candidate_id") or "",
        )
    )
    return rows


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_all, evaluable, raw_meta = load_raw_gold_silver(args.raw_db, since_ts)
    context_rows = load_context_rows(args.db, since_ts, args.context_carrier, args.max_scan_rows) if Path(args.db).exists() else []
    signal_contexts = pick_signal_contexts(context_rows, args.context_carrier)
    observations = load_candidate_rows(args.db, since_ts, set(signal_contexts), args.max_scan_rows) if Path(args.db).exists() else []
    matured_contexts, kline_available = matured_volume_contexts(args.kline_db, signal_contexts, args.kline_limit)
    raw_signal_ids = {row["signal_id"] for row in evaluable if row.get("signal_id")}
    raw_all_signal_ids = {row["signal_id"] for row in raw_all if row.get("signal_id")}
    candidate_ids = {row.get("candidate_id") for row in observations if row.get("candidate_id")}
    rows_by_key = {}
    for row in observations:
        sid = row.get("signal_id")
        cid = row.get("candidate_id")
        if not sid or not cid:
            continue
        key = (sid, cid)
        current = rows_by_key.get(key)
        if current is None or (row.get("observed_at") or 0) >= (current.get("observed_at") or 0):
            rows_by_key[key] = row
    slice_stats = defaultdict(lambda: defaultdict(int))
    baseline_stats = defaultdict(lambda: defaultdict(int))
    maturity_counts = Counter()
    profile_counts = Counter()
    reason_counts = Counter()
    scanned_signal_ids = set(signal_contexts)
    for sid, context in matured_contexts.items():
        profile = context.get("matured_volume_profile") or "unknown"
        profile_counts[profile] += 1
        reason_counts[context.get("matured_volume_profile_reason") or "UNKNOWN"] += 1
        if profile == "unknown":
            maturity_counts["unknown"] += 1
        else:
            maturity_counts["known"] += 1
    for (sid, _cid), row in rows_by_key.items():
        if sid not in scanned_signal_ids:
            continue
        context = matured_contexts.get(sid) or {}
        profile = context.get("matured_volume_profile") or "unknown"
        aggregate_slice(row, raw_signal_ids, slice_stats, baseline_stats, profile)
    slices = finalize_stats(slice_stats, baseline_stats)
    h1_slices = [
        row for row in slices
        if row.get("candidate_id") in H1_CANDIDATES and row.get("slice_value") == "building"
    ]
    verdict_counts = Counter(row.get("verdict") for row in slices)
    top_slices = slices[: int(args.limit)]
    raw_all_reconciliation = signal_id_reconciliation(raw_all, scanned_signal_ids)
    evaluable_reconciliation = signal_id_reconciliation(evaluable, scanned_signal_ids)
    denominator = {
        "raw_all_gold_silver": {
            "event_rows": len(raw_all),
            "unique_tokens": len({row.get("token_ca") for row in raw_all if row.get("token_ca")}),
            "joined_signal_id_rows": len(raw_all_signal_ids & scanned_signal_ids),
            "signal_id_join_rate": rate(len(raw_all_signal_ids & scanned_signal_ids), len(raw_all_signal_ids)),
            "joined_event_rows": raw_all_reconciliation.get("joined_event_rows"),
            "joined_event_rate": raw_all_reconciliation.get("joined_event_rate"),
            "duplicate_event_rows": raw_all_reconciliation.get("duplicate_event_rows"),
        },
        "evaluable_gold_silver": {
            "event_rows": len(evaluable),
            "unique_tokens": len({row.get("token_ca") for row in evaluable if row.get("token_ca")}),
            "joined_signal_id_rows": len(raw_signal_ids & scanned_signal_ids),
            "signal_id_join_rate": rate(len(raw_signal_ids & scanned_signal_ids), len(raw_signal_ids)),
            "joined_event_rows": evaluable_reconciliation.get("joined_event_rows"),
            "joined_event_rate": evaluable_reconciliation.get("joined_event_rate"),
            "duplicate_event_rows": evaluable_reconciliation.get("duplicate_event_rows"),
        },
    }
    known_rate = rate(maturity_counts["known"], len(matured_contexts))
    if not kline_available:
        classification = "BLOCKED_KLINE_CACHE_UNAVAILABLE"
        next_action = "restore_or_mount_kline_cache_for_matured_volume_cross"
    elif not observations:
        classification = "BLOCKED_DATA"
        next_action = "candidate_shadow_observations_unavailable"
    elif (known_rate or 0) < 0.8:
        classification = "BLOCKED_MATURED_VOLUME_COVERAGE"
        next_action = "continue_matured_volume_recheck_before_evaluating_volume_slices"
    elif h1_slices and any(row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH" for row in h1_slices):
        classification = "MATURED_VOLUME_DISCOVERY_WATCH"
        next_action = "track_h1_matured_building_volume_in_next_clean_window"
    else:
        classification = "MATURED_VOLUME_DISCOVERY_NO_SIGNAL"
        next_action = "keep_volume_sensitive_slices_shadow_only"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "matured_volume_capture_cross_audit",
        "generated_at": utc_now(),
        "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
        "evidence_level": EVIDENCE_LEVEL,
        "usage": "shadow_only_matured_volume_context",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "formal_denominator_changed": False,
        "original_observations_unchanged": True,
        "inputs": {
            "paper_db": args.db,
            "raw_db": args.raw_db,
            "kline_db": args.kline_db,
            "context_carrier": args.context_carrier,
        },
        "raw_dog_source": raw_meta,
        "candidate_count_expected": args.expected_candidates,
        "candidate_count_observed": len(candidate_ids),
        "candidate_count_ok": len(candidate_ids) == int(args.expected_candidates),
        "scan_strategy": {
            "context_rows": "context_carrier_observed_at_window_no_rowid_floor",
            "candidate_rows": "signal_id_scoped_index_join_no_rowid_floor",
            "max_scan_rows": args.max_scan_rows,
            "note": "Signal-scoped joins avoid truncating the 24h raw dog denominator while avoiding broad rowid scans.",
        },
        "signals_scanned": len(scanned_signal_ids),
        "context_rows_scanned": len(context_rows),
        "candidate_observation_rows_scanned": len(observations),
        "deduped_candidate_observation_rows": len(rows_by_key),
        "denominator": denominator,
        "signal_id_reconciliation": {
            "raw_all_gold_silver": raw_all_reconciliation,
            "evaluable_gold_silver": evaluable_reconciliation,
            "primary_join_metric": "joined_event_rate",
            "note": "Read-only attribution for matured volume cross scope. It does not change the formal denominator.",
        },
        "matured_volume_context": {
            "kline_cache_available": kline_available,
            "signals_with_matured_context": len(matured_contexts),
            "known_rows": maturity_counts["known"],
            "unknown_rows": maturity_counts["unknown"],
            "known_rate": known_rate,
            "profile_counts": dict(profile_counts.most_common()),
            "reason_counts": dict(reason_counts.most_common()),
        },
        "slice_dimension": "matured_volume_profile",
        "judgment_counts": dict(verdict_counts.most_common()),
        "top_slices": top_slices,
        "h1_matured_building_volume": {
            "definition": {
                "candidate_ids": sorted(H1_CANDIDATES),
                "matured_volume_profile": "building",
            },
            "rows": h1_slices,
            "status": (
                "MATURED_VOLUME_DISCOVERY_WATCH"
                if any(row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH" for row in h1_slices)
                else "NO_H1_MATURED_VOLUME_HIT"
            ),
        },
        "overall": {
            "classification": classification,
            "next_action": next_action,
            "promotion_allowed": False,
        },
    }


def compact_summary(report):
    return {
        "overall": report.get("overall"),
        "promotion_allowed": False,
        "candidate_count_observed": report.get("candidate_count_observed"),
        "signals_scanned": report.get("signals_scanned"),
        "denominator": report.get("denominator"),
        "signal_id_reconciliation": report.get("signal_id_reconciliation"),
        "matured_volume_context": report.get("matured_volume_context"),
        "h1_matured_building_volume": report.get("h1_matured_building_volume"),
        "judgment_counts": report.get("judgment_counts"),
    }


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


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
              signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        obs_rows = [
            ("1", "DOG1", now - 600, "current_all", "baseline", 1, "all", now - 500, {"volume_profile": "unknown"}),
            ("1", "DOG1", now - 600, "kline:active_mom20_first3", "kline", 1, "match", now - 500, {}),
            ("1", "DOG1", now - 600, "kline:lowvol_active20_support", "kline", 0, "no", now - 500, {}),
            ("2", "NORM", now - 600, "current_all", "baseline", 1, "all", now - 500, {"volume_profile": "unknown"}),
            ("2", "NORM", now - 600, "kline:active_mom20_first3", "kline", 1, "match", now - 500, {}),
            ("2", "NORM", now - 600, "kline:lowvol_active20_support", "kline", 1, "match", now - 500, {}),
        ]
        for row in obs_rows:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (*row[:8], json.dumps(row[8])),
            )
        paper.commit()
        paper.close()
        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, signal_ts INTEGER, observation_status TEXT,
              kline_covered INTEGER, baseline_confidence TEXT, same_source_path INTEGER,
              outlier_flag INTEGER, sustained_evaluable INTEGER,
              raw_primary_tier TEXT, raw_sustained_tier TEXT
            )
            """
        )
        raw.execute(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("1", "DOG1", now - 600, "matured", 1, "high", 1, 0, 1, "silver", "silver"),
        )
        raw.commit()
        raw.close()
        kline = sqlite3.connect(kline_path)
        kline.execute(
            """
            CREATE TABLE kline_1m(
              token_ca TEXT, pool_address TEXT DEFAULT '', timestamp INTEGER,
              open REAL, high REAL, low REAL, close REAL, volume REAL,
              PRIMARY KEY(token_ca, timestamp)
            )
            """
        )
        kline.executemany(
            "INSERT INTO kline_1m(token_ca,timestamp,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)",
            [
                ("DOG1", now - 600, 1, 1.1, 0.9, 1.0, 10),
                ("DOG1", now - 540, 1, 1.2, 0.9, 1.1, 20),
                ("DOG1", now - 480, 1, 1.4, 1.0, 1.3, 30),
                ("NORM", now - 600, 1, 1.1, 0.9, 1.0, 10),
                ("NORM", now - 540, 1, 1.2, 0.9, 1.1, 20),
                ("NORM", now - 480, 1, 1.4, 1.0, 1.3, 30),
            ],
        )
        kline.commit()
        kline.close()
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            kline_db=str(kline_path),
            hours=1,
            expected_candidates=3,
            context_carrier="current_all",
            max_scan_rows=300_000,
            kline_limit=125,
            limit=10,
            now_ts=now,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["formal_denominator_changed"] is False
        assert report["matured_volume_context"]["known_rate"] == 1.0
        assert report["denominator"]["evaluable_gold_silver"]["event_rows"] == 1
        assert report["signal_id_reconciliation"]["evaluable_gold_silver"]["joined_event_rate"] == 1.0
        assert report["signal_id_reconciliation"]["raw_all_gold_silver"]["joined_event_rows"] == 1
        assert report["h1_matured_building_volume"]["rows"]
        assert report["candidate_count_observed"] == 3
        compact = compact_summary(report)
        assert compact["signal_id_reconciliation"]["primary_join_metric"] == "joined_event_rate"
        assert compact["matured_volume_context"]["profile_counts"]["building"] == 2
    print("SELF_TEST_PASS matured_volume_capture_cross_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--context-carrier", default=DEFAULT_CONTEXT_CARRIER)
    parser.add_argument("--max-scan-rows", type=int, default=DEFAULT_MAX_SCAN_ROWS)
    parser.add_argument("--kline-limit", type=int, default=125)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    print(json.dumps(compact_summary(report), sort_keys=True))


if __name__ == "__main__":
    main()
