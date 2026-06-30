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


SCHEMA_VERSION = "volume_kline_coverage_audit.v1"
DEFAULT_CONTEXT_CARRIER = "current_all"
UNKNOWN_VALUES = {"", "unknown", "unk", "null", "none"}
NOT_APPLICABLE_VALUES = {"not_applicable", "not-applicable", "n/a", "na", "not applicable"}


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
        "by_lifecycle_profile": "lifecycle_profile",
        "by_signal_type": "signal_type",
        "by_candidate_family": "candidate_family",
    }
    out = {}
    selected = [(row, payload) for row, payload in rows if predicate(row, payload)]
    for label, key in dims.items():
        out[label] = dict(Counter(payload_dim(payload, key) for _row, payload in selected).most_common())
    return out


def volume_context_audit(rows):
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
    blocker = known < den * 0.8
    missing_or_unknown = lambda _row, payload: value_status(payload, "volume_profile") in {"missing", "unknown"}
    root_causes = []
    missing_writer = breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "missing")["by_writer_path"]
    if missing_writer.get("MISSING", 0) or missing_writer.get("candidate_shadow_observer:inferred", 0):
        root_causes.append("volume_profile_missing_in_context_carrier_payload")
    if unknown:
        root_causes.append("volume_profile_unknown_from_insufficient_or_unclassified_kline")
    if not root_causes and blocker:
        root_causes.append("volume_profile_coverage_below_threshold")
    return {
        "coverage_denominator_type": "signal_context_carrier_rows",
        "context_carrier_candidate_id": DEFAULT_CONTEXT_CARRIER,
        "rows_scanned": den,
        "field_present_rows": field_present,
        "field_present_rate": rate(field_present, den),
        "known_rows": known,
        "known_rate": rate(known, den),
        "missing_rows": missing,
        "missing_rate": rate(missing, den),
        "unknown_rows": unknown,
        "unknown_rate": rate(unknown, den),
        "not_applicable_rows": not_applicable,
        "not_applicable_rate": rate(not_applicable, den),
        "value_counts": dict(value_counts.most_common()),
        "blocker": "volume_profile_coverage_below_80pct" if blocker else None,
        "root_causes": root_causes,
        "missing_or_unknown_breakdown": breakdown(rows, missing_or_unknown),
        "missing_breakdown": breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "missing"),
        "unknown_breakdown": breakdown(rows, lambda _row, payload: value_status(payload, "volume_profile") == "unknown"),
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
    return {
        "available": True,
        "source": "raw_signal_outcomes",
        "raw_all_gold_silver_event_rows": len(rows),
        "raw_all_gold_silver_unique_tokens": unique_tokens,
        "kline_covered_rows": len(covered_rows),
        "kline_uncovered_rows": len(uncovered_rows),
        "kline_coverage_rate": coverage_rate,
        "kline_coverage_pct": pct(len(covered_rows), len(rows)),
        "blocker": "kline_coverage_below_80pct" if (coverage_rate or 0) < 0.8 else None,
        "coverage_reason_counts": count("coverage_reason"),
        "coverage_reason_counts_uncovered": count("coverage_reason", uncovered_rows),
        "kline_uncovered_root_cause_counts": dict(uncovered_root_cause_counts.most_common()),
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
    try:
        context_rows = load_context_rows(paper, since_ts, args.context_carrier)
        volume = volume_context_audit(context_rows)
        kline = raw_kline_audit(raw, since_ts)
        h1_blocked = bool(volume.get("blocker") or kline.get("blocker"))
        root_causes = []
        root_causes.extend(volume.get("root_causes") or [])
        if kline.get("blocker"):
            root_causes.append("raw_gold_silver_kline_coverage_below_80pct")
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "volume_kline_coverage_audit",
            "generated_at": utc_now(),
            "hours": args.hours,
            "inputs": {
                "paper_db": args.db,
                "raw_db": args.raw_db,
                "context_carrier": args.context_carrier,
                "now_ts": now_ts,
                "since_ts": since_ts,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "canonical_backfill_performed": False,
            "volume_context": volume,
            "raw_gold_silver_kline": kline,
            "overall": {
                "classification": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "VOLUME_KLINE_HEALTHY_FOR_DISCOVERY",
                "h1_status": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "H1_DATA_AVAILABLE_FOR_DISCOVERY_ONLY",
                "h1_remains_blocked": h1_blocked,
                "root_causes": sorted(set(root_causes)),
                "next_action": "investigate_volume_writer_or_kline_source_coverage" if h1_blocked else "allow_discovery_only_volume_kline_slices",
                "promotion_allowed": False,
            },
        }
    finally:
        paper.close()
        if raw is not None:
            raw.close()


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
            "field_present_rate": volume.get("field_present_rate"),
            "missing_rate": volume.get("missing_rate"),
            "unknown_rate": volume.get("unknown_rate"),
            "value_counts": volume.get("value_counts"),
            "blocker": volume.get("blocker"),
            "root_causes": volume.get("root_causes"),
        },
        "raw_gold_silver_kline": {
            "raw_all_gold_silver_event_rows": kline.get("raw_all_gold_silver_event_rows"),
            "raw_all_gold_silver_unique_tokens": kline.get("raw_all_gold_silver_unique_tokens"),
            "kline_coverage_rate": kline.get("kline_coverage_rate"),
            "kline_uncovered_rows": kline.get("kline_uncovered_rows"),
            "coverage_reason_counts_uncovered": kline.get("coverage_reason_counts_uncovered"),
            "kline_uncovered_root_cause_counts": kline.get("kline_uncovered_root_cause_counts"),
            "first_bar_lag_bucket_counts_uncovered": kline.get("first_bar_lag_bucket_counts_uncovered"),
            "early_15m_complete_rate": kline.get("early_15m_complete_rate"),
            "blocker": kline.get("blocker"),
        },
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper_path = root / "paper.db"
        raw_path = root / "raw.db"
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
              coverage_reason TEXT, pool_found INTEGER, provider TEXT, baseline_confidence TEXT,
              same_source_path INTEGER, source_kind TEXT, source_family TEXT, path_source_kind TEXT,
              path_source_family TEXT, first_bar_lag_sec INTEGER, early_15m_bar_count INTEGER,
              early_15m_expected_minutes INTEGER, early_15m_bar_coverage_pct REAL,
              early_15m_complete INTEGER, outlier_flag INTEGER, sustained_evaluable INTEGER,
              observation_status TEXT
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "1", "T1", now - 60, "ATH", "gold", "gold", 1, "same_source_path", 1, "gmgn", "high", 1, "dex", "native", "dex", "native", 30, 15, 15, 100, 1, 0, 1, "matured"),
                (2, "2", "T2", now - 60, "ATH", "silver", "silver", 0, "no_kline_for_token", 0, None, "not_evaluable", 0, None, None, None, None, None, 0, 15, None, 0, 0, 1, "matured"),
            ],
        )
        raw.commit()
        raw.close()
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            hours=1,
            context_carrier="current_all",
            now_ts=now,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["volume_context"]["known_rows"] == 1
        assert report["volume_context"]["missing_rows"] == 1
        assert report["volume_context"]["unknown_rows"] == 1
        assert report["raw_gold_silver_kline"]["kline_covered_rows"] == 1
        assert report["raw_gold_silver_kline"]["kline_uncovered_rows"] == 1
        assert report["raw_gold_silver_kline"]["kline_uncovered_root_cause_counts"]["not_same_source_path"] == 1
        assert report["overall"]["classification"] == "DATA_BLOCKED_VOLUME_KLINE"
        compact = compact_summary(report)
        assert compact["overall"]["promotion_allowed"] is False
    print("SELF_TEST_PASS volume_kline_coverage_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
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
