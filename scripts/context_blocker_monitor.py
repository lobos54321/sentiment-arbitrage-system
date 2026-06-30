#!/usr/bin/env python3
"""Read-only context blocker monitor for capture readiness.

This script audits the current data/context blockers without changing any
strategy, gates, execution mode, paper/live executor, wallet, or risk setting.

It intentionally reads only:
- candidate_shadow_observations current_all context carrier rows, and
- raw_signal_outcomes gold/silver denominator rows when available.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import tempfile
import time
from collections import Counter
from pathlib import Path


SCHEMA_VERSION = "context_blocker_monitor.v1"
DEFAULT_CONTEXT_CARRIER = "current_all"
EXPECTED_COMMIT = "1830286fcd8f326d40b19ceb4b394d70db1eb0bf"


def utc_now_ts() -> int:
    return int(time.time())


def iso(ts):
    if ts is None:
        return None
    return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def table_exists(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def cols(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def safe_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def jloads(text):
    try:
        return json.loads(text or "{}")
    except Exception:
        return {}


def payload_value(payload, key, default="MISSING"):
    if key not in payload:
        return default
    value = payload.get(key)
    if value in (None, ""):
        return "UNKNOWN"
    return str(value)


def load_context_rows(db, since_ts, carrier_candidate_id):
    if not table_exists(db, "candidate_shadow_observations"):
        return []
    rows = db.execute(
        """
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json
        FROM candidate_shadow_observations
        WHERE candidate_id = ?
          AND observed_at >= ?
        ORDER BY observed_at ASC
        """,
        (carrier_candidate_id, int(since_ts)),
    ).fetchall()
    out = []
    for row in rows:
        row = dict(row)
        out.append((row, jloads(row.get("payload_json"))))
    return out


def presence_stats(rows, key):
    den = len(rows)
    present = missing = unknown = not_applicable = true_rows = false_rows = 0
    for _row, payload in rows:
        if key not in payload:
            missing += 1
            continue
        value = payload.get(key)
        if isinstance(value, str) and value.strip().lower() in {"unknown", "unk"}:
            unknown += 1
        elif value is None or (isinstance(value, str) and value.strip().lower() in {"not_applicable", "n/a", "not-applicable"}):
            not_applicable += 1
            present += 1
        elif bool(value):
            true_rows += 1
            present += 1
        else:
            false_rows += 1
            present += 1
    return {
        "denominator_rows": den,
        "present_rows": present,
        "missing_rows": missing,
        "unknown_rows": unknown,
        "not_applicable_rows": not_applicable,
        "true_rows": true_rows,
        "false_rows": false_rows,
        "present_rate": rate(present, den),
        "missing_rate": rate(missing, den),
        "unknown_rate": rate(unknown, den),
        "not_applicable_rate": rate(not_applicable, den),
        "true_rate": rate(true_rows, den),
        "false_rate": rate(false_rows, den),
    }


def missing_breakdown(rows, target_key):
    dims = {
        "by_context_schema_version": "context_schema_version",
        "by_source_component": "source_component",
        "by_writer_path": "quote_context_writer_path",
        "by_candidate_family": "candidate_family",
        "by_signal_type": "signal_type",
    }
    result = {}
    for label, key in dims.items():
        counter = Counter()
        for _row, payload in rows:
            if target_key not in payload:
                counter[payload_value(payload, key)] += 1
        result[label] = dict(counter.most_common())
    return result


def load_raw_gold_silver_kline_coverage(raw_db, since_ts):
    result = {
        "available": False,
        "source": "raw_signal_outcomes",
        "raw_all_gold_silver_event_rows": 0,
        "raw_all_gold_silver_unique_tokens": 0,
        "kline_coverage_rate": None,
        "kline_covered_rows": 0,
        "dropped_kline_uncovered": 0,
        "reason": None,
    }
    if raw_db is None or not table_exists(raw_db, "raw_signal_outcomes"):
        result["reason"] = "raw_signal_outcomes_unavailable"
        return result
    columns = cols(raw_db, "raw_signal_outcomes")
    tier_exprs = []
    if "raw_primary_tier" in columns:
        tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if "raw_sustained_tier" in columns:
        tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    if not tier_exprs:
        result["reason"] = "missing_gold_silver_tier_columns"
        return result
    select = []
    for name in ("id", "signal_id", "token_ca", "signal_ts", "raw_primary_tier", "raw_sustained_tier", "kline_covered"):
        select.append(name if name in columns else f"NULL AS {name}")
    rows = raw_db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE COALESCE(signal_ts, 0) >= ?
          AND ({' OR '.join(tier_exprs)})
        """,
        (int(since_ts),),
    ).fetchall()
    if not rows:
        return {**result, "available": True, "reason": "raw_gold_silver_empty"}
    covered = 0
    for row in rows:
        if "kline_covered" in columns and safe_bool(row["kline_covered"]):
            covered += 1
    unique = len({row["token_ca"] for row in rows if row["token_ca"]})
    return {
        **result,
        "available": True,
        "reason": None,
        "raw_all_gold_silver_event_rows": len(rows),
        "raw_all_gold_silver_unique_tokens": unique,
        "kline_covered_rows": covered,
        "dropped_kline_uncovered": len(rows) - covered if "kline_covered" in columns else None,
        "kline_coverage_rate": rate(covered, len(rows)) if "kline_covered" in columns else None,
    }


def build_report(args):
    now_ts = int(args.now_ts or utc_now_ts())
    rolling_since = now_ts - int(float(args.hours) * 3600)
    deploy_ts = int(args.deploy_ts or 0)
    if deploy_ts <= 0:
        raise SystemExit("--deploy-ts is required for clean-window monitoring")

    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    raw_db = None
    if args.raw_db and Path(args.raw_db).exists():
        raw_db = sqlite3.connect(args.raw_db)
        raw_db.row_factory = sqlite3.Row
    try:
        rolling_rows = load_context_rows(paper_db, rolling_since, args.context_carrier)
        post_deploy_rows = [(row, payload) for row, payload in rolling_rows if int(row.get("observed_at") or 0) >= deploy_ts]
        pre_fix_rows = [
            (row, payload)
            for row, payload in rolling_rows
            if int(row.get("observed_at") or 0) < deploy_ts or "quote_context_writer_path" not in payload
        ]

        post_quote_clean = presence_stats(post_deploy_rows, "source_quote_clean")
        post_quote_exec = presence_stats(post_deploy_rows, "source_quote_executable")
        rolling_quote_clean = presence_stats(rolling_rows, "source_quote_clean")
        rolling_quote_exec = presence_stats(rolling_rows, "source_quote_executable")
        post_quote_healthy = (post_quote_clean["present_rate"] or 0) >= 0.99 and (post_quote_exec["present_rate"] or 0) >= 0.99
        if post_quote_healthy:
            quote_classification = "VERIFIED_POST_DEPLOY"
        else:
            quote_classification = "NEEDS_WRITER_FIX"

        estimated_clean_at = deploy_ts + int(float(args.hours) * 3600)
        if len(pre_fix_rows) == 0 and (rolling_quote_clean["present_rate"] or 0) >= 0.99 and (rolling_quote_exec["present_rate"] or 0) >= 0.99:
            clean_status = "CLEAN_WINDOW_READY"
        elif quote_classification == "VERIFIED_POST_DEPLOY":
            clean_status = "QUOTE_CLEAN_WINDOW_PENDING"
        else:
            clean_status = "NEEDS_WRITER_FIX"

        volume = presence_stats(rolling_rows, "volume_profile")
        context_kline_rows = []
        context_kline_missing_rows = []
        for row, payload in rolling_rows:
            has_kline = any(
                key in payload
                for key in (
                    "volume_profile",
                    "candle_pattern",
                    "kline_bar_count",
                    "entry_bar_open_ts",
                    "entry_bar_close_ts",
                    "fbr_time_legal",
                )
            )
            (context_kline_rows if has_kline else context_kline_missing_rows).append((row, payload))
        raw_kline = load_raw_gold_silver_kline_coverage(raw_db, rolling_since)
        raw_kline_rate = raw_kline.get("kline_coverage_rate")
        context_kline_rate = rate(len(context_kline_rows), len(rolling_rows))
        h1_blocked = (volume["present_rate"] or 0) < 0.8 or (raw_kline_rate is not None and raw_kline_rate < 0.8)

        report = {
            "schema_version": SCHEMA_VERSION,
            "report_type": "context_blocker_monitor",
            "generated_at": now_ts,
            "generated_at_iso": iso(now_ts),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "canonical_backfill_performed": False,
            "inputs": {
                "paper_db": args.db,
                "raw_db": args.raw_db,
                "hours": args.hours,
                "deploy_ts": deploy_ts,
                "deploy_iso": iso(deploy_ts),
                "context_carrier": args.context_carrier,
                "expected_commit": args.expected_commit,
            },
            "task_a_post_deploy_quote_smoke_test": {
                "classification": quote_classification,
                "rows_scanned": len(post_deploy_rows),
                "source_quote_clean_present_rate": post_quote_clean["present_rate"],
                "source_quote_executable_present_rate": post_quote_exec["present_rate"],
                "source_quote_clean_missing_rows": post_quote_clean["missing_rows"],
                "source_quote_executable_missing_rows": post_quote_exec["missing_rows"],
                "missing_rows": max(post_quote_clean["missing_rows"], post_quote_exec["missing_rows"]),
                "writer_path_breakdown": dict(Counter(payload_value(payload, "quote_context_writer_path") for _row, payload in post_deploy_rows).most_common()),
                "post_deploy_quote_context_healthy": post_quote_healthy,
            },
            "task_b_clean_window_monitor": {
                "classification": clean_status,
                "pre_fix_rows_remaining": len(pre_fix_rows),
                "post_fix_rows": len(post_deploy_rows),
                "rolling24_rows": len(rolling_rows),
                "estimated_clean_window_status": clean_status,
                "estimated_clean_at_ts": estimated_clean_at,
                "estimated_clean_at_iso": iso(estimated_clean_at),
                "seconds_until_natural_clean_window": max(0, estimated_clean_at - now_ts),
                "quote_coverage_post_fix_rows_only": {
                    "rows_scanned": len(post_deploy_rows),
                    "source_quote_clean_present_rate": post_quote_clean["present_rate"],
                    "source_quote_executable_present_rate": post_quote_exec["present_rate"],
                    "source_quote_clean_missing_rows": post_quote_clean["missing_rows"],
                    "source_quote_executable_missing_rows": post_quote_exec["missing_rows"],
                },
                "quote_coverage_rolling24": {
                    "rows_scanned": len(rolling_rows),
                    "source_quote_clean_present_rate": rolling_quote_clean["present_rate"],
                    "source_quote_executable_present_rate": rolling_quote_exec["present_rate"],
                    "source_quote_clean_missing_rows": rolling_quote_clean["missing_rows"],
                    "source_quote_executable_missing_rows": rolling_quote_exec["missing_rows"],
                },
            },
            "task_c_volume_kline_coverage_audit": {
                "classification": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "VOLUME_KLINE_HEALTHY",
                "rows_scanned": len(rolling_rows),
                "volume_profile_present_rate": volume["present_rate"],
                "volume_profile_missing_rate": volume["missing_rate"],
                "volume_profile_unknown_rate": volume["unknown_rate"],
                "volume_profile_not_applicable_rate": volume["not_applicable_rate"],
                "volume_profile_present_rows": volume["present_rows"],
                "volume_profile_missing_rows": volume["missing_rows"],
                "volume_profile_unknown_rows": volume["unknown_rows"],
                "volume_profile_not_applicable_rows": volume["not_applicable_rows"],
                "candidate_context_kline_coverage_rate": context_kline_rate,
                "candidate_context_kline_missing_rows": len(context_kline_missing_rows),
                "raw_gold_silver_kline_coverage": raw_kline,
                "kline_coverage_rate": raw_kline_rate if raw_kline_rate is not None else context_kline_rate,
                "missing_breakdown_volume_profile": missing_breakdown(rolling_rows, "volume_profile"),
                "missing_breakdown_kline": {
                    "by_context_schema_version": dict(Counter(payload_value(payload, "context_schema_version") for _row, payload in context_kline_missing_rows).most_common()),
                    "by_source_component": dict(Counter(payload_value(payload, "source_component") for _row, payload in context_kline_missing_rows).most_common()),
                    "by_writer_path": dict(Counter(payload_value(payload, "quote_context_writer_path") for _row, payload in context_kline_missing_rows).most_common()),
                    "by_candidate_family": dict(Counter(payload_value(payload, "candidate_family") for _row, payload in context_kline_missing_rows).most_common()),
                    "by_signal_type": dict(Counter(payload_value(payload, "signal_type") for _row, payload in context_kline_missing_rows).most_common()),
                },
                "h1_status": "DATA_BLOCKED_VOLUME_KLINE" if h1_blocked else "H1_DATA_AVAILABLE_FOR_DISCOVERY_ONLY",
                "h1_remains_blocked": h1_blocked,
            },
        }
        report["overall_verdict"] = {
            "quote_writer_fix": quote_classification,
            "rolling24_quote_status": clean_status,
            "h1_volume_kline_status": report["task_c_volume_kline_coverage_audit"]["classification"],
            "promotion_allowed": False,
        }
        return report
    finally:
        paper_db.close()
        if raw_db is not None:
            raw_db.close()


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def compact_summary(report):
    task_a = report["task_a_post_deploy_quote_smoke_test"]
    task_b = report["task_b_clean_window_monitor"]
    task_c = report["task_c_volume_kline_coverage_audit"]
    return {
        "overall_verdict": report.get("overall_verdict"),
        "promotion_allowed": False,
        "task_a": {
            "classification": task_a.get("classification"),
            "rows_scanned": task_a.get("rows_scanned"),
            "source_quote_clean_present_rate": task_a.get("source_quote_clean_present_rate"),
            "source_quote_executable_present_rate": task_a.get("source_quote_executable_present_rate"),
            "missing_rows": task_a.get("missing_rows"),
            "writer_path_breakdown": task_a.get("writer_path_breakdown"),
        },
        "task_b": {
            "classification": task_b.get("classification"),
            "pre_fix_rows_remaining": task_b.get("pre_fix_rows_remaining"),
            "post_fix_rows": task_b.get("post_fix_rows"),
            "estimated_clean_at_iso": task_b.get("estimated_clean_at_iso"),
            "quote_coverage_post_fix_rows_only": task_b.get("quote_coverage_post_fix_rows_only"),
            "quote_coverage_rolling24": task_b.get("quote_coverage_rolling24"),
        },
        "task_c": {
            "classification": task_c.get("classification"),
            "volume_profile_present_rate": task_c.get("volume_profile_present_rate"),
            "volume_profile_missing_rate": task_c.get("volume_profile_missing_rate"),
            "volume_profile_unknown_rate": task_c.get("volume_profile_unknown_rate"),
            "volume_profile_not_applicable_rate": task_c.get("volume_profile_not_applicable_rate"),
            "kline_coverage_rate": task_c.get("kline_coverage_rate"),
            "h1_remains_blocked": task_c.get("h1_remains_blocked"),
        },
    }


def self_test():
    now = 2_000_000
    deploy = now - 100
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper = root / "paper.db"
        raw = root / "raw.db"
        db = sqlite3.connect(paper)
        db.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        rows = [
            (1, "A", now - 200, "current_all", "base", 1, "x", now - 200, {"context_schema_version": "v2", "candidate_family": "base", "signal_type": "ATH"}),
            (2, "B", now - 50, "current_all", "base", 1, "x", now - 50, {"context_schema_version": "v2", "candidate_family": "base", "signal_type": "ATH", "quote_context_writer_path": "candidate_shadow_observer:inferred", "source_quote_clean": False, "source_quote_executable": False, "volume_profile": "building", "candle_pattern": "green"}),
            (3, "C", now - 25, "current_all", "base", 1, "x", now - 25, {"context_schema_version": "v2", "candidate_family": "base", "signal_type": "NEW_TRENDING", "quote_context_writer_path": "candidate_shadow_observer:inferred", "source_quote_clean": True, "source_quote_executable": True, "volume_profile": "unknown", "fbr_time_legal": True}),
        ]
        db.executemany(
            "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, json.dumps(i)) for a, b, c, d, e, f, g, h, i in rows],
        )
        db.commit()
        db.close()
        rdb = sqlite3.connect(raw)
        rdb.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              id INTEGER, signal_id INTEGER, token_ca TEXT, signal_ts INTEGER,
              raw_primary_tier TEXT, raw_sustained_tier TEXT, kline_covered INTEGER
            )
            """
        )
        rdb.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?)",
            [
                (1, 1, "A", now - 200, "gold", None, 1),
                (2, 2, "B", now - 50, "silver", None, 0),
            ],
        )
        rdb.commit()
        rdb.close()
        args = argparse.Namespace(
            db=str(paper),
            raw_db=str(raw),
            hours=1,
            deploy_ts=deploy,
            now_ts=now,
            context_carrier="current_all",
            expected_commit=EXPECTED_COMMIT,
        )
        report = build_report(args)
        assert report["task_a_post_deploy_quote_smoke_test"]["classification"] == "VERIFIED_POST_DEPLOY"
        assert report["task_a_post_deploy_quote_smoke_test"]["missing_rows"] == 0
        assert report["task_b_clean_window_monitor"]["classification"] == "QUOTE_CLEAN_WINDOW_PENDING"
        assert report["task_b_clean_window_monitor"]["pre_fix_rows_remaining"] == 1
        assert report["task_c_volume_kline_coverage_audit"]["classification"] == "DATA_BLOCKED_VOLUME_KLINE"
        assert report["promotion_allowed"] is False
    print("SELF_TEST_PASS context_blocker_monitor")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--deploy-ts", type=int, default=0)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--context-carrier", default=DEFAULT_CONTEXT_CARRIER)
    parser.add_argument("--expected-commit", default=EXPECTED_COMMIT)
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
