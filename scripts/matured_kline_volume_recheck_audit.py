#!/usr/bin/env python3
"""Read-only matured kline volume-profile recheck audit.

This report asks whether candidate-shadow `volume_profile=unknown` rows would
become classifiable if the observer rechecked kline data after more bars had
matured. It only reads candidate observations and kline cache data, and never
backfills, changes strategy, changes gates, changes A_CLASS, or enables paper
/ live execution.
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


SCHEMA_VERSION = "matured_kline_volume_recheck_audit.v1"
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


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


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


def payload_text(payload, key):
    if key not in payload:
        return "MISSING"
    value = payload.get(key)
    if value is None:
        return "UNKNOWN"
    text = str(value).strip()
    return text if text else "UNKNOWN"


def bucket_bar_count(value):
    parsed = safe_float(value)
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


def bucket_age(seconds):
    parsed = safe_float(seconds)
    if parsed is None:
        return "missing"
    if parsed < 0:
        return "invalid_negative"
    if parsed < 60:
        return "lt_60s"
    if parsed < 180:
        return "60_180s"
    if parsed < 300:
        return "180_300s"
    if parsed < 900:
        return "300_900s"
    return "gte_900s"


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


def load_context_rows(db, since_ts, context_carrier, max_rows):
    if not table_exists(db, "candidate_shadow_observations"):
        return []
    rows = db.execute(
        """
        SELECT signal_id, token_ca, signal_ts, observed_at, payload_json
        FROM candidate_shadow_observations
        WHERE candidate_id = ?
          AND COALESCE(observed_at, 0) >= ?
        ORDER BY observed_at DESC
        LIMIT ?
        """,
        (context_carrier, int(since_ts), int(max_rows)),
    ).fetchall()
    return [(dict(row), jloads(row["payload_json"])) for row in rows]


def load_kline_bars(kline_db, token_ca, signal_ts, limit):
    if not token_ca or signal_ts is None or not table_exists(kline_db, "kline_1m"):
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


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    paper = sqlite3.connect(args.db)
    paper.row_factory = sqlite3.Row
    kline = None
    if args.kline_db and Path(args.kline_db).exists():
        kline = sqlite3.connect(args.kline_db)
        kline.row_factory = sqlite3.Row
    try:
        context_rows = load_context_rows(paper, since_ts, args.context_carrier, args.max_rows)
        kline_available = bool(kline is not None and table_exists(kline, "kline_1m"))
        status_counts = Counter(value_status(payload, "volume_profile") for _row, payload in context_rows)
        original_reason_counts = Counter(payload_text(payload, "volume_profile_reason") for _row, payload in context_rows)
        target_rows = [
            (row, payload)
            for row, payload in context_rows
            if value_status(payload, "volume_profile") in {"unknown", "missing"}
        ]
        rechecked = []
        rechecked_profile_counts = Counter()
        rechecked_reason_counts = Counter()
        original_to_rechecked = Counter()
        bar_buckets = Counter()
        signal_age_buckets = Counter()
        observed_lag_buckets = Counter()
        recoverable = 0
        still_unknown = 0
        if kline_available:
            for row, payload in target_rows:
                signal_ts = safe_float(row.get("signal_ts"))
                if signal_ts is None:
                    signal_ts = safe_float(payload.get("signal_ts"))
                bars = load_kline_bars(kline, row.get("token_ca"), signal_ts, args.kline_limit)
                current_profile = volume_profile(bars[:5])
                current_reason = profile_reason(bars[:5])
                if current_profile != "unknown":
                    recoverable += 1
                else:
                    still_unknown += 1
                rechecked_profile_counts[current_profile] += 1
                rechecked_reason_counts[current_reason] += 1
                original_to_rechecked[(str(payload.get("volume_profile") or "MISSING"), current_profile)] += 1
                bar_buckets[bucket_bar_count(len(bars))] += 1
                signal_age_buckets[bucket_age(now_ts - signal_ts if signal_ts is not None else None)] += 1
                observed_at = safe_float(row.get("observed_at"))
                observed_lag_buckets[bucket_age(observed_at - signal_ts if observed_at is not None and signal_ts is not None else None)] += 1
                if len(rechecked) < args.limit:
                    rechecked.append(
                        {
                            "signal_id": row.get("signal_id"),
                            "token_ca": row.get("token_ca"),
                            "signal_ts": row.get("signal_ts"),
                            "observed_at": row.get("observed_at"),
                            "original_volume_profile": payload.get("volume_profile", "MISSING"),
                            "original_volume_profile_reason": payload.get("volume_profile_reason", "MISSING"),
                            "original_kline_bar_count": payload.get("kline_bar_count"),
                            "current_kline_bar_count": len(bars),
                            "current_volume_profile": current_profile,
                            "current_volume_profile_reason": current_reason,
                            "signal_age_bucket_now": bucket_age(now_ts - signal_ts if signal_ts is not None else None),
                        }
                    )
        den = len(target_rows)
        recoverable_rate = rate(recoverable, den)
        if not kline_available:
            next_action = "kline_cache_unavailable_for_recheck"
        elif den == 0:
            next_action = "no_unknown_volume_rows_to_recheck"
        elif (recoverable_rate or 0) >= 0.5:
            next_action = "shadow_delayed_volume_recheck_likely_useful"
        elif recoverable:
            next_action = "shadow_delayed_volume_recheck_watch"
        else:
            next_action = "kline_source_coverage_or_fetch_timing_remains_blocker"
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "matured_kline_volume_recheck_audit",
            "generated_at": utc_now(),
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {
                "paper_db": args.db,
                "kline_db": args.kline_db,
                "context_carrier": args.context_carrier,
                "max_rows": args.max_rows,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "canonical_backfill_performed": False,
            "formal_denominator_changed": False,
            "kline_cache_available": kline_available,
            "context_rows_scanned": len(context_rows),
            "original_volume_profile_status_counts": dict(status_counts.most_common()),
            "original_volume_profile_reason_counts": dict(original_reason_counts.most_common()),
            "unknown_or_missing_rows": den,
            "recheck": {
                "rechecked_rows": den if kline_available else 0,
                "recoverable_known_rows": recoverable,
                "recoverable_known_rate": recoverable_rate,
                "still_unknown_rows": still_unknown,
                "still_unknown_rate": rate(still_unknown, den),
                "current_volume_profile_counts": dict(rechecked_profile_counts.most_common()),
                "current_volume_profile_reason_counts": dict(rechecked_reason_counts.most_common()),
                "current_kline_bar_count_bucket_counts": dict(bar_buckets.most_common()),
                "signal_age_bucket_counts_now": dict(signal_age_buckets.most_common()),
                "observed_lag_bucket_counts": dict(observed_lag_buckets.most_common()),
                "original_to_rechecked_profile_counts": [
                    {"original": key[0], "rechecked": key[1], "count": count}
                    for key, count in original_to_rechecked.most_common()
                ],
                "samples": rechecked,
            },
            "overall": {
                "classification": "DISCOVERY_ONLY_MATURED_KLINE_RECHECK",
                "next_action": next_action,
                "promotion_allowed": False,
            },
        }
    finally:
        paper.close()
        if kline is not None:
            kline.close()


def compact_summary(report):
    return {
        "overall": report.get("overall"),
        "promotion_allowed": False,
        "context_rows_scanned": report.get("context_rows_scanned"),
        "unknown_or_missing_rows": report.get("unknown_or_missing_rows"),
        "kline_cache_available": report.get("kline_cache_available"),
        "recheck": {
            key: (report.get("recheck") or {}).get(key)
            for key in (
                "rechecked_rows",
                "recoverable_known_rows",
                "recoverable_known_rate",
                "still_unknown_rows",
                "current_volume_profile_counts",
                "current_volume_profile_reason_counts",
                "current_kline_bar_count_bucket_counts",
                "signal_age_bucket_counts_now",
            )
        },
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
        rows = [
            (1, "TOK1", now - 600, "current_all", "base", 1, "all", now - 500, {"volume_profile": "unknown", "volume_profile_reason": "insufficient_kline_bars_lt_3", "kline_bar_count": 1}),
            (2, "TOK2", now - 300, "current_all", "base", 1, "all", now - 290, {"volume_profile": "unknown", "volume_profile_reason": "kline_bars_unavailable", "kline_bar_count": 0}),
            (3, "TOK3", now - 300, "current_all", "base", 1, "all", now - 290, {"volume_profile": "building", "volume_profile_reason": "classified_from_first_5_bars", "kline_bar_count": 5}),
        ]
        for row in rows:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (*row[:8], json.dumps(row[8])),
            )
        paper.commit()
        paper.close()
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
                ("TOK1", now - 600, 1, 1.1, 0.9, 1, 10),
                ("TOK1", now - 540, 1, 1.2, 0.9, 1.1, 20),
                ("TOK1", now - 480, 1.1, 1.4, 1.0, 1.3, 30),
                ("TOK2", now - 300, 1, 1.1, 0.9, 1, 10),
            ],
        )
        kline.commit()
        kline.close()
        args = argparse.Namespace(
            db=str(paper_path),
            kline_db=str(kline_path),
            hours=1,
            context_carrier="current_all",
            max_rows=100,
            kline_limit=125,
            limit=10,
            now_ts=now,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["formal_denominator_changed"] is False
        assert report["context_rows_scanned"] == 3
        assert report["unknown_or_missing_rows"] == 2
        assert report["recheck"]["recoverable_known_rows"] == 1
        assert report["recheck"]["current_volume_profile_counts"]["building"] == 1
        assert report["overall"]["next_action"] == "shadow_delayed_volume_recheck_likely_useful"
        compact = compact_summary(report)
        assert compact["recheck"]["recoverable_known_rate"] == 0.5
    print("SELF_TEST_PASS matured_kline_volume_recheck_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--context-carrier", default=DEFAULT_CONTEXT_CARRIER)
    parser.add_argument("--max-rows", type=int, default=200_000)
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
