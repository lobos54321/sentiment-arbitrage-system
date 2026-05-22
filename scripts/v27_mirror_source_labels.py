#!/usr/bin/env python3
"""Backfill legacy source dog label seeds into the v2.7 event log."""

import argparse
import calendar
import fcntl
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402


DEFAULT_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_source_label_mirror.lock")
SOURCE_LABEL_EVENT_TYPE = "source_dog_label_recorded"


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _value(row, *names):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _normalize_chain(value):
    if value is None:
        return "unknown_chain"
    normalized = str(value).strip().lower()
    if normalized in {"sol", "solana"}:
        return "solana"
    if normalized in {"bsc", "bnb", "binance", "binance_smart_chain"}:
        return "bsc"
    if normalized in {"eth", "ethereum"}:
        return "ethereum"
    if normalized in {"base"}:
        return "base"
    return normalized or "unknown_chain"


def _to_epoch_seconds(value):
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            ts = float(stripped)
            return ts / 1000.0 if ts > 1_000_000_000_000 else ts
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    parsed = time.strptime(stripped.replace("Z", "").split(".")[0], fmt)
                    return float(calendar.timegm(parsed))
                except Exception:
                    continue
            return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def _timestamp_to_iso(value):
    ts = _to_epoch_seconds(value)
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _row_filters(since_id=None, until_id=None):
    clauses = []
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return where, params


def iter_source_feature_rows(db, *, since_id=None, until_id=None, limit=None, table="signal_features"):
    if not _table_exists(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _label_from_row(row):
    is_gold = _as_bool(row.get("is_gold_dog"))
    is_silver = _as_bool(row.get("is_silver_dog"))
    gain = _as_float(row.get("max_gain_24h"))
    if is_gold is True:
        return "gold", "legacy_is_gold_dog_flag", gain
    if is_silver is True:
        return "silver", "legacy_is_silver_dog_flag", gain
    if gain is None:
        return None, "legacy_label_unresolved", gain
    if gain >= 100.0:
        return "gold", "legacy_max_gain_24h_pct", gain
    if gain >= 50.0:
        return "silver", "legacy_max_gain_24h_pct", gain
    if gain >= 30.0:
        return "copper", "legacy_max_gain_24h_pct", gain
    return "none", "legacy_max_gain_24h_pct", gain


def _source_label_payload(row):
    token_ca = _value(row, "token_ca", "ca", "contract_address")
    chain = _normalize_chain(_value(row, "chain"))
    label, quality, gain = _label_from_row(row)
    label_available_at = _value(row, "captured_at", "tracked_at")
    reference_price = _as_float(_value(row, "entry_price"))
    return {
        "source_label_id": row.get("id"),
        "token_ca": token_ca,
        "symbol": _value(row, "symbol"),
        "chain": chain,
        "canonical_pool_group": _value(row, "canonical_pool_group", "pool_group") or "unknown_pool",
        "lifecycle_epoch": _value(row, "lifecycle_epoch") or 0,
        "source_dog_label": label,
        "source_dog_label_version": "legacy_signal_features_seed_v0.1",
        "source_label_quality": quality,
        "source_label_research_only": True,
        "source_reference_price_type": "legacy_entry_price" if reference_price is not None else "missing",
        "source_reference_price": reference_price,
        "source_label_window": "24h",
        "source_peak_type": "legacy_max_gain_24h_pct",
        "source_peak_value": gain,
        "source_gold_threshold": 100.0,
        "source_silver_threshold": 50.0,
        "source_copper_threshold": 30.0,
        "source_label_available_at": label_available_at,
        "legacy_signal_features": row,
    }


def mirror_source_labels(
    db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    dry_run=False,
    table="signal_features",
):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
    }
    with _connect(db_path) as db:
        if not _table_exists(db, table):
            summary["failed"] += 1
            summary["failures"].append({"reason": f"{table} table missing"})
            return summary
        for row in iter_source_feature_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                payload = _source_label_payload(row)
                token_ca = payload.get("token_ca")
                aggregate_id = f"source_label:{payload.get('chain')}:{token_ca}:unknown_pool:0" if token_ca else f"source_label:row:{row.get('id')}"
                result = V27EventLog(event_log_dir).append_event(
                    event_type=SOURCE_LABEL_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="signal_features",
                    idempotency_key=f"signal_features_source_label:{row.get('id')}",
                    observed_at=_timestamp_to_iso(payload.get("source_label_available_at")),
                    available_at=_timestamp_to_iso(payload.get("source_label_available_at")),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"source_label_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"source_label_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def _source_label_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="signal_features"):
    return [row["id"] for row in iter_source_feature_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_source_label_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != SOURCE_LABEL_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        source_label_id = payload.get("source_label_id")
        if source_label_id is None:
            continue
        counts[int(source_label_id)] = counts.get(int(source_label_id), 0) + 1
    return counts


def max_mirrored_source_label_id(event_log_dir):
    mirrored = _mirrored_source_label_ids(event_log_dir)
    return max(mirrored) if mirrored else None


def next_unmirrored_since_id(event_log_dir, configured_since_id=None):
    max_id = max_mirrored_source_label_id(event_log_dir)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_source_label_mirror_parity(db_path, event_log_dir, *, since_id=None, until_id=None, limit=None, table="signal_features"):
    summary = {
        "db_path": str(db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_source_label_ids": [],
        "duplicate_source_label_ids": [],
        "orphan_mirrored_source_label_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    with _connect(db_path) as db:
        if not _table_exists(db, table):
            summary["event_log_error"] = f"{table} table missing"
            return summary
        db_ids = _source_label_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)

    mirrored_counts = _mirrored_source_label_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None

    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.get(source_label_id, 0) for source_label_id in db_id_set) if scoped else sum(mirrored_counts.values())
    summary["missing_source_label_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_source_label_ids"] = sorted([source_label_id for source_label_id in db_id_set if mirrored_counts.get(source_label_id, 0) > 1])
    summary["orphan_mirrored_source_label_ids"] = [] if scoped else sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)

    summary["parity_ok"] = (
        not summary["missing_source_label_ids"]
        and not summary["duplicate_source_label_ids"]
        and summary["event_log_error"] is None
    )
    return summary


def acquire_loop_lock(lock_path):
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    fh.write(f"{os.getpid()}\n")
    fh.flush()
    return fh


def run_mirror_once(args):
    db_path = Path(args.db)
    event_log_dir = Path(args.event_log_dir)
    since_id = next_unmirrored_since_id(event_log_dir, args.since_id) if args.new_only else args.since_id
    mirror_summary = mirror_source_labels(
        db_path,
        event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
        table=args.table,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_source_label_mirror_parity(
            db_path,
            event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
        )
    return {
        "mirror": mirror_summary,
        "verify": verify_summary,
        "cursor": {
            "new_only": bool(args.new_only),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_source_label_id": max_mirrored_source_label_id(event_log_dir),
        },
    }


def run_mirror_loop(args):
    interval = max(5, int(args.interval))
    stop_requested = False

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    lock_fh = acquire_loop_lock(args.lock_file)
    if lock_fh is None:
        print(f"v2.7 source label mirror lock held at {args.lock_file}; duplicate worker idling", flush=True)
        while not stop_requested:
            time.sleep(interval)
        return {"status": "duplicate_worker_stopped", "lock_file": str(args.lock_file)}

    if args.initial_delay:
        time.sleep(max(0, int(args.initial_delay)))

    last_result = None
    try:
        while not stop_requested:
            try:
                last_result = run_mirror_once(args)
                print(json.dumps(last_result, ensure_ascii=False, sort_keys=True), flush=True)
            except Exception as exc:
                print(json.dumps({
                    "mirror": {
                        "failed": 1,
                        "failures": [{"reason": str(exc)}],
                    },
                    "verify": None,
                    "error": str(exc),
                }, ensure_ascii=False, sort_keys=True), flush=True)
            slept = 0
            while slept < interval and not stop_requested:
                time.sleep(min(1, interval - slept))
                slept += 1
    finally:
        try:
            lock_fh.close()
        except Exception:
            pass
    return last_result or {"status": "stopped_before_first_mirror"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--table", default="signal_features")
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--new-only", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--initial-delay", type=int, default=0)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    event_log_dir = Path(args.event_log_dir)

    if args.verify_only:
        summary = verify_source_label_mirror_parity(
            db_path,
            event_log_dir,
            since_id=args.since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
        raise SystemExit(0 if summary["parity_ok"] else 1)

    result = run_mirror_loop(args) if args.loop else run_mirror_once(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    mirror_summary = result.get("mirror") or {}
    verify_summary = result.get("verify")
    if mirror_summary.get("failed") or (verify_summary and not verify_summary["parity_ok"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
