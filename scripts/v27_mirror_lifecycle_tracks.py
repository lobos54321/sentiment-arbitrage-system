#!/usr/bin/env python3
"""Backfill legacy lifecycle/pool identity tracks into the v2.7 event log."""

import argparse
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


DEFAULT_LIFECYCLE_DB = PROJECT_ROOT / "data" / "lifecycle_tracks.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_lifecycle_mirror.lock")
LIFECYCLE_EVENT_TYPE = "token_lifecycle_identity_resolved"


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


def _to_epoch_seconds(value):
    if value is None:
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


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _lifecycle_id(token_ca, signal_ts):
    ts = _as_int(_to_epoch_seconds(signal_ts), default=0)
    return f"{token_ca}:{ts}" if token_ca and ts else None


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


def iter_lifecycle_rows(db, *, since_id=None, until_id=None, limit=None, table="tracks"):
    if not _table_exists(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _lifecycle_payload(row, *, default_chain="solana"):
    pool_address = row.get("pool_address")
    token_ca = row.get("token_ca")
    return {
        "lifecycle_track_id": row.get("id"),
        "token_ca": token_ca,
        "symbol": row.get("symbol"),
        "chain": default_chain,
        "signal_ts": row.get("signal_ts"),
        "entry_price": row.get("entry_price"),
        "entry_ts": row.get("entry_ts"),
        "pool_address": pool_address,
        "canonical_pool_group": pool_address or "unknown_pool",
        "lifecycle_epoch": 0,
        "lifecycle_id": _lifecycle_id(token_ca, row.get("signal_ts")),
        "lifecycle_status": row.get("status"),
        "complete_ts": row.get("complete_ts"),
        "complete_reason": row.get("complete_reason"),
        "pool_resolution_quality": "legacy_lifecycle_track" if pool_address else "missing_pool",
        "legacy_lifecycle_track": row,
    }


def mirror_lifecycle_tracks(
    lifecycle_db,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    dry_run=False,
    table="tracks",
    default_chain="solana",
):
    summary = {
        "lifecycle_db": str(lifecycle_db),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
    }
    with _connect(lifecycle_db) as db:
        if not _table_exists(db, table):
            summary["failed"] += 1
            summary["failures"].append({"reason": f"{table} table missing"})
            return summary
        for row in iter_lifecycle_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            if dry_run:
                continue
            try:
                payload = _lifecycle_payload(row, default_chain=default_chain)
                token_ca = payload.get("token_ca")
                pool = payload.get("canonical_pool_group") or "unknown_pool"
                aggregate_id = f"token_lifecycle:{payload.get('chain')}:{token_ca}:{pool}:0" if token_ca else f"token_lifecycle:row:{row.get('id')}"
                result = V27EventLog(event_log_dir).append_event(
                    event_type=LIFECYCLE_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="lifecycle_tracks",
                    idempotency_key=f"lifecycle_tracks:{row.get('id')}",
                    observed_at=_timestamp_to_iso(payload.get("signal_ts")),
                    available_at=_timestamp_to_iso(payload.get("entry_ts")),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"lifecycle_track_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"lifecycle_track_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def _track_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="tracks"):
    return [row["id"] for row in iter_lifecycle_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_track_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != LIFECYCLE_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        track_id = payload.get("lifecycle_track_id")
        if track_id is None:
            continue
        counts[int(track_id)] = counts.get(int(track_id), 0) + 1
    return counts


def max_mirrored_track_id(event_log_dir):
    mirrored = _mirrored_track_ids(event_log_dir)
    return max(mirrored) if mirrored else None


def next_unmirrored_since_id(event_log_dir, configured_since_id=None):
    max_id = max_mirrored_track_id(event_log_dir)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_lifecycle_mirror_parity(lifecycle_db, event_log_dir, *, since_id=None, until_id=None, limit=None, table="tracks"):
    summary = {
        "lifecycle_db": str(lifecycle_db),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_track_ids": [],
        "duplicate_track_ids": [],
        "orphan_mirrored_track_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
    }
    with _connect(lifecycle_db) as db:
        if not _table_exists(db, table):
            summary["event_log_error"] = f"{table} table missing"
            return summary
        db_ids = _track_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)

    mirrored_counts = _mirrored_track_ids(event_log_dir)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None

    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.get(track_id, 0) for track_id in db_id_set) if scoped else sum(mirrored_counts.values())
    summary["missing_track_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_track_ids"] = sorted([track_id for track_id in db_id_set if mirrored_counts.get(track_id, 0) > 1])
    summary["orphan_mirrored_track_ids"] = [] if scoped else sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = not summary["missing_track_ids"] and not summary["duplicate_track_ids"] and summary["event_log_error"] is None
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
    lifecycle_db = Path(args.lifecycle_db)
    event_log_dir = Path(args.event_log_dir)
    since_id = next_unmirrored_since_id(event_log_dir, args.since_id) if args.new_only else args.since_id
    mirror_summary = mirror_lifecycle_tracks(
        lifecycle_db,
        event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
        table=args.table,
        default_chain=args.default_chain,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_lifecycle_mirror_parity(
            lifecycle_db,
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
            "max_mirrored_track_id": max_mirrored_track_id(event_log_dir),
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
        print(f"v2.7 lifecycle mirror lock held at {args.lock_file}; duplicate worker idling", flush=True)
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
    parser.add_argument("--lifecycle-db", default=str(DEFAULT_LIFECYCLE_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--table", default="tracks")
    parser.add_argument("--default-chain", default="solana")
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

    lifecycle_db = Path(args.lifecycle_db)
    event_log_dir = Path(args.event_log_dir)

    if args.verify_only:
        summary = verify_lifecycle_mirror_parity(
            lifecycle_db,
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
