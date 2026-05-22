#!/usr/bin/env python3
"""Refresh materialized v2.7 read-model snapshots from the event log.

This worker is observability-only. It rebuilds denominator read models from the
append-only v2.7 event log, writes them atomically, then validates the snapshot
that dashboard/health checks are allowed to consume.
"""

import argparse
import fcntl
import json
import os
import signal
import sys
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_denominator_projection import (  # noqa: E402
    DEFAULT_EVENT_LOG_DIR,
    DEFAULT_SPEC_MANIFEST,
    build_denominator_projection,
    build_denominator_read_model_snapshot,
)
from v27_mode_readiness import build_mode_readiness_matrix  # noqa: E402
from v27_read_model_freshness import DEFAULT_DENOMINATOR_SNAPSHOT, validate_snapshot_file  # noqa: E402


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "v27_read_models"
DEFAULT_DENOMINATOR_PROJECTION = DEFAULT_OUTPUT_DIR / "denominator_projection.json"
DEFAULT_REFRESH_HEALTH = DEFAULT_OUTPUT_DIR / "denominator_freshness.json"
DEFAULT_LOCK_FILE = Path("/tmp/v27_read_model_refresh.lock")


def write_json_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def refresh_denominator_read_model(
    *,
    event_log_dir=DEFAULT_EVENT_LOG_DIR,
    projection_path=DEFAULT_DENOMINATOR_PROJECTION,
    snapshot_path=DEFAULT_DENOMINATOR_SNAPSHOT,
    health_path=DEFAULT_REFRESH_HEALTH,
    mode_readiness_path=None,
    spec_manifest_path=DEFAULT_SPEC_MANIFEST,
    include_records=False,
    max_allowed_lag_seq=0,
    max_allowed_lag_ms=300_000,
    max_snapshot_age_ms=300_000,
):
    projection = build_denominator_projection(event_log_dir, include_records=include_records)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=max_allowed_lag_seq,
        max_allowed_lag_ms=max_allowed_lag_ms,
        spec_manifest_path=spec_manifest_path,
    )

    write_json_atomic(projection_path, projection)
    write_json_atomic(snapshot_path, snapshot)
    verifier_report = validate_snapshot_file(snapshot_path, max_snapshot_age_ms=max_snapshot_age_ms)
    mode_readiness_path = Path(mode_readiness_path) if mode_readiness_path else Path(health_path).parent / "mode_readiness.json"
    mode_readiness = build_mode_readiness_matrix(
        event_log_dir=Path(event_log_dir),
        snapshot_path=Path(snapshot_path),
        manifest_path=Path(spec_manifest_path),
        max_snapshot_age_ms=max_snapshot_age_ms,
    )

    refresh_report = {
        "refresh_schema_version": "v2.7.0.read_model_refresh.v1",
        "event_log_dir": str(event_log_dir),
        "projection_path": str(projection_path),
        "snapshot_path": str(snapshot_path),
        "health_path": str(health_path),
        "mode_readiness_path": str(mode_readiness_path),
        "projection_hash": snapshot.get("projection_hash"),
        "snapshot_hash": snapshot.get("snapshot_hash"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "read_model_seq": verifier_report.get("read_model_seq"),
        "event_log_latest_seq": verifier_report.get("event_log_latest_seq"),
        "projection_status": verifier_report.get("projection_status"),
        "dashboard_safe": bool(verifier_report.get("health", {}).get("dashboard_safe")),
        "blocking_reasons": verifier_report.get("blocking_reasons") or [],
        "mode_readiness": {
            "highest_allowed_mode": mode_readiness.get("highest_allowed_mode"),
            "observe_only_ready": mode_readiness.get("health", {}).get("observe_only_ready"),
            "shadow_ready": mode_readiness.get("health", {}).get("shadow_ready"),
            "ultra_tiny_ready": mode_readiness.get("health", {}).get("ultra_tiny_ready"),
            "normal_tiny_ready": mode_readiness.get("health", {}).get("normal_tiny_ready"),
            "blocking_contracts": {
                mode: (mode_readiness.get("modes", {}).get(mode, {}) or {}).get("blocking_contracts", [])
                for mode in ("observe_only", "shadow", "ultra_tiny", "normal_tiny")
            },
        },
        "verifier_report": verifier_report,
        "health": {
            "status": "read_model_refresh_ok" if verifier_report.get("health", {}).get("dashboard_safe") else "read_model_refresh_not_ready",
            "dashboard_safe": bool(verifier_report.get("health", {}).get("dashboard_safe")),
            "normal_tiny_ready": False,
        },
    }
    write_json_atomic(mode_readiness_path, mode_readiness)
    write_json_atomic(health_path, refresh_report)
    return refresh_report


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


def run_refresh_loop(args):
    interval = max(5, int(args.interval))
    stop_requested = False

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    lock_fh = acquire_loop_lock(args.lock_file)
    if lock_fh is None:
        print(f"v2.7 read-model refresh lock held at {args.lock_file}; duplicate worker idling", flush=True)
        while not stop_requested:
            time.sleep(interval)
        return {"status": "duplicate_worker_stopped", "lock_file": str(args.lock_file)}

    if args.initial_delay:
        time.sleep(max(0, int(args.initial_delay)))

    last_report = None
    try:
        while not stop_requested:
            try:
                last_report = run_refresh_once(args)
                print(json.dumps(last_report, ensure_ascii=False, sort_keys=True), flush=True)
            except Exception as exc:
                print(json.dumps({
                    "refresh_schema_version": "v2.7.0.read_model_refresh.v1",
                    "health": {
                        "status": "read_model_refresh_exception",
                        "dashboard_safe": False,
                        "normal_tiny_ready": False,
                    },
                    "blocking_reasons": ["read_model_refresh_exception"],
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
    return last_report or {"status": "stopped_before_first_refresh"}


def run_refresh_once(args):
    output_dir = Path(args.output_dir)
    projection_path = Path(args.projection_path) if args.projection_path else output_dir / "denominator_projection.json"
    snapshot_path = Path(args.snapshot_path) if args.snapshot_path else output_dir / "denominator_snapshot.json"
    health_path = Path(args.health_path) if args.health_path else output_dir / "denominator_freshness.json"
    mode_readiness_path = Path(args.mode_readiness_path) if args.mode_readiness_path else output_dir / "mode_readiness.json"

    return refresh_denominator_read_model(
        event_log_dir=Path(args.event_log_dir),
        projection_path=projection_path,
        snapshot_path=snapshot_path,
        health_path=health_path,
        mode_readiness_path=mode_readiness_path,
        spec_manifest_path=Path(args.spec_manifest),
        include_records=args.include_records,
        max_allowed_lag_seq=args.max_allowed_lag_seq,
        max_allowed_lag_ms=args.max_allowed_lag_ms,
        max_snapshot_age_ms=args.max_snapshot_age_ms,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--projection-path")
    parser.add_argument("--snapshot-path")
    parser.add_argument("--health-path")
    parser.add_argument("--mode-readiness-path")
    parser.add_argument("--spec-manifest", default=str(DEFAULT_SPEC_MANIFEST))
    parser.add_argument("--include-records", action="store_true")
    parser.add_argument("--max-allowed-lag-seq", type=int, default=0)
    parser.add_argument("--max-allowed-lag-ms", type=int, default=300_000)
    parser.add_argument("--max-snapshot-age-ms", type=int, default=300_000)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--initial-delay", type=int, default=0)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = run_refresh_loop(args) if args.loop else run_refresh_once(args)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not report.get("health", {}).get("dashboard_safe"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
