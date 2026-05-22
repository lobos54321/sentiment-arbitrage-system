#!/usr/bin/env python3
"""Refresh materialized v2.7 read-model snapshots from the event log.

This worker is observability-only. It rebuilds denominator read models from the
append-only v2.7 event log, writes them atomically, then validates the snapshot
that dashboard/health checks are allowed to consume.
"""

import argparse
import json
import os
import sys
import tempfile
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
from v27_read_model_freshness import DEFAULT_DENOMINATOR_SNAPSHOT, validate_snapshot_file  # noqa: E402


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "v27_read_models"
DEFAULT_DENOMINATOR_PROJECTION = DEFAULT_OUTPUT_DIR / "denominator_projection.json"
DEFAULT_REFRESH_HEALTH = DEFAULT_OUTPUT_DIR / "denominator_freshness.json"


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

    refresh_report = {
        "refresh_schema_version": "v2.7.0.read_model_refresh.v1",
        "event_log_dir": str(event_log_dir),
        "projection_path": str(projection_path),
        "snapshot_path": str(snapshot_path),
        "health_path": str(health_path),
        "projection_hash": snapshot.get("projection_hash"),
        "snapshot_hash": snapshot.get("snapshot_hash"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "read_model_seq": verifier_report.get("read_model_seq"),
        "event_log_latest_seq": verifier_report.get("event_log_latest_seq"),
        "projection_status": verifier_report.get("projection_status"),
        "dashboard_safe": bool(verifier_report.get("health", {}).get("dashboard_safe")),
        "blocking_reasons": verifier_report.get("blocking_reasons") or [],
        "verifier_report": verifier_report,
        "health": {
            "status": "read_model_refresh_ok" if verifier_report.get("health", {}).get("dashboard_safe") else "read_model_refresh_not_ready",
            "dashboard_safe": bool(verifier_report.get("health", {}).get("dashboard_safe")),
            "normal_tiny_ready": False,
        },
    }
    write_json_atomic(health_path, refresh_report)
    return refresh_report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--projection-path")
    parser.add_argument("--snapshot-path")
    parser.add_argument("--health-path")
    parser.add_argument("--spec-manifest", default=str(DEFAULT_SPEC_MANIFEST))
    parser.add_argument("--include-records", action="store_true")
    parser.add_argument("--max-allowed-lag-seq", type=int, default=0)
    parser.add_argument("--max-allowed-lag-ms", type=int, default=300_000)
    parser.add_argument("--max-snapshot-age-ms", type=int, default=300_000)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    projection_path = Path(args.projection_path) if args.projection_path else output_dir / "denominator_projection.json"
    snapshot_path = Path(args.snapshot_path) if args.snapshot_path else output_dir / "denominator_snapshot.json"
    health_path = Path(args.health_path) if args.health_path else output_dir / "denominator_freshness.json"

    report = refresh_denominator_read_model(
        event_log_dir=Path(args.event_log_dir),
        projection_path=projection_path,
        snapshot_path=snapshot_path,
        health_path=health_path,
        spec_manifest_path=Path(args.spec_manifest),
        include_records=args.include_records,
        max_allowed_lag_seq=args.max_allowed_lag_seq,
        max_allowed_lag_ms=args.max_allowed_lag_ms,
        max_snapshot_age_ms=args.max_snapshot_age_ms,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not report["health"]["dashboard_safe"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
