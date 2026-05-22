#!/usr/bin/env python3
"""Validate materialized v2.7 read-model snapshots before dashboard/gate use."""

import argparse
import json
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_denominator_projection import _lag_ms, _utc_now_iso  # noqa: E402
from v27_event_log import sha256_hex  # noqa: E402


DEFAULT_DENOMINATOR_SNAPSHOT = PROJECT_ROOT / "data" / "v27_read_models" / "denominator_snapshot.json"
DENOMINATOR_SNAPSHOT_SCHEMA_VERSION = "v2.7.0.denominator_read_model.v1"


def _load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _expected_snapshot_hash(snapshot):
    return sha256_hex({key: value for key, value in snapshot.items() if key != "snapshot_hash"})


def _expected_projection_hash(snapshot):
    projection = snapshot.get("projection")
    if not isinstance(projection, dict):
        return None
    return sha256_hex({key: value for key, value in projection.items() if key != "event_log_dir"})


def validate_snapshot_file(snapshot_path=DEFAULT_DENOMINATOR_SNAPSHOT, *, max_snapshot_age_ms=300_000, now_iso=None):
    snapshot_path = Path(snapshot_path)
    now_iso = now_iso or _utc_now_iso()
    report = {
        "snapshot_path": str(snapshot_path),
        "checked_at": now_iso,
        "snapshot_present": snapshot_path.exists(),
        "snapshot_parse_ok": False,
        "snapshot_schema_version": None,
        "snapshot_schema_ok": False,
        "snapshot_hash_ok": False,
        "projection_hash_ok": False,
        "spec_valid": False,
        "read_model_fresh_enough": False,
        "snapshot_age_ms": None,
        "max_snapshot_age_ms": max_snapshot_age_ms,
        "snapshot_age_ok": False,
        "read_model_seq": None,
        "event_log_latest_seq": None,
        "projection_status": None,
        "blocking_reasons": [],
        "health": {
            "dashboard_safe": False,
            "normal_tiny_ready": False,
            "status": "snapshot_missing",
        },
    }
    if not snapshot_path.exists():
        report["blocking_reasons"].append("snapshot_missing")
        return report

    try:
        snapshot = _load_json(snapshot_path)
    except Exception as exc:
        report["blocking_reasons"].append("snapshot_parse_failed")
        report["parse_error"] = str(exc)
        report["health"]["status"] = "snapshot_invalid"
        return report

    report["snapshot_parse_ok"] = True
    report["snapshot_schema_version"] = snapshot.get("snapshot_schema_version")
    report["snapshot_schema_ok"] = snapshot.get("snapshot_schema_version") == DENOMINATOR_SNAPSHOT_SCHEMA_VERSION
    report["snapshot_hash_ok"] = snapshot.get("snapshot_hash") == _expected_snapshot_hash(snapshot)

    expected_projection_hash = _expected_projection_hash(snapshot)
    report["projection_hash_ok"] = bool(expected_projection_hash and snapshot.get("projection_hash") == expected_projection_hash)

    spec = snapshot.get("spec") if isinstance(snapshot.get("spec"), dict) else {}
    read_model = snapshot.get("read_model") if isinstance(snapshot.get("read_model"), dict) else {}
    projection = snapshot.get("projection") if isinstance(snapshot.get("projection"), dict) else {}
    report["spec_valid"] = bool(spec.get("spec_valid"))
    report["read_model_fresh_enough"] = bool(read_model.get("read_model_fresh_enough"))
    report["snapshot_age_ms"] = _lag_ms(snapshot.get("generated_at"), now_iso)
    report["snapshot_age_ok"] = report["snapshot_age_ms"] is not None and report["snapshot_age_ms"] <= max_snapshot_age_ms
    report["read_model_seq"] = read_model.get("read_model_seq")
    report["event_log_latest_seq"] = read_model.get("event_log_latest_seq")
    report["projection_status"] = (projection.get("health") or {}).get("status")

    if not report["snapshot_schema_ok"]:
        report["blocking_reasons"].append("snapshot_schema_mismatch")
    if not report["snapshot_hash_ok"]:
        report["blocking_reasons"].append("snapshot_hash_mismatch")
    if not report["projection_hash_ok"]:
        report["blocking_reasons"].append("projection_hash_mismatch")
    if not report["spec_valid"]:
        report["blocking_reasons"].append("spec_invalid")
    if not report["read_model_fresh_enough"]:
        report["blocking_reasons"].append("read_model_stale")
    if not report["snapshot_age_ok"]:
        report["blocking_reasons"].append("snapshot_file_stale")

    dashboard_safe = not report["blocking_reasons"]
    report["health"] = {
        "dashboard_safe": dashboard_safe,
        "normal_tiny_ready": False,
        "status": "dashboard_read_model_fresh" if dashboard_safe else "dashboard_read_model_stale",
    }
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-path", default=str(DEFAULT_DENOMINATOR_SNAPSHOT))
    parser.add_argument("--max-snapshot-age-ms", type=int, default=300_000)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = validate_snapshot_file(args.snapshot_path, max_snapshot_age_ms=args.max_snapshot_age_ms)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not report["health"]["dashboard_safe"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
