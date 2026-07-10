#!/usr/bin/env python3
"""Audit whether current-looking reports share a fresh, consistent evidence lineage."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Any

from sqlite_evidence_utils import atomic_write_json, iso_or_none, parse_time, utc_now_iso


SCHEMA_VERSION = "evidence_clock_audit.v1"
EVIDENCE_TIME_KEYS = (
    "data_cut_at",
    "input_max_ts",
    "primary_capture_generated_at",
    "capture_generated_at",
    "source_generated_at",
)
PUBLISH_TIME_KEYS = (
    "generated_at",
    "completed_at",
    "updated_at",
    "created_at",
)
RUN_START_KEYS = ("run_started_at", "started_at")
RUN_ID_KEYS = ("run_id", "latest_run_id", "source_run_id", "primary_run_id")


def load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return None, str(error)
    return (value if isinstance(value, dict) else {"value": value}), None


def walk_values(value: Any, keys: tuple[str, ...]) -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []

    def visit(node: Any, prefix: str = "") -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                child_path = f"{prefix}.{key}" if prefix else str(key)
                if key in keys and child not in (None, ""):
                    found.append((child_path, child))
                visit(child, child_path)
        elif isinstance(node, list):
            for index, child in enumerate(node[:200]):
                visit(child, f"{prefix}[{index}]")

    visit(value)
    return found


def artifact_record(label: str, path: Path, now: datetime) -> dict[str, Any]:
    record: dict[str, Any] = {
        "label": label,
        "path": str(path),
        "exists": path.exists(),
        "parse_error": None,
        "run_ids": [],
        "time_candidates": [],
        "data_cut_time": None,
        "publish_time": None,
        "run_started_at": None,
        "effective_time": None,
        "age_sec": None,
        "size_bytes": path.stat().st_size if path.exists() else None,
    }
    if not path.exists():
        return record
    payload, error = load_json(path)
    if error:
        record["parse_error"] = error
        return record
    direct_run_ids = [payload.get(key) for key in RUN_ID_KEYS if payload.get(key) not in (None, "")]
    if direct_run_ids:
        record["run_ids"] = sorted({str(value) for value in direct_run_ids})
    else:
        run_values = walk_values(payload, RUN_ID_KEYS)
        record["run_ids"] = sorted({str(value) for _, value in run_values[:1] if value not in (None, "")})

    evidence_times = []
    publish_times = []
    for role, keys, output in (
        ("evidence", EVIDENCE_TIME_KEYS, evidence_times),
        ("publish", PUBLISH_TIME_KEYS, publish_times),
    ):
        for key_path, raw_value in walk_values(payload, keys):
            parsed = parse_time(raw_value)
            record["time_candidates"].append({
                "role": role,
                "field": key_path,
                "raw": raw_value,
                "parsed": iso_or_none(parsed),
            })
            if parsed:
                output.append(parsed)

    direct_run_start = next((payload.get(key) for key in RUN_START_KEYS if payload.get(key) not in (None, "")), None)
    if direct_run_start is None:
        nested_starts = walk_values(payload, RUN_START_KEYS)
        direct_run_start = nested_starts[0][1] if nested_starts else None
    run_started_at = parse_time(direct_run_start)
    record["run_started_at"] = iso_or_none(run_started_at)
    record["data_cut_time"] = iso_or_none(max(evidence_times)) if evidence_times else None
    record["publish_time"] = iso_or_none(max(publish_times)) if publish_times else None
    effective = max(evidence_times) if evidence_times else (max(publish_times) if publish_times else None)
    if effective is None:
        effective = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        record["time_candidates"].append({
            "role": "filesystem",
            "field": "filesystem.mtime",
            "raw": path.stat().st_mtime,
            "parsed": iso_or_none(effective),
        })
    record["effective_time"] = iso_or_none(effective)
    record["age_sec"] = max(0.0, (now - effective).total_seconds())
    return record


def discover_default_artifacts(agent_runs: Path, latest_status: Path) -> list[tuple[str, Path]]:
    latest = agent_runs / "latest"
    candidates: list[tuple[str, Path]] = [("latest_status", latest_status)]
    preferred = {
        "primary_capture": ["capture_discovery_24h.json", "candidate_capture_discovery_24h.json"],
        "p4_oos": ["capture_cross_post_freeze_oos_validation.json"],
        "phase3": ["phase3_wide_net_paper_experiment_summary.json", "phase3_goal_loop.json"],
    }
    for label, names in preferred.items():
        selected = next((latest / name for name in names if (latest / name).exists()), latest / names[0])
        candidates.append((label, selected))
    p8_paths = sorted({*latest.glob("*p8*.json"), *latest.glob("*pump*.json")})
    if p8_paths:
        candidates.append(("p8", p8_paths[-1]))
    stage_state = agent_runs / "stage_state.json"
    if stage_state.exists():
        candidates.append(("stage_state", stage_state))
    return candidates


def parse_artifact_args(values: list[str]) -> list[tuple[str, Path]]:
    parsed = []
    for value in values:
        if "=" not in value:
            raise ValueError(f"artifact must be label=path: {value}")
        label, raw_path = value.split("=", 1)
        parsed.append((label.strip(), Path(raw_path).expanduser()))
    return parsed


def build_report(
    artifacts: list[tuple[str, Path]],
    *,
    now: datetime,
    current_max_age_hours: float = 2.0,
    research_max_age_hours: float = 26.0,
) -> dict[str, Any]:
    records = [artifact_record(label, path, now) for label, path in artifacts]
    missing = [record["label"] for record in records if not record["exists"] or record["parse_error"]]
    lineage_labels = {"latest_status", "primary_capture", "stage_state"}
    run_ids = sorted({
        run_id
        for record in records
        if record["label"] in lineage_labels
        for run_id in record["run_ids"]
    })
    all_artifact_run_ids = sorted({run_id for record in records for run_id in record["run_ids"]})
    stale = []
    for record in records:
        if record["age_sec"] is None:
            continue
        threshold_hours = current_max_age_hours if record["label"] in {"latest_status", "primary_capture", "stage_state"} else research_max_age_hours
        record["max_age_hours"] = threshold_hours
        record["stale"] = record["age_sec"] > threshold_hours * 3600
        if record["stale"]:
            stale.append(record["label"])

    status_record = next((record for record in records if record["label"] == "latest_status"), None)
    capture_record = next((record for record in records if record["label"] == "primary_capture"), None)
    primary_capture_predates_run = False
    if status_record and capture_record and status_record["run_started_at"] and capture_record["effective_time"]:
        status_time = parse_time(status_record["run_started_at"])
        capture_time = parse_time(capture_record["effective_time"])
        primary_capture_predates_run = bool(status_time and capture_time and capture_time < status_time)

    if len(run_ids) > 1:
        classification = "MIXED_RUN_LINEAGE"
    elif missing:
        classification = "UNFINISHED_RUN"
    elif stale or primary_capture_predates_run:
        classification = "STALE_INPUT"
    else:
        classification = "CURRENT"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso_or_none(now),
        "classification": classification,
        "read_only": True,
        "promotion_allowed": False,
        "artifacts": records,
        "run_ids": run_ids,
        "all_artifact_run_ids": all_artifact_run_ids,
        "missing_or_unreadable": missing,
        "stale_artifacts": stale,
        "primary_capture_predates_run_start": primary_capture_predates_run,
        "lineage_consistent": len(run_ids) <= 1,
    }


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        now = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)
        status = root / "status.json"
        capture = root / "capture.json"
        status.write_text(json.dumps({
            "run_id": "r1",
            "run_started_at": "2026-07-09T23:50:00Z",
            "generated_at": "2026-07-10T00:00:00Z",
        }))
        capture.write_text(json.dumps({"run_id": "r1", "generated_at": "2026-07-09T20:00:00Z"}))
        stale = build_report([("latest_status", status), ("primary_capture", capture)], now=now)
        assert stale["classification"] == "STALE_INPUT"
        capture.write_text(json.dumps({"run_id": "r2", "generated_at": "2026-07-09T23:59:00Z"}))
        mixed = build_report([("latest_status", status), ("primary_capture", capture)], now=now)
        assert mixed["classification"] == "MIXED_RUN_LINEAGE"
        capture.write_text(json.dumps({
            "run_id": "r1",
            "data_cut_at": "2026-07-09T23:59:00Z",
            "generated_at": "2026-07-10T00:00:00Z",
        }))
        current_pair = build_report([("latest_status", status), ("primary_capture", capture)], now=now)
        assert current_pair["classification"] == "CURRENT"
        capture.write_text(json.dumps({
            "run_id": "r1",
            "data_cut_at": "2026-07-09T18:00:00Z",
            "generated_at": "2026-07-10T00:00:00Z",
        }))
        stale_cut = build_report([("latest_status", status), ("primary_capture", capture)], now=now)
        assert stale_cut["classification"] == "STALE_INPUT"
        current = build_report(
            [("latest_status", status)],
            now=now,
            current_max_age_hours=2,
        )
        assert current["classification"] == "CURRENT"
    print("SELF_TEST_PASS evidence_clock_audit")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-runs", default="/app/data/agent_runs")
    parser.add_argument("--latest-status", default="/app/data/agent_runs/latest/reviewer_verdict.json")
    parser.add_argument("--artifact", action="append", default=[], help="Explicit label=path; repeatable")
    parser.add_argument("--current-max-age-hours", type=float, default=2.0)
    parser.add_argument("--research-max-age-hours", type=float, default=26.0)
    parser.add_argument("--now")
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    now = parse_time(args.now) if args.now else datetime.now(timezone.utc)
    if now is None:
        raise SystemExit("--now is not a valid timestamp")
    artifacts = parse_artifact_args(args.artifact) if args.artifact else discover_default_artifacts(
        Path(args.agent_runs), Path(args.latest_status)
    )
    report = build_report(
        artifacts,
        now=now,
        current_max_age_hours=args.current_max_age_hours,
        research_max_age_hours=args.research_max_age_hours,
    )
    if args.out:
        atomic_write_json(args.out, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["classification"] == "CURRENT" else 2


if __name__ == "__main__":
    raise SystemExit(main())
