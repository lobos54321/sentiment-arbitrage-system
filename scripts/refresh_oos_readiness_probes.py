#!/usr/bin/env python3
"""Refresh short OOS readiness probes for the capture discovery loop.

Read-only helper. It only materializes matured-volume OOS probe reports and
their hypothesis validation reports. It never changes strategy, gates,
A_CLASS mode, final_entry_contract, executor settings, wallet, or risk.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path


SCHEMA_VERSION = "refresh_oos_readiness_probes.v1"
DEFAULT_PROBE_HOURS = "0.25,0.5,1"
DEFAULT_POST_FREEZE_MIN_HOURS = 0.05
DEFAULT_POST_FREEZE_SAFETY_SEC = 120


def label_for_hours(value: str) -> str:
    text = str(value).strip()
    if not text:
        return "0h"
    try:
        number = float(text)
    except Exception:
        return text.replace(".", "p")
    if number.is_integer():
        return f"{int(number)}h"
    return f"{text.replace('.', 'p')}h"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_time(value) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(dt.datetime.fromisoformat(text).timestamp())
    except Exception:
        return None


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def run_cmd(name: str, cmd: list[str], timeout_sec: int) -> dict:
    started = time.time()
    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout_sec,
    )
    return {
        "name": name,
        "cmd": cmd,
        "returncode": proc.returncode,
        "ok": proc.returncode == 0,
        "duration_sec": round(time.time() - started, 3),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


def compact_cross(path: Path) -> dict:
    report = load_json(path)
    denominator = report.get("denominator") or {}
    evaluable = denominator.get("evaluable_gold_silver") or {}
    matured = report.get("matured_volume_context") or {}
    overall = report.get("overall") or {}
    return {
        "available": bool(report),
        "path": str(path),
        "classification": overall.get("classification"),
        "next_action": overall.get("next_action"),
        "promotion_allowed": False,
        "signals_scanned": report.get("signals_scanned"),
        "evaluable_raw_gs_event_rows": evaluable.get("event_rows"),
        "matured_volume_known_rate": matured.get("known_rate"),
        "candidate_count_expected": report.get("candidate_count_expected"),
        "candidate_count_observed": report.get("candidate_count_observed"),
        "candidate_count_ok": report.get("candidate_count_ok"),
        "watch_slice_count": sum(
            1 for row in (report.get("top_slices") or [])
            if row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH"
        ),
    }


def compact_validation(path: Path) -> dict:
    report = load_json(path)
    validation = report.get("matured_volume_hypothesis_validation") or {}
    quality = validation.get("eval_window_quality") or {}
    overall = report.get("overall") or {}
    return {
        "available": bool(report),
        "path": str(path),
        "classification": overall.get("classification"),
        "next_action": overall.get("next_action"),
        "promotion_allowed": False,
        "registered_hypothesis_count": validation.get("registered_hypothesis_count"),
        "found_in_current_report_count": validation.get("found_in_current_report_count"),
        "repeated_watch_count": validation.get("repeated_watch_count"),
        "oos_repeated_watch_count": validation.get("oos_repeated_watch_count"),
        "registry_frozen_before_eval_window": validation.get("registry_frozen_before_eval_window"),
        "sufficient_for_oos_judgment": quality.get("sufficient_for_oos_judgment"),
        "blockers": quality.get("blockers") or [],
        "signals_scanned": quality.get("signals_scanned"),
        "evaluable_raw_gs_event_rows": quality.get("evaluable_raw_gs_event_rows"),
        "matured_volume_known_rate": quality.get("matured_volume_known_rate"),
        "min_oos_signals": quality.get("min_oos_signals"),
        "min_oos_raw_gs_events": quality.get("min_oos_raw_gs_events"),
        "min_oos_matured_volume_known_rate": quality.get("min_oos_matured_volume_known_rate"),
    }


def build_report(args: argparse.Namespace) -> dict:
    run_dir = Path(args.run_dir)
    registry = load_json(Path(args.registry))
    probes = [part.strip() for part in str(args.probe_hours).split(",") if part.strip()]
    registry_frozen_at = (
        registry.get("oos_hypothesis_frozen_at")
        or registry.get("hypothesis_frozen_at")
        or registry.get("updated_at")
    )
    registry_updated_ts = parse_time(registry_frozen_at)
    now_ts = int(time.time())
    post_freeze_probe = {
        "enabled": bool(args.post_freeze_probe),
        "registry_frozen_at": registry_frozen_at,
        "registry_updated_at": registry.get("updated_at"),
        "global_hypothesis_frozen_at": registry.get("hypothesis_frozen_at"),
        "oos_hypothesis_frozen_at": registry.get("oos_hypothesis_frozen_at"),
        "oos_hypothesis_set_signature_present": bool(registry.get("oos_hypothesis_set_signature")),
        "registry_updated_ts": registry_updated_ts,
        "now_ts": now_ts,
        "safety_sec": args.post_freeze_safety_sec,
        "min_hours": args.post_freeze_min_hours,
        "added": False,
        "hours": None,
        "reason": None,
    }
    if args.post_freeze_probe:
        if registry_updated_ts is None:
            post_freeze_probe["reason"] = "registry_updated_at_missing_or_unparseable"
        else:
            usable_sec = now_ts - int(registry_updated_ts) - int(args.post_freeze_safety_sec)
            usable_hours = max(0.0, float(usable_sec) / 3600.0)
            if usable_hours < float(args.post_freeze_min_hours):
                post_freeze_probe["reason"] = "post_freeze_window_too_young"
                post_freeze_probe["hours"] = round(usable_hours, 4)
            else:
                hours_text = f"{usable_hours:.4f}".rstrip("0").rstrip(".")
                if hours_text not in probes:
                    probes.append(hours_text)
                    post_freeze_probe["added"] = True
                post_freeze_probe["hours"] = float(hours_text)
                post_freeze_probe["reason"] = "post_freeze_safe_probe_added"
    commands = []
    results = []
    for hours in probes:
        label = label_for_hours(hours)
        cross_path = run_dir / f"matured_volume_capture_cross_audit_oos_probe_{label}.json"
        validation_path = run_dir / f"hypothesis_validation_audit_oos_probe_{label}.json"
        cross_cmd = [
            sys.executable,
            str(Path(__file__).resolve().parent / "matured_volume_capture_cross_audit.py"),
            "--db", args.db,
            "--raw-db", args.raw_db,
            "--kline-db", args.kline_db,
            "--hours", hours,
            "--expected-candidates", str(args.expected_candidates),
            "--max-scan-rows", str(args.max_scan_rows),
            "--out", str(cross_path),
        ]
        validation_cmd = [
            sys.executable,
            str(Path(__file__).resolve().parent / "hypothesis_validation_audit.py"),
            "--registry", args.registry,
            "--matured-volume-cross", str(cross_path),
            "--out", str(validation_path),
        ]
        cross_result = run_cmd(f"matured_volume_cross_oos_probe_{label}", cross_cmd, args.timeout_sec)
        commands.append(cross_result)
        if cross_result["ok"]:
            validation_result = run_cmd(f"hypothesis_validation_oos_probe_{label}", validation_cmd, args.timeout_sec)
            commands.append(validation_result)
        results.append({
            "probe": label,
            "hours": hours,
            "cross": compact_cross(cross_path),
            "validation": compact_validation(validation_path),
        })
    failed = [cmd for cmd in commands if not cmd["ok"]]
    requested_probe_count = len(probes)
    executed_probe_count = len(results)
    if failed:
        classification = "OOS_PROBE_REFRESH_FAILED"
        next_action = "inspect_failed_probe_command"
    elif executed_probe_count == 0 and post_freeze_probe.get("reason") == "post_freeze_window_too_young":
        classification = "OOS_PROBES_WAITING_FOR_POST_FREEZE_WINDOW"
        next_action = "wait_for_post_freeze_oos_window"
    elif executed_probe_count == 0:
        classification = "OOS_PROBES_NOT_RUN"
        next_action = "provide_probe_hours_or_wait_for_post_freeze_oos_window"
    else:
        classification = "OOS_PROBES_REFRESHED"
        next_action = "rerun_reviewer_or_wait_for_sufficient_oos_raw_gs_events"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "report_type": "oos_readiness_probe_refresh",
        "evidence_level": "discovery_readiness_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "inputs": {
            "paper_db": args.db,
            "raw_db": args.raw_db,
            "kline_db": args.kline_db,
            "registry": args.registry,
            "run_dir": str(run_dir),
            "probe_hours": probes,
        },
        "post_freeze_probe": post_freeze_probe,
        "commands": commands,
        "probes": results,
        "requested_probe_count": requested_probe_count,
        "executed_probe_count": executed_probe_count,
        "probe_count": executed_probe_count,
        "skipped_probe_count": max(0, requested_probe_count - executed_probe_count),
        "failed_command_count": len(failed),
        "classification": classification,
        "next_action": next_action,
    }
    if args.out:
        write_json(Path(args.out), summary)
    return summary


def self_test() -> None:
    import sqlite3
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        paper = root / "paper.db"
        raw = root / "raw.db"
        kline = root / "kline.db"
        registry = root / "hypothesis_registry.json"
        run_dir = root / "agent_runs" / "latest"
        run_dir.mkdir(parents=True)
        write_json(registry, {
            "schema_version": "hypothesis_registry.v2",
            "updated_at": int(time.time()) - 7200,
            "promotion_allowed": False,
            "shadow_only_matured_volume_watch": [],
        })
        for path in (paper, raw, kline):
            conn = sqlite3.connect(path)
            conn.close()
        args = argparse.Namespace(
            db=str(paper),
            raw_db=str(raw),
            kline_db=str(kline),
            registry=str(registry),
            run_dir=str(run_dir),
            probe_hours="0.25",
            expected_candidates=84,
            max_scan_rows=1000,
            timeout_sec=30,
            post_freeze_probe=True,
            post_freeze_min_hours=0.05,
            post_freeze_safety_sec=120,
            out=str(root / "summary.json"),
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["strategy_change_allowed"] is False
        assert len(report["probes"]) == 2
        assert report["executed_probe_count"] == 2
        assert report["classification"] == "OOS_PROBES_REFRESHED"
        assert report["post_freeze_probe"]["added"] is True
        assert (run_dir / "matured_volume_capture_cross_audit_oos_probe_0p25h.json").exists()
        assert (run_dir / "hypothesis_validation_audit_oos_probe_0p25h.json").exists()
        assert Path(args.out).exists()

        young_registry = root / "young_hypothesis_registry.json"
        young_run_dir = root / "agent_runs" / "young"
        young_run_dir.mkdir(parents=True)
        write_json(young_registry, {
            "schema_version": "hypothesis_registry.v2",
            "updated_at": int(time.time()),
            "promotion_allowed": False,
            "shadow_only_matured_volume_watch": [],
        })
        young_args = argparse.Namespace(
            db=str(paper),
            raw_db=str(raw),
            kline_db=str(kline),
            registry=str(young_registry),
            run_dir=str(young_run_dir),
            probe_hours="",
            expected_candidates=84,
            max_scan_rows=1000,
            timeout_sec=30,
            post_freeze_probe=True,
            post_freeze_min_hours=0.05,
            post_freeze_safety_sec=120,
            out=str(root / "young_summary.json"),
        )
        young_report = build_report(young_args)
        assert young_report["classification"] == "OOS_PROBES_WAITING_FOR_POST_FREEZE_WINDOW"
        assert young_report["next_action"] == "wait_for_post_freeze_oos_window"
        assert young_report["requested_probe_count"] == 0
        assert young_report["executed_probe_count"] == 0
        assert young_report["post_freeze_probe"]["reason"] == "post_freeze_window_too_young"
    print("SELF_TEST_PASS refresh_oos_readiness_probes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--registry", default="/app/data/hypothesis_registry.json")
    parser.add_argument("--run-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--probe-hours", default=DEFAULT_PROBE_HOURS)
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
    parser.add_argument("--timeout-sec", type=int, default=180)
    parser.add_argument("--post-freeze-probe", dest="post_freeze_probe", action="store_true", default=True)
    parser.add_argument("--no-post-freeze-probe", dest="post_freeze_probe", action="store_false")
    parser.add_argument("--post-freeze-min-hours", type=float, default=DEFAULT_POST_FREEZE_MIN_HOURS)
    parser.add_argument("--post-freeze-safety-sec", type=int, default=DEFAULT_POST_FREEZE_SAFETY_SEC)
    parser.add_argument("--out", default="/app/data/agent_runs/latest/oos_readiness_probe_refresh.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    print(json.dumps({
        "classification": report["classification"],
        "failed_command_count": report["failed_command_count"],
        "probe_count": report.get("probe_count", len(report["probes"])),
        "executed_probe_count": report.get("executed_probe_count", len(report["probes"])),
        "out": args.out,
    }, sort_keys=True))
    return 1 if report["failed_command_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
