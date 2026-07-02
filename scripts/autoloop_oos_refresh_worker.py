#!/usr/bin/env python3
"""Read-only AutoLoop OOS refresh worker.

This worker keeps post-freeze OOS artifacts current without running the full
capture discovery loop. It only executes the staged ``oos`` and ``finalize``
steps against the latest agent run directory.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "autoloop_oos_refresh_worker.v1"
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def status_payload(args: argparse.Namespace, **fields) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "worker_type": "read_only_autoloop_oos_refresh",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "run_dir": str(latest_run_dir(args)),
        "interval_sec": int(args.interval_sec),
        **fields,
    }


def latest_run_dir(args: argparse.Namespace) -> Path:
    return Path(args.run_dir) if args.run_dir else Path(args.out_root) / "latest"


def latest_artifact_health(run_dir: Path) -> dict:
    required = [
        "reviewer_verdict.json",
        "capture_cross_oos_freeze_registry.json",
        "pass_allow_60_oos_freeze_registry.json",
    ]
    artifacts = {}
    missing = []
    for name in required:
        path = run_dir / name
        artifacts[name] = path.exists()
        if not path.exists():
            missing.append(name)
    return {
        "run_dir_exists": run_dir.exists(),
        "required_artifacts": artifacts,
        "missing_required_artifacts": missing,
        "ready": run_dir.exists() and not missing,
    }


def build_stage_command(args: argparse.Namespace) -> list[str]:
    run_dir = latest_run_dir(args)
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "agent_autoloop_stage_runner.py"),
        "--stage",
        "oos,finalize",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_dir.name,
        "--paper-db",
        str(args.paper_db),
        "--raw-db",
        str(args.raw_db),
        "--kline-db",
        str(args.kline_db),
        "--data-dir",
        str(args.data_dir),
        "--hours",
        str(args.hours),
        "--capture-hours",
        str(args.capture_hours),
        "--expected-candidates",
        str(args.expected_candidates),
        "--out-root",
        str(args.out_root),
        "--handoff-dir",
        str(args.handoff_dir),
        "--registry",
        str(args.registry),
        "--report-timeout-sec",
        str(args.report_timeout_sec),
        "--test-timeout-sec",
        str(args.test_timeout_sec),
        "--max-scan-rows",
        str(args.max_scan_rows),
    ]
    if args.oos_probe_hours:
        cmd.extend(["--oos-probe-hours", str(args.oos_probe_hours)])
    if int(args.quote_fix_deploy_ts or 0) > 0:
        cmd.extend(["--quote-fix-deploy-ts", str(args.quote_fix_deploy_ts)])
    return cmd


def acquire_lock(lock_file: Path) -> int | None:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(str(lock_file), flags)
    except FileExistsError:
        return None
    os.write(fd, f"{os.getpid()} {utc_now()}\n".encode("utf-8"))
    return fd


def release_lock(lock_file: Path, fd: int | None) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
    try:
        lock_file.unlink()
    except FileNotFoundError:
        pass


def run_refresh_once(args: argparse.Namespace) -> dict:
    run_dir = latest_run_dir(args)
    health = latest_artifact_health(run_dir)
    if not health["ready"]:
        payload = status_payload(
            args,
            running=False,
            status="waiting_for_latest_autoloop_artifacts",
            latest_artifact_health=health,
            next_action="wait_for_initial_autoloop_latest_artifacts",
        )
        write_json(Path(args.status_out), payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return payload

    lock_fd = acquire_lock(Path(args.lock_file))
    if lock_fd is None:
        payload = status_payload(
            args,
            running=False,
            status="skipped_lock_held",
            latest_artifact_health=health,
            lock_file=str(args.lock_file),
        )
        write_json(Path(args.status_out), payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return payload

    cmd = build_stage_command(args)
    started_at = utc_now()
    write_json(
        Path(args.status_out),
        status_payload(
            args,
            running=True,
            status="running",
            started_at=started_at,
            command=cmd,
            latest_artifact_health=health,
        ),
    )
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            text=True,
            capture_output=True,
            timeout=int(args.run_timeout_sec),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        stdout_tail = (proc.stdout or "")[-8000:]
        stderr_tail = (proc.stderr or "")[-8000:]
        verdict = read_json(run_dir / "reviewer_verdict.json")
        capture_oos = read_json(run_dir / "capture_cross_post_freeze_oos_validation.json")
        pass_allow_oos = read_json(run_dir / "pass_allow_60_post_freeze_oos_validation.json")
        payload = status_payload(
            args,
            running=False,
            status="completed" if proc.returncode == 0 else "failed",
            started_at=started_at,
            finished_at=utc_now(),
            exit_code=proc.returncode,
            command=cmd,
            latest_artifact_health=latest_artifact_health(run_dir),
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            verdict_classification=verdict.get("classification"),
            verdict_next_action=verdict.get("next_action"),
            capture_cross_post_freeze_oos_classification=capture_oos.get("classification"),
            capture_cross_post_freeze_raw_gold_silver_event_rows=capture_oos.get("raw_gold_silver_event_rows"),
            capture_cross_post_freeze_validated_definition_count=capture_oos.get("validated_definition_count"),
            pass_allow_post_freeze_oos_classification=pass_allow_oos.get("classification"),
            pass_allow_post_freeze_raw_gold_silver_event_rows=pass_allow_oos.get("raw_gold_silver_event_rows"),
        )
        write_json(Path(args.status_out), payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return payload
    except subprocess.TimeoutExpired as error:
        payload = status_payload(
            args,
            running=False,
            status="timeout",
            started_at=started_at,
            finished_at=utc_now(),
            timeout_sec=int(args.run_timeout_sec),
            command=cmd,
            stdout_tail=(error.stdout or "")[-8000:] if isinstance(error.stdout, str) else "",
            stderr_tail=(error.stderr or "")[-8000:] if isinstance(error.stderr, str) else "",
            next_action="retry_next_interval",
        )
        write_json(Path(args.status_out), payload)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return payload
    finally:
        release_lock(Path(args.lock_file), lock_fd)


def sleep_countdown(seconds: int) -> None:
    if seconds > 0:
        time.sleep(seconds)


def self_test() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        run_dir = root / "agent_runs" / "latest"
        args = argparse.Namespace(
            paper_db=root / "paper.db",
            raw_db=root / "raw.db",
            kline_db=root / "kline.db",
            data_dir=root,
            hours=24,
            capture_hours="24",
            expected_candidates=84,
            out_root=root / "agent_runs",
            handoff_dir=root / "agent_handoffs",
            registry=root / "hypothesis_registry.json",
            report_timeout_sec=60,
            test_timeout_sec=60,
            max_scan_rows=10000,
            oos_probe_hours="0.25,0.5,1",
            quote_fix_deploy_ts=0,
            run_dir=run_dir,
            interval_sec=1,
            initial_delay_sec=0,
            run_timeout_sec=60,
            max_runs=1,
            status_out=root / "status.json",
            lock_file=root / "worker.lock",
            once=True,
        )
        waiting = run_refresh_once(args)
        assert waiting["status"] == "waiting_for_latest_autoloop_artifacts"
        run_dir.mkdir(parents=True)
        for name in (
            "reviewer_verdict.json",
            "capture_cross_oos_freeze_registry.json",
            "pass_allow_60_oos_freeze_registry.json",
        ):
            write_json(run_dir / name, {"ok": True})
        cmd = build_stage_command(args)
        assert "oos,finalize" in cmd
        assert str(run_dir) in cmd
        fd = acquire_lock(root / "lock.test")
        assert fd is not None
        assert acquire_lock(root / "lock.test") is None
        release_lock(root / "lock.test", fd)
    print("SELF_TEST_PASS autoloop_oos_refresh_worker")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--capture-hours", default="24")
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--out-root", default="/app/data/agent_runs")
    parser.add_argument("--handoff-dir", default="/app/data/agent_handoffs")
    parser.add_argument("--registry", default="/app/data/hypothesis_registry.json")
    parser.add_argument("--report-timeout-sec", type=int, default=300)
    parser.add_argument("--test-timeout-sec", type=int, default=120)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
    parser.add_argument("--oos-probe-hours", default="0.25,0.5,1")
    parser.add_argument("--quote-fix-deploy-ts", type=int, default=0)
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--interval-sec", type=int, default=900)
    parser.add_argument("--initial-delay-sec", type=int, default=120)
    parser.add_argument("--run-timeout-sec", type=int, default=900)
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--status-out", default="/app/data/autoloop-oos-refresh-status.json")
    parser.add_argument("--lock-file", default="/tmp/autoloop-oos-refresh.lock")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    if not args.once:
        sleep_countdown(int(args.initial_delay_sec))
    run_count = 0
    while True:
        run_count += 1
        run_refresh_once(args)
        if args.once or (int(args.max_runs) > 0 and run_count >= int(args.max_runs)):
            return
        sleep_countdown(int(args.interval_sec))


if __name__ == "__main__":
    main()
