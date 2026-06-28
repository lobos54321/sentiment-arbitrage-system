#!/usr/bin/env python3
"""Bounded autonomous gold/silver capture discovery loop.

Discovery-only. This loop writes reviewer artifacts and never changes strategy,
entry policy, hard gates, exit gates, canary size, executor, wallet, or risk.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from generate_codex_handoff import build_handoff, write_text as write_handoff_text
from review_agent_verdict import build_verdict, write_json


SCHEMA_VERSION = "agent_capture_discovery_loop.v1"
DEFAULT_OUT_ROOT = "/app/data/agent_runs"
DEFAULT_HANDOFF_DIR = "/app/data/agent_handoffs"
DEFAULT_REGISTRY = "/app/data/hypothesis_registry.json"
REPORT_TEST_COMMANDS = (
    ("capture_self_test", ["scripts/offline_candidate_capture_discovery.py", "--self-test"]),
    ("pnl_cross_self_test", ["scripts/offline_candidate_cross_eval.py", "--self-test"]),
    ("virtual_markov_self_test", ["scripts/build_candidate_virtual_markov.py", "--self-test"]),
    ("reviewer_self_test", ["scripts/review_agent_verdict.py", "--self-test"]),
    ("handoff_self_test", ["scripts/generate_codex_handoff.py", "--self-test"]),
)


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run_id():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def log_event(event, **fields):
    payload = {
        "schema_version": "agent_capture_discovery_loop_event.v1",
        "event": event,
        "at": utc_now(),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True), flush=True)


def file_available(path):
    return bool(path) and Path(path).exists() and Path(path).is_file()


def sqlite_has_table(path, table):
    if not file_available(path):
        return False
    db = sqlite3.connect(path)
    try:
        row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)
    finally:
        db.close()


def command_result(name, args, *, timeout):
    started = time.time()
    cmd = [sys.executable, *args]
    log_event("command_start", name=name, timeout_sec=timeout, cmd=cmd)
    try:
        proc = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        result = {
            "name": name,
            "cmd": cmd,
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
        }
        log_event("command_end", name=name, ok=result["ok"], returncode=proc.returncode, duration_sec=result["duration_sec"])
        return result
    except subprocess.TimeoutExpired as exc:
        result = {
            "name": name,
            "cmd": cmd,
            "ok": False,
            "returncode": None,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "error": f"timeout_after_{timeout}s",
        }
        log_event("command_timeout", name=name, timeout_sec=timeout, duration_sec=result["duration_sec"])
        return result


def run_self_tests(timeout):
    results = [command_result(name, args, timeout=timeout) for name, args in REPORT_TEST_COMMANDS]
    return {
        "schema_version": "agent_capture_discovery_tests.v1",
        "generated_at": utc_now(),
        "passed": all(row["ok"] for row in results),
        "results": results,
    }


def blocked_capture_report(reason, paper_db, raw_db, hours, expected_candidates):
    return {
        "schema_version": "offline_candidate_capture_discovery.v1",
        "report_type": "capture_first_candidate_discovery",
        "evidence_level": "discovery_same_window",
        "evidence_role": "primary_gold_silver_capture_discovery",
        "can_promote_live": False,
        "generated_at": utc_now(),
        "db": paper_db,
        "raw_dog_source": {
            "source": "raw_signal_outcomes_db",
            "path": raw_db,
            "available": file_available(raw_db),
        },
        "hours": hours,
        "candidate_count_expected": expected_candidates,
        "report_health": {
            "promotion_allowed": False,
            "promotion_blockers": [reason],
        },
        "coverage": {
            "candidate_count_expected": expected_candidates,
            "candidate_count_observed": 0,
            "signal_count": 0,
            "observation_rows": 0,
            "expected_observation_rows": 0,
            "coverage_pct": 0,
            "bad_signal_count": 0,
        },
        "context_health": {
            "context_schema_version_counts": {},
            "quote_clean_definition_counts": {},
            "quote_sensitive_slices_evaluable": False,
            "gaps": [reason],
        },
        "raw_gold_silver_denominator": {
            "available": False,
            "rows_complete_against_summary": False,
            "event_rows": 0,
            "unique_tokens": 0,
        },
        "denominator_audit": {},
        "raw_dog_observation_join": {"join_rate": 0, "raw_dog_event_rows": 0},
        "raw_all_dog_observation_join": {"join_rate": 0, "raw_dog_event_rows": 0},
        "candidate_baseline": [],
        "context_slices": [],
        "judgment_counts": {"DISCOVERY_HIT": 0, "WATCH": 0, "TOO_SMALL": 0, "NO_SIGNAL": 0},
        "missed_dog_attribution": [],
        "watchlist_hypotheses": [],
    }


def run_report(name, args, out_path, *, timeout):
    result = command_result(name, args, timeout=timeout)
    result["out_path"] = str(out_path)
    result["out_exists"] = Path(out_path).exists()
    return result


def run_reports(run_dir, args):
    primary_hours = int(args.hours)
    capture_path = run_dir / f"candidate_capture_discovery_{primary_hours}h.json"
    pnl_path = run_dir / f"candidate_pnl_cross_{primary_hours}h.json"
    markov_paths = {
        profile: run_dir / f"candidate_virtual_markov_{profile}_{primary_hours}h.json"
        for profile in args.markov_profiles.split(",")
        if profile
    }
    diagnostics = []

    db_ready = file_available(args.paper_db) and sqlite_has_table(args.paper_db, "candidate_shadow_observations")
    raw_ready = file_available(args.raw_db) and sqlite_has_table(args.raw_db, "raw_signal_outcomes")
    if not db_ready:
        capture = blocked_capture_report("paper_db_unavailable_or_missing_candidate_shadow_observations", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "paper_db_unavailable_or_missing_candidate_shadow_observations"})
        return capture_path, None, {}, diagnostics
    if not raw_ready:
        capture = blocked_capture_report("raw_signal_outcomes_db_unavailable", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "raw_signal_outcomes_db_unavailable"})
        return capture_path, None, {}, diagnostics

    diagnostics.append(run_report(
        "capture_discovery",
        [
            "scripts/offline_candidate_capture_discovery.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--max-scan-rows", str(args.max_scan_rows),
            "--out", str(capture_path),
        ],
        capture_path,
        timeout=args.report_timeout_sec,
    ))
    diagnostics.append(run_report(
        "pnl_cross_secondary",
        [
            "scripts/offline_candidate_cross_eval.py",
            "--db", args.paper_db,
            "--hours", str(primary_hours),
            "--max-scan-rows", str(args.max_scan_rows),
            "--out", str(pnl_path),
        ],
        pnl_path,
        timeout=args.report_timeout_sec,
    ))
    successful_markov = {}
    for profile, path in markov_paths.items():
        diagnostics.append(run_report(
            f"virtual_markov_{profile}",
            [
                "scripts/build_candidate_virtual_markov.py",
                "--db", args.paper_db,
                "--hours", str(primary_hours),
                "--profile", profile,
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(path),
            ],
            path,
            timeout=args.report_timeout_sec,
        ))
        if path.exists():
            successful_markov[profile] = path
    return capture_path, pnl_path if pnl_path.exists() else None, successful_markov, diagnostics


def compact_hypothesis(metrics):
    best = metrics.get("best_slice") or {}
    return {
        "definition": metrics.get("definition") or {},
        "status": metrics.get("status"),
        "rows_found": metrics.get("rows_found"),
        "latest_best_slice": best,
    }


def update_hypothesis_registry(path, verdict, capture):
    target = Path(path)
    if target.exists():
        try:
            registry = load_json(target)
        except Exception:
            registry = {}
    else:
        registry = {}
    recent = list(registry.get("recent_runs") or [])
    recent.append({
        "generated_at": verdict.get("generated_at"),
        "classification": verdict.get("classification"),
        "blockers": verdict.get("blockers") or [],
        "capture_judgment_counts": verdict.get("capture_judgment_counts") or {},
    })
    registry = {
        "schema_version": "hypothesis_registry.v1",
        "updated_at": utc_now(),
        "phase": "discovery_mesh",
        "promotion_allowed": False,
        "hypotheses": {
            "H1_building_volume_active_microstructure": compact_hypothesis(verdict.get("H1_capture_metrics") or {}),
            "H2_shallow_pullback_matrix_evaluator": compact_hypothesis(verdict.get("H2_capture_metrics") or {}),
        },
        "watchlist_hypotheses": capture.get("watchlist_hypotheses", [])[:25],
        "recent_runs": recent[-20:],
    }
    write_json(target, registry)
    return registry


def build_run_summary(verdict, paths, diagnostics, tests):
    lines = [
        "# Gold/Silver Capture Discovery AutoLoop Summary",
        "",
        f"- generated_at: `{utc_now()}`",
        f"- phase: `discovery_mesh`",
        f"- verdict: `{verdict.get('classification')}`",
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`",
        f"- strategy_change_allowed: `{str(bool(verdict.get('strategy_change_allowed'))).lower()}`",
        "",
        "## Integrity",
        "",
        f"- candidate_count_expected: `{verdict.get('candidate_count_expected')}`",
        f"- candidate_count_observed: `{verdict.get('candidate_count_observed')}`",
        f"- observation_coverage_pct: `{verdict.get('observation_coverage_pct')}`",
        f"- raw_dog_rows_complete: `{str(bool(verdict.get('raw_dog_rows_complete'))).lower()}`",
        f"- signal_id_join_rate: `{verdict.get('signal_id_join_rate')}`",
        f"- blockers: `{json.dumps(verdict.get('blockers') or [], sort_keys=True)}`",
        "",
        "## H1/H2",
        "",
        f"- H1 status: `{(verdict.get('H1_capture_metrics') or {}).get('status')}`",
        f"- H2 status: `{(verdict.get('H2_capture_metrics') or {}).get('status')}`",
        "",
        "## Secondary Evidence",
        "",
        f"- PnL cross: `{(verdict.get('PnL_cross_secondary_status') or {}).get('status')}`",
        f"- virtual Markov: `{(verdict.get('virtual_Markov_discovery_status') or {}).get('status')}`",
        "",
        "## Artifacts",
        "",
    ]
    for name, path in sorted(paths.items()):
        lines.append(f"- {name}: `{path}`")
    lines.extend(["", "## Tests", ""])
    lines.append(f"- passed: `{str(bool(tests.get('passed'))).lower()}`")
    for result in tests.get("results", []):
        lines.append(f"- {result.get('name')}: `{str(bool(result.get('ok'))).lower()}`")
    lines.extend(["", "## Report Commands", ""])
    for result in diagnostics:
        lines.append(f"- {result.get('name')}: ok=`{str(bool(result.get('ok'))).lower()}` duration_sec=`{result.get('duration_sec')}`")
    lines.append("")
    return "\n".join(lines)


def sync_latest(run_dir, latest_dir, handoff_dir, verdict_path, summary_path, handoff_path):
    latest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(verdict_path, latest_dir / "reviewer_verdict.json")
    shutil.copy2(summary_path, latest_dir / "run_summary.md")
    for report in run_dir.glob("*.json"):
        if report.name != "reviewer_verdict.json":
            shutil.copy2(report, latest_dir / report.name)
    handoff_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(handoff_path, handoff_dir / "latest_codex_handoff.md")


def write_materialized_artifacts(
    args,
    *,
    rid,
    run_dir,
    latest_dir,
    handoff_dir,
    capture_path,
    pnl_path=None,
    markov_paths=None,
    diagnostics=None,
    tests=None,
    state="final",
):
    diagnostics = diagnostics or []
    markov_paths = markov_paths or {}
    tests = tests or {
        "schema_version": "agent_capture_discovery_tests.v1",
        "generated_at": utc_now(),
        "passed": False,
        "status": "pending",
        "results": [],
    }
    tests_path = run_dir / "tests.json"
    write_json(tests_path, tests)

    capture = load_json(capture_path)
    pnl = load_json(pnl_path) if pnl_path and Path(pnl_path).exists() else None
    markov_reports = {name: load_json(path) for name, path in markov_paths.items() if Path(path).exists()}
    verdict = build_verdict(capture, pnl, markov_reports, tests=tests if tests.get("status") != "pending" else {})
    if any(not row.get("ok") for row in diagnostics):
        verdict["blockers"] = sorted(set((verdict.get("blockers") or []) + ["report_generation_failed"]))
        verdict["classification"] = "BLOCKED_DATA"
        verdict["promotion_allowed"] = False
    if state != "final":
        verdict["blockers"] = sorted(set((verdict.get("blockers") or []) + [state]))
        verdict["classification"] = "BLOCKED_DATA"
        verdict["promotion_allowed"] = False
    verdict["loop"] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": rid,
        "run_dir": str(run_dir),
        "state": state,
        "report_diagnostics": diagnostics,
    }

    verdict_path = run_dir / "reviewer_verdict.json"
    write_json(verdict_path, verdict)
    registry = update_hypothesis_registry(args.registry, verdict, capture)

    handoff_text = build_handoff(verdict)
    handoff_path = run_dir / "codex_handoff.md"
    write_handoff_text(handoff_path, handoff_text)

    artifact_paths = {
        "run_dir": str(run_dir),
        "latest_dir": str(latest_dir),
        "reviewer_verdict": str(verdict_path),
        "run_summary": str(run_dir / "run_summary.md"),
        "codex_handoff": str(handoff_path),
        "hypothesis_registry": str(args.registry),
        "capture_report": str(capture_path),
        "pnl_cross_report": str(pnl_path) if pnl_path else None,
        "tests": str(tests_path),
    }
    for profile, path in sorted(markov_paths.items()):
        artifact_paths[f"markov_{profile}"] = str(path)
    summary = build_run_summary(verdict, artifact_paths, diagnostics, tests)
    summary_path = run_dir / "run_summary.md"
    write_text(summary_path, summary)
    sync_latest(run_dir, latest_dir, handoff_dir, verdict_path, summary_path, handoff_path)
    return verdict, registry, verdict_path, summary_path, handoff_path, tests_path


def run_once(args):
    rid = run_id()
    out_root = Path(args.out_root)
    run_dir = out_root / rid
    latest_dir = out_root / "latest"
    handoff_dir = Path(args.handoff_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_event("run_start", run_id=rid, run_dir=str(run_dir), hours=args.hours)

    capture_path = run_dir / f"candidate_capture_discovery_{int(args.hours)}h.json"
    initial_capture = blocked_capture_report(
        "agent_run_started",
        args.paper_db,
        args.raw_db,
        int(args.hours),
        args.expected_candidates,
    )
    write_json(capture_path, initial_capture)
    write_materialized_artifacts(
        args,
        rid=rid,
        run_dir=run_dir,
        latest_dir=latest_dir,
        handoff_dir=handoff_dir,
        capture_path=capture_path,
        state="agent_run_started",
    )

    tests = run_self_tests(args.test_timeout_sec)
    tests_path = run_dir / "tests.json"
    write_json(tests_path, tests)
    log_event("self_tests_done", run_id=rid, passed=tests.get("passed"))

    capture_path, pnl_path, markov_paths, diagnostics = run_reports(run_dir, args)
    verdict, registry, verdict_path, summary_path, handoff_path, _tests_path = write_materialized_artifacts(
        args,
        rid=rid,
        run_dir=run_dir,
        latest_dir=latest_dir,
        handoff_dir=handoff_dir,
        capture_path=capture_path,
        pnl_path=pnl_path,
        markov_paths=markov_paths,
        diagnostics=diagnostics,
        tests=tests,
        state="final",
    )
    log_event("run_end", run_id=rid, classification=verdict.get("classification"), blockers=verdict.get("blockers") or [])

    return {
        "run_id": rid,
        "classification": verdict.get("classification"),
        "blockers": verdict.get("blockers") or [],
        "promotion_allowed": verdict.get("promotion_allowed"),
        "latest_verdict": str(latest_dir / "reviewer_verdict.json"),
        "latest_summary": str(latest_dir / "run_summary.md"),
        "latest_handoff": str(handoff_dir / "latest_codex_handoff.md"),
        "hypothesis_registry": str(args.registry),
        "tests_passed": tests.get("passed"),
        "registry_updated_at": registry.get("updated_at"),
    }


def create_self_test_dbs(root):
    now = int(time.time())
    paper = root / "paper.db"
    raw = root / "raw.db"
    db = sqlite3.connect(paper)
    db.executescript(
        """
        CREATE TABLE candidate_shadow_observations(
          signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
          matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
        );
        CREATE TABLE candidate_shadow_virtual_trades(
          signal_id TEXT, token_ca TEXT, candidate_id TEXT, family TEXT,
          status TEXT, net_pnl_pct REAL, observed_at INTEGER
        );
        """
    )
    candidates = ["kline:active_mom20_first3", "entry_mode_registry:smart_entry_pullback_bounce"]
    for signal_id, token, is_dog in [("1", "DOG", True), ("2", "NORM", False)]:
        for candidate in candidates:
            payload = {
                "context_schema_version": "candidate-shadow-context-v2.no_signal_price_quote_inference",
                "quote_clean_definition": "source_or_executable_quote_only_no_signal_price",
                "source_quote_clean": True,
                "volume_profile": "building" if candidate.startswith("kline:") else "UNKNOWN",
                "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
                "source_component": "matrix_evaluator",
            }
            matched = 1 if is_dog else 0
            db.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (signal_id, token, now - 120, candidate, "test", matched, "self_test", now, json.dumps(payload)),
            )
            db.execute(
                "INSERT INTO candidate_shadow_virtual_trades VALUES (?,?,?,?,?,?,?)",
                (signal_id, token, candidate, "test", "VIRTUAL_CLOSED", 5.0 if is_dog else -1.0, now),
            )
    db.commit()
    db.close()
    raw_db = sqlite3.connect(raw)
    raw_db.executescript(
        """
        CREATE TABLE raw_signal_outcomes(
          signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
          observation_status TEXT, kline_covered INTEGER, baseline_confidence TEXT,
          same_source_path INTEGER, outlier_flag INTEGER, sustained_evaluable INTEGER,
          raw_primary_tier TEXT, raw_sustained_tier TEXT,
          max_sustained_peak_pct REAL, time_to_sustained_peak_sec INTEGER,
          raw_dog_entered INTEGER, raw_dog_realized INTEGER, did_enter INTEGER,
          held_to_silver INTEGER, held_to_gold INTEGER, exit_reason TEXT
        );
        """
    )
    raw_db.execute(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("1", "DOG", "DOG", now - 120, "matured", 1, "high", 1, 0, 1, "silver", "silver", 80.0, 600, 0, 0, 0, 0, 0, None),
    )
    raw_db.commit()
    raw_db.close()
    return paper, raw


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper, raw = create_self_test_dbs(root)
        args = argparse.Namespace(
            paper_db=str(paper),
            raw_db=str(raw),
            hours=1,
            expected_candidates=2,
            out_root=str(root / "agent_runs"),
            handoff_dir=str(root / "agent_handoffs"),
            registry=str(root / "hypothesis_registry.json"),
            markov_profiles="runtime,kline",
            report_timeout_sec=60,
            test_timeout_sec=60,
            max_scan_rows=2_000_000,
        )
        result = run_once(args)
        assert Path(result["latest_verdict"]).exists()
        assert Path(result["latest_summary"]).exists()
        assert Path(result["latest_handoff"]).exists()
        assert Path(result["hypothesis_registry"]).exists()
        verdict = load_json(result["latest_verdict"])
        assert verdict["candidate_count_expected"] == 2
        assert verdict["promotion_allowed"] is False
    print("SELF_TEST_PASS agent_capture_discovery_loop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    parser.add_argument("--handoff-dir", default=DEFAULT_HANDOFF_DIR)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--markov-profiles", default="runtime,kline")
    parser.add_argument("--report-timeout-sec", type=int, default=600)
    parser.add_argument("--test-timeout-sec", type=int, default=120)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
    parser.add_argument("--max-runs", type=int, default=1)
    parser.add_argument("--interval-sec", type=int, default=300)
    parser.add_argument("--initial-delay-sec", type=int, default=0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    outputs = []
    runs = max(1, args.max_runs)
    initial_delay = max(0, args.initial_delay_sec)
    if initial_delay:
        log_event("initial_delay_start", delay_sec=initial_delay)
        time.sleep(initial_delay)
        log_event("initial_delay_done", delay_sec=initial_delay)
    for index in range(runs):
        outputs.append(run_once(args))
        if index + 1 < runs:
            time.sleep(max(1, args.interval_sec))
    print(json.dumps({"schema_version": SCHEMA_VERSION, "runs": outputs}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
