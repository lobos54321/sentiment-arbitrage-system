#!/usr/bin/env python3
"""Build the human H1 packet for a one-time kline data repair; never approves it."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile
from typing import Any

from sqlite_evidence_utils import atomic_write_json, utc_now_iso


SCHEMA_VERSION = "phase4_h1_recovery_approval_packet.v1"
EXPECTED_PRODUCTION_TARGET = Path("/app/data/kline_cache.db")
ALLOWED_CHANGED_FILES = {
    "scripts/sqlite_evidence_utils.py",
    "scripts/kline_db_health_audit.py",
    "scripts/evidence_clock_audit.py",
    "scripts/sqlite_evidence_snapshot.py",
    "scripts/kline_db_recovery.py",
    "scripts/phase4_h1_recovery_approval_packet.py",
    "src/market-data/sqlite-file-health.js",
    "src/market-data/kline-repository.js",
    "src/optimizer/fixed-evaluator.js",
    "src/tracking/kline-collector.js",
    "scripts/paper_trade_monitor.py",
    "scripts/run-raw-path-observer.js",
    "tests/sqlite-file-health.test.mjs",
    "tests/kline-repository-health.test.mjs",
    "tests/kline-runtime-open-guards.test.mjs",
    "tests/test_phase4_evidence_recovery.py",
    "docs/agents/codex-goal-capture60-phase4-evidence-recovery.md",
}
REQUIRED_TESTS = {
    "phase4_pytest",
    "python_self_tests",
    "sqlite_header_node_test",
    "kline_repository_node_test",
    "kline_runtime_guards_node_test",
    "node_syntax_checks",
    "raw_path_regression",
}
REQUIRED_CHANGED_FILES = {
    "scripts/sqlite_evidence_utils.py",
    "scripts/kline_db_health_audit.py",
    "scripts/evidence_clock_audit.py",
    "scripts/sqlite_evidence_snapshot.py",
    "scripts/kline_db_recovery.py",
    "scripts/phase4_h1_recovery_approval_packet.py",
    "src/market-data/sqlite-file-health.js",
    "src/market-data/kline-repository.js",
    "src/optimizer/fixed-evaluator.js",
    "src/tracking/kline-collector.js",
    "scripts/paper_trade_monitor.py",
    "scripts/run-raw-path-observer.js",
    "tests/sqlite-file-health.test.mjs",
    "tests/kline-repository-health.test.mjs",
    "tests/kline-runtime-open-guards.test.mjs",
    "tests/test_phase4_evidence_recovery.py",
}


def load_json(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def build_packet(
    kline_health: dict[str, Any],
    evidence_clock: dict[str, Any],
    recovery_dry_run: dict[str, Any],
    test_report: dict[str, Any],
    diff_scope: dict[str, Any],
) -> dict[str, Any]:
    changed_files = [str(path) for path in diff_scope.get("changed_files", [])]
    forbidden_files = [path for path in changed_files if path not in ALLOWED_CHANGED_FILES]
    health_classification = kline_health.get("classification") or (kline_health.get("primary") or {}).get("classification")
    dry_run_only = recovery_dry_run.get("mutation_performed") is False and recovery_dry_run.get("mode") == "dry_run"
    test_results = test_report.get("results") if isinstance(test_report.get("results"), list) else []
    test_result_map = {
        str(row.get("name")): row
        for row in test_results
        if isinstance(row, dict) and row.get("name")
    }
    missing_tests = sorted(REQUIRED_TESTS - set(test_result_map))
    failed_tests = sorted(
        name for name, row in test_result_map.items()
        if name in REQUIRED_TESTS and int(row.get("exit_code", 1)) != 0
    )
    tests_passed = test_report.get("tests_passed") is True and not missing_tests and not failed_tests
    missing_required_changed_files = sorted(REQUIRED_CHANGED_FILES - set(changed_files))
    scope_verified = (
        diff_scope.get("scope_verified_against_base") is True
        and bool(diff_scope.get("base_commit"))
        and bool(diff_scope.get("candidate_commit"))
    )
    acceptable_health = health_classification in {"INVALID_HEADER", "MALFORMED", "MISSING"}
    target_db = recovery_dry_run.get("target_db")
    health_payload = kline_health.get("primary") or kline_health
    health_target = health_payload.get("path") if isinstance(health_payload, dict) else None
    expected_target = EXPECTED_PRODUCTION_TARGET.resolve()
    target_matches_production = bool(target_db) and Path(str(target_db)).resolve() == expected_target
    health_target_matches_recovery = (
        bool(health_target)
        and bool(target_db)
        and Path(str(health_target)).resolve() == Path(str(target_db)).resolve()
    )
    health_process_scan = kline_health.get("process_references") or {}
    dry_run_process_scan = recovery_dry_run.get("process_references") or {}
    process_visibility_complete = (
        health_process_scan.get("available") is True
        and not health_process_scan.get("errors")
        and dry_run_process_scan.get("available") is True
        and not dry_run_process_scan.get("errors")
    )
    source_audit_read_only = kline_health.get("mutation_performed") is False
    ready = (
        acceptable_health
        and dry_run_only
        and target_matches_production
        and health_target_matches_recovery
        and process_visibility_complete
        and source_audit_read_only
        and tests_passed
        and scope_verified
        and not forbidden_files
        and not missing_required_changed_files
    )
    quarantine_path = recovery_dry_run.get("proposed_quarantine_path")
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "checkpoint": "H1_ONE_TIME_KLINE_DATA_REPAIR",
        "status": "HUMAN_APPROVAL_REQUIRED",
        "ready_for_human_review": ready,
        "approval_granted": False,
        "promotion_allowed": False,
        "production_strategy_change_allowed": False,
        "inputs": {
            "kline_health_classification": health_classification,
            "evidence_clock_classification": evidence_clock.get("classification"),
            "recovery_is_dry_run": dry_run_only,
            "tests_passed": tests_passed,
            "missing_required_tests": missing_tests,
            "failed_required_tests": failed_tests,
            "changed_files": changed_files,
            "forbidden_files_changed": forbidden_files,
            "missing_required_changed_files": missing_required_changed_files,
            "scope_verified_against_base": scope_verified,
            "base_commit": diff_scope.get("base_commit"),
            "candidate_commit": diff_scope.get("candidate_commit"),
            "target_matches_production": target_matches_production,
            "health_target_matches_recovery": health_target_matches_recovery,
            "process_visibility_complete": process_visibility_complete,
            "source_audit_read_only": source_audit_read_only,
        },
        "repair_contract": {
            "target_db": target_db,
            "exact_quarantine_path": quarantine_path,
            "new_database_mode": "empty_verified_schema_then_bounded_backfill",
            "backfill_scope": "must_be_supplied_and_budgeted_by_operator_before_H1_approval",
            "worker_sequence": [
                "write maintenance_requested marker",
                "obtain acknowledgement from every kline reader/writer",
                "verify zero active file-descriptor references",
                "execute approved quarantine and verified initialization",
                "run header validation and PRAGMA quick_check",
                "remove maintenance marker",
                "restart the same observer workers only",
                "run bounded research backfill",
            ],
            "rollback": (
                f"stop kline workers; move the new {target_db} aside; atomically restore "
                f"{quarantine_path} to {target_db}; validate header and quick_check"
            ),
        },
        "authorized_if_human_approves": [
            "pause kline-reading and kline-writing observer workers",
            "quarantine the exact approved invalid database without deletion",
            "create a verified empty SQLite schema through temp and atomic rename",
            "restart the same workers",
            "run a bounded research backfill",
        ],
        "not_authorized": [
            "strategy or entry-policy changes",
            "hard-gate or exit-gate changes",
            "final_entry_contract changes",
            "A_CLASS mode changes",
            "paper/live executor enablement",
            "canary, wallet, position-size, concurrency, or risk changes",
            "promotion",
        ],
        "approval_marker_template": {
            "approval_type": "phase4_kline_recovery_h1",
            "approval_id": "HUMAN_MUST_FILL",
            "approved": False,
            "operator": "HUMAN_MUST_FILL",
            "target_db": target_db,
            "expected_source_sha256": (kline_health.get("primary") or kline_health).get("sha256"),
            "quarantine_path": quarantine_path,
            "expires_at": "HUMAN_MUST_FILL",
        },
        "maintenance_marker_template": {
            "maintenance_requested": False,
            "target_db": target_db,
            "all_kline_workers_acknowledged": False,
            "required_kline_workers": ["HUMAN_MUST_ENUMERATE"],
            "acknowledged_kline_workers": [],
        },
        "next_action": "human_review_h1_packet_do_not_execute_recovery",
    }


def render_markdown(packet: dict[str, Any]) -> str:
    inputs = packet["inputs"]
    contract = packet["repair_contract"]
    lines = [
        "# Phase 4 H1 - One-Time Kline Data Repair",
        "",
        f"- Status: `{packet['status']}`",
        f"- Ready for human review: `{str(packet['ready_for_human_review']).lower()}`",
        f"- Approval granted: `{str(packet['approval_granted']).lower()}`",
        f"- Kline health: `{inputs['kline_health_classification']}`",
        f"- Evidence clock: `{inputs['evidence_clock_classification']}`",
        f"- Tests passed: `{str(inputs['tests_passed']).lower()}`",
        f"- Target: `{contract['target_db']}`",
        f"- Quarantine: `{contract['exact_quarantine_path']}`",
        "",
        "## Scope",
        "",
        "This packet does not perform recovery. A human must review the audit, exact path, backfill budget, and worker sequence, then create a separate approval marker.",
        "",
        "## Authorized After Approval",
        "",
    ]
    lines.extend(f"- {item}" for item in packet["authorized_if_human_approves"])
    lines.extend(["", "## Not Authorized", ""])
    lines.extend(f"- {item}" for item in packet["not_authorized"])
    lines.extend(["", "## Rollback", "", contract["rollback"], ""])
    return "\n".join(lines)


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        health = {
            "classification": "INVALID_HEADER",
            "mutation_performed": False,
            "primary": {
                "classification": "INVALID_HEADER",
                "path": "/app/data/kline_cache.db",
                "sha256": "abc",
            },
            "process_references": {"available": True, "errors": [], "references": []},
        }
        clock = {"classification": "STALE_INPUT"}
        dry = {
            "mode": "dry_run",
            "mutation_performed": False,
            "target_db": "/app/data/kline_cache.db",
            "proposed_quarantine_path": "/app/data/kline_cache.db.quarantine-20260710T000000Z",
            "process_references": {"available": True, "errors": [], "references": []},
        }
        tests = {
            "tests_passed": True,
            "results": [
                {"name": name, "exit_code": 0}
                for name in sorted(REQUIRED_TESTS)
            ],
        }
        scope = {
            "changed_files": sorted(REQUIRED_CHANGED_FILES),
            "scope_verified_against_base": True,
            "base_commit": "base",
            "candidate_commit": "candidate",
        }
        packet = build_packet(health, clock, dry, tests, scope)
        assert packet["ready_for_human_review"] is True
        assert packet["approval_granted"] is False
        assert packet["promotion_allowed"] is False
        assert "HUMAN_APPROVAL_REQUIRED" in render_markdown(packet)
        atomic_write_json(root / "packet.json", packet)
        assert (root / "packet.json").exists()
    print("SELF_TEST_PASS phase4_h1_recovery_approval_packet")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kline-health")
    parser.add_argument("--evidence-clock")
    parser.add_argument("--recovery-dry-run")
    parser.add_argument("--test-report")
    parser.add_argument("--diff-scope")
    parser.add_argument("--out")
    parser.add_argument("--out-md")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    required = [args.kline_health, args.evidence_clock, args.recovery_dry_run, args.test_report, args.diff_scope, args.out]
    if any(not value for value in required):
        raise SystemExit("all input flags and --out are required")
    packet = build_packet(
        load_json(args.kline_health),
        load_json(args.evidence_clock),
        load_json(args.recovery_dry_run),
        load_json(args.test_report),
        load_json(args.diff_scope),
    )
    atomic_write_json(args.out, packet)
    if args.out_md:
        Path(args.out_md).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out_md).write_text(render_markdown(packet), encoding="utf-8")
    print(json.dumps(packet, indent=2, sort_keys=True))
    return 0 if packet["ready_for_human_review"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
