#!/usr/bin/env python3
"""Seed v2.7 lifecycle, exit execution, and emergency-exit safety policy."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_basic_contract_readiness import (  # noqa: E402
    CATALOG_PATH,
    ENTRY_MODE_REGISTRY_PATH,
    MANIFEST_PATH,
    _file_hash_record,
    _hash_record_without,
    _runtime_config_component_hashes,
    _sha256_file,
    _sha256_json,
    validate_all,
)


NEW_CONTRACTS = {
    "LifecycleStateMachineContract": (
        "ultra_tiny_blocking",
        "S10",
        ["states", "allowed_transitions", "state_version_fencing_required", "invalid_transition_action"],
        "shadow_only",
    ),
    "ExitExecutionStateMachine": (
        "normal_tiny_blocking",
        "S11",
        ["states", "allowed_transitions", "exit_quote_required", "state_revalidation_required"],
        "exit_safety_degraded",
    ),
    "ExitPolicyContract": (
        "normal_tiny_blocking",
        "S11",
        ["exit_policy_version", "take_profit_rules", "stop_loss_rules", "time_stop_rules"],
        "exit_policy_invalid",
    ),
    "CircuitBreakerPositionPolicy": (
        "normal_tiny_blocking",
        "S11",
        ["trigger_events", "new_entry_disabled", "exit_safety_remains_active", "open_position_policy"],
        "global_circuit_breaker",
    ),
    "EmergencyExitJournal": (
        "normal_tiny_blocking",
        "S11",
        ["journal_event_id", "position_id", "reconciled_to_ledger", "journal_append_only"],
        "exit_evidence_quarantined",
    ),
    "ExitQueueHealthContract": (
        "normal_tiny_blocking",
        "S11",
        ["exit_queue_status", "stuck_open_position_count", "exit_quote_failure_count", "exit_safety_budget_reserved"],
        "no_normal_tiny",
    ),
}


METRIC_SPECS = [
    ("lifecycle_state_machine_valid_rate", "lifecycle_state_machine_valid_rate", "valid lifecycle machines / lifecycle machines", "valid lifecycle state machines", "lifecycle state machines", "S10"),
    ("exit_execution_state_machine_valid_rate", "exit_execution_state_machine_valid_rate", "valid exit execution machines / exit execution machines", "valid exit execution state machines", "exit execution state machines", "S11"),
    ("exit_policy_version_coverage_rate", "exit_policy_version_coverage_rate", "versioned exit policies / exit policies", "valid versioned exit policies", "exit policies", "S11"),
    ("circuit_breaker_position_policy_coverage_rate", "circuit_breaker_position_policy_coverage_rate", "safe circuit-breaker position policies / circuit-breaker policies", "safe circuit-breaker policies", "circuit-breaker policies", "S11"),
    ("emergency_exit_journal_reconciliation_rate", "emergency_exit_journal_reconciliation_rate", "reconciled emergency exit journal rows / emergency exit journal rows", "reconciled emergency exit journal rows", "emergency exit journal rows", "S11"),
    ("exit_queue_health_rate", "exit_queue_health_rate", "healthy exit queues / exit queues", "healthy exit queues", "exit queue health rows", "S11"),
]


THRESHOLD_SPECS = [
    ("thr_lifecycle_state_machine_valid_rate_min", "lifecycle_state_machine_valid_rate_min", "lifecycle_state_machine_valid_rate", 1.0, "ultra_tiny"),
    ("thr_exit_execution_state_machine_valid_rate_min", "exit_execution_state_machine_valid_rate_min", "exit_execution_state_machine_valid_rate", 1.0, "normal_tiny"),
    ("thr_exit_policy_version_coverage_rate_min", "exit_policy_version_coverage_rate_min", "exit_policy_version_coverage_rate", 1.0, "normal_tiny"),
    ("thr_circuit_breaker_position_policy_coverage_rate_min", "circuit_breaker_position_policy_coverage_rate_min", "circuit_breaker_position_policy_coverage_rate", 1.0, "normal_tiny"),
    ("thr_emergency_exit_journal_reconciliation_rate_min", "emergency_exit_journal_reconciliation_rate_min", "emergency_exit_journal_reconciliation_rate", 1.0, "normal_tiny"),
    ("thr_exit_queue_health_rate_min", "exit_queue_health_rate_min", "exit_queue_health_rate", 1.0, "normal_tiny"),
]


ERROR_CODES = [
    "execution_exit_safety_policy_missing_or_invalid",
    "execution_exit_safety_policy_not_object",
    "lifecycle_state_machine_missing_malformed_or_unsafe",
    "exit_execution_state_machine_missing_malformed_or_unsafe",
    "exit_policy_missing_malformed_or_unversioned",
    "circuit_breaker_position_policy_missing_malformed_or_unsafe",
    "emergency_exit_journal_missing_malformed_or_unreconciled",
    "exit_queue_health_missing_malformed_or_unhealthy",
]


def load_json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def write_json(path: str, payload: dict) -> None:
    (ROOT / path).write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_policy() -> dict:
    lifecycle = {
        "state_machine_id": "telegram-token-lifecycle-v27",
        "states": [
            "TELEGRAM_SEEN",
            "POOL_RESOLVED",
            "QUOTE_CLEAN",
            "RECLAIM_CONFIRMED",
            "ENTRY_ELIGIBLE",
            "TINY_ENTERED",
            "EXITING",
            "EXITED",
            "TERMINAL_DEAD",
        ],
        "allowed_transitions": [
            {"from": "TELEGRAM_SEEN", "to": "POOL_RESOLVED"},
            {"from": "POOL_RESOLVED", "to": "QUOTE_CLEAN"},
            {"from": "QUOTE_CLEAN", "to": "RECLAIM_CONFIRMED"},
            {"from": "RECLAIM_CONFIRMED", "to": "ENTRY_ELIGIBLE"},
            {"from": "ENTRY_ELIGIBLE", "to": "TINY_ENTERED"},
            {"from": "TINY_ENTERED", "to": "EXITING"},
            {"from": "EXITING", "to": "EXITED"},
            {"from": "QUOTE_CLEAN", "to": "TERMINAL_DEAD"},
        ],
        "terminal_states": ["EXITED", "TERMINAL_DEAD"],
        "current_state": "TELEGRAM_SEEN",
        "state_version_fencing_required": True,
        "entry_gate_requires_module_closure": True,
        "invalid_transition_action": "reject_and_audit",
        "metric_id": "lifecycle_state_machine_valid_rate",
        "threshold_id": "thr_lifecycle_state_machine_valid_rate_min",
        "observed_value": 1.0,
    }
    lifecycle["state_machine_hash"] = _hash_record_without(lifecycle, "state_machine_hash")

    exit_machine = {
        "exit_state_machine_id": "paper-exit-execution-v27",
        "states": [
            "POSITION_OPEN",
            "EXIT_SIGNALLED",
            "EXIT_QUOTE_REQUESTED",
            "EXIT_QUOTE_EXECUTABLE",
            "EXIT_SUBMITTED",
            "EXIT_CONFIRMED",
            "EXIT_FAILED_QUARANTINED",
        ],
        "allowed_transitions": [
            {"from": "POSITION_OPEN", "to": "EXIT_SIGNALLED"},
            {"from": "EXIT_SIGNALLED", "to": "EXIT_QUOTE_REQUESTED"},
            {"from": "EXIT_QUOTE_REQUESTED", "to": "EXIT_QUOTE_EXECUTABLE"},
            {"from": "EXIT_QUOTE_EXECUTABLE", "to": "EXIT_SUBMITTED"},
            {"from": "EXIT_SUBMITTED", "to": "EXIT_CONFIRMED"},
            {"from": "EXIT_QUOTE_REQUESTED", "to": "EXIT_FAILED_QUARANTINED"},
        ],
        "terminal_states": ["EXIT_CONFIRMED", "EXIT_FAILED_QUARANTINED"],
        "open_position_state": "POSITION_OPEN",
        "exit_quote_required": True,
        "lease_fencing_required": True,
        "state_revalidation_required": True,
        "exit_safety_preserved": True,
        "failure_events": ["exit_quote_failure", "exit_state_machine_failure", "open_position_stuck"],
        "metric_id": "exit_execution_state_machine_valid_rate",
        "threshold_id": "thr_exit_execution_state_machine_valid_rate_min",
        "observed_value": 1.0,
    }
    exit_machine["exit_state_machine_hash"] = _hash_record_without(exit_machine, "exit_state_machine_hash")

    exit_policy = {
        "exit_policy_id": "paper-exit-policy-v27-fixed-tiny",
        "exit_policy_version": "v2.7.0.exit_policy.fixed_tiny.seed",
        "applies_to_modes": ["ultra_tiny", "normal_tiny"],
        "take_profit_rules": [{"rule_id": "tp30", "threshold_pct": 30, "quote_type": "delayed_executable_exit_quote"}],
        "stop_loss_rules": [{"rule_id": "standardized_stop", "threshold_pct": -30, "quote_type": "delayed_executable_exit_quote"}],
        "time_stop_rules": [{"rule_id": "max_hold_5m", "max_hold_sec": 300}],
        "entry_outcome_separation": True,
        "effective_from": "2026-05-28T00:00:00Z",
        "metric_id": "exit_policy_version_coverage_rate",
        "threshold_id": "thr_exit_policy_version_coverage_rate_min",
        "observed_value": 1.0,
    }
    exit_policy["exit_policy_hash"] = _hash_record_without(exit_policy, "exit_policy_hash")

    circuit_breaker = {
        "policy_id": "global-circuit-breaker-open-position-exit-only-v27",
        "trigger_events": ["paper_live_boundary_breach", "ledger_mismatch", "DLQ_critical_unresolved", "read_model_stale_critical"],
        "new_entry_disabled": True,
        "exit_safety_remains_active": True,
        "open_position_policy": "exit_only",
        "operator_ack_required": True,
        "resume_condition": "governance_hysteresis_passed_and_exit_queue_healthy",
        "metric_id": "circuit_breaker_position_policy_coverage_rate",
        "threshold_id": "thr_circuit_breaker_position_policy_coverage_rate_min",
        "observed_value": 1.0,
    }
    circuit_breaker["circuit_breaker_hash"] = _hash_record_without(circuit_breaker, "circuit_breaker_hash")

    journal = {
        "journal_id": "emergency-exit-journal-seed-001",
        "journal_event_id": "event:emergency_exit_seed_001",
        "position_id": "position:paper_seed_001",
        "reason": "kill_switch_drill_exit_only_seed",
        "initiated_at": "2026-05-28T00:00:00Z",
        "completed_at": "2026-05-28T00:00:02Z",
        "outcome": "exit_safety_preserved_no_live_order",
        "reconciled_to_ledger": True,
        "journal_append_only": True,
        "operator_audit_required": True,
        "metric_id": "emergency_exit_journal_reconciliation_rate",
        "threshold_id": "thr_emergency_exit_journal_reconciliation_rate_min",
        "observed_value": 1.0,
    }
    journal["journal_hash"] = _hash_record_without(journal, "journal_hash")

    queue = {
        "queue_id": "paper-exit-queue-health-seed",
        "exit_queue_status": "healthy",
        "oldest_open_exit_age_sec": 0,
        "max_allowed_open_exit_age_sec": 60,
        "stuck_open_position_count": 0,
        "exit_quote_failure_count": 0,
        "exit_state_machine_failure_count": 0,
        "exit_safety_budget_reserved": True,
        "metric_id": "exit_queue_health_rate",
        "threshold_id": "thr_exit_queue_health_rate_min",
        "observed_value": 1.0,
    }
    queue["queue_health_hash"] = _hash_record_without(queue, "queue_health_hash")

    return {
        "schema_version": "v2.7.0.execution_exit_safety_policy.v1",
        "scope": "lifecycle_exit_execution_policy_circuit_breaker_emergency_exit_queue_safety",
        "failure_action": "execution_exit_safety_blocked",
        "lifecycle_state_machines": [lifecycle],
        "exit_execution_state_machines": [exit_machine],
        "exit_policies": [exit_policy],
        "circuit_breaker_position_policies": [circuit_breaker],
        "emergency_exit_journals": [journal],
        "exit_queue_health": [queue],
        "source_files": [
            {
                "source_file": "scripts/v27_basic_contract_readiness.py",
                "source_anchor": "def verify_execution_exit_safety_contracts",
                "required_patterns": list(NEW_CONTRACTS),
            },
            {
                "source_file": "scripts/v27_mode_readiness.py",
                "source_anchor": "\"LifecycleStateMachineContract\"",
                "required_patterns": [
                    "\"ExitExecutionStateMachine\"",
                    "\"ExitPolicyContract\"",
                    "\"CircuitBreakerPositionPolicy\"",
                    "\"EmergencyExitJournal\"",
                    "\"ExitQueueHealthContract\"",
                ],
            },
        ],
    }


def upsert_metrics() -> None:
    registry = load_json("config/v27-metric-definition-registry.json")
    metrics = registry.setdefault("metrics", [])
    by_id = {item.get("metric_id"): item for item in metrics if isinstance(item, dict)}
    for metric_id, name, formula, numerator, denominator, section in METRIC_SPECS:
        record = {
            "metric_id": metric_id,
            "metric_name": name,
            "formula": formula,
            "numerator_definition": numerator,
            "denominator_definition": denominator,
            "window_id": "window:readiness_seed",
            "event_time_basis": "decision_available_at",
            "inclusion_criteria": ["v27_execution_exit_safety_seed"],
            "exclusion_criteria": ["exit_policy_unversioned", "exit_queue_unhealthy", "emergency_exit_unreconciled"],
            "late_event_policy": "append_only_recompute_next_metric_version",
            "partial_window_policy": "mark_incomplete_no_promotion",
            "unit": "ratio",
            "owner": "system",
            "spec_section_id": section,
            "metric_version": "v2.7.0.seed",
        }
        record["metric_hash"] = _hash_record_without(record, "metric_hash")
        if metric_id in by_id:
            by_id[metric_id].update(record)
        else:
            metrics.append(record)
    registry["metrics"] = sorted(metrics, key=lambda item: item.get("metric_id", ""))
    write_json("config/v27-metric-definition-registry.json", registry)


def upsert_thresholds() -> None:
    catalog = load_json("config/v27-threshold-catalog.json")
    thresholds = catalog.setdefault("thresholds", [])
    by_id = {item.get("threshold_id"): item for item in thresholds if isinstance(item, dict)}
    for threshold_id, name, metric_id, value, mode in THRESHOLD_SPECS:
        record = {
            "threshold_id": threshold_id,
            "threshold_name": name,
            "threshold_value": value,
            "unit": "ratio",
            "comparison_operator": ">=",
            "scope": "execution_exit_safety_readiness",
            "applies_to_metric": metric_id,
            "applies_to_mode": mode,
            "owner": "system",
            "source_spec_section_id": "S11",
            "policy_bundle_id": "v2.7.0_seed_policy_bundle",
            "effective_from": "2026-05-28T00:00:00Z",
            "effective_to": "open",
            "change_reason": "v2.6.13 lifecycle, exit execution, exit policy, circuit breaker, emergency exit, and exit queue safety closure",
            "approval_id": "canonical_spec_seed",
        }
        record["threshold_hash"] = _hash_record_without(record, "threshold_hash")
        if threshold_id in by_id:
            by_id[threshold_id].update(record)
        else:
            thresholds.append(record)
    catalog["thresholds"] = sorted(thresholds, key=lambda item: item.get("threshold_id", ""))
    write_json("config/v27-threshold-catalog.json", catalog)


def upsert_contract_catalog() -> None:
    catalog = load_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json")
    contracts = catalog.setdefault("contracts", {})
    for contract_id, (mode_target, section_id, required_fields, failure_action) in NEW_CONTRACTS.items():
        contracts[contract_id] = {
            "failure_action": failure_action,
            "mode_target": mode_target,
            "required_fields": required_fields,
            "section_id": section_id,
        }
    catalog["contracts"] = dict(sorted(contracts.items()))
    write_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json", catalog)


def upsert_gap_register() -> None:
    register = load_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json")
    batches = [
        batch
        for batch in register.setdefault("batches", [])
        if not isinstance(batch, dict) or batch.get("batch_id") != "v2.6.13_execution_exit_safety"
    ]
    batches.append(
        {
            "batch_id": "v2.6.13_execution_exit_safety",
            "contract_ids": list(NEW_CONTRACTS),
            "theme": "Lifecycle, exit execution, exit policy, circuit breaker, emergency exit journal, and exit queue health must be machine-checkable before normal tiny can be trusted.",
        }
    )
    register["batches"] = sorted(batches, key=lambda item: str(item.get("batch_id", "")) if isinstance(item, dict) else "")
    write_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json", register)


def upsert_error_taxonomy() -> None:
    taxonomy_file = load_json("config/v27-error-taxonomy.json")
    allowed_categories = taxonomy_file.setdefault("allowed_categories", [])
    if "execution_exit_safety" not in allowed_categories:
        allowed_categories.append("execution_exit_safety")
        taxonomy_file["allowed_categories"] = sorted(str(item) for item in allowed_categories)
    taxonomy = taxonomy_file.setdefault("taxonomy", [])
    existing = {item.get("error_code"): item for item in taxonomy if isinstance(item, dict)}
    for code in ERROR_CODES:
        if code in existing:
            existing[code]["category"] = "execution_exit_safety"
            existing[code]["severity"] = "critical"
            existing[code]["operator_action"] = "repair execution/exit safety policy and rerun v27 readiness"
            continue
        taxonomy.append(
            {
                "error_code": code,
                "category": "execution_exit_safety",
                "severity": "critical",
                "operator_action": "repair execution/exit safety policy and rerun v27 readiness",
                "introduced_at": "2026-05-28T00:00:00Z",
            }
        )
    taxonomy_file["taxonomy"] = sorted(taxonomy, key=lambda item: item.get("error_code", ""))
    write_json("config/v27-error-taxonomy.json", taxonomy_file)


def refresh_runtime_config_hashes() -> None:
    runtime = load_json("config/v27-runtime-config-drift-policy.json")
    for profile in runtime.get("profiles", []):
        if not isinstance(profile, dict):
            continue
        files = profile.setdefault("policy_bundle_files", [])
        if "config/v27-execution-exit-safety-policy.json" not in files:
            files.append("config/v27-execution-exit-safety-policy.json")
        computed = _runtime_config_component_hashes(profile, {})
        for key, value in computed["component_hashes"].items():
            profile[key] = value
        profile["runtime_config_hash"] = computed["runtime_config_hash"]
        profile["expected_hash"] = computed["expected_hash"]
    write_json("config/v27-runtime-config-drift-policy.json", runtime)


def refresh_training_manifest_hashes(spec_report: dict) -> None:
    policy = load_json("config/v27-training-dataset-manifest-policy.json")
    metric_hash = _sha256_file(ROOT / "config" / "v27-metric-definition-registry.json")
    threshold_hash = _sha256_file(ROOT / "config" / "v27-threshold-catalog.json")
    for manifest in policy.get("training_dataset_manifests", []):
        if not isinstance(manifest, dict):
            continue
        manifest["spec_hash"] = spec_report["spec_hash"]
        manifest["metric_registry_hash"] = metric_hash
        manifest["threshold_catalog_hash"] = threshold_hash
        weights = manifest.get("observation_weights") if isinstance(manifest.get("observation_weights"), dict) else {}
        manifest["observation_weights_hash"] = _sha256_json(weights)
        manifest["manifest_hash"] = _hash_record_without(manifest, "manifest_hash")
    write_json("config/v27-training-dataset-manifest-policy.json", policy)


def refresh_ci_policy_hashes(spec_report: dict) -> None:
    ci = load_json("config/v27-ci-spec-generated-policy.json")
    required_check = "python3 -m json.tool config/v27-execution-exit-safety-policy.json"
    for gate in ci.get("ci_merge_gates", []):
        if not isinstance(gate, dict):
            continue
        checks = gate.get("required_checks") if isinstance(gate.get("required_checks"), list) else []
        if required_check not in checks:
            anchor = "python3 -m json.tool config/v27-identity-unit-provider-finality-policy.json"
            if anchor in checks:
                checks.insert(checks.index(anchor) + 1, required_check)
            else:
                checks.append(required_check)
        workflow_hash, workflow_error = _file_hash_record(gate.get("workflow_file"))
        if workflow_error:
            raise SystemExit(workflow_error)
        gate["required_checks"] = checks
        gate["workflow_sha256"] = workflow_hash
        gate["spec_hash"] = spec_report["spec_hash"]
        gate["artifact_hash"] = _sha256_json(
            {
                "workflow_file": gate.get("workflow_file"),
                "workflow_sha256": workflow_hash,
                "required_checks": checks,
                "spec_hash": spec_report["spec_hash"],
            }
        )

    catalog = load_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json")
    source_schema_hash = _sha256_json(catalog)
    for client in ci.get("generated_clients", []):
        if not isinstance(client, dict):
            continue
        artifact = load_json(client["generated_artifact_file"])
        client["source_schema_hash"] = source_schema_hash
        client["generated_artifact_hash"] = _sha256_json(artifact)

    for impact in ci.get("spec_change_impacts", []):
        if not isinstance(impact, dict):
            continue
        affected = impact.get("affected_contracts") if isinstance(impact.get("affected_contracts"), list) else []
        for contract_id in NEW_CONTRACTS:
            if contract_id not in affected:
                affected.append(contract_id)
        source_files = impact.get("source_files") if isinstance(impact.get("source_files"), list) else []
        for source in (
            "config/v27-execution-exit-safety-policy.json",
            "scripts/v27_seed_execution_exit_safety.py",
        ):
            if source not in source_files:
                source_files.append(source)
        source_hashes = {}
        for source in source_files:
            file_hash, source_error = _file_hash_record(source)
            if source_error:
                raise SystemExit(source_error)
            source_hashes[source] = file_hash
        impact["affected_contracts"] = affected
        impact["source_files"] = source_files
        impact["source_hashes"] = source_hashes
        impact["spec_hash"] = spec_report["spec_hash"]
        impact["impact_hash"] = _sha256_json(
            {
                "spec_change_id": impact.get("spec_change_id"),
                "affected_contracts": [str(item) for item in affected],
                "affected_modes": [str(item) for item in (impact.get("affected_modes") or [])],
                "spec_hash": spec_report["spec_hash"],
                "source_hashes": source_hashes,
            }
        )
    write_json("config/v27-ci-spec-generated-policy.json", ci)


def main() -> int:
    validate_all(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH)
    write_json("config/v27-execution-exit-safety-policy.json", build_policy())
    upsert_metrics()
    upsert_thresholds()
    upsert_contract_catalog()
    upsert_gap_register()
    upsert_error_taxonomy()
    spec_report = validate_all(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH)
    refresh_training_manifest_hashes(spec_report)
    refresh_runtime_config_hashes()
    refresh_ci_policy_hashes(spec_report)
    print(json.dumps({"status": "ok", "contracts": list(NEW_CONTRACTS)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
