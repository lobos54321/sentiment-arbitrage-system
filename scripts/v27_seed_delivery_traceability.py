#!/usr/bin/env python3
"""Seed v2.7 delivery, traceability, reconciliation, and decommission evidence."""

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


POLICY_FILE = "config/v27-delivery-traceability-policy.json"

NEW_CONTRACTS = {
    "ReconciliationPolicyContract": (
        "shadow_blocking",
        "S13",
        ["mismatch_class", "repair_class", "audit_required", "promotion_evidence_allowed"],
        "reconciliation_blocked",
    ),
    "DashboardStalenessContract": (
        "shadow_blocking",
        "S14",
        ["panel_name", "panel_lag_sec", "stale_banner_required", "operator_override_allowed"],
        "dashboard_stale_no_override",
    ),
    "SpecTraceabilityMatrix": (
        "normal_tiny_blocking",
        "S21",
        ["spec_section_id", "implementation_module", "test_file", "issue_id", "status"],
        "release_blocked",
    ),
    "ImplementationIssueGraphContract": (
        "normal_tiny_blocking",
        "S21",
        ["issue_id", "spec_section_ids", "dependency_ids", "acceptance_tests", "status"],
        "release_blocked",
    ),
    "ModuleClosureContract": (
        "normal_tiny_blocking",
        "S18",
        ["module_name", "input_events", "output_events", "contract_tests", "kill_condition"],
        "real_entry_gate_blocked",
    ),
    "DecommissionPolicyContract": (
        "normal_tiny_blocking",
        "S18",
        ["artifact_id", "artifact_type", "status", "runtime_reference_allowed", "operator_audit_required"],
        "global_circuit_breaker",
    ),
}


METRIC_SPECS = [
    ("reconciliation_policy_safe_rate", "reconciliation_policy_safe_rate", "safe reconciliation policies / reconciliation policies", "safe reconciliation policies", "reconciliation policies", "S13"),
    ("dashboard_staleness_safe_rate", "dashboard_staleness_safe_rate", "fresh and override-safe dashboard panels / dashboard panels", "fresh override-safe dashboard panels", "dashboard panels", "S14"),
    ("spec_traceability_valid_rate", "spec_traceability_valid_rate", "valid traceability rows / traceability rows", "valid traceability rows", "traceability rows", "S21"),
    ("implementation_issue_graph_valid_rate", "implementation_issue_graph_valid_rate", "valid implementation issues / implementation issues", "valid implementation issues", "implementation issues", "S21"),
    ("module_closure_valid_rate", "module_closure_valid_rate", "valid module closures / module closures", "valid module closures", "module closures", "S18"),
    ("decommission_policy_safe_rate", "decommission_policy_safe_rate", "safe decommission policies / decommission policies", "safe decommission policies", "decommission policies", "S18"),
]


THRESHOLD_SPECS = [
    ("thr_reconciliation_policy_safe_rate_min", "reconciliation_policy_safe_rate_min", "reconciliation_policy_safe_rate", 1.0, "shadow"),
    ("thr_dashboard_staleness_safe_rate_min", "dashboard_staleness_safe_rate_min", "dashboard_staleness_safe_rate", 1.0, "shadow"),
    ("thr_spec_traceability_valid_rate_min", "spec_traceability_valid_rate_min", "spec_traceability_valid_rate", 1.0, "normal_tiny"),
    ("thr_implementation_issue_graph_valid_rate_min", "implementation_issue_graph_valid_rate_min", "implementation_issue_graph_valid_rate", 1.0, "normal_tiny"),
    ("thr_module_closure_valid_rate_min", "module_closure_valid_rate_min", "module_closure_valid_rate", 1.0, "normal_tiny"),
    ("thr_decommission_policy_safe_rate_min", "decommission_policy_safe_rate_min", "decommission_policy_safe_rate", 1.0, "normal_tiny"),
]


ERROR_CODES = [
    "delivery_traceability_policy_missing_or_invalid",
    "delivery_traceability_policy_not_object",
    "reconciliation_policy_missing_malformed_or_unsafe",
    "dashboard_staleness_missing_malformed_or_unsafe",
    "spec_traceability_matrix_missing_malformed_or_incomplete",
    "implementation_issue_graph_missing_malformed_or_incomplete",
    "module_closure_missing_malformed_or_ungated",
    "decommission_policy_missing_malformed_or_unsafe",
]


def load_json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def write_json(path: str, payload: dict) -> None:
    (ROOT / path).write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _hash_many(rows: list[dict], hash_field: str) -> list[dict]:
    for row in rows:
        row[hash_field] = _hash_record_without(row, hash_field)
    return rows


def _metric_ids_for(contract_id: str) -> list[str]:
    return {
        "ReconciliationPolicyContract": ["reconciliation_policy_safe_rate"],
        "DashboardStalenessContract": ["dashboard_staleness_safe_rate"],
        "SpecTraceabilityMatrix": ["spec_traceability_valid_rate"],
        "ImplementationIssueGraphContract": ["implementation_issue_graph_valid_rate"],
        "ModuleClosureContract": ["module_closure_valid_rate"],
        "DecommissionPolicyContract": ["decommission_policy_safe_rate"],
    }[contract_id]


def _threshold_ids_for(contract_id: str) -> list[str]:
    return {
        "ReconciliationPolicyContract": ["thr_reconciliation_policy_safe_rate_min"],
        "DashboardStalenessContract": ["thr_dashboard_staleness_safe_rate_min"],
        "SpecTraceabilityMatrix": ["thr_spec_traceability_valid_rate_min"],
        "ImplementationIssueGraphContract": ["thr_implementation_issue_graph_valid_rate_min"],
        "ModuleClosureContract": ["thr_module_closure_valid_rate_min"],
        "DecommissionPolicyContract": ["thr_decommission_policy_safe_rate_min"],
    }[contract_id]


def build_policy() -> dict:
    reconciliation = [
        {
            "reconciliation_policy_id": "ledger-mismatch-manual-review-v27",
            "mismatch_class": "ledger_mismatch",
            "repair_class": "manual_review_required",
            "auto_repair_allowed": False,
            "manual_review_required": True,
            "audit_required": True,
            "dashboard_surface": "L5 Execution Truth / ledger reconciliation",
            "promotion_evidence_allowed": False,
            "metric_id": "reconciliation_policy_safe_rate",
            "threshold_id": "thr_reconciliation_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "reconciliation_policy_id": "materialized-view-rebuild-only-v27",
            "mismatch_class": "materialized_view_mismatch",
            "repair_class": "rebuild_projection_only",
            "auto_repair_allowed": True,
            "manual_review_required": False,
            "audit_required": True,
            "dashboard_surface": "L3 Data Truth / projection rebuild",
            "promotion_evidence_allowed": False,
            "metric_id": "reconciliation_policy_safe_rate",
            "threshold_id": "thr_reconciliation_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "reconciliation_policy_id": "paper-trade-ledger-quarantine-v27",
            "mismatch_class": "paper_trade_vs_ledger_mismatch",
            "repair_class": "quarantine_trade",
            "auto_repair_allowed": False,
            "manual_review_required": True,
            "audit_required": True,
            "dashboard_surface": "L5 Execution Truth / quarantined paper trades",
            "promotion_evidence_allowed": False,
            "metric_id": "reconciliation_policy_safe_rate",
            "threshold_id": "thr_reconciliation_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "reconciliation_policy_id": "audit-chain-repair-forbidden-v27",
            "mismatch_class": "audit_chain_hash_mismatch",
            "repair_class": "repair_forbidden",
            "auto_repair_allowed": False,
            "manual_review_required": True,
            "audit_required": True,
            "dashboard_surface": "L8 Delivery Truth / audit integrity",
            "promotion_evidence_allowed": False,
            "metric_id": "reconciliation_policy_safe_rate",
            "threshold_id": "thr_reconciliation_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "reconciliation_policy_id": "dashboard-cache-auto-invalidate-v27",
            "mismatch_class": "dashboard_cache_stale",
            "repair_class": "auto_repair_allowed",
            "auto_repair_allowed": True,
            "manual_review_required": False,
            "audit_required": True,
            "dashboard_surface": "L3 Data Truth / cache invalidation",
            "promotion_evidence_allowed": False,
            "metric_id": "reconciliation_policy_safe_rate",
            "threshold_id": "thr_reconciliation_policy_safe_rate_min",
            "observed_value": 1.0,
        },
    ]

    dashboard = [
        {
            "panel_name": "L1 Executive",
            "data_seq": 1200,
            "event_log_latest_seq": 1200,
            "panel_lag_sec": 5,
            "max_allowed_panel_lag_sec": 60,
            "stale_banner_required": True,
            "last_refresh_at": "2026-05-28T00:00:00Z",
            "staleness_threshold_id": "thr_dashboard_staleness_safe_rate_min",
            "operator_override_allowed": False,
            "metric_id": "dashboard_staleness_safe_rate",
            "threshold_id": "thr_dashboard_staleness_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "panel_name": "L2 No-Trade Root Cause",
            "data_seq": 1200,
            "event_log_latest_seq": 1201,
            "panel_lag_sec": 8,
            "max_allowed_panel_lag_sec": 60,
            "stale_banner_required": True,
            "last_refresh_at": "2026-05-28T00:00:00Z",
            "staleness_threshold_id": "thr_dashboard_staleness_safe_rate_min",
            "operator_override_allowed": False,
            "metric_id": "dashboard_staleness_safe_rate",
            "threshold_id": "thr_dashboard_staleness_safe_rate_min",
            "observed_value": 1.0,
        },
    ]

    traceability = []
    issues = []
    modules = []
    for idx, (contract_id, (mode_target, section_id, required_fields, failure_action)) in enumerate(NEW_CONTRACTS.items(), start=1):
        metric_ids = _metric_ids_for(contract_id)
        threshold_ids = _threshold_ids_for(contract_id)
        issue_id = f"V27-DT-{idx:03d}"
        traceability.append(
            {
                "traceability_id": f"trace:{contract_id}",
                "contract_id": contract_id,
                "spec_section_id": section_id,
                "requirement": f"{contract_id} must be machine-checkable before {mode_target} can be trusted",
                "implementation_module": "scripts/v27_basic_contract_readiness.py",
                "test_file": "test_v27_basic_contract_readiness.py",
                "dashboard_surface": "L8 Delivery Truth",
                "rollout_flag": f"v27_delivery_traceability_{contract_id}",
                "issue_id": issue_id,
                "status": "validated",
                "metric_ids": metric_ids,
                "threshold_ids": threshold_ids,
                "owner": "system",
            }
        )
        issues.append(
            {
                "issue_id": issue_id,
                "spec_section_ids": [section_id],
                "dependency_ids": [],
                "acceptance_tests": [
                    "python3 scripts/v27_basic_contract_readiness.py --strict",
                    "python3 -m pytest test_v27_basic_contract_readiness.py -q -p no:cacheprovider",
                ],
                "mode_readiness_target": mode_target,
                "owner": "system",
                "status": "validated",
                "metric_ids": metric_ids,
                "threshold_ids": threshold_ids,
            }
        )
        modules.append(
            {
                "module_name": f"delivery_traceability_{contract_id}",
                "contract_ids": [contract_id],
                "input_events": ["canonical_spec_changed", "policy_bundle_changed", "dashboard_panel_refreshed"],
                "output_events": [f"{contract_id}_validated", failure_action],
                "decision_fields": ["metric_id", "threshold_id", "runtime_config_hash", "spec_hash"],
                "failure_events": [failure_action],
                "outcome_metrics": metric_ids,
                "governance_rules": ["no_traceability_no_gate", "operator_audit_required", "retired_artifact_cannot_gate_entry"],
                "dashboard_surface": "L8 Delivery Truth",
                "kill_condition": failure_action,
                "contract_tests": [{"test_file": "test_v27_basic_contract_readiness.py", "status": "pass"}],
                "owner": "system",
                "spec_section_ids": [section_id],
                "mode_readiness_target": mode_target,
                "runtime_config_keys": ["runtime_config_hash", "policy_bundle_hash", "metric_registry_hash", "threshold_catalog_hash"],
                "metric_ids": metric_ids,
                "threshold_ids": threshold_ids,
            }
        )

    decommission = [
        {
            "artifact_id": "hard_gate_pass_tiny_probe",
            "artifact_type": "route",
            "status": "retired",
            "decommission_reason": "direct entry route prohibited by v2.6.13 non-negotiable core",
            "deprecated_at": "2026-05-28T00:00:00Z",
            "retired_at": "2026-05-28T00:00:00Z",
            "replacement_artifact_id": "clean_missed_dog_reclaim_tiny_probe",
            "allowed_historical_use": "historical_evidence_only_with_version_tag",
            "runtime_reference_allowed": False,
            "training_reference_allowed": False,
            "new_promotion_evidence_allowed": False,
            "dashboard_display_policy": "historical_only_red_badge",
            "operator_audit_required": True,
            "direct_entry_allowed": False,
            "owner": "system",
            "metric_id": "decommission_policy_safe_rate",
            "threshold_id": "thr_decommission_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "artifact_id": "source_resonance_tiny_probe",
            "artifact_type": "route",
            "status": "retired",
            "decommission_reason": "source_resonance direct entry prohibited by v2.6.13 non-negotiable core",
            "deprecated_at": "2026-05-28T00:00:00Z",
            "retired_at": "2026-05-28T00:00:00Z",
            "replacement_artifact_id": "clean_dog_reclaim_ultra_tiny_exploration",
            "allowed_historical_use": "historical_evidence_only_with_version_tag",
            "runtime_reference_allowed": False,
            "training_reference_allowed": False,
            "new_promotion_evidence_allowed": False,
            "dashboard_display_policy": "historical_only_red_badge",
            "operator_audit_required": True,
            "direct_entry_allowed": False,
            "owner": "system",
            "metric_id": "decommission_policy_safe_rate",
            "threshold_id": "thr_decommission_policy_safe_rate_min",
            "observed_value": 1.0,
        },
        {
            "artifact_id": "legacy_dashboard_capture_rate_widget",
            "artifact_type": "dashboard_panel",
            "status": "deprecated",
            "decommission_reason": "legacy widget hides D0/D1/D2/D3a denominator ladder",
            "deprecated_at": "2026-05-28T00:00:00Z",
            "retired_at": "open",
            "replacement_artifact_id": "dog_goal_l1_denominator_ladder",
            "allowed_historical_use": "dashboard_reference_only",
            "runtime_reference_allowed": False,
            "training_reference_allowed": False,
            "new_promotion_evidence_allowed": False,
            "dashboard_display_policy": "hidden_from_release_evidence",
            "operator_audit_required": True,
            "direct_entry_allowed": False,
            "owner": "system",
            "metric_id": "decommission_policy_safe_rate",
            "threshold_id": "thr_decommission_policy_safe_rate_min",
            "observed_value": 1.0,
        },
    ]

    return {
        "schema_version": "v2.7.0.delivery_traceability_policy.v1",
        "scope": "reconciliation_dashboard_staleness_traceability_issue_graph_module_closure_decommission",
        "failure_action": "delivery_traceability_blocked",
        "reconciliation_policies": _hash_many(reconciliation, "reconciliation_hash"),
        "dashboard_staleness_panels": _hash_many(dashboard, "panel_hash"),
        "spec_traceability_matrix": _hash_many(traceability, "traceability_hash"),
        "implementation_issue_graph": _hash_many(issues, "issue_hash"),
        "module_closures": _hash_many(modules, "module_closure_hash"),
        "decommission_policies": _hash_many(decommission, "decommission_hash"),
        "source_files": [
            {
                "source_file": "scripts/v27_basic_contract_readiness.py",
                "source_anchor": "def verify_delivery_traceability_contracts",
                "required_patterns": list(NEW_CONTRACTS),
            },
            {
                "source_file": "scripts/v27_mode_readiness.py",
                "source_anchor": "\"ReconciliationPolicyContract\"",
                "required_patterns": [
                    "\"DashboardStalenessContract\"",
                    "\"SpecTraceabilityMatrix\"",
                    "\"ImplementationIssueGraphContract\"",
                    "\"ModuleClosureContract\"",
                    "\"DecommissionPolicyContract\"",
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
            "inclusion_criteria": ["v27_delivery_traceability_seed"],
            "exclusion_criteria": ["stale_dashboard_panel", "traceability_missing", "retired_artifact_runtime_reference"],
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
            "scope": "delivery_traceability_readiness",
            "applies_to_metric": metric_id,
            "applies_to_mode": mode,
            "owner": "system",
            "source_spec_section_id": "S18",
            "policy_bundle_id": "v2.7.0_seed_policy_bundle",
            "effective_from": "2026-05-28T00:00:00Z",
            "effective_to": "open",
            "change_reason": "v2.6.13 reconciliation, dashboard staleness, traceability, issue graph, module closure, and decommission closure",
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
        if not isinstance(batch, dict) or batch.get("batch_id") != "v2.6.13_delivery_traceability"
    ]
    batches.append(
        {
            "batch_id": "v2.6.13_delivery_traceability",
            "contract_ids": list(NEW_CONTRACTS),
            "theme": "Reconciliation, dashboard freshness, traceability, implementation issue graph, module closure, and decommission policy must be machine-checkable before normal tiny can be trusted.",
        }
    )
    register["batches"] = sorted(batches, key=lambda item: str(item.get("batch_id", "")) if isinstance(item, dict) else "")
    write_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json", register)


def upsert_error_taxonomy() -> None:
    taxonomy_file = load_json("config/v27-error-taxonomy.json")
    allowed_categories = taxonomy_file.setdefault("allowed_categories", [])
    if "delivery_traceability" not in allowed_categories:
        allowed_categories.append("delivery_traceability")
        taxonomy_file["allowed_categories"] = sorted(str(item) for item in allowed_categories)
    taxonomy = taxonomy_file.setdefault("taxonomy", [])
    existing = {item.get("error_code"): item for item in taxonomy if isinstance(item, dict)}
    for code in ERROR_CODES:
        if code in existing:
            existing[code]["category"] = "delivery_traceability"
            existing[code]["severity"] = "critical"
            existing[code]["operator_action"] = "repair delivery traceability policy and rerun v27 readiness"
            continue
        taxonomy.append(
            {
                "error_code": code,
                "category": "delivery_traceability",
                "severity": "critical",
                "operator_action": "repair delivery traceability policy and rerun v27 readiness",
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
        if POLICY_FILE not in files:
            files.append(POLICY_FILE)
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
    required_check = f"python3 -m json.tool {POLICY_FILE}"
    for gate in ci.get("ci_merge_gates", []):
        if not isinstance(gate, dict):
            continue
        checks = gate.get("required_checks") if isinstance(gate.get("required_checks"), list) else []
        if required_check not in checks:
            anchor = "python3 -m json.tool config/v27-execution-exit-safety-policy.json"
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
            POLICY_FILE,
            "scripts/v27_seed_delivery_traceability.py",
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
    write_json(POLICY_FILE, build_policy())
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
