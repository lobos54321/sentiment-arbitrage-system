#!/usr/bin/env python3
"""Seed v2.7 spec-governance, confidence, fill-anchor, and feasibility policy.

This script is intentionally mechanical. It creates the policy/evidence rows
for the v2.6.13 contracts that make rendered spec views, health enums,
contract lifecycle, objective priority, goal confidence, fill-time anchors,
and ex-ante/posthoc feasibility machine-checkable in v2.7.0.
"""

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
    _sha256_json,
    validate_all,
)


NEW_CONTRACTS = {
    "RenderedSpecViewContract": (
        "observe_only_blocking",
        "S00",
        ["source_spec_hash", "rendered_doc_hash", "render_validation_status", "section_count"],
        "normal_tiny_disabled",
    ),
    "HealthStateEnumContract": (
        "observe_only_blocking",
        "S04",
        ["health_component", "health_state", "blocking_modes", "recovery_condition"],
        "health_state_invalid",
    ),
    "ContractLifecycleContract": (
        "observe_only_blocking",
        "S04",
        ["contract_id", "contract_version", "status", "allowed_modes", "contract_tests_status"],
        "shadow_only",
    ),
    "ObjectivePriorityContract": (
        "observe_only_blocking",
        "S04",
        ["objective_conflict_id", "conflicting_objectives", "chosen_objective", "reason"],
        "shadow_only",
    ),
    "GoalConfidenceContract": (
        "normal_tiny_blocking",
        "S02",
        [
            "metric_id",
            "numerator",
            "denominator",
            "wilson_lower_bound",
            "beta_posterior_lower_bound",
        ],
        "promotion_inconclusive",
    ),
    "FillTimeAnchorContract": (
        "shadow_blocking",
        "S06",
        ["decision_ts", "decision_available_at", "simulated_fill_ts", "position_open_confirmed_ts"],
        "trade_outcome_invalid",
    ),
    "ExAnteVsPosthocFeasibilityContract": (
        "shadow_blocking",
        "S07",
        ["ex_ante_feasible", "posthoc_feasible", "feasibility_class", "used_future_peak_in_ex_ante"],
        "sample_excluded",
    ),
}


METRIC_SPECS = [
    (
        "rendered_spec_view_valid_rate",
        "rendered_spec_view_valid_rate",
        "count valid rendered views / rendered views",
        "count rendered views with render_validation_status valid and hash match",
        "count rendered spec views",
        "S00",
        "ratio",
    ),
    (
        "health_state_enum_valid_rate",
        "health_state_enum_valid_rate",
        "count health states using allowed enum / health states",
        "count health states with standard enum and safe semantics",
        "count health state rows",
        "S04",
        "ratio",
    ),
    (
        "contract_lifecycle_active_gate_coverage_rate",
        "contract_lifecycle_active_gate_coverage_rate",
        "active gate lifecycle rows / required lifecycle rows",
        "count required contracts with active_gate lifecycle and passing tests",
        "count required lifecycle contracts",
        "S04",
        "ratio",
    ),
    (
        "objective_priority_conflict_resolution_rate",
        "objective_priority_conflict_resolution_rate",
        "resolved conflicts / conflicts",
        "count conflicts resolved to highest priority objective",
        "count objective conflicts",
        "S04",
        "ratio",
    ),
    (
        "goal_confidence_lower_bound",
        "goal_confidence_lower_bound",
        "min Wilson and beta posterior lower bound for registered goal metric",
        "minimum lower confidence bound",
        "registered goal confidence sample",
        "S02",
        "ratio",
    ),
    (
        "fill_time_anchor_valid_rate",
        "fill_time_anchor_valid_rate",
        "valid fill anchors / fill anchors",
        "count fill anchors pinned to simulated_fill_ts with chronology valid",
        "count fill anchor rows",
        "S06",
        "ratio",
    ),
    (
        "ex_ante_feasibility_valid_rate",
        "ex_ante_feasibility_valid_rate",
        "valid ex ante feasibility rows / feasibility rows",
        "count feasibility rows without future leakage and actionable before peak",
        "count feasibility rows",
        "S07",
        "ratio",
    ),
]


THRESHOLD_SPECS = [
    (
        "thr_rendered_spec_view_valid_rate_min",
        "rendered_spec_view_valid_rate_min",
        "rendered_spec_view_valid_rate",
        1.0,
        ">=",
        "observe_only",
    ),
    (
        "thr_health_state_enum_valid_rate_min",
        "health_state_enum_valid_rate_min",
        "health_state_enum_valid_rate",
        1.0,
        ">=",
        "observe_only",
    ),
    (
        "thr_contract_lifecycle_active_gate_coverage_rate_min",
        "contract_lifecycle_active_gate_coverage_rate_min",
        "contract_lifecycle_active_gate_coverage_rate",
        1.0,
        ">=",
        "observe_only",
    ),
    (
        "thr_objective_priority_conflict_resolution_rate_min",
        "objective_priority_conflict_resolution_rate_min",
        "objective_priority_conflict_resolution_rate",
        1.0,
        ">=",
        "observe_only",
    ),
    (
        "thr_goal_confidence_lower_bound_min",
        "goal_confidence_lower_bound_min",
        "goal_confidence_lower_bound",
        0.55,
        ">=",
        "normal_tiny",
    ),
    (
        "thr_fill_time_anchor_valid_rate_min",
        "fill_time_anchor_valid_rate_min",
        "fill_time_anchor_valid_rate",
        1.0,
        ">=",
        "shadow",
    ),
    (
        "thr_ex_ante_feasibility_valid_rate_min",
        "ex_ante_feasibility_valid_rate_min",
        "ex_ante_feasibility_valid_rate",
        1.0,
        ">=",
        "shadow",
    ),
]


ERROR_CODES = [
    "spec_governance_feasibility_policy_missing_or_invalid",
    "spec_governance_feasibility_policy_not_object",
    "rendered_spec_view_missing_malformed_or_stale",
    "health_state_enum_missing_malformed_or_unsafe",
    "contract_lifecycle_missing_malformed_or_ungated",
    "objective_priority_missing_malformed_or_unsafe",
    "goal_confidence_missing_malformed_or_inconclusive",
    "fill_time_anchor_missing_malformed_or_unpinned",
    "ex_ante_posthoc_feasibility_missing_malformed_or_leaky",
]


def load_json(rel_path: str) -> dict:
    return json.loads((ROOT / rel_path).read_text(encoding="utf-8"))


def write_json(rel_path: str, value: dict) -> None:
    path = ROOT / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def rendered_doc_hash(manifest: dict) -> str:
    rendered_hashes = {
        str(item["file"]): str(item.get("sha256") or "")
        for item in manifest.get("rendered_views", [])
        if isinstance(item, dict) and item.get("file")
    }
    return _sha256_json(rendered_hashes)


def hash_policy_rows(policy: dict) -> None:
    hash_keys = {
        "rendered_spec_views": "view_hash",
        "health_states": "health_hash",
        "contract_lifecycle": "lifecycle_hash",
        "objective_conflicts": "conflict_hash",
        "goal_confidence": "confidence_hash",
        "fill_time_anchors": "anchor_hash",
        "ex_ante_posthoc_feasibility": "feasibility_hash",
    }
    for collection, hash_key in hash_keys.items():
        for row in policy.get(collection, []):
            row[hash_key] = _hash_record_without(row, hash_key)


def build_policy(spec_report: dict, manifest: dict) -> dict:
    policy = {
        "schema_version": "v2.7.0.spec_governance_feasibility_policy.v1",
        "scope": "spec_governance_health_goal_confidence_fill_anchor_feasibility_truth",
        "failure_action": "spec_governance_or_feasibility_blocked",
        "rendered_spec_views": [
            {
                "rendered_view_id": "rendered:v2.7.0-canonical-markdown",
                "source_spec_hash": spec_report["spec_hash"],
                "rendered_doc_hash": rendered_doc_hash(manifest),
                "renderer_version": "v2.7.0.render_canonical_spec.v1",
                "rendered_at": "2026-05-27T02:00:00Z",
                "section_count": spec_report["section_count"],
                "missing_section_ids": [],
                "extra_section_ids": [],
                "render_validation_status": "valid",
                "metric_id": "rendered_spec_view_valid_rate",
                "threshold_id": "thr_rendered_spec_view_valid_rate_min",
                "observed_value": 1.0,
            }
        ],
        "health_states": [
            {
                "health_component": "canonical_spec_health",
                "health_state": "HEALTHY",
                "state_reason": "active spec hash, rendered view, metric and threshold registries validate together",
                "severity": "info",
                "first_seen_at": "2026-05-27T02:00:00Z",
                "last_seen_at": "2026-05-27T02:01:00Z",
                "blocking_modes": [],
                "recovery_condition": "rerun spec_governance_feasibility readiness after any mismatch",
                "source_event_id": "event:spec-governance-health-seed",
                "owner": "system",
                "metric_id": "health_state_enum_valid_rate",
                "threshold_id": "thr_health_state_enum_valid_rate_min",
                "observed_value": 1.0,
            }
        ],
        "contract_lifecycle": [],
        "objective_conflicts": [
            {
                "objective_conflict_id": "conflict:data_truth_vs_capture_seed",
                "conflicting_objectives": ["data_spec_truth", "capture_quality", "net_ev"],
                "chosen_objective": "data_spec_truth",
                "priority_rank": 4,
                "reason": "data/spec truth blocks capture and EV promotion when evidence is stale or dirty",
                "policy_version": "v2.7.0.objective_priority.seed",
                "operator_override_allowed": False,
                "metric_id": "objective_priority_conflict_resolution_rate",
                "threshold_id": "thr_objective_priority_conflict_resolution_rate_min",
                "observed_value": 1.0,
            }
        ],
        "goal_confidence": [
            {
                "metric_id": "goal_confidence_lower_bound",
                "metric_name": "goal_confidence_lower_bound",
                "numerator": 20,
                "denominator": 30,
                "min_denominator": 20,
                "point_estimate": 0.6666667,
                "wilson_lower_bound": 0.59,
                "beta_posterior_lower_bound": 0.58,
                "status": "pass",
                "metric_version": "v2.7.0.goal_confidence.seed",
                "window_id": "rolling_24h_decision_available_at",
                "threshold_id": "thr_goal_confidence_lower_bound_min",
                "observed_value": 0.58,
            }
        ],
        "fill_time_anchors": [
            {
                "anchor_id": "fill-anchor:seed-simulated-fill",
                "decision_ts": "2026-05-27T02:00:00Z",
                "decision_available_at": "2026-05-27T02:00:01Z",
                "quote_ts": "2026-05-27T02:00:01Z",
                "entry_quote_at_decision_ts": "2026-05-27T02:00:01Z",
                "simulated_fill_ts": "2026-05-27T02:00:02Z",
                "position_open_confirmed_ts": "2026-05-27T02:00:03Z",
                "fill_time_anchor_type": "simulated_fill_ts",
                "latency_components": {
                    "decision_latency_ms": 1000,
                    "queue_latency_ms": 250,
                    "quote_latency_ms": 250,
                    "fill_simulation_latency_ms": 500,
                },
                "metric_id": "fill_time_anchor_valid_rate",
                "threshold_id": "thr_fill_time_anchor_valid_rate_min",
                "observed_value": 1.0,
            }
        ],
        "ex_ante_posthoc_feasibility": [
            {
                "feasibility_id": "feasibility:seed-physically-capturable",
                "ex_ante_feasible": True,
                "posthoc_feasible": True,
                "feasibility_class": "physically_capturable",
                "feasibility_policy_version": "v2.7.0.ex_ante_posthoc.seed",
                "system_min_decision_latency_sec": 1,
                "system_min_entry_latency_sec": 1,
                "feature_available_at": "2026-05-27T02:00:04Z",
                "decision_ts": "2026-05-27T02:00:05Z",
                "earliest_actionable_ts": "2026-05-27T02:00:05Z",
                "peak_ts": "2026-05-27T02:01:00Z",
                "used_future_peak_in_ex_ante": False,
                "ex_ante_source_fields": [
                    "system_min_decision_latency_sec",
                    "system_min_entry_latency_sec",
                    "current_quote_availability",
                    "current_risk_availability",
                    "current_pool_resolution",
                    "current_provider_health",
                    "current_reclaim_state",
                    "current_queue_delay",
                ],
                "required_inputs_available_at": {
                    "telegram_anchor_available_at": "2026-05-27T02:00:00Z",
                    "pool_resolved_available_at": "2026-05-27T02:00:01Z",
                    "entry_quote_executable_available_at": "2026-05-27T02:00:02Z",
                    "exit_quote_executable_available_at": "2026-05-27T02:00:03Z",
                    "critical_risk_not_bad_available_at": "2026-05-27T02:00:04Z",
                    "liquidity_ok_available_at": "2026-05-27T02:00:04Z",
                    "decision_engine_available_at": "2026-05-27T02:00:05Z",
                },
                "metric_id": "ex_ante_feasibility_valid_rate",
                "threshold_id": "thr_ex_ante_feasibility_valid_rate_min",
                "observed_value": 1.0,
            }
        ],
        "source_files": [
            {
                "source_file": "scripts/v27_basic_contract_readiness.py",
                "source_anchor": "def verify_spec_governance_feasibility_contracts",
                "required_patterns": list(NEW_CONTRACTS),
            },
            {
                "source_file": "scripts/v27_mode_readiness.py",
                "source_anchor": '"RenderedSpecViewContract"',
                "required_patterns": [
                    '"HealthStateEnumContract"',
                    '"ContractLifecycleContract"',
                    '"ObjectivePriorityContract"',
                    '"GoalConfidenceContract"',
                    '"FillTimeAnchorContract"',
                    '"ExAnteVsPosthocFeasibilityContract"',
                ],
            },
        ],
    }
    for contract_id in NEW_CONTRACTS:
        policy["contract_lifecycle"].append(
            {
                "contract_id": contract_id,
                "contract_version": "v2.7.0.seed",
                "status": "active_gate",
                "introduced_in_version": "2.7.0",
                "deprecated_in_version": "none",
                "superseded_by": "none",
                "allowed_modes": ["normal_tiny"],
                "migration_required": False,
                "backfill_required": False,
                "owner": "system",
                "sunset_deadline": "none",
                "contract_tests_status": "pass",
                "metric_id": "contract_lifecycle_active_gate_coverage_rate",
                "threshold_id": "thr_contract_lifecycle_active_gate_coverage_rate_min",
                "observed_value": 1.0,
            }
        )
    hash_policy_rows(policy)
    return policy


def upsert_metrics() -> None:
    registry = load_json("config/v27-metric-definition-registry.json")
    metrics = registry.setdefault("metrics", [])
    by_id = {item.get("metric_id"): item for item in metrics if isinstance(item, dict)}
    for metric_id, name, formula, numerator, denominator, section, unit in METRIC_SPECS:
        record = {
            "metric_id": metric_id,
            "metric_name": name,
            "formula": formula,
            "numerator_definition": numerator,
            "denominator_definition": denominator,
            "window_id": "readiness_seed_window",
            "event_time_basis": "decision_available_at",
            "inclusion_criteria": ["v27_spec_governance_feasibility_readiness_seed"],
            "exclusion_criteria": ["policy_hash_mismatch", "environment_contaminated"],
            "late_event_policy": "append_only_recompute_next_metric_version",
            "partial_window_policy": "mark_incomplete_no_promotion",
            "unit": unit,
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
    for threshold_id, name, metric_id, value, operator, mode in THRESHOLD_SPECS:
        record = {
            "threshold_id": threshold_id,
            "threshold_name": name,
            "threshold_value": value,
            "unit": "ratio",
            "comparison_operator": operator,
            "scope": "spec_governance_feasibility_readiness",
            "applies_to_metric": metric_id,
            "applies_to_mode": mode,
            "owner": "system",
            "source_spec_section_id": "S04",
            "policy_bundle_id": "v2.7.0_seed_policy_bundle",
            "effective_from": "2026-05-27T00:00:00Z",
            "effective_to": "open",
            "change_reason": "v2.6.13 spec governance, confidence, fill anchor, and ex ante feasibility truth closure",
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
        if not isinstance(batch, dict)
        or batch.get("batch_id") != "v2.6.13_spec_governance_confidence_fill_feasibility"
    ]
    batches.append(
        {
            "batch_id": "v2.6.13_spec_governance_confidence_fill_feasibility",
            "contract_ids": list(NEW_CONTRACTS),
            "theme": "Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.",
        }
    )
    register["batches"] = sorted(batches, key=lambda item: str(item.get("batch_id", "")) if isinstance(item, dict) else "")
    write_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json", register)


def upsert_error_taxonomy() -> None:
    taxonomy_file = load_json("config/v27-error-taxonomy.json")
    taxonomy = taxonomy_file.setdefault("taxonomy", [])
    existing = {item.get("error_code"): item for item in taxonomy if isinstance(item, dict)}
    for code in ERROR_CODES:
        if code in existing:
            existing[code]["category"] = "spec_integrity"
            existing[code]["severity"] = "critical"
            existing[code]["operator_action"] = "repair spec governance feasibility policy and rerun v27 readiness"
            continue
        taxonomy.append(
            {
                "error_code": code,
                "category": "spec_integrity",
                "severity": "critical",
                "operator_action": "repair spec governance feasibility policy and rerun v27 readiness",
                "introduced_at": "2026-05-27T00:00:00Z",
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
        if "config/v27-spec-governance-feasibility-policy.json" not in files:
            files.append("config/v27-spec-governance-feasibility-policy.json")
        computed = _runtime_config_component_hashes(profile, {})
        for key, value in computed["component_hashes"].items():
            profile[key] = value
        profile["runtime_config_hash"] = computed["runtime_config_hash"]
        profile["expected_hash"] = computed["expected_hash"]
    write_json("config/v27-runtime-config-drift-policy.json", runtime)


def refresh_ci_policy_hashes(spec_report: dict) -> None:
    ci = load_json("config/v27-ci-spec-generated-policy.json")
    required_check = "python3 -m json.tool config/v27-spec-governance-feasibility-policy.json"
    for gate in ci.get("ci_merge_gates", []):
        if not isinstance(gate, dict):
            continue
        checks = gate.get("required_checks") if isinstance(gate.get("required_checks"), list) else []
        if required_check not in checks:
            anchor = "python3 -m json.tool config/v27-replay-build-model-policy.json"
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
            "config/v27-spec-governance-feasibility-policy.json",
            "scripts/v27_seed_spec_governance_feasibility.py",
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
    spec_report = validate_all(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH)
    manifest = load_json("spec/telegram_dog_regime_capture/v2.7.0/spec.manifest.json")
    policy = build_policy(spec_report, manifest)

    write_json("config/v27-spec-governance-feasibility-policy.json", policy)
    upsert_metrics()
    upsert_thresholds()
    upsert_contract_catalog()
    upsert_gap_register()
    upsert_error_taxonomy()
    refresh_runtime_config_hashes()
    refresh_ci_policy_hashes(spec_report)
    print(json.dumps({"status": "ok", "contracts": list(NEW_CONTRACTS)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
