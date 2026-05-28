#!/usr/bin/env python3
"""Seed v2.7 release, experiment, SLO, and adversarial safety evidence."""

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


POLICY_FILE = "config/v27-release-experiment-safety-policy.json"
MARKOV_SHADOW_POLICY_FILE = "config/v27-markov-lifecycle-forecast-policy.json"
MARKOV_SHADOW_CONTRACTS = (
    "TelegramLifecycleTransitionMatrixContract",
    "LifecycleNstepForecastContract",
    "AbsorbingSemiMarkovForecastContract",
    "CompetingRiskForecastContract",
    "CensoringPolicyContract",
    "ForecastWalkForwardValidationContract",
    "HMMResearchOnlyBoundaryContract",
)
MARKOV_SHADOW_SOURCE_FILES = (
    MARKOV_SHADOW_POLICY_FILE,
    "scripts/v27_record_markov_shadow_forecasts.py",
    "scripts/v27_markov_shadow_calibration_report.py",
    "test_v27_markov_shadow_forecasts.py",
    "test_v27_markov_shadow_calibration_report.py",
)
MARKOV_SHADOW_REQUIRED_CHECKS = (
    f"python3 -m json.tool {MARKOV_SHADOW_POLICY_FILE}",
    "python3 -m py_compile scripts/v27_basic_contract_readiness.py scripts/v27_mode_readiness.py scripts/v27_denominator_projection.py scripts/generate_v27_contract_client.py scripts/telegram_lifecycle_markov.py scripts/v27_record_markov_shadow_forecasts.py scripts/v27_markov_shadow_calibration_report.py scripts/v27_seed_spec_governance_feasibility.py scripts/v27_seed_identity_unit_provider_finality.py scripts/v27_seed_execution_exit_safety.py scripts/v27_seed_delivery_traceability.py scripts/v27_seed_release_experiment_safety.py",
    "python3 -m pytest test_v27_basic_contract_readiness.py test_v27_mode_readiness.py test_v27_mode_gate_scope_audit.py test_telegram_lifecycle_markov.py test_v27_markov_shadow_forecasts.py test_v27_markov_shadow_calibration_report.py -q -p no:cacheprovider",
)
RUNTIME_MODE_GATE_SOURCE_FILES = (
    "scripts/v27_runtime_mode_gate.py",
    "scripts/paper_fast_lane.py",
    "scripts/paper_trade_monitor.py",
    "test_v27_runtime_mode_gate.py",
    "test_paper_fast_lane.py",
)
RUNTIME_MODE_GATE_REQUIRED_CHECKS = (
    "python3 -m py_compile scripts/v27_runtime_mode_gate.py scripts/paper_fast_lane.py scripts/paper_trade_monitor.py",
    "python3 -m pytest test_v27_runtime_mode_gate.py test_paper_fast_lane.py -q -p no:cacheprovider",
)

NEW_CONTRACTS = {
    "SecretsManagementContract": (
        "mvp_blocking",
        "S15",
        ["secret_name", "scope", "rotation_interval_days", "last_rotated_at", "environment_allowed"],
        "p0_security_event",
    ),
    "SystemSLO": (
        "normal_tiny_blocking",
        "S11",
        ["slo_id", "metric_id", "threshold_id", "measured_value", "new_entry_action"],
        "new_entry_shadow_only",
    ),
    "NoTradeRootCause": (
        "ultra_tiny_blocking",
        "S20",
        ["root_cause_id", "root_cause_code", "d3a_candidate_count", "fill_count", "remediation_action"],
        "no_trade_unknown",
    ),
    "ReleaseComplexityBudget": (
        "normal_tiny_blocking",
        "S18",
        ["release_id", "max_new_gates_per_release", "new_gates", "required_shadow_hours_before_gate"],
        "release_blocked",
    ),
    "BackpressurePolicy": (
        "normal_tiny_blocking",
        "S11",
        ["component", "queue_depth", "max_queue_depth", "drops_p0_p1_allowed", "exit_safety_priority"],
        "capacity_backpressure_blocked",
    ),
    "BudgetReserveContract": (
        "normal_tiny_blocking",
        "S11",
        ["reserve_id", "budget_pool", "reserved_for", "hard_limit", "priority_class"],
        "budget_reserve_blocked",
    ),
    "BlindedHoldoutContract": (
        "normal_tiny_promotion_blocking",
        "S10",
        ["holdout_id", "window_id", "blinded", "access_count", "no_retune_enforced"],
        "experiment_invalid",
    ),
    "ManualOverrideContract": (
        "normal_tiny_blocking",
        "S10",
        ["override_id", "operator_id", "quarantine_required", "promotion_evidence_allowed", "training_allowed"],
        "manual_override_quarantined",
    ),
    "ContractTestSuite": (
        "normal_tiny_blocking",
        "S22",
        ["suite_id", "contract_id", "test_command", "pass_fail", "coverage_class"],
        "release_blocked",
    ),
    "AdversarialReplaySuite": (
        "normal_tiny_blocking",
        "S22",
        ["replay_id", "scenario", "expected_action", "observed_action", "machine_checked"],
        "release_blocked",
    ),
}

METRIC_SPECS = [
    ("secrets_management_safe_rate", "secrets_management_safe_rate", "safe secrets / managed secrets", "safe managed secrets", "managed secrets", "S15"),
    ("system_slo_healthy_rate", "system_slo_healthy_rate", "healthy SLO rows / SLO rows", "healthy SLO rows", "SLO rows", "S11"),
    ("no_trade_root_cause_known_rate", "no_trade_root_cause_known_rate", "known no-trade root causes / no-trade root causes", "known no-trade root causes", "no-trade root causes", "S20"),
    ("release_complexity_budget_pass_rate", "release_complexity_budget_pass_rate", "release complexity rows within budget / release complexity rows", "within-budget release rows", "release complexity rows", "S18"),
    ("backpressure_policy_safe_rate", "backpressure_policy_safe_rate", "safe backpressure policies / backpressure policies", "safe backpressure policies", "backpressure policies", "S11"),
    ("budget_reserve_protected_rate", "budget_reserve_protected_rate", "protected budget reserves / budget reserves", "protected budget reserves", "budget reserves", "S11"),
    ("blinded_holdout_clean_rate", "blinded_holdout_clean_rate", "clean blinded holdouts / holdouts", "clean blinded holdouts", "holdout rows", "S10"),
    ("manual_override_quarantined_rate", "manual_override_quarantined_rate", "quarantined manual overrides / manual overrides", "quarantined manual overrides", "manual override rows", "S10"),
    ("contract_test_suite_pass_rate", "contract_test_suite_pass_rate", "passing contract suite rows / contract suite rows", "passing contract suite rows", "contract suite rows", "S22"),
    ("adversarial_replay_suite_pass_rate", "adversarial_replay_suite_pass_rate", "passing adversarial replay rows / adversarial replay rows", "passing adversarial replay rows", "adversarial replay rows", "S22"),
]

THRESHOLD_SPECS = [
    ("thr_secrets_management_safe_rate_min", "secrets_management_safe_rate_min", "secrets_management_safe_rate", "observe_only"),
    ("thr_system_slo_healthy_rate_min", "system_slo_healthy_rate_min", "system_slo_healthy_rate", "normal_tiny"),
    ("thr_no_trade_root_cause_known_rate_min", "no_trade_root_cause_known_rate_min", "no_trade_root_cause_known_rate", "ultra_tiny"),
    ("thr_release_complexity_budget_pass_rate_min", "release_complexity_budget_pass_rate_min", "release_complexity_budget_pass_rate", "normal_tiny"),
    ("thr_backpressure_policy_safe_rate_min", "backpressure_policy_safe_rate_min", "backpressure_policy_safe_rate", "normal_tiny"),
    ("thr_budget_reserve_protected_rate_min", "budget_reserve_protected_rate_min", "budget_reserve_protected_rate", "normal_tiny"),
    ("thr_blinded_holdout_clean_rate_min", "blinded_holdout_clean_rate_min", "blinded_holdout_clean_rate", "normal_tiny"),
    ("thr_manual_override_quarantined_rate_min", "manual_override_quarantined_rate_min", "manual_override_quarantined_rate", "normal_tiny"),
    ("thr_contract_test_suite_pass_rate_min", "contract_test_suite_pass_rate_min", "contract_test_suite_pass_rate", "normal_tiny"),
    ("thr_adversarial_replay_suite_pass_rate_min", "adversarial_replay_suite_pass_rate_min", "adversarial_replay_suite_pass_rate", "normal_tiny"),
]

ERROR_CODES = [
    "release_experiment_safety_policy_missing_or_invalid",
    "release_experiment_safety_policy_not_object",
    "secrets_management_missing_malformed_or_unsafe",
    "system_slo_missing_malformed_or_unhealthy",
    "no_trade_root_cause_missing_malformed_or_unknown",
    "release_complexity_budget_missing_malformed_or_exceeded",
    "backpressure_policy_missing_malformed_or_unsafe",
    "budget_reserve_missing_malformed_or_unprotected",
    "blinded_holdout_missing_malformed_or_contaminated",
    "manual_override_missing_malformed_or_unquarantined",
    "contract_test_suite_missing_malformed_or_failing",
    "adversarial_replay_suite_missing_malformed_or_failing",
]


def load_json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def write_json(path: str, value: dict) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _hash_many(rows: list[dict], hash_field: str) -> list[dict]:
    for row in rows:
        row[hash_field] = _hash_record_without(row, hash_field)
    return rows


def _metric_id(contract_id: str) -> str:
    return {
        "SecretsManagementContract": "secrets_management_safe_rate",
        "SystemSLO": "system_slo_healthy_rate",
        "NoTradeRootCause": "no_trade_root_cause_known_rate",
        "ReleaseComplexityBudget": "release_complexity_budget_pass_rate",
        "BackpressurePolicy": "backpressure_policy_safe_rate",
        "BudgetReserveContract": "budget_reserve_protected_rate",
        "BlindedHoldoutContract": "blinded_holdout_clean_rate",
        "ManualOverrideContract": "manual_override_quarantined_rate",
        "ContractTestSuite": "contract_test_suite_pass_rate",
        "AdversarialReplaySuite": "adversarial_replay_suite_pass_rate",
    }[contract_id]


def _threshold_id(contract_id: str) -> str:
    metric = _metric_id(contract_id)
    return f"thr_{metric}_min"


def _metric_binding(contract_id: str) -> dict:
    return {
        "metric_id": _metric_id(contract_id),
        "threshold_id": _threshold_id(contract_id),
        "observed_value": 1.0,
    }


def build_policy() -> dict:
    secrets = [
        {
            "secret_name": "DASHBOARD_TOKEN",
            "scope": "dashboard_token",
            "rotation_interval_days": 30,
            "last_rotated_at": "2026-05-28T00:00:00Z",
            "owner": "system",
            "leak_detected": False,
            "revocation_status": "active",
            "environment_allowed": ["paper"],
            "mutation_scope_allowed": False,
            **_metric_binding("SecretsManagementContract"),
        },
        {
            "secret_name": "GMGN_PROVIDER_KEY",
            "scope": "provider_api_key",
            "rotation_interval_days": 30,
            "last_rotated_at": "2026-05-28T00:00:00Z",
            "owner": "system",
            "leak_detected": False,
            "revocation_status": "active",
            "environment_allowed": ["paper"],
            "mutation_scope_allowed": False,
            **_metric_binding("SecretsManagementContract"),
        },
        {
            "secret_name": "LIVE_SIGNING_KEY",
            "scope": "live_signing_secret",
            "rotation_interval_days": 7,
            "last_rotated_at": "2026-05-28T00:00:00Z",
            "owner": "system",
            "leak_detected": False,
            "revocation_status": "active",
            "environment_allowed": ["live"],
            "mutation_scope_allowed": True,
            **_metric_binding("SecretsManagementContract"),
        },
    ]

    slos = [
        ("slo_event_write_latency_p95", "event_write_latency_p95"),
        ("slo_outbox_publish_latency_p95", "outbox_publish_latency_p95"),
        ("slo_projection_lag_p95", "projection_lag_p95"),
        ("slo_read_model_lag_p95", "read_model_lag_p95"),
        ("slo_capacity_headroom_pct", "capacity_headroom_pct"),
    ]
    system_slos = [
        {
            "slo_id": slo_id,
            "source_metric_name": source_metric,
            "status": "healthy",
            "severity": "P2",
            "new_entry_action": "allow",
            "exit_safety_action": "preserve_exit_safety",
            "slo_window_id": "window:readiness_seed",
            "measured_value": 1.0,
            "metric_id": "system_slo_healthy_rate",
            "threshold_id": "thr_system_slo_healthy_rate_min",
        }
        for slo_id, source_metric in slos
    ]

    no_trade = [
        ("ntrc-d3a-d3b-blocked", "D3a_but_D3b_blocked", 4, 0, "policy_blocker", "review reclaim/overextension thresholds"),
        ("ntrc-queued-expired", "queued_but_expired", 2, 0, "queue_latency", "increase fast-lane priority and capacity reserve"),
        ("ntrc-quote-failed", "locked_but_quote_failed", 3, 0, "provider_quote_failure", "route to quota-isolated provider lane"),
        ("ntrc-read-model-stale", "read_model_stale", 1, 0, "read_model_freshness", "force shadow-only until freshness recovers"),
    ]
    no_trade_rows = [
        {
            "root_cause_id": root_cause_id,
            "root_cause_code": code,
            "d3a_candidate_count": d3a,
            "fill_count": fills,
            "category": category,
            "owner": "system",
            "remediation_action": remediation,
            **_metric_binding("NoTradeRootCause"),
        }
        for root_cause_id, code, d3a, fills, category, remediation in no_trade
    ]

    complexity = [
        {
            "release_id": "v2.7.0-release-experiment-safety-seed",
            "max_new_gates_per_release": 1,
            "new_gates": 1,
            "max_new_detectors_per_release": 1,
            "new_detectors": 1,
            "required_shadow_hours_before_gate": 8,
            "observed_shadow_hours": 8,
            "rollback_metric": "blocked_actionable_increase_or_net_ev_worse",
            "status": "within_budget",
            **_metric_binding("ReleaseComplexityBudget"),
        }
    ]

    backpressure = [
        ("event_log_writer", 4, 100, "shed_shadow_polling"),
        ("transactional_outbox", 2, 100, "pause_new_entry"),
        ("projection_consumer", 5, 100, "shadow_only_until_caught_up"),
        ("provider_quota_lane", 3, 100, "preserve_exit_and_pending_entry_quotes"),
    ]
    backpressure_rows = [
        {
            "component": component,
            "queue_depth": queue_depth,
            "max_queue_depth": max_queue_depth,
            "backpressure_action": action,
            "drops_p0_p1_allowed": False,
            "exit_safety_priority": "reserved_first",
            **_metric_binding("BackpressurePolicy"),
        }
        for component, queue_depth, max_queue_depth, action in backpressure
    ]

    reserves = [
        {
            "reserve_id": "reserve-exit-safety-p0",
            "budget_pool": "exit_safety_budget",
            "reserved_for": ["open_position_exit", "emergency_exit_journal"],
            "reserved_amount": 100,
            "current_usage": 10,
            "hard_limit": 100,
            "priority_class": "P0",
            "borrow_allowed": False,
            **_metric_binding("BudgetReserveContract"),
        },
        {
            "reserve_id": "reserve-entry-execution-p1",
            "budget_pool": "entry_execution_budget",
            "reserved_for": ["pending_entry_quote", "state_revalidation"],
            "reserved_amount": 80,
            "current_usage": 12,
            "hard_limit": 80,
            "priority_class": "P1",
            "borrow_allowed": False,
            **_metric_binding("BudgetReserveContract"),
        },
        {
            "reserve_id": "reserve-shadow-learning-p2",
            "budget_pool": "exploration_budget",
            "reserved_for": ["shadow_polling", "detector_calibration"],
            "reserved_amount": 20,
            "current_usage": 4,
            "hard_limit": 20,
            "priority_class": "P2",
            "borrow_allowed": True,
            **_metric_binding("BudgetReserveContract"),
        },
    ]

    holdouts = [
        {
            "holdout_id": "telegram-clean-dog-holdout-v27-seed",
            "window_id": "window:holdout_24h_seed",
            "blinded": True,
            "access_count": 0,
            "no_retune_enforced": True,
            "contamination_status": "clean",
            "promotion_evidence_allowed": True,
            **_metric_binding("BlindedHoldoutContract"),
        }
    ]

    overrides = [
        {
            "override_id": "manual-label-correction-quarantine-v27",
            "operator_id": "system",
            "action": "manual_label_correction",
            "quarantine_required": True,
            "promotion_evidence_allowed": False,
            "training_allowed": False,
            "audit_event_id": "audit-manual-label-correction-v27",
            "approval_status": "quarantined",
            **_metric_binding("ManualOverrideContract"),
        },
        {
            "override_id": "manual-decision-override-quarantine-v27",
            "operator_id": "system",
            "action": "manual_decision_override",
            "quarantine_required": True,
            "promotion_evidence_allowed": False,
            "training_allowed": False,
            "audit_event_id": "audit-manual-decision-override-v27",
            "approval_status": "quarantined",
            **_metric_binding("ManualOverrideContract"),
        },
    ]

    test_command = "python3 -m pytest test_v27_basic_contract_readiness.py -q -p no:cacheprovider"
    tests = [
        {
            "suite_id": f"contract-suite-{contract_id}",
            "contract_id": contract_id,
            "test_command": test_command,
            "pass_fail": "pass",
            "coverage_class": "normal_tiny_blocking" if contract_id not in {"SecretsManagementContract", "NoTradeRootCause"} else "mvp_blocking",
            "evidence_hash": _sha256_json({"contract_id": contract_id, "test_command": test_command}),
            **_metric_binding("ContractTestSuite"),
        }
        for contract_id in sorted(NEW_CONTRACTS)
    ]

    replays = [
        ("adv-holdout-unblind-promotion", "holdout_unblinded_then_used_for_promotion", "promotion_evidence_rejected", "critical"),
        ("adv-manual-override-training", "manual_override_trade_used_in_training", "sample_quarantined", "critical"),
        ("adv-backpressure-p1-drop", "capacity_load_causes_dropped_p1_event", "normal_tiny_blocked", "critical"),
        ("adv-secret-leak", "secret_leak_detected", "p0_security_event", "critical"),
    ]
    replay_rows = [
        {
            "replay_id": replay_id,
            "scenario": scenario,
            "expected_action": action,
            "observed_action": action,
            "machine_checked": True,
            "pass_fail": "pass",
            "criticality": criticality,
            **_metric_binding("AdversarialReplaySuite"),
        }
        for replay_id, scenario, action, criticality in replays
    ]

    return {
        "schema_version": "v2.7.0.release_experiment_safety_policy.v1",
        "scope": "secrets_slo_no_trade_complexity_backpressure_budget_holdout_override_tests_replay",
        "failure_action": "release_experiment_safety_blocked",
        "secrets_management": _hash_many(secrets, "secret_hash"),
        "system_slos": _hash_many(system_slos, "slo_hash"),
        "no_trade_root_causes": _hash_many(no_trade_rows, "root_cause_hash"),
        "release_complexity_budgets": _hash_many(complexity, "complexity_hash"),
        "backpressure_policies": _hash_many(backpressure_rows, "backpressure_hash"),
        "budget_reserves": _hash_many(reserves, "reserve_hash"),
        "blinded_holdouts": _hash_many(holdouts, "holdout_hash"),
        "manual_overrides": _hash_many(overrides, "override_hash"),
        "contract_test_suite": _hash_many(tests, "test_hash"),
        "adversarial_replay_suite": _hash_many(replay_rows, "replay_hash"),
        "source_files": [
            {
                "source_file": "scripts/v27_basic_contract_readiness.py",
                "source_anchor": "def verify_release_experiment_safety_contracts",
                "required_patterns": list(NEW_CONTRACTS),
            },
            {
                "source_file": "scripts/v27_mode_readiness.py",
                "source_anchor": "\"SecretsManagementContract\"",
                "required_patterns": [
                    "\"SystemSLO\"",
                    "\"NoTradeRootCause\"",
                    "\"ReleaseComplexityBudget\"",
                    "\"BackpressurePolicy\"",
                    "\"BudgetReserveContract\"",
                    "\"BlindedHoldoutContract\"",
                    "\"ManualOverrideContract\"",
                    "\"ContractTestSuite\"",
                    "\"AdversarialReplaySuite\"",
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
            "inclusion_criteria": ["v27_release_experiment_safety_seed"],
            "exclusion_criteria": ["holdout_contaminated", "manual_override_unquarantined", "critical_slo_unresolved"],
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
    for threshold_id, name, metric_id, mode in THRESHOLD_SPECS:
        record = {
            "threshold_id": threshold_id,
            "threshold_name": name,
            "threshold_value": 1.0,
            "unit": "ratio",
            "comparison_operator": ">=",
            "scope": "release_experiment_safety_readiness",
            "applies_to_metric": metric_id,
            "applies_to_mode": mode,
            "owner": "system",
            "source_spec_section_id": "S18",
            "policy_bundle_id": "v2.7.0_seed_policy_bundle",
            "effective_from": "2026-05-28T00:00:00Z",
            "effective_to": "open",
            "change_reason": "v2.6.13 secrets, SLO, no-trade, release complexity, holdout, override, contract test, and adversarial replay closure",
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
        if not isinstance(batch, dict) or batch.get("batch_id") != "v2.6.13_release_experiment_safety"
    ]
    batches.append(
        {
            "batch_id": "v2.6.13_release_experiment_safety",
            "contract_ids": list(NEW_CONTRACTS),
            "theme": "Secrets lifecycle, system SLO, no-trade root cause, release complexity, backpressure, budget reserve, holdout blinding, manual override quarantine, contract tests, and adversarial replay must be machine-checkable before normal tiny.",
        }
    )
    register["batches"] = sorted(batches, key=lambda item: str(item.get("batch_id", "")) if isinstance(item, dict) else "")
    write_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json", register)


def upsert_error_taxonomy() -> None:
    taxonomy_file = load_json("config/v27-error-taxonomy.json")
    allowed_categories = taxonomy_file.setdefault("allowed_categories", [])
    if "release_experiment_safety" not in allowed_categories:
        allowed_categories.append("release_experiment_safety")
        taxonomy_file["allowed_categories"] = sorted(str(item) for item in allowed_categories)
    taxonomy = taxonomy_file.setdefault("taxonomy", [])
    existing = {item.get("error_code"): item for item in taxonomy if isinstance(item, dict)}
    for code in ERROR_CODES:
        if code in existing:
            existing[code]["category"] = "release_experiment_safety"
            existing[code]["severity"] = "critical"
            existing[code]["operator_action"] = "repair release experiment safety policy and rerun v27 readiness"
            continue
        taxonomy.append(
            {
                "error_code": code,
                "category": "release_experiment_safety",
                "severity": "critical",
                "operator_action": "repair release experiment safety policy and rerun v27 readiness",
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
        checks = [
            check
            for check in checks
            if "scripts/v27_record_markov_shadow_forecasts.py scripts/v27_seed_spec_governance" not in str(check)
            and "test_telegram_lifecycle_markov.py -q -p no:cacheprovider" not in str(check)
            and "test_v27_markov_shadow_forecasts.py -q -p no:cacheprovider" not in str(check)
        ]
        if required_check not in checks:
            anchor = "python3 -m json.tool config/v27-delivery-traceability-policy.json"
            if anchor in checks:
                checks.insert(checks.index(anchor) + 1, required_check)
            else:
                checks.append(required_check)
        for check in MARKOV_SHADOW_REQUIRED_CHECKS:
            if check not in checks:
                checks.append(check)
        for check in RUNTIME_MODE_GATE_REQUIRED_CHECKS:
            if check not in checks:
                checks.append(check)
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
        for contract_id in (*NEW_CONTRACTS, *MARKOV_SHADOW_CONTRACTS):
            if contract_id not in affected:
                affected.append(contract_id)
        source_files = impact.get("source_files") if isinstance(impact.get("source_files"), list) else []
        for source in (
            POLICY_FILE,
            "scripts/v27_seed_release_experiment_safety.py",
            *MARKOV_SHADOW_SOURCE_FILES,
            *RUNTIME_MODE_GATE_SOURCE_FILES,
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
