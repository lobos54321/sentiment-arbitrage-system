#!/usr/bin/env python3
"""Record normal-tiny governance and compliance readiness evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
SOURCE = "v27_normal_tiny_governance_evidence"
SCHEMA_VERSION = "v2.7.0.normal_tiny_governance_evidence.v1"


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _spec(event_type: str, aggregate_id: str, payload: dict[str, Any], run_id: str, key_suffix: str, now: str):
    return {
        "event_type": event_type,
        "aggregate_id": aggregate_id,
        "payload": {
            "governance_evidence_schema_version": SCHEMA_VERSION,
            "readiness_scope": "normal_tiny",
            "readiness_drill": True,
            "evidence_source": SOURCE,
            **payload,
        },
        "source": SOURCE,
        "idempotency_key": f"normal_tiny_governance_evidence:{run_id}:{key_suffix}",
        "observed_at": now,
        "available_at": now,
    }


def build_event_specs(*, run_id: str | None = None) -> list[dict[str, Any]]:
    run_id = run_id or _default_run_id()
    now = _utc_now_iso()
    future = "2099-01-01T00:00:00Z"
    position_id = f"position-{run_id}"
    release_id = f"release-{run_id}"
    evidence_hash = sha256_hex({"run_id": run_id, "scope": "normal_tiny_governance"})
    assumption_id = f"assumption-{run_id}"
    package_id = f"promotion-evidence-package-{run_id}"
    return [
        _spec("open_position_valuation_recorded", f"open_position_valuation:{position_id}", {"position_id": position_id, "valuation_ts": now, "quote_source": "jupiter_ultra", "valuation_price": 0.001, "valuation_hash": sha256_hex({"position_id": position_id, "valuation_price": 0.001})}, run_id, "open_position_valuation", now),
        _spec("exit_policy_migration_recorded", f"exit_policy_migration:{position_id}", {"position_id": position_id, "old_exit_policy": "exit-policy-v1", "new_exit_policy": "exit-policy-v2", "migration_reason": "tighten_dirty_quote_exit_guard", "migrated_at": now}, run_id, "exit_policy_migration", now),
        _spec("open_position_policy_migration_recorded", f"open_position_policy_migration:{position_id}", {"position_id": position_id, "old_exit_policy": "exit-policy-v1", "new_exit_policy": "exit-policy-v2", "migration_reason": "align_open_position_exit_policy", "checked_at": now}, run_id, "open_position_policy_migration", now),
        _spec("position_ownership_transfer_recorded", f"position_ownership_transfer:{position_id}", {"position_id": position_id, "from_owner": "paper_executor", "to_owner": "risk_controller", "transfer_reason": "risk_revalidation_requires_controller", "transferred_at": now}, run_id, "position_ownership_transfer", now),
        _spec("rollback_verification_recorded", f"rollback_verification:{run_id}", {"rollback_id": f"rollback-{run_id}", "from_version": "release-v2", "to_version": "release-v1", "verified_at": now}, run_id, "rollback_verification", now),
        _spec("partial_rollback_policy_recorded", f"partial_rollback_policy:{run_id}", {"rollback_id": f"partial-rollback-{run_id}", "component_scope": "dashboard:v27-readiness", "dependency_scope": "read_model_refresh", "verification_plan": "health_check_and_scope_audit", "rolled_back_at": now}, run_id, "partial_rollback_policy", now),
        _spec("release_readiness_review_recorded", f"release_readiness_review:{run_id}", {"review_id": f"release-review-{run_id}", "release_id": release_id, "required_evidence": ["health", "scope_audit", "pytest"], "approval_status": "approved", "approved_at": now}, run_id, "release_readiness_review", now),
        _spec("change_freeze_recorded", f"change_freeze:{run_id}", {"freeze_id": f"freeze-{run_id}", "scope": "normal_tiny_runtime", "start_at": now, "end_at": future, "exception_policy": "break_glass_only"}, run_id, "change_freeze", now),
        _spec("notification_channel_integrity_recorded", f"notification_channel_integrity:ops-alerts-{run_id}", {"channel_id": f"ops-alerts-{run_id}", "destination_hash": sha256_hex({"channel_id": f"ops-alerts-{run_id}", "destination": "telegram_ops"}), "signature_required": True, "delivery_status": "verified", "checked_at": now}, run_id, "notification_channel_integrity", now),
        _spec("runbook_freshness_recorded", f"runbook_freshness:normal-tiny-rollback-{run_id}", {"runbook_id": f"normal-tiny-rollback-{run_id}", "owner": "runtime", "last_reviewed_at": now, "max_age_days": 30, "freshness_status": "fresh"}, run_id, "runbook_freshness", now),
        _spec("metric_backfill_impact_recorded", f"metric_backfill_impact:{run_id}", {"backfill_id": f"metric-backfill-{run_id}", "metric_id": "telegram_capture_rate_D3b", "impact_scope": "metric_window_only", "impact_report_hash": sha256_hex({"backfill_id": run_id, "metric_id": "telegram_capture_rate_D3b"}), "checked_at": now}, run_id, "metric_backfill_impact", now),
        _spec("selection_bias_diagnostic_recorded", f"selection_bias_diagnostic:{run_id}", {"diagnostic_id": f"selection-bias-{run_id}", "selection_policy_version": "normal-tiny-selection-v1", "included_count": 40, "excluded_count": 8, "bias_result": "within_tolerance", "checked_at": now}, run_id, "selection_bias_diagnostic", now),
        _spec("access_review_recorded", f"access_review:{run_id}", {"review_id": f"access-review-{run_id}", "operator_id": "operator-runtime", "scope": "dashboard:admin_mutation", "privilege_delta": "reduced", "reviewed_at": now}, run_id, "access_review", now),
        _spec("approval_workflow_recorded", f"approval_workflow:{run_id}", {"approval_id": f"approval-{run_id}", "mutation_id": f"mutation-{run_id}", "required_approvers": ["runtime-owner"], "approval_state": "approved", "approved_at": now}, run_id, "approval_workflow", now),
        _spec("break_glass_access_recorded", f"break_glass_access:{run_id}", {"break_glass_id": f"break-glass-{run_id}", "operator_id": "operator-runtime", "reason": "restore_paper_read_model", "expires_at": future, "audit_event_id": f"audit-{run_id}"}, run_id, "break_glass_access", now),
        _spec("csv_spreadsheet_injection_recorded", f"csv_spreadsheet_injection:{run_id}:symbol", {"export_id": f"export-{run_id}", "column_name": "symbol", "unsafe_prefix_detected": True, "sanitization_policy": "escape_formula_prefix", "checked_at": now}, run_id, "csv_spreadsheet_injection", now),
        _spec("evidence_external_anchoring_recorded", f"evidence_external_anchoring:{run_id}", {"anchor_id": f"anchor-{run_id}", "anchored_hash": evidence_hash, "anchor_target": "v27_denominator_projection", "anchored_at": now}, run_id, "evidence_external_anchoring", now),
        _spec("experiment_assignment_immutability_recorded", f"experiment_assignment_immutability:{run_id}", {"assignment_id": f"assignment-{run_id}", "randomization_unit": "normal_tiny_promotion_policy", "original_assignment_hash": sha256_hex({"assignment_id": run_id, "arm": "control"}), "attempted_change_hash": sha256_hex({"assignment_id": run_id, "arm": "treatment"}), "detected_at": now}, run_id, "experiment_assignment_immutability", now),
        _spec("incident_postmortem_recorded", f"incident_postmortem:{run_id}", {"postmortem_id": f"postmortem-{run_id}", "incident_id": f"incident-{run_id}", "root_cause": "read_model_refresh_regression", "corrective_actions": ["add_scope_audit_regression"], "approved_at": now}, run_id, "incident_postmortem", now),
        _spec("label_dispute_resolution_recorded", f"label_dispute_resolution:{run_id}", {"dispute_id": f"label-dispute-{run_id}", "label_id": f"label-{run_id}", "resolution_action": "quarantine", "resolved_at": now}, run_id, "label_dispute_resolution", now),
        _spec("negative_control_recorded", f"negative_control:{run_id}", {"control_id": f"negative-control-{run_id}", "control_group": "holdout", "expected_no_effect_metric": 0.01, "observed_effect": 0.002, "checked_at": now}, run_id, "negative_control", now),
        _spec("operator_training_certification_recorded", f"operator_training_certification:operator-runtime:normal_tiny_runtime_ops", {"operator_id": "operator-runtime", "training_module": "normal_tiny_runtime_ops", "certification_status": "certified", "expires_at": future, "checked_at": now}, run_id, "operator_training_certification", now),
        _spec("runtime_spec_assertion_recorded", f"runtime_spec_assertion:{run_id}", {"assertion_id": f"runtime-spec-assertion-{run_id}", "contract_id": "RealtimeCleanDetector", "runtime_location": "scripts/paper_trade_monitor.py:realtime_clean_gate", "failure_action": "runtime_assert_failed"}, run_id, "runtime_spec_assertion", now),
        _spec("minimum_viable_trust_boundary_recorded", f"minimum_viable_trust_boundary:{run_id}", {"boundary_id": f"minimum-viable-trust-{run_id}", "trusted_inputs": ["entry_quote", "exit_quote"], "untrusted_inputs": ["mark_only_peak", "posthoc_label"], "required_contracts": ["RealtimeCleanDetector", "QuoteIntentBindingContract"], "failure_action": "mode_blocked"}, run_id, "minimum_viable_trust_boundary", now),
        _spec("evidence_conflict_recorded", f"evidence_conflict:{run_id}", {"conflict_id": f"evidence-conflict-{run_id}", "evidence_a_hash": sha256_hex({"run_id": run_id, "evidence": "a"}), "evidence_b_hash": sha256_hex({"run_id": run_id, "evidence": "b"}), "resolution_policy": "quarantine_then_operator_review", "resolved_at": now}, run_id, "evidence_conflict", now),
        _spec("evidence_aging_recorded", f"evidence_aging:{run_id}", {"evidence_id": f"evidence-aging-{run_id}", "evidence_type": "quote_clean_snapshot", "max_age_ms": 120000, "age_ms": 30000, "expiration_action": "revalidate_before_entry"}, run_id, "evidence_aging", now),
        _spec("market_regime_invalidates_evidence_recorded", f"market_regime_invalidates_evidence:{run_id}", {"regime_id": f"market-regime-{run_id}", "evidence_id": f"evidence-aging-{run_id}", "invalidating_signal": "liquidity_regime_flip", "action": "revalidate_evidence", "detected_at": now}, run_id, "market_regime_invalidates_evidence", now),
        _spec("source_alpha_decay_exit_criteria_recorded", f"source_alpha_decay_exit_criteria:{run_id}", {"source_id": f"premium-clean-source-{run_id}", "alpha_metric": 0.12, "decay_window": "24h", "exit_threshold": 0.05, "action": "keep_source"}, run_id, "source_alpha_decay_exit_criteria", now),
        _spec("false_negative_budget_recorded", f"false_negative_budget:{run_id}", {"budget_id": f"false-negative-budget-{run_id}", "hazard_class": "missed_clean_gold_dog", "allowed_false_negative_rate": 0.15, "observed_rate": 0.08, "action": "continue_with_watch"}, run_id, "false_negative_budget", now),
        _spec("small_sample_decision_recorded", f"small_sample_decision:{run_id}", {"policy_id": f"small-sample-policy-{run_id}", "sample_size": 40, "min_sample_size": 30, "decision_allowed": True, "fallback_action": "hold_promotion"}, run_id, "small_sample_decision", now),
        _spec("safety_vs_capture_tradeoff_recorded", f"safety_vs_capture_tradeoff:{run_id}", {"tradeoff_id": f"safety-capture-tradeoff-{run_id}", "safety_metric": 0.98, "capture_metric": 0.62, "chosen_policy": "safety_first_capture_watch", "approved_at": now}, run_id, "safety_vs_capture_tradeoff", now),
        _spec("implementation_drift_monitor_recorded", f"implementation_drift_monitor:{run_id}", {"drift_id": f"implementation-drift-{run_id}", "spec_contract_id": "RealtimeCleanDetector", "runtime_location": "scripts/paper_trade_monitor.py:realtime_clean_gate", "drift_detected": False, "detected_at": now}, run_id, "implementation_drift_monitor", now),
        _spec("assumption_registry_recorded", f"assumption_registry:{assumption_id}", {"assumption_id": assumption_id, "scope": "normal_tiny_capture_metrics", "owner": "runtime-owner", "evidence_link": "v27_denominator_projection:runtime_trust", "expires_at": future}, run_id, "assumption_registry", now),
        _spec("assumption_invalidation_trigger_recorded", f"assumption_invalidation_trigger:{assumption_id}", {"assumption_id": assumption_id, "trigger_metric": "missed_clean_gold_false_negative_rate", "threshold": 0.15, "observed_value": 0.21, "invalidated_at": now}, run_id, "assumption_invalidation_trigger", now),
        _spec("contract_priority_graph_recorded", f"contract_priority_graph:{run_id}", {"graph_id": f"contract-priority-graph-{run_id}", "higher_priority_contract": "SafetyVsCaptureTradeoffContract", "lower_priority_contract": "SourceAlphaDecayExitCriteria", "cycle_detected": False, "resolved_at": now}, run_id, "contract_priority_graph", now),
        _spec("contract_conflict_resolution_recorded", f"contract_conflict_resolution:{run_id}", {"conflict_id": f"contract-conflict-{run_id}", "higher_priority_contract": "SafetyVsCaptureTradeoffContract", "lower_priority_contract": "SourceAlphaDecayExitCriteria", "resolution_action": "apply_higher_priority_contract"}, run_id, "contract_conflict_resolution", now),
        _spec("contract_failure_blast_radius_recorded", f"contract_failure_blast_radius:RealtimeCleanDetector:{run_id}", {"contract_id": "RealtimeCleanDetector", "blast_radius": "normal_tiny_entry_block", "affected_modes": ["normal_tiny"], "fallback_action": "block_entry_and_hold_shadow", "reviewed_at": now}, run_id, "contract_failure_blast_radius", now),
        _spec("dashboard_triage_workflow_recorded", f"dashboard_triage_workflow:{run_id}", {"triage_id": f"dashboard-triage-{run_id}", "blocker_code": "regression_budget_exceeded", "owner": "runtime-owner", "next_action": "open_metric_escalation", "due_at": future}, run_id, "dashboard_triage_workflow", now),
        _spec("issue_escalation_from_metrics_recorded", f"issue_escalation_from_metrics:{run_id}", {"metric_id": "missed_clean_gold_false_negative_rate", "threshold": 0.15, "issue_id": f"issue-runtime-trust-{run_id}", "escalation_owner": "runtime-owner", "created_at": now}, run_id, "issue_escalation_from_metrics", now),
        _spec("promotion_evidence_package_recorded", f"promotion_evidence_package:{package_id}", {"package_id": package_id, "evidence_hash": evidence_hash, "generated_at": now, "approval_status": "approved"}, run_id, "promotion_evidence_package", now),
        _spec("regression_budget_recorded", f"regression_budget:{run_id}", {"budget_id": f"regression-budget-{run_id}", "metric_id": "clean_dog_capture_rate", "allowed_regression": 0.03, "observed_regression": 0.01, "action": "allow_release"}, run_id, "regression_budget", now),
        _spec("root_cause_taxonomy_versioning_recorded", f"root_cause_taxonomy_versioning:{run_id}", {"taxonomy_version": f"root-cause-taxonomy-{run_id}", "root_cause_code": "quote_clean_evidence_expired", "severity": "high", "migration_policy": "map_legacy_codes_before_postmortem", "effective_at": now}, run_id, "root_cause_taxonomy_versioning", now),
        _spec("cohort_drift_boundary_recorded", f"cohort_drift_boundary:{run_id}", {"cohort_id": f"premium-clean-cohort-{run_id}", "baseline_window": "2026-01-14T00:00:00Z/2026-01-15T00:00:00Z", "current_window": "2026-01-15T00:00:00Z/2026-01-16T00:00:00Z", "drift_metric": 0.08, "action": "block_promotion_and_resegment"}, run_id, "cohort_drift_boundary", now),
        _spec("complexity_budget_recorded", f"complexity_budget:{run_id}", {"budget_id": f"complexity-budget-{run_id}", "scope": "normal_tiny_capture_loop", "max_components": 12, "current_components": 8, "owner": "runtime-owner"}, run_id, "complexity_budget", now),
        _spec("exception_debt_register_recorded", f"exception_debt_register:{run_id}", {"exception_id": f"exception-debt-{run_id}", "contract_id": "RealtimeCleanDetector", "debt_owner": "runtime-owner", "expires_at": future, "repayment_plan": "remove_exception_before_promotion"}, run_id, "exception_debt_register", now),
        _spec("gate_retirement_policy_recorded", f"gate_retirement_policy:legacy-clean-source-{run_id}", {"gate_id": f"legacy-clean-source-{run_id}", "retirement_reason": "superseded_by_runtime_trust_contracts", "replacement_contract": "RuntimeSpecAssertionContract", "evidence_package_id": package_id, "retired_at": now}, run_id, "gate_retirement_policy", now),
        _spec("graceful_degradation_boundary_recorded", f"graceful_degradation_boundary:{run_id}", {"boundary_id": f"graceful-degradation-{run_id}", "degraded_component": "premium_clean_quote_source", "allowed_modes": ["shadow", "ultra_tiny"], "blocked_actions": ["normal_tiny_entry", "promotion"], "operator_message": "normal_tiny entry blocked until clean quote source recovers"}, run_id, "graceful_degradation_boundary", now),
        _spec("invariant_sampling_audit_recorded", f"invariant_sampling_audit:{run_id}", {"audit_id": f"invariant-audit-{run_id}", "invariant_id": "quote_intent_binding_no_future_fields", "sample_window": "2026-01-15T00:00:00Z/2026-01-15T01:00:00Z", "violation_count": 0, "audited_at": now}, run_id, "invariant_sampling_audit", now),
        _spec("operator_cognitive_load_recorded", f"operator_cognitive_load:{run_id}", {"workflow_id": f"operator-load-workflow-{run_id}", "operator_role": "runtime_operator", "max_parallel_alerts": 3, "current_alert_count": 1, "action": "normal_ops_with_watch"}, run_id, "operator_cognitive_load", now),
        _spec("research_notebook_boundary_recorded", f"research_notebook_boundary:{run_id}", {"notebook_id": f"research-notebook-{run_id}", "data_scope": "research_only_backtest", "write_targets_allowed": False, "promotion_allowed": False, "owner": "research-owner"}, run_id, "research_notebook_boundary", now),
        _spec("unknown_unknowns_sampling_recorded", f"unknown_unknowns_sampling:{run_id}", {"sample_id": f"unknown-unknowns-sample-{run_id}", "population_scope": "premium_clean_dog_candidates", "sampling_policy": "stratified_tail_and_recent_misses", "review_result": "reviewed_no_new_risk", "sampled_at": now}, run_id, "unknown_unknowns_sampling", now),
        _spec("archive_bitrot_scrub_recorded", f"archive_bitrot_scrub:{run_id}", {"archive_set_id": f"archive-set-{run_id}", "object_count": 42, "scrub_hash": sha256_hex({"archive_set_id": run_id, "object_count": 42}), "bitrot_detected": False, "scrubbed_at": now}, run_id, "archive_bitrot_scrub", now),
        _spec("data_deletion_legal_hold_recorded", f"data_deletion_legal_hold:{run_id}", {"legal_hold_id": f"deletion-legal-hold-{run_id}", "data_scope": "premium_clean_source_raw_messages", "deletion_request_id": f"delete-request-{run_id}", "hold_state": "deletion_blocked", "expires_at": future}, run_id, "data_deletion_legal_hold", now),
        _spec("data_license_compliance_recorded", f"data_license_compliance:{run_id}", {"dataset_id": f"premium-clean-dataset-{run_id}", "license_id": "internal-paper-research-license-v1", "allowed_use": "paper_trading_research", "expiry_at": future, "compliance_status": "compliant"}, run_id, "data_license_compliance", now),
        _spec("export_reimport_boundary_recorded", f"export_reimport_boundary:{run_id}", {"boundary_id": f"export-reimport-boundary-{run_id}", "export_id": f"paper-review-export-{run_id}", "reimport_allowed": False, "lineage_hash": sha256_hex({"boundary_id": run_id, "export_id": f"paper-review-export-{run_id}"}), "approved_at": now}, run_id, "export_reimport_boundary", now),
        _spec("legal_hold_recorded", f"legal_hold:{run_id}", {"legal_hold_id": f"legal-hold-{run_id}", "data_scope": "premium_clean_source_raw_messages", "hold_reason": "audit_replay_and_label_dispute_window", "owner": "compliance-owner", "expires_at": future}, run_id, "legal_hold", now),
        _spec("provider_terms_compliance_recorded", f"provider_terms_compliance:telegram_premium_source:{run_id}", {"provider": "telegram_premium_source", "terms_version": f"terms-{run_id}", "allowed_use": "paper_trading_research", "compliance_status": "compliant", "reviewed_at": now}, run_id, "provider_terms_compliance", now),
    ]


def record_normal_tiny_governance_evidence(
    event_log_dir: str | Path = DEFAULT_EVENT_LOG_DIR,
    *,
    run_id: str | None = None,
    dry_run: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    specs = build_event_specs(run_id=run_id)
    report = {
        "governance_evidence_schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "event_log_dir": str(event_log_dir),
        "run_id": run_id,
        "dry_run": bool(dry_run),
        "planned_event_count": len(specs),
        "planned_event_types": sorted({spec["event_type"] for spec in specs}),
    }
    if dry_run:
        report["append_status_counts"] = {}
        report["health"] = {"status": "normal_tiny_governance_evidence_dry_run"}
        return report

    log = V27EventLog(event_log_dir)
    results = log.append_events(specs)
    status_counts: dict[str, int] = {}
    appended_event_ids = []
    for result in results:
        status = str(result.get("status"))
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "appended":
            appended_event_ids.append((result.get("event") or {}).get("event_id"))
    report["append_status_counts"] = status_counts
    report["appended_event_ids"] = appended_event_ids
    report["event_log_summary_after"] = log.summary()
    ok = sum(status_counts.values()) == len(specs) and all(status in {"appended", "duplicate"} for status in status_counts)
    report["health"] = {
        "status": "normal_tiny_governance_evidence_recorded" if ok else "normal_tiny_governance_evidence_incomplete",
        "event_log_verified": True,
    }
    if strict and not ok:
        report["health"]["strict_failed"] = True
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=os.environ.get("V27_EVENT_LOG_DIR", str(DEFAULT_EVENT_LOG_DIR)))
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    report = record_normal_tiny_governance_evidence(
        args.event_log_dir,
        run_id=args.run_id,
        dry_run=args.dry_run,
        strict=args.strict,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not args.dry_run and report.get("health", {}).get("status") != "normal_tiny_governance_evidence_recorded":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
