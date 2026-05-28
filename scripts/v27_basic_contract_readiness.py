#!/usr/bin/env python3
"""Verify v2.7 observe-only foundation contracts from local machine evidence."""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_mirror_telegram_signals import _signal_payload  # noqa: E402
from v27_paper_mode_safety import build_paper_mode_safety_boundary  # noqa: E402
from v27_spec_validate import CATALOG_PATH, ENTRY_MODE_REGISTRY_PATH, MANIFEST_PATH, validate_all  # noqa: E402


DEFAULT_CHAIN_CONFIG = PROJECT_ROOT / "config" / "v27-chain-config.json"
DEFAULT_SOURCE_REGISTRY = PROJECT_ROOT / "config" / "v27-source-registry.json"
DEFAULT_SOURCE_PARSER_AUTH_POLICY = PROJECT_ROOT / "config" / "v27-source-parser-auth-policy.json"
DEFAULT_SHADOW_OBSERVATION_IDENTITY_POLICY = PROJECT_ROOT / "config" / "v27-shadow-observation-identity-policy.json"
DEFAULT_CHANNELS_CSV = PROJECT_ROOT / "config" / "channels.csv"
DEFAULT_SYSTEM_CONFIG = PROJECT_ROOT / "config" / "system.config.json"
DEFAULT_ENTRY_MODE_REGISTRY = PROJECT_ROOT / "config" / "entry-mode-registry.json"
DEFAULT_GOVERNANCE_READINESS = PROJECT_ROOT / "config" / "v27-governance-readiness.json"
DEFAULT_ACCESS_CONTROL_POLICY = PROJECT_ROOT / "config" / "v27-access-control-policy.json"
DEFAULT_WRITE_PATH_REGISTRY = PROJECT_ROOT / "config" / "v27-write-path-registry.json"
DEFAULT_DIRECT_DB_MUTATION_POLICY = PROJECT_ROOT / "config" / "v27-direct-database-mutation-policy.json"
DEFAULT_AGGREGATE_BOUNDARIES = PROJECT_ROOT / "config" / "v27-aggregate-boundaries.json"
DEFAULT_EVENT_SCHEMA_COMPATIBILITY = PROJECT_ROOT / "config" / "v27-event-schema-compatibility.json"
DEFAULT_READ_MODEL_SNAPSHOT_POLICY = PROJECT_ROOT / "config" / "v27-read-model-snapshot-policy.json"
DEFAULT_RUNTIME_WORKER_HEALTH_POLICY = PROJECT_ROOT / "config" / "v27-runtime-worker-health-policy.json"
DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY = PROJECT_ROOT / "config" / "v27-db-runtime-concurrency-policy.json"
DEFAULT_BACKGROUND_JOB_REGISTRY = PROJECT_ROOT / "config" / "v27-background-job-registry.json"
DEFAULT_ENTRY_POINT_INVENTORY = PROJECT_ROOT / "config" / "v27-entry-point-inventory.json"
DEFAULT_STATIC_POLICY_ENFORCEMENT = PROJECT_ROOT / "config" / "v27-static-policy-enforcement.json"
DEFAULT_FEATURE_FLAG_DEPENDENCIES = PROJECT_ROOT / "config" / "v27-feature-flag-dependencies.json"
DEFAULT_FILESYSTEM_PRESSURE_POLICY = PROJECT_ROOT / "config" / "v27-filesystem-pressure-policy.json"
DEFAULT_API_RESPONSE_POLICY = PROJECT_ROOT / "config" / "v27-api-response-policy.json"
DEFAULT_API_RESPONSE_ENVELOPE_POLICY = PROJECT_ROOT / "config" / "v27-api-response-envelope-policy.json"
DEFAULT_ERROR_TAXONOMY = PROJECT_ROOT / "config" / "v27-error-taxonomy.json"
DEFAULT_LOG_REDACTION_POLICY = PROJECT_ROOT / "config" / "v27-log-redaction-policy.json"
DEFAULT_SERVICE_READINESS_PROBES = PROJECT_ROOT / "config" / "v27-service-readiness-probes.json"
DEFAULT_DASHBOARD_ACTION_SEPARATION_POLICY = PROJECT_ROOT / "config" / "v27-dashboard-action-separation-policy.json"
DEFAULT_NUMERIC_PRECISION_POLICY = PROJECT_ROOT / "config" / "v27-numeric-precision-policy.json"
DEFAULT_METRIC_DEFINITION_REGISTRY = PROJECT_ROOT / "config" / "v27-metric-definition-registry.json"
DEFAULT_THRESHOLD_CATALOG = PROJECT_ROOT / "config" / "v27-threshold-catalog.json"
DEFAULT_RUNTIME_CONFIG_DRIFT_POLICY = PROJECT_ROOT / "config" / "v27-runtime-config-drift-policy.json"
DEFAULT_ENVIRONMENT_SEPARATION_POLICY = PROJECT_ROOT / "config" / "v27-environment-separation-policy.json"
DEFAULT_NULL_VALUE_POLICY = PROJECT_ROOT / "config" / "v27-null-value-policy.json"
DEFAULT_FEATURE_VECTOR_SNAPSHOT_POLICY = PROJECT_ROOT / "config" / "v27-feature-vector-snapshot-policy.json"
DEFAULT_TRAINING_DATASET_MANIFEST_POLICY = PROJECT_ROOT / "config" / "v27-training-dataset-manifest-policy.json"
DEFAULT_DATA_LINEAGE_GRAPH_POLICY = PROJECT_ROOT / "config" / "v27-data-lineage-graph-policy.json"
DEFAULT_DETECTOR_SHADOW_CALIBRATION_POLICY = PROJECT_ROOT / "config" / "v27-detector-shadow-calibration-policy.json"
DEFAULT_CAPACITY_LOAD_LATENCY_POLICY = PROJECT_ROOT / "config" / "v27-capacity-load-latency-policy.json"
DEFAULT_OPERATOR_RUNTIME_SAFETY_POLICY = PROJECT_ROOT / "config" / "v27-operator-runtime-safety-policy.json"
DEFAULT_REPLAY_BUILD_MODEL_POLICY = PROJECT_ROOT / "config" / "v27-replay-build-model-policy.json"
DEFAULT_SPEC_GOVERNANCE_FEASIBILITY_POLICY = PROJECT_ROOT / "config" / "v27-spec-governance-feasibility-policy.json"
DEFAULT_IDENTITY_UNIT_PROVIDER_FINALITY_POLICY = PROJECT_ROOT / "config" / "v27-identity-unit-provider-finality-policy.json"
DEFAULT_EXECUTION_EXIT_SAFETY_POLICY = PROJECT_ROOT / "config" / "v27-execution-exit-safety-policy.json"
DEFAULT_DELIVERY_TRACEABILITY_POLICY = PROJECT_ROOT / "config" / "v27-delivery-traceability-policy.json"
DEFAULT_RELEASE_EXPERIMENT_SAFETY_POLICY = PROJECT_ROOT / "config" / "v27-release-experiment-safety-policy.json"
DEFAULT_MARKOV_LIFECYCLE_FORECAST_POLICY = PROJECT_ROOT / "config" / "v27-markov-lifecycle-forecast-policy.json"
DEFAULT_REASON_TAXONOMY_POLICY = PROJECT_ROOT / "config" / "v27-reason-taxonomy-policy.json"
DEFAULT_SECURITY_SESSION_POLICY = PROJECT_ROOT / "config" / "v27-security-session-policy.json"
DEFAULT_RUNTIME_PIPELINE_POLICY = PROJECT_ROOT / "config" / "v27-runtime-pipeline-policy.json"
DEFAULT_CI_SPEC_GENERATED_POLICY = PROJECT_ROOT / "config" / "v27-ci-spec-generated-policy.json"
ADMIN_SESSION_SECURITY_REQUIRED_FIELDS = (
    "session_id",
    "operator_id",
    "mfa_required",
    "expires_at",
    "csrf_protection",
)
SECRET_ACCESS_AUDIT_REQUIRED_FIELDS = (
    "secret_id",
    "accessor_id",
    "access_reason",
    "audit_event_id",
    "accessed_at",
)
SECRETS_MANAGEMENT_REQUIRED_FIELDS = (
    "secret_name",
    "scope",
    "rotation_interval_days",
    "last_rotated_at",
    "owner",
    "leak_detected",
    "revocation_status",
    "environment_allowed",
)
SYSTEM_SLO_REQUIRED_FIELDS = (
    "slo_id",
    "metric_id",
    "threshold_id",
    "measured_value",
    "status",
    "severity",
    "new_entry_action",
    "exit_safety_action",
)
NO_TRADE_ROOT_CAUSE_REQUIRED_FIELDS = (
    "root_cause_id",
    "root_cause_code",
    "d3a_candidate_count",
    "fill_count",
    "category",
    "owner",
    "remediation_action",
    "metric_id",
    "threshold_id",
)
RELEASE_COMPLEXITY_REQUIRED_FIELDS = (
    "release_id",
    "max_new_gates_per_release",
    "new_gates",
    "max_new_detectors_per_release",
    "new_detectors",
    "required_shadow_hours_before_gate",
    "observed_shadow_hours",
    "rollback_metric",
    "status",
    "metric_id",
    "threshold_id",
)
BACKPRESSURE_POLICY_REQUIRED_FIELDS = (
    "component",
    "queue_depth",
    "max_queue_depth",
    "backpressure_action",
    "drops_p0_p1_allowed",
    "exit_safety_priority",
    "metric_id",
    "threshold_id",
)
BUDGET_RESERVE_REQUIRED_FIELDS = (
    "reserve_id",
    "budget_pool",
    "reserved_for",
    "reserved_amount",
    "current_usage",
    "hard_limit",
    "priority_class",
    "borrow_allowed",
    "metric_id",
    "threshold_id",
)
BLINDED_HOLDOUT_REQUIRED_FIELDS = (
    "holdout_id",
    "window_id",
    "blinded",
    "access_count",
    "no_retune_enforced",
    "contamination_status",
    "promotion_evidence_allowed",
    "metric_id",
    "threshold_id",
)
MANUAL_OVERRIDE_REQUIRED_FIELDS = (
    "override_id",
    "operator_id",
    "action",
    "quarantine_required",
    "promotion_evidence_allowed",
    "training_allowed",
    "audit_event_id",
    "approval_status",
    "metric_id",
    "threshold_id",
)
CONTRACT_TEST_SUITE_REQUIRED_FIELDS = (
    "suite_id",
    "contract_id",
    "test_command",
    "pass_fail",
    "coverage_class",
    "evidence_hash",
    "metric_id",
    "threshold_id",
)
ADVERSARIAL_REPLAY_SUITE_REQUIRED_FIELDS = (
    "replay_id",
    "scenario",
    "expected_action",
    "observed_action",
    "machine_checked",
    "pass_fail",
    "criticality",
    "metric_id",
    "threshold_id",
)
MARKOV_CENSORING_POLICY_REQUIRED_FIELDS = (
    "censoring_policy_version",
    "outcome_status",
    "censoring_reason",
    "training_weight_policy",
)
MARKOV_WALK_FORWARD_REQUIRED_FIELDS = (
    "validation_id",
    "cutoff_seq",
    "train_window_id",
    "no_lookahead_proof",
)
MARKOV_HMM_BOUNDARY_REQUIRED_FIELDS = (
    "artifact_id",
    "research_only",
    "online_filtering_only",
    "full_sequence_viterbi_allowed",
)
TELEGRAM_SESSION_SECURITY_REQUIRED_FIELDS = (
    "session_id",
    "account_id",
    "auth_state",
    "device_fingerprint_hash",
    "checked_at",
)
PARSER_AMBIGUITY_REQUIRED_FIELDS = (
    "message_id",
    "candidate_anchors",
    "selected_anchor",
    "ambiguity_reason",
)
PARSER_CANARY_CORPUS_REQUIRED_FIELDS = (
    "corpus_id",
    "parser_version",
    "canary_case_count",
    "failure_count",
    "checked_at",
)
TELEGRAM_FORWARDED_MESSAGE_REQUIRED_FIELDS = (
    "message_id",
    "forwarded_from",
    "source_policy",
    "trust_level",
    "action",
)
PREMIUM_SOURCE_ACCESS_REQUIRED_FIELDS = (
    "source_id",
    "access_probe_id",
    "auth_state",
    "last_success_at",
    "failure_action",
)
SOURCE_AUTHENTICITY_REQUIRED_FIELDS = (
    "source_id",
    "channel_id",
    "authenticity_status",
    "evidence_hash",
)
PARSER_CONFUSABLES_REQUIRED_FIELDS = (
    "message_id",
    "confusable_token",
    "normalized_token",
    "risk_class",
    "policy_action",
)
IMAGE_OCR_SIGNAL_REQUIRED_FIELDS = (
    "message_id",
    "ocr_engine_version",
    "image_hash",
    "confidence",
    "policy_action",
)
SOURCE_IMPERSONATION_REQUIRED_FIELDS = (
    "source_id",
    "message_id",
    "impersonation_signal",
    "confidence",
    "action",
)
IDENTITY_MERGE_SPLIT_REQUIRED_FIELDS = (
    "merge_split_id",
    "old_identity_key",
    "new_identity_key",
    "resolution_reason",
)
REKEYING_REQUIRED_FIELDS = (
    "old_key",
    "new_key",
    "rekey_reason",
    "supersedes_event_id",
)
SOURCE_GAP_BACKFILL_REQUIRED_FIELDS = (
    "backfill_id",
    "source_id",
    "gap_window",
    "allowed_fields",
    "backfilled_at",
)
OBSERVATION_POLICY_REQUIRED_FIELDS = (
    "observation_id",
    "observation_policy_version",
    "allowed_sources",
    "forbidden_fields",
    "recorded_at",
)
COUNTERFACTUAL_ENTRY_TIME_REQUIRED_FIELDS = (
    "counterfactual_entry_ts",
    "counterfactual_policy_version",
    "counterfactual_model_snapshot_id",
)
QUEUE_ACK_NACK_REQUIRED_FIELDS = (
    "queue_id",
    "task_id",
    "ack_state",
    "nack_reason",
    "recorded_at",
)
PIPELINE_PROGRESS_REQUIRED_FIELDS = (
    "pipeline_id",
    "stage_name",
    "max_stall_ms",
    "last_progress_at",
    "stall_action",
)
THREAD_POOL_ISOLATION_REQUIRED_FIELDS = (
    "pool_name",
    "workload_class",
    "max_workers",
    "reserved_capacity",
    "checked_at",
)
CICD_MERGE_GATE_REQUIRED_FIELDS = (
    "merge_gate_id",
    "required_checks",
    "spec_hash",
    "artifact_hash",
    "gate_result",
)
GENERATED_CLIENT_REQUIRED_FIELDS = (
    "client_name",
    "source_schema_hash",
    "generated_artifact_hash",
    "generation_tool_version",
    "checked_at",
)
SPEC_CHANGE_IMPACT_REQUIRED_FIELDS = (
    "spec_change_id",
    "affected_contracts",
    "affected_modes",
    "impact_hash",
    "approved_at",
)
NUMERIC_PRECISION_REQUIRED_FIELDS = (
    "unit",
    "decimal_scale",
    "rounding_mode",
    "overflow_policy",
)
NUMERIC_PRECISION_REQUIRED_UNITS = {
    "basis_points",
    "market_cap_usd",
    "percentage",
    "price_quote",
    "sol",
    "token_base_units",
    "unix_ms",
}
NUMERIC_PRECISION_ROUNDING = {
    "ROUND_DOWN": ROUND_DOWN,
    "ROUND_HALF_EVEN": ROUND_HALF_EVEN,
    "ROUND_HALF_UP": ROUND_HALF_UP,
}
NUMERIC_PRECISION_OVERFLOW_POLICIES = {"reject", "fail_closed"}
METRIC_DEFINITION_REQUIRED_FIELDS = (
    "metric_id",
    "metric_name",
    "formula",
    "numerator_definition",
    "denominator_definition",
    "window_id",
    "event_time_basis",
    "inclusion_criteria",
    "exclusion_criteria",
    "late_event_policy",
    "partial_window_policy",
    "unit",
    "owner",
    "spec_section_id",
    "metric_version",
    "metric_hash",
)
METRIC_EVENT_TIME_BASIS = {
    "decision_available_at",
    "matrix_build_cutoff_seq",
    "position_closed_at",
    "simulated_fill_ts",
    "snapshot_collected_at",
}
THRESHOLD_CATALOG_REQUIRED_FIELDS = (
    "threshold_id",
    "threshold_name",
    "threshold_value",
    "unit",
    "comparison_operator",
    "scope",
    "applies_to_metric",
    "applies_to_mode",
    "owner",
    "source_spec_section_id",
    "policy_bundle_id",
    "effective_from",
    "effective_to",
    "change_reason",
    "approval_id",
    "threshold_hash",
)
THRESHOLD_COMPARISON_OPERATORS = {">=", ">", "<=", "<", "==", "!="}
THRESHOLD_ALLOWED_MODES = {"observe_only", "shadow", "ultra_tiny", "normal_tiny", "all_modes"}
RUNTIME_CONFIG_DRIFT_REQUIRED_FIELDS = (
    "runtime_config_hash",
    "env_vars_hash",
    "feature_flags_hash",
    "provider_config_hash",
    "route_registry_hash",
    "source_registry_hash",
    "threshold_catalog_hash",
    "metric_registry_hash",
    "policy_bundle_hash",
    "loaded_at",
    "expected_hash",
    "drift_detected",
    "drift_action",
)
RUNTIME_CONFIG_DRIFT_ACTIONS = {
    "block_new_promotion_and_revalidate_before_execution",
    "shadow_only",
    "global_circuit_breaker",
}
ENVIRONMENT_SEPARATION_REQUIRED_FIELDS = (
    "environment_id",
    "environment_type",
    "allowed_event_logs",
    "allowed_databases",
    "allowed_provider_keys",
    "allowed_routes",
    "allowed_modes",
    "write_permissions",
    "read_permissions",
    "data_export_allowed",
    "promotion_allowed",
    "environment_hash",
)
ENVIRONMENT_TYPES = {
    "local_dev",
    "research",
    "shadow",
    "paper",
    "backfill_research",
    "dashboard_readonly",
    "operator_admin",
    "live_prohibited",
}
NULL_VALUE_POLICY_REQUIRED_FIELDS = (
    "field_name",
    "null_class",
    "allowed_in_modes",
    "default_value_allowed",
    "imputation_policy",
    "training_allowed",
    "decision_allowed",
    "dashboard_display",
    "owner",
    "policy_version",
    "policy_hash",
)
NULL_VALUE_CLASSES = {
    "missing_not_observed",
    "not_applicable",
    "provider_unknown",
    "parse_failed",
    "delayed_unavailable",
    "redacted",
    "invalid",
    "true_zero",
}
REQUIRED_NULL_POLICY_FIELDS = {
    "critical_risk_status",
    "entry_quote_price",
    "exit_quote_price",
    "reference_price",
    "token_identity_confidence",
    "feature_available_at",
    "gmgn_risk_status",
}
FEATURE_AVAILABILITY_REQUIRED_FIELDS = (
    "feature_name",
    "feature_window_start",
    "feature_window_end",
    "feature_available_at",
    "decision_available_at",
    "label_available_at",
    "feature_source",
    "feature_research_only",
    "null_policy_field",
    "availability_hash",
)
FEATURE_VECTOR_SNAPSHOT_REQUIRED_FIELDS = (
    "feature_vector_hash",
    "feature_names_ordered",
    "feature_values_serialized",
    "missing_value_policy",
    "normalization_version",
    "model_input_schema_version",
    "decision_ts",
    "feature_available_at_map",
    "source_lineage_node_ids",
)
TRAINING_DATASET_MANIFEST_REQUIRED_FIELDS = (
    "dataset_id",
    "event_log_hash_range",
    "included_sample_ids",
    "excluded_sample_ids",
    "exclusion_reasons",
    "label_versions",
    "feature_versions",
    "observation_weights_hash",
    "created_at",
    "build_hash",
    "spec_hash",
    "metric_registry_hash",
    "threshold_catalog_hash",
    "manifest_hash",
)
DATA_LINEAGE_NODE_REQUIRED_FIELDS = (
    "lineage_node_id",
    "node_type",
    "source_id",
    "source_hash",
    "parent_node_ids",
    "edge_type",
    "created_at",
    "environment_id",
    "spec_hash",
    "build_hash",
    "lineage_hash",
)
REQUIRED_DATA_LINEAGE_NODE_TYPES = {
    "raw_telegram_message",
    "parsed_signal",
    "token_identity",
    "quote_event",
    "feature_snapshot",
    "feature_vector",
    "forecast",
    "decision",
    "execution_event",
    "ledger_event",
    "outcome_label",
    "metric_value",
    "dashboard_panel",
    "training_dataset_manifest",
}
DETECTOR_SHADOW_REQUIRED_FIELDS = (
    "detector_id",
    "contract_id",
    "detector_name",
    "detector_version",
    "detector_output_states",
    "allowed_modes",
    "gate_allowed",
    "threshold_ids",
    "metric_ids",
    "required_feature_available_at_fields",
    "feature_available_at_required",
    "source_event_type",
    "failure_action",
    "detector_hash",
)
DETECTOR_CALIBRATION_REQUIRED_FIELDS = (
    "calibration_id",
    "detector_id",
    "metric_id",
    "threshold_id",
    "window_id",
    "sample_n",
    "observed_value",
    "comparison_operator",
    "threshold_value",
    "calibration_status",
    "promotion_allowed",
    "contaminated_sample_count",
    "checked_at",
    "calibration_hash",
)
DETECTOR_SHADOW_CONTRACTS = {
    "reclaim_detector": {
        "contract_id": "ReclaimDetector",
        "required_outputs": {
            "RECLAIM_FORMING_OBSERVED",
            "RECLAIM_CONFIRMED_OBSERVED",
            "RECLAIM_FAILED_OBSERVED",
        },
        "blocking_reason": "reclaim_detector_missing_malformed_or_unsafe",
    },
    "overextension_detector": {
        "contract_id": "OverextensionDetector",
        "required_outputs": {
            "OVEREXTENDED_OBSERVED",
            "NOT_OVEREXTENDED_OBSERVED",
            "LATE_CLEAN_NOT_ACTIONABLE",
        },
        "blocking_reason": "overextension_detector_missing_malformed_or_unsafe",
    },
}
DETECTOR_CALIBRATION_STATUSES = {"shadow_healthy", "shadow_insufficient", "shadow_failed"}
CAPACITY_PLAN_REQUIRED_FIELDS = (
    "capacity_plan_id",
    "component",
    "expected_peak_qps",
    "measured_peak_qps",
    "p95_latency_budget_ms",
    "p99_latency_budget_ms",
    "queue_depth_limit",
    "headroom_pct",
    "degradation_threshold",
    "owner",
    "last_verified_at",
    "metric_id",
    "threshold_id",
    "protects_priorities",
    "exit_safety_reserved",
    "capacity_hash",
)
LOAD_TEST_REPLAY_REQUIRED_FIELDS = (
    "load_test_id",
    "event_log_hash",
    "replay_speed_multiplier",
    "synthetic_burst_profile",
    "components_under_test",
    "expected_invariants",
    "observed_invariants",
    "pass_fail",
    "run_at",
    "build_hash",
    "runtime_config_hash",
    "metric_id",
    "threshold_id",
    "load_test_hash",
)
LATENCY_ATTRIBUTION_REQUIRED_FIELDS = (
    "token_lifecycle_key",
    "signal_seen_at",
    "signal_available_at",
    "pool_resolved_available_at",
    "quote_available_at",
    "risk_available_at",
    "reclaim_available_at",
    "decision_started_at",
    "decision_available_at",
    "queued_at",
    "claimed_at",
    "quote_refreshed_at",
    "simulated_fill_ts",
    "peak_ts",
    "latency_class",
    "latency_ms",
    "blocking_component",
    "owner",
    "metric_id",
    "threshold_id",
    "latency_hash",
)
PROVIDER_QUOTA_ISOLATION_REQUIRED_FIELDS = (
    "provider",
    "budget_pool",
    "priority_order",
    "quota_limit_per_min",
    "current_usage_per_min",
    "exit_safety_reserved_pct",
    "shadow_polling_limit_per_min",
    "quota_isolation_status",
    "metric_id",
    "threshold_id",
    "quota_hash",
)
ECONOMIC_COST_BUDGET_REQUIRED_FIELDS = (
    "budget_id",
    "budget_pool",
    "owner",
    "reserved_for",
    "soft_limit",
    "hard_limit",
    "current_usage",
    "measurement_window_ms",
    "exit_safety_reserved",
    "metric_id",
    "threshold_id",
    "budget_hash",
)
REQUIRED_CAPACITY_COMPONENTS = {
    "telegram_ingest",
    "quote_polling",
    "risk_fetch",
    "event_log_write",
    "outbox_publish",
    "projection_consumer",
    "read_model_update",
    "forecast_builder",
    "decision_arbiter",
    "entry_executor",
    "exit_executor",
    "dashboard_query",
    "alert_delivery",
}
REQUIRED_LOAD_TEST_SCENARIOS = {
    "telegram_burst",
    "quote_provider_slow",
    "provider_429",
    "dlq_poison_event",
    "projection_lag",
    "read_model_lag",
    "exit_queue_burst",
    "outbox_lag",
    "worker_crash_during_entry",
    "worker_crash_during_exit",
}
LATENCY_CLASSES = {
    "source_latency",
    "ingestion_latency",
    "parse_latency",
    "pool_resolution_latency",
    "provider_quote_latency",
    "risk_provider_latency",
    "read_model_latency",
    "forecast_latency",
    "decision_latency",
    "queue_latency",
    "execution_latency",
    "projection_latency",
    "dashboard_latency",
}
OPERATOR_AUDIT_REQUIRED_FIELDS = (
    "audit_event_id",
    "operator_id",
    "action",
    "before_value",
    "after_value",
    "reason",
    "ticket_or_experiment_id",
    "timestamp",
    "approval_required",
    "approval_status",
    "environment_id",
    "metric_id",
    "threshold_id",
    "observed_value",
    "operator_audit_hash",
)
OPERATOR_SAFETY_REQUIRED_FIELDS = (
    "safety_check_id",
    "operator_id",
    "action",
    "danger_level",
    "environment_id",
    "dashboard_freshness_ok",
    "required_runbook_ack",
    "required_second_approval",
    "cooldown_ok",
    "blast_radius_preview",
    "confirmation_phrase_required",
    "operator_safety_status",
    "audit_event_id",
    "metric_id",
    "threshold_id",
    "observed_value",
    "safety_hash",
)
OWNERSHIP_ONCALL_REQUIRED_FIELDS = (
    "component",
    "owner",
    "oncall_primary",
    "oncall_secondary",
    "escalation_path",
    "runbook_url",
    "ack_sla_minutes",
    "resolution_sla_minutes",
    "last_reviewed_at",
    "metric_id",
    "threshold_id",
    "observed_value",
    "ownership_hash",
)
ALERT_POLICY_REQUIRED_FIELDS = (
    "alert_id",
    "severity",
    "trigger_condition",
    "auto_action",
    "owner_component",
    "runbook_id",
    "notification_channel_id",
    "metric_id",
    "threshold_id",
    "observed_value",
    "alert_hash",
)
ALERT_ACK_ESCALATION_REQUIRED_FIELDS = (
    "alert_instance_id",
    "alert_id",
    "severity",
    "created_at",
    "ack_required_by",
    "acked_at",
    "acked_by",
    "escalation_target",
    "escalation_level",
    "auto_action_taken",
    "resolved_at",
    "resolution_note",
    "ack_sla_met",
    "metric_id",
    "threshold_id",
    "observed_value",
    "alert_ack_hash",
)
KILL_SWITCH_DRILL_REQUIRED_FIELDS = (
    "drill_id",
    "kill_switch_type",
    "target_scope",
    "initiated_by",
    "started_at",
    "completed_at",
    "expected_effect",
    "observed_effect",
    "open_positions_policy_checked",
    "exit_safety_preserved",
    "new_entry_blocked",
    "recovery_steps_verified",
    "pass_fail",
    "metric_id",
    "threshold_id",
    "observed_value",
    "evidence_hash",
)
OPERATOR_DANGER_LEVELS = {"read", "mutation", "admin_mutation", "critical"}
OPERATOR_SAFETY_STATUSES = {"safe", "rejected_safe"}
REQUIRED_OWNERSHIP_COMPONENTS = {
    "event_log",
    "outbox",
    "dlq",
    "projection",
    "read_model",
    "dashboard",
    "entry_executor",
    "exit_executor",
    "position_ledger",
    "capital_ledger",
    "provider_quota",
    "source_registry",
    "route_registry",
    "metric_registry",
    "threshold_catalog",
    "runtime_config",
    "paper_live_safety",
    "holdout",
    "operator_access",
}
REQUIRED_ALERT_IDS = {
    "global_circuit_breaker_triggered",
    "duplicate_execution_detected",
    "stale_state_execution_detected",
    "paper_live_boundary_breach",
    "outbox_stuck_critical",
    "DLQ_critical_unresolved",
    "projection_ordering_critical",
    "read_model_stale_critical",
    "canonical_spec_hash_mismatch",
    "metric_registry_critical_mismatch",
    "threshold_catalog_critical_mismatch",
    "runtime_config_unauthorized_drift",
    "environment_contamination_critical",
    "operator_unsafe_override_critical",
    "kill_switch_failed",
    "no_trade_2h_with_D3a_candidates",
    "fast_lane_p1_sla_breach",
    "capacity_headroom_low",
    "load_test_replay_failed",
    "latency_attribution_spike",
}
REQUIRED_KILL_SWITCH_TYPES = {
    "global_circuit_breaker",
    "route_kill_switch",
    "source_kill_switch",
    "provider_kill_switch",
    "model_kill_switch",
    "feature_flag_rollback",
    "paper_live_safety_breaker",
    "entry_disable_exit_only",
}
REPLAY_DETERMINISM_REQUIRED_FIELDS = (
    "replay_check_id",
    "event_log_hash",
    "policy_manifest_hash",
    "model_snapshot_id",
    "threshold_catalog_hash",
    "metric_registry_hash",
    "runtime_config_hash",
    "feature_code_version",
    "build_hash",
    "spec_hash",
    "decision_hash",
    "forecast_hash",
    "outcome_hash",
    "ledger_hash",
    "feature_vector_hash",
    "pass_fail",
    "metric_id",
    "threshold_id",
    "observed_value",
    "replay_hash",
)
REPRODUCIBLE_BUILD_REQUIRED_FIELDS = (
    "build_id",
    "code_commit_hash",
    "dependency_lock_hash",
    "runtime_version",
    "container_image_hash",
    "feature_code_hash",
    "model_code_hash",
    "build_created_at",
    "reproducible_build_hash",
    "metric_id",
    "threshold_id",
    "observed_value",
)
SUPPLY_CHAIN_ARTIFACT_REQUIRED_FIELDS = (
    "artifact_id",
    "artifact_type",
    "code_commit_hash",
    "dependency_lock_hash",
    "container_image_hash",
    "signature_status",
    "SBOM_hash",
    "vulnerability_scan_status",
    "provenance_attestation",
    "approved_builder",
    "created_at",
    "promoted_by",
    "metric_id",
    "threshold_id",
    "observed_value",
    "artifact_hash",
)
POLICY_BUNDLE_COMPATIBILITY_REQUIRED_FIELDS = (
    "policy_bundle_id",
    "model_snapshot_id",
    "state_definition_version",
    "feature_schema_version",
    "label_contract_version",
    "source_dog_label_version",
    "trade_outcome_label_version",
    "reference_price_version",
    "stop_contract_version",
    "threshold_version",
    "metric_registry_version",
    "exit_policy_version",
    "runtime_config_version",
    "compatibility_status",
    "metric_id",
    "threshold_id",
    "observed_value",
    "compatibility_hash",
)
MODEL_EXPIRY_REQUIRED_FIELDS = (
    "model_snapshot_id",
    "trained_until",
    "max_age_minutes",
    "min_recent_samples",
    "recent_sample_count",
    "expiry_ts",
    "checked_at",
    "expired_action_cap",
    "model_expiry_status",
    "metric_id",
    "threshold_id",
    "observed_value",
    "model_expiry_hash",
)
FORECAST_SANITY_REQUIRED_FIELDS = (
    "forecast_id",
    "model_snapshot_id",
    "raw_forecast",
    "sanitized_forecast",
    "sanity_cap_reason",
    "forecast_sanity_version",
    "threshold_id",
    "metric_id",
    "feature_vector_hash",
    "sample_n",
    "fallback_level",
    "calibration_bucket_status",
    "data_quality_score",
    "forecast_sanity_status",
    "observed_value",
    "forecast_sanity_hash",
)
SUPPLY_CHAIN_ARTIFACT_TYPES = {
    "code",
    "container",
    "dependency_lock",
    "model_snapshot",
    "policy_bundle",
    "feature_binary",
    "dashboard_bundle",
}
MODEL_EXPIRED_ACTION_CAPS = {"shadow", "ultra_tiny"}
RENDERED_SPEC_VIEW_REQUIRED_FIELDS = (
    "rendered_view_id",
    "source_spec_hash",
    "rendered_doc_hash",
    "renderer_version",
    "rendered_at",
    "section_count",
    "missing_section_ids",
    "extra_section_ids",
    "render_validation_status",
    "metric_id",
    "threshold_id",
    "observed_value",
    "view_hash",
)
HEALTH_STATE_REQUIRED_FIELDS = (
    "health_component",
    "health_state",
    "state_reason",
    "severity",
    "first_seen_at",
    "last_seen_at",
    "blocking_modes",
    "recovery_condition",
    "source_event_id",
    "owner",
    "metric_id",
    "threshold_id",
    "observed_value",
    "health_hash",
)
CONTRACT_LIFECYCLE_REQUIRED_FIELDS = (
    "contract_id",
    "contract_version",
    "status",
    "introduced_in_version",
    "deprecated_in_version",
    "superseded_by",
    "allowed_modes",
    "migration_required",
    "backfill_required",
    "owner",
    "sunset_deadline",
    "contract_tests_status",
    "metric_id",
    "threshold_id",
    "observed_value",
    "lifecycle_hash",
)
OBJECTIVE_PRIORITY_REQUIRED_FIELDS = (
    "objective_conflict_id",
    "conflicting_objectives",
    "chosen_objective",
    "priority_rank",
    "reason",
    "policy_version",
    "operator_override_allowed",
    "metric_id",
    "threshold_id",
    "observed_value",
    "conflict_hash",
)
GOAL_CONFIDENCE_REQUIRED_FIELDS = (
    "metric_id",
    "metric_name",
    "numerator",
    "denominator",
    "min_denominator",
    "point_estimate",
    "wilson_lower_bound",
    "beta_posterior_lower_bound",
    "status",
    "metric_version",
    "window_id",
    "threshold_id",
    "observed_value",
    "confidence_hash",
)
FILL_TIME_ANCHOR_REQUIRED_FIELDS = (
    "anchor_id",
    "decision_ts",
    "decision_available_at",
    "quote_ts",
    "entry_quote_at_decision_ts",
    "simulated_fill_ts",
    "position_open_confirmed_ts",
    "fill_time_anchor_type",
    "latency_components",
    "metric_id",
    "threshold_id",
    "observed_value",
    "anchor_hash",
)
EX_ANTE_POSTHOC_FEASIBILITY_REQUIRED_FIELDS = (
    "feasibility_id",
    "ex_ante_feasible",
    "posthoc_feasible",
    "feasibility_class",
    "feasibility_policy_version",
    "system_min_decision_latency_sec",
    "system_min_entry_latency_sec",
    "feature_available_at",
    "decision_ts",
    "earliest_actionable_ts",
    "peak_ts",
    "used_future_peak_in_ex_ante",
    "ex_ante_source_fields",
    "required_inputs_available_at",
    "metric_id",
    "threshold_id",
    "observed_value",
    "feasibility_hash",
)
HEALTH_STATE_ENUM_VALUES = {
    "HEALTHY",
    "WARN",
    "DEGRADED",
    "STALE",
    "DIRTY",
    "UNAVAILABLE",
    "BLOCKED",
    "FATAL",
    "UNKNOWN",
}
CONTRACT_LIFECYCLE_STATUSES = {
    "draft",
    "research_only",
    "shadow_only",
    "gating_candidate",
    "active_gate",
    "deprecated",
    "retired",
}
OBJECTIVE_PRIORITY_RANKS = {
    "paper_live_safety": 1,
    "exit_safety": 2,
    "ledger_capital_correctness": 3,
    "data_spec_truth": 4,
    "no_duplicate_stale_execution": 5,
    "capture_quality": 6,
    "net_ev": 7,
    "exploration_learning": 8,
    "dashboard_convenience": 9,
    "roi_expansion": 10,
}
FEASIBILITY_CLASSES = {
    "physically_capturable",
    "latency_impossible",
    "provider_impossible",
    "reclaim_confirm_too_late",
    "quote_clean_after_peak",
    "telegram_signal_after_peak",
    "pool_resolution_after_peak",
    "risk_available_after_peak",
}
FORBIDDEN_EX_ANTE_FIELDS = {
    "future_peak_ts",
    "future_outcome",
    "future_clean_quote",
    "future_liquidity_recovery",
    "posthoc_label",
}
SPEC_GOVERNANCE_FEASIBILITY_CONTRACTS = {
    "RenderedSpecViewContract",
    "HealthStateEnumContract",
    "ContractLifecycleContract",
    "ObjectivePriorityContract",
    "GoalConfidenceContract",
    "FillTimeAnchorContract",
    "ExAnteVsPosthocFeasibilityContract",
}
TOKEN_IDENTITY_REQUIRED_FIELDS = (
    "identity_id",
    "chain",
    "token_ca",
    "normalized_ca",
    "checksum",
    "symbol",
    "symbol_conflict_count",
    "pool_address",
    "pool_authority",
    "quote_mint",
    "liquidity_pair_valid",
    "identity_confidence",
    "metric_id",
    "threshold_id",
    "observed_value",
    "identity_hash",
)
DATA_UNIT_REQUIRED_FIELDS = (
    "unit_id",
    "token_decimals",
    "quote_mint",
    "quote_decimals",
    "price_unit",
    "liquidity_unit",
    "market_cap_unit",
    "quote_size_sol",
    "normalized_price",
    "unit_validation_status",
    "unit_conversion_version",
    "metric_id",
    "threshold_id",
    "observed_value",
    "unit_hash",
)
CHAIN_FINALITY_REQUIRED_FIELDS = (
    "finality_id",
    "chain",
    "slot",
    "block_time",
    "commitment_level",
    "finalized_at",
    "rpc_provider",
    "rpc_consistency_check",
    "indexer_lag_sec",
    "chain_reorg_detected",
    "metric_id",
    "threshold_id",
    "observed_value",
    "finality_hash",
)
PROVIDER_SCHEMA_REQUIRED_FIELDS = (
    "provider_name",
    "schema_version",
    "required_fields",
    "optional_fields",
    "field_type_contract",
    "canary_parse_result",
    "schema_drift_detected",
    "last_schema_check_at",
    "missing_required_field_rate",
    "field_type_error_rate",
    "unexpected_enum_rate",
    "null_spike_rate",
    "value_range_anomaly",
    "metric_id",
    "threshold_id",
    "observed_value",
    "schema_hash",
)
IDENTITY_UNIT_PROVIDER_FINALITY_CONTRACTS = {
    "TokenIdentityContract",
    "DataUnitContract",
    "ChainFinalityContract",
    "ProviderSchemaContract",
}
FINALITY_COMMITMENT_ORDER = {
    "processed": 1,
    "confirmed": 2,
    "finalized": 3,
}
LIFECYCLE_STATE_MACHINE_REQUIRED_FIELDS = (
    "state_machine_id",
    "states",
    "allowed_transitions",
    "terminal_states",
    "current_state",
    "state_version_fencing_required",
    "entry_gate_requires_module_closure",
    "invalid_transition_action",
    "metric_id",
    "threshold_id",
    "observed_value",
    "state_machine_hash",
)
EXIT_EXECUTION_STATE_MACHINE_REQUIRED_FIELDS = (
    "exit_state_machine_id",
    "states",
    "allowed_transitions",
    "terminal_states",
    "open_position_state",
    "exit_quote_required",
    "lease_fencing_required",
    "state_revalidation_required",
    "exit_safety_preserved",
    "failure_events",
    "metric_id",
    "threshold_id",
    "observed_value",
    "exit_state_machine_hash",
)
EXIT_POLICY_REQUIRED_FIELDS = (
    "exit_policy_id",
    "exit_policy_version",
    "applies_to_modes",
    "take_profit_rules",
    "stop_loss_rules",
    "time_stop_rules",
    "entry_outcome_separation",
    "effective_from",
    "metric_id",
    "threshold_id",
    "observed_value",
    "exit_policy_hash",
)
CIRCUIT_BREAKER_POSITION_POLICY_REQUIRED_FIELDS = (
    "policy_id",
    "trigger_events",
    "new_entry_disabled",
    "exit_safety_remains_active",
    "open_position_policy",
    "operator_ack_required",
    "resume_condition",
    "metric_id",
    "threshold_id",
    "observed_value",
    "circuit_breaker_hash",
)
EMERGENCY_EXIT_JOURNAL_REQUIRED_FIELDS = (
    "journal_id",
    "journal_event_id",
    "position_id",
    "reason",
    "initiated_at",
    "completed_at",
    "outcome",
    "reconciled_to_ledger",
    "journal_append_only",
    "operator_audit_required",
    "metric_id",
    "threshold_id",
    "observed_value",
    "journal_hash",
)
EXIT_QUEUE_HEALTH_REQUIRED_FIELDS = (
    "queue_id",
    "exit_queue_status",
    "oldest_open_exit_age_sec",
    "max_allowed_open_exit_age_sec",
    "stuck_open_position_count",
    "exit_quote_failure_count",
    "exit_state_machine_failure_count",
    "exit_safety_budget_reserved",
    "metric_id",
    "threshold_id",
    "observed_value",
    "queue_health_hash",
)
EXECUTION_EXIT_SAFETY_CONTRACTS = {
    "LifecycleStateMachineContract",
    "ExitExecutionStateMachine",
    "ExitPolicyContract",
    "CircuitBreakerPositionPolicy",
    "EmergencyExitJournal",
    "ExitQueueHealthContract",
}
RECONCILIATION_POLICY_REQUIRED_FIELDS = (
    "reconciliation_policy_id",
    "mismatch_class",
    "repair_class",
    "auto_repair_allowed",
    "manual_review_required",
    "audit_required",
    "dashboard_surface",
    "promotion_evidence_allowed",
    "metric_id",
    "threshold_id",
    "observed_value",
    "reconciliation_hash",
)
DASHBOARD_STALENESS_REQUIRED_FIELDS = (
    "panel_name",
    "data_seq",
    "event_log_latest_seq",
    "panel_lag_sec",
    "max_allowed_panel_lag_sec",
    "stale_banner_required",
    "last_refresh_at",
    "staleness_threshold_id",
    "operator_override_allowed",
    "metric_id",
    "threshold_id",
    "observed_value",
    "panel_hash",
)
SPEC_TRACEABILITY_MATRIX_REQUIRED_FIELDS = (
    "traceability_id",
    "contract_id",
    "spec_section_id",
    "requirement",
    "implementation_module",
    "test_file",
    "dashboard_surface",
    "rollout_flag",
    "issue_id",
    "status",
    "metric_ids",
    "threshold_ids",
    "owner",
    "traceability_hash",
)
IMPLEMENTATION_ISSUE_GRAPH_REQUIRED_FIELDS = (
    "issue_id",
    "spec_section_ids",
    "dependency_ids",
    "acceptance_tests",
    "mode_readiness_target",
    "owner",
    "status",
    "metric_ids",
    "threshold_ids",
    "issue_hash",
)
MODULE_CLOSURE_REQUIRED_FIELDS = (
    "module_name",
    "input_events",
    "output_events",
    "decision_fields",
    "failure_events",
    "outcome_metrics",
    "governance_rules",
    "dashboard_surface",
    "kill_condition",
    "contract_tests",
    "owner",
    "spec_section_ids",
    "mode_readiness_target",
    "runtime_config_keys",
    "metric_ids",
    "threshold_ids",
    "module_closure_hash",
)
DECOMMISSION_POLICY_REQUIRED_FIELDS = (
    "artifact_id",
    "artifact_type",
    "status",
    "decommission_reason",
    "deprecated_at",
    "retired_at",
    "replacement_artifact_id",
    "allowed_historical_use",
    "runtime_reference_allowed",
    "training_reference_allowed",
    "new_promotion_evidence_allowed",
    "dashboard_display_policy",
    "operator_audit_required",
    "direct_entry_allowed",
    "owner",
    "metric_id",
    "threshold_id",
    "observed_value",
    "decommission_hash",
)
DELIVERY_TRACEABILITY_CONTRACTS = {
    "ReconciliationPolicyContract",
    "DashboardStalenessContract",
    "SpecTraceabilityMatrix",
    "ImplementationIssueGraphContract",
    "ModuleClosureContract",
    "DecommissionPolicyContract",
}
RELEASE_EXPERIMENT_SAFETY_CONTRACTS = {
    "SecretsManagementContract",
    "SystemSLO",
    "NoTradeRootCause",
    "ReleaseComplexityBudget",
    "BackpressurePolicy",
    "BudgetReserveContract",
    "BlindedHoldoutContract",
    "ManualOverrideContract",
    "ContractTestSuite",
    "AdversarialReplaySuite",
}
ACCESS_CONTROL_REQUIRED_FIELDS = (
    "endpoint",
    "required_role",
    "token_scope",
    "audit_log_required",
    "danger_level",
)
AUDIT_LOG_REQUIRED_FIELDS = (
    "audit_event_id",
    "prev_audit_hash",
    "audit_payload_hash",
    "audit_chain_hash",
    "created_at",
)
WRITE_PATH_REQUIRED_FIELDS = (
    "write_path_id",
    "module",
    "target_store",
    "requires_outbox",
    "owner",
)
WRITE_PATH_SOURCE_FIELDS = (
    "entry_point",
    "mutation_type",
    "mode_gate",
    "source_file",
    "source_anchor",
)
DIRECT_DB_MUTATION_REQUIRED_FIELDS = (
    "write_path_id",
    "target_store",
    "approved_mutation_path",
    "break_glass_id",
)
AGGREGATE_BOUNDARY_REQUIRED_FIELDS = (
    "aggregate_type",
    "aggregate_id_pattern",
    "sequence_scope",
    "owner_store",
)
AGGREGATE_SEQUENCE_SCOPES = {"aggregate_id", "global_and_aggregate"}
CLOCK_ROLLBACK_REQUIRED_FIELDS = (
    "clock_source",
    "wall_clock_ts",
    "monotonic_ts",
    "rollback_detected",
    "guard_action",
)
EVENT_SCHEMA_COMPATIBILITY_REQUIRED_FIELDS = (
    "event_type",
    "schema_version",
    "producer_version",
    "consumer_version",
    "compatibility_result",
)
ENUM_EVOLUTION_REQUIRED_FIELDS = (
    "enum_name",
    "old_value",
    "new_value",
    "compatibility_policy",
    "migration_action",
)
MUTATION_COMMAND_IDEMPOTENCY_REQUIRED_FIELDS = (
    "command_id",
    "idempotency_key",
    "mutation_target",
    "dedupe_hash",
    "result_hash",
)
PROJECTION_VERSION_ISOLATION_REQUIRED_FIELDS = (
    "projection_name",
    "projection_version",
    "snapshot_field",
    "isolation_key_fields",
    "consumer_action",
)
SNAPSHOT_COMPACTION_INVARIANT_REQUIRED_FIELDS = (
    "invariant_id",
    "artifact",
    "hash_field",
    "hash_source",
    "excludes_fields",
    "failure_action",
)
SNAPSHOT_READ_BARRIER_REQUIRED_FIELDS = (
    "barrier_id",
    "consumer",
    "required_checks",
    "unsafe_statuses",
    "failure_action",
)
WORKER_HEARTBEAT_REQUIRED_FIELDS = (
    "event_type",
    "required_roles",
    "required_payload_fields",
    "projection_health_key",
    "max_heartbeat_lag_ms",
    "failure_action",
)
SILENT_WORKER_DEATH_REQUIRED_FIELDS = (
    "job_name",
    "pid_env",
    "detection_anchor",
    "restart_action",
)
WARM_START_CONTROL_REQUIRED_FIELDS = (
    "control_id",
    "source_file",
    "source_anchor",
    "protected_paths",
    "failure_action",
)
CONNECTION_POOL_PARTITION_REQUIRED_FIELDS = (
    "pool_name",
    "partition_key",
    "max_connections",
    "critical_reserved_connections",
    "checked_at",
)
DB_LOCK_CONTENTION_REQUIRED_FIELDS = (
    "store",
    "lock_name",
    "contention_threshold_ms",
    "retry_policy",
    "fallback_action",
)
DATABASE_TRANSACTION_ISOLATION_REQUIRED_FIELDS = (
    "store",
    "isolation_level",
    "transaction_id",
    "deadlock_retry_policy",
    "invariant_scope",
)
DISTRIBUTED_LOCK_BACKEND_HEALTH_REQUIRED_FIELDS = (
    "backend_name",
    "health_status",
    "stale_read_detected",
    "split_brain_detected",
)
BACKGROUND_JOB_REQUIRED_FIELDS = (
    "job_name",
    "entry_point",
    "allowed_modes",
    "lease_policy",
    "owner",
)
BACKGROUND_JOB_ALLOWED_MODES = {"observe_only", "shadow", "ultra_tiny", "normal_tiny"}
SCHEDULED_JOB_MODE_GATE_REQUIRED_FIELDS = (
    "job_name",
    "mode",
    "allowed_to_run",
    "gate_reason",
    "checked_at",
)
FEATURE_FLAG_DEPENDENCY_REQUIRED_FIELDS = (
    "feature_flag",
    "depends_on",
    "mode_scope",
    "dependency_state",
    "activation_action",
)
FEATURE_FLAG_DEPENDENCY_STATES = {
    "disabled_by_default",
    "optional_safe",
    "paper_only_required",
    "required_pass",
}
FEATURE_FLAG_ACTIVATION_ACTIONS = {
    "allow_when_dependencies_ready",
    "block_until_dependencies_ready",
    "keep_disabled_until_enabled",
    "quarantine_live_execution",
}
FILESYSTEM_PRESSURE_REQUIRED_FIELDS = (
    "filesystem_path",
    "free_bytes",
    "wal_bytes",
    "pressure_action",
)
FILESYSTEM_PRESSURE_POLICY_REQUIRED_FIELDS = (
    "filesystem_path",
    "min_free_bytes",
    "max_wal_bytes",
    "pressure_action",
    "wal_files",
)
ENTRY_POINT_REQUIRED_FIELDS = (
    "entry_point_id",
    "code_location",
    "route_registry_required",
    "arbiter_required",
)
ENTRY_POINT_ALLOWED_TYPES = {"route_group", "server", "script", "cron", "deploy"}
STATIC_POLICY_REQUIRED_FIELDS = (
    "static_check_id",
    "forbidden_pattern",
    "scan_target",
    "result",
)
API_RESPONSE_REQUIRED_FIELDS = (
    "endpoint",
    "response_schema_version",
    "status_code_policy",
    "error_envelope",
    "cache_control",
)
API_RESPONSE_ENVELOPE_REQUIRED_FIELDS = (
    "endpoint",
    "response_schema_version",
    "source_anchor",
)
API_RESPONSE_ENVELOPE_SPEC_FIELDS = (
    "endpoint",
    "envelope_version",
    "payload_hash",
    "error_shape",
    "generated_at",
)
ERROR_TAXONOMY_REQUIRED_FIELDS = (
    "error_code",
    "category",
    "severity",
    "operator_action",
    "introduced_at",
)
HUMAN_REASON_REQUIRED_FIELDS = (
    "reason_code",
    "human_message",
    "operator_action",
    "locale",
    "owner",
)
MACHINE_REASON_REQUIRED_FIELDS = (
    "reason_code",
    "machine_code",
    "schema_version",
    "blocking_contract",
    "failure_action",
)
LOG_REDACTION_STREAM_REQUIRED_FIELDS = (
    "log_stream",
    "secret_pattern_set",
    "source_file",
    "redaction_anchor",
    "write_anchor",
    "sample_case_ids",
)
SERVICE_READINESS_PROBE_REQUIRED_FIELDS = (
    "service_name",
    "probe_id",
    "health_status",
    "dependency_status",
    "source_file",
    "source_anchor",
)
SERVICE_READINESS_CONTRACT_FIELDS = (
    "service_name",
    "probe_id",
    "health_status",
    "dependency_status",
    "checked_at",
)
DASHBOARD_ACTION_SEPARATION_REQUIRED_FIELDS = (
    "action_id",
    "view_route",
    "mutation_route",
    "separation_enforced",
    "audit_required",
)
WRITE_PATH_ALLOWED_MODE_GATES = {
    "observe_only",
    "shadow",
    "ultra_tiny",
    "normal_tiny",
    "admin_break_glass",
    "diagnostics",
}
NORMAL_TINY_BLOCKING_CONTRACTS = {
    "RawProviderEvidenceContract",
    "LabelFinalizationContract",
    "OutcomeWindowCloseContract",
    "RandomnessControlContract",
    "DeploymentRolloutStateMachine",
    "WorkerFleetConsistencyContract",
    "BackupRestoreDrillContract",
    "IncidentEvidenceFreezeContract",
    "CircuitBreakerResumeContract",
    "QueueDurabilityContract",
    "CandidateCancellationContract",
    "RetryStormControlContract",
    "ProviderCoverageMapContract",
    "TrainingServingSkewContract",
    "EvidenceEligibilityMatrix",
    "TopFixQueueContract",
    "SafetyCaseContract",
    "WaiverPolicyContract",
    "SafeDefaultContract",
    "ProjectStopLossContract",
}


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _contract(contract_id, passed, reason, evidence):
    return {
        "contract_id": contract_id,
        "status": "pass" if passed else "missing_evidence",
        "blocking_reason": None if passed else reason,
        "evidence": evidence,
    }


def _bool_env(env, name, default):
    value = (env or {}).get(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _int_env(env, name, default):
    try:
        return int((env or {}).get(name, default))
    except (TypeError, ValueError):
        return default


def _float_env(env, name, default):
    try:
        return float((env or {}).get(name, default))
    except (TypeError, ValueError):
        return default


def _sha256_json(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _missing_required_fields(record, fields):
    missing = []
    for field in fields:
        value = record.get(field) if isinstance(record, dict) else None
        if value is None or value == "" or value == [] or value == {}:
            missing.append(field)
    return missing


def _hash_record_without(record, key):
    if not isinstance(record, dict):
        return None
    reduced = {field: value for field, value in record.items() if field != key}
    return _sha256_json(reduced)


def _validate_registry_source_files(source_files):
    source_checks = []
    source_errors = []
    source_files = source_files if isinstance(source_files, list) else []
    for index, source in enumerate(source_files):
        if not isinstance(source, dict):
            source_errors.append({"index": index, "source_file": None, "reason": "source_record_not_object"})
            continue
        source_file = source.get("source_file")
        source_anchor = source.get("source_anchor")
        required_patterns = source.get("required_patterns") if isinstance(source.get("required_patterns"), list) else []
        text, error = _read_project_text(source_file)
        if error:
            source_errors.append({"index": index, **error})
            continue
        missing_patterns = [str(pattern) for pattern in required_patterns if str(pattern) not in text]
        anchor_present = bool(source_anchor and str(source_anchor) in text)
        check = {
            "source_file": source_file,
            "source_anchor": source_anchor,
            "anchor_present": anchor_present,
            "required_pattern_count": len(required_patterns),
            "missing_patterns": missing_patterns,
        }
        source_checks.append(check)
        if not anchor_present or missing_patterns:
            source_errors.append({"index": index, "source_file": source_file, "reason": "source_anchor_or_pattern_missing", **check})
    return source_checks, source_errors


def _scan_threshold_hardcode_guard(guard):
    guard = guard if isinstance(guard, dict) else {}
    guard_id = str(guard.get("guard_id") or "")
    protected_source_files = guard.get("protected_source_files") if isinstance(guard.get("protected_source_files"), list) else []
    forbidden_patterns = guard.get("forbidden_literal_patterns") if isinstance(guard.get("forbidden_literal_patterns"), list) else []
    malformed_patterns = []
    source_errors = []
    violations = []
    for index, pattern in enumerate(forbidden_patterns):
        if not isinstance(pattern, dict):
            malformed_patterns.append({"index": index, "literal": None, "reason": "pattern_not_object"})
            continue
        literal = str(pattern.get("literal") or "")
        threshold_id = str(pattern.get("threshold_id") or "")
        if not literal or not threshold_id:
            malformed_patterns.append({"index": index, "literal": literal or None, "reason": "literal_and_threshold_id_required"})

    valid_patterns = [pattern for pattern in forbidden_patterns if isinstance(pattern, dict) and pattern.get("literal") and pattern.get("threshold_id")]
    for source_file in protected_source_files:
        text, error = _read_project_text(source_file)
        if error:
            source_errors.append(error)
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for pattern in valid_patterns:
                literal = str(pattern.get("literal"))
                if literal in line:
                    violations.append(
                        {
                            "source_file": source_file,
                            "line": line_number,
                            "literal": literal,
                            "threshold_id": str(pattern.get("threshold_id")),
                            "reason": str(pattern.get("reason") or "threshold_literal_must_come_from_catalog"),
                        }
                    )
    evidence = {
        "guard_id": guard_id,
        "protected_source_files": protected_source_files,
        "forbidden_literal_count": len(forbidden_patterns),
        "malformed_patterns": malformed_patterns,
        "violations": violations,
        "source_errors": source_errors,
    }
    errors = []
    if not guard_id:
        errors.append("guard_id_required")
    if not protected_source_files:
        errors.append("protected_source_files_required")
    if not forbidden_patterns:
        errors.append("forbidden_literal_patterns_required")
    if malformed_patterns:
        errors.append("malformed_forbidden_literal_patterns")
    if source_errors:
        errors.append("guard_source_missing")
    if violations:
        errors.append("hardcoded_threshold_literal_detected")
    return evidence, sorted(set(errors))


def verify_metric_definition_registry(registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("MetricDefinitionRegistry", False, "metric_definition_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("MetricDefinitionRegistry", False, "metric_definition_registry_not_object", {"registry_path": str(registry_path)})

    metrics = registry.get("metrics") if isinstance(registry.get("metrics"), list) else []
    malformed_metrics = []
    duplicate_metric_ids = []
    metric_records = {}
    seen_metric_ids = set()
    for index, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            malformed_metrics.append({"index": index, "metric_id": None, "missing_fields": list(METRIC_DEFINITION_REQUIRED_FIELDS), "violations": ["metric_not_object"]})
            continue
        metric_id = str(metric.get("metric_id") or "")
        missing = _missing_required_fields(metric, METRIC_DEFINITION_REQUIRED_FIELDS)
        violations = []
        if metric_id in seen_metric_ids:
            duplicate_metric_ids.append(metric_id)
        if metric_id:
            seen_metric_ids.add(metric_id)
            metric_records[metric_id] = metric
        if metric_id and not re.match(r"^[a-z][a-z0-9_]*$", metric_id):
            violations.append("metric_id_must_be_lower_snake_case")
        if str(metric.get("metric_name") or "").strip() == "":
            violations.append("metric_name_required")
        event_time_basis = str(metric.get("event_time_basis") or "")
        if event_time_basis not in METRIC_EVENT_TIME_BASIS:
            violations.append("event_time_basis_not_allowed")
        for field_name in ("inclusion_criteria", "exclusion_criteria"):
            if not isinstance(metric.get(field_name), list) or not metric.get(field_name):
                violations.append(f"{field_name}_must_be_nonempty_list")
        if str(metric.get("late_event_policy") or "").strip() == "":
            violations.append("late_event_policy_required")
        if str(metric.get("partial_window_policy") or "").strip() == "":
            violations.append("partial_window_policy_required")
        if str(metric.get("metric_version") or "").strip() == "":
            violations.append("metric_version_required")
        metric_hash = str(metric.get("metric_hash") or "")
        expected_metric_hash = _hash_record_without(metric, "metric_hash") if metric_id else None
        if metric_hash and expected_metric_hash and metric_hash != expected_metric_hash:
            violations.append("metric_hash_mismatch")
        if missing or violations:
            malformed_metrics.append({"index": index, "metric_id": metric_id or None, "missing_fields": missing, "violations": violations})

    missing_required_metrics = sorted(
        {
            "telegram_capture_rate_d3a",
            "telegram_capture_rate_d3b",
            "entered_net_delayed_executable_peak30_rate",
            "telegram_realized_roi_24h",
            "filesystem_free_bytes",
            "p_absorb_peak30",
            "p_absorb_stop_before_peak",
            "reclaim_confirmed_precision_for_peak30",
            "overextended_false_negative_peak30_rate",
            "capacity_headroom_pct",
            "load_test_replay_pass_rate",
            "latency_sla_breach_rate",
            "provider_quota_exit_reserve_pct",
            "economic_cost_budget_utilization",
            "operator_audit_completeness_rate",
            "operator_safety_reject_count",
            "oncall_coverage_rate",
            "alert_policy_coverage_rate",
            "alert_ack_sla_rate",
            "kill_switch_drill_pass_rate",
            "replay_determinism_pass_rate",
            "reproducible_build_hash_coverage_rate",
            "supply_chain_artifact_verified_rate",
            "policy_bundle_compatibility_rate",
            "model_not_expired_rate",
            "forecast_sanity_guard_pass_rate",
            "rendered_spec_view_valid_rate",
            "health_state_enum_valid_rate",
            "contract_lifecycle_active_gate_coverage_rate",
            "objective_priority_conflict_resolution_rate",
            "goal_confidence_lower_bound",
            "fill_time_anchor_valid_rate",
            "ex_ante_feasibility_valid_rate",
            "token_identity_confidence_min",
            "data_unit_validation_rate",
            "chain_finality_health_rate",
            "provider_schema_canary_pass_rate",
            "reconciliation_policy_safe_rate",
            "dashboard_staleness_safe_rate",
            "spec_traceability_valid_rate",
            "implementation_issue_graph_valid_rate",
            "module_closure_valid_rate",
            "decommission_policy_safe_rate",
        }
        - set(metric_records)
    )

    source_checks, source_errors = _validate_registry_source_files(registry.get("source_files"))
    passed = (
        registry.get("schema_version") == "v2.7.0.metric_definition_registry.v1"
        and registry.get("failure_action") == "metric_invalid"
        and bool(metrics)
        and not malformed_metrics
        and not duplicate_metric_ids
        and not missing_required_metrics
        and not source_errors
    )
    return _contract(
        "MetricDefinitionRegistry",
        passed,
        "metric_definition_registry_missing_malformed_or_drifted",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "scope": registry.get("scope"),
            "failure_action": registry.get("failure_action"),
            "required_fields": list(METRIC_DEFINITION_REQUIRED_FIELDS),
            "metric_count": len(metrics),
            "missing_required_metrics": missing_required_metrics,
            "duplicate_metric_ids": sorted(str(item) for item in duplicate_metric_ids),
            "malformed_metrics": malformed_metrics,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_threshold_catalog(threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG, metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY):
    try:
        catalog = _load_json(threshold_catalog_path)
        metric_registry = _load_json(metric_registry_path)
    except Exception as exc:
        return _contract("ThresholdCatalogContract", False, "threshold_catalog_missing_or_invalid", {"error": str(exc)})
    if not isinstance(catalog, dict):
        return _contract("ThresholdCatalogContract", False, "threshold_catalog_not_object", {"threshold_catalog_path": str(threshold_catalog_path)})
    if not isinstance(metric_registry, dict):
        return _contract("ThresholdCatalogContract", False, "metric_registry_not_object", {"metric_registry_path": str(metric_registry_path)})

    metrics = metric_registry.get("metrics") if isinstance(metric_registry.get("metrics"), list) else []
    metric_ids = {str(metric.get("metric_id") or "") for metric in metrics if isinstance(metric, dict) and metric.get("metric_id")}

    thresholds = catalog.get("thresholds") if isinstance(catalog.get("thresholds"), list) else []
    malformed_thresholds = []
    duplicate_threshold_ids = []
    threshold_records = {}
    seen_threshold_ids = set()
    for index, threshold in enumerate(thresholds):
        if not isinstance(threshold, dict):
            malformed_thresholds.append({"index": index, "threshold_id": None, "missing_fields": list(THRESHOLD_CATALOG_REQUIRED_FIELDS), "violations": ["threshold_not_object"]})
            continue
        threshold_id = str(threshold.get("threshold_id") or "")
        missing = _missing_required_fields(threshold, THRESHOLD_CATALOG_REQUIRED_FIELDS)
        violations = []
        if threshold_id in seen_threshold_ids:
            duplicate_threshold_ids.append(threshold_id)
        if threshold_id:
            seen_threshold_ids.add(threshold_id)
            threshold_records[threshold_id] = threshold
        if threshold_id and not re.match(r"^[a-z][a-z0-9_]*$", threshold_id):
            violations.append("threshold_id_must_be_lower_snake_case")
        if str(threshold.get("threshold_name") or "").strip() == "":
            violations.append("threshold_name_required")
        if str(threshold.get("unit") or "").strip() == "":
            violations.append("threshold_unit_required")
        if str(threshold.get("comparison_operator") or "") not in THRESHOLD_COMPARISON_OPERATORS:
            violations.append("comparison_operator_not_allowed")
        threshold_value = threshold.get("threshold_value")
        try:
            Decimal(str(threshold_value))
        except (InvalidOperation, ValueError, TypeError):
            violations.append("threshold_value_not_numeric")
        applies_to_metric = str(threshold.get("applies_to_metric") or "")
        if applies_to_metric not in metric_ids:
            violations.append("applies_to_metric_unknown")
        applies_to_mode = threshold.get("applies_to_mode")
        if isinstance(applies_to_mode, list):
            modes = [str(mode) for mode in applies_to_mode]
        elif isinstance(applies_to_mode, str) and applies_to_mode:
            modes = [applies_to_mode]
        else:
            modes = []
        if not modes or any(mode not in THRESHOLD_ALLOWED_MODES for mode in modes):
            violations.append("applies_to_mode_not_allowed")
        if not _parse_iso_ts(threshold.get("effective_from")):
            violations.append("effective_from_invalid")
        if str(threshold.get("effective_to") or "").strip() != "open" and not _parse_iso_ts(threshold.get("effective_to")):
            violations.append("effective_to_invalid")
        if str(threshold.get("approval_id") or "").strip() == "":
            violations.append("approval_id_required")
        threshold_hash = str(threshold.get("threshold_hash") or "")
        expected_threshold_hash = _hash_record_without(threshold, "threshold_hash") if threshold_id else None
        if threshold_hash and expected_threshold_hash and threshold_hash != expected_threshold_hash:
            violations.append("threshold_hash_mismatch")
        if missing or violations:
            malformed_thresholds.append({"index": index, "threshold_id": threshold_id or None, "missing_fields": missing, "violations": violations})

    missing_required_thresholds = sorted(
        {
            "thr_capture_rate_d3a_24h_min",
            "thr_capture_rate_d3b_24h_min",
            "thr_entered_peak30_rate_24h_min",
            "thr_realized_roi_24h_min",
            "thr_filesystem_free_bytes_min",
            "thr_p_absorb_peak30_shadow_min",
            "thr_p_absorb_stop_before_peak_shadow_max",
            "thr_reclaim_confirmed_precision_shadow_min",
            "thr_overextended_false_negative_shadow_max",
            "thr_capacity_headroom_pct_min",
            "thr_load_test_replay_pass_rate_min",
            "thr_latency_sla_breach_rate_max",
            "thr_provider_quota_exit_reserve_pct_min",
            "thr_economic_cost_budget_utilization_max",
            "thr_operator_audit_completeness_rate_min",
            "thr_operator_safety_reject_count_max",
            "thr_oncall_coverage_rate_min",
            "thr_alert_policy_coverage_rate_min",
            "thr_alert_ack_sla_rate_min",
            "thr_kill_switch_drill_pass_rate_min",
            "thr_replay_determinism_pass_rate_min",
            "thr_reproducible_build_hash_coverage_rate_min",
            "thr_supply_chain_artifact_verified_rate_min",
            "thr_policy_bundle_compatibility_rate_min",
            "thr_model_not_expired_rate_min",
            "thr_forecast_sanity_guard_pass_rate_min",
            "thr_rendered_spec_view_valid_rate_min",
            "thr_health_state_enum_valid_rate_min",
            "thr_contract_lifecycle_active_gate_coverage_rate_min",
            "thr_objective_priority_conflict_resolution_rate_min",
            "thr_goal_confidence_lower_bound_min",
            "thr_fill_time_anchor_valid_rate_min",
            "thr_ex_ante_feasibility_valid_rate_min",
            "thr_token_identity_confidence_min",
            "thr_data_unit_validation_rate_min",
            "thr_chain_finality_health_rate_min",
            "thr_provider_schema_canary_pass_rate_min",
            "thr_reconciliation_policy_safe_rate_min",
            "thr_dashboard_staleness_safe_rate_min",
            "thr_spec_traceability_valid_rate_min",
            "thr_implementation_issue_graph_valid_rate_min",
            "thr_module_closure_valid_rate_min",
            "thr_decommission_policy_safe_rate_min",
        }
        - set(threshold_records)
    )

    hardcode_guard_evidence, hardcode_guard_errors = _scan_threshold_hardcode_guard(catalog.get("hardcoded_threshold_guard"))
    source_checks, source_errors = _validate_registry_source_files(catalog.get("source_files"))
    passed = (
        catalog.get("schema_version") == "v2.7.0.threshold_catalog.v1"
        and catalog.get("failure_action") == "policy_bundle_incompatible"
        and bool(thresholds)
        and not malformed_thresholds
        and not duplicate_threshold_ids
        and not missing_required_thresholds
        and not hardcode_guard_errors
        and not source_errors
    )
    return _contract(
        "ThresholdCatalogContract",
        passed,
        "threshold_catalog_missing_malformed_or_drifted",
        {
            "threshold_catalog_path": str(threshold_catalog_path),
            "schema_version": catalog.get("schema_version"),
            "scope": catalog.get("scope"),
            "failure_action": catalog.get("failure_action"),
            "required_fields": list(THRESHOLD_CATALOG_REQUIRED_FIELDS),
            "threshold_count": len(thresholds),
            "missing_required_thresholds": missing_required_thresholds,
            "duplicate_threshold_ids": sorted(str(item) for item in duplicate_threshold_ids),
            "malformed_thresholds": malformed_thresholds,
            "hardcode_guard": hardcode_guard_evidence,
            "hardcode_guard_errors": hardcode_guard_errors,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def _project_file_hashes(source_files):
    hashes = {}
    errors = []
    for source_file in source_files if isinstance(source_files, list) else []:
        raw_source_file = str(source_file)
        resolved = _resolve_project_file(raw_source_file)
        if not resolved or not resolved.exists():
            errors.append({"source_file": raw_source_file, "reason": "source_missing"})
            continue
        hashes[raw_source_file] = _sha256_file(resolved)
    return hashes, errors


def _hash_file_group(source_files):
    hashes, errors = _project_file_hashes(source_files)
    return _sha256_json(hashes), hashes, errors


def _runtime_env_values(profile, env):
    values = {}
    violations = []
    for item in profile.get("env_vars") if isinstance(profile.get("env_vars"), list) else []:
        if not isinstance(item, dict):
            violations.append("env_var_record_not_object")
            continue
        name = str(item.get("name") or "")
        if not name:
            violations.append("env_var_name_required")
            continue
        actual = (env or {}).get(name)
        if actual is None:
            actual = item.get("default_value")
        actual = str(actual)
        expected = str(item.get("expected_value") or "")
        values[name] = actual
        if item.get("required") is True and actual == "":
            violations.append(f"{name}_required")
        if expected and actual != expected:
            violations.append(f"{name}_drifted")
    return values, violations


def _runtime_config_component_hashes(profile, env):
    env_values, env_violations = _runtime_env_values(profile, env)
    env_vars_hash = _sha256_json(env_values)
    feature_flags_hash, feature_flag_files, feature_flag_errors = _hash_file_group(profile.get("feature_flag_files"))
    provider_config_hash, provider_config_files, provider_config_errors = _hash_file_group(profile.get("provider_config_files"))
    policy_bundle_hash, policy_bundle_files, policy_bundle_errors = _hash_file_group(profile.get("policy_bundle_files"))

    single_files = {
        "route_registry_hash": profile.get("route_registry_file"),
        "source_registry_hash": profile.get("source_registry_file"),
        "threshold_catalog_hash": profile.get("threshold_catalog_file"),
        "metric_registry_hash": profile.get("metric_registry_file"),
    }
    single_hashes = {}
    single_errors = []
    for key, source_file in single_files.items():
        hashes, errors = _project_file_hashes([source_file])
        single_hashes[key] = next(iter(hashes.values()), None)
        single_errors.extend(errors)

    component_hashes = {
        "env_vars_hash": env_vars_hash,
        "feature_flags_hash": feature_flags_hash,
        "provider_config_hash": provider_config_hash,
        "route_registry_hash": single_hashes.get("route_registry_hash"),
        "source_registry_hash": single_hashes.get("source_registry_hash"),
        "threshold_catalog_hash": single_hashes.get("threshold_catalog_hash"),
        "metric_registry_hash": single_hashes.get("metric_registry_hash"),
        "policy_bundle_hash": policy_bundle_hash,
    }
    runtime_config_hash = _sha256_json(component_hashes)
    return {
        "component_hashes": component_hashes,
        "runtime_config_hash": runtime_config_hash,
        "expected_hash": runtime_config_hash,
        "env_values": env_values,
        "feature_flag_files": feature_flag_files,
        "provider_config_files": provider_config_files,
        "policy_bundle_files": policy_bundle_files,
        "source_errors": feature_flag_errors + provider_config_errors + policy_bundle_errors + single_errors,
        "env_violations": env_violations,
    }


def verify_runtime_config_drift_contract(policy_path=DEFAULT_RUNTIME_CONFIG_DRIFT_POLICY, env=None):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("RuntimeConfigDriftContract", False, "runtime_config_drift_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("RuntimeConfigDriftContract", False, "runtime_config_drift_policy_not_object", {"policy_path": str(policy_path)})

    profiles = policy.get("profiles") if isinstance(policy.get("profiles"), list) else []
    active_profile_id = str(policy.get("active_profile_id") or "")
    profile_by_id = {str(item.get("profile_id")): item for item in profiles if isinstance(item, dict) and item.get("profile_id")}
    malformed_profiles = []
    profile_evidence = []
    for index, profile in enumerate(profiles):
        if not isinstance(profile, dict):
            malformed_profiles.append({"index": index, "profile_id": None, "missing_fields": list(RUNTIME_CONFIG_DRIFT_REQUIRED_FIELDS), "violations": ["profile_not_object"]})
            continue
        profile_id = str(profile.get("profile_id") or "")
        missing = _missing_required_fields(profile, ("profile_id",) + RUNTIME_CONFIG_DRIFT_REQUIRED_FIELDS)
        violations = []
        if profile_id and not re.match(r"^[a-z][a-z0-9_]*$", profile_id):
            violations.append("profile_id_must_be_lower_snake_case")
        if not _parse_iso_ts(profile.get("loaded_at")):
            violations.append("loaded_at_invalid")
        if str(profile.get("drift_action") or "") not in RUNTIME_CONFIG_DRIFT_ACTIONS:
            violations.append("drift_action_not_allowed")
        if profile.get("drift_detected") is not False:
            violations.append("drift_detected_must_be_false_for_readiness")

        computed = _runtime_config_component_hashes(profile, env or {})
        if computed["source_errors"]:
            violations.append("source_file_missing")
        if computed["env_violations"]:
            violations.append("env_var_drift")
        for key, actual_hash in computed["component_hashes"].items():
            if profile.get(key) != actual_hash:
                violations.append(f"{key}_mismatch")
        if profile.get("runtime_config_hash") != computed["runtime_config_hash"]:
            violations.append("runtime_config_hash_mismatch")
        if profile.get("expected_hash") != computed["expected_hash"]:
            violations.append("expected_hash_mismatch")

        profile_evidence.append(
            {
                "profile_id": profile_id,
                "env_values": computed["env_values"],
                "component_hashes": computed["component_hashes"],
                "runtime_config_hash": profile.get("runtime_config_hash"),
                "expected_runtime_config_hash": computed["runtime_config_hash"],
                "source_errors": computed["source_errors"],
                "env_violations": computed["env_violations"],
            }
        )
        if missing or violations:
            malformed_profiles.append({"index": index, "profile_id": profile_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_config_drift_policy.v1"
        and policy.get("failure_action") == "runtime_config_drift"
        and active_profile_id in profile_by_id
        and bool(profiles)
        and not malformed_profiles
        and not source_errors
    )
    return _contract(
        "RuntimeConfigDriftContract",
        passed,
        "runtime_config_drift_missing_malformed_or_drifted",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "active_profile_id": active_profile_id,
            "required_fields": list(RUNTIME_CONFIG_DRIFT_REQUIRED_FIELDS),
            "profile_count": len(profiles),
            "profile_evidence": profile_evidence,
            "malformed_profiles": malformed_profiles,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_environment_separation_contract(policy_path=DEFAULT_ENVIRONMENT_SEPARATION_POLICY, env=None):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("EnvironmentSeparationContract", False, "environment_separation_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("EnvironmentSeparationContract", False, "environment_separation_policy_not_object", {"policy_path": str(policy_path)})

    environments = policy.get("environments") if isinstance(policy.get("environments"), list) else []
    environment_by_id = {
        str(item.get("environment_id")): item
        for item in environments
        if isinstance(item, dict) and item.get("environment_id")
    }
    active_environment_id = str((env or {}).get("V27_ENVIRONMENT_ID") or policy.get("active_environment_default") or "")
    malformed_environments = []
    duplicate_environment_ids = []
    environment_evidence = []
    seen_environment_ids = set()
    for index, environment in enumerate(environments):
        if not isinstance(environment, dict):
            malformed_environments.append({"index": index, "environment_id": None, "missing_fields": list(ENVIRONMENT_SEPARATION_REQUIRED_FIELDS), "violations": ["environment_not_object"]})
            continue
        environment_id = str(environment.get("environment_id") or "")
        missing = [
            field
            for field in ENVIRONMENT_SEPARATION_REQUIRED_FIELDS
            if field not in environment
            or environment.get(field) is None
            or (isinstance(environment.get(field), str) and not environment.get(field))
        ]
        violations = []
        if environment_id in seen_environment_ids:
            duplicate_environment_ids.append(environment_id)
        if environment_id:
            seen_environment_ids.add(environment_id)
        if environment_id and not re.match(r"^[a-z][a-z0-9_]*$", environment_id):
            violations.append("environment_id_must_be_lower_snake_case")
        environment_type = str(environment.get("environment_type") or "")
        if environment_type not in ENVIRONMENT_TYPES:
            violations.append("environment_type_not_allowed")
        for field in ("allowed_event_logs", "allowed_databases", "allowed_provider_keys", "allowed_routes", "allowed_modes", "write_permissions", "read_permissions"):
            if not isinstance(environment.get(field), list):
                violations.append(f"{field}_must_be_list")
        allowed_modes = set(str(item) for item in environment.get("allowed_modes") or [])
        if not allowed_modes <= (BACKGROUND_JOB_ALLOWED_MODES | {"all_modes"}):
            violations.append("allowed_modes_not_allowed")
        provider_keys = [str(item).lower() for item in environment.get("allowed_provider_keys") or []]
        if any("live_private_key" in item or "signing" in item or "real_order_router" in item for item in provider_keys):
            violations.append("live_capability_allowed")
        if environment_type == "dashboard_readonly" and environment.get("write_permissions"):
            violations.append("dashboard_readonly_has_write_permissions")
        if environment_type in {"local_dev", "research", "backfill_research", "dashboard_readonly"} and environment.get("promotion_allowed") is True:
            violations.append("non_promotion_environment_allows_promotion")
        if not isinstance(environment.get("data_export_allowed"), bool):
            violations.append("data_export_allowed_must_be_bool")
        if not isinstance(environment.get("promotion_allowed"), bool):
            violations.append("promotion_allowed_must_be_bool")
        expected_environment_hash = _hash_record_without(environment, "environment_hash")
        if environment.get("environment_hash") != expected_environment_hash:
            violations.append("environment_hash_mismatch")
        environment_evidence.append(
            {
                "environment_id": environment_id,
                "environment_type": environment_type,
                "allowed_modes": sorted(allowed_modes),
                "write_permission_count": len(environment.get("write_permissions") or []),
                "promotion_allowed": environment.get("promotion_allowed"),
                "environment_hash": environment.get("environment_hash"),
                "expected_environment_hash": expected_environment_hash,
            }
        )
        if missing or violations:
            malformed_environments.append({"index": index, "environment_id": environment_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    runtime_violations = []
    active_environment = environment_by_id.get(active_environment_id)
    if active_environment is None:
        runtime_violations.append("active_environment_unknown")
    live_execution_enabled = str((env or {}).get("PREMIUM_LIVE_EXECUTION_ENABLED") or "false").strip().lower()
    if live_execution_enabled not in {"", "0", "false", "no", "off"}:
        runtime_violations.append("live_execution_env_enabled")
    live_secret_env_names = ["LIVE_PRIVATE_KEY", "SOLANA_PRIVATE_KEY", "NETWORK_TRANSACTION_SIGNING_KEY"]
    live_secret_present = [name for name in live_secret_env_names if (env or {}).get(name)]
    if live_secret_present:
        runtime_violations.append("live_secret_present")

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.environment_separation_policy.v1"
        and policy.get("failure_action") == "environment_contamination"
        and bool(environments)
        and not malformed_environments
        and not duplicate_environment_ids
        and not runtime_violations
        and not source_errors
    )
    return _contract(
        "EnvironmentSeparationContract",
        passed,
        "environment_separation_missing_malformed_or_contaminated",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "active_environment_id": active_environment_id,
            "required_fields": list(ENVIRONMENT_SEPARATION_REQUIRED_FIELDS),
            "environment_count": len(environments),
            "duplicate_environment_ids": sorted(str(item) for item in duplicate_environment_ids),
            "malformed_environments": malformed_environments,
            "runtime_violations": runtime_violations,
            "live_secret_present": live_secret_present,
            "environment_evidence": environment_evidence,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_null_value_policy_contract(policy_path=DEFAULT_NULL_VALUE_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("NullValuePolicyContract", False, "null_value_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("NullValuePolicyContract", False, "null_value_policy_not_object", {"policy_path": str(policy_path)})

    null_policies = policy.get("null_policies") if isinstance(policy.get("null_policies"), list) else []
    seen_fields = set()
    duplicate_fields = []
    malformed_policies = []
    for index, item in enumerate(null_policies):
        if not isinstance(item, dict):
            malformed_policies.append({"index": index, "field_name": None, "missing_fields": list(NULL_VALUE_POLICY_REQUIRED_FIELDS), "violations": ["null_policy_not_object"]})
            continue
        field_name = str(item.get("field_name") or "")
        missing = _missing_required_fields(item, NULL_VALUE_POLICY_REQUIRED_FIELDS)
        violations = []
        if field_name in seen_fields:
            duplicate_fields.append(field_name)
        if field_name:
            seen_fields.add(field_name)
        if field_name and not re.match(r"^[a-z][a-z0-9_]*$", field_name):
            violations.append("field_name_must_be_lower_snake_case")
        if item.get("null_class") not in NULL_VALUE_CLASSES:
            violations.append("null_class_not_allowed")
        allowed_modes = item.get("allowed_in_modes") if isinstance(item.get("allowed_in_modes"), list) else []
        if not allowed_modes or any(str(mode) not in THRESHOLD_ALLOWED_MODES for mode in allowed_modes):
            violations.append("allowed_in_modes_not_allowed")
        for field in ("default_value_allowed", "training_allowed", "decision_allowed"):
            if not isinstance(item.get(field), bool):
                violations.append(f"{field}_must_be_bool")
        if field_name in REQUIRED_NULL_POLICY_FIELDS and item.get("default_value_allowed") is True:
            violations.append("critical_field_default_value_forbidden")
        if field_name in REQUIRED_NULL_POLICY_FIELDS and item.get("decision_allowed") is True and item.get("null_class") != "true_zero":
            violations.append("critical_unknown_decision_allowed")
        if str(item.get("imputation_policy") or "").strip() == "":
            violations.append("imputation_policy_required")
        if str(item.get("dashboard_display") or "").strip() == "":
            violations.append("dashboard_display_required")
        expected_policy_hash = _hash_record_without(item, "policy_hash")
        if item.get("policy_hash") != expected_policy_hash:
            violations.append("policy_hash_mismatch")
        if missing or violations:
            malformed_policies.append({"index": index, "field_name": field_name or None, "missing_fields": missing, "violations": sorted(set(violations))})

    missing_required_fields = sorted(REQUIRED_NULL_POLICY_FIELDS - seen_fields)
    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.null_value_policy.v1"
        and policy.get("failure_action") == "feature_invalid_or_shadow_only"
        and bool(null_policies)
        and not duplicate_fields
        and not missing_required_fields
        and not malformed_policies
        and not source_errors
    )
    return _contract(
        "NullValuePolicyContract",
        passed,
        "null_value_policy_missing_malformed_or_unsafe",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "null_policy_count": len(null_policies),
            "duplicate_fields": sorted(str(item) for item in duplicate_fields),
            "missing_required_fields": missing_required_fields,
            "malformed_policies": malformed_policies,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_feature_availability_contract(
    policy_path=DEFAULT_FEATURE_VECTOR_SNAPSHOT_POLICY,
    null_policy_path=DEFAULT_NULL_VALUE_POLICY,
):
    try:
        policy = _load_json(policy_path)
        null_policy = _load_json(null_policy_path)
    except Exception as exc:
        return _contract("FeatureAvailabilityContract", False, "feature_availability_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("FeatureAvailabilityContract", False, "feature_availability_policy_not_object", {"policy_path": str(policy_path)})
    if not isinstance(null_policy, dict):
        return _contract("FeatureAvailabilityContract", False, "null_value_policy_not_object", {"null_policy_path": str(null_policy_path)})

    null_fields = {
        str(item.get("field_name"))
        for item in null_policy.get("null_policies", [])
        if isinstance(item, dict) and item.get("field_name")
    }
    records = policy.get("feature_availability") if isinstance(policy.get("feature_availability"), list) else []
    seen_features = set()
    duplicate_features = []
    malformed_records = []
    for index, item in enumerate(records):
        if not isinstance(item, dict):
            malformed_records.append({"index": index, "feature_name": None, "missing_fields": list(FEATURE_AVAILABILITY_REQUIRED_FIELDS), "violations": ["feature_availability_not_object"]})
            continue
        feature_name = str(item.get("feature_name") or "")
        missing = _missing_required_fields(item, FEATURE_AVAILABILITY_REQUIRED_FIELDS)
        violations = []
        if feature_name in seen_features:
            duplicate_features.append(feature_name)
        if feature_name:
            seen_features.add(feature_name)
        if feature_name and not re.match(r"^[a-z][a-z0-9_]*$", feature_name):
            violations.append("feature_name_must_be_lower_snake_case")
        window_start = _parse_iso_ts(item.get("feature_window_start"))
        window_end = _parse_iso_ts(item.get("feature_window_end"))
        feature_available_at = _parse_iso_ts(item.get("feature_available_at"))
        decision_available_at = _parse_iso_ts(item.get("decision_available_at"))
        label_available_at = _parse_iso_ts(item.get("label_available_at"))
        if not all([window_start, window_end, feature_available_at, decision_available_at, label_available_at]):
            violations.append("timestamp_invalid")
        else:
            if window_end < window_start:
                violations.append("feature_window_end_before_start")
            if window_end > decision_available_at:
                violations.append("feature_window_end_after_decision")
            if feature_available_at > decision_available_at:
                violations.append("feature_available_after_decision")
            if label_available_at <= decision_available_at:
                violations.append("label_available_not_after_decision")
        if not isinstance(item.get("feature_research_only"), bool):
            violations.append("feature_research_only_must_be_bool")
        null_policy_field = str(item.get("null_policy_field") or "")
        if null_policy_field not in null_fields:
            violations.append("null_policy_field_unknown")
        expected_hash = _hash_record_without(item, "availability_hash")
        if item.get("availability_hash") != expected_hash:
            violations.append("availability_hash_mismatch")
        if missing or violations:
            malformed_records.append({"index": index, "feature_name": feature_name or None, "missing_fields": missing, "violations": sorted(set(violations))})

    required_features = {"critical_risk_status", "entry_quote_price", "exit_quote_price", "reference_price"}
    missing_required_features = sorted(required_features - seen_features)
    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.feature_vector_snapshot_policy.v1"
        and policy.get("failure_action") == "feature_leakage_detected"
        and bool(records)
        and not duplicate_features
        and not missing_required_features
        and not malformed_records
        and not source_errors
    )
    return _contract(
        "FeatureAvailabilityContract",
        passed,
        "feature_availability_missing_malformed_or_leaky",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "feature_availability_count": len(records),
            "duplicate_features": sorted(str(item) for item in duplicate_features),
            "missing_required_features": missing_required_features,
            "malformed_feature_availability": malformed_records,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_feature_vector_snapshot_contract(
    policy_path=DEFAULT_FEATURE_VECTOR_SNAPSHOT_POLICY,
    null_policy_path=DEFAULT_NULL_VALUE_POLICY,
):
    try:
        policy = _load_json(policy_path)
        null_policy = _load_json(null_policy_path)
    except Exception as exc:
        return _contract("FeatureVectorSnapshotContract", False, "feature_vector_snapshot_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("FeatureVectorSnapshotContract", False, "feature_vector_snapshot_policy_not_object", {"policy_path": str(policy_path)})
    if not isinstance(null_policy, dict):
        return _contract("FeatureVectorSnapshotContract", False, "null_value_policy_not_object", {"null_policy_path": str(null_policy_path)})

    null_fields = {
        str(item.get("field_name"))
        for item in null_policy.get("null_policies", [])
        if isinstance(item, dict) and item.get("field_name")
    }
    snapshots = policy.get("feature_vector_snapshots") if isinstance(policy.get("feature_vector_snapshots"), list) else []
    malformed_snapshots = []
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, dict):
            malformed_snapshots.append({"index": index, "feature_vector_hash": None, "missing_fields": list(FEATURE_VECTOR_SNAPSHOT_REQUIRED_FIELDS), "violations": ["feature_vector_snapshot_not_object"]})
            continue
        missing = _missing_required_fields(snapshot, FEATURE_VECTOR_SNAPSHOT_REQUIRED_FIELDS)
        violations = []
        feature_names = snapshot.get("feature_names_ordered") if isinstance(snapshot.get("feature_names_ordered"), list) else []
        feature_values = snapshot.get("feature_values_serialized") if isinstance(snapshot.get("feature_values_serialized"), dict) else {}
        feature_available_at_map = snapshot.get("feature_available_at_map") if isinstance(snapshot.get("feature_available_at_map"), dict) else {}
        if not feature_names:
            violations.append("feature_names_ordered_required")
        if set(str(name) for name in feature_names) != set(str(name) for name in feature_values):
            violations.append("feature_names_values_mismatch")
        if set(str(name) for name in feature_names) != set(str(name) for name in feature_available_at_map):
            violations.append("feature_names_availability_mismatch")
        decision_ts = _parse_iso_ts(snapshot.get("decision_ts"))
        if not decision_ts:
            violations.append("decision_ts_invalid")
        else:
            for feature_name, available_at in feature_available_at_map.items():
                parsed = _parse_iso_ts(available_at)
                if not parsed:
                    violations.append(f"{feature_name}_available_at_invalid")
                elif parsed > decision_ts:
                    violations.append(f"{feature_name}_available_after_decision")
        if str(snapshot.get("missing_value_policy") or "") not in null_fields:
            violations.append("missing_value_policy_unknown")
        if not isinstance(snapshot.get("source_lineage_node_ids"), list) or not snapshot.get("source_lineage_node_ids"):
            violations.append("source_lineage_node_ids_required")
        expected_hash = _hash_record_without(snapshot, "feature_vector_hash")
        if snapshot.get("feature_vector_hash") != expected_hash:
            violations.append("feature_vector_hash_mismatch")
        if missing or violations:
            malformed_snapshots.append({"index": index, "feature_vector_hash": snapshot.get("feature_vector_hash"), "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.feature_vector_snapshot_policy.v1"
        and policy.get("failure_action") == "feature_leakage_detected"
        and bool(snapshots)
        and not malformed_snapshots
        and not source_errors
    )
    return _contract(
        "FeatureVectorSnapshotContract",
        passed,
        "feature_vector_snapshot_missing_malformed_or_unreproducible",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "snapshot_count": len(snapshots),
            "malformed_feature_vector_snapshots": malformed_snapshots,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_data_lineage_graph_contract(policy_path=DEFAULT_DATA_LINEAGE_GRAPH_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("DataLineageGraphContract", False, "data_lineage_graph_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("DataLineageGraphContract", False, "data_lineage_graph_policy_not_object", {"policy_path": str(policy_path)})

    nodes = policy.get("lineage_nodes") if isinstance(policy.get("lineage_nodes"), list) else []
    node_ids = {str(node.get("lineage_node_id")) for node in nodes if isinstance(node, dict) and node.get("lineage_node_id")}
    node_types = {str(node.get("node_type")) for node in nodes if isinstance(node, dict) and node.get("node_type")}
    duplicate_node_ids = []
    seen_node_ids = set()
    malformed_nodes = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            malformed_nodes.append({"index": index, "lineage_node_id": None, "missing_fields": list(DATA_LINEAGE_NODE_REQUIRED_FIELDS), "violations": ["lineage_node_not_object"]})
            continue
        node_id = str(node.get("lineage_node_id") or "")
        missing = [
            field
            for field in DATA_LINEAGE_NODE_REQUIRED_FIELDS
            if field not in node
            or node.get(field) is None
            or (isinstance(node.get(field), str) and not node.get(field))
        ]
        violations = []
        if node_id in seen_node_ids:
            duplicate_node_ids.append(node_id)
        if node_id:
            seen_node_ids.add(node_id)
        if node_id and not re.match(r"^[a-z][a-z0-9_:-]*$", node_id):
            violations.append("lineage_node_id_invalid")
        if str(node.get("node_type") or "") not in REQUIRED_DATA_LINEAGE_NODE_TYPES:
            violations.append("node_type_not_required")
        parent_ids = node.get("parent_node_ids") if isinstance(node.get("parent_node_ids"), list) else []
        unknown_parent_ids = sorted(str(parent_id) for parent_id in parent_ids if str(parent_id) not in node_ids)
        if unknown_parent_ids:
            violations.append("unknown_parent_node_id")
        if str(node.get("edge_type") or "") not in {"root", "derived_from", "used_by", "generated", "supersedes", "excluded_by", "quarantined_by", "audited_by"}:
            violations.append("edge_type_not_allowed")
        if not _parse_iso_ts(node.get("created_at")):
            violations.append("created_at_invalid")
        if str(node.get("environment_id") or "") == "":
            violations.append("environment_id_required")
        expected_hash = _hash_record_without(node, "lineage_hash")
        if node.get("lineage_hash") != expected_hash:
            violations.append("lineage_hash_mismatch")
        if missing or violations:
            malformed_nodes.append({"index": index, "lineage_node_id": node_id or None, "missing_fields": missing, "violations": sorted(set(violations)), "unknown_parent_ids": unknown_parent_ids})

    missing_required_node_types = sorted(REQUIRED_DATA_LINEAGE_NODE_TYPES - node_types)
    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.data_lineage_graph_policy.v1"
        and policy.get("failure_action") == "sample_not_eligible_for_training"
        and bool(nodes)
        and not duplicate_node_ids
        and not missing_required_node_types
        and not malformed_nodes
        and not source_errors
    )
    return _contract(
        "DataLineageGraphContract",
        passed,
        "data_lineage_graph_missing_malformed_or_broken",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "lineage_node_count": len(nodes),
            "duplicate_node_ids": sorted(str(item) for item in duplicate_node_ids),
            "missing_required_node_types": missing_required_node_types,
            "malformed_lineage_nodes": malformed_nodes,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_training_dataset_manifest_contract(
    policy_path=DEFAULT_TRAINING_DATASET_MANIFEST_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
):
    try:
        policy = _load_json(policy_path)
        metric_registry_hash = _sha256_file(_resolve_project_file(metric_registry_path))
        threshold_catalog_hash = _sha256_file(_resolve_project_file(threshold_catalog_path))
        spec_report = validate_all(manifest_path=manifest_path, catalog_path=catalog_path, registry_path=registry_path)
        spec_error = None
    except Exception as exc:
        return _contract("TrainingDatasetManifestContract", False, "training_dataset_manifest_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("TrainingDatasetManifestContract", False, "training_dataset_manifest_policy_not_object", {"policy_path": str(policy_path)})

    manifests = policy.get("training_dataset_manifests") if isinstance(policy.get("training_dataset_manifests"), list) else []
    duplicate_dataset_ids = []
    seen_dataset_ids = set()
    malformed_manifests = []
    for index, manifest in enumerate(manifests):
        if not isinstance(manifest, dict):
            malformed_manifests.append({"index": index, "dataset_id": None, "missing_fields": list(TRAINING_DATASET_MANIFEST_REQUIRED_FIELDS), "violations": ["training_manifest_not_object"]})
            continue
        dataset_id = str(manifest.get("dataset_id") or "")
        missing = _missing_required_fields(manifest, TRAINING_DATASET_MANIFEST_REQUIRED_FIELDS)
        violations = []
        if dataset_id in seen_dataset_ids:
            duplicate_dataset_ids.append(dataset_id)
        if dataset_id:
            seen_dataset_ids.add(dataset_id)
        if dataset_id and not re.match(r"^[a-z][a-z0-9_:-]*$", dataset_id):
            violations.append("dataset_id_invalid")
        for field in ("included_sample_ids", "excluded_sample_ids"):
            if not isinstance(manifest.get(field), list):
                violations.append(f"{field}_must_be_list")
        if not manifest.get("included_sample_ids"):
            violations.append("included_sample_ids_required")
        if not isinstance(manifest.get("exclusion_reasons"), dict):
            violations.append("exclusion_reasons_must_be_object")
        if not isinstance(manifest.get("label_versions"), dict):
            violations.append("label_versions_must_be_object")
        if not isinstance(manifest.get("feature_versions"), dict):
            violations.append("feature_versions_must_be_object")
        if _parse_iso_ts(manifest.get("created_at")) is None:
            violations.append("created_at_invalid")
        if manifest.get("spec_hash") != spec_report.get("spec_hash"):
            violations.append("spec_hash_mismatch")
        if manifest.get("metric_registry_hash") != metric_registry_hash:
            violations.append("metric_registry_hash_mismatch")
        if manifest.get("threshold_catalog_hash") != threshold_catalog_hash:
            violations.append("threshold_catalog_hash_mismatch")
        observation_weights = manifest.get("observation_weights") if isinstance(manifest.get("observation_weights"), dict) else {}
        if manifest.get("observation_weights_hash") != _sha256_json(observation_weights):
            violations.append("observation_weights_hash_mismatch")
        expected_hash = _hash_record_without(manifest, "manifest_hash")
        if manifest.get("manifest_hash") != expected_hash:
            violations.append("manifest_hash_mismatch")
        if missing or violations:
            malformed_manifests.append({"index": index, "dataset_id": dataset_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    passed = (
        policy.get("schema_version") == "v2.7.0.training_dataset_manifest_policy.v1"
        and policy.get("failure_action") == "model_promotion_blocked"
        and bool(manifests)
        and not duplicate_dataset_ids
        and not malformed_manifests
        and not source_errors
        and not spec_error
    )
    return _contract(
        "TrainingDatasetManifestContract",
        passed,
        "training_dataset_manifest_missing_malformed_or_unlinked",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "manifest_count": len(manifests),
            "duplicate_dataset_ids": sorted(str(item) for item in duplicate_dataset_ids),
            "malformed_manifests": malformed_manifests,
            "spec_hash": spec_report.get("spec_hash"),
            "metric_registry_hash": metric_registry_hash,
            "threshold_catalog_hash": threshold_catalog_hash,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def _detector_threshold_passes(observed_value, operator, threshold_value):
    try:
        observed = Decimal(str(observed_value))
        threshold = Decimal(str(threshold_value))
    except (InvalidOperation, ValueError, TypeError):
        return False
    if operator == ">=":
        return observed >= threshold
    if operator == ">":
        return observed > threshold
    if operator == "<=":
        return observed <= threshold
    if operator == "<":
        return observed < threshold
    if operator == "==":
        return observed == threshold
    if operator == "!=":
        return observed != threshold
    return False


def _failed_detector_shadow_contracts(reason, evidence):
    return [
        _contract("ReclaimDetector", False, reason, evidence),
        _contract("OverextensionDetector", False, reason, evidence),
        _contract("DetectorCalibrationContract", False, reason, evidence),
    ]


def verify_detector_shadow_calibration_contracts(
    policy_path=DEFAULT_DETECTOR_SHADOW_CALIBRATION_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        return _failed_detector_shadow_contracts(
            "detector_shadow_calibration_policy_missing_or_invalid",
            {"policy_path": str(policy_path), "error": str(exc)},
        )
    if not isinstance(policy, dict):
        return _failed_detector_shadow_contracts(
            "detector_shadow_calibration_policy_not_object",
            {"policy_path": str(policy_path)},
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        return _failed_detector_shadow_contracts(
            "detector_shadow_calibration_policy_missing_or_invalid",
            {
                "policy_path": str(policy_path),
                "metric_registry_path": str(metric_registry_path),
                "threshold_catalog_path": str(threshold_catalog_path),
                "error": "metric_or_threshold_registry_not_object",
            },
        )

    metric_records = {
        str(metric.get("metric_id")): metric
        for metric in metric_registry.get("metrics", [])
        if isinstance(metric, dict) and metric.get("metric_id")
    }
    threshold_records = {
        str(threshold.get("threshold_id")): threshold
        for threshold in threshold_catalog.get("thresholds", [])
        if isinstance(threshold, dict) and threshold.get("threshold_id")
    }

    detectors = policy.get("detectors") if isinstance(policy.get("detectors"), list) else []
    calibrations = policy.get("calibrations") if isinstance(policy.get("calibrations"), list) else []
    detectors_by_id = {}
    malformed_detectors = []
    duplicate_detector_ids = []
    seen_detector_ids = set()
    for index, detector in enumerate(detectors):
        if not isinstance(detector, dict):
            malformed_detectors.append(
                {
                    "index": index,
                    "detector_id": None,
                    "contract_id": None,
                    "missing_fields": list(DETECTOR_SHADOW_REQUIRED_FIELDS),
                    "violations": ["detector_not_object"],
                }
            )
            continue
        detector_id = str(detector.get("detector_id") or "")
        contract_id = str(detector.get("contract_id") or "")
        if detector_id in seen_detector_ids:
            duplicate_detector_ids.append(detector_id)
        if detector_id:
            seen_detector_ids.add(detector_id)
            detectors_by_id[detector_id] = detector
        missing = _missing_required_fields(detector, DETECTOR_SHADOW_REQUIRED_FIELDS)
        violations = []
        spec = DETECTOR_SHADOW_CONTRACTS.get(detector_id)
        if not spec:
            violations.append("detector_id_not_registered_for_shadow_calibration")
        elif contract_id != spec["contract_id"]:
            violations.append("contract_id_mismatch")
        outputs = set(str(item) for item in detector.get("detector_output_states", []) if item)
        if spec and not spec["required_outputs"].issubset(outputs):
            violations.append("required_detector_outputs_missing")
        allowed_modes = set(str(item) for item in detector.get("allowed_modes", []) if item)
        if not {"observe_only", "shadow"}.issubset(allowed_modes):
            violations.append("allowed_modes_must_include_observe_only_and_shadow")
        if {"ultra_tiny", "normal_tiny"} & allowed_modes:
            violations.append("shadow_detector_cannot_allow_entry_modes")
        if detector.get("gate_allowed") is not False:
            violations.append("gate_allowed_must_be_false")
        if detector.get("feature_available_at_required") is not True:
            violations.append("feature_available_at_required_must_be_true")
        required_available_at = detector.get("required_feature_available_at_fields")
        if not isinstance(required_available_at, list) or not required_available_at:
            violations.append("required_feature_available_at_fields_required")
        if str(detector.get("source_event_type") or "") != "detector_shadow_event":
            violations.append("source_event_type_must_be_detector_shadow_event")
        if str(detector.get("failure_action") or "") != "cannot_be_normal_tiny_gate":
            violations.append("failure_action_must_prevent_normal_tiny_gate")
        threshold_ids = [str(item) for item in detector.get("threshold_ids", []) if item]
        metric_ids = [str(item) for item in detector.get("metric_ids", []) if item]
        if not threshold_ids:
            violations.append("threshold_ids_required")
        if not metric_ids:
            violations.append("metric_ids_required")
        for threshold_id in threshold_ids:
            if threshold_id not in threshold_records:
                violations.append("threshold_id_unknown")
                break
        for metric_id in metric_ids:
            if metric_id not in metric_records:
                violations.append("metric_id_unknown")
                break
        detector_hash = str(detector.get("detector_hash") or "")
        expected_detector_hash = _hash_record_without(detector, "detector_hash") if detector_id else None
        if detector_hash and expected_detector_hash and detector_hash != expected_detector_hash:
            violations.append("detector_hash_mismatch")
        if missing or violations:
            malformed_detectors.append(
                {
                    "index": index,
                    "detector_id": detector_id or None,
                    "contract_id": contract_id or None,
                    "missing_fields": missing,
                    "violations": sorted(set(violations)),
                }
            )

    calibrations_by_detector = {}
    malformed_calibrations = []
    duplicate_calibration_ids = []
    seen_calibration_ids = set()
    for index, calibration in enumerate(calibrations):
        if not isinstance(calibration, dict):
            malformed_calibrations.append(
                {
                    "index": index,
                    "calibration_id": None,
                    "detector_id": None,
                    "missing_fields": list(DETECTOR_CALIBRATION_REQUIRED_FIELDS),
                    "violations": ["calibration_not_object"],
                }
            )
            continue
        calibration_id = str(calibration.get("calibration_id") or "")
        detector_id = str(calibration.get("detector_id") or "")
        metric_id = str(calibration.get("metric_id") or "")
        threshold_id = str(calibration.get("threshold_id") or "")
        if calibration_id in seen_calibration_ids:
            duplicate_calibration_ids.append(calibration_id)
        if calibration_id:
            seen_calibration_ids.add(calibration_id)
        if detector_id:
            calibrations_by_detector.setdefault(detector_id, []).append(calibration)
        missing = _missing_required_fields(calibration, DETECTOR_CALIBRATION_REQUIRED_FIELDS)
        violations = []
        if detector_id not in DETECTOR_SHADOW_CONTRACTS:
            violations.append("detector_id_not_registered_for_calibration")
        if detector_id not in detectors_by_id:
            violations.append("detector_record_missing")
        metric = metric_records.get(metric_id)
        threshold = threshold_records.get(threshold_id)
        if not metric:
            violations.append("metric_id_unknown")
        if not threshold:
            violations.append("threshold_id_unknown")
        if threshold and str(threshold.get("applies_to_metric") or "") != metric_id:
            violations.append("threshold_metric_mismatch")
        operator = str(calibration.get("comparison_operator") or "")
        threshold_operator = str(threshold.get("comparison_operator") or "") if threshold else ""
        if operator not in THRESHOLD_COMPARISON_OPERATORS:
            violations.append("comparison_operator_not_allowed")
        if threshold and operator != threshold_operator:
            violations.append("comparison_operator_mismatch")
        if threshold and str(calibration.get("threshold_value")) != str(threshold.get("threshold_value")):
            violations.append("threshold_value_mismatch")
        if not _detector_threshold_passes(calibration.get("observed_value"), operator, calibration.get("threshold_value")):
            violations.append("observed_value_fails_threshold")
        try:
            if int(calibration.get("sample_n")) <= 0:
                violations.append("sample_n_must_be_positive")
        except (TypeError, ValueError):
            violations.append("sample_n_must_be_positive")
        try:
            if int(calibration.get("contaminated_sample_count")) != 0:
                violations.append("contaminated_samples_not_allowed")
        except (TypeError, ValueError):
            violations.append("contaminated_samples_not_allowed")
        if calibration.get("promotion_allowed") is not False:
            violations.append("promotion_allowed_must_be_false_for_seed_shadow_policy")
        if str(calibration.get("calibration_status") or "") not in DETECTOR_CALIBRATION_STATUSES:
            violations.append("calibration_status_not_allowed")
        if not _parse_iso_ts(calibration.get("checked_at")):
            violations.append("checked_at_invalid")
        calibration_hash = str(calibration.get("calibration_hash") or "")
        expected_calibration_hash = _hash_record_without(calibration, "calibration_hash") if calibration_id else None
        if calibration_hash and expected_calibration_hash and calibration_hash != expected_calibration_hash:
            violations.append("calibration_hash_mismatch")
        if missing or violations:
            malformed_calibrations.append(
                {
                    "index": index,
                    "calibration_id": calibration_id or None,
                    "detector_id": detector_id or None,
                    "metric_id": metric_id or None,
                    "threshold_id": threshold_id or None,
                    "missing_fields": missing,
                    "violations": sorted(set(violations)),
                }
            )

    missing_required_detectors = sorted(set(DETECTOR_SHADOW_CONTRACTS) - set(detectors_by_id))
    missing_calibration_detector_ids = sorted(
        detector_id for detector_id in DETECTOR_SHADOW_CONTRACTS if detector_id not in calibrations_by_detector
    )
    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.detector_shadow_calibration_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "detector_shadow_only":
        policy_errors.append("failure_action_must_be_detector_shadow_only")
    if duplicate_detector_ids:
        policy_errors.append("duplicate_detector_ids")
    if duplicate_calibration_ids:
        policy_errors.append("duplicate_calibration_ids")
    if missing_required_detectors:
        policy_errors.append("required_detectors_missing")
    if missing_calibration_detector_ids:
        policy_errors.append("required_calibrations_missing")
    if source_errors:
        policy_errors.append("source_file_check_failed")

    detector_evidence_base = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "detector_count": len(detectors),
        "calibration_count": len(calibrations),
        "missing_required_detectors": missing_required_detectors,
        "duplicate_detector_ids": sorted(str(item) for item in duplicate_detector_ids),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    reports = []
    for detector_id, spec in DETECTOR_SHADOW_CONTRACTS.items():
        detector_errors = [
            item
            for item in malformed_detectors
            if item.get("detector_id") in {None, detector_id} or item.get("contract_id") == spec["contract_id"]
        ]
        passed = (
            detector_id in detectors_by_id
            and not detector_errors
            and not policy_errors
            and not source_errors
        )
        evidence = {
            **detector_evidence_base,
            "detector_id": detector_id,
            "contract_id": spec["contract_id"],
            "detector": detectors_by_id.get(detector_id),
            "malformed_detector_records": detector_errors,
        }
        reports.append(_contract(spec["contract_id"], passed, spec["blocking_reason"], evidence))

    calibration_policy_errors = [
        error
        for error in policy_errors
        if error not in {"required_detectors_missing"}
    ]
    calibration_passed = (
        bool(calibrations)
        and not malformed_calibrations
        and not missing_calibration_detector_ids
        and not duplicate_calibration_ids
        and not calibration_policy_errors
        and not source_errors
    )
    reports.append(
        _contract(
            "DetectorCalibrationContract",
            calibration_passed,
            "detector_calibration_missing_malformed_or_contaminated",
            {
                **detector_evidence_base,
                "duplicate_calibration_ids": sorted(str(item) for item in duplicate_calibration_ids),
                "missing_calibration_detector_ids": missing_calibration_detector_ids,
                "malformed_calibrations": malformed_calibrations,
            },
        )
    )
    return reports


def _metric_threshold_maps(metric_registry, threshold_catalog):
    metrics = {
        str(metric.get("metric_id")): metric
        for metric in metric_registry.get("metrics", [])
        if isinstance(metric, dict) and metric.get("metric_id")
    }
    thresholds = {
        str(threshold.get("threshold_id")): threshold
        for threshold in threshold_catalog.get("thresholds", [])
        if isinstance(threshold, dict) and threshold.get("threshold_id")
    }
    return metrics, thresholds


def _policy_threshold_violation(record, metric_records, threshold_records, value_field):
    violations = []
    metric_id = str(record.get("metric_id") or "")
    threshold_id = str(record.get("threshold_id") or "")
    metric = metric_records.get(metric_id)
    threshold = threshold_records.get(threshold_id)
    if not metric:
        violations.append("metric_id_unknown")
    if not threshold:
        violations.append("threshold_id_unknown")
    if threshold and str(threshold.get("applies_to_metric") or "") != metric_id:
        violations.append("threshold_metric_mismatch")
    if threshold and not _detector_threshold_passes(record.get(value_field), threshold.get("comparison_operator"), threshold.get("threshold_value")):
        violations.append("record_value_fails_threshold")
    return violations


def _policy_metric_threshold_binding_violations(record, metric_records, threshold_records):
    violations = []
    metric_id = str(record.get("metric_id") or "")
    threshold_id = str(record.get("threshold_id") or "")
    metric = metric_records.get(metric_id)
    threshold = threshold_records.get(threshold_id)
    if not metric:
        violations.append("metric_id_unknown")
    if not threshold:
        violations.append("threshold_id_unknown")
    if threshold and str(threshold.get("applies_to_metric") or "") != metric_id:
        violations.append("threshold_metric_mismatch")
    return violations


def verify_capacity_load_latency_contracts(
    policy_path=DEFAULT_CAPACITY_LOAD_LATENCY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return [
            _contract("CapacityPlanningContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("LoadTestReplayContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("LatencyAttributionContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("ProviderQuotaIsolationContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("EconomicCostBudgetContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
        ]
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return [
            _contract("CapacityPlanningContract", False, "capacity_load_latency_policy_not_object", evidence),
            _contract("LoadTestReplayContract", False, "capacity_load_latency_policy_not_object", evidence),
            _contract("LatencyAttributionContract", False, "capacity_load_latency_policy_not_object", evidence),
            _contract("ProviderQuotaIsolationContract", False, "capacity_load_latency_policy_not_object", evidence),
            _contract("EconomicCostBudgetContract", False, "capacity_load_latency_policy_not_object", evidence),
        ]
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return [
            _contract("CapacityPlanningContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("LoadTestReplayContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("LatencyAttributionContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("ProviderQuotaIsolationContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
            _contract("EconomicCostBudgetContract", False, "capacity_load_latency_policy_missing_or_invalid", evidence),
        ]

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.capacity_load_latency_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "capacity_or_latency_degraded":
        policy_errors.append("failure_action_must_be_capacity_or_latency_degraded")

    capacity_plans = policy.get("capacity_plans") if isinstance(policy.get("capacity_plans"), list) else []
    malformed_capacity = []
    duplicate_capacity_ids = []
    capacity_components = set()
    seen_capacity_ids = set()
    for index, row in enumerate(capacity_plans):
        if not isinstance(row, dict):
            malformed_capacity.append({"index": index, "capacity_plan_id": None, "missing_fields": list(CAPACITY_PLAN_REQUIRED_FIELDS), "violations": ["capacity_plan_not_object"]})
            continue
        plan_id = str(row.get("capacity_plan_id") or "")
        component = str(row.get("component") or "")
        if plan_id in seen_capacity_ids:
            duplicate_capacity_ids.append(plan_id)
        if plan_id:
            seen_capacity_ids.add(plan_id)
        if component:
            capacity_components.add(component)
        missing = _missing_required_fields(row, CAPACITY_PLAN_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "headroom_pct")
        for field in ("expected_peak_qps", "measured_peak_qps", "p95_latency_budget_ms", "p99_latency_budget_ms", "queue_depth_limit", "headroom_pct", "degradation_threshold"):
            try:
                value = Decimal(str(row.get(field)))
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
                continue
            if value < 0:
                violations.append(f"{field}_nonnegative_required")
        try:
            if Decimal(str(row.get("p99_latency_budget_ms"))) < Decimal(str(row.get("p95_latency_budget_ms"))):
                violations.append("p99_latency_budget_lt_p95")
        except (InvalidOperation, ValueError, TypeError):
            pass
        try:
            if Decimal(str(row.get("headroom_pct"))) < Decimal(str(row.get("degradation_threshold"))):
                violations.append("headroom_below_degradation_threshold")
        except (InvalidOperation, ValueError, TypeError):
            pass
        priorities = set(str(item) for item in row.get("protects_priorities", []) if item)
        if not {"P0", "P1"}.issubset(priorities):
            violations.append("p0_p1_capacity_not_protected")
        if row.get("exit_safety_reserved") is not True:
            violations.append("exit_safety_reserved_required")
        if _parse_iso_ts(row.get("last_verified_at")) is None:
            violations.append("last_verified_at_invalid")
        expected_hash = _hash_record_without(row, "capacity_hash") if plan_id else None
        if row.get("capacity_hash") and expected_hash and row.get("capacity_hash") != expected_hash:
            violations.append("capacity_hash_mismatch")
        if missing or violations:
            malformed_capacity.append({"index": index, "capacity_plan_id": plan_id or None, "component": component or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_capacity_components = sorted(REQUIRED_CAPACITY_COMPONENTS - capacity_components)

    load_tests = policy.get("load_tests") if isinstance(policy.get("load_tests"), list) else []
    malformed_load_tests = []
    load_scenarios = set()
    for index, row in enumerate(load_tests):
        if not isinstance(row, dict):
            malformed_load_tests.append({"index": index, "load_test_id": None, "missing_fields": list(LOAD_TEST_REPLAY_REQUIRED_FIELDS), "violations": ["load_test_not_object"]})
            continue
        load_test_id = str(row.get("load_test_id") or "")
        missing = _missing_required_fields(row, LOAD_TEST_REPLAY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "pass_rate")
        scenarios = set(str(item) for item in row.get("synthetic_burst_profile", []) if item)
        load_scenarios.update(scenarios)
        if row.get("pass_fail") != "pass":
            violations.append("load_test_not_pass")
        expected = set(str(item) for item in row.get("expected_invariants", []) if item)
        observed = set(str(item) for item in row.get("observed_invariants", []) if item)
        if expected and not expected.issubset(observed):
            violations.append("expected_invariants_not_observed")
        if not isinstance(row.get("components_under_test"), list) or not row.get("components_under_test"):
            violations.append("components_under_test_required")
        try:
            if Decimal(str(row.get("replay_speed_multiplier"))) <= 0:
                violations.append("replay_speed_multiplier_positive_required")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("replay_speed_multiplier_positive_required")
        if _parse_iso_ts(row.get("run_at")) is None:
            violations.append("run_at_invalid")
        expected_hash = _hash_record_without(row, "load_test_hash") if load_test_id else None
        if row.get("load_test_hash") and expected_hash and row.get("load_test_hash") != expected_hash:
            violations.append("load_test_hash_mismatch")
        if missing or violations:
            malformed_load_tests.append({"index": index, "load_test_id": load_test_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_load_scenarios = sorted(REQUIRED_LOAD_TEST_SCENARIOS - load_scenarios)

    latencies = policy.get("latency_attributions") if isinstance(policy.get("latency_attributions"), list) else []
    malformed_latency = []
    latency_classes = set()
    for index, row in enumerate(latencies):
        if not isinstance(row, dict):
            malformed_latency.append({"index": index, "token_lifecycle_key": None, "missing_fields": list(LATENCY_ATTRIBUTION_REQUIRED_FIELDS), "violations": ["latency_attribution_not_object"]})
            continue
        token_key = str(row.get("token_lifecycle_key") or "")
        missing = _missing_required_fields(row, LATENCY_ATTRIBUTION_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "sla_breach_rate")
        latency_class = str(row.get("latency_class") or "")
        latency_classes.add(latency_class)
        if latency_class not in LATENCY_CLASSES:
            violations.append("latency_class_not_allowed")
        try:
            if int(row.get("latency_ms")) < 0:
                violations.append("latency_ms_nonnegative_required")
        except (TypeError, ValueError):
            violations.append("latency_ms_nonnegative_required")
        chronological_fields = [
            "signal_seen_at",
            "signal_available_at",
            "pool_resolved_available_at",
            "quote_available_at",
            "risk_available_at",
            "reclaim_available_at",
            "decision_started_at",
            "decision_available_at",
            "queued_at",
            "claimed_at",
            "quote_refreshed_at",
            "simulated_fill_ts",
            "peak_ts",
        ]
        parsed = [(_parse_iso_ts(row.get(field)), field) for field in chronological_fields]
        if any(ts is None for ts, _field in parsed):
            violations.append("latency_timestamp_invalid")
        else:
            for (left_ts, left_field), (right_ts, right_field) in zip(parsed, parsed[1:]):
                if left_ts > right_ts:
                    violations.append(f"{left_field}_after_{right_field}")
                    break
        expected_hash = _hash_record_without(row, "latency_hash") if token_key else None
        if row.get("latency_hash") and expected_hash and row.get("latency_hash") != expected_hash:
            violations.append("latency_hash_mismatch")
        if missing or violations:
            malformed_latency.append({"index": index, "token_lifecycle_key": token_key or None, "latency_class": latency_class or None, "missing_fields": missing, "violations": sorted(set(violations))})

    quota_rows = policy.get("provider_quota_isolation") if isinstance(policy.get("provider_quota_isolation"), list) else []
    malformed_quota = []
    exit_first_quota_count = 0
    for index, row in enumerate(quota_rows):
        if not isinstance(row, dict):
            malformed_quota.append({"index": index, "provider": None, "missing_fields": list(PROVIDER_QUOTA_ISOLATION_REQUIRED_FIELDS), "violations": ["quota_record_not_object"]})
            continue
        provider = str(row.get("provider") or "")
        missing = _missing_required_fields(row, PROVIDER_QUOTA_ISOLATION_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "exit_safety_reserved_pct")
        priority_order = [str(item) for item in row.get("priority_order", []) if item]
        if priority_order[:2] != ["open_position_exit", "pending_entry_quote"]:
            violations.append("exit_and_entry_priorities_not_first")
        else:
            exit_first_quota_count += 1
        for field in ("quota_limit_per_min", "current_usage_per_min", "shadow_polling_limit_per_min"):
            try:
                if Decimal(str(row.get(field))) < 0:
                    violations.append(f"{field}_nonnegative_required")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        try:
            if Decimal(str(row.get("current_usage_per_min"))) > Decimal(str(row.get("quota_limit_per_min"))):
                violations.append("current_usage_exceeds_quota_limit")
        except (InvalidOperation, ValueError, TypeError):
            pass
        if str(row.get("quota_isolation_status") or "") != "healthy":
            violations.append("quota_isolation_status_not_healthy")
        expected_hash = _hash_record_without(row, "quota_hash") if provider else None
        if row.get("quota_hash") and expected_hash and row.get("quota_hash") != expected_hash:
            violations.append("quota_hash_mismatch")
        if missing or violations:
            malformed_quota.append({"index": index, "provider": provider or None, "missing_fields": missing, "violations": sorted(set(violations))})

    budgets = policy.get("economic_cost_budgets") if isinstance(policy.get("economic_cost_budgets"), list) else []
    malformed_budgets = []
    budget_pools = set()
    for index, row in enumerate(budgets):
        if not isinstance(row, dict):
            malformed_budgets.append({"index": index, "budget_id": None, "missing_fields": list(ECONOMIC_COST_BUDGET_REQUIRED_FIELDS), "violations": ["budget_record_not_object"]})
            continue
        budget_id = str(row.get("budget_id") or "")
        budget_pools.add(str(row.get("budget_pool") or ""))
        missing = _missing_required_fields(row, ECONOMIC_COST_BUDGET_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "utilization")
        for field in ("soft_limit", "hard_limit", "current_usage", "measurement_window_ms"):
            try:
                if Decimal(str(row.get(field))) < 0:
                    violations.append(f"{field}_nonnegative_required")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        try:
            if Decimal(str(row.get("soft_limit"))) > Decimal(str(row.get("hard_limit"))):
                violations.append("soft_limit_exceeds_hard_limit")
            if Decimal(str(row.get("current_usage"))) > Decimal(str(row.get("hard_limit"))):
                violations.append("current_usage_exceeds_hard_limit")
        except (InvalidOperation, ValueError, TypeError):
            pass
        if row.get("exit_safety_reserved") is not True and str(row.get("budget_pool") or "") == "exit_safety_budget":
            violations.append("exit_safety_budget_must_be_reserved")
        if str(row.get("budget_pool") or "") == "exploration_budget" and "exit_safety" in set(str(item) for item in row.get("reserved_for", []) if item):
            violations.append("exploration_budget_cannot_reserve_exit_safety")
        expected_hash = _hash_record_without(row, "budget_hash") if budget_id else None
        if row.get("budget_hash") and expected_hash and row.get("budget_hash") != expected_hash:
            violations.append("budget_hash_mismatch")
        if missing or violations:
            malformed_budgets.append({"index": index, "budget_id": budget_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    capacity_passed = (
        bool(capacity_plans)
        and not policy_errors
        and not source_errors
        and not malformed_capacity
        and not duplicate_capacity_ids
        and not missing_capacity_components
    )
    load_passed = (
        bool(load_tests)
        and not policy_errors
        and not source_errors
        and not malformed_load_tests
        and not missing_load_scenarios
    )
    latency_passed = bool(latencies) and not policy_errors and not source_errors and not malformed_latency
    quota_passed = bool(quota_rows) and exit_first_quota_count == len(quota_rows) and not policy_errors and not source_errors and not malformed_quota
    budget_passed = bool(budgets) and {"exit_safety_budget", "exploration_budget"}.issubset(budget_pools) and not policy_errors and not source_errors and not malformed_budgets
    return [
        _contract(
            "CapacityPlanningContract",
            capacity_passed,
            "capacity_planning_missing_malformed_or_insufficient",
            {
                **common,
                "capacity_plan_count": len(capacity_plans),
                "missing_capacity_components": missing_capacity_components,
                "duplicate_capacity_ids": sorted(str(item) for item in duplicate_capacity_ids),
                "malformed_capacity_plans": malformed_capacity,
            },
        ),
        _contract(
            "LoadTestReplayContract",
            load_passed,
            "load_test_replay_missing_malformed_or_failed",
            {
                **common,
                "load_test_count": len(load_tests),
                "missing_load_scenarios": missing_load_scenarios,
                "malformed_load_tests": malformed_load_tests,
            },
        ),
        _contract(
            "LatencyAttributionContract",
            latency_passed,
            "latency_attribution_missing_malformed_or_late",
            {
                **common,
                "latency_attribution_count": len(latencies),
                "latency_classes": sorted(item for item in latency_classes if item),
                "malformed_latency_attributions": malformed_latency,
            },
        ),
        _contract(
            "ProviderQuotaIsolationContract",
            quota_passed,
            "provider_quota_isolation_missing_malformed_or_unreserved",
            {
                **common,
                "quota_record_count": len(quota_rows),
                "exit_first_quota_count": exit_first_quota_count,
                "malformed_quota_records": malformed_quota,
            },
        ),
        _contract(
            "EconomicCostBudgetContract",
            budget_passed,
            "economic_cost_budget_missing_malformed_or_overrun",
            {
                **common,
                "budget_count": len(budgets),
                "budget_pools": sorted(item for item in budget_pools if item),
                "malformed_budgets": malformed_budgets,
            },
        ),
    ]


def verify_operator_runtime_safety_contracts(
    policy_path=DEFAULT_OPERATOR_RUNTIME_SAFETY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return [
            _contract("OperatorAudit", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("OperatorSafetyContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("OwnershipOnCallContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("AlertPolicy", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("AlertAckEscalationPolicy", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("KillSwitchDrillContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
        ]
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return [
            _contract("OperatorAudit", False, "operator_runtime_safety_policy_not_object", evidence),
            _contract("OperatorSafetyContract", False, "operator_runtime_safety_policy_not_object", evidence),
            _contract("OwnershipOnCallContract", False, "operator_runtime_safety_policy_not_object", evidence),
            _contract("AlertPolicy", False, "operator_runtime_safety_policy_not_object", evidence),
            _contract("AlertAckEscalationPolicy", False, "operator_runtime_safety_policy_not_object", evidence),
            _contract("KillSwitchDrillContract", False, "operator_runtime_safety_policy_not_object", evidence),
        ]
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return [
            _contract("OperatorAudit", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("OperatorSafetyContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("OwnershipOnCallContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("AlertPolicy", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("AlertAckEscalationPolicy", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
            _contract("KillSwitchDrillContract", False, "operator_runtime_safety_policy_missing_or_invalid", evidence),
        ]

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.operator_runtime_safety_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "operator_or_kill_switch_safety_degraded":
        policy_errors.append("failure_action_must_be_operator_or_kill_switch_safety_degraded")

    operator_audits = policy.get("operator_audits") if isinstance(policy.get("operator_audits"), list) else []
    malformed_operator_audits = []
    duplicate_audit_event_ids = []
    audit_event_ids = set()
    for index, row in enumerate(operator_audits):
        if not isinstance(row, dict):
            malformed_operator_audits.append({"index": index, "audit_event_id": None, "missing_fields": list(OPERATOR_AUDIT_REQUIRED_FIELDS), "violations": ["operator_audit_not_object"]})
            continue
        audit_event_id = str(row.get("audit_event_id") or "")
        if audit_event_id in audit_event_ids:
            duplicate_audit_event_ids.append(audit_event_id)
        if audit_event_id:
            audit_event_ids.add(audit_event_id)
        missing = _missing_required_fields(row, OPERATOR_AUDIT_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if _parse_iso_ts(row.get("timestamp")) is None:
            violations.append("timestamp_invalid")
        if row.get("approval_required") is not True:
            violations.append("approval_required_must_be_true_for_operator_runtime_safety")
        if str(row.get("approval_status") or "") != "approved":
            violations.append("approval_status_not_approved")
        if not str(row.get("environment_id") or "").strip():
            violations.append("environment_id_required")
        expected_hash = _hash_record_without(row, "operator_audit_hash") if audit_event_id else None
        if row.get("operator_audit_hash") and expected_hash and row.get("operator_audit_hash") != expected_hash:
            violations.append("operator_audit_hash_mismatch")
        if missing or violations:
            malformed_operator_audits.append({"index": index, "audit_event_id": audit_event_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    safety_checks = policy.get("operator_safety_checks") if isinstance(policy.get("operator_safety_checks"), list) else []
    malformed_safety_checks = []
    unsafe_allowed_count = 0
    for index, row in enumerate(safety_checks):
        if not isinstance(row, dict):
            malformed_safety_checks.append({"index": index, "safety_check_id": None, "missing_fields": list(OPERATOR_SAFETY_REQUIRED_FIELDS), "violations": ["operator_safety_check_not_object"]})
            continue
        safety_check_id = str(row.get("safety_check_id") or "")
        missing = _missing_required_fields(row, OPERATOR_SAFETY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        danger_level = str(row.get("danger_level") or "")
        status = str(row.get("operator_safety_status") or "")
        if danger_level not in OPERATOR_DANGER_LEVELS:
            violations.append("danger_level_not_allowed")
        if status not in OPERATOR_SAFETY_STATUSES:
            violations.append("operator_safety_status_not_allowed")
        if str(row.get("audit_event_id") or "") not in audit_event_ids:
            violations.append("audit_event_id_missing")
        high_danger = danger_level in {"admin_mutation", "critical"}
        if high_danger:
            if row.get("dashboard_freshness_ok") is not True:
                violations.append("dashboard_freshness_required_for_high_danger")
            if row.get("required_runbook_ack") is not True:
                violations.append("runbook_ack_required_for_high_danger")
            if row.get("required_second_approval") is not True:
                violations.append("second_approval_required_for_high_danger")
            if row.get("confirmation_phrase_required") is not True:
                violations.append("confirmation_phrase_required_for_high_danger")
        if row.get("cooldown_ok") is not True:
            violations.append("cooldown_not_ok")
        if not isinstance(row.get("blast_radius_preview"), dict) or not row.get("blast_radius_preview"):
            violations.append("blast_radius_preview_required")
        if status == "safe" and violations:
            unsafe_allowed_count += 1
        expected_hash = _hash_record_without(row, "safety_hash") if safety_check_id else None
        if row.get("safety_hash") and expected_hash and row.get("safety_hash") != expected_hash:
            violations.append("safety_hash_mismatch")
        if missing or violations:
            malformed_safety_checks.append({"index": index, "safety_check_id": safety_check_id or None, "action": row.get("action"), "missing_fields": missing, "violations": sorted(set(violations))})

    ownership_rows = policy.get("ownership_oncall") if isinstance(policy.get("ownership_oncall"), list) else []
    malformed_ownership_rows = []
    ownership_components = set()
    for index, row in enumerate(ownership_rows):
        if not isinstance(row, dict):
            malformed_ownership_rows.append({"index": index, "component": None, "missing_fields": list(OWNERSHIP_ONCALL_REQUIRED_FIELDS), "violations": ["ownership_oncall_not_object"]})
            continue
        component = str(row.get("component") or "")
        if component:
            ownership_components.add(component)
        missing = _missing_required_fields(row, OWNERSHIP_ONCALL_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            if int(row.get("ack_sla_minutes")) <= 0:
                violations.append("ack_sla_minutes_positive_required")
            if int(row.get("resolution_sla_minutes")) <= 0:
                violations.append("resolution_sla_minutes_positive_required")
        except (TypeError, ValueError):
            violations.append("sla_minutes_numeric_required")
        if str(row.get("oncall_primary") or "") == str(row.get("oncall_secondary") or ""):
            violations.append("primary_secondary_oncall_must_differ")
        if _parse_iso_ts(row.get("last_reviewed_at")) is None:
            violations.append("last_reviewed_at_invalid")
        if not str(row.get("runbook_url") or "").strip():
            violations.append("runbook_url_required")
        expected_hash = _hash_record_without(row, "ownership_hash") if component else None
        if row.get("ownership_hash") and expected_hash and row.get("ownership_hash") != expected_hash:
            violations.append("ownership_hash_mismatch")
        if missing or violations:
            malformed_ownership_rows.append({"index": index, "component": component or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_ownership_components = sorted(REQUIRED_OWNERSHIP_COMPONENTS - ownership_components)

    alert_policies = policy.get("alert_policies") if isinstance(policy.get("alert_policies"), list) else []
    malformed_alert_policies = []
    duplicate_alert_ids = []
    alert_ids = set()
    for index, row in enumerate(alert_policies):
        if not isinstance(row, dict):
            malformed_alert_policies.append({"index": index, "alert_id": None, "missing_fields": list(ALERT_POLICY_REQUIRED_FIELDS), "violations": ["alert_policy_not_object"]})
            continue
        alert_id = str(row.get("alert_id") or "")
        if alert_id in alert_ids:
            duplicate_alert_ids.append(alert_id)
        if alert_id:
            alert_ids.add(alert_id)
        missing = _missing_required_fields(row, ALERT_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        severity = str(row.get("severity") or "")
        if severity not in {"P0", "P1"}:
            violations.append("severity_not_allowed")
        if str(row.get("owner_component") or "") not in ownership_components:
            violations.append("owner_component_missing_from_oncall")
        auto_actions = set(str(item) for item in row.get("auto_action", []) if item)
        if severity == "P0" and "new_entry_disabled" not in auto_actions:
            violations.append("p0_alert_must_disable_new_entry")
        if severity == "P0" and "exit_safety_preserved" not in auto_actions:
            violations.append("p0_alert_must_preserve_exit_safety")
        expected_hash = _hash_record_without(row, "alert_hash") if alert_id else None
        if row.get("alert_hash") and expected_hash and row.get("alert_hash") != expected_hash:
            violations.append("alert_hash_mismatch")
        if missing or violations:
            malformed_alert_policies.append({"index": index, "alert_id": alert_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_alert_ids = sorted(REQUIRED_ALERT_IDS - alert_ids)

    alert_acks = policy.get("alert_ack_escalations") if isinstance(policy.get("alert_ack_escalations"), list) else []
    malformed_alert_acks = []
    for index, row in enumerate(alert_acks):
        if not isinstance(row, dict):
            malformed_alert_acks.append({"index": index, "alert_instance_id": None, "missing_fields": list(ALERT_ACK_ESCALATION_REQUIRED_FIELDS), "violations": ["alert_ack_escalation_not_object"]})
            continue
        alert_instance_id = str(row.get("alert_instance_id") or "")
        missing = _missing_required_fields(row, ALERT_ACK_ESCALATION_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        alert_id = str(row.get("alert_id") or "")
        if alert_id not in alert_ids:
            violations.append("alert_id_missing_from_policy")
        severity = str(row.get("severity") or "")
        created_at = _parse_iso_ts(row.get("created_at"))
        required_by = _parse_iso_ts(row.get("ack_required_by"))
        acked_at = _parse_iso_ts(row.get("acked_at"))
        resolved_at = _parse_iso_ts(row.get("resolved_at"))
        if None in {created_at, required_by, acked_at, resolved_at}:
            violations.append("alert_ack_timestamp_invalid")
        else:
            if not (created_at <= acked_at <= required_by <= resolved_at):
                violations.append("alert_ack_chronology_invalid")
            max_ack_seconds = 300 if severity == "P0" else 1800 if severity == "P1" else None
            if max_ack_seconds is None:
                violations.append("severity_not_allowed")
            elif (required_by - created_at).total_seconds() > max_ack_seconds:
                violations.append("ack_required_by_exceeds_sla")
        if row.get("ack_sla_met") is not True:
            violations.append("ack_sla_not_met")
        if not str(row.get("resolution_note") or "").strip():
            violations.append("resolution_note_required")
        if not str(row.get("escalation_target") or "").strip():
            violations.append("escalation_target_required")
        expected_hash = _hash_record_without(row, "alert_ack_hash") if alert_instance_id else None
        if row.get("alert_ack_hash") and expected_hash and row.get("alert_ack_hash") != expected_hash:
            violations.append("alert_ack_hash_mismatch")
        if missing or violations:
            malformed_alert_acks.append({"index": index, "alert_instance_id": alert_instance_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    kill_switch_drills = policy.get("kill_switch_drills") if isinstance(policy.get("kill_switch_drills"), list) else []
    malformed_kill_switch_drills = []
    kill_switch_types = set()
    for index, row in enumerate(kill_switch_drills):
        if not isinstance(row, dict):
            malformed_kill_switch_drills.append({"index": index, "drill_id": None, "missing_fields": list(KILL_SWITCH_DRILL_REQUIRED_FIELDS), "violations": ["kill_switch_drill_not_object"]})
            continue
        drill_id = str(row.get("drill_id") or "")
        kill_switch_type = str(row.get("kill_switch_type") or "")
        if kill_switch_type:
            kill_switch_types.add(kill_switch_type)
        missing = _missing_required_fields(row, KILL_SWITCH_DRILL_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        started_at = _parse_iso_ts(row.get("started_at"))
        completed_at = _parse_iso_ts(row.get("completed_at"))
        if started_at is None or completed_at is None or started_at > completed_at:
            violations.append("kill_switch_drill_timestamp_invalid")
        if row.get("pass_fail") != "pass":
            violations.append("kill_switch_drill_not_pass")
        if row.get("open_positions_policy_checked") is not True:
            violations.append("open_positions_policy_not_checked")
        if row.get("exit_safety_preserved") is not True:
            violations.append("exit_safety_not_preserved")
        if row.get("new_entry_blocked") is not True:
            violations.append("new_entry_not_blocked")
        if row.get("recovery_steps_verified") is not True:
            violations.append("recovery_steps_not_verified")
        expected_effect = set(str(item) for item in row.get("expected_effect", []) if item)
        observed_effect = set(str(item) for item in row.get("observed_effect", []) if item)
        if expected_effect and not expected_effect.issubset(observed_effect):
            violations.append("expected_effect_not_observed")
        expected_hash = _hash_record_without(row, "evidence_hash") if drill_id else None
        if row.get("evidence_hash") and expected_hash and row.get("evidence_hash") != expected_hash:
            violations.append("kill_switch_evidence_hash_mismatch")
        if missing or violations:
            malformed_kill_switch_drills.append({"index": index, "drill_id": drill_id or None, "kill_switch_type": kill_switch_type or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_kill_switch_types = sorted(REQUIRED_KILL_SWITCH_TYPES - kill_switch_types)

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    audit_passed = bool(operator_audits) and not policy_errors and not source_errors and not malformed_operator_audits and not duplicate_audit_event_ids
    safety_passed = bool(safety_checks) and not policy_errors and not source_errors and not malformed_safety_checks and unsafe_allowed_count == 0
    ownership_passed = bool(ownership_rows) and not policy_errors and not source_errors and not malformed_ownership_rows and not missing_ownership_components
    alert_policy_passed = bool(alert_policies) and not policy_errors and not source_errors and not malformed_alert_policies and not duplicate_alert_ids and not missing_alert_ids
    alert_ack_passed = bool(alert_acks) and not policy_errors and not source_errors and not malformed_alert_acks
    kill_switch_passed = bool(kill_switch_drills) and not policy_errors and not source_errors and not malformed_kill_switch_drills and not missing_kill_switch_types
    return [
        _contract(
            "OperatorAudit",
            audit_passed,
            "operator_audit_missing_malformed_or_incomplete",
            {
                **common,
                "operator_audit_count": len(operator_audits),
                "duplicate_audit_event_ids": sorted(str(item) for item in duplicate_audit_event_ids),
                "malformed_operator_audits": malformed_operator_audits,
            },
        ),
        _contract(
            "OperatorSafetyContract",
            safety_passed,
            "operator_safety_missing_malformed_or_unsafe",
            {
                **common,
                "operator_safety_check_count": len(safety_checks),
                "unsafe_allowed_count": unsafe_allowed_count,
                "malformed_safety_checks": malformed_safety_checks,
            },
        ),
        _contract(
            "OwnershipOnCallContract",
            ownership_passed,
            "ownership_oncall_missing_malformed_or_unowned",
            {
                **common,
                "ownership_component_count": len(ownership_components),
                "missing_ownership_components": missing_ownership_components,
                "malformed_ownership_rows": malformed_ownership_rows,
            },
        ),
        _contract(
            "AlertPolicy",
            alert_policy_passed,
            "alert_policy_missing_malformed_or_incomplete",
            {
                **common,
                "alert_policy_count": len(alert_policies),
                "missing_alert_ids": missing_alert_ids,
                "duplicate_alert_ids": sorted(str(item) for item in duplicate_alert_ids),
                "malformed_alert_policies": malformed_alert_policies,
            },
        ),
        _contract(
            "AlertAckEscalationPolicy",
            alert_ack_passed,
            "alert_ack_escalation_missing_malformed_or_unacked",
            {
                **common,
                "alert_ack_count": len(alert_acks),
                "malformed_alert_acks": malformed_alert_acks,
            },
        ),
        _contract(
            "KillSwitchDrillContract",
            kill_switch_passed,
            "kill_switch_drill_missing_malformed_or_failed",
            {
                **common,
                "kill_switch_drill_count": len(kill_switch_drills),
                "missing_kill_switch_types": missing_kill_switch_types,
                "malformed_kill_switch_drills": malformed_kill_switch_drills,
            },
        ),
    ]


def verify_replay_build_model_contracts(
    policy_path=DEFAULT_REPLAY_BUILD_MODEL_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return [
            _contract("ReplayDeterminismCheck", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ReproducibleBuildContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("SupplyChainSecurityContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("PolicyBundleCompatibilityContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ModelExpiryContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ForecastSanityGuard", False, "replay_build_model_policy_missing_or_invalid", evidence),
        ]
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return [
            _contract("ReplayDeterminismCheck", False, "replay_build_model_policy_not_object", evidence),
            _contract("ReproducibleBuildContract", False, "replay_build_model_policy_not_object", evidence),
            _contract("SupplyChainSecurityContract", False, "replay_build_model_policy_not_object", evidence),
            _contract("PolicyBundleCompatibilityContract", False, "replay_build_model_policy_not_object", evidence),
            _contract("ModelExpiryContract", False, "replay_build_model_policy_not_object", evidence),
            _contract("ForecastSanityGuard", False, "replay_build_model_policy_not_object", evidence),
        ]
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return [
            _contract("ReplayDeterminismCheck", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ReproducibleBuildContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("SupplyChainSecurityContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("PolicyBundleCompatibilityContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ModelExpiryContract", False, "replay_build_model_policy_missing_or_invalid", evidence),
            _contract("ForecastSanityGuard", False, "replay_build_model_policy_missing_or_invalid", evidence),
        ]

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.replay_build_model_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "replay_build_model_promotion_blocked":
        policy_errors.append("failure_action_must_be_replay_build_model_promotion_blocked")

    replay_checks = policy.get("replay_determinism_checks") if isinstance(policy.get("replay_determinism_checks"), list) else []
    malformed_replay_checks = []
    replay_model_snapshot_ids = set()
    for index, row in enumerate(replay_checks):
        if not isinstance(row, dict):
            malformed_replay_checks.append({"index": index, "replay_check_id": None, "missing_fields": list(REPLAY_DETERMINISM_REQUIRED_FIELDS), "violations": ["replay_check_not_object"]})
            continue
        replay_check_id = str(row.get("replay_check_id") or "")
        replay_model_snapshot_ids.add(str(row.get("model_snapshot_id") or ""))
        missing = _missing_required_fields(row, REPLAY_DETERMINISM_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("pass_fail") != "pass":
            violations.append("replay_check_not_pass")
        for field in (
            "event_log_hash",
            "policy_manifest_hash",
            "threshold_catalog_hash",
            "metric_registry_hash",
            "runtime_config_hash",
            "build_hash",
            "spec_hash",
            "decision_hash",
            "forecast_hash",
            "outcome_hash",
            "ledger_hash",
            "feature_vector_hash",
        ):
            if not _sha256_hex_like(row.get(field)):
                violations.append(f"{field}_must_be_sha256")
        expected_hash = _hash_record_without(row, "replay_hash") if replay_check_id else None
        if row.get("replay_hash") and expected_hash and row.get("replay_hash") != expected_hash:
            violations.append("replay_hash_mismatch")
        if missing or violations:
            malformed_replay_checks.append({"index": index, "replay_check_id": replay_check_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    builds = policy.get("reproducible_builds") if isinstance(policy.get("reproducible_builds"), list) else []
    malformed_builds = []
    build_ids = set()
    build_hashes = set()
    for index, row in enumerate(builds):
        if not isinstance(row, dict):
            malformed_builds.append({"index": index, "build_id": None, "missing_fields": list(REPRODUCIBLE_BUILD_REQUIRED_FIELDS), "violations": ["reproducible_build_not_object"]})
            continue
        build_id = str(row.get("build_id") or "")
        if build_id:
            build_ids.add(build_id)
        build_hashes.add(str(row.get("reproducible_build_hash") or ""))
        missing = _missing_required_fields(row, REPRODUCIBLE_BUILD_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        for field in ("code_commit_hash", "dependency_lock_hash", "container_image_hash", "feature_code_hash", "model_code_hash"):
            if not _sha256_hex_like(row.get(field)):
                violations.append(f"{field}_must_be_sha256")
        if _parse_iso_ts(row.get("build_created_at")) is None:
            violations.append("build_created_at_invalid")
        if not str(row.get("runtime_version") or "").strip():
            violations.append("runtime_version_required")
        expected_hash = _hash_record_without(row, "reproducible_build_hash") if build_id else None
        if row.get("reproducible_build_hash") and expected_hash and row.get("reproducible_build_hash") != expected_hash:
            violations.append("reproducible_build_hash_mismatch")
        if missing or violations:
            malformed_builds.append({"index": index, "build_id": build_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    artifacts = policy.get("supply_chain_artifacts") if isinstance(policy.get("supply_chain_artifacts"), list) else []
    malformed_artifacts = []
    artifact_types = set()
    for index, row in enumerate(artifacts):
        if not isinstance(row, dict):
            malformed_artifacts.append({"index": index, "artifact_id": None, "missing_fields": list(SUPPLY_CHAIN_ARTIFACT_REQUIRED_FIELDS), "violations": ["supply_chain_artifact_not_object"]})
            continue
        artifact_id = str(row.get("artifact_id") or "")
        artifact_type = str(row.get("artifact_type") or "")
        artifact_types.add(artifact_type)
        missing = _missing_required_fields(row, SUPPLY_CHAIN_ARTIFACT_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if artifact_type not in SUPPLY_CHAIN_ARTIFACT_TYPES:
            violations.append("artifact_type_not_allowed")
        for field in ("code_commit_hash", "dependency_lock_hash", "container_image_hash", "SBOM_hash"):
            if not _sha256_hex_like(row.get(field)):
                violations.append(f"{field}_must_be_sha256")
        if row.get("signature_status") != "verified":
            violations.append("signature_not_verified")
        if row.get("vulnerability_scan_status") != "pass":
            violations.append("vulnerability_scan_not_pass")
        if row.get("provenance_attestation") != "verified":
            violations.append("provenance_attestation_not_verified")
        if not str(row.get("approved_builder") or "").strip():
            violations.append("approved_builder_required")
        if _parse_iso_ts(row.get("created_at")) is None:
            violations.append("created_at_invalid")
        expected_hash = _hash_record_without(row, "artifact_hash") if artifact_id else None
        if row.get("artifact_hash") and expected_hash and row.get("artifact_hash") != expected_hash:
            violations.append("artifact_hash_mismatch")
        if missing or violations:
            malformed_artifacts.append({"index": index, "artifact_id": artifact_id or None, "artifact_type": artifact_type or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_artifact_types = sorted(SUPPLY_CHAIN_ARTIFACT_TYPES - artifact_types)

    bundles = policy.get("policy_bundle_compatibility") if isinstance(policy.get("policy_bundle_compatibility"), list) else []
    malformed_bundles = []
    bundle_model_snapshot_ids = set()
    for index, row in enumerate(bundles):
        if not isinstance(row, dict):
            malformed_bundles.append({"index": index, "policy_bundle_id": None, "missing_fields": list(POLICY_BUNDLE_COMPATIBILITY_REQUIRED_FIELDS), "violations": ["policy_bundle_compatibility_not_object"]})
            continue
        policy_bundle_id = str(row.get("policy_bundle_id") or "")
        bundle_model_snapshot_ids.add(str(row.get("model_snapshot_id") or ""))
        missing = _missing_required_fields(row, POLICY_BUNDLE_COMPATIBILITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("compatibility_status") != "compatible":
            violations.append("policy_bundle_not_compatible")
        for field in POLICY_BUNDLE_COMPATIBILITY_REQUIRED_FIELDS:
            if field.endswith("_version") and not str(row.get(field) or "").strip():
                violations.append(f"{field}_required")
        expected_hash = _hash_record_without(row, "compatibility_hash") if policy_bundle_id else None
        if row.get("compatibility_hash") and expected_hash and row.get("compatibility_hash") != expected_hash:
            violations.append("compatibility_hash_mismatch")
        if missing or violations:
            malformed_bundles.append({"index": index, "policy_bundle_id": policy_bundle_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    model_expiry_rows = policy.get("model_expiry") if isinstance(policy.get("model_expiry"), list) else []
    malformed_model_expiry = []
    active_model_snapshot_ids = set()
    for index, row in enumerate(model_expiry_rows):
        if not isinstance(row, dict):
            malformed_model_expiry.append({"index": index, "model_snapshot_id": None, "missing_fields": list(MODEL_EXPIRY_REQUIRED_FIELDS), "violations": ["model_expiry_not_object"]})
            continue
        model_snapshot_id = str(row.get("model_snapshot_id") or "")
        active_model_snapshot_ids.add(model_snapshot_id)
        missing = _missing_required_fields(row, MODEL_EXPIRY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        trained_until = _parse_iso_ts(row.get("trained_until"))
        expiry_ts = _parse_iso_ts(row.get("expiry_ts"))
        checked_at = _parse_iso_ts(row.get("checked_at"))
        if trained_until is None or expiry_ts is None or checked_at is None:
            violations.append("model_expiry_timestamp_invalid")
        elif not (trained_until <= checked_at <= expiry_ts):
            violations.append("model_expired_or_checked_before_training")
        try:
            if int(row.get("max_age_minutes")) <= 0:
                violations.append("max_age_minutes_positive_required")
            if int(row.get("recent_sample_count")) < int(row.get("min_recent_samples")):
                violations.append("recent_sample_count_below_min")
        except (TypeError, ValueError):
            violations.append("model_sample_fields_numeric_required")
        if row.get("model_expiry_status") != "active":
            violations.append("model_not_active")
        if str(row.get("expired_action_cap") or "") not in MODEL_EXPIRED_ACTION_CAPS:
            violations.append("expired_action_cap_not_allowed")
        expected_hash = _hash_record_without(row, "model_expiry_hash") if model_snapshot_id else None
        if row.get("model_expiry_hash") and expected_hash and row.get("model_expiry_hash") != expected_hash:
            violations.append("model_expiry_hash_mismatch")
        if missing or violations:
            malformed_model_expiry.append({"index": index, "model_snapshot_id": model_snapshot_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    forecast_rows = policy.get("forecast_sanity_guards") if isinstance(policy.get("forecast_sanity_guards"), list) else []
    malformed_forecast_rows = []
    for index, row in enumerate(forecast_rows):
        if not isinstance(row, dict):
            malformed_forecast_rows.append({"index": index, "forecast_id": None, "missing_fields": list(FORECAST_SANITY_REQUIRED_FIELDS), "violations": ["forecast_sanity_guard_not_object"]})
            continue
        forecast_id = str(row.get("forecast_id") or "")
        missing = _missing_required_fields(row, FORECAST_SANITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            raw = Decimal(str(row.get("raw_forecast")))
            sanitized = Decimal(str(row.get("sanitized_forecast")))
            if raw < 0 or raw > 1 or sanitized < 0 or sanitized > 1:
                violations.append("forecast_probability_out_of_range")
            if sanitized > raw:
                violations.append("sanitized_forecast_exceeds_raw")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("forecast_numeric_required")
        try:
            if int(row.get("sample_n")) <= 0:
                violations.append("sample_n_positive_required")
        except (TypeError, ValueError):
            violations.append("sample_n_numeric_required")
        try:
            if Decimal(str(row.get("data_quality_score"))) <= 0:
                violations.append("data_quality_score_positive_required")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("data_quality_score_numeric_required")
        if row.get("fallback_level") == "global":
            violations.append("global_fallback_cannot_high_conviction")
        if row.get("calibration_bucket_status") != "healthy":
            violations.append("calibration_bucket_not_healthy")
        if row.get("forecast_sanity_status") != "pass":
            violations.append("forecast_sanity_not_pass")
        if not _sha256_hex_like(row.get("feature_vector_hash")):
            violations.append("feature_vector_hash_must_be_sha256")
        expected_hash = _hash_record_without(row, "forecast_sanity_hash") if forecast_id else None
        if row.get("forecast_sanity_hash") and expected_hash and row.get("forecast_sanity_hash") != expected_hash:
            violations.append("forecast_sanity_hash_mismatch")
        if missing or violations:
            malformed_forecast_rows.append({"index": index, "forecast_id": forecast_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    cross_link_violations = []
    if bundle_model_snapshot_ids - active_model_snapshot_ids:
        cross_link_violations.append("policy_bundle_model_snapshot_missing_expiry")
    if replay_model_snapshot_ids - active_model_snapshot_ids:
        cross_link_violations.append("replay_model_snapshot_missing_expiry")

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "cross_link_violations": cross_link_violations,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    replay_passed = bool(replay_checks) and not policy_errors and not source_errors and not cross_link_violations and not malformed_replay_checks
    build_passed = bool(builds) and not policy_errors and not source_errors and not malformed_builds
    supply_passed = bool(artifacts) and not policy_errors and not source_errors and not malformed_artifacts and not missing_artifact_types
    bundle_passed = bool(bundles) and not policy_errors and not source_errors and not cross_link_violations and not malformed_bundles
    expiry_passed = bool(model_expiry_rows) and not policy_errors and not source_errors and not malformed_model_expiry
    forecast_passed = bool(forecast_rows) and not policy_errors and not source_errors and not malformed_forecast_rows
    return [
        _contract(
            "ReplayDeterminismCheck",
            replay_passed,
            "replay_determinism_missing_malformed_or_nondeterministic",
            {
                **common,
                "replay_check_count": len(replay_checks),
                "malformed_replay_checks": malformed_replay_checks,
            },
        ),
        _contract(
            "ReproducibleBuildContract",
            build_passed,
            "reproducible_build_missing_malformed_or_unpinned",
            {
                **common,
                "build_count": len(builds),
                "build_ids": sorted(build_ids),
                "malformed_builds": malformed_builds,
            },
        ),
        _contract(
            "SupplyChainSecurityContract",
            supply_passed,
            "supply_chain_security_missing_malformed_or_unverified",
            {
                **common,
                "artifact_count": len(artifacts),
                "artifact_types": sorted(item for item in artifact_types if item),
                "missing_artifact_types": missing_artifact_types,
                "malformed_artifacts": malformed_artifacts,
            },
        ),
        _contract(
            "PolicyBundleCompatibilityContract",
            bundle_passed,
            "policy_bundle_compatibility_missing_malformed_or_incompatible",
            {
                **common,
                "policy_bundle_count": len(bundles),
                "malformed_bundles": malformed_bundles,
            },
        ),
        _contract(
            "ModelExpiryContract",
            expiry_passed,
            "model_expiry_missing_malformed_or_expired",
            {
                **common,
                "model_expiry_count": len(model_expiry_rows),
                "active_model_snapshot_ids": sorted(item for item in active_model_snapshot_ids if item),
                "malformed_model_expiry": malformed_model_expiry,
            },
        ),
        _contract(
            "ForecastSanityGuard",
            forecast_passed,
            "forecast_sanity_guard_missing_malformed_or_bypassed",
            {
                **common,
                "forecast_sanity_count": len(forecast_rows),
                "malformed_forecast_rows": malformed_forecast_rows,
            },
        ),
    ]


def _contracts_from_error(contract_ids, blocking_reason, evidence):
    return [_contract(contract_id, False, blocking_reason, evidence) for contract_id in contract_ids]


MARKOV_LIFECYCLE_FORECAST_CONTRACTS = (
    "TelegramLifecycleTransitionMatrixContract",
    "LifecycleNstepForecastContract",
    "AbsorbingSemiMarkovForecastContract",
    "CompetingRiskForecastContract",
    "CensoringPolicyContract",
    "ForecastWalkForwardValidationContract",
    "HMMResearchOnlyBoundaryContract",
)


def _probability_value_errors(value, field_name):
    try:
        probability = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return [f"{field_name}_probability_numeric_required"]
    if probability < 0 or probability > 1:
        return [f"{field_name}_probability_out_of_range"]
    return []


def _probability_distribution_errors(distribution, states, row_name):
    if not isinstance(distribution, dict):
        return [f"{row_name}_distribution_not_object"]
    errors = []
    total = Decimal("0")
    for state in states:
        if state not in distribution:
            errors.append(f"{row_name}_{state}_missing")
            continue
        value_errors = _probability_value_errors(distribution.get(state), f"{row_name}_{state}")
        errors.extend(value_errors)
        if not value_errors:
            total += Decimal(str(distribution.get(state)))
    if abs(total - Decimal("1")) > Decimal("0.000001"):
        errors.append(f"{row_name}_probabilities_do_not_sum_to_one")
    return errors


def _failed_markov_lifecycle_forecast_contracts(reason, evidence):
    return _contracts_from_error(MARKOV_LIFECYCLE_FORECAST_CONTRACTS, reason, evidence)


def verify_markov_lifecycle_forecast_contracts(policy_path=DEFAULT_MARKOV_LIFECYCLE_FORECAST_POLICY):
    try:
        policy = _load_json(policy_path)
        from telegram_lifecycle_markov import (  # noqa: PLC0415
            ABSORBING_STATES,
            FORECAST_BOUNDARY,
            STATE_ORDER,
            build_lifecycle_forecast_snapshot,
        )
    except Exception as exc:
        return _failed_markov_lifecycle_forecast_contracts(
            "markov_lifecycle_forecast_policy_missing_or_invalid",
            {"policy_path": str(policy_path), "error": str(exc)},
        )
    if not isinstance(policy, dict):
        return _failed_markov_lifecycle_forecast_contracts(
            "markov_lifecycle_forecast_policy_not_object",
            {"policy_path": str(policy_path)},
        )

    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.markov_lifecycle_forecast_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "forecast_research_only":
        policy_errors.append("failure_action_must_be_forecast_research_only")

    config = policy.get("snapshot_config") if isinstance(policy.get("snapshot_config"), dict) else {}
    events = policy.get("sample_lifecycle_events") if isinstance(policy.get("sample_lifecycle_events"), list) else []
    try:
        snapshot = build_lifecycle_forecast_snapshot(
            events,
            start_state=config.get("start_state") or "RECLAIM_CONFIRMED",
            cutoff_seq=int(config.get("cutoff_seq")) if config.get("cutoff_seq") is not None else None,
            horizons=tuple(config.get("horizons") or (1, 3, 5, 15)),
            max_absorption_steps=int(config.get("max_absorption_steps") or 60),
            model_snapshot_id=config.get("model_snapshot_id"),
        )
        snapshot_error = None
    except Exception as exc:  # noqa: BLE001
        snapshot = {}
        snapshot_error = str(exc)

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
        "snapshot_error": snapshot_error,
        "model_snapshot_id": snapshot.get("model_snapshot_id"),
        "sample_n": snapshot.get("sample_n"),
        "matrix_build_cutoff_seq": snapshot.get("matrix_build_cutoff_seq"),
    }

    transition_errors = []
    if snapshot_error:
        transition_errors.append("snapshot_build_failed")
    for field in ("model_snapshot_id", "state_definition_version", "transition_matrix", "matrix_build_cutoff_seq", "sample_n"):
        if not snapshot.get(field) and field != "matrix_build_cutoff_seq":
            transition_errors.append(f"{field}_missing")
    try:
        if int(snapshot.get("sample_n") or 0) <= 0:
            transition_errors.append("sample_n_positive_required")
    except (TypeError, ValueError):
        transition_errors.append("sample_n_numeric_required")
    matrix = snapshot.get("transition_matrix")
    if not isinstance(matrix, dict):
        transition_errors.append("transition_matrix_not_object")
    else:
        for state in STATE_ORDER:
            row_errors = _probability_distribution_errors(matrix.get(state), STATE_ORDER, f"matrix_{state}")
            transition_errors.extend(row_errors)
        for state in ABSORBING_STATES:
            row = matrix.get(state) if isinstance(matrix.get(state), dict) else {}
            if row.get(state) != 1.0:
                transition_errors.append(f"absorbing_state_{state}_not_self_loop")
    if snapshot.get("absorbing_transition_violation_count") not in {0, None}:
        transition_errors.append("absorbing_transition_violations_present")
    boundary = snapshot.get("contract_boundaries") if isinstance(snapshot.get("contract_boundaries"), dict) else {}
    if boundary.get("mode_target") != "shadow_first":
        transition_errors.append("boundary_mode_target_not_shadow_first")
    if boundary.get("entry_gate_allowed") is not False:
        transition_errors.append("entry_gate_allowed_must_be_false")
    if boundary.get("ordinary_stationary_distribution_allowed_as_entry_gate") is not False:
        transition_errors.append("stationary_distribution_entry_gate_must_be_false")
    if boundary != dict(FORECAST_BOUNDARY):
        transition_errors.append("forecast_boundary_drift")

    n_step_errors = []
    n_step = snapshot.get("n_step_forecasts") if isinstance(snapshot.get("n_step_forecasts"), dict) else {}
    for field in ("model_snapshot_id", "start_state", "matrix_build_cutoff_seq"):
        if not snapshot.get(field) and field != "matrix_build_cutoff_seq":
            n_step_errors.append(f"{field}_missing")
    for horizon in sorted({int(item) for item in (config.get("horizons") or [])}):
        key = str(horizon)
        if key not in n_step:
            n_step_errors.append(f"horizon_{key}_missing")
        else:
            n_step_errors.extend(_probability_distribution_errors(n_step.get(key), STATE_ORDER, f"n_step_{key}"))

    absorption_errors = []
    absorbing_definition = set(str(item) for item in (policy.get("absorbing_state_definition") or []) if item)
    if set(ABSORBING_STATES) - absorbing_definition:
        absorption_errors.append("absorbing_state_definition_incomplete")
    state_duration_sec = policy.get("state_duration_sec")
    if not isinstance(state_duration_sec, dict) or not state_duration_sec:
        absorption_errors.append("state_duration_sec_missing")
    absorption = snapshot.get("absorption_forecast") if isinstance(snapshot.get("absorption_forecast"), dict) else {}
    absorbing_probabilities = absorption.get("absorbing_state_probabilities")
    if not isinstance(absorbing_probabilities, dict):
        absorption_errors.append("absorption_probabilities_missing")
    else:
        for state in ABSORBING_STATES:
            absorption_errors.extend(_probability_value_errors(absorbing_probabilities.get(state), f"absorb_{state}"))
    if absorption.get("expected_time_to_absorption_lower_bound_steps") is None:
        absorption_errors.append("expected_time_to_absorption_missing")

    censoring_rows = policy.get("censoring_policies") if isinstance(policy.get("censoring_policies"), list) else []
    malformed_censoring = []
    censoring_versions = set()
    for index, row in enumerate(censoring_rows):
        if not isinstance(row, dict):
            malformed_censoring.append({"index": index, "violations": ["censoring_policy_not_object"]})
            continue
        censoring_versions.add(str(row.get("censoring_policy_version") or ""))
        missing = _missing_required_fields(row, MARKOV_CENSORING_POLICY_REQUIRED_FIELDS)
        violations = []
        if not str(row.get("training_weight_policy") or "").strip():
            violations.append("training_weight_policy_required")
        if missing or violations:
            malformed_censoring.append({"index": index, "missing_fields": missing, "violations": violations})
    if not censoring_rows:
        malformed_censoring.append({"index": None, "violations": ["censoring_policy_required"]})

    competing_errors = []
    for field in ("p_absorb_peak30", "p_absorb_stop_before_peak"):
        competing_errors.extend(_probability_value_errors(absorption.get(field), field))
    competing_censoring_version = str(policy.get("competing_risk_censoring_policy_version") or "")
    if competing_censoring_version not in censoring_versions:
        competing_errors.append("censoring_policy_version_missing_or_unknown")

    walk_forward_rows = policy.get("walk_forward_validations") if isinstance(policy.get("walk_forward_validations"), list) else []
    malformed_walk_forward = []
    for index, row in enumerate(walk_forward_rows):
        if not isinstance(row, dict):
            malformed_walk_forward.append({"index": index, "validation_id": None, "violations": ["walk_forward_validation_not_object"]})
            continue
        missing = _missing_required_fields(row, MARKOV_WALK_FORWARD_REQUIRED_FIELDS)
        violations = []
        if "cutoff" not in str(row.get("no_lookahead_proof") or "").lower():
            violations.append("no_lookahead_proof_must_reference_cutoff")
        if row.get("promotion_allowed") is not False:
            violations.append("promotion_allowed_must_be_false")
        try:
            if int(row.get("cutoff_seq")) != int(snapshot.get("matrix_build_cutoff_seq")):
                violations.append("cutoff_seq_mismatch")
        except (TypeError, ValueError):
            violations.append("cutoff_seq_numeric_required")
        if missing or violations:
            malformed_walk_forward.append(
                {
                    "index": index,
                    "validation_id": row.get("validation_id"),
                    "missing_fields": missing,
                    "violations": sorted(set(violations)),
                }
            )
    if not walk_forward_rows:
        malformed_walk_forward.append({"index": None, "validation_id": None, "violations": ["walk_forward_validation_required"]})

    hmm_rows = policy.get("hmm_research_only_boundaries") if isinstance(policy.get("hmm_research_only_boundaries"), list) else []
    malformed_hmm = []
    for index, row in enumerate(hmm_rows):
        if not isinstance(row, dict):
            malformed_hmm.append({"index": index, "artifact_id": None, "violations": ["hmm_boundary_not_object"]})
            continue
        missing = _missing_required_fields(row, MARKOV_HMM_BOUNDARY_REQUIRED_FIELDS)
        violations = []
        if row.get("research_only") is not True:
            violations.append("research_only_must_be_true")
        if row.get("online_filtering_only") is not True:
            violations.append("online_filtering_only_must_be_true")
        if row.get("full_sequence_viterbi_allowed") is not False:
            violations.append("full_sequence_viterbi_must_be_false")
        if row.get("entry_gate_allowed") is not False:
            violations.append("entry_gate_allowed_must_be_false")
        if missing or violations:
            malformed_hmm.append(
                {
                    "index": index,
                    "artifact_id": row.get("artifact_id"),
                    "missing_fields": missing,
                    "violations": sorted(set(violations)),
                }
            )
    if not hmm_rows:
        malformed_hmm.append({"index": None, "artifact_id": None, "violations": ["hmm_boundary_required"]})

    base_ok = not policy_errors and not source_errors and snapshot_error is None
    return [
        _contract(
            "TelegramLifecycleTransitionMatrixContract",
            base_ok and not transition_errors,
            "telegram_lifecycle_transition_matrix_missing_malformed_or_leaky",
            {**common, "transition_matrix_errors": sorted(set(transition_errors))},
        ),
        _contract(
            "LifecycleNstepForecastContract",
            base_ok and not transition_errors and not n_step_errors,
            "lifecycle_nstep_forecast_missing_malformed_or_leaky",
            {**common, "n_step_errors": sorted(set(n_step_errors))},
        ),
        _contract(
            "AbsorbingSemiMarkovForecastContract",
            base_ok and not transition_errors and not absorption_errors,
            "absorbing_semimarkov_forecast_missing_malformed_or_leaky",
            {**common, "absorbing_errors": sorted(set(absorption_errors))},
        ),
        _contract(
            "CompetingRiskForecastContract",
            base_ok and not transition_errors and not competing_errors and not malformed_censoring,
            "competing_risk_forecast_missing_malformed_or_uncensored",
            {**common, "competing_risk_errors": sorted(set(competing_errors)), "malformed_censoring": malformed_censoring},
        ),
        _contract(
            "CensoringPolicyContract",
            base_ok and not malformed_censoring,
            "censoring_policy_missing_malformed_or_promotion_leaky",
            {**common, "malformed_censoring": malformed_censoring},
        ),
        _contract(
            "ForecastWalkForwardValidationContract",
            base_ok and not malformed_walk_forward,
            "forecast_walk_forward_validation_missing_or_lookahead_leaky",
            {**common, "malformed_walk_forward_validations": malformed_walk_forward},
        ),
        _contract(
            "HMMResearchOnlyBoundaryContract",
            base_ok and not malformed_hmm,
            "hmm_research_only_boundary_missing_or_entry_leaky",
            {**common, "malformed_hmm_boundaries": malformed_hmm},
        ),
    ]


def _rendered_doc_hash_from_manifest(manifest_path=MANIFEST_PATH):
    manifest = _load_json(manifest_path)
    rendered_files = manifest.get("rendered_views") if isinstance(manifest, dict) else []
    rendered_hashes = {}
    for item in rendered_files if isinstance(rendered_files, list) else []:
        if not isinstance(item, dict):
            continue
        filename = item.get("file")
        if not filename:
            continue
        rendered_hashes[str(filename)] = str(item.get("sha256") or "")
    return _sha256_json(rendered_hashes), len(rendered_hashes)


def verify_spec_governance_feasibility_contracts(
    policy_path=DEFAULT_SPEC_GOVERNANCE_FEASIBILITY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
        spec_report = validate_all(manifest_path=manifest_path, catalog_path=catalog_path, registry_path=registry_path)
        rendered_doc_hash, rendered_file_count = _rendered_doc_hash_from_manifest(manifest_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return _contracts_from_error(
            sorted(SPEC_GOVERNANCE_FEASIBILITY_CONTRACTS),
            "spec_governance_feasibility_policy_missing_or_invalid",
            evidence,
        )
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return _contracts_from_error(
            sorted(SPEC_GOVERNANCE_FEASIBILITY_CONTRACTS),
            "spec_governance_feasibility_policy_not_object",
            evidence,
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return _contracts_from_error(
            sorted(SPEC_GOVERNANCE_FEASIBILITY_CONTRACTS),
            "spec_governance_feasibility_policy_missing_or_invalid",
            evidence,
        )

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.spec_governance_feasibility_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "spec_governance_or_feasibility_blocked":
        policy_errors.append("failure_action_must_block_spec_governance_or_feasibility")

    rendered_views = policy.get("rendered_spec_views") if isinstance(policy.get("rendered_spec_views"), list) else []
    malformed_rendered_views = []
    for index, row in enumerate(rendered_views):
        if not isinstance(row, dict):
            malformed_rendered_views.append({"index": index, "rendered_view_id": None, "missing_fields": list(RENDERED_SPEC_VIEW_REQUIRED_FIELDS), "violations": ["rendered_view_not_object"]})
            continue
        view_id = str(row.get("rendered_view_id") or "")
        missing = [
            field
            for field in _missing_required_fields(row, RENDERED_SPEC_VIEW_REQUIRED_FIELDS)
            if field not in {"missing_section_ids", "extra_section_ids"} or field not in row
        ]
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("source_spec_hash") != spec_report.get("spec_hash"):
            violations.append("source_spec_hash_mismatch")
        if row.get("rendered_doc_hash") != rendered_doc_hash:
            violations.append("rendered_doc_hash_mismatch")
        try:
            if int(row.get("section_count")) != int(spec_report.get("section_count")):
                violations.append("section_count_mismatch")
        except (TypeError, ValueError):
            violations.append("section_count_numeric_required")
        if row.get("missing_section_ids") not in ([], None):
            violations.append("missing_sections_not_allowed")
        if row.get("extra_section_ids") not in ([], None):
            violations.append("extra_sections_not_allowed")
        if row.get("render_validation_status") != "valid":
            violations.append("render_validation_not_valid")
        if _parse_iso_ts(row.get("rendered_at")) is None:
            violations.append("rendered_at_invalid")
        expected_hash = _hash_record_without(row, "view_hash") if view_id else None
        if row.get("view_hash") and expected_hash and row.get("view_hash") != expected_hash:
            violations.append("view_hash_mismatch")
        if missing or violations:
            malformed_rendered_views.append({"index": index, "rendered_view_id": view_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    health_states = policy.get("health_states") if isinstance(policy.get("health_states"), list) else []
    malformed_health_states = []
    for index, row in enumerate(health_states):
        if not isinstance(row, dict):
            malformed_health_states.append({"index": index, "health_component": None, "missing_fields": list(HEALTH_STATE_REQUIRED_FIELDS), "violations": ["health_state_not_object"]})
            continue
        component = str(row.get("health_component") or "")
        missing = [
            field
            for field in _missing_required_fields(row, HEALTH_STATE_REQUIRED_FIELDS)
            if field != "blocking_modes" or field not in row
        ]
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        health_state = str(row.get("health_state") or "")
        if health_state not in HEALTH_STATE_ENUM_VALUES:
            violations.append("health_state_not_allowed")
        if health_state == "UNKNOWN":
            violations.append("unknown_state_cannot_pass_readiness")
        if health_state == "FATAL" and "normal_tiny" not in set(row.get("blocking_modes") or []):
            violations.append("fatal_state_must_block_normal_tiny")
        first_seen = _parse_iso_ts(row.get("first_seen_at"))
        last_seen = _parse_iso_ts(row.get("last_seen_at"))
        if first_seen is None or last_seen is None:
            violations.append("health_timestamp_invalid")
        elif first_seen > last_seen:
            violations.append("health_timestamp_order_invalid")
        if not isinstance(row.get("blocking_modes"), list):
            violations.append("blocking_modes_must_be_list")
        expected_hash = _hash_record_without(row, "health_hash") if component else None
        if row.get("health_hash") and expected_hash and row.get("health_hash") != expected_hash:
            violations.append("health_hash_mismatch")
        if missing or violations:
            malformed_health_states.append({"index": index, "health_component": component or None, "missing_fields": missing, "violations": sorted(set(violations))})

    lifecycles = policy.get("contract_lifecycle") if isinstance(policy.get("contract_lifecycle"), list) else []
    lifecycle_contract_ids = set()
    malformed_lifecycles = []
    for index, row in enumerate(lifecycles):
        if not isinstance(row, dict):
            malformed_lifecycles.append({"index": index, "contract_id": None, "missing_fields": list(CONTRACT_LIFECYCLE_REQUIRED_FIELDS), "violations": ["contract_lifecycle_not_object"]})
            continue
        contract_id = str(row.get("contract_id") or "")
        lifecycle_contract_ids.add(contract_id)
        missing = _missing_required_fields(row, CONTRACT_LIFECYCLE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        status = str(row.get("status") or "")
        if status not in CONTRACT_LIFECYCLE_STATUSES:
            violations.append("contract_lifecycle_status_not_allowed")
        if status != "active_gate":
            violations.append("contract_not_active_gate_for_readiness")
        if row.get("contract_tests_status") != "pass":
            violations.append("contract_tests_not_pass")
        allowed_modes = set(str(item) for item in row.get("allowed_modes", []) if item)
        if "normal_tiny" not in allowed_modes:
            violations.append("normal_tiny_mode_not_allowed")
        if str(row.get("deprecated_in_version") or "") != "none":
            violations.append("active_contract_cannot_be_deprecated")
        if str(row.get("superseded_by") or "") != "none":
            violations.append("active_contract_cannot_be_superseded")
        if str(row.get("sunset_deadline") or "") != "none":
            violations.append("active_contract_cannot_have_sunset_deadline")
        expected_hash = _hash_record_without(row, "lifecycle_hash") if contract_id else None
        if row.get("lifecycle_hash") and expected_hash and row.get("lifecycle_hash") != expected_hash:
            violations.append("lifecycle_hash_mismatch")
        if missing or violations:
            malformed_lifecycles.append({"index": index, "contract_id": contract_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_lifecycle_contracts = sorted(SPEC_GOVERNANCE_FEASIBILITY_CONTRACTS - lifecycle_contract_ids)

    objective_conflicts = policy.get("objective_conflicts") if isinstance(policy.get("objective_conflicts"), list) else []
    malformed_objective_conflicts = []
    for index, row in enumerate(objective_conflicts):
        if not isinstance(row, dict):
            malformed_objective_conflicts.append({"index": index, "objective_conflict_id": None, "missing_fields": list(OBJECTIVE_PRIORITY_REQUIRED_FIELDS), "violations": ["objective_conflict_not_object"]})
            continue
        conflict_id = str(row.get("objective_conflict_id") or "")
        missing = _missing_required_fields(row, OBJECTIVE_PRIORITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        conflicts = [str(item) for item in row.get("conflicting_objectives", []) if item]
        chosen = str(row.get("chosen_objective") or "")
        unknown = sorted(set(conflicts + [chosen]) - set(OBJECTIVE_PRIORITY_RANKS))
        if unknown:
            violations.append("objective_name_not_registered")
        if chosen not in conflicts:
            violations.append("chosen_objective_not_in_conflict_set")
        if conflicts and not unknown:
            expected_choice = min(conflicts, key=lambda item: OBJECTIVE_PRIORITY_RANKS[item])
            if chosen != expected_choice:
                violations.append("chosen_objective_not_highest_priority")
            try:
                if int(row.get("priority_rank")) != OBJECTIVE_PRIORITY_RANKS[chosen]:
                    violations.append("priority_rank_mismatch")
            except (TypeError, ValueError):
                violations.append("priority_rank_numeric_required")
        if row.get("operator_override_allowed") is not False and chosen in {"paper_live_safety", "exit_safety", "ledger_capital_correctness", "data_spec_truth"}:
            violations.append("safety_or_truth_override_forbidden")
        expected_hash = _hash_record_without(row, "conflict_hash") if conflict_id else None
        if row.get("conflict_hash") and expected_hash and row.get("conflict_hash") != expected_hash:
            violations.append("conflict_hash_mismatch")
        if missing or violations:
            malformed_objective_conflicts.append({"index": index, "objective_conflict_id": conflict_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    goal_confidence = policy.get("goal_confidence") if isinstance(policy.get("goal_confidence"), list) else []
    malformed_goal_confidence = []
    for index, row in enumerate(goal_confidence):
        if not isinstance(row, dict):
            malformed_goal_confidence.append({"index": index, "metric_id": None, "missing_fields": list(GOAL_CONFIDENCE_REQUIRED_FIELDS), "violations": ["goal_confidence_not_object"]})
            continue
        metric_id = str(row.get("metric_id") or "")
        missing = _missing_required_fields(row, GOAL_CONFIDENCE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            numerator = Decimal(str(row.get("numerator")))
            denominator = Decimal(str(row.get("denominator")))
            min_denominator = Decimal(str(row.get("min_denominator")))
            point = Decimal(str(row.get("point_estimate")))
            wilson = Decimal(str(row.get("wilson_lower_bound")))
            beta = Decimal(str(row.get("beta_posterior_lower_bound")))
            if denominator <= 0 or numerator < 0 or numerator > denominator:
                violations.append("goal_confidence_counts_invalid")
            if denominator < min_denominator:
                violations.append("denominator_below_min")
            if denominator > 0 and abs((numerator / denominator) - point) > Decimal("0.0001"):
                violations.append("point_estimate_mismatch")
            if min(wilson, beta) < Decimal(str(row.get("observed_value"))):
                violations.append("observed_value_exceeds_lower_bound")
        except (InvalidOperation, ValueError, TypeError, ZeroDivisionError):
            violations.append("goal_confidence_numeric_required")
        if row.get("status") != "pass":
            violations.append("goal_confidence_not_pass")
        expected_hash = _hash_record_without(row, "confidence_hash") if metric_id else None
        if row.get("confidence_hash") and expected_hash and row.get("confidence_hash") != expected_hash:
            violations.append("confidence_hash_mismatch")
        if missing or violations:
            malformed_goal_confidence.append({"index": index, "metric_id": metric_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    fill_time_anchors = policy.get("fill_time_anchors") if isinstance(policy.get("fill_time_anchors"), list) else []
    malformed_fill_time_anchors = []
    for index, row in enumerate(fill_time_anchors):
        if not isinstance(row, dict):
            malformed_fill_time_anchors.append({"index": index, "anchor_id": None, "missing_fields": list(FILL_TIME_ANCHOR_REQUIRED_FIELDS), "violations": ["fill_time_anchor_not_object"]})
            continue
        anchor_id = str(row.get("anchor_id") or "")
        missing = _missing_required_fields(row, FILL_TIME_ANCHOR_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        decision_ts = _parse_iso_ts(row.get("decision_ts"))
        decision_available_at = _parse_iso_ts(row.get("decision_available_at"))
        quote_ts = _parse_iso_ts(row.get("quote_ts"))
        entry_quote_ts = _parse_iso_ts(row.get("entry_quote_at_decision_ts"))
        simulated_fill_ts = _parse_iso_ts(row.get("simulated_fill_ts"))
        open_confirmed_ts = _parse_iso_ts(row.get("position_open_confirmed_ts"))
        if None in {decision_ts, decision_available_at, quote_ts, entry_quote_ts, simulated_fill_ts, open_confirmed_ts}:
            violations.append("fill_anchor_timestamp_invalid")
        elif not (decision_ts <= decision_available_at <= simulated_fill_ts <= open_confirmed_ts):
            violations.append("fill_anchor_chronology_invalid")
        if row.get("fill_time_anchor_type") != "simulated_fill_ts":
            violations.append("fill_time_anchor_type_must_be_simulated_fill_ts")
        latency_components = row.get("latency_components")
        required_latency = {"decision_latency_ms", "queue_latency_ms", "quote_latency_ms", "fill_simulation_latency_ms"}
        if not isinstance(latency_components, dict) or not required_latency.issubset(set(latency_components)):
            violations.append("latency_components_missing")
        else:
            for key in required_latency:
                try:
                    if Decimal(str(latency_components.get(key))) < 0:
                        violations.append(f"{key}_negative")
                except (InvalidOperation, ValueError, TypeError):
                    violations.append(f"{key}_numeric_required")
        expected_hash = _hash_record_without(row, "anchor_hash") if anchor_id else None
        if row.get("anchor_hash") and expected_hash and row.get("anchor_hash") != expected_hash:
            violations.append("anchor_hash_mismatch")
        if missing or violations:
            malformed_fill_time_anchors.append({"index": index, "anchor_id": anchor_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    feasibilities = policy.get("ex_ante_posthoc_feasibility") if isinstance(policy.get("ex_ante_posthoc_feasibility"), list) else []
    malformed_feasibility = []
    for index, row in enumerate(feasibilities):
        if not isinstance(row, dict):
            malformed_feasibility.append({"index": index, "feasibility_id": None, "missing_fields": list(EX_ANTE_POSTHOC_FEASIBILITY_REQUIRED_FIELDS), "violations": ["feasibility_not_object"]})
            continue
        feasibility_id = str(row.get("feasibility_id") or "")
        missing = _missing_required_fields(row, EX_ANTE_POSTHOC_FEASIBILITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        decision_ts = _parse_iso_ts(row.get("decision_ts"))
        feature_available_at = _parse_iso_ts(row.get("feature_available_at"))
        earliest_actionable_ts = _parse_iso_ts(row.get("earliest_actionable_ts"))
        peak_ts = _parse_iso_ts(row.get("peak_ts"))
        if None in {decision_ts, feature_available_at, earliest_actionable_ts, peak_ts}:
            violations.append("feasibility_timestamp_invalid")
        else:
            if feature_available_at > decision_ts:
                violations.append("feature_available_after_decision")
            if earliest_actionable_ts > peak_ts:
                violations.append("earliest_actionable_after_peak")
        if row.get("used_future_peak_in_ex_ante") is not False:
            violations.append("future_peak_used_in_ex_ante")
        if row.get("feasibility_class") not in FEASIBILITY_CLASSES:
            violations.append("feasibility_class_not_allowed")
        if row.get("ex_ante_feasible") is not True:
            violations.append("seed_ex_ante_must_be_feasible")
        if row.get("posthoc_feasible") is not True:
            violations.append("seed_posthoc_must_be_feasible")
        source_fields = set(str(item) for item in row.get("ex_ante_source_fields", []) if item)
        if not source_fields:
            violations.append("ex_ante_source_fields_required")
        if source_fields & FORBIDDEN_EX_ANTE_FIELDS:
            violations.append("forbidden_ex_ante_source_field")
        required_inputs = row.get("required_inputs_available_at")
        if not isinstance(required_inputs, dict) or not required_inputs:
            violations.append("required_inputs_available_at_required")
        else:
            for field, value in required_inputs.items():
                parsed = _parse_iso_ts(value)
                if parsed is None:
                    violations.append(f"{field}_available_at_invalid")
                elif decision_ts is not None and parsed > decision_ts:
                    violations.append(f"{field}_available_after_decision")
        try:
            if Decimal(str(row.get("system_min_decision_latency_sec"))) < 0:
                violations.append("system_min_decision_latency_negative")
            if Decimal(str(row.get("system_min_entry_latency_sec"))) < 0:
                violations.append("system_min_entry_latency_negative")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("system_latency_numeric_required")
        expected_hash = _hash_record_without(row, "feasibility_hash") if feasibility_id else None
        if row.get("feasibility_hash") and expected_hash and row.get("feasibility_hash") != expected_hash:
            violations.append("feasibility_hash_mismatch")
        if missing or violations:
            malformed_feasibility.append({"index": index, "feasibility_id": feasibility_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
        "spec_hash": spec_report.get("spec_hash"),
        "rendered_doc_hash": rendered_doc_hash,
        "rendered_file_count": rendered_file_count,
    }
    return [
        _contract(
            "RenderedSpecViewContract",
            bool(rendered_views) and not policy_errors and not source_errors and not malformed_rendered_views,
            "rendered_spec_view_missing_malformed_or_stale",
            {**common, "rendered_view_count": len(rendered_views), "malformed_rendered_views": malformed_rendered_views},
        ),
        _contract(
            "HealthStateEnumContract",
            bool(health_states) and not policy_errors and not source_errors and not malformed_health_states,
            "health_state_enum_missing_malformed_or_unsafe",
            {**common, "health_state_count": len(health_states), "malformed_health_states": malformed_health_states},
        ),
        _contract(
            "ContractLifecycleContract",
            bool(lifecycles) and not policy_errors and not source_errors and not malformed_lifecycles and not missing_lifecycle_contracts,
            "contract_lifecycle_missing_malformed_or_ungated",
            {**common, "contract_lifecycle_count": len(lifecycles), "missing_lifecycle_contracts": missing_lifecycle_contracts, "malformed_lifecycles": malformed_lifecycles},
        ),
        _contract(
            "ObjectivePriorityContract",
            bool(objective_conflicts) and not policy_errors and not source_errors and not malformed_objective_conflicts,
            "objective_priority_missing_malformed_or_unsafe",
            {**common, "objective_conflict_count": len(objective_conflicts), "malformed_objective_conflicts": malformed_objective_conflicts},
        ),
        _contract(
            "GoalConfidenceContract",
            bool(goal_confidence) and not policy_errors and not source_errors and not malformed_goal_confidence,
            "goal_confidence_missing_malformed_or_inconclusive",
            {**common, "goal_confidence_count": len(goal_confidence), "malformed_goal_confidence": malformed_goal_confidence},
        ),
        _contract(
            "FillTimeAnchorContract",
            bool(fill_time_anchors) and not policy_errors and not source_errors and not malformed_fill_time_anchors,
            "fill_time_anchor_missing_malformed_or_unpinned",
            {**common, "fill_time_anchor_count": len(fill_time_anchors), "malformed_fill_time_anchors": malformed_fill_time_anchors},
        ),
        _contract(
            "ExAnteVsPosthocFeasibilityContract",
            bool(feasibilities) and not policy_errors and not source_errors and not malformed_feasibility,
            "ex_ante_posthoc_feasibility_missing_malformed_or_leaky",
            {**common, "feasibility_count": len(feasibilities), "malformed_feasibility": malformed_feasibility},
        ),
    ]


def verify_identity_unit_provider_finality_contracts(
    policy_path=DEFAULT_IDENTITY_UNIT_PROVIDER_FINALITY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return _contracts_from_error(
            sorted(IDENTITY_UNIT_PROVIDER_FINALITY_CONTRACTS),
            "identity_unit_provider_finality_policy_missing_or_invalid",
            evidence,
        )
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return _contracts_from_error(
            sorted(IDENTITY_UNIT_PROVIDER_FINALITY_CONTRACTS),
            "identity_unit_provider_finality_policy_not_object",
            evidence,
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return _contracts_from_error(
            sorted(IDENTITY_UNIT_PROVIDER_FINALITY_CONTRACTS),
            "identity_unit_provider_finality_policy_missing_or_invalid",
            evidence,
        )

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.identity_unit_provider_finality_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "identity_unit_provider_finality_blocked":
        policy_errors.append("failure_action_must_block_identity_unit_provider_finality")

    identities = policy.get("token_identities") if isinstance(policy.get("token_identities"), list) else []
    malformed_identities = []
    for index, row in enumerate(identities):
        if not isinstance(row, dict):
            malformed_identities.append({"index": index, "identity_id": None, "missing_fields": list(TOKEN_IDENTITY_REQUIRED_FIELDS), "violations": ["token_identity_not_object"]})
            continue
        identity_id = str(row.get("identity_id") or "")
        missing = _missing_required_fields(row, TOKEN_IDENTITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if not str(row.get("chain") or "").strip():
            violations.append("chain_required")
        token_ca = str(row.get("token_ca") or "")
        normalized_ca = str(row.get("normalized_ca") or "")
        if len(token_ca) < 32:
            violations.append("token_ca_too_short")
        if len(normalized_ca) < 32:
            violations.append("normalized_ca_too_short")
        if not str(row.get("checksum") or "").strip():
            violations.append("checksum_required")
        if str(row.get("symbol") or "") == token_ca:
            violations.append("symbol_cannot_be_primary_key")
        try:
            if Decimal(str(row.get("symbol_conflict_count"))) < 0:
                violations.append("symbol_conflict_count_negative")
            confidence = Decimal(str(row.get("identity_confidence")))
            observed = Decimal(str(row.get("observed_value")))
            if confidence <= 0 or confidence > 1:
                violations.append("identity_confidence_out_of_range")
            if observed != confidence:
                violations.append("observed_value_must_match_identity_confidence")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("identity_numeric_required")
        if row.get("liquidity_pair_valid") is not True:
            violations.append("liquidity_pair_must_be_valid")
        expected_hash = _hash_record_without(row, "identity_hash") if identity_id else None
        if row.get("identity_hash") and expected_hash and row.get("identity_hash") != expected_hash:
            violations.append("identity_hash_mismatch")
        if missing or violations:
            malformed_identities.append({"index": index, "identity_id": identity_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    data_units = policy.get("data_units") if isinstance(policy.get("data_units"), list) else []
    malformed_data_units = []
    for index, row in enumerate(data_units):
        if not isinstance(row, dict):
            malformed_data_units.append({"index": index, "unit_id": None, "missing_fields": list(DATA_UNIT_REQUIRED_FIELDS), "violations": ["data_unit_not_object"]})
            continue
        unit_id = str(row.get("unit_id") or "")
        missing = _missing_required_fields(row, DATA_UNIT_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        for field in ("token_decimals", "quote_decimals"):
            try:
                value = Decimal(str(row.get(field)))
                if value < 0 or value != value.to_integral_value():
                    violations.append(f"{field}_invalid")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        for field in ("quote_size_sol", "normalized_price"):
            try:
                if Decimal(str(row.get(field))) <= 0:
                    violations.append(f"{field}_must_be_positive")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        if row.get("unit_validation_status") != "valid":
            violations.append("unit_validation_status_not_valid")
        for field in ("price_unit", "liquidity_unit", "market_cap_unit", "quote_mint", "unit_conversion_version"):
            if not str(row.get(field) or "").strip():
                violations.append(f"{field}_required")
        expected_hash = _hash_record_without(row, "unit_hash") if unit_id else None
        if row.get("unit_hash") and expected_hash and row.get("unit_hash") != expected_hash:
            violations.append("unit_hash_mismatch")
        if missing or violations:
            malformed_data_units.append({"index": index, "unit_id": unit_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    finalities = policy.get("chain_finality") if isinstance(policy.get("chain_finality"), list) else []
    malformed_finalities = []
    min_commitment = str(policy.get("min_commitment_level") or "finalized")
    max_indexer_lag = Decimal(str(policy.get("max_indexer_lag_sec", "5")))
    for index, row in enumerate(finalities):
        if not isinstance(row, dict):
            malformed_finalities.append({"index": index, "finality_id": None, "missing_fields": list(CHAIN_FINALITY_REQUIRED_FIELDS), "violations": ["chain_finality_not_object"]})
            continue
        finality_id = str(row.get("finality_id") or "")
        missing = _missing_required_fields(row, CHAIN_FINALITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        commitment = str(row.get("commitment_level") or "")
        if commitment not in FINALITY_COMMITMENT_ORDER:
            violations.append("commitment_level_unknown")
        elif FINALITY_COMMITMENT_ORDER[commitment] < FINALITY_COMMITMENT_ORDER.get(min_commitment, 3):
            violations.append("commitment_level_below_minimum")
        if _parse_iso_ts(row.get("block_time")) is None or _parse_iso_ts(row.get("finalized_at")) is None:
            violations.append("finality_timestamp_invalid")
        try:
            if Decimal(str(row.get("slot"))) < 0:
                violations.append("slot_negative")
            if Decimal(str(row.get("indexer_lag_sec"))) > max_indexer_lag:
                violations.append("indexer_lag_above_policy")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("finality_numeric_required")
        if row.get("rpc_consistency_check") != "pass":
            violations.append("rpc_consistency_check_not_pass")
        if row.get("chain_reorg_detected") is not False:
            violations.append("chain_reorg_detected")
        expected_hash = _hash_record_without(row, "finality_hash") if finality_id else None
        if row.get("finality_hash") and expected_hash and row.get("finality_hash") != expected_hash:
            violations.append("finality_hash_mismatch")
        if missing or violations:
            malformed_finalities.append({"index": index, "finality_id": finality_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    provider_schemas = policy.get("provider_schemas") if isinstance(policy.get("provider_schemas"), list) else []
    malformed_provider_schemas = []
    for index, row in enumerate(provider_schemas):
        if not isinstance(row, dict):
            malformed_provider_schemas.append({"index": index, "provider_name": None, "missing_fields": list(PROVIDER_SCHEMA_REQUIRED_FIELDS), "violations": ["provider_schema_not_object"]})
            continue
        provider_name = str(row.get("provider_name") or "")
        missing = _missing_required_fields(row, PROVIDER_SCHEMA_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if not isinstance(row.get("required_fields"), list) or not row.get("required_fields"):
            violations.append("required_fields_required")
        if not isinstance(row.get("optional_fields"), list):
            violations.append("optional_fields_must_be_list")
        if not isinstance(row.get("field_type_contract"), dict) or not row.get("field_type_contract"):
            violations.append("field_type_contract_required")
        if row.get("canary_parse_result") != "pass":
            violations.append("canary_parse_result_not_pass")
        if row.get("schema_drift_detected") is not False:
            violations.append("schema_drift_detected")
        if _parse_iso_ts(row.get("last_schema_check_at")) is None:
            violations.append("last_schema_check_at_invalid")
        for field in ("missing_required_field_rate", "field_type_error_rate", "unexpected_enum_rate", "null_spike_rate"):
            try:
                value = Decimal(str(row.get(field)))
                if value < 0 or value > 1:
                    violations.append(f"{field}_out_of_range")
                elif value != 0:
                    violations.append(f"{field}_must_be_zero_for_seed")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        if row.get("value_range_anomaly") not in (False, 0, "none"):
            violations.append("value_range_anomaly_detected")
        expected_hash = _hash_record_without(row, "schema_hash") if provider_name else None
        if row.get("schema_hash") and expected_hash and row.get("schema_hash") != expected_hash:
            violations.append("provider_schema_hash_mismatch")
        if missing or violations:
            malformed_provider_schemas.append({"index": index, "provider_name": provider_name or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    return [
        _contract(
            "TokenIdentityContract",
            bool(identities) and not policy_errors and not source_errors and not malformed_identities,
            "token_identity_missing_malformed_or_low_confidence",
            {**common, "identity_count": len(identities), "malformed_identities": malformed_identities},
        ),
        _contract(
            "DataUnitContract",
            bool(data_units) and not policy_errors and not source_errors and not malformed_data_units,
            "data_unit_missing_malformed_or_invalid",
            {**common, "data_unit_count": len(data_units), "malformed_data_units": malformed_data_units},
        ),
        _contract(
            "ChainFinalityContract",
            bool(finalities) and not policy_errors and not source_errors and not malformed_finalities,
            "chain_finality_missing_malformed_or_dirty",
            {**common, "finality_count": len(finalities), "malformed_finalities": malformed_finalities},
        ),
        _contract(
            "ProviderSchemaContract",
            bool(provider_schemas) and not policy_errors and not source_errors and not malformed_provider_schemas,
            "provider_schema_missing_malformed_or_drifted",
            {**common, "provider_schema_count": len(provider_schemas), "malformed_provider_schemas": malformed_provider_schemas},
        ),
    ]


def _transition_violations(row, *, states, terminal_states):
    violations = []
    transitions = row.get("allowed_transitions")
    if not isinstance(transitions, list) or not transitions:
        return ["allowed_transitions_required"]
    for index, transition in enumerate(transitions):
        if not isinstance(transition, dict):
            violations.append(f"transition_{index}_not_object")
            continue
        from_state = str(transition.get("from") or "")
        to_state = str(transition.get("to") or "")
        if from_state not in states:
            violations.append(f"transition_{index}_from_state_unknown")
        if to_state not in states:
            violations.append(f"transition_{index}_to_state_unknown")
        if from_state in terminal_states and to_state not in terminal_states:
            violations.append(f"transition_{index}_terminal_state_cannot_reopen")
    return violations


def verify_execution_exit_safety_contracts(
    policy_path=DEFAULT_EXECUTION_EXIT_SAFETY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return _contracts_from_error(
            sorted(EXECUTION_EXIT_SAFETY_CONTRACTS),
            "execution_exit_safety_policy_missing_or_invalid",
            evidence,
        )
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return _contracts_from_error(
            sorted(EXECUTION_EXIT_SAFETY_CONTRACTS),
            "execution_exit_safety_policy_not_object",
            evidence,
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return _contracts_from_error(
            sorted(EXECUTION_EXIT_SAFETY_CONTRACTS),
            "execution_exit_safety_policy_missing_or_invalid",
            evidence,
        )

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.execution_exit_safety_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "execution_exit_safety_blocked":
        policy_errors.append("failure_action_must_block_execution_exit_safety")

    lifecycle_machines = policy.get("lifecycle_state_machines") if isinstance(policy.get("lifecycle_state_machines"), list) else []
    malformed_lifecycle_machines = []
    for index, row in enumerate(lifecycle_machines):
        if not isinstance(row, dict):
            malformed_lifecycle_machines.append({"index": index, "state_machine_id": None, "missing_fields": list(LIFECYCLE_STATE_MACHINE_REQUIRED_FIELDS), "violations": ["lifecycle_state_machine_not_object"]})
            continue
        machine_id = str(row.get("state_machine_id") or "")
        missing = _missing_required_fields(row, LIFECYCLE_STATE_MACHINE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        states = set(str(item) for item in row.get("states", []) if item)
        terminal_states = set(str(item) for item in row.get("terminal_states", []) if item)
        if not states:
            violations.append("states_required")
        if not terminal_states or not terminal_states.issubset(states):
            violations.append("terminal_states_must_be_subset")
        if str(row.get("current_state") or "") not in states:
            violations.append("current_state_unknown")
        if row.get("state_version_fencing_required") is not True:
            violations.append("state_version_fencing_required")
        if row.get("entry_gate_requires_module_closure") is not True:
            violations.append("module_closure_required_for_entry_gate")
        if row.get("invalid_transition_action") != "reject_and_audit":
            violations.append("invalid_transition_action_must_reject_and_audit")
        violations.extend(_transition_violations(row, states=states, terminal_states=terminal_states))
        expected_hash = _hash_record_without(row, "state_machine_hash") if machine_id else None
        if row.get("state_machine_hash") and expected_hash and row.get("state_machine_hash") != expected_hash:
            violations.append("state_machine_hash_mismatch")
        if missing or violations:
            malformed_lifecycle_machines.append({"index": index, "state_machine_id": machine_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    exit_state_machines = policy.get("exit_execution_state_machines") if isinstance(policy.get("exit_execution_state_machines"), list) else []
    malformed_exit_state_machines = []
    for index, row in enumerate(exit_state_machines):
        if not isinstance(row, dict):
            malformed_exit_state_machines.append({"index": index, "exit_state_machine_id": None, "missing_fields": list(EXIT_EXECUTION_STATE_MACHINE_REQUIRED_FIELDS), "violations": ["exit_state_machine_not_object"]})
            continue
        machine_id = str(row.get("exit_state_machine_id") or "")
        missing = _missing_required_fields(row, EXIT_EXECUTION_STATE_MACHINE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        states = set(str(item) for item in row.get("states", []) if item)
        terminal_states = set(str(item) for item in row.get("terminal_states", []) if item)
        if str(row.get("open_position_state") or "") not in states:
            violations.append("open_position_state_unknown")
        if not terminal_states or not terminal_states.issubset(states):
            violations.append("terminal_states_must_be_subset")
        if row.get("exit_quote_required") is not True:
            violations.append("exit_quote_required")
        if row.get("lease_fencing_required") is not True:
            violations.append("lease_fencing_required")
        if row.get("state_revalidation_required") is not True:
            violations.append("state_revalidation_required")
        if row.get("exit_safety_preserved") is not True:
            violations.append("exit_safety_must_be_preserved")
        if not isinstance(row.get("failure_events"), list) or not row.get("failure_events"):
            violations.append("failure_events_required")
        violations.extend(_transition_violations(row, states=states, terminal_states=terminal_states))
        expected_hash = _hash_record_without(row, "exit_state_machine_hash") if machine_id else None
        if row.get("exit_state_machine_hash") and expected_hash and row.get("exit_state_machine_hash") != expected_hash:
            violations.append("exit_state_machine_hash_mismatch")
        if missing or violations:
            malformed_exit_state_machines.append({"index": index, "exit_state_machine_id": machine_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    exit_policies = policy.get("exit_policies") if isinstance(policy.get("exit_policies"), list) else []
    malformed_exit_policies = []
    for index, row in enumerate(exit_policies):
        if not isinstance(row, dict):
            malformed_exit_policies.append({"index": index, "exit_policy_id": None, "missing_fields": list(EXIT_POLICY_REQUIRED_FIELDS), "violations": ["exit_policy_not_object"]})
            continue
        policy_id = str(row.get("exit_policy_id") or "")
        missing = _missing_required_fields(row, EXIT_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if not str(row.get("exit_policy_version") or "").startswith("v2.7.0"):
            violations.append("exit_policy_version_must_be_v2_7")
        if "normal_tiny" not in set(str(item) for item in row.get("applies_to_modes", []) if item):
            violations.append("normal_tiny_mode_required")
        for field in ("take_profit_rules", "stop_loss_rules", "time_stop_rules"):
            if not isinstance(row.get(field), list) or not row.get(field):
                violations.append(f"{field}_required")
        if row.get("entry_outcome_separation") is not True:
            violations.append("entry_outcome_separation_required")
        if _parse_iso_ts(row.get("effective_from")) is None:
            violations.append("effective_from_invalid")
        expected_hash = _hash_record_without(row, "exit_policy_hash") if policy_id else None
        if row.get("exit_policy_hash") and expected_hash and row.get("exit_policy_hash") != expected_hash:
            violations.append("exit_policy_hash_mismatch")
        if missing or violations:
            malformed_exit_policies.append({"index": index, "exit_policy_id": policy_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    circuit_breakers = policy.get("circuit_breaker_position_policies") if isinstance(policy.get("circuit_breaker_position_policies"), list) else []
    malformed_circuit_breakers = []
    for index, row in enumerate(circuit_breakers):
        if not isinstance(row, dict):
            malformed_circuit_breakers.append({"index": index, "policy_id": None, "missing_fields": list(CIRCUIT_BREAKER_POSITION_POLICY_REQUIRED_FIELDS), "violations": ["circuit_breaker_position_policy_not_object"]})
            continue
        policy_id = str(row.get("policy_id") or "")
        missing = _missing_required_fields(row, CIRCUIT_BREAKER_POSITION_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if not isinstance(row.get("trigger_events"), list) or not row.get("trigger_events"):
            violations.append("trigger_events_required")
        if row.get("new_entry_disabled") is not True:
            violations.append("new_entry_must_be_disabled")
        if row.get("exit_safety_remains_active") is not True:
            violations.append("exit_safety_must_remain_active")
        if str(row.get("open_position_policy") or "") not in {"exit_only", "emergency_exit_allowed"}:
            violations.append("open_position_policy_not_safe")
        if row.get("operator_ack_required") is not True:
            violations.append("operator_ack_required")
        if not str(row.get("resume_condition") or "").strip():
            violations.append("resume_condition_required")
        expected_hash = _hash_record_without(row, "circuit_breaker_hash") if policy_id else None
        if row.get("circuit_breaker_hash") and expected_hash and row.get("circuit_breaker_hash") != expected_hash:
            violations.append("circuit_breaker_hash_mismatch")
        if missing or violations:
            malformed_circuit_breakers.append({"index": index, "policy_id": policy_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    emergency_journals = policy.get("emergency_exit_journals") if isinstance(policy.get("emergency_exit_journals"), list) else []
    malformed_emergency_journals = []
    for index, row in enumerate(emergency_journals):
        if not isinstance(row, dict):
            malformed_emergency_journals.append({"index": index, "journal_id": None, "missing_fields": list(EMERGENCY_EXIT_JOURNAL_REQUIRED_FIELDS), "violations": ["emergency_exit_journal_not_object"]})
            continue
        journal_id = str(row.get("journal_id") or "")
        missing = _missing_required_fields(row, EMERGENCY_EXIT_JOURNAL_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        initiated_at = _parse_iso_ts(row.get("initiated_at"))
        completed_at = _parse_iso_ts(row.get("completed_at"))
        if initiated_at is None or completed_at is None:
            violations.append("journal_timestamp_invalid")
        elif completed_at < initiated_at:
            violations.append("journal_timestamp_order_invalid")
        for field in ("journal_event_id", "position_id", "reason", "outcome"):
            if not str(row.get(field) or "").strip():
                violations.append(f"{field}_required")
        if row.get("reconciled_to_ledger") is not True:
            violations.append("journal_must_reconcile_to_ledger")
        if row.get("journal_append_only") is not True:
            violations.append("journal_must_be_append_only")
        if row.get("operator_audit_required") is not True:
            violations.append("operator_audit_required")
        expected_hash = _hash_record_without(row, "journal_hash") if journal_id else None
        if row.get("journal_hash") and expected_hash and row.get("journal_hash") != expected_hash:
            violations.append("journal_hash_mismatch")
        if missing or violations:
            malformed_emergency_journals.append({"index": index, "journal_id": journal_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    exit_queue_health = policy.get("exit_queue_health") if isinstance(policy.get("exit_queue_health"), list) else []
    malformed_exit_queue_health = []
    for index, row in enumerate(exit_queue_health):
        if not isinstance(row, dict):
            malformed_exit_queue_health.append({"index": index, "queue_id": None, "missing_fields": list(EXIT_QUEUE_HEALTH_REQUIRED_FIELDS), "violations": ["exit_queue_health_not_object"]})
            continue
        queue_id = str(row.get("queue_id") or "")
        missing = _missing_required_fields(row, EXIT_QUEUE_HEALTH_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("exit_queue_status") != "healthy":
            violations.append("exit_queue_status_not_healthy")
        try:
            oldest = Decimal(str(row.get("oldest_open_exit_age_sec")))
            max_allowed = Decimal(str(row.get("max_allowed_open_exit_age_sec")))
            if oldest < 0 or max_allowed < 0:
                violations.append("exit_queue_age_negative")
            elif oldest > max_allowed:
                violations.append("oldest_exit_age_above_policy")
            for field in ("stuck_open_position_count", "exit_quote_failure_count", "exit_state_machine_failure_count"):
                if Decimal(str(row.get(field))) != 0:
                    violations.append(f"{field}_must_be_zero")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("exit_queue_numeric_required")
        if row.get("exit_safety_budget_reserved") is not True:
            violations.append("exit_safety_budget_must_be_reserved")
        expected_hash = _hash_record_without(row, "queue_health_hash") if queue_id else None
        if row.get("queue_health_hash") and expected_hash and row.get("queue_health_hash") != expected_hash:
            violations.append("queue_health_hash_mismatch")
        if missing or violations:
            malformed_exit_queue_health.append({"index": index, "queue_id": queue_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    return [
        _contract(
            "LifecycleStateMachineContract",
            bool(lifecycle_machines) and not policy_errors and not source_errors and not malformed_lifecycle_machines,
            "lifecycle_state_machine_missing_malformed_or_unsafe",
            {**common, "lifecycle_state_machine_count": len(lifecycle_machines), "malformed_lifecycle_machines": malformed_lifecycle_machines},
        ),
        _contract(
            "ExitExecutionStateMachine",
            bool(exit_state_machines) and not policy_errors and not source_errors and not malformed_exit_state_machines,
            "exit_execution_state_machine_missing_malformed_or_unsafe",
            {**common, "exit_state_machine_count": len(exit_state_machines), "malformed_exit_state_machines": malformed_exit_state_machines},
        ),
        _contract(
            "ExitPolicyContract",
            bool(exit_policies) and not policy_errors and not source_errors and not malformed_exit_policies,
            "exit_policy_missing_malformed_or_unversioned",
            {**common, "exit_policy_count": len(exit_policies), "malformed_exit_policies": malformed_exit_policies},
        ),
        _contract(
            "CircuitBreakerPositionPolicy",
            bool(circuit_breakers) and not policy_errors and not source_errors and not malformed_circuit_breakers,
            "circuit_breaker_position_policy_missing_malformed_or_unsafe",
            {**common, "circuit_breaker_policy_count": len(circuit_breakers), "malformed_circuit_breakers": malformed_circuit_breakers},
        ),
        _contract(
            "EmergencyExitJournal",
            bool(emergency_journals) and not policy_errors and not source_errors and not malformed_emergency_journals,
            "emergency_exit_journal_missing_malformed_or_unreconciled",
            {**common, "emergency_exit_journal_count": len(emergency_journals), "malformed_emergency_journals": malformed_emergency_journals},
        ),
        _contract(
            "ExitQueueHealthContract",
            bool(exit_queue_health) and not policy_errors and not source_errors and not malformed_exit_queue_health,
            "exit_queue_health_missing_malformed_or_unhealthy",
            {**common, "exit_queue_health_count": len(exit_queue_health), "malformed_exit_queue_health": malformed_exit_queue_health},
        ),
    ]


def _list_metric_threshold_violations(row, metric_records, threshold_records):
    violations = []
    metric_ids = row.get("metric_ids") if isinstance(row.get("metric_ids"), list) else []
    threshold_ids = row.get("threshold_ids") if isinstance(row.get("threshold_ids"), list) else []
    if not metric_ids:
        violations.append("metric_ids_required")
    if not threshold_ids:
        violations.append("threshold_ids_required")
    for metric_id in metric_ids:
        if str(metric_id) not in metric_records:
            violations.append(f"metric_id_unknown:{metric_id}")
    for threshold_id in threshold_ids:
        threshold = threshold_records.get(str(threshold_id))
        if not threshold:
            violations.append(f"threshold_id_unknown:{threshold_id}")
            continue
        applies_to = str(threshold.get("applies_to_metric") or "")
        if applies_to and applies_to not in {str(metric_id) for metric_id in metric_ids}:
            violations.append(f"threshold_metric_mismatch:{threshold_id}")
    return violations


def verify_delivery_traceability_contracts(
    policy_path=DEFAULT_DELIVERY_TRACEABILITY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return _contracts_from_error(
            sorted(DELIVERY_TRACEABILITY_CONTRACTS),
            "delivery_traceability_policy_missing_or_invalid",
            evidence,
        )
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return _contracts_from_error(
            sorted(DELIVERY_TRACEABILITY_CONTRACTS),
            "delivery_traceability_policy_not_object",
            evidence,
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return _contracts_from_error(
            sorted(DELIVERY_TRACEABILITY_CONTRACTS),
            "delivery_traceability_policy_missing_or_invalid",
            evidence,
        )

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.delivery_traceability_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "delivery_traceability_blocked":
        policy_errors.append("failure_action_must_block_delivery_traceability")

    repair_classes_required = {
        "auto_repair_allowed",
        "manual_review_required",
        "repair_forbidden",
        "rebuild_projection_only",
        "quarantine_trade",
    }
    reconciliation_rows = policy.get("reconciliation_policies") if isinstance(policy.get("reconciliation_policies"), list) else []
    malformed_reconciliation = []
    repair_classes_seen = set()
    for index, row in enumerate(reconciliation_rows):
        if not isinstance(row, dict):
            malformed_reconciliation.append({"index": index, "reconciliation_policy_id": None, "missing_fields": list(RECONCILIATION_POLICY_REQUIRED_FIELDS), "violations": ["reconciliation_policy_not_object"]})
            continue
        policy_id = str(row.get("reconciliation_policy_id") or "")
        mismatch_class = str(row.get("mismatch_class") or "")
        repair_class = str(row.get("repair_class") or "")
        repair_classes_seen.add(repair_class)
        missing = _missing_required_fields(row, RECONCILIATION_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if repair_class not in repair_classes_required:
            violations.append("repair_class_unknown")
        if row.get("audit_required") is not True:
            violations.append("audit_required")
        if "ledger" in mismatch_class and row.get("auto_repair_allowed") is True:
            violations.append("ledger_mismatch_cannot_auto_repair")
        if mismatch_class == "materialized_view_mismatch" and repair_class != "rebuild_projection_only":
            violations.append("materialized_view_mismatch_must_rebuild_projection_only")
        if mismatch_class == "paper_trade_vs_ledger_mismatch" and repair_class != "quarantine_trade":
            violations.append("paper_trade_ledger_mismatch_must_quarantine_trade")
        if repair_class == "quarantine_trade" and row.get("promotion_evidence_allowed") is not False:
            violations.append("quarantined_trade_must_be_excluded_from_promotion")
        if repair_class in {"manual_review_required", "repair_forbidden"} and row.get("manual_review_required") is not True:
            violations.append("manual_review_required")
        if not str(row.get("dashboard_surface") or "").strip():
            violations.append("dashboard_surface_required")
        expected_hash = _hash_record_without(row, "reconciliation_hash") if policy_id else None
        if row.get("reconciliation_hash") and expected_hash and row.get("reconciliation_hash") != expected_hash:
            violations.append("reconciliation_hash_mismatch")
        if missing or violations:
            malformed_reconciliation.append({"index": index, "reconciliation_policy_id": policy_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_repair_classes = sorted(repair_classes_required - repair_classes_seen)

    dashboard_rows = policy.get("dashboard_staleness_panels") if isinstance(policy.get("dashboard_staleness_panels"), list) else []
    malformed_dashboard = []
    for index, row in enumerate(dashboard_rows):
        if not isinstance(row, dict):
            malformed_dashboard.append({"index": index, "panel_name": None, "missing_fields": list(DASHBOARD_STALENESS_REQUIRED_FIELDS), "violations": ["dashboard_staleness_panel_not_object"]})
            continue
        panel_name = str(row.get("panel_name") or "")
        missing = _missing_required_fields(row, DASHBOARD_STALENESS_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        staleness_threshold_id = str(row.get("staleness_threshold_id") or "")
        if staleness_threshold_id != str(row.get("threshold_id") or ""):
            violations.append("staleness_threshold_id_must_match_threshold_id")
        try:
            data_seq = int(row.get("data_seq"))
            latest_seq = int(row.get("event_log_latest_seq"))
            if data_seq > latest_seq:
                violations.append("data_seq_cannot_exceed_event_log_latest_seq")
        except (TypeError, ValueError):
            violations.append("dashboard_seq_numeric_required")
        try:
            lag = Decimal(str(row.get("panel_lag_sec")))
            max_lag = Decimal(str(row.get("max_allowed_panel_lag_sec")))
            if lag < 0 or max_lag < 0:
                violations.append("dashboard_lag_negative")
            elif lag > max_lag:
                violations.append("panel_lag_above_threshold")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("dashboard_lag_numeric_required")
        if row.get("stale_banner_required") is not True:
            violations.append("stale_banner_required")
        if row.get("operator_override_allowed") is not False:
            violations.append("operator_override_must_be_disabled_on_dashboard_panel")
        if _parse_iso_ts(row.get("last_refresh_at")) is None:
            violations.append("last_refresh_at_invalid")
        expected_hash = _hash_record_without(row, "panel_hash") if panel_name else None
        if row.get("panel_hash") and expected_hash and row.get("panel_hash") != expected_hash:
            violations.append("panel_hash_mismatch")
        if missing or violations:
            malformed_dashboard.append({"index": index, "panel_name": panel_name or None, "missing_fields": missing, "violations": sorted(set(violations))})

    required_contracts = set(DELIVERY_TRACEABILITY_CONTRACTS)
    traceability_rows = policy.get("spec_traceability_matrix") if isinstance(policy.get("spec_traceability_matrix"), list) else []
    malformed_traceability = []
    traced_contracts = set()
    allowed_trace_status = {"tested", "deployed", "validated"}
    for index, row in enumerate(traceability_rows):
        if not isinstance(row, dict):
            malformed_traceability.append({"index": index, "traceability_id": None, "missing_fields": list(SPEC_TRACEABILITY_MATRIX_REQUIRED_FIELDS), "violations": ["traceability_row_not_object"]})
            continue
        traceability_id = str(row.get("traceability_id") or "")
        contract_id = str(row.get("contract_id") or "")
        if contract_id:
            traced_contracts.add(contract_id)
        missing = _missing_required_fields(row, SPEC_TRACEABILITY_MATRIX_REQUIRED_FIELDS)
        violations = _list_metric_threshold_violations(row, metric_records, threshold_records)
        if str(row.get("status") or "") not in allowed_trace_status:
            violations.append("traceability_status_not_tested_deployed_or_validated")
        for field in ("spec_section_id", "requirement", "implementation_module", "test_file", "dashboard_surface", "rollout_flag", "issue_id", "owner"):
            if not str(row.get(field) or "").strip():
                violations.append(f"{field}_required")
        expected_hash = _hash_record_without(row, "traceability_hash") if traceability_id else None
        if row.get("traceability_hash") and expected_hash and row.get("traceability_hash") != expected_hash:
            violations.append("traceability_hash_mismatch")
        if missing or violations:
            malformed_traceability.append({"index": index, "traceability_id": traceability_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_traceability_contracts = sorted(required_contracts - traced_contracts)

    issue_rows = policy.get("implementation_issue_graph") if isinstance(policy.get("implementation_issue_graph"), list) else []
    issue_ids = {
        str(row.get("issue_id"))
        for row in issue_rows
        if isinstance(row, dict) and row.get("issue_id")
    }
    malformed_issues = []
    allowed_issue_status = {"done", "validated"}
    for index, row in enumerate(issue_rows):
        if not isinstance(row, dict):
            malformed_issues.append({"index": index, "issue_id": None, "missing_fields": list(IMPLEMENTATION_ISSUE_GRAPH_REQUIRED_FIELDS), "violations": ["issue_graph_row_not_object"]})
            continue
        issue_id = str(row.get("issue_id") or "")
        missing = _missing_required_fields(row, IMPLEMENTATION_ISSUE_GRAPH_REQUIRED_FIELDS)
        if "dependency_ids" in missing and isinstance(row.get("dependency_ids"), list):
            missing.remove("dependency_ids")
        violations = _list_metric_threshold_violations(row, metric_records, threshold_records)
        if str(row.get("status") or "") not in allowed_issue_status:
            violations.append("issue_status_not_done_or_validated")
        if row.get("status") in allowed_issue_status and not row.get("acceptance_tests"):
            violations.append("acceptance_tests_required_for_done")
        if not isinstance(row.get("dependency_ids"), list):
            violations.append("dependency_ids_must_be_list")
        for dependency_id in row.get("dependency_ids", []) if isinstance(row.get("dependency_ids"), list) else []:
            if str(dependency_id) not in issue_ids:
                violations.append(f"dependency_id_unknown:{dependency_id}")
        if not isinstance(row.get("spec_section_ids"), list) or not row.get("spec_section_ids"):
            violations.append("spec_section_ids_required")
        expected_hash = _hash_record_without(row, "issue_hash") if issue_id else None
        if row.get("issue_hash") and expected_hash and row.get("issue_hash") != expected_hash:
            violations.append("issue_hash_mismatch")
        if missing or violations:
            malformed_issues.append({"index": index, "issue_id": issue_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    module_rows = policy.get("module_closures") if isinstance(policy.get("module_closures"), list) else []
    malformed_modules = []
    module_contracts_seen = set()
    for index, row in enumerate(module_rows):
        if not isinstance(row, dict):
            malformed_modules.append({"index": index, "module_name": None, "missing_fields": list(MODULE_CLOSURE_REQUIRED_FIELDS), "violations": ["module_closure_row_not_object"]})
            continue
        module_name = str(row.get("module_name") or "")
        for contract_id in row.get("contract_ids", []) if isinstance(row.get("contract_ids"), list) else []:
            module_contracts_seen.add(str(contract_id))
        missing = _missing_required_fields(row, MODULE_CLOSURE_REQUIRED_FIELDS)
        violations = _list_metric_threshold_violations(row, metric_records, threshold_records)
        contract_tests = row.get("contract_tests") if isinstance(row.get("contract_tests"), list) else []
        if not contract_tests:
            violations.append("contract_tests_required")
        for test_index, test in enumerate(contract_tests):
            if not isinstance(test, dict):
                violations.append(f"contract_test_{test_index}_not_object")
                continue
            if test.get("status") != "pass":
                violations.append("contract_tests_must_pass")
        for field in ("input_events", "output_events", "decision_fields", "failure_events", "outcome_metrics", "governance_rules", "spec_section_ids", "runtime_config_keys"):
            if not isinstance(row.get(field), list) or not row.get(field):
                violations.append(f"{field}_required")
        if not str(row.get("kill_condition") or "").strip():
            violations.append("kill_condition_required")
        if not str(row.get("dashboard_surface") or "").strip():
            violations.append("dashboard_surface_required")
        if not str(row.get("mode_readiness_target") or "").strip():
            violations.append("mode_readiness_target_required")
        expected_hash = _hash_record_without(row, "module_closure_hash") if module_name else None
        if row.get("module_closure_hash") and expected_hash and row.get("module_closure_hash") != expected_hash:
            violations.append("module_closure_hash_mismatch")
        if missing or violations:
            malformed_modules.append({"index": index, "module_name": module_name or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_module_closure_contracts = sorted(required_contracts - module_contracts_seen)

    decommission_rows = policy.get("decommission_policies") if isinstance(policy.get("decommission_policies"), list) else []
    malformed_decommission = []
    for index, row in enumerate(decommission_rows):
        if not isinstance(row, dict):
            malformed_decommission.append({"index": index, "artifact_id": None, "missing_fields": list(DECOMMISSION_POLICY_REQUIRED_FIELDS), "violations": ["decommission_policy_row_not_object"]})
            continue
        artifact_id = str(row.get("artifact_id") or "")
        status = str(row.get("status") or "")
        missing = _missing_required_fields(row, DECOMMISSION_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if status not in {"deprecated", "retired"}:
            violations.append("decommission_status_not_deprecated_or_retired")
        if status == "retired":
            if row.get("runtime_reference_allowed") is not False:
                violations.append("retired_artifact_runtime_reference_forbidden")
            if row.get("training_reference_allowed") is not False:
                violations.append("retired_artifact_training_reference_forbidden")
        if status == "deprecated" and row.get("new_promotion_evidence_allowed") is not False:
            violations.append("deprecated_artifact_cannot_support_new_promotion")
        if row.get("operator_audit_required") is not True:
            violations.append("operator_audit_required")
        if row.get("direct_entry_allowed") is not False:
            violations.append("direct_entry_must_be_false")
        if row.get("artifact_type") == "route" and ("hard_gate" in artifact_id or "source_resonance" in artifact_id) and row.get("runtime_reference_allowed") is not False:
            violations.append("old_direct_entry_route_alias_runtime_reference_forbidden")
        retired_at = str(row.get("retired_at") or "")
        if retired_at != "open" and _parse_iso_ts(retired_at) is None:
            violations.append("retired_at_invalid")
        if _parse_iso_ts(row.get("deprecated_at")) is None:
            violations.append("deprecated_at_invalid")
        expected_hash = _hash_record_without(row, "decommission_hash") if artifact_id else None
        if row.get("decommission_hash") and expected_hash and row.get("decommission_hash") != expected_hash:
            violations.append("decommission_hash_mismatch")
        if missing or violations:
            malformed_decommission.append({"index": index, "artifact_id": artifact_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    return [
        _contract(
            "ReconciliationPolicyContract",
            bool(reconciliation_rows) and not policy_errors and not source_errors and not missing_repair_classes and not malformed_reconciliation,
            "reconciliation_policy_missing_malformed_or_unsafe",
            {
                **common,
                "reconciliation_policy_count": len(reconciliation_rows),
                "missing_repair_classes": missing_repair_classes,
                "malformed_reconciliation_policies": malformed_reconciliation,
            },
        ),
        _contract(
            "DashboardStalenessContract",
            bool(dashboard_rows) and not policy_errors and not source_errors and not malformed_dashboard,
            "dashboard_staleness_missing_malformed_or_unsafe",
            {
                **common,
                "dashboard_panel_count": len(dashboard_rows),
                "malformed_dashboard_staleness_panels": malformed_dashboard,
            },
        ),
        _contract(
            "SpecTraceabilityMatrix",
            bool(traceability_rows) and not policy_errors and not source_errors and not missing_traceability_contracts and not malformed_traceability,
            "spec_traceability_matrix_missing_malformed_or_incomplete",
            {
                **common,
                "traceability_row_count": len(traceability_rows),
                "missing_traceability_contracts": missing_traceability_contracts,
                "malformed_traceability_rows": malformed_traceability,
            },
        ),
        _contract(
            "ImplementationIssueGraphContract",
            bool(issue_rows) and not policy_errors and not source_errors and not malformed_issues,
            "implementation_issue_graph_missing_malformed_or_incomplete",
            {
                **common,
                "issue_count": len(issue_rows),
                "malformed_issues": malformed_issues,
            },
        ),
        _contract(
            "ModuleClosureContract",
            bool(module_rows) and not policy_errors and not source_errors and not missing_module_closure_contracts and not malformed_modules,
            "module_closure_missing_malformed_or_ungated",
            {
                **common,
                "module_closure_count": len(module_rows),
                "missing_module_closure_contracts": missing_module_closure_contracts,
                "malformed_module_closures": malformed_modules,
            },
        ),
        _contract(
            "DecommissionPolicyContract",
            bool(decommission_rows) and not policy_errors and not source_errors and not malformed_decommission,
            "decommission_policy_missing_malformed_or_unsafe",
            {
                **common,
                "decommission_policy_count": len(decommission_rows),
                "malformed_decommission_policies": malformed_decommission,
            },
        ),
    ]


def _release_row_hash_violation(row, hash_field, identity_field):
    identity = str(row.get(identity_field) or "")
    expected_hash = _hash_record_without(row, hash_field) if identity else None
    if row.get(hash_field) and expected_hash and row.get(hash_field) != expected_hash:
        return f"{hash_field}_mismatch"
    return None


def verify_release_experiment_safety_contracts(
    policy_path=DEFAULT_RELEASE_EXPERIMENT_SAFETY_POLICY,
    metric_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
):
    try:
        policy = _load_json(policy_path)
        metric_registry = _load_json(metric_registry_path)
        threshold_catalog = _load_json(threshold_catalog_path)
    except Exception as exc:
        evidence = {"policy_path": str(policy_path), "error": str(exc)}
        return _contracts_from_error(
            sorted(RELEASE_EXPERIMENT_SAFETY_CONTRACTS),
            "release_experiment_safety_policy_missing_or_invalid",
            evidence,
        )
    if not isinstance(policy, dict):
        evidence = {"policy_path": str(policy_path)}
        return _contracts_from_error(
            sorted(RELEASE_EXPERIMENT_SAFETY_CONTRACTS),
            "release_experiment_safety_policy_not_object",
            evidence,
        )
    if not isinstance(metric_registry, dict) or not isinstance(threshold_catalog, dict):
        evidence = {"policy_path": str(policy_path), "error": "metric_or_threshold_registry_not_object"}
        return _contracts_from_error(
            sorted(RELEASE_EXPERIMENT_SAFETY_CONTRACTS),
            "release_experiment_safety_policy_missing_or_invalid",
            evidence,
        )

    metric_records, threshold_records = _metric_threshold_maps(metric_registry, threshold_catalog)
    policy_errors = []
    if policy.get("schema_version") != "v2.7.0.release_experiment_safety_policy.v1":
        policy_errors.append("schema_version_mismatch")
    if policy.get("failure_action") != "release_experiment_safety_blocked":
        policy_errors.append("failure_action_must_block_release_experiment_safety")

    secrets = policy.get("secrets_management") if isinstance(policy.get("secrets_management"), list) else []
    malformed_secrets = []
    for index, row in enumerate(secrets):
        if not isinstance(row, dict):
            malformed_secrets.append({"index": index, "secret_name": None, "missing_fields": list(SECRETS_MANAGEMENT_REQUIRED_FIELDS), "violations": ["secret_row_not_object"]})
            continue
        name = str(row.get("secret_name") or "")
        scope = str(row.get("scope") or "")
        environments = set(str(item) for item in row.get("environment_allowed", []) if item) if isinstance(row.get("environment_allowed"), list) else set()
        missing = _missing_required_fields(row, SECRETS_MANAGEMENT_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            if int(row.get("rotation_interval_days")) <= 0:
                violations.append("rotation_interval_days_positive_required")
        except (TypeError, ValueError):
            violations.append("rotation_interval_days_positive_required")
        if _parse_iso_ts(row.get("last_rotated_at")) is None:
            violations.append("last_rotated_at_invalid")
        if row.get("leak_detected") is not False:
            violations.append("leak_detected_must_be_false")
        if str(row.get("revocation_status") or "") not in {"active", "revoked_with_rotation_complete"}:
            violations.append("revocation_status_invalid")
        if not environments:
            violations.append("environment_allowed_required")
        if scope == "dashboard_token" and row.get("mutation_scope_allowed") is not False:
            violations.append("dashboard_token_mutation_scope_forbidden")
        if scope == "live_signing_secret" and environments != {"live"}:
            violations.append("live_signing_secret_must_be_live_only")
        if "paper" in environments and "live" in environments:
            violations.append("paper_and_live_secret_environment_must_not_mix")
        hash_violation = _release_row_hash_violation(row, "secret_hash", "secret_name")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_secrets.append({"index": index, "secret_name": name or None, "missing_fields": missing, "violations": sorted(set(violations))})

    slos = policy.get("system_slos") if isinstance(policy.get("system_slos"), list) else []
    malformed_slos = []
    for index, row in enumerate(slos):
        if not isinstance(row, dict):
            malformed_slos.append({"index": index, "slo_id": None, "missing_fields": list(SYSTEM_SLO_REQUIRED_FIELDS), "violations": ["slo_row_not_object"]})
            continue
        slo_id = str(row.get("slo_id") or "")
        missing = _missing_required_fields(row, SYSTEM_SLO_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "measured_value")
        if str(row.get("status") or "") != "healthy":
            violations.append("slo_status_not_healthy")
        if str(row.get("severity") or "") in {"P0", "P1", "critical"}:
            violations.append("critical_slo_unresolved")
        if str(row.get("new_entry_action") or "") not in {"allow", "shadow_only", "circuit_breaker"}:
            violations.append("new_entry_action_invalid")
        if str(row.get("exit_safety_action") or "") != "preserve_exit_safety":
            violations.append("exit_safety_must_be_preserved")
        if str(row.get("new_entry_action") or "") == "allow" and str(row.get("status") or "") != "healthy":
            violations.append("new_entry_allow_requires_healthy_slo")
        hash_violation = _release_row_hash_violation(row, "slo_hash", "slo_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_slos.append({"index": index, "slo_id": slo_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    no_trade_rows = policy.get("no_trade_root_causes") if isinstance(policy.get("no_trade_root_causes"), list) else []
    malformed_no_trade = []
    no_trade_codes = set()
    for index, row in enumerate(no_trade_rows):
        if not isinstance(row, dict):
            malformed_no_trade.append({"index": index, "root_cause_id": None, "missing_fields": list(NO_TRADE_ROOT_CAUSE_REQUIRED_FIELDS), "violations": ["no_trade_row_not_object"]})
            continue
        root_cause_id = str(row.get("root_cause_id") or "")
        code = str(row.get("root_cause_code") or "")
        no_trade_codes.add(code)
        missing = _missing_required_fields(row, NO_TRADE_ROOT_CAUSE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            d3a_count = int(row.get("d3a_candidate_count"))
            fill_count = int(row.get("fill_count"))
            if d3a_count < 0 or fill_count < 0:
                violations.append("candidate_and_fill_counts_nonnegative_required")
            if d3a_count > 0 and fill_count == 0 and str(row.get("category") or "") in {"", "unknown"}:
                violations.append("d3a_zero_fill_requires_known_root_cause")
        except (TypeError, ValueError):
            violations.append("candidate_and_fill_counts_numeric_required")
        if not str(row.get("remediation_action") or "").strip():
            violations.append("remediation_action_required")
        hash_violation = _release_row_hash_violation(row, "root_cause_hash", "root_cause_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_no_trade.append({"index": index, "root_cause_id": root_cause_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    complexity_rows = policy.get("release_complexity_budgets") if isinstance(policy.get("release_complexity_budgets"), list) else []
    malformed_complexity = []
    for index, row in enumerate(complexity_rows):
        if not isinstance(row, dict):
            malformed_complexity.append({"index": index, "release_id": None, "missing_fields": list(RELEASE_COMPLEXITY_REQUIRED_FIELDS), "violations": ["complexity_row_not_object"]})
            continue
        release_id = str(row.get("release_id") or "")
        missing = _missing_required_fields(row, RELEASE_COMPLEXITY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        numeric_fields = ("max_new_gates_per_release", "new_gates", "max_new_detectors_per_release", "new_detectors", "required_shadow_hours_before_gate", "observed_shadow_hours")
        values = {}
        for field in numeric_fields:
            try:
                values[field] = Decimal(str(row.get(field)))
                if values[field] < 0:
                    violations.append(f"{field}_nonnegative_required")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        if not violations:
            if values["new_gates"] > values["max_new_gates_per_release"]:
                violations.append("new_gates_exceed_release_budget")
            if values["new_detectors"] > values["max_new_detectors_per_release"]:
                violations.append("new_detectors_exceed_release_budget")
            if values["observed_shadow_hours"] < values["required_shadow_hours_before_gate"]:
                violations.append("required_shadow_hours_not_met")
        if str(row.get("status") or "") != "within_budget":
            violations.append("release_complexity_status_not_within_budget")
        if not str(row.get("rollback_metric") or "").strip():
            violations.append("rollback_metric_required")
        hash_violation = _release_row_hash_violation(row, "complexity_hash", "release_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_complexity.append({"index": index, "release_id": release_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    backpressure_rows = policy.get("backpressure_policies") if isinstance(policy.get("backpressure_policies"), list) else []
    malformed_backpressure = []
    for index, row in enumerate(backpressure_rows):
        if not isinstance(row, dict):
            malformed_backpressure.append({"index": index, "component": None, "missing_fields": list(BACKPRESSURE_POLICY_REQUIRED_FIELDS), "violations": ["backpressure_row_not_object"]})
            continue
        component = str(row.get("component") or "")
        missing = _missing_required_fields(row, BACKPRESSURE_POLICY_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        try:
            queue_depth = Decimal(str(row.get("queue_depth")))
            max_queue_depth = Decimal(str(row.get("max_queue_depth")))
            if queue_depth < 0 or max_queue_depth < 0:
                violations.append("queue_depth_nonnegative_required")
            if queue_depth > max_queue_depth:
                violations.append("queue_depth_exceeds_max")
        except (InvalidOperation, ValueError, TypeError):
            violations.append("queue_depth_numeric_required")
        if row.get("drops_p0_p1_allowed") is not False:
            violations.append("p0_p1_drop_forbidden")
        if str(row.get("exit_safety_priority") or "") != "reserved_first":
            violations.append("exit_safety_priority_must_be_reserved_first")
        if str(row.get("backpressure_action") or "") in {"drop_p0", "drop_p1", "drop_exit"}:
            violations.append("critical_backpressure_action_forbidden")
        hash_violation = _release_row_hash_violation(row, "backpressure_hash", "component")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_backpressure.append({"index": index, "component": component or None, "missing_fields": missing, "violations": sorted(set(violations))})

    reserve_rows = policy.get("budget_reserves") if isinstance(policy.get("budget_reserves"), list) else []
    malformed_reserves = []
    reserve_priorities = set()
    for index, row in enumerate(reserve_rows):
        if not isinstance(row, dict):
            malformed_reserves.append({"index": index, "reserve_id": None, "missing_fields": list(BUDGET_RESERVE_REQUIRED_FIELDS), "violations": ["budget_reserve_row_not_object"]})
            continue
        reserve_id = str(row.get("reserve_id") or "")
        priority = str(row.get("priority_class") or "")
        reserve_priorities.add(priority)
        missing = _missing_required_fields(row, BUDGET_RESERVE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        for field in ("reserved_amount", "current_usage", "hard_limit"):
            try:
                if Decimal(str(row.get(field))) < 0:
                    violations.append(f"{field}_nonnegative_required")
            except (InvalidOperation, ValueError, TypeError):
                violations.append(f"{field}_numeric_required")
        try:
            if Decimal(str(row.get("current_usage"))) > Decimal(str(row.get("hard_limit"))):
                violations.append("current_usage_exceeds_hard_limit")
        except (InvalidOperation, ValueError, TypeError):
            pass
        if priority in {"P0", "P1"} and row.get("borrow_allowed") is not False:
            violations.append("p0_p1_reserve_borrow_forbidden")
        if not isinstance(row.get("reserved_for"), list) or not row.get("reserved_for"):
            violations.append("reserved_for_required")
        hash_violation = _release_row_hash_violation(row, "reserve_hash", "reserve_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_reserves.append({"index": index, "reserve_id": reserve_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_reserve_priorities = sorted({"P0", "P1"} - reserve_priorities)

    holdout_rows = policy.get("blinded_holdouts") if isinstance(policy.get("blinded_holdouts"), list) else []
    malformed_holdouts = []
    for index, row in enumerate(holdout_rows):
        if not isinstance(row, dict):
            malformed_holdouts.append({"index": index, "holdout_id": None, "missing_fields": list(BLINDED_HOLDOUT_REQUIRED_FIELDS), "violations": ["holdout_row_not_object"]})
            continue
        holdout_id = str(row.get("holdout_id") or "")
        missing = _missing_required_fields(row, BLINDED_HOLDOUT_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("blinded") is not True:
            violations.append("holdout_must_be_blinded")
        try:
            if int(row.get("access_count")) != 0:
                violations.append("holdout_access_count_must_be_zero")
        except (TypeError, ValueError):
            violations.append("holdout_access_count_numeric_required")
        if row.get("no_retune_enforced") is not True:
            violations.append("no_retune_must_be_enforced")
        if str(row.get("contamination_status") or "") != "clean":
            violations.append("holdout_contamination_not_clean")
        if row.get("promotion_evidence_allowed") is not True:
            violations.append("clean_blinded_holdout_must_be_available_for_promotion_evidence")
        hash_violation = _release_row_hash_violation(row, "holdout_hash", "holdout_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_holdouts.append({"index": index, "holdout_id": holdout_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    override_rows = policy.get("manual_overrides") if isinstance(policy.get("manual_overrides"), list) else []
    malformed_overrides = []
    for index, row in enumerate(override_rows):
        if not isinstance(row, dict):
            malformed_overrides.append({"index": index, "override_id": None, "missing_fields": list(MANUAL_OVERRIDE_REQUIRED_FIELDS), "violations": ["manual_override_row_not_object"]})
            continue
        override_id = str(row.get("override_id") or "")
        missing = _missing_required_fields(row, MANUAL_OVERRIDE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("quarantine_required") is not True:
            violations.append("manual_override_must_quarantine")
        if row.get("promotion_evidence_allowed") is not False:
            violations.append("manual_override_cannot_be_promotion_evidence")
        if row.get("training_allowed") is not False:
            violations.append("manual_override_training_forbidden")
        if str(row.get("approval_status") or "") not in {"approved", "quarantined"}:
            violations.append("approval_status_invalid")
        if not str(row.get("audit_event_id") or "").strip():
            violations.append("audit_event_id_required")
        hash_violation = _release_row_hash_violation(row, "override_hash", "override_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_overrides.append({"index": index, "override_id": override_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    test_rows = policy.get("contract_test_suite") if isinstance(policy.get("contract_test_suite"), list) else []
    malformed_tests = []
    tested_contracts = set()
    for index, row in enumerate(test_rows):
        if not isinstance(row, dict):
            malformed_tests.append({"index": index, "suite_id": None, "missing_fields": list(CONTRACT_TEST_SUITE_REQUIRED_FIELDS), "violations": ["contract_test_row_not_object"]})
            continue
        suite_id = str(row.get("suite_id") or "")
        contract_id = str(row.get("contract_id") or "")
        tested_contracts.add(contract_id)
        missing = _missing_required_fields(row, CONTRACT_TEST_SUITE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("pass_fail") != "pass":
            violations.append("contract_test_not_pass")
        if str(row.get("coverage_class") or "") not in {"mvp_blocking", "normal_tiny_blocking", "non_negotiable_core"}:
            violations.append("coverage_class_invalid")
        if not str(row.get("test_command") or "").strip():
            violations.append("test_command_required")
        hash_violation = _release_row_hash_violation(row, "test_hash", "suite_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_tests.append({"index": index, "suite_id": suite_id or None, "missing_fields": missing, "violations": sorted(set(violations))})
    missing_contract_tests = sorted(RELEASE_EXPERIMENT_SAFETY_CONTRACTS - tested_contracts)

    replay_rows = policy.get("adversarial_replay_suite") if isinstance(policy.get("adversarial_replay_suite"), list) else []
    malformed_replays = []
    critical_replay_count = 0
    for index, row in enumerate(replay_rows):
        if not isinstance(row, dict):
            malformed_replays.append({"index": index, "replay_id": None, "missing_fields": list(ADVERSARIAL_REPLAY_SUITE_REQUIRED_FIELDS), "violations": ["adversarial_replay_row_not_object"]})
            continue
        replay_id = str(row.get("replay_id") or "")
        missing = _missing_required_fields(row, ADVERSARIAL_REPLAY_SUITE_REQUIRED_FIELDS)
        violations = _policy_threshold_violation(row, metric_records, threshold_records, "observed_value")
        if row.get("machine_checked") is not True:
            violations.append("adversarial_replay_must_be_machine_checked")
        if row.get("pass_fail") != "pass":
            violations.append("adversarial_replay_not_pass")
        if row.get("observed_action") != row.get("expected_action"):
            violations.append("observed_action_must_match_expected_action")
        if str(row.get("criticality") or "") == "critical":
            critical_replay_count += 1
        hash_violation = _release_row_hash_violation(row, "replay_hash", "replay_id")
        if hash_violation:
            violations.append(hash_violation)
        if missing or violations:
            malformed_replays.append({"index": index, "replay_id": replay_id or None, "missing_fields": missing, "violations": sorted(set(violations))})

    source_checks, source_errors = _validate_registry_source_files(policy.get("source_files"))
    common = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "failure_action": policy.get("failure_action"),
        "policy_errors": policy_errors,
        "source_checks": source_checks,
        "source_errors": source_errors,
    }
    return [
        _contract(
            "SecretsManagementContract",
            bool(secrets) and not policy_errors and not source_errors and not malformed_secrets,
            "secrets_management_missing_malformed_or_unsafe",
            {**common, "secret_count": len(secrets), "malformed_secrets": malformed_secrets},
        ),
        _contract(
            "SystemSLO",
            bool(slos) and not policy_errors and not source_errors and not malformed_slos,
            "system_slo_missing_malformed_or_unhealthy",
            {**common, "slo_count": len(slos), "malformed_slos": malformed_slos},
        ),
        _contract(
            "NoTradeRootCause",
            bool(no_trade_rows) and not policy_errors and not source_errors and not malformed_no_trade,
            "no_trade_root_cause_missing_malformed_or_unknown",
            {
                **common,
                "root_cause_count": len(no_trade_rows),
                "root_cause_codes": sorted(code for code in no_trade_codes if code),
                "malformed_no_trade_root_causes": malformed_no_trade,
            },
        ),
        _contract(
            "ReleaseComplexityBudget",
            bool(complexity_rows) and not policy_errors and not source_errors and not malformed_complexity,
            "release_complexity_budget_missing_malformed_or_exceeded",
            {**common, "release_complexity_count": len(complexity_rows), "malformed_release_complexity": malformed_complexity},
        ),
        _contract(
            "BackpressurePolicy",
            bool(backpressure_rows) and not policy_errors and not source_errors and not malformed_backpressure,
            "backpressure_policy_missing_malformed_or_unsafe",
            {**common, "backpressure_policy_count": len(backpressure_rows), "malformed_backpressure_policies": malformed_backpressure},
        ),
        _contract(
            "BudgetReserveContract",
            bool(reserve_rows) and not policy_errors and not source_errors and not missing_reserve_priorities and not malformed_reserves,
            "budget_reserve_missing_malformed_or_unprotected",
            {
                **common,
                "budget_reserve_count": len(reserve_rows),
                "missing_reserve_priorities": missing_reserve_priorities,
                "malformed_budget_reserves": malformed_reserves,
            },
        ),
        _contract(
            "BlindedHoldoutContract",
            bool(holdout_rows) and not policy_errors and not source_errors and not malformed_holdouts,
            "blinded_holdout_missing_malformed_or_contaminated",
            {**common, "holdout_count": len(holdout_rows), "malformed_holdouts": malformed_holdouts},
        ),
        _contract(
            "ManualOverrideContract",
            bool(override_rows) and not policy_errors and not source_errors and not malformed_overrides,
            "manual_override_missing_malformed_or_unquarantined",
            {**common, "manual_override_count": len(override_rows), "malformed_manual_overrides": malformed_overrides},
        ),
        _contract(
            "ContractTestSuite",
            bool(test_rows) and not policy_errors and not source_errors and not missing_contract_tests and not malformed_tests,
            "contract_test_suite_missing_malformed_or_failing",
            {
                **common,
                "contract_test_count": len(test_rows),
                "missing_contract_tests": missing_contract_tests,
                "malformed_contract_tests": malformed_tests,
            },
        ),
        _contract(
            "AdversarialReplaySuite",
            bool(replay_rows) and critical_replay_count >= 3 and not policy_errors and not source_errors and not malformed_replays,
            "adversarial_replay_suite_missing_malformed_or_failing",
            {
                **common,
                "adversarial_replay_count": len(replay_rows),
                "critical_replay_count": critical_replay_count,
                "malformed_adversarial_replays": malformed_replays,
            },
        ),
    ]


def _parse_iso_ts(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_governance_readiness(governance_path):
    try:
        payload = _load_json(governance_path)
    except Exception as exc:
        return None, {"governance_path": str(governance_path), "error": str(exc)}
    if not isinstance(payload, dict):
        return None, {"governance_path": str(governance_path), "error": "governance_readiness_not_object"}
    return payload, {"governance_path": str(governance_path), "schema_version": payload.get("schema_version"), "updated_at": payload.get("updated_at")}


def _resolve_source_file(source_file):
    path = Path(str(source_file or ""))
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _source_lines(source_file):
    source_path = _resolve_source_file(source_file)
    if not source_path.exists():
        return None, {"source_file": str(source_file), "error": "source_file_missing"}
    return source_path.read_text(encoding="utf-8").splitlines(), None


def _extract_dashboard_routes(lines):
    route_line_indexes = []
    endpoints_by_line = {}
    for index, line in enumerate(lines):
        endpoints = re.findall(r"url\.pathname\s*===\s*['\"]([^'\"]+)['\"]", line)
        if not endpoints:
            continue
        route_line_indexes.append(index)
        endpoints_by_line[index] = endpoints

    routes = []
    for route_index, line_index in enumerate(route_line_indexes):
        next_line_index = route_line_indexes[route_index + 1] if route_index + 1 < len(route_line_indexes) else len(lines)
        block = lines[line_index:next_line_index]
        check_auth_line = None
        post_guard_line = None
        audit_event_line = None
        mutation_markers = []
        for offset, text in enumerate(block):
            if check_auth_line is None and "checkAuth(req, url, res)" in text:
                check_auth_line = line_index + offset + 1
            if post_guard_line is None and ("req.method !== 'POST'" in text or "requirePost(req, res)" in text):
                post_guard_line = line_index + offset + 1
            if audit_event_line is None and "requireDashboardAuditEvent(req, res, url" in text:
                audit_event_line = line_index + offset + 1
            if (
                "triggerV27" in text
                or "cleanupOpenPaperPositions(" in text
                or ".run(" in text
                or "manualPause(" in text
                or "resumeTrading(" in text
                or "resetDailyLoss(" in text
            ):
                mutation_markers.append({"line": line_index + offset + 1, "text": text.strip()})
        for endpoint in endpoints_by_line.get(line_index, []):
            routes.append(
                {
                    "endpoint": endpoint,
                    "line": line_index + 1,
                    "has_check_auth": check_auth_line is not None,
                    "check_auth_line": check_auth_line,
                    "has_post_guard": post_guard_line is not None,
                    "post_guard_line": post_guard_line,
                    "has_audit_event": audit_event_line is not None,
                    "audit_event_line": audit_event_line,
                    "mutation_markers": mutation_markers[:5],
                }
            )
    return routes


def _dashboard_route_block(lines, endpoint):
    route_line_indexes = []
    endpoints_by_line = {}
    for index, line in enumerate(lines):
        endpoints = re.findall(r"url\.pathname\s*===\s*['\"]([^'\"]+)['\"]", line)
        if not endpoints:
            continue
        route_line_indexes.append(index)
        endpoints_by_line[index] = endpoints
    for route_index, line_index in enumerate(route_line_indexes):
        if endpoint not in endpoints_by_line.get(line_index, []):
            continue
        next_line_index = route_line_indexes[route_index + 1] if route_index + 1 < len(route_line_indexes) else len(lines)
        return "\n".join(lines[line_index:next_line_index])
    return ""


def _resolve_access_policy(endpoint, defaults, overrides):
    policy = {"endpoint": endpoint, **(defaults or {})}
    policy.update(overrides.get(endpoint) or {})
    policy["endpoint"] = endpoint
    return policy


def _write_registry_post_endpoints(write_path_registry_path):
    try:
        registry = _load_json(write_path_registry_path)
    except Exception:
        return set(), False
    endpoints = set()
    for item in registry.get("write_paths") or []:
        if not isinstance(item, dict):
            continue
        entry_point = str(item.get("entry_point") or "")
        if entry_point.startswith("POST "):
            endpoints.add(entry_point.removeprefix("POST ").strip())
    return endpoints, True


def verify_access_control_policy(policy_path=DEFAULT_ACCESS_CONTROL_POLICY, write_path_registry_path=DEFAULT_WRITE_PATH_REGISTRY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("AccessControlContract", False, "access_control_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("AccessControlContract", False, "access_control_policy_not_object", {"policy_path": str(policy_path)})

    lines, source_error = _source_lines(policy.get("source_file"))
    if source_error:
        return _contract("AccessControlContract", False, "access_control_source_missing", {"policy_path": str(policy_path), **source_error})

    source_text = "\n".join(lines)
    auth_boundary = {
        "dashboard_token_required": "if (!DASHBOARD_TOKEN)" in source_text and "writeHead(403" in source_text,
        "invalid_token_rejected": "token !== DASHBOARD_TOKEN" in source_text and "writeHead(401" in source_text,
        "token_sources": sorted(
            item
            for item, present in {
                "query_token": "url.searchParams.get('token')" in source_text,
                "x_dashboard_token_header": "x-dashboard-token" in source_text,
            }.items()
            if present
        ),
    }
    routes = _extract_dashboard_routes(lines)
    public_endpoints = set(policy.get("public_endpoints") or [])
    defaults = policy.get("protected_defaults") if isinstance(policy.get("protected_defaults"), dict) else {}
    overrides_list = policy.get("endpoint_overrides") if isinstance(policy.get("endpoint_overrides"), list) else []
    overrides = {}
    malformed_policies = []
    duplicate_policy_endpoints = []
    for index, item in enumerate(overrides_list):
        if not isinstance(item, dict):
            malformed_policies.append({"index": index, "endpoint": None, "missing_fields": list(ACCESS_CONTROL_REQUIRED_FIELDS), "violations": ["policy_not_object"]})
            continue
        endpoint = item.get("endpoint")
        if endpoint in overrides:
            duplicate_policy_endpoints.append(endpoint)
        overrides[endpoint] = item

    danger_requires_post = set(policy.get("danger_levels_requiring_post") or [])
    danger_requires_audit = set(policy.get("danger_levels_requiring_audit") or [])
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    literal_endpoints = set(route_by_endpoint)
    protected_routes = [route for route in routes if route["endpoint"] not in public_endpoints]
    unauthenticated_routes = [
        {"endpoint": route["endpoint"], "line": route["line"]}
        for route in protected_routes
        if not route["has_check_auth"]
    ]

    resolved_endpoint_policies = []
    missing_policy_fields = []
    mutation_without_post_guard = []
    mutation_without_audit_requirement = []
    mutation_like_routes_without_mutation_policy = []
    for route in protected_routes:
        endpoint = route["endpoint"]
        resolved = _resolve_access_policy(endpoint, defaults, overrides)
        missing = _missing_required_fields(resolved, ACCESS_CONTROL_REQUIRED_FIELDS)
        violations = []
        if not isinstance(resolved.get("audit_log_required"), bool):
            violations.append("audit_log_required_bool")
        danger = str(resolved.get("danger_level") or "")
        if danger in danger_requires_post and not route["has_post_guard"]:
            mutation_without_post_guard.append({"endpoint": endpoint, "line": route["line"], "danger_level": danger})
        if danger in danger_requires_audit and resolved.get("audit_log_required") is not True:
            mutation_without_audit_requirement.append({"endpoint": endpoint, "danger_level": danger})
        if route["mutation_markers"] and danger not in danger_requires_post:
            mutation_like_routes_without_mutation_policy.append(
                {
                    "endpoint": endpoint,
                    "danger_level": danger,
                    "markers": route["mutation_markers"],
                }
            )
        if missing or violations:
            missing_policy_fields.append({"endpoint": endpoint, "missing_fields": missing, "violations": violations})
        resolved_endpoint_policies.append(
            {
                "endpoint": endpoint,
                "required_role": resolved.get("required_role"),
                "token_scope": resolved.get("token_scope"),
                "audit_log_required": resolved.get("audit_log_required"),
                "danger_level": resolved.get("danger_level"),
            }
        )

    unknown_policy_endpoints = sorted(endpoint for endpoint in overrides if endpoint not in literal_endpoints)
    write_path_endpoints, write_registry_loaded = _write_registry_post_endpoints(write_path_registry_path)
    write_path_policy_gaps = []
    for endpoint in sorted(write_path_endpoints):
        resolved = _resolve_access_policy(endpoint, defaults, overrides)
        route = route_by_endpoint.get(endpoint)
        if (
            endpoint not in literal_endpoints
            or resolved.get("audit_log_required") is not True
            or str(resolved.get("danger_level") or "") not in danger_requires_post
            or not route
            or not route.get("has_post_guard")
            or not route.get("has_check_auth")
        ):
            write_path_policy_gaps.append(
                {
                    "endpoint": endpoint,
                    "registered_route": endpoint in literal_endpoints,
                    "audit_log_required": resolved.get("audit_log_required"),
                    "danger_level": resolved.get("danger_level"),
                    "has_post_guard": route.get("has_post_guard") if route else False,
                    "has_check_auth": route.get("has_check_auth") if route else False,
                }
            )

    dynamic_failures = []
    for index, item in enumerate(policy.get("dynamic_protected_routes") or []):
        if not isinstance(item, dict):
            dynamic_failures.append({"index": index, "endpoint": None, "error": "dynamic_policy_not_object"})
            continue
        missing = _missing_required_fields(item, ACCESS_CONTROL_REQUIRED_FIELDS + ("source_anchor",))
        anchor = str(item.get("source_anchor") or "")
        anchor_indexes = [idx for idx, text in enumerate(lines) if anchor and anchor in text]
        check_auth_near_anchor = any(
            "checkAuth(req, url, res)" in text
            for anchor_index in anchor_indexes
            for text in lines[anchor_index:min(anchor_index + 12, len(lines))]
        )
        if missing or not anchor_indexes or not check_auth_near_anchor:
            dynamic_failures.append(
                {
                    "index": index,
                    "endpoint": item.get("endpoint"),
                    "missing_fields": missing,
                    "anchor_found": bool(anchor_indexes),
                    "check_auth_near_anchor": check_auth_near_anchor,
                }
            )

    passed = (
        policy.get("schema_version") == "v2.7.0.access_control_policy.v1"
        and all(auth_boundary.values())
        and bool(routes)
        and not malformed_policies
        and not duplicate_policy_endpoints
        and not unauthenticated_routes
        and not missing_policy_fields
        and not mutation_without_post_guard
        and not mutation_without_audit_requirement
        and not mutation_like_routes_without_mutation_policy
        and not unknown_policy_endpoints
        and write_registry_loaded
        and not write_path_policy_gaps
        and not dynamic_failures
    )
    return _contract(
        "AccessControlContract",
        passed,
        "access_control_policy_missing_malformed_or_incomplete",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "source_file": policy.get("source_file"),
            "auth_boundary": auth_boundary,
            "literal_route_count": len(routes),
            "public_route_count": len([route for route in routes if route["endpoint"] in public_endpoints]),
            "protected_route_count": len(protected_routes),
            "resolved_policy_count": len(resolved_endpoint_policies),
            "mutation_policy_count": len(
                [item for item in resolved_endpoint_policies if str(item.get("danger_level") or "") in danger_requires_post]
            ),
            "write_path_endpoint_count": len(write_path_endpoints),
            "unauthenticated_routes": unauthenticated_routes,
            "missing_policy_fields": missing_policy_fields,
            "malformed_policies": malformed_policies,
            "duplicate_policy_endpoints": sorted(str(item) for item in duplicate_policy_endpoints),
            "unknown_policy_endpoints": unknown_policy_endpoints,
            "mutation_without_post_guard": mutation_without_post_guard,
            "mutation_without_audit_requirement": mutation_without_audit_requirement,
            "mutation_like_routes_without_mutation_policy": mutation_like_routes_without_mutation_policy[:20],
            "write_path_policy_gaps": write_path_policy_gaps,
            "dynamic_failures": dynamic_failures,
            "sample_resolved_policies": resolved_endpoint_policies[:20],
        },
    )


def verify_audit_log_integrity(policy_path=DEFAULT_ACCESS_CONTROL_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("AuditLogIntegrityContract", False, "audit_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("AuditLogIntegrityContract", False, "audit_policy_not_object", {"policy_path": str(policy_path)})

    lines, source_error = _source_lines(policy.get("source_file"))
    if source_error:
        return _contract("AuditLogIntegrityContract", False, "audit_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(lines)
    routes = _extract_dashboard_routes(lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    overrides = [
        item
        for item in (policy.get("endpoint_overrides") or [])
        if isinstance(item, dict)
    ]
    audit_required_endpoints = sorted(
        str(item.get("endpoint"))
        for item in overrides
        if item.get("audit_log_required") is True and item.get("endpoint")
    )
    missing_audit_hooks = []
    for endpoint in audit_required_endpoints:
        route = route_by_endpoint.get(endpoint)
        if not route or not route.get("has_audit_event"):
            missing_audit_hooks.append(
                {
                    "endpoint": endpoint,
                    "registered_route": bool(route),
                    "has_audit_event": bool(route and route.get("has_audit_event")),
                }
            )

    helper_required_fragments = {
        "schema_version": "DASHBOARD_AUDIT_SCHEMA_VERSION" in source_text and "v2.7.0.audit_log_integrity.v1" in source_text,
        "sha256_hashing": "createHash('sha256')" in source_text,
        "append_only_jsonl": "fs.appendFileSync(auditLogPath" in source_text,
        "chain_verifier": "verifyDashboardAuditChain" in source_text,
        "fail_closed_response": "Audit log unavailable" in source_text,
    }
    chain_field_presence = {
        field: field in source_text
        for field in AUDIT_LOG_REQUIRED_FIELDS
    }
    passed = (
        bool(audit_required_endpoints)
        and all(helper_required_fragments.values())
        and all(chain_field_presence.values())
        and not missing_audit_hooks
    )
    return _contract(
        "AuditLogIntegrityContract",
        passed,
        "audit_log_integrity_missing_malformed_or_incomplete",
        {
            "policy_path": str(policy_path),
            "source_file": policy.get("source_file"),
            "schema_version": "v2.7.0.audit_log_integrity.v1",
            "audit_required_endpoint_count": len(audit_required_endpoints),
            "audit_required_endpoints": audit_required_endpoints,
            "helper_required_fragments": helper_required_fragments,
            "chain_field_presence": chain_field_presence,
            "missing_audit_hooks": missing_audit_hooks,
        },
    )


def _scan_write_path_target(target):
    source_file = target.get("source_file") if isinstance(target, dict) else None
    source_path = _resolve_source_file(source_file)
    include_patterns = target.get("include_patterns") if isinstance(target, dict) else None
    exclude_patterns = target.get("exclude_patterns") if isinstance(target, dict) else None
    include_patterns = include_patterns if isinstance(include_patterns, list) else []
    exclude_patterns = exclude_patterns if isinstance(exclude_patterns, list) else []
    if not source_file or not include_patterns:
        return [], [{"source_file": source_file, "error": "scan_target_missing_source_file_or_include_patterns"}]
    if not source_path.exists():
        return [], [{"source_file": source_file, "error": "scan_target_source_file_missing"}]

    occurrences = []
    lines = source_path.read_text(encoding="utf-8").splitlines()
    for line_no, line in enumerate(lines, start=1):
        if not any(str(pattern) in line for pattern in include_patterns):
            continue
        if any(str(pattern) in line for pattern in exclude_patterns):
            continue
        occurrences.append(
            {
                "source_file": str(source_file),
                "line": line_no,
                "text": line.strip(),
            }
        )
    return occurrences, []


def _find_anchor_occurrences(source_file, source_anchor, scanned_occurrences):
    return [
        item
        for item in scanned_occurrences
        if item.get("source_file") == source_file and str(source_anchor or "") in item.get("text", "")
    ]


def verify_write_path_registry(registry_path=DEFAULT_WRITE_PATH_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("WritePathRegistryContract", False, "write_path_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("WritePathRegistryContract", False, "write_path_registry_not_object", {"registry_path": str(registry_path)})

    static_scan = registry.get("static_scan") if isinstance(registry.get("static_scan"), dict) else {}
    scan_targets = static_scan.get("targets") if isinstance(static_scan, dict) else []
    scan_targets = scan_targets if isinstance(scan_targets, list) else []
    write_paths = registry.get("write_paths") if isinstance(registry.get("write_paths"), list) else []

    scanned_occurrences = []
    scan_errors = []
    for target in scan_targets:
        occurrences, errors = _scan_write_path_target(target if isinstance(target, dict) else {})
        scanned_occurrences.extend(occurrences)
        scan_errors.extend(errors)

    malformed = []
    duplicate_write_path_ids = []
    duplicate_source_bindings = []
    seen_write_path_ids = set()
    seen_source_bindings = set()
    registered_anchors = {}
    for index, item in enumerate(write_paths):
        if not isinstance(item, dict):
            malformed.append({"index": index, "write_path_id": None, "missing_fields": list(WRITE_PATH_REQUIRED_FIELDS), "violations": ["write_path_not_object"]})
            continue
        write_path_id = item.get("write_path_id")
        missing = _missing_required_fields(item, WRITE_PATH_REQUIRED_FIELDS + WRITE_PATH_SOURCE_FIELDS)
        violations = []
        if write_path_id in seen_write_path_ids:
            duplicate_write_path_ids.append(write_path_id)
        if write_path_id:
            seen_write_path_ids.add(write_path_id)
        if not isinstance(item.get("requires_outbox"), bool):
            violations.append("requires_outbox_bool")
        if item.get("requires_outbox") is False and not item.get("outbox_reason"):
            violations.append("outbox_reason_required_when_requires_outbox_false")
        if str(item.get("mode_gate") or "") not in WRITE_PATH_ALLOWED_MODE_GATES:
            violations.append("mode_gate_invalid")
        try:
            source_anchor_occurrence = int(item.get("source_anchor_occurrence") or 1)
        except (TypeError, ValueError):
            source_anchor_occurrence = 0
        if source_anchor_occurrence <= 0:
            violations.append("source_anchor_occurrence_positive_int")
        source_file = str(item.get("source_file") or "")
        source_anchor = str(item.get("source_anchor") or "")
        source_binding = (source_file, source_anchor, source_anchor_occurrence)
        if source_binding in seen_source_bindings:
            duplicate_source_bindings.append(
                {
                    "source_file": source_file,
                    "source_anchor": source_anchor,
                    "source_anchor_occurrence": source_anchor_occurrence,
                    "write_path_id": write_path_id,
                }
            )
        seen_source_bindings.add(source_binding)
        if source_file and source_anchor:
            registered_anchors.setdefault(source_file, set()).add(source_anchor)
            anchor_occurrences = _find_anchor_occurrences(source_file, source_anchor, scanned_occurrences)
            if len(anchor_occurrences) < source_anchor_occurrence:
                violations.append("source_anchor_not_found")
        if missing or violations:
            malformed.append(
                {
                    "index": index,
                    "write_path_id": write_path_id,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    unregistered_occurrences = []
    for occurrence in scanned_occurrences:
        source_file = occurrence.get("source_file")
        anchors = registered_anchors.get(source_file, set())
        if not any(anchor in occurrence.get("text", "") for anchor in anchors):
            unregistered_occurrences.append(occurrence)

    passed = (
        registry.get("schema_version") == "v2.7.0.write_path_registry.v1"
        and bool(scan_targets)
        and bool(write_paths)
        and not scan_errors
        and not malformed
        and not duplicate_write_path_ids
        and not duplicate_source_bindings
        and not unregistered_occurrences
    )
    return _contract(
        "WritePathRegistryContract",
        passed,
        "write_path_registry_missing_malformed_or_incomplete",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "scope": registry.get("scope"),
            "scan_target_count": len(scan_targets),
            "scanned_mutation_count": len(scanned_occurrences),
            "registered_write_path_count": len(write_paths),
            "duplicate_write_path_ids": sorted(str(item) for item in duplicate_write_path_ids),
            "duplicate_source_bindings": duplicate_source_bindings,
            "scan_errors": scan_errors,
            "malformed_write_paths": malformed,
            "unregistered_mutation_count": len(unregistered_occurrences),
            "unregistered_mutations": unregistered_occurrences[:20],
            "registered_targets": sorted(
                {
                    str(item.get("target_store"))
                    for item in write_paths
                    if isinstance(item, dict) and item.get("target_store")
                }
            ),
        },
    )


def _entry_point_endpoint(entry_point):
    parts = str(entry_point or "").split()
    if len(parts) >= 2 and parts[0].upper() in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        return parts[0].upper(), parts[1]
    return None, None


def verify_direct_database_mutation_ban(
    policy_path=DEFAULT_DIRECT_DB_MUTATION_POLICY,
    registry_path=DEFAULT_WRITE_PATH_REGISTRY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        policy = _load_json(policy_path)
        registry = _load_json(registry_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("DirectDatabaseMutationBan", False, "direct_db_mutation_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict) or not isinstance(registry, dict) or not isinstance(access_policy, dict):
        return _contract(
            "DirectDatabaseMutationBan",
            False,
            "direct_db_mutation_policy_not_object",
            {
                "policy_path": str(policy_path),
                "registry_path": str(registry_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    write_paths = registry.get("write_paths") if isinstance(registry.get("write_paths"), list) else []
    direct_db_paths = [
        item for item in write_paths
        if isinstance(item, dict) and str(item.get("target_store") or "").startswith("sqlite:")
    ]
    direct_by_id = {str(item.get("write_path_id")): item for item in direct_db_paths if item.get("write_path_id")}
    approved_paths = policy.get("approved_mutation_paths") if isinstance(policy.get("approved_mutation_paths"), list) else []
    rules = policy.get("rules") if isinstance(policy.get("rules"), dict) else {}
    required_mode_gate = str(rules.get("required_registry_mode_gate") or "admin_break_glass")
    access_by_endpoint = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }

    malformed_policy_rows = []
    duplicate_policy_write_path_ids = []
    seen_policy_ids = set()
    approved_by_id = {}
    for index, item in enumerate(approved_paths):
        if not isinstance(item, dict):
            malformed_policy_rows.append({"index": index, "write_path_id": None, "missing_fields": list(DIRECT_DB_MUTATION_REQUIRED_FIELDS), "violations": ["policy_row_not_object"]})
            continue
        write_path_id = str(item.get("write_path_id") or "")
        missing = _missing_required_fields(item, DIRECT_DB_MUTATION_REQUIRED_FIELDS)
        violations = []
        if write_path_id in seen_policy_ids:
            duplicate_policy_write_path_ids.append(write_path_id)
        if write_path_id:
            seen_policy_ids.add(write_path_id)
            approved_by_id[write_path_id] = item
        method, endpoint = _entry_point_endpoint(item.get("approved_mutation_path"))
        if method != "POST" or not endpoint:
            violations.append("approved_mutation_path_must_be_post_endpoint")
        if item.get("break_glass_id") and not str(item.get("break_glass_id")).startswith("BG-DDB-"):
            violations.append("break_glass_id_prefix_invalid")
        registry_item = direct_by_id.get(write_path_id)
        if registry_item:
            if item.get("target_store") != registry_item.get("target_store"):
                violations.append("target_store_mismatch_registry")
            if item.get("approved_mutation_path") != registry_item.get("entry_point"):
                violations.append("approved_mutation_path_mismatch_registry")
        elif write_path_id:
            violations.append("approved_path_not_in_direct_db_registry")
        if missing or violations:
            malformed_policy_rows.append(
                {
                    "index": index,
                    "write_path_id": write_path_id or None,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    unapproved_direct_db_mutations = [
        {
            "write_path_id": item.get("write_path_id"),
            "target_store": item.get("target_store"),
            "entry_point": item.get("entry_point"),
        }
        for item in direct_db_paths
        if str(item.get("write_path_id")) not in approved_by_id
    ]
    registry_gate_violations = []
    access_control_violations = []
    outbox_rationale_violations = []
    for item in direct_db_paths:
        write_path_id = str(item.get("write_path_id") or "")
        if str(item.get("mode_gate") or "") != required_mode_gate:
            registry_gate_violations.append(
                {
                    "write_path_id": write_path_id,
                    "mode_gate": item.get("mode_gate"),
                    "required_mode_gate": required_mode_gate,
                }
            )
        if rules.get("require_outbox_rationale", True) and item.get("requires_outbox") is False and not item.get("outbox_reason"):
            outbox_rationale_violations.append({"write_path_id": write_path_id})
        method, endpoint = _entry_point_endpoint(item.get("entry_point"))
        endpoint_policy = access_by_endpoint.get(endpoint)
        if not endpoint_policy:
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "missing_access_policy"})
            continue
        if rules.get("require_post", True) and (
            method != "POST"
            or endpoint_policy.get("method_guard_required") is not True
            or "POST" not in [str(value).upper() for value in (endpoint_policy.get("allowed_methods") or [])]
        ):
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "post_guard_missing"})
        if rules.get("require_audit_log", True) and endpoint_policy.get("audit_log_required") is not True:
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "audit_requirement_missing"})

    passed = (
        policy.get("schema_version") == "v2.7.0.direct_database_mutation_ban.v1"
        and bool(direct_db_paths)
        and len(approved_paths) == len(direct_db_paths)
        and not malformed_policy_rows
        and not duplicate_policy_write_path_ids
        and not unapproved_direct_db_mutations
        and not registry_gate_violations
        and not access_control_violations
        and not outbox_rationale_violations
    )
    return _contract(
        "DirectDatabaseMutationBan",
        passed,
        "direct_db_mutation_ban_missing_malformed_or_bypassed",
        {
            "policy_path": str(policy_path),
            "registry_path": str(registry_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": policy.get("schema_version"),
            "default_action": rules.get("default_action"),
            "required_registry_mode_gate": required_mode_gate,
            "direct_db_write_path_count": len(direct_db_paths),
            "approved_mutation_path_count": len(approved_paths),
            "direct_db_targets": sorted({str(item.get("target_store")) for item in direct_db_paths if item.get("target_store")}),
            "malformed_policy_rows": malformed_policy_rows,
            "duplicate_policy_write_path_ids": sorted(str(item) for item in duplicate_policy_write_path_ids),
            "unapproved_direct_db_mutations": unapproved_direct_db_mutations,
            "registry_gate_violations": registry_gate_violations,
            "access_control_violations": access_control_violations,
            "outbox_rationale_violations": outbox_rationale_violations,
        },
    )


def verify_aggregate_boundary_contract(policy_path=DEFAULT_AGGREGATE_BOUNDARIES):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("AggregateBoundaryContract", False, "aggregate_boundary_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("AggregateBoundaryContract", False, "aggregate_boundary_policy_not_object", {"policy_path": str(policy_path)})

    source_file = policy.get("source_file") or "scripts/v27_event_log.py"
    source_text, source_error = _read_project_text(source_file)
    source_anchors = [str(anchor) for anchor in (policy.get("source_anchors") or [])]
    missing_source_anchors = sorted(anchor for anchor in source_anchors if anchor not in source_text)
    boundaries = policy.get("aggregate_boundaries") if isinstance(policy.get("aggregate_boundaries"), list) else []
    malformed_boundaries = []
    duplicate_aggregate_types = []
    seen_types = set()
    pattern_results = []
    for index, boundary in enumerate(boundaries):
        if not isinstance(boundary, dict):
            malformed_boundaries.append({"index": index, "aggregate_type": None, "missing_fields": list(AGGREGATE_BOUNDARY_REQUIRED_FIELDS), "violations": ["boundary_not_object"]})
            continue
        aggregate_type = str(boundary.get("aggregate_type") or "")
        missing = _missing_required_fields(boundary, AGGREGATE_BOUNDARY_REQUIRED_FIELDS)
        violations = []
        if aggregate_type in seen_types:
            duplicate_aggregate_types.append(aggregate_type)
        if aggregate_type:
            seen_types.add(aggregate_type)
        if str(boundary.get("sequence_scope") or "") not in AGGREGATE_SEQUENCE_SCOPES:
            violations.append("sequence_scope_invalid")
        if str(boundary.get("owner_store") or "") != "v27_event_log":
            violations.append("owner_store_must_be_v27_event_log")
        pattern = str(boundary.get("aggregate_id_pattern") or "")
        sample = str(boundary.get("sample_aggregate_id") or "")
        pattern_valid = False
        sample_matches = False
        try:
            compiled = re.compile(pattern)
            pattern_valid = True
            sample_matches = bool(sample and compiled.match(sample))
        except re.error as exc:
            violations.append(f"aggregate_id_pattern_invalid:{exc}")
        if not sample:
            violations.append("sample_aggregate_id_required")
        elif pattern_valid and not sample_matches:
            violations.append("sample_aggregate_id_does_not_match_pattern")
        pattern_results.append(
            {
                "aggregate_type": aggregate_type,
                "pattern_valid": pattern_valid,
                "sample_matches": sample_matches,
                "sample_hash": _sha256_json({"aggregate_type": aggregate_type, "sample_aggregate_id": sample}) if sample else None,
            }
        )
        if missing or violations:
            malformed_boundaries.append(
                {
                    "index": index,
                    "aggregate_type": aggregate_type or None,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    required_types = {
        "telegram_signal",
        "source_label",
        "paper_missed",
        "token_lifecycle",
        "runtime_recovery",
        "v27_contract_event",
    }
    missing_required_types = sorted(required_types - seen_types)
    passed = (
        policy.get("schema_version") == "v2.7.0.aggregate_boundaries.v1"
        and policy.get("failure_action") == "event_log_unhealthy"
        and bool(boundaries)
        and not source_error
        and not missing_source_anchors
        and not malformed_boundaries
        and not duplicate_aggregate_types
        and not missing_required_types
    )
    return _contract(
        "AggregateBoundaryContract",
        passed,
        "aggregate_boundary_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "source_file": source_file,
            "source_error": source_error,
            "boundary_count": len(boundaries),
            "aggregate_types": sorted(seen_types),
            "missing_required_types": missing_required_types,
            "duplicate_aggregate_types": sorted(str(item) for item in duplicate_aggregate_types),
            "malformed_boundaries": malformed_boundaries,
            "missing_source_anchors": missing_source_anchors,
            "pattern_results": pattern_results,
        },
    )


def _clock_sample():
    return {
        "wall_clock_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "wall_clock_ts": _utc_now_iso(),
    }


def verify_clock_rollback_guard_contract(clock_samples=None):
    samples = list(clock_samples) if clock_samples is not None else [_clock_sample(), _clock_sample()]
    malformed_samples = []
    rollback_detected = False
    for index, sample in enumerate(samples):
        missing = _missing_required_fields(
            {
                "clock_source": sample.get("clock_source", "system_time_and_monotonic"),
                "wall_clock_ts": sample.get("wall_clock_ts"),
                "monotonic_ts": sample.get("monotonic_ns"),
                "rollback_detected": False,
                "guard_action": sample.get("guard_action", "mark_time_dirty_and_block_promotion"),
            },
            CLOCK_ROLLBACK_REQUIRED_FIELDS,
        )
        violations = []
        if not isinstance(sample.get("wall_clock_ns"), int):
            violations.append("wall_clock_ns_required")
        if not isinstance(sample.get("monotonic_ns"), int):
            violations.append("monotonic_ns_required")
        if _parse_iso_ts(sample.get("wall_clock_ts")) is None:
            violations.append("wall_clock_ts_invalid")
        if missing or violations:
            malformed_samples.append({"index": index, "missing_fields": missing, "violations": violations})
        if index > 0:
            prev = samples[index - 1]
            if isinstance(sample.get("wall_clock_ns"), int) and isinstance(prev.get("wall_clock_ns"), int):
                rollback_detected = rollback_detected or sample["wall_clock_ns"] < prev["wall_clock_ns"]
            if isinstance(sample.get("monotonic_ns"), int) and isinstance(prev.get("monotonic_ns"), int):
                rollback_detected = rollback_detected or sample["monotonic_ns"] < prev["monotonic_ns"]

    latest = samples[-1] if samples else {}
    evidence_row = {
        "clock_source": "system_time_and_monotonic",
        "wall_clock_ts": latest.get("wall_clock_ts"),
        "monotonic_ts": latest.get("monotonic_ns"),
        "rollback_detected": rollback_detected,
        "guard_action": "mark_time_dirty_and_block_promotion",
    }
    passed = bool(samples) and not malformed_samples and not rollback_detected
    return _contract(
        "ClockRollbackGuardContract",
        passed,
        "clock_rollback_guard_unverified_or_dirty",
        {
            **evidence_row,
            "sample_count": len(samples),
            "malformed_samples": malformed_samples,
            "sample_hashes": [
                _sha256_json(
                    {
                        "wall_clock_ns": sample.get("wall_clock_ns"),
                        "monotonic_ns": sample.get("monotonic_ns"),
                        "wall_clock_ts": sample.get("wall_clock_ts"),
                    }
                )
                for sample in samples
            ],
        },
    )


def _load_event_schema_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "event_schema_policy_not_object"}
    return policy, {"policy_path": str(policy_path), "schema_version": policy.get("schema_version"), "scope": policy.get("scope")}


def _verify_source_anchors(source_anchors):
    missing = []
    source_errors = []
    for index, item in enumerate(source_anchors if isinstance(source_anchors, list) else []):
        if not isinstance(item, dict):
            missing.append({"index": index, "source_file": None, "anchor": None, "reason": "source_anchor_not_object"})
            continue
        source_file = item.get("source_file")
        anchor = str(item.get("anchor") or "")
        text, error = _read_project_text(source_file)
        if error:
            source_errors.append({"index": index, **error})
            continue
        if not anchor or anchor not in text:
            missing.append({"index": index, "source_file": source_file, "anchor": anchor, "reason": "anchor_missing"})
    return missing, source_errors


def verify_event_schema_compatibility_contract(policy_path=DEFAULT_EVENT_SCHEMA_COMPATIBILITY):
    policy, base_evidence = _load_event_schema_policy(policy_path)
    if policy is None:
        return _contract("EventSchemaCompatibilityContract", False, "event_schema_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    projection_text, projection_error = _read_project_text("scripts/v27_denominator_projection.py")
    if projection_error:
        source_errors.append({"source": "projection", **projection_error})
    allowed_versions = set(str(item) for item in (policy.get("allowed_event_schema_versions") or []))
    schemas = policy.get("event_schemas") if isinstance(policy.get("event_schemas"), list) else []
    malformed_schemas = []
    duplicate_event_types = []
    consumer_gaps = []
    seen_event_types = set()
    compatible_results = {"backward_compatible", "producer_consumer_match"}
    for index, item in enumerate(schemas):
        if not isinstance(item, dict):
            malformed_schemas.append({"index": index, "event_type": None, "missing_fields": list(EVENT_SCHEMA_COMPATIBILITY_REQUIRED_FIELDS), "violations": ["schema_not_object"]})
            continue
        event_type = str(item.get("event_type") or "")
        missing = _missing_required_fields(item, EVENT_SCHEMA_COMPATIBILITY_REQUIRED_FIELDS)
        violations = []
        if event_type in seen_event_types:
            duplicate_event_types.append(event_type)
        if event_type:
            seen_event_types.add(event_type)
        if event_type and not re.match(r"^[a-z][a-z0-9_]*$", event_type):
            violations.append("event_type_must_be_lower_snake_case")
        if str(item.get("schema_version") or "") not in allowed_versions:
            violations.append("schema_version_not_allowed")
        if str(item.get("compatibility_result") or "") not in compatible_results:
            violations.append("compatibility_result_not_compatible")
        if event_type and event_type not in projection_text:
            consumer_gaps.append({"event_type": event_type, "consumer_version": item.get("consumer_version")})
        if missing or violations:
            malformed_schemas.append({"index": index, "event_type": event_type or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.event_schema_compatibility.v1"
        and policy.get("failure_action") == "event_rejected"
        and bool(allowed_versions)
        and len(schemas) >= 10
        and not source_anchor_violations
        and not source_errors
        and not malformed_schemas
        and not duplicate_event_types
        and not consumer_gaps
    )
    return _contract(
        "EventSchemaCompatibilityContract",
        passed,
        "event_schema_compatibility_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "allowed_event_schema_versions": sorted(allowed_versions),
            "event_schema_count": len(schemas),
            "event_types": sorted(seen_event_types),
            "duplicate_event_types": sorted(str(item) for item in duplicate_event_types),
            "malformed_schemas": malformed_schemas,
            "consumer_gaps": consumer_gaps,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def verify_enum_evolution_contract(policy_path=DEFAULT_EVENT_SCHEMA_COMPATIBILITY):
    policy, base_evidence = _load_event_schema_policy(policy_path)
    if policy is None:
        return _contract("EnumEvolutionContract", False, "enum_evolution_policy_missing_or_invalid", base_evidence)

    rows = policy.get("enum_evolution") if isinstance(policy.get("enum_evolution"), list) else []
    malformed_rows = []
    duplicate_rows = []
    enum_names = set()
    seen = set()
    allowed_policies = {"append_only_no_rename", "backward_compatible_alias"}
    allowed_actions = {
        "catalog_scope_audit_required",
        "no_migration_required",
        "register_consumer_before_producer",
        "safe_default_fail_closed",
    }
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "enum_name": None, "missing_fields": list(ENUM_EVOLUTION_REQUIRED_FIELDS), "violations": ["enum_row_not_object"]})
            continue
        enum_name = str(item.get("enum_name") or "")
        key = (enum_name, str(item.get("old_value") or ""), str(item.get("new_value") or ""))
        missing = _missing_required_fields(item, ENUM_EVOLUTION_REQUIRED_FIELDS)
        violations = []
        if key in seen:
            duplicate_rows.append(":".join(key))
        seen.add(key)
        if enum_name:
            enum_names.add(enum_name)
        if str(item.get("compatibility_policy") or "") not in allowed_policies:
            violations.append("compatibility_policy_invalid")
        if str(item.get("migration_action") or "") not in allowed_actions:
            violations.append("migration_action_invalid")
        if missing or violations:
            malformed_rows.append({"index": index, "enum_name": enum_name or None, "missing_fields": missing, "violations": violations})

    required_enum_names = {"event_schema_version", "event_type", "mode_target", "entry_mode_tier"}
    missing_enum_names = sorted(required_enum_names - enum_names)
    passed = (
        policy.get("schema_version") == "v2.7.0.event_schema_compatibility.v1"
        and bool(rows)
        and not malformed_rows
        and not duplicate_rows
        and not missing_enum_names
    )
    return _contract(
        "EnumEvolutionContract",
        passed,
        "enum_evolution_missing_malformed_or_unsafe",
        {
            **base_evidence,
            "enum_evolution_count": len(rows),
            "enum_names": sorted(enum_names),
            "missing_enum_names": missing_enum_names,
            "duplicate_rows": sorted(duplicate_rows),
            "malformed_rows": malformed_rows,
        },
    )


def verify_mutation_command_idempotency_contract(policy_path=DEFAULT_EVENT_SCHEMA_COMPATIBILITY):
    policy, base_evidence = _load_event_schema_policy(policy_path)
    if policy is None:
        return _contract("MutationCommandIdempotencyContract", False, "mutation_idempotency_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    commands = policy.get("mutation_commands") if isinstance(policy.get("mutation_commands"), list) else []
    malformed_commands = []
    duplicate_command_ids = []
    seen_command_ids = set()
    command_evidence = []
    for index, item in enumerate(commands):
        if not isinstance(item, dict):
            malformed_commands.append({"index": index, "command_id": None, "missing_fields": list(MUTATION_COMMAND_IDEMPOTENCY_REQUIRED_FIELDS), "violations": ["command_not_object"]})
            continue
        command_id = str(item.get("command_id") or "")
        sample_payload = item.get("sample_payload") if isinstance(item.get("sample_payload"), dict) else {}
        dedupe_hash_material = [str(value) for value in (item.get("dedupe_hash_material") or [])]
        result_hash_material = [str(value) for value in (item.get("result_hash_material") or [])]
        dedupe_hash = _sha256_json({key: sample_payload.get(key) for key in dedupe_hash_material})
        result_hash = _sha256_json({"command_id": command_id, "idempotency_key": item.get("idempotency_key"), "mutation_target": item.get("mutation_target"), "dedupe_hash": dedupe_hash, "result_hash_material": result_hash_material})
        evidence_row = {
            "command_id": command_id,
            "idempotency_key": item.get("idempotency_key"),
            "mutation_target": item.get("mutation_target"),
            "dedupe_hash": dedupe_hash,
            "result_hash": result_hash,
        }
        command_evidence.append(evidence_row)
        missing = _missing_required_fields(evidence_row, MUTATION_COMMAND_IDEMPOTENCY_REQUIRED_FIELDS)
        violations = []
        if command_id in seen_command_ids:
            duplicate_command_ids.append(command_id)
        if command_id:
            seen_command_ids.add(command_id)
        if not dedupe_hash_material:
            violations.append("dedupe_hash_material_required")
        if not result_hash_material:
            violations.append("result_hash_material_required")
        if not isinstance(sample_payload, dict) or not sample_payload:
            violations.append("sample_payload_required")
        if missing or violations:
            malformed_commands.append({"index": index, "command_id": command_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.event_schema_compatibility.v1"
        and bool(commands)
        and not source_anchor_violations
        and not source_errors
        and not malformed_commands
        and not duplicate_command_ids
    )
    return _contract(
        "MutationCommandIdempotencyContract",
        passed,
        "mutation_command_idempotency_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "command_count": len(commands),
            "commands": command_evidence,
            "duplicate_command_ids": sorted(str(item) for item in duplicate_command_ids),
            "malformed_commands": malformed_commands,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def _load_read_model_snapshot_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "read_model_snapshot_policy_not_object"}
    return policy, {"policy_path": str(policy_path), "schema_version": policy.get("schema_version"), "scope": policy.get("scope")}


def verify_projection_version_isolation_contract(policy_path=DEFAULT_READ_MODEL_SNAPSHOT_POLICY):
    policy, base_evidence = _load_read_model_snapshot_policy(policy_path)
    if policy is None:
        return _contract("ProjectionVersionIsolationContract", False, "read_model_snapshot_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("projection_versions") if isinstance(policy.get("projection_versions"), list) else []
    malformed_rows = []
    duplicate_projection_keys = []
    projection_keys = set()
    required_isolation_fields = {"projection_name", "projection_version", "projection_hash", "spec.spec_hash"}
    allowed_consumer_actions = {"reject_mismatched_projection_hash"}
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "projection_name": None, "missing_fields": list(PROJECTION_VERSION_ISOLATION_REQUIRED_FIELDS), "violations": ["projection_version_row_not_object"]})
            continue
        projection_name = str(item.get("projection_name") or "")
        projection_version = str(item.get("projection_version") or "")
        key = f"{projection_name}:{projection_version}"
        missing = _missing_required_fields(item, PROJECTION_VERSION_ISOLATION_REQUIRED_FIELDS)
        violations = []
        if key in projection_keys:
            duplicate_projection_keys.append(key)
        if projection_name and projection_version:
            projection_keys.add(key)
        isolation_fields = set(str(field) for field in (item.get("isolation_key_fields") or []))
        missing_isolation_fields = sorted(required_isolation_fields - isolation_fields)
        if missing_isolation_fields:
            violations.append("isolation_key_fields_incomplete:" + ",".join(missing_isolation_fields))
        if item.get("snapshot_field") != "projection_version":
            violations.append("snapshot_field_must_be_projection_version")
        if item.get("consumer_action") not in allowed_consumer_actions:
            violations.append("consumer_action_invalid")
        if not projection_name.startswith("v27_"):
            violations.append("projection_name_must_be_v27_scoped")
        if not projection_version.startswith("v"):
            violations.append("projection_version_must_be_versioned")
        if missing or violations:
            malformed_rows.append({"index": index, "projection_name": projection_name or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.read_model_snapshot_policy.v1"
        and policy.get("failure_action") == "dashboard_snapshot_rejected"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_projection_keys
    )
    return _contract(
        "ProjectionVersionIsolationContract",
        passed,
        "projection_version_isolation_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "projection_version_count": len(rows),
            "projection_keys": sorted(projection_keys),
            "required_isolation_fields": sorted(required_isolation_fields),
            "duplicate_projection_keys": sorted(duplicate_projection_keys),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def verify_snapshot_compaction_invariant_contract(policy_path=DEFAULT_READ_MODEL_SNAPSHOT_POLICY):
    policy, base_evidence = _load_read_model_snapshot_policy(policy_path)
    if policy is None:
        return _contract("SnapshotCompactionInvariantContract", False, "read_model_snapshot_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("snapshot_compaction_invariants") if isinstance(policy.get("snapshot_compaction_invariants"), list) else []
    malformed_rows = []
    duplicate_invariant_ids = []
    invariant_ids = set()
    hash_fields = set()
    allowed_hash_sources = {
        "projection_payload_without_event_log_dir",
        "snapshot_payload_without_snapshot_hash",
    }
    allowed_failure_actions = {"projection_hash_mismatch", "snapshot_hash_mismatch"}
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "invariant_id": None, "missing_fields": list(SNAPSHOT_COMPACTION_INVARIANT_REQUIRED_FIELDS), "violations": ["compaction_invariant_not_object"]})
            continue
        invariant_id = str(item.get("invariant_id") or "")
        missing = _missing_required_fields(item, SNAPSHOT_COMPACTION_INVARIANT_REQUIRED_FIELDS)
        violations = []
        if invariant_id in invariant_ids:
            duplicate_invariant_ids.append(invariant_id)
        if invariant_id:
            invariant_ids.add(invariant_id)
        hash_field = str(item.get("hash_field") or "")
        if hash_field:
            hash_fields.add(hash_field)
        excludes_fields = set(str(field) for field in (item.get("excludes_fields") or []))
        if hash_field == "projection_hash" and "event_log_dir" not in excludes_fields:
            violations.append("projection_compaction_must_exclude_event_log_dir")
        if hash_field == "snapshot_hash" and "snapshot_hash" not in excludes_fields:
            violations.append("snapshot_compaction_must_exclude_snapshot_hash")
        if item.get("hash_source") not in allowed_hash_sources:
            violations.append("hash_source_invalid")
        if item.get("failure_action") not in allowed_failure_actions:
            violations.append("failure_action_invalid")
        if missing or violations:
            malformed_rows.append({"index": index, "invariant_id": invariant_id or None, "missing_fields": missing, "violations": violations})

    missing_hash_fields = sorted({"projection_hash", "snapshot_hash"} - hash_fields)
    passed = (
        policy.get("schema_version") == "v2.7.0.read_model_snapshot_policy.v1"
        and policy.get("failure_action") == "dashboard_snapshot_rejected"
        and len(rows) >= 2
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_invariant_ids
        and not missing_hash_fields
    )
    return _contract(
        "SnapshotCompactionInvariantContract",
        passed,
        "snapshot_compaction_invariant_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "invariant_count": len(rows),
            "invariant_ids": sorted(invariant_ids),
            "hash_fields": sorted(hash_fields),
            "missing_hash_fields": missing_hash_fields,
            "duplicate_invariant_ids": sorted(duplicate_invariant_ids),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def verify_snapshot_compaction_read_barrier_contract(policy_path=DEFAULT_READ_MODEL_SNAPSHOT_POLICY):
    policy, base_evidence = _load_read_model_snapshot_policy(policy_path)
    if policy is None:
        return _contract("SnapshotCompactionReadBarrier", False, "read_model_snapshot_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("read_barriers") if isinstance(policy.get("read_barriers"), list) else []
    malformed_rows = []
    duplicate_barrier_ids = []
    barrier_ids = set()
    required_checks = {
        "snapshot_schema_ok",
        "snapshot_hash_ok",
        "projection_hash_ok",
        "spec_valid",
        "read_model_fresh_enough",
        "snapshot_age_ok",
        "projection_built",
        "event_log_ok",
    }
    required_unsafe_statuses = {"event_log_invalid", "not_built", "seed_empty"}
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "barrier_id": None, "missing_fields": list(SNAPSHOT_READ_BARRIER_REQUIRED_FIELDS), "violations": ["read_barrier_not_object"]})
            continue
        barrier_id = str(item.get("barrier_id") or "")
        missing = _missing_required_fields(item, SNAPSHOT_READ_BARRIER_REQUIRED_FIELDS)
        violations = []
        if barrier_id in barrier_ids:
            duplicate_barrier_ids.append(barrier_id)
        if barrier_id:
            barrier_ids.add(barrier_id)
        checks = set(str(check) for check in (item.get("required_checks") or []))
        unsafe_statuses = set(str(status) for status in (item.get("unsafe_statuses") or []))
        missing_checks = sorted(required_checks - checks)
        missing_unsafe_statuses = sorted(required_unsafe_statuses - unsafe_statuses)
        if missing_checks:
            violations.append("required_checks_incomplete:" + ",".join(missing_checks))
        if missing_unsafe_statuses:
            violations.append("unsafe_statuses_incomplete:" + ",".join(missing_unsafe_statuses))
        if item.get("failure_action") != "dashboard_snapshot_rejected":
            violations.append("failure_action_must_reject_dashboard_snapshot")
        if item.get("consumer") != "dashboard_and_mode_readiness":
            violations.append("consumer_must_bind_dashboard_and_mode_readiness")
        if missing or violations:
            malformed_rows.append({"index": index, "barrier_id": barrier_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.read_model_snapshot_policy.v1"
        and policy.get("failure_action") == "dashboard_snapshot_rejected"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_barrier_ids
    )
    return _contract(
        "SnapshotCompactionReadBarrier",
        passed,
        "snapshot_compaction_read_barrier_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "barrier_count": len(rows),
            "barrier_ids": sorted(barrier_ids),
            "required_checks": sorted(required_checks),
            "required_unsafe_statuses": sorted(required_unsafe_statuses),
            "duplicate_barrier_ids": sorted(duplicate_barrier_ids),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def _load_runtime_worker_health_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "runtime_worker_health_policy_not_object"}
    return policy, {"policy_path": str(policy_path), "schema_version": policy.get("schema_version"), "scope": policy.get("scope")}


def verify_worker_heartbeat_contract(policy_path=DEFAULT_RUNTIME_WORKER_HEALTH_POLICY):
    policy, base_evidence = _load_runtime_worker_health_policy(policy_path)
    if policy is None:
        return _contract("WorkerHeartbeatContract", False, "runtime_worker_health_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("worker_heartbeats") if isinstance(policy.get("worker_heartbeats"), list) else []
    malformed_rows = []
    required_payload_fields = {"worker_id", "role", "build_hash", "runtime_config_hash", "policy_bundle_id", "heartbeat_at"}
    required_roles = {"dashboard", "paper-trader", "lifecycle-tracker", "v27-read-model-refresh"}
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "event_type": None, "missing_fields": list(WORKER_HEARTBEAT_REQUIRED_FIELDS), "violations": ["heartbeat_row_not_object"]})
            continue
        missing = _missing_required_fields(item, WORKER_HEARTBEAT_REQUIRED_FIELDS)
        violations = []
        payload_fields = set(str(field) for field in (item.get("required_payload_fields") or []))
        roles = set(str(role) for role in (item.get("required_roles") or []))
        missing_payload_fields = sorted(required_payload_fields - payload_fields)
        missing_roles = sorted(required_roles - roles)
        if item.get("event_type") != "worker_fleet_heartbeat_recorded":
            violations.append("event_type_must_be_worker_fleet_heartbeat_recorded")
        if item.get("projection_health_key") != "worker_fleet_consistency_ok":
            violations.append("projection_health_key_invalid")
        if missing_payload_fields:
            violations.append("required_payload_fields_incomplete:" + ",".join(missing_payload_fields))
        if missing_roles:
            violations.append("required_roles_incomplete:" + ",".join(missing_roles))
        try:
            if int(item.get("max_heartbeat_lag_ms")) <= 0:
                violations.append("max_heartbeat_lag_ms_must_be_positive")
        except (TypeError, ValueError):
            violations.append("max_heartbeat_lag_ms_must_be_positive")
        if item.get("failure_action") != "block_promotion_until_fresh_heartbeat":
            violations.append("failure_action_invalid")
        if missing or violations:
            malformed_rows.append({"index": index, "event_type": item.get("event_type"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_worker_health_policy.v1"
        and policy.get("failure_action") == "worker_runtime_not_ready"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
    )
    return _contract(
        "WorkerHeartbeatContract",
        passed,
        "worker_heartbeat_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "heartbeat_policy_count": len(rows),
            "required_roles": sorted(required_roles),
            "required_payload_fields": sorted(required_payload_fields),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def verify_silent_worker_death_detector_contract(
    policy_path=DEFAULT_RUNTIME_WORKER_HEALTH_POLICY,
    background_job_registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY,
):
    policy, base_evidence = _load_runtime_worker_health_policy(policy_path)
    if policy is None:
        return _contract("SilentWorkerDeathDetector", False, "runtime_worker_health_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    try:
        registry = _load_json(background_job_registry_path)
    except Exception as exc:
        registry = {}
        source_errors.append({"source_file": str(background_job_registry_path), "reason": "registry_missing_or_invalid", "error": str(exc)})
    jobs = {job.get("job_name"): job for job in registry.get("jobs", []) if isinstance(job, dict)}
    rows = policy.get("silent_death_detectors") if isinstance(policy.get("silent_death_detectors"), list) else []
    malformed_rows = []
    duplicate_jobs = []
    seen_jobs = set()
    run_script_text, run_script_error = _read_project_text("scripts/run_zeabur_services.sh")
    if run_script_error:
        source_errors.append(run_script_error)
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "job_name": None, "missing_fields": list(SILENT_WORKER_DEATH_REQUIRED_FIELDS), "violations": ["silent_death_detector_not_object"]})
            continue
        job_name = str(item.get("job_name") or "")
        missing = _missing_required_fields(item, SILENT_WORKER_DEATH_REQUIRED_FIELDS)
        violations = []
        if job_name in seen_jobs:
            duplicate_jobs.append(job_name)
        if job_name:
            seen_jobs.add(job_name)
        job = jobs.get(job_name)
        if not job:
            violations.append("job_not_in_background_registry")
        else:
            lease_policy = job.get("lease_policy") if isinstance(job.get("lease_policy"), dict) else {}
            if item.get("pid_env") != lease_policy.get("pid_env"):
                violations.append("pid_env_must_match_background_registry")
            if lease_policy.get("kind") != "supervised_restart_loop":
                violations.append("job_must_use_supervised_restart_loop")
        detection_anchor = str(item.get("detection_anchor") or "")
        if detection_anchor and detection_anchor not in run_script_text:
            violations.append("detection_anchor_missing_from_run_script")
        if item.get("restart_action") != "supervised_restart_loop":
            violations.append("restart_action_invalid")
        if missing or violations:
            malformed_rows.append({"index": index, "job_name": job_name or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_worker_health_policy.v1"
        and policy.get("failure_action") == "worker_runtime_not_ready"
        and len(rows) >= 5
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_jobs
    )
    return _contract(
        "SilentWorkerDeathDetector",
        passed,
        "silent_worker_death_detector_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "registry_path": str(background_job_registry_path),
            "failure_action": policy.get("failure_action"),
            "detector_count": len(rows),
            "detected_jobs": sorted(seen_jobs),
            "duplicate_jobs": sorted(duplicate_jobs),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def verify_warm_start_safety_contract(policy_path=DEFAULT_RUNTIME_WORKER_HEALTH_POLICY):
    policy, base_evidence = _load_runtime_worker_health_policy(policy_path)
    if policy is None:
        return _contract("WarmStartSafetyContract", False, "runtime_worker_health_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("warm_start_controls") if isinstance(policy.get("warm_start_controls"), list) else []
    malformed_rows = []
    duplicate_control_ids = []
    control_ids = set()
    allowed_failure_actions = {"quarantine_bad_volume_before_start", "run_preflight_before_restart"}
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "control_id": None, "missing_fields": list(WARM_START_CONTROL_REQUIRED_FIELDS), "violations": ["warm_start_control_not_object"]})
            continue
        control_id = str(item.get("control_id") or "")
        missing = _missing_required_fields(item, WARM_START_CONTROL_REQUIRED_FIELDS)
        violations = []
        if control_id in control_ids:
            duplicate_control_ids.append(control_id)
        if control_id:
            control_ids.add(control_id)
        protected_paths = [str(path) for path in (item.get("protected_paths") or [])]
        if not protected_paths:
            violations.append("protected_paths_required")
        elif not all(path.startswith("/app/data/") for path in protected_paths):
            violations.append("protected_paths_must_be_app_data")
        text, error = _read_project_text(item.get("source_file"))
        if error:
            source_errors.append({"index": index, **error})
        elif str(item.get("source_anchor") or "") not in text:
            violations.append("source_anchor_missing")
        if item.get("failure_action") not in allowed_failure_actions:
            violations.append("failure_action_invalid")
        if missing or violations:
            malformed_rows.append({"index": index, "control_id": control_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_worker_health_policy.v1"
        and policy.get("failure_action") == "worker_runtime_not_ready"
        and len(rows) >= 2
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_control_ids
    )
    return _contract(
        "WarmStartSafetyContract",
        passed,
        "warm_start_safety_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "control_count": len(rows),
            "control_ids": sorted(control_ids),
            "duplicate_control_ids": sorted(duplicate_control_ids),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_errors": source_errors,
        },
    )


def _load_db_runtime_concurrency_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "db_runtime_concurrency_policy_not_object"}
    return policy, {"policy_path": str(policy_path), "schema_version": policy.get("schema_version"), "scope": policy.get("scope")}


def _verify_row_source_anchor(item, index, *, anchor_field="source_anchor"):
    text, error = _read_project_text(item.get("source_file"))
    if error:
        return {"index": index, **error}
    anchor = str(item.get(anchor_field) or "")
    if not anchor or anchor not in text:
        return {
            "index": index,
            "source_file": item.get("source_file"),
            "reason": f"{anchor_field}_missing",
            anchor_field: anchor,
        }
    return None


def verify_connection_pool_partition_contract(policy_path=DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY):
    policy, base_evidence = _load_db_runtime_concurrency_policy(policy_path)
    if policy is None:
        return _contract("ConnectionPoolPartitionContract", False, "db_runtime_concurrency_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("connection_pools") if isinstance(policy.get("connection_pools"), list) else []
    malformed_rows = []
    duplicate_pool_names = []
    source_violations = []
    pool_names = set()
    required_pools = {"paper_sqlite_writer_pool", "market_data_distributed_singleflight"}

    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "pool_name": None, "missing_fields": list(CONNECTION_POOL_PARTITION_REQUIRED_FIELDS), "violations": ["connection_pool_not_object"]})
            continue
        pool_name = str(item.get("pool_name") or "")
        missing = _missing_required_fields(item, CONNECTION_POOL_PARTITION_REQUIRED_FIELDS + ("source_file", "source_anchor"))
        violations = []
        if pool_name in pool_names:
            duplicate_pool_names.append(pool_name)
        if pool_name:
            pool_names.add(pool_name)
        try:
            max_connections = int(item.get("max_connections"))
            if max_connections <= 0:
                violations.append("max_connections_must_be_positive")
        except (TypeError, ValueError):
            max_connections = None
            violations.append("max_connections_must_be_positive")
        try:
            critical_reserved = int(item.get("critical_reserved_connections"))
            if critical_reserved <= 0:
                violations.append("critical_reserved_connections_must_be_positive")
            if max_connections is not None and critical_reserved > max_connections:
                violations.append("critical_reserved_connections_cannot_exceed_max")
        except (TypeError, ValueError):
            violations.append("critical_reserved_connections_must_be_positive")
        if _parse_iso_ts(item.get("checked_at")) is None:
            violations.append("checked_at_invalid")
        if ":" not in str(item.get("partition_key") or ""):
            violations.append("partition_key_must_be_namespaced")
        source_violation = _verify_row_source_anchor(item, index)
        if source_violation:
            source_violations.append({"pool_name": pool_name or None, **source_violation})
        if missing or violations:
            malformed_rows.append({"index": index, "pool_name": pool_name or None, "missing_fields": missing, "violations": violations})

    missing_required_pools = sorted(required_pools - pool_names)
    passed = (
        policy.get("schema_version") == "v2.7.0.db_runtime_concurrency_policy.v1"
        and policy.get("failure_action") == "storage_or_lock_backend_degraded"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_pool_names
        and not source_violations
        and not missing_required_pools
    )
    return _contract(
        "ConnectionPoolPartitionContract",
        passed,
        "connection_pool_partition_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "pool_count": len(rows),
            "pool_names": sorted(pool_names),
            "required_pools": sorted(required_pools),
            "missing_required_pools": missing_required_pools,
            "duplicate_pool_names": sorted(str(item) for item in duplicate_pool_names),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_violations": source_violations,
            "source_errors": source_errors,
        },
    )


def verify_db_lock_contention_policy(policy_path=DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY):
    policy, base_evidence = _load_db_runtime_concurrency_policy(policy_path)
    if policy is None:
        return _contract("DBLockContentionPolicy", False, "db_runtime_concurrency_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("db_lock_contention_policies") if isinstance(policy.get("db_lock_contention_policies"), list) else []
    malformed_rows = []
    duplicate_locks = []
    source_violations = []
    lock_keys = set()
    stores = set()
    allowed_fallbacks = {
        "rollback_and_retry_then_raise",
        "database_locked_backoff_and_skip_due_update",
        "warn_integrity_marker_or_quarantine_paper_db",
    }
    required_stores = {"sqlite:paper_trades", "sqlite:missed_attribution", "sqlite:volume_preflight"}

    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "lock_name": None, "missing_fields": list(DB_LOCK_CONTENTION_REQUIRED_FIELDS), "violations": ["lock_contention_policy_not_object"]})
            continue
        store = str(item.get("store") or "")
        lock_name = str(item.get("lock_name") or "")
        key = (store, lock_name)
        missing = _missing_required_fields(item, DB_LOCK_CONTENTION_REQUIRED_FIELDS + ("source_file", "source_anchor"))
        violations = []
        if key in lock_keys:
            duplicate_locks.append(":".join(key))
        if store and lock_name:
            lock_keys.add(key)
            stores.add(store)
        try:
            if int(item.get("contention_threshold_ms")) <= 0:
                violations.append("contention_threshold_ms_must_be_positive")
        except (TypeError, ValueError):
            violations.append("contention_threshold_ms_must_be_positive")
        if not isinstance(item.get("retry_policy"), dict) or not item.get("retry_policy"):
            violations.append("retry_policy_non_empty_object_required")
        if item.get("fallback_action") not in allowed_fallbacks:
            violations.append("fallback_action_invalid")
        source_violation = _verify_row_source_anchor(item, index)
        if source_violation:
            source_violations.append({"store": store or None, "lock_name": lock_name or None, **source_violation})
        if missing or violations:
            malformed_rows.append({"index": index, "store": store or None, "lock_name": lock_name or None, "missing_fields": missing, "violations": violations})

    missing_required_stores = sorted(required_stores - stores)
    passed = (
        policy.get("schema_version") == "v2.7.0.db_runtime_concurrency_policy.v1"
        and policy.get("failure_action") == "storage_or_lock_backend_degraded"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_locks
        and not source_violations
        and not missing_required_stores
    )
    return _contract(
        "DBLockContentionPolicy",
        passed,
        "db_lock_contention_policy_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "policy_count": len(rows),
            "stores": sorted(stores),
            "required_stores": sorted(required_stores),
            "missing_required_stores": missing_required_stores,
            "duplicate_locks": sorted(str(item) for item in duplicate_locks),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_violations": source_violations,
            "source_errors": source_errors,
        },
    )


def verify_database_transaction_isolation_contract(policy_path=DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY):
    policy, base_evidence = _load_db_runtime_concurrency_policy(policy_path)
    if policy is None:
        return _contract("DatabaseTransactionIsolationContract", False, "db_runtime_concurrency_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("transaction_isolation_contracts") if isinstance(policy.get("transaction_isolation_contracts"), list) else []
    malformed_rows = []
    duplicate_transaction_ids = []
    source_violations = []
    transaction_ids = set()
    stores = set()
    allowed_isolation_levels = {
        "single_writer_file_lock_plus_wal",
        "single_writer_file_lock_plus_commit",
        "better_sqlite3_transaction",
    }
    required_stores = {"sqlite:paper_trades", "sqlite:paper_decision_audit", "sqlite:kline_cache"}

    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "transaction_id": None, "missing_fields": list(DATABASE_TRANSACTION_ISOLATION_REQUIRED_FIELDS), "violations": ["transaction_isolation_row_not_object"]})
            continue
        transaction_id = str(item.get("transaction_id") or "")
        store = str(item.get("store") or "")
        missing = _missing_required_fields(item, DATABASE_TRANSACTION_ISOLATION_REQUIRED_FIELDS + ("source_file", "source_anchor"))
        violations = []
        if transaction_id in transaction_ids:
            duplicate_transaction_ids.append(transaction_id)
        if transaction_id:
            transaction_ids.add(transaction_id)
        if store:
            stores.add(store)
        if item.get("isolation_level") not in allowed_isolation_levels:
            violations.append("isolation_level_invalid")
        if not isinstance(item.get("invariant_scope"), list) or not item.get("invariant_scope"):
            violations.append("invariant_scope_non_empty_list_required")
        if not str(item.get("deadlock_retry_policy") or "").strip():
            violations.append("deadlock_retry_policy_required")
        source_violation = _verify_row_source_anchor(item, index)
        if source_violation:
            source_violations.append({"transaction_id": transaction_id or None, **source_violation})
        if missing or violations:
            malformed_rows.append({"index": index, "transaction_id": transaction_id or None, "store": store or None, "missing_fields": missing, "violations": violations})

    missing_required_stores = sorted(required_stores - stores)
    passed = (
        policy.get("schema_version") == "v2.7.0.db_runtime_concurrency_policy.v1"
        and policy.get("failure_action") == "storage_or_lock_backend_degraded"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_transaction_ids
        and not source_violations
        and not missing_required_stores
    )
    return _contract(
        "DatabaseTransactionIsolationContract",
        passed,
        "database_transaction_isolation_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "failure_action": policy.get("failure_action"),
            "transaction_count": len(rows),
            "transaction_ids": sorted(transaction_ids),
            "stores": sorted(stores),
            "required_stores": sorted(required_stores),
            "missing_required_stores": missing_required_stores,
            "duplicate_transaction_ids": sorted(str(item) for item in duplicate_transaction_ids),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_violations": source_violations,
            "source_errors": source_errors,
        },
    )


def verify_distributed_lock_backend_health_contract(policy_path=DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY):
    policy, base_evidence = _load_db_runtime_concurrency_policy(policy_path)
    if policy is None:
        return _contract("DistributedLockBackendHealthContract", False, "db_runtime_concurrency_policy_missing_or_invalid", base_evidence)

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    rows = policy.get("distributed_lock_backends") if isinstance(policy.get("distributed_lock_backends"), list) else []
    malformed_rows = []
    duplicate_backend_names = []
    source_violations = []
    backend_names = set()
    allowed_health = {"ready", "ready_or_fail_open_to_local_producer"}
    required_backends = {"redis_market_data_singleflight", "sqlite_file_lock_single_writer"}

    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            malformed_rows.append({"index": index, "backend_name": None, "missing_fields": list(DISTRIBUTED_LOCK_BACKEND_HEALTH_REQUIRED_FIELDS), "violations": ["distributed_lock_backend_not_object"]})
            continue
        backend_name = str(item.get("backend_name") or "")
        missing = _missing_required_fields(item, DISTRIBUTED_LOCK_BACKEND_HEALTH_REQUIRED_FIELDS + ("source_file", "acquire_anchor", "release_anchor", "fallback_anchor"))
        violations = []
        if backend_name in backend_names:
            duplicate_backend_names.append(backend_name)
        if backend_name:
            backend_names.add(backend_name)
        if item.get("health_status") not in allowed_health:
            violations.append("health_status_invalid")
        if item.get("stale_read_detected") is not False:
            violations.append("stale_read_detected_must_be_false")
        if item.get("split_brain_detected") is not False:
            violations.append("split_brain_detected_must_be_false")
        text, error = _read_project_text(item.get("source_file"))
        if error:
            source_violations.append({"index": index, "backend_name": backend_name or None, **error})
        else:
            for anchor_field in ("acquire_anchor", "release_anchor", "fallback_anchor"):
                anchor = str(item.get(anchor_field) or "")
                if not anchor or anchor not in text:
                    source_violations.append(
                        {
                            "index": index,
                            "backend_name": backend_name or None,
                            "source_file": item.get("source_file"),
                            "reason": f"{anchor_field}_missing",
                            anchor_field: anchor,
                        }
                    )
        if missing or violations:
            malformed_rows.append({"index": index, "backend_name": backend_name or None, "missing_fields": missing, "violations": violations})

    missing_required_backends = sorted(required_backends - backend_names)
    passed = (
        policy.get("schema_version") == "v2.7.0.db_runtime_concurrency_policy.v1"
        and policy.get("failure_action") == "storage_or_lock_backend_degraded"
        and bool(rows)
        and not source_anchor_violations
        and not source_errors
        and not malformed_rows
        and not duplicate_backend_names
        and not source_violations
        and not missing_required_backends
    )
    return _contract(
        "DistributedLockBackendHealthContract",
        passed,
        "distributed_lock_backend_health_missing_malformed_or_unenforced",
        {
            **base_evidence,
            "contract_failure_action": "lock_backend_unhealthy",
            "policy_failure_action": policy.get("failure_action"),
            "backend_count": len(rows),
            "backend_names": sorted(backend_names),
            "required_backends": sorted(required_backends),
            "missing_required_backends": missing_required_backends,
            "duplicate_backend_names": sorted(str(item) for item in duplicate_backend_names),
            "malformed_rows": malformed_rows,
            "source_anchor_violations": source_anchor_violations,
            "source_violations": source_violations,
            "source_errors": source_errors,
        },
    )


def _resolve_project_file(raw_path):
    if not raw_path:
        return None
    path = Path(str(raw_path))
    return path if path.is_absolute() else PROJECT_ROOT / path


def verify_background_job_registry(registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("BackgroundJobRegistryContract", False, "background_job_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("BackgroundJobRegistryContract", False, "background_job_registry_not_object", {"registry_path": str(registry_path)})

    jobs = registry.get("jobs") if isinstance(registry.get("jobs"), list) else []
    malformed_jobs = []
    duplicate_job_names = []
    missing_entry_point_files = []
    missing_source_anchors = []
    seen_job_names = set()
    restart_loop_jobs = 0
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            malformed_jobs.append({"index": index, "job_name": None, "missing_fields": list(BACKGROUND_JOB_REQUIRED_FIELDS), "violations": ["job_not_object"]})
            continue
        job_name = str(job.get("job_name") or "")
        missing = _missing_required_fields(job, BACKGROUND_JOB_REQUIRED_FIELDS)
        violations = []
        if job_name in seen_job_names:
            duplicate_job_names.append(job_name)
        if job_name:
            seen_job_names.add(job_name)
        allowed_modes = job.get("allowed_modes")
        if not isinstance(allowed_modes, list) or not allowed_modes:
            violations.append("allowed_modes_non_empty_list_required")
        else:
            invalid_modes = sorted(str(mode) for mode in allowed_modes if str(mode) not in BACKGROUND_JOB_ALLOWED_MODES)
            if invalid_modes:
                violations.append(f"allowed_modes_invalid:{','.join(invalid_modes)}")
        lease_policy = job.get("lease_policy")
        if not isinstance(lease_policy, dict) or not lease_policy.get("kind"):
            violations.append("lease_policy_kind_required")
        elif str(lease_policy.get("kind")) == "supervised_restart_loop":
            restart_loop_jobs += 1
            if not lease_policy.get("pid_env"):
                violations.append("supervised_restart_loop_pid_env_required")
            try:
                if int(lease_policy.get("restart_delay_sec", 0)) <= 0:
                    violations.append("supervised_restart_loop_restart_delay_positive")
            except (TypeError, ValueError):
                violations.append("supervised_restart_loop_restart_delay_positive")
        entry_point_file = _resolve_project_file(job.get("entry_point_file"))
        if entry_point_file and not entry_point_file.exists():
            missing_entry_point_files.append({"job_name": job_name, "entry_point_file": job.get("entry_point_file")})
        source_file = _resolve_project_file(job.get("source_file"))
        source_anchor = str(job.get("source_anchor") or "")
        if not source_file or not source_anchor:
            violations.append("source_file_and_anchor_required")
        elif not source_file.exists():
            missing_source_anchors.append({"job_name": job_name, "source_file": job.get("source_file"), "source_anchor": source_anchor, "reason": "source_file_missing"})
        else:
            source_text = source_file.read_text(encoding="utf-8")
            if source_anchor not in source_text:
                missing_source_anchors.append({"job_name": job_name, "source_file": job.get("source_file"), "source_anchor": source_anchor, "reason": "source_anchor_missing"})
        if missing or violations:
            malformed_jobs.append({"index": index, "job_name": job_name or None, "missing_fields": missing, "violations": violations})

    passed = (
        registry.get("schema_version") == "v2.7.0.background_job_registry.v1"
        and bool(jobs)
        and restart_loop_jobs >= 5
        and not malformed_jobs
        and not duplicate_job_names
        and not missing_entry_point_files
        and not missing_source_anchors
    )
    return _contract(
        "BackgroundJobRegistryContract",
        passed,
        "background_job_registry_missing_malformed_or_incomplete",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "scope": registry.get("scope"),
            "job_count": len(jobs),
            "restart_loop_job_count": restart_loop_jobs,
            "job_names": sorted(str(job.get("job_name")) for job in jobs if isinstance(job, dict) and job.get("job_name")),
            "duplicate_job_names": sorted(str(item) for item in duplicate_job_names),
            "malformed_jobs": malformed_jobs,
            "missing_entry_point_files": missing_entry_point_files,
            "missing_source_anchors": missing_source_anchors,
        },
    )


def verify_scheduled_job_mode_gate_contract(registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("ScheduledJobModeGateContract", False, "scheduled_job_mode_gate_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("ScheduledJobModeGateContract", False, "scheduled_job_mode_gate_registry_not_object", {"registry_path": str(registry_path)})

    checked_at = registry.get("updated_at")
    jobs = registry.get("jobs") if isinstance(registry.get("jobs"), list) else []
    gate_rows = []
    malformed_rows = []
    invalid_checked_at = _parse_iso_ts(checked_at) is None
    expected_modes = sorted(BACKGROUND_JOB_ALLOWED_MODES)
    for job_index, job in enumerate(jobs):
        if not isinstance(job, dict):
            malformed_rows.append({"index": job_index, "job_name": None, "violations": ["job_not_object"]})
            continue
        job_name = str(job.get("job_name") or "")
        allowed_modes = [str(mode) for mode in (job.get("allowed_modes") or [])]
        allowed_mode_set = set(allowed_modes)
        invalid_modes = sorted(mode for mode in allowed_mode_set if mode not in BACKGROUND_JOB_ALLOWED_MODES)
        for mode in expected_modes:
            allowed_to_run = mode in allowed_mode_set
            row = {
                "job_name": job_name,
                "mode": mode,
                "allowed_to_run": allowed_to_run,
                "gate_reason": "mode_allowed_by_background_job_registry" if allowed_to_run else "mode_not_listed_for_job",
                "checked_at": checked_at,
            }
            missing = _missing_required_fields(row, SCHEDULED_JOB_MODE_GATE_REQUIRED_FIELDS)
            violations = []
            if invalid_checked_at:
                violations.append("checked_at_invalid")
            if invalid_modes:
                violations.append(f"invalid_allowed_modes:{','.join(invalid_modes)}")
            if not isinstance(row["allowed_to_run"], bool):
                violations.append("allowed_to_run_must_be_bool")
            gate_rows.append(row)
            if missing or violations:
                malformed_rows.append(
                    {
                        "index": len(gate_rows) - 1,
                        "job_name": job_name or None,
                        "mode": mode,
                        "missing_fields": missing,
                        "violations": violations,
                    }
                )

    denied_rows = [row for row in gate_rows if row.get("allowed_to_run") is False]
    passed = (
        registry.get("schema_version") == "v2.7.0.background_job_registry.v1"
        and bool(jobs)
        and len(gate_rows) == len(jobs) * len(BACKGROUND_JOB_ALLOWED_MODES)
        and not malformed_rows
    )
    return _contract(
        "ScheduledJobModeGateContract",
        passed,
        "scheduled_job_mode_gate_missing_malformed_or_incomplete",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "checked_at": checked_at,
            "job_count": len(jobs),
            "mode_count": len(BACKGROUND_JOB_ALLOWED_MODES),
            "gate_row_count": len(gate_rows),
            "denied_row_count": len(denied_rows),
            "denied_rows": denied_rows,
            "malformed_rows": malformed_rows,
            "sample_gate_rows": gate_rows[:20],
        },
    )


def _extract_env_flag_names(source_text):
    return set(re.findall(r"envFlag\(\s*['\"]([^'\"]+)['\"]", source_text))


def _catalog_contract_ids(catalog_path):
    catalog = _load_json(catalog_path)
    contracts = catalog.get("contracts") if isinstance(catalog, dict) else {}
    return set(str(contract_id) for contract_id in contracts.keys()) if isinstance(contracts, dict) else set()


def verify_feature_flag_dependency_contract(
    policy_path=DEFAULT_FEATURE_FLAG_DEPENDENCIES,
    catalog_path=CATALOG_PATH,
):
    try:
        policy = _load_json(policy_path)
        catalog_contracts = _catalog_contract_ids(catalog_path)
    except Exception as exc:
        return _contract("FeatureFlagDependencyContract", False, "feature_flag_dependency_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("FeatureFlagDependencyContract", False, "feature_flag_dependency_policy_not_object", {"policy_path": str(policy_path)})

    source_file = policy.get("source_file") or "src/index.js"
    source_text, source_error = _read_project_text(source_file)
    source_flags = _extract_env_flag_names(source_text) if not source_error else set()
    dependencies = policy.get("feature_flag_dependencies") if isinstance(policy.get("feature_flag_dependencies"), list) else []
    malformed_dependencies = []
    duplicate_feature_flags = []
    source_anchor_violations = []
    unknown_dependencies = []
    seen_flags = set()
    policy_flags = set()

    for index, item in enumerate(dependencies):
        if not isinstance(item, dict):
            malformed_dependencies.append({"index": index, "feature_flag": None, "missing_fields": list(FEATURE_FLAG_DEPENDENCY_REQUIRED_FIELDS), "violations": ["dependency_not_object"]})
            continue
        feature_flag = str(item.get("feature_flag") or "")
        policy_flags.add(feature_flag)
        missing = _missing_required_fields(item, FEATURE_FLAG_DEPENDENCY_REQUIRED_FIELDS)
        violations = []
        if feature_flag in seen_flags:
            duplicate_feature_flags.append(feature_flag)
        if feature_flag:
            seen_flags.add(feature_flag)
        depends_on = [str(value) for value in (item.get("depends_on") or [])]
        if not depends_on:
            violations.append("depends_on_non_empty_list_required")
        for dependency in depends_on:
            if dependency not in catalog_contracts:
                unknown_dependencies.append({"feature_flag": feature_flag, "dependency": dependency})
        mode_scope = [str(value) for value in (item.get("mode_scope") or [])]
        invalid_modes = sorted(mode for mode in mode_scope if mode not in BACKGROUND_JOB_ALLOWED_MODES)
        if not mode_scope:
            violations.append("mode_scope_non_empty_list_required")
        if invalid_modes:
            violations.append(f"mode_scope_invalid:{','.join(invalid_modes)}")
        if str(item.get("dependency_state") or "") not in FEATURE_FLAG_DEPENDENCY_STATES:
            violations.append("dependency_state_invalid")
        if str(item.get("activation_action") or "") not in FEATURE_FLAG_ACTIVATION_ACTIONS:
            violations.append("activation_action_invalid")
        if "default_enabled" in item and not isinstance(item.get("default_enabled"), bool):
            violations.append("default_enabled_must_be_bool")
        source_anchor = str(item.get("source_anchor") or "")
        if source_anchor and source_anchor not in source_text:
            source_anchor_violations.append({"feature_flag": feature_flag, "source_anchor": source_anchor})
        if missing or violations:
            malformed_dependencies.append(
                {
                    "index": index,
                    "feature_flag": feature_flag or None,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    uncovered_source_flags = sorted(source_flags - policy_flags)
    unknown_policy_flags = sorted(policy_flags - source_flags)
    passed = (
        policy.get("schema_version") == "v2.7.0.feature_flag_dependencies.v1"
        and policy.get("failure_action") == "feature_flag_blocked"
        and bool(dependencies)
        and not source_error
        and not malformed_dependencies
        and not duplicate_feature_flags
        and not source_anchor_violations
        and not unknown_dependencies
        and not uncovered_source_flags
        and not unknown_policy_flags
    )
    return _contract(
        "FeatureFlagDependencyContract",
        passed,
        "feature_flag_dependency_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "catalog_path": str(catalog_path),
            "source_file": source_file,
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "feature_flag_count": len(dependencies),
            "source_feature_flag_count": len(source_flags),
            "uncovered_source_flags": uncovered_source_flags,
            "unknown_policy_flags": unknown_policy_flags,
            "duplicate_feature_flags": sorted(str(item) for item in duplicate_feature_flags),
            "unknown_dependencies": unknown_dependencies,
            "malformed_dependencies": malformed_dependencies,
            "source_anchor_violations": source_anchor_violations,
            "source_error": source_error,
        },
    )


def _file_size_or_zero(path):
    try:
        path = _resolve_project_file(path)
        if path.exists() and path.is_file():
            return path.stat().st_size
    except OSError:
        return None
    return 0


def _stat_free_bytes(path):
    try:
        resolved = _resolve_project_file(path)
        target = resolved if resolved.exists() else resolved.parent
        stats = os.statvfs(target)
        return int(stats.f_bavail) * int(stats.f_frsize), None
    except OSError as exc:
        return None, str(exc)


def verify_filesystem_disk_pressure_policy(policy_path=DEFAULT_FILESYSTEM_PRESSURE_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("FilesystemDiskPressurePolicy", False, "filesystem_pressure_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("FilesystemDiskPressurePolicy", False, "filesystem_pressure_policy_not_object", {"policy_path": str(policy_path)})

    source_file = policy.get("source_file") or "src/web/dashboard-server.js"
    source_text, source_error = _read_project_text(source_file)
    source_anchors = [str(anchor) for anchor in (policy.get("source_anchors") or [])]
    missing_source_anchors = sorted(anchor for anchor in source_anchors if anchor not in source_text)
    filesystems = policy.get("filesystems") if isinstance(policy.get("filesystems"), list) else []
    malformed_filesystems = []
    pressure_violations = []
    measurements = []

    for index, item in enumerate(filesystems):
        if not isinstance(item, dict):
            malformed_filesystems.append({"index": index, "filesystem_path": None, "missing_fields": list(FILESYSTEM_PRESSURE_POLICY_REQUIRED_FIELDS), "violations": ["filesystem_not_object"]})
            continue
        filesystem_path = str(item.get("filesystem_path") or "")
        missing = _missing_required_fields(item, FILESYSTEM_PRESSURE_POLICY_REQUIRED_FIELDS)
        violations = []
        if str(item.get("pressure_action") or "") not in {"warn_and_block_promotion_if_below_floor", "checkpoint_wal_and_warn", "fail_closed"}:
            violations.append("pressure_action_invalid")
        try:
            min_free_bytes = int(item.get("min_free_bytes"))
            max_wal_bytes = int(item.get("max_wal_bytes"))
        except (TypeError, ValueError):
            min_free_bytes = None
            max_wal_bytes = None
            violations.append("thresholds_must_be_int")
        wal_files = item.get("wal_files")
        if not isinstance(wal_files, list) or not wal_files:
            violations.append("wal_files_non_empty_list_required")
            wal_files = []
        free_bytes, stat_error = _stat_free_bytes(filesystem_path)
        wal_file_sizes = []
        wal_bytes = 0
        for raw_path in wal_files:
            size = _file_size_or_zero(raw_path)
            if size is None:
                violations.append(f"wal_file_unreadable:{raw_path}")
                continue
            wal_bytes += size
            wal_file_sizes.append({"path": str(raw_path), "bytes": size})
        measurement = {
            "filesystem_path": filesystem_path,
            "free_bytes": free_bytes,
            "wal_bytes": wal_bytes,
            "pressure_action": item.get("pressure_action"),
            "min_free_bytes": min_free_bytes,
            "max_wal_bytes": max_wal_bytes,
            "wal_file_sizes": wal_file_sizes,
            "stat_error": stat_error,
        }
        measurements.append(measurement)
        if stat_error:
            violations.append("filesystem_stat_failed")
        if free_bytes is not None and min_free_bytes is not None and free_bytes < min_free_bytes:
            pressure_violations.append({"filesystem_path": filesystem_path, "reason": "free_bytes_below_floor", "free_bytes": free_bytes, "min_free_bytes": min_free_bytes})
        if max_wal_bytes is not None and wal_bytes > max_wal_bytes:
            pressure_violations.append({"filesystem_path": filesystem_path, "reason": "wal_bytes_above_ceiling", "wal_bytes": wal_bytes, "max_wal_bytes": max_wal_bytes})
        if missing or violations:
            malformed_filesystems.append({"index": index, "filesystem_path": filesystem_path or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.filesystem_pressure_policy.v1"
        and policy.get("failure_action") == "storage_degraded"
        and bool(filesystems)
        and not source_error
        and not missing_source_anchors
        and not malformed_filesystems
        and not pressure_violations
    )
    return _contract(
        "FilesystemDiskPressurePolicy",
        passed,
        "filesystem_pressure_missing_malformed_or_degraded",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "source_file": source_file,
            "source_error": source_error,
            "missing_source_anchors": missing_source_anchors,
            "filesystem_count": len(filesystems),
            "measurements": measurements,
            "malformed_filesystems": malformed_filesystems,
            "pressure_violations": pressure_violations,
            "required_fields": list(FILESYSTEM_PRESSURE_REQUIRED_FIELDS),
        },
    )


def _verify_code_location(location, label):
    violations = []
    if not isinstance(location, dict):
        return [{"location": label, "reason": "location_not_object"}]
    raw_file = location.get("file")
    anchor = location.get("anchor")
    if not raw_file:
        violations.append({"location": label, "reason": "file_missing"})
        return violations
    path = _resolve_project_file(raw_file)
    if not path.exists():
        violations.append({"location": label, "file": str(raw_file), "reason": "file_not_found"})
        return violations
    if anchor:
        text = path.read_text(encoding="utf-8")
        if str(anchor) not in text:
            violations.append({"location": label, "file": str(raw_file), "anchor": str(anchor), "reason": "anchor_not_found"})
    return violations


def verify_entry_point_inventory(
    inventory_path=DEFAULT_ENTRY_POINT_INVENTORY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        inventory = _load_json(inventory_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("EntryPointInventoryContract", False, "entry_point_inventory_missing_or_invalid", {"error": str(exc)})
    if not isinstance(inventory, dict) or not isinstance(access_policy, dict):
        return _contract(
            "EntryPointInventoryContract",
            False,
            "entry_point_inventory_not_object",
            {
                "inventory_path": str(inventory_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    source_lines, source_error = _source_lines(access_policy.get("source_file"))
    if source_error:
        return _contract("EntryPointInventoryContract", False, "entry_point_source_missing", {"inventory_path": str(inventory_path), **source_error})
    routes = _extract_dashboard_routes(source_lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    public_endpoints = set(str(item) for item in (access_policy.get("public_endpoints") or []))
    protected_route_count = sum(1 for route in routes if route.get("endpoint") not in public_endpoints)
    overrides = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }
    audit_required_endpoints = {
        endpoint
        for endpoint, item in overrides.items()
        if item.get("audit_log_required") is True
    }
    dynamic_source_anchors = {
        str(item.get("source_anchor"))
        for item in (access_policy.get("dynamic_protected_routes") or [])
        if isinstance(item, dict) and item.get("source_anchor")
    }

    entries = inventory.get("entry_points") if isinstance(inventory.get("entry_points"), list) else []
    malformed_entries = []
    duplicate_entry_point_ids = []
    location_violations = []
    route_group_violations = []
    dynamic_route_violations = []
    seen_ids = set()
    covered_route_endpoints = set()
    route_registry_required_count = 0
    arbiter_required_count = 0
    entry_type_counts = {}

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            malformed_entries.append({"index": index, "entry_point_id": None, "missing_fields": list(ENTRY_POINT_REQUIRED_FIELDS), "violations": ["entry_not_object"]})
            continue
        entry_id = str(entry.get("entry_point_id") or "")
        entry_type = str(entry.get("entry_type") or "")
        missing = _missing_required_fields(entry, ENTRY_POINT_REQUIRED_FIELDS)
        violations = []
        if entry_id in seen_ids:
            duplicate_entry_point_ids.append(entry_id)
        if entry_id:
            seen_ids.add(entry_id)
        if entry_type not in ENTRY_POINT_ALLOWED_TYPES:
            violations.append("entry_type_invalid_or_missing")
        else:
            entry_type_counts[entry_type] = entry_type_counts.get(entry_type, 0) + 1
        for bool_field in ("route_registry_required", "arbiter_required"):
            if not isinstance(entry.get(bool_field), bool):
                violations.append(f"{bool_field}_must_be_bool")
        if entry.get("route_registry_required") is True:
            route_registry_required_count += 1
            if not entry.get("route_registry_reason"):
                violations.append("route_registry_reason_required")
        if entry.get("arbiter_required") is True:
            arbiter_required_count += 1
            if not entry.get("arbiter_reason"):
                violations.append("arbiter_reason_required")

        location_violations.extend(
            {"entry_point_id": entry_id, **violation}
            for violation in _verify_code_location(entry.get("code_location"), "code_location")
        )
        for optional_location in ("launcher_location", "target_location"):
            if optional_location in entry:
                location_violations.extend(
                    {"entry_point_id": entry_id, **violation}
                    for violation in _verify_code_location(entry.get(optional_location), optional_location)
                )

        route_group = entry.get("route_group") if isinstance(entry.get("route_group"), dict) else None
        if route_group:
            endpoints = [str(endpoint) for endpoint in (route_group.get("endpoints") or [])]
            covered_route_endpoints.update(endpoints)
            for endpoint in endpoints:
                if endpoint not in route_by_endpoint:
                    route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "route_not_found"})
            expected_literal = route_group.get("expected_literal_route_count")
            if expected_literal is not None and int(expected_literal) != len(routes):
                route_group_violations.append(
                    {
                        "entry_point_id": entry_id,
                        "expected_literal_route_count": expected_literal,
                        "actual_literal_route_count": len(routes),
                        "reason": "literal_route_count_mismatch",
                    }
                )
            expected_protected = route_group.get("expected_protected_route_count")
            if expected_protected is not None and int(expected_protected) != protected_route_count:
                route_group_violations.append(
                    {
                        "entry_point_id": entry_id,
                        "expected_protected_route_count": expected_protected,
                        "actual_protected_route_count": protected_route_count,
                        "reason": "protected_route_count_mismatch",
                    }
                )
            if route_group.get("require_access_control") or route_group.get("require_post") or route_group.get("require_audit"):
                for endpoint in endpoints:
                    policy = overrides.get(endpoint)
                    if not policy:
                        route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "access_policy_override_missing"})
                        continue
                    if route_group.get("require_post"):
                        allowed = [str(value).upper() for value in (policy.get("allowed_methods") or [])]
                        if policy.get("method_guard_required") is not True or "POST" not in allowed:
                            route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "post_guard_missing"})
                    if route_group.get("require_audit") and policy.get("audit_log_required") is not True:
                        route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "audit_requirement_missing"})

        dynamic_group = entry.get("dynamic_route_group") if isinstance(entry.get("dynamic_route_group"), dict) else None
        if dynamic_group:
            source_anchor = str(dynamic_group.get("source_anchor") or "")
            if dynamic_group.get("require_access_control") and source_anchor not in dynamic_source_anchors:
                dynamic_route_violations.append({"entry_point_id": entry_id, "source_anchor": source_anchor, "reason": "dynamic_access_policy_missing"})

        if missing or violations:
            malformed_entries.append({"index": index, "entry_point_id": entry_id or None, "missing_fields": missing, "violations": violations})

    uncovered_audit_required_routes = sorted(audit_required_endpoints - covered_route_endpoints)
    passed = (
        inventory.get("schema_version") == "v2.7.0.entry_point_inventory.v1"
        and bool(entries)
        and len(routes) >= 60
        and protected_route_count >= 50
        and route_registry_required_count >= 2
        and arbiter_required_count >= 20
        and not malformed_entries
        and not duplicate_entry_point_ids
        and not location_violations
        and not route_group_violations
        and not dynamic_route_violations
        and not uncovered_audit_required_routes
    )
    return _contract(
        "EntryPointInventoryContract",
        passed,
        "entry_point_inventory_missing_malformed_or_incomplete",
        {
            "inventory_path": str(inventory_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": inventory.get("schema_version"),
            "entry_point_count": len(entries),
            "entry_type_counts": entry_type_counts,
            "dashboard_literal_route_count": len(routes),
            "dashboard_protected_route_count": protected_route_count,
            "route_registry_required_count": route_registry_required_count,
            "arbiter_required_count": arbiter_required_count,
            "duplicate_entry_point_ids": sorted(str(item) for item in duplicate_entry_point_ids),
            "malformed_entries": malformed_entries,
            "location_violations": location_violations,
            "route_group_violations": route_group_violations,
            "dynamic_route_violations": dynamic_route_violations,
            "audit_required_route_count": len(audit_required_endpoints),
            "uncovered_audit_required_routes": uncovered_audit_required_routes,
        },
    )


def _static_policy_scan_files(scan_target):
    raw_targets = scan_target if isinstance(scan_target, list) else [scan_target]
    files = []
    for raw_target in raw_targets:
        if not raw_target:
            continue
        raw_text = str(raw_target)
        if any(char in raw_text for char in "*?[]") and not Path(raw_text).is_absolute():
            files.extend(sorted(path for path in PROJECT_ROOT.glob(raw_text) if path.is_file()))
            continue
        files.append(_resolve_project_file(raw_text))
    unique = []
    seen = set()
    for path in files:
        if not path:
            continue
        resolved = str(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _static_policy_line(text, offset):
    line_no = text.count("\n", 0, offset) + 1
    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    if line_end == -1:
        line_end = len(text)
    return line_no, text[line_start:line_end].strip()[:180]


def verify_static_policy_enforcement(policy_path=DEFAULT_STATIC_POLICY_ENFORCEMENT):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("StaticPolicyEnforcementContract", False, "static_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("StaticPolicyEnforcementContract", False, "static_policy_not_object", {"policy_path": str(policy_path)})

    checks = policy.get("checks") if isinstance(policy.get("checks"), list) else []
    malformed_checks = []
    duplicate_static_check_ids = []
    scan_errors = []
    forbidden_matches = []
    seen_ids = set()
    scan_target_files = set()

    for index, check in enumerate(checks):
        if not isinstance(check, dict):
            malformed_checks.append({"index": index, "static_check_id": None, "missing_fields": list(STATIC_POLICY_REQUIRED_FIELDS), "violations": ["check_not_object"]})
            continue
        check_id = str(check.get("static_check_id") or "")
        missing = _missing_required_fields(check, STATIC_POLICY_REQUIRED_FIELDS)
        violations = []
        if check_id in seen_ids:
            duplicate_static_check_ids.append(check_id)
        if check_id:
            seen_ids.add(check_id)
        if check.get("result") != "pass":
            violations.append("result_must_be_pass")
        try:
            pattern = re.compile(str(check.get("forbidden_pattern") or ""))
        except re.error as exc:
            pattern = None
            violations.append("forbidden_pattern_invalid")
            scan_errors.append({"static_check_id": check_id, "reason": "forbidden_pattern_invalid", "error": str(exc)})

        target_files = _static_policy_scan_files(check.get("scan_target"))
        if not target_files:
            scan_errors.append({"static_check_id": check_id, "scan_target": check.get("scan_target"), "reason": "scan_target_empty"})
        for path in target_files:
            scan_target_files.add(str(path))
            if not path.exists():
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_missing"})
                continue
            if not path.is_file():
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_not_file"})
                continue
            if pattern is None:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_decode_failed", "error": str(exc)})
                continue
            for match in pattern.finditer(text):
                line_no, line_text = _static_policy_line(text, match.start())
                forbidden_matches.append(
                    {
                        "static_check_id": check_id,
                        "file": str(path),
                        "line": line_no,
                        "match": line_text,
                    }
                )

        if missing or violations:
            malformed_checks.append({"index": index, "static_check_id": check_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.static_policy_enforcement.v1"
        and bool(checks)
        and not malformed_checks
        and not duplicate_static_check_ids
        and not scan_errors
        and not forbidden_matches
    )
    return _contract(
        "StaticPolicyEnforcementContract",
        passed,
        "static_policy_missing_malformed_or_violated",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "static_check_count": len(checks),
            "scan_target_file_count": len(scan_target_files),
            "duplicate_static_check_ids": sorted(str(item) for item in duplicate_static_check_ids),
            "malformed_checks": malformed_checks,
            "scan_errors": scan_errors,
            "forbidden_match_count": len(forbidden_matches),
            "forbidden_matches": forbidden_matches,
        },
    )


def verify_api_response_contract(
    policy_path=DEFAULT_API_RESPONSE_POLICY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        policy = _load_json(policy_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("APIResponseContract", False, "api_response_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict) or not isinstance(access_policy, dict):
        return _contract(
            "APIResponseContract",
            False,
            "api_response_policy_not_object",
            {
                "policy_path": str(policy_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    source_lines, source_error = _source_lines(policy.get("source_file") or access_policy.get("source_file"))
    if source_error:
        return _contract("APIResponseContract", False, "api_response_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(source_lines)
    routes = _extract_dashboard_routes(source_lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    overrides = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }
    v27_evidence_endpoints = {
        endpoint
        for endpoint, item in overrides.items()
        if item.get("token_scope") == "v27:evidence_mutation"
    }

    response_policies = policy.get("response_policies") if isinstance(policy.get("response_policies"), list) else []
    malformed_policies = []
    duplicate_endpoints = []
    route_violations = []
    source_violations = []
    seen_endpoints = set()

    for index, item in enumerate(response_policies):
        if not isinstance(item, dict):
            malformed_policies.append({"index": index, "endpoint": None, "missing_fields": list(API_RESPONSE_REQUIRED_FIELDS), "violations": ["policy_not_object"]})
            continue
        endpoint = str(item.get("endpoint") or "")
        missing = _missing_required_fields(item, API_RESPONSE_REQUIRED_FIELDS)
        violations = []
        if endpoint in seen_endpoints:
            duplicate_endpoints.append(endpoint)
        if endpoint:
            seen_endpoints.add(endpoint)

        status_policy = item.get("status_code_policy")
        if not isinstance(status_policy, dict):
            violations.append("status_code_policy_not_object")
        else:
            if status_policy.get("accepted") != 202:
                violations.append("accepted_status_must_be_202")
            if status_policy.get("rejected") != 409:
                violations.append("rejected_status_must_be_409")
            if status_policy.get("method_not_allowed") != 405:
                violations.append("method_not_allowed_status_must_be_405")
            auth_failed = status_policy.get("auth_failed")
            if not isinstance(auth_failed, list) or sorted(int(value) for value in auth_failed) != [401, 403]:
                violations.append("auth_failed_statuses_must_be_401_403")
            if status_policy.get("audit_unavailable") != 500:
                violations.append("audit_unavailable_status_must_be_500")

        error_envelope = item.get("error_envelope")
        if not isinstance(error_envelope, dict):
            violations.append("error_envelope_not_object")
        else:
            if error_envelope.get("required") is not True:
                violations.append("error_envelope_required_must_be_true")
            if error_envelope.get("error_field") != "error":
                violations.append("error_field_must_be_error")
            if error_envelope.get("guard_errors") is not True:
                violations.append("guard_errors_must_be_true")
            if error_envelope.get("rejected_response_error_required") is not True:
                violations.append("rejected_response_error_required_must_be_true")

        if item.get("cache_control") != "no-store":
            violations.append("cache_control_must_be_no_store")

        route = route_by_endpoint.get(endpoint)
        if not route:
            route_violations.append({"endpoint": endpoint, "reason": "route_not_found"})
        else:
            if route.get("has_post_guard") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "post_guard_missing"})
            if route.get("has_check_auth") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "auth_guard_missing"})
            if route.get("has_audit_event") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "audit_guard_missing"})
        access_override = overrides.get(endpoint)
        if not access_override:
            route_violations.append({"endpoint": endpoint, "reason": "access_policy_override_missing"})
        elif access_override.get("audit_log_required") is not True:
            route_violations.append({"endpoint": endpoint, "reason": "access_policy_audit_required_missing"})

        route_block = _dashboard_route_block(source_lines, endpoint)
        source_anchor = str(item.get("source_anchor") or "")
        response_schema_version = str(item.get("response_schema_version") or "")
        if source_anchor and source_anchor not in source_text:
            source_violations.append({"endpoint": endpoint, "reason": "source_anchor_missing", "source_anchor": source_anchor})
        if response_schema_version and response_schema_version not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "response_schema_version_missing_in_route"})
        if "buildV27ManualEvidenceApiResponse(" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "response_builder_missing"})
        if "apiJsonHeaders()" not in route_block and "apiJsonHeaders('no-store')" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "no_store_header_missing"})
        if "? 202 : 409" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "accepted_rejected_status_branch_missing"})

        if missing or violations:
            malformed_policies.append({"index": index, "endpoint": endpoint or None, "missing_fields": missing, "violations": violations})

    policy_endpoints = {
        str(item.get("endpoint"))
        for item in response_policies
        if isinstance(item, dict) and item.get("endpoint")
    }
    uncovered_v27_evidence_endpoints = sorted(v27_evidence_endpoints - policy_endpoints)
    unknown_policy_endpoints = sorted(policy_endpoints - v27_evidence_endpoints)
    guard_helper_fragments = {
        "api_json_headers_default_no_store": "apiJsonHeaders(cacheControl = 'no-store')" in source_text,
        "api_json_headers_cache_control": "'Cache-Control': cacheControl" in source_text,
        "response_generated_at": "generated_at: generatedAt" in source_text,
        "response_schema_version": "response_schema_version: responseSchemaVersion" in source_text,
        "response_legacy_refresh_schema_version": "refresh_schema_version: responseSchemaVersion" in source_text,
        "response_materialized_false": "materialized: false" in source_text,
        "auth_403_no_store": "res.writeHead(403, apiJsonHeaders())" in source_text,
        "auth_401_no_store": "res.writeHead(401, apiJsonHeaders())" in source_text,
        "method_405_no_store": "res.writeHead(405, apiJsonHeaders())" in source_text,
        "audit_500_no_store": "res.writeHead(500, apiJsonHeaders())" in source_text and "Audit log unavailable" in source_text,
        "rejected_response_error": "payload.accepted === false && !payload.error" in source_text,
    }
    missing_guard_helper_fragments = sorted(key for key, present in guard_helper_fragments.items() if not present)

    passed = (
        policy.get("schema_version") == "v2.7.0.api_response_policy.v1"
        and bool(response_policies)
        and len(response_policies) >= 6
        and not malformed_policies
        and not duplicate_endpoints
        and not uncovered_v27_evidence_endpoints
        and not unknown_policy_endpoints
        and not route_violations
        and not source_violations
        and not missing_guard_helper_fragments
    )
    return _contract(
        "APIResponseContract",
        passed,
        "api_response_policy_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "source_file": policy.get("source_file") or access_policy.get("source_file"),
            "endpoint_count": len(response_policies),
            "endpoints": sorted(str(item.get("endpoint")) for item in response_policies if isinstance(item, dict) and item.get("endpoint")),
            "v27_evidence_endpoint_count": len(v27_evidence_endpoints),
            "uncovered_v27_evidence_endpoints": uncovered_v27_evidence_endpoints,
            "unknown_policy_endpoints": unknown_policy_endpoints,
            "duplicate_endpoints": sorted(str(item) for item in duplicate_endpoints),
            "malformed_policies": malformed_policies,
            "route_violations": route_violations,
            "source_violations": source_violations,
            "guard_helper_fragments": guard_helper_fragments,
            "missing_guard_helper_fragments": missing_guard_helper_fragments,
        },
    )


def _api_response_error_shape(payload):
    has_error = bool(payload.get("error") or payload.get("error_code") or payload.get("accepted") is False)
    return {
        "has_error": has_error,
        "accepted": None if "accepted" not in payload else bool(payload.get("accepted")),
        "error_field": "error" if payload.get("error") else None,
        "error_code": payload.get("error_code") or None,
        "status": payload.get("status") or None,
    }


def _build_api_response_envelope_sample(sample, envelope_version):
    result = sample.get("result") if isinstance(sample.get("result"), dict) else {}
    response_schema_version = str(sample.get("response_schema_version") or "")
    payload = {
        "generated_at": sample.get("generated_at"),
        "materialized": False,
        "endpoint": sample.get("endpoint"),
        "envelope_version": envelope_version,
        "response_schema_version": response_schema_version,
        "refresh_schema_version": response_schema_version,
        **result,
    }
    if payload.get("accepted") is False and not payload.get("error"):
        payload["error"] = payload.get("status") or "manual_evidence_request_rejected"
    if payload.get("accepted") is False and not payload.get("error_code"):
        payload["error_code"] = payload.get("error") or "manual_evidence_request_rejected"
    payload["error_shape"] = _api_response_error_shape(payload)
    payload["payload_hash"] = _sha256_json({key: value for key, value in payload.items() if key != "payload_hash"})
    return payload


def verify_api_response_envelope_contract(
    policy_path=DEFAULT_API_RESPONSE_ENVELOPE_POLICY,
):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_policy_not_object", {"policy_path": str(policy_path)})

    base_policy_path = _resolve_project_file(policy.get("base_response_policy_path")) or DEFAULT_API_RESPONSE_POLICY
    try:
        base_policy = _load_json(base_policy_path)
    except Exception as exc:
        return _contract(
            "APIResponseEnvelopeContract",
            False,
            "api_response_envelope_policy_missing_or_invalid",
            {
                "policy_path": str(policy_path),
                "base_response_policy_path": str(base_policy_path),
                "error": str(exc),
            },
        )
    if not isinstance(base_policy, dict):
        return _contract(
            "APIResponseEnvelopeContract",
            False,
            "api_response_envelope_policy_not_object",
            {
                "policy_path": str(policy_path),
                "base_response_policy_path": str(base_policy_path),
            },
        )

    source_lines, source_error = _source_lines(policy.get("source_file") or base_policy.get("source_file"))
    if source_error:
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(source_lines)

    base_response_policies = base_policy.get("response_policies") if isinstance(base_policy.get("response_policies"), list) else []
    base_endpoints = {
        str(item.get("endpoint"))
        for item in base_response_policies
        if isinstance(item, dict) and item.get("endpoint")
    }
    response_envelopes = policy.get("response_envelopes") if isinstance(policy.get("response_envelopes"), list) else []
    envelope_version = str(policy.get("envelope_version") or "")
    required_fields = [str(item) for item in (policy.get("required_fields") or [])]

    schema_violations = []
    if set(required_fields) != set(API_RESPONSE_ENVELOPE_SPEC_FIELDS):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if policy.get("failure_action") != "api_envelope_invalid":
        schema_violations.append("failure_action_must_be_api_envelope_invalid")
    if policy.get("hash_algorithm") != "sha256_canonical_json_without_payload_hash":
        schema_violations.append("hash_algorithm_must_exclude_payload_hash")
    if not envelope_version:
        schema_violations.append("envelope_version_required")

    malformed_envelopes = []
    duplicate_endpoints = []
    source_violations = []
    seen_endpoints = set()
    for index, item in enumerate(response_envelopes):
        if not isinstance(item, dict):
            malformed_envelopes.append({"index": index, "endpoint": None, "missing_fields": list(API_RESPONSE_ENVELOPE_REQUIRED_FIELDS), "violations": ["envelope_policy_not_object"]})
            continue
        endpoint = str(item.get("endpoint") or "")
        missing = _missing_required_fields(item, API_RESPONSE_ENVELOPE_REQUIRED_FIELDS)
        violations = []
        if endpoint in seen_endpoints:
            duplicate_endpoints.append(endpoint)
        if endpoint:
            seen_endpoints.add(endpoint)
        if endpoint not in base_endpoints:
            violations.append("endpoint_not_in_base_response_policy")
        route_block = _dashboard_route_block(source_lines, endpoint)
        if not route_block:
            source_violations.append({"endpoint": endpoint, "reason": "route_not_found"})
        else:
            response_schema_version = str(item.get("response_schema_version") or "")
            source_anchor = str(item.get("source_anchor") or "")
            if response_schema_version and response_schema_version not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "response_schema_version_missing_in_route"})
            if source_anchor and source_anchor not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "source_anchor_missing", "source_anchor": source_anchor})
            if "{ endpoint: url.pathname }" not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "endpoint_binding_missing"})
        if missing or violations:
            malformed_envelopes.append({"index": index, "endpoint": endpoint or None, "missing_fields": missing, "violations": violations})

    policy_endpoints = {
        str(item.get("endpoint"))
        for item in response_envelopes
        if isinstance(item, dict) and item.get("endpoint")
    }
    uncovered_base_response_endpoints = sorted(base_endpoints - policy_endpoints)
    unknown_envelope_endpoints = sorted(policy_endpoints - base_endpoints)

    error_shape_policy = policy.get("error_shape") if isinstance(policy.get("error_shape"), dict) else {}
    error_shape_required_fields = [str(item) for item in (error_shape_policy.get("required_fields") or [])]
    error_shape_violations = []
    if set(error_shape_required_fields) != {"has_error", "accepted", "error_field", "error_code", "status"}:
        error_shape_violations.append("error_shape_required_fields_incomplete")
    if error_shape_policy.get("accepted_false_requires_error") is not True:
        error_shape_violations.append("accepted_false_requires_error_must_be_true")
    if error_shape_policy.get("accepted_false_requires_error_code") is not True:
        error_shape_violations.append("accepted_false_requires_error_code_must_be_true")

    malformed_samples = []
    sample_evidence = []
    sample_cases = policy.get("sample_cases") if isinstance(policy.get("sample_cases"), list) else []
    for index, sample in enumerate(sample_cases):
        if not isinstance(sample, dict):
            malformed_samples.append({"index": index, "sample_id": None, "violations": ["sample_not_object"]})
            continue
        sample_id = str(sample.get("sample_id") or "")
        violations = []
        missing = _missing_required_fields(sample, ("sample_id", "endpoint", "response_schema_version", "generated_at", "result", "expected_error_shape"))
        payload = _build_api_response_envelope_sample(sample, envelope_version)
        payload_missing_fields = [field for field in API_RESPONSE_ENVELOPE_SPEC_FIELDS if payload.get(field) in (None, "", [], {})]
        if payload_missing_fields:
            violations.append("payload_missing_required_fields")
        expected_error_shape = sample.get("expected_error_shape")
        if expected_error_shape != payload.get("error_shape"):
            violations.append("expected_error_shape_mismatch")
        if not re.fullmatch(r"[a-f0-9]{64}", str(payload.get("payload_hash") or "")):
            violations.append("payload_hash_invalid")
        if payload.get("payload_hash") != _sha256_json({key: value for key, value in payload.items() if key != "payload_hash"}):
            violations.append("payload_hash_mismatch")
        sample_evidence.append(
            {
                "sample_id": sample_id,
                "endpoint": payload.get("endpoint"),
                "envelope_version": payload.get("envelope_version"),
                "payload_hash": payload.get("payload_hash"),
                "error_shape": payload.get("error_shape"),
                "generated_at": payload.get("generated_at"),
            }
        )
        if missing or violations:
            malformed_samples.append({"index": index, "sample_id": sample_id or None, "missing_fields": missing, "violations": violations, "payload_missing_fields": payload_missing_fields})

    helper_fragments = {
        "envelope_version_constant": f"V27_API_RESPONSE_ENVELOPE_VERSION = '{envelope_version}'" in source_text,
        "endpoint_field": "endpoint: options.endpoint || null" in source_text,
        "envelope_version_field": "envelope_version: V27_API_RESPONSE_ENVELOPE_VERSION" in source_text,
        "error_shape_helper": "function buildApiResponseErrorShape(payload = {})" in source_text,
        "payload_hash_helper": "function apiEnvelopePayloadForHash(payload = {})" in source_text,
        "payload_hash_excludes_self": "const { payload_hash, ...unsignedPayload } = payload || {};" in source_text,
        "payload_hash_assignment": "payload.payload_hash = auditSha256Hex(apiEnvelopePayloadForHash(payload));" in source_text,
    }
    missing_helper_fragments = sorted(key for key, present in helper_fragments.items() if not present)

    passed = (
        policy.get("schema_version") == "v2.7.0.api_response_envelope_policy.v1"
        and bool(response_envelopes)
        and bool(sample_cases)
        and not schema_violations
        and not error_shape_violations
        and not malformed_envelopes
        and not duplicate_endpoints
        and not uncovered_base_response_endpoints
        and not unknown_envelope_endpoints
        and not source_violations
        and not malformed_samples
        and not missing_helper_fragments
    )
    return _contract(
        "APIResponseEnvelopeContract",
        passed,
        "api_response_envelope_policy_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "base_response_policy_path": str(base_policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "envelope_version": envelope_version,
            "hash_algorithm": policy.get("hash_algorithm"),
            "required_fields": required_fields,
            "endpoint_count": len(response_envelopes),
            "base_response_endpoint_count": len(base_endpoints),
            "sample_case_count": len(sample_cases),
            "sample_evidence": sample_evidence,
            "schema_violations": schema_violations,
            "error_shape_violations": error_shape_violations,
            "duplicate_endpoints": sorted(str(item) for item in duplicate_endpoints),
            "malformed_envelopes": malformed_envelopes,
            "uncovered_base_response_endpoints": uncovered_base_response_endpoints,
            "unknown_envelope_endpoints": unknown_envelope_endpoints,
            "source_violations": source_violations,
            "malformed_samples": malformed_samples,
            "helper_fragments": helper_fragments,
            "missing_helper_fragments": missing_helper_fragments,
        },
    )


def _read_project_text(path):
    resolved = _resolve_project_file(path)
    if not resolved or not resolved.exists():
        return "", {"source_file": str(path), "reason": "source_missing"}
    try:
        return resolved.read_text(encoding="utf-8"), None
    except UnicodeDecodeError as exc:
        return "", {"source_file": str(path), "reason": "source_decode_failed", "error": str(exc)}


def _extract_basic_readiness_error_codes(source_text):
    codes = set(
        re.findall(
            r"_contract\(\s*(?:['\"][^'\"]+['\"]|[A-Za-z_][A-Za-z0-9_]*)\s*,\s*[^,]+,\s*['\"]([^'\"]+)['\"]",
            source_text,
            flags=re.S,
        )
    )
    codes.update(re.findall(r"reason\s+or\s+['\"]([^'\"]+)['\"]", source_text))
    codes.update(re.findall(r"_contracts_from_error\(\s*[^,]+,\s*['\"]([^'\"]+)['\"]", source_text, flags=re.S))
    codes.update(re.findall(r"_failed_detector_shadow_contracts\(\s*['\"]([^'\"]+)['\"]", source_text, flags=re.S))
    codes.update(re.findall(r"['\"]blocking_reason['\"]\s*:\s*['\"]([^'\"]+)['\"]", source_text))
    return codes


def _extract_dashboard_error_codes(source_text):
    codes = set(re.findall(r"error_code\s*:\s*['\"]([^'\"]+)['\"]", source_text))
    if "manual_evidence_request_rejected" in source_text:
        codes.add("manual_evidence_request_rejected")
    codes.update(
        re.findall(
            r"accepted\s*:\s*false\s*,[\s\S]{0,180}?status\s*:\s*['\"]([^'\"]+)['\"]",
            source_text,
        )
    )
    return codes


def _extract_paper_mode_error_codes(source_text):
    return set(
        re.findall(
            r"['\"](paper_[a-z0-9_]*(?:detected|missing|unverified))['\"]",
            source_text,
        )
    )


def verify_error_taxonomy(
    taxonomy_path=DEFAULT_ERROR_TAXONOMY,
    dashboard_source_path=None,
    basic_readiness_source_path=None,
    paper_mode_safety_source_path=None,
):
    try:
        taxonomy = _load_json(taxonomy_path)
    except Exception as exc:
        return _contract("ErrorTaxonomyContract", False, "error_taxonomy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(taxonomy, dict):
        return _contract("ErrorTaxonomyContract", False, "error_taxonomy_not_object", {"taxonomy_path": str(taxonomy_path)})

    coverage = taxonomy.get("coverage") if isinstance(taxonomy.get("coverage"), dict) else {}
    dashboard_source_path = dashboard_source_path or coverage.get("dashboard_source_file") or "src/web/dashboard-server.js"
    basic_readiness_source_path = basic_readiness_source_path or coverage.get("basic_readiness_source_file") or "scripts/v27_basic_contract_readiness.py"
    paper_mode_safety_source_path = paper_mode_safety_source_path or coverage.get("paper_mode_safety_source_file") or "scripts/v27_paper_mode_safety.py"

    source_errors = []
    dashboard_text, source_error = _read_project_text(dashboard_source_path)
    if source_error:
        source_errors.append({"source": "dashboard", **source_error})
    basic_text, source_error = _read_project_text(basic_readiness_source_path)
    if source_error:
        source_errors.append({"source": "basic_readiness", **source_error})
    paper_text, source_error = _read_project_text(paper_mode_safety_source_path)
    if source_error:
        source_errors.append({"source": "paper_mode_safety", **source_error})

    observed_by_source = {
        "dashboard_api_error_codes": sorted(_extract_dashboard_error_codes(dashboard_text)),
        "basic_readiness_blocking_reasons": sorted(_extract_basic_readiness_error_codes(basic_text)),
        "paper_mode_safety_reasons": sorted(_extract_paper_mode_error_codes(paper_text)),
    }
    required_codes = set()
    for codes in observed_by_source.values():
        required_codes.update(codes)

    allowed_categories = set(str(item) for item in (taxonomy.get("allowed_categories") or []))
    allowed_severities = set(str(item) for item in (taxonomy.get("allowed_severities") or []))
    entries = taxonomy.get("taxonomy") if isinstance(taxonomy.get("taxonomy"), list) else []
    malformed_entries = []
    duplicate_error_codes = []
    taxonomy_codes = set()
    seen_codes = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            malformed_entries.append({"index": index, "error_code": None, "missing_fields": list(ERROR_TAXONOMY_REQUIRED_FIELDS), "violations": ["entry_not_object"]})
            continue
        error_code = str(entry.get("error_code") or "")
        missing = _missing_required_fields(entry, ERROR_TAXONOMY_REQUIRED_FIELDS)
        violations = []
        if error_code in seen_codes:
            duplicate_error_codes.append(error_code)
        if error_code:
            seen_codes.add(error_code)
            taxonomy_codes.add(error_code)
        if error_code and not re.match(r"^[a-z][a-z0-9_]*$", error_code):
            violations.append("error_code_must_be_lower_snake_case")
        if str(entry.get("category") or "") not in allowed_categories:
            violations.append("category_not_allowed")
        if str(entry.get("severity") or "") not in allowed_severities:
            violations.append("severity_not_allowed")
        if entry.get("introduced_at") and _parse_iso_ts(entry.get("introduced_at")) is None:
            violations.append("introduced_at_invalid_iso_timestamp")
        if missing or violations:
            malformed_entries.append({"index": index, "error_code": error_code or None, "missing_fields": missing, "violations": violations})

    unclassified_error_codes = sorted(required_codes - taxonomy_codes)
    unused_taxonomy_codes = sorted(taxonomy_codes - required_codes)
    passed = (
        taxonomy.get("schema_version") == "v2.7.0.error_taxonomy.v1"
        and bool(entries)
        and bool(allowed_categories)
        and bool(allowed_severities)
        and not source_errors
        and not malformed_entries
        and not duplicate_error_codes
        and not unclassified_error_codes
        and not unused_taxonomy_codes
    )
    return _contract(
        "ErrorTaxonomyContract",
        passed,
        "error_taxonomy_missing_malformed_or_incomplete",
        {
            "taxonomy_path": str(taxonomy_path),
            "schema_version": taxonomy.get("schema_version"),
            "scope": taxonomy.get("scope"),
            "failure_action": taxonomy.get("failure_action"),
            "taxonomy_entry_count": len(entries),
            "required_error_code_count": len(required_codes),
            "observed_by_source": observed_by_source,
            "duplicate_error_codes": sorted(str(item) for item in duplicate_error_codes),
            "malformed_entries": malformed_entries,
            "source_errors": source_errors,
            "unclassified_error_codes": unclassified_error_codes,
            "unused_taxonomy_codes": unused_taxonomy_codes,
        },
    )


def _expected_basic_readiness_reason_bindings(source_text):
    bindings = {}
    for contract_id, reason_code in re.findall(
        r"_contract\(\s*['\"]([^'\"]+)['\"]\s*,\s*[^,]+,\s*['\"]([^'\"]+)['\"]",
        source_text,
        flags=re.S,
    ):
        bindings.setdefault(reason_code, contract_id)
    return bindings


def _verify_reason_taxonomy(policy_path=DEFAULT_REASON_TAXONOMY_POLICY, *, contract_id, required_fields, schema_version):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract(contract_id, False, "reason_taxonomy_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract(contract_id, False, "reason_taxonomy_policy_not_object", {"policy_path": str(policy_path)})

    human_contract = contract_id == "HumanReadableReasonContract"
    required_field_key = "human_required_fields" if human_contract else "machine_required_fields"
    schema_version_key = "human_reason_schema_version" if human_contract else "machine_reason_schema_version"
    coverage = policy.get("coverage") if isinstance(policy.get("coverage"), dict) else {}
    basic_readiness_source_path = coverage.get("basic_readiness_source_file") or "scripts/v27_basic_contract_readiness.py"
    source_text, source_error = _read_project_text(basic_readiness_source_path)
    expected_bindings = {} if source_error else _expected_basic_readiness_reason_bindings(source_text)
    catalog_failure_actions = {}
    catalog_error = None
    catalog_path = _resolve_project_file(coverage.get("contract_catalog_file") or CATALOG_PATH)
    try:
        catalog = _load_json(catalog_path)
        catalog_failure_actions = {
            contract: record.get("failure_action")
            for contract, record in (catalog.get("contracts") or {}).items()
            if isinstance(record, dict)
        }
    except Exception as exc:
        catalog_error = {"source_file": str(catalog_path), "reason": "catalog_missing_or_invalid", "error": str(exc)}

    taxonomy_by_code = {}
    taxonomy_error = None
    taxonomy_path = _resolve_project_file(coverage.get("error_taxonomy_file") or DEFAULT_ERROR_TAXONOMY)
    try:
        taxonomy = _load_json(taxonomy_path)
        taxonomy_by_code = {
            item.get("error_code"): item
            for item in (taxonomy.get("taxonomy") or [])
            if isinstance(item, dict) and item.get("error_code")
        }
    except Exception as exc:
        taxonomy_error = {"source_file": str(taxonomy_path), "reason": "taxonomy_missing_or_invalid", "error": str(exc)}

    allowed_locales = set(str(item) for item in (policy.get("allowed_locales") or []))
    allowed_schema_versions = set(str(item) for item in (policy.get("allowed_schema_versions") or []))
    default_locale = str(policy.get("default_locale") or "")
    owner_by_category = policy.get("owner_by_category") if isinstance(policy.get("owner_by_category"), dict) else {}
    message_template = str(policy.get("human_message_template") or "{blocking_contract} is blocked by {reason_code}.")
    reason_evidence = []
    malformed_reasons = []
    missing_reason_codes = []
    for index, (reason_code, blocking_contract) in enumerate(sorted(expected_bindings.items())):
        taxonomy_entry = taxonomy_by_code.get(reason_code)
        violations = []
        if not taxonomy_entry:
            missing_reason_codes.append(reason_code)
            taxonomy_entry = {}
        operator_action = str(taxonomy_entry.get("operator_action") or "")
        owner = str(owner_by_category.get(taxonomy_entry.get("category")) or owner_by_category.get("default") or "")
        failure_action = catalog_failure_actions.get(blocking_contract)
        human_message = message_template.format(
            blocking_contract=blocking_contract,
            reason_code=reason_code,
            operator_action=operator_action,
        )
        if human_contract:
            reason = {
                "reason_code": reason_code,
                "human_message": human_message,
                "operator_action": operator_action,
                "locale": default_locale,
                "owner": owner,
            }
            missing = _missing_required_fields(reason, required_fields)
            if str(reason.get("locale") or "") not in allowed_locales:
                violations.append("locale_not_allowed")
            if len(str(reason.get("human_message") or "").strip()) < 12:
                violations.append("human_message_too_short")
            if len(str(reason.get("operator_action") or "").strip()) < 12:
                violations.append("operator_action_too_short")
        else:
            reason = {
                "reason_code": reason_code,
                "machine_code": reason_code.upper(),
                "schema_version": schema_version,
                "blocking_contract": blocking_contract,
                "failure_action": failure_action,
            }
            missing = _missing_required_fields(reason, required_fields)
            if str(reason.get("schema_version") or "") not in allowed_schema_versions:
                violations.append("schema_version_not_allowed")
            if str(reason.get("machine_code") or "") != reason_code.upper():
                violations.append("machine_code_must_be_upper_reason_code")
            if not catalog_failure_actions.get(blocking_contract):
                violations.append("failure_action_missing_from_catalog")
        if missing or violations:
            malformed_reasons.append({"index": index, "reason_code": reason_code or None, "missing_fields": missing, "violations": violations})
        reason_evidence.append(reason)

    schema_violations = []
    if policy.get("schema_version") != "v2.7.0.reason_taxonomy_policy.v1":
        schema_violations.append("schema_version_invalid")
    if policy.get("failure_action") != "reason_missing":
        schema_violations.append("failure_action_must_be_reason_missing")
    if policy.get(schema_version_key) != schema_version:
        schema_violations.append(f"{schema_version_key}_invalid")
    if set(policy.get(required_field_key) or []) != set(required_fields):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if not allowed_locales:
        schema_violations.append("allowed_locales_required")
    if not allowed_schema_versions:
        schema_violations.append("allowed_schema_versions_required")

    passed = (
        not source_error
        and not catalog_error
        and not taxonomy_error
        and not schema_violations
        and bool(reason_evidence)
        and not malformed_reasons
        and not missing_reason_codes
    )
    return _contract(
        contract_id,
        passed,
        "reason_taxonomy_policy_missing_malformed_or_incomplete",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "reason_schema_version": policy.get(schema_version_key),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "required_fields": list(required_fields),
            "reason_count": len(reason_evidence),
            "expected_reason_count": len(expected_bindings),
            "coverage": coverage,
            "source_error": source_error,
            "catalog_error": catalog_error,
            "taxonomy_error": taxonomy_error,
            "schema_violations": schema_violations,
            "malformed_reasons": malformed_reasons,
            "missing_reason_codes": missing_reason_codes,
            "sample_reasons": reason_evidence[:20],
        },
    )


def verify_human_readable_reason_contract(policy_path=DEFAULT_REASON_TAXONOMY_POLICY):
    return _verify_reason_taxonomy(
        policy_path,
        contract_id="HumanReadableReasonContract",
        required_fields=HUMAN_REASON_REQUIRED_FIELDS,
        schema_version="v2.7.0.human_reason.v1",
    )


def verify_machine_readable_reason_contract(policy_path=DEFAULT_REASON_TAXONOMY_POLICY):
    return _verify_reason_taxonomy(
        policy_path,
        contract_id="MachineReadableReasonContract",
        required_fields=MACHINE_REASON_REQUIRED_FIELDS,
        schema_version="v2.7.0.machine_reason.v1",
    )


def _apply_log_redaction_patterns(raw, patterns):
    text = str(raw)
    for pattern in patterns:
        regex = pattern.get("regex") if isinstance(pattern, dict) else None
        replacement = pattern.get("replacement") if isinstance(pattern, dict) else None
        if not regex or replacement is None:
            continue
        text = re.sub(str(regex), str(replacement), text, flags=re.IGNORECASE)
    return text


def verify_log_redaction_verification(policy_path=DEFAULT_LOG_REDACTION_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("LogRedactionVerificationContract", False, "log_redaction_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("LogRedactionVerificationContract", False, "log_redaction_policy_not_object", {"policy_path": str(policy_path)})

    pattern_set = policy.get("secret_pattern_set") if isinstance(policy.get("secret_pattern_set"), dict) else {}
    secret_pattern_set = str(pattern_set.get("secret_pattern_set") or "")
    patterns = pattern_set.get("patterns") if isinstance(pattern_set.get("patterns"), list) else []
    sample_cases = {
        str(item.get("sample_id")): item
        for item in (policy.get("sample_cases") or [])
        if isinstance(item, dict) and item.get("sample_id")
    }
    streams = policy.get("streams") if isinstance(policy.get("streams"), list) else []

    malformed_patterns = []
    duplicate_pattern_ids = []
    seen_pattern_ids = set()
    for index, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            malformed_patterns.append({"index": index, "pattern_id": None, "violations": ["pattern_not_object"]})
            continue
        pattern_id = str(pattern.get("pattern_id") or "")
        violations = []
        if pattern_id in seen_pattern_ids:
            duplicate_pattern_ids.append(pattern_id)
        if pattern_id:
            seen_pattern_ids.add(pattern_id)
        if not pattern_id:
            violations.append("pattern_id_required")
        if not pattern.get("regex"):
            violations.append("regex_required")
        else:
            try:
                re.compile(str(pattern.get("regex")), flags=re.IGNORECASE)
            except re.error as exc:
                violations.append(f"regex_invalid:{exc}")
        if pattern.get("replacement") is None:
            violations.append("replacement_required")
        if violations:
            malformed_patterns.append({"index": index, "pattern_id": pattern_id or None, "violations": violations})

    malformed_samples = []
    sample_results = {}
    for sample_id, sample in sample_cases.items():
        raw = str(sample.get("raw") or "")
        redacted = _apply_log_redaction_patterns(raw, patterns)
        absent_failures = [
            fragment for fragment in (sample.get("expected_fragments_absent") or [])
            if str(fragment) and str(fragment) in redacted
        ]
        present_failures = [
            fragment for fragment in (sample.get("expected_fragments_present") or [])
            if str(fragment) and str(fragment) not in redacted
        ]
        redaction_passed = not absent_failures and not present_failures and redacted != raw
        if not raw or not redaction_passed:
            malformed_samples.append(
                {
                    "sample_id": sample_id,
                    "absent_failures": absent_failures,
                    "present_failures": present_failures,
                    "raw_present": bool(raw),
                    "redaction_changed_sample": redacted != raw,
                }
            )
        sample_results[sample_id] = {
            "sample_hash": _sha256_json({"sample_id": sample_id, "raw": raw}),
            "redaction_passed": redaction_passed,
        }

    checked_at = _utc_now_iso()
    malformed_streams = []
    source_violations = []
    stream_evidence = []
    for index, stream in enumerate(streams):
        if not isinstance(stream, dict):
            malformed_streams.append({"index": index, "log_stream": None, "missing_fields": list(LOG_REDACTION_STREAM_REQUIRED_FIELDS), "violations": ["stream_not_object"]})
            continue
        log_stream = str(stream.get("log_stream") or "")
        missing = _missing_required_fields(stream, LOG_REDACTION_STREAM_REQUIRED_FIELDS)
        violations = []
        if stream.get("secret_pattern_set") != secret_pattern_set:
            violations.append("secret_pattern_set_mismatch")
        sample_case_ids = [str(item) for item in (stream.get("sample_case_ids") or [])]
        unknown_samples = sorted(sample_id for sample_id in sample_case_ids if sample_id not in sample_cases)
        if unknown_samples:
            violations.append("unknown_sample_case_ids")
        stream_sample_passed = all(sample_results.get(sample_id, {}).get("redaction_passed") for sample_id in sample_case_ids)
        sample_hash = _sha256_json(
            {
                "log_stream": log_stream,
                "secret_pattern_set": stream.get("secret_pattern_set"),
                "sample_hashes": [sample_results.get(sample_id, {}).get("sample_hash") for sample_id in sample_case_ids],
            }
        )

        source_text, source_error = _read_project_text(stream.get("source_file"))
        if source_error:
            source_violations.append({"log_stream": log_stream, **source_error})
        else:
            redaction_anchor = str(stream.get("redaction_anchor") or "")
            write_anchor = str(stream.get("write_anchor") or "")
            if redaction_anchor and redaction_anchor not in source_text:
                source_violations.append({"log_stream": log_stream, "reason": "redaction_anchor_missing", "redaction_anchor": redaction_anchor})
            if write_anchor and write_anchor not in source_text:
                source_violations.append({"log_stream": log_stream, "reason": "write_anchor_missing", "write_anchor": write_anchor})
            if log_stream == "v27_manual_evidence_child_process_logs":
                raw_write_count = source_text.count("logStream.write(")
                if raw_write_count != 1 or "logStream.write(redactLogMessage(chunk));" not in source_text:
                    source_violations.append({"log_stream": log_stream, "reason": "raw_log_stream_write_bypass", "raw_log_stream_write_count": raw_write_count})

        redaction_passed = stream_sample_passed and not unknown_samples and not missing and not violations
        stream_evidence.append(
            {
                "log_stream": log_stream,
                "secret_pattern_set": stream.get("secret_pattern_set"),
                "sample_hash": sample_hash,
                "redaction_passed": redaction_passed,
                "checked_at": checked_at,
                "sample_case_ids": sample_case_ids,
            }
        )
        if missing or violations:
            malformed_streams.append({"index": index, "log_stream": log_stream or None, "missing_fields": missing, "violations": violations, "unknown_sample_case_ids": unknown_samples})

    passed = (
        policy.get("schema_version") == "v2.7.0.log_redaction_policy.v1"
        and bool(secret_pattern_set)
        and bool(patterns)
        and bool(sample_cases)
        and bool(streams)
        and not malformed_patterns
        and not duplicate_pattern_ids
        and not malformed_samples
        and not malformed_streams
        and not source_violations
        and all(item.get("redaction_passed") for item in stream_evidence)
    )
    return _contract(
        "LogRedactionVerificationContract",
        passed,
        "log_redaction_verification_missing_malformed_or_failed",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "secret_pattern_set": secret_pattern_set,
            "pattern_count": len(patterns),
            "sample_case_count": len(sample_cases),
            "stream_count": len(streams),
            "streams": stream_evidence,
            "malformed_patterns": malformed_patterns,
            "duplicate_pattern_ids": sorted(str(item) for item in duplicate_pattern_ids),
            "malformed_samples": malformed_samples,
            "malformed_streams": malformed_streams,
            "source_violations": source_violations,
        },
    )


def _security_session_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "security_session_policy_not_object"}
    return policy, None


def _policy_source_anchor_violations(record, *, source_anchor_key="source_anchor"):
    source_file = record.get("source_file") if isinstance(record, dict) else None
    source_text, source_error = _read_project_text(source_file)
    if source_error:
        return [{**source_error}]
    anchors = record.get(source_anchor_key)
    if isinstance(anchors, list):
        expected_anchors = [str(anchor) for anchor in anchors if str(anchor)]
    else:
        expected_anchors = [str(anchors)] if anchors else []
    missing_anchors = [anchor for anchor in expected_anchors if anchor not in source_text]
    return [
        {
            "source_file": source_file,
            "reason": "source_anchor_missing",
            "missing_anchors": missing_anchors,
        }
    ] if missing_anchors else []


def verify_admin_session_security_contract(policy_path=DEFAULT_SECURITY_SESSION_POLICY):
    policy, policy_error = _security_session_policy(policy_path)
    if policy_error:
        return _contract("AdminSessionSecurityContract", False, "admin_session_security_missing_malformed_or_unenforced", policy_error)

    sessions = policy.get("admin_sessions") if isinstance(policy.get("admin_sessions"), list) else []
    malformed_sessions = []
    source_violations = []
    csrf_modes = {"post_only_mutation_and_non_cookie_token", "double_submit_token"}
    for index, session in enumerate(sessions):
        if not isinstance(session, dict):
            malformed_sessions.append({"index": index, "session_id": None, "missing_fields": list(ADMIN_SESSION_SECURITY_REQUIRED_FIELDS), "violations": ["session_not_object"]})
            continue
        missing = _missing_required_fields(session, ADMIN_SESSION_SECURITY_REQUIRED_FIELDS)
        violations = []
        if session.get("mfa_required") is not True:
            violations.append("mfa_required_must_be_true")
        if _parse_iso_ts(session.get("expires_at")) is None:
            violations.append("expires_at_invalid")
        if str(session.get("csrf_protection") or "") not in csrf_modes:
            violations.append("csrf_protection_invalid")
        if str(session.get("operator_id") or "") in {"root", "anonymous", "unknown"}:
            violations.append("operator_id_not_bound")
        if str(session.get("required_role") or "") and str(session.get("required_role")) != "dashboard_admin":
            violations.append("required_role_must_be_dashboard_admin")
        source_violations.extend(
            {"index": index, "session_id": session.get("session_id"), **violation}
            for violation in _policy_source_anchor_violations(session, source_anchor_key="source_anchors")
        )
        if missing or violations:
            malformed_sessions.append({"index": index, "session_id": session.get("session_id"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.security_session_policy.v1"
        and bool(sessions)
        and not malformed_sessions
        and not source_violations
    )
    return _contract(
        "AdminSessionSecurityContract",
        passed,
        "admin_session_security_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "session_count": len(sessions),
            "required_fields": list(ADMIN_SESSION_SECURITY_REQUIRED_FIELDS),
            "malformed_sessions": malformed_sessions,
            "source_violations": source_violations,
            "sessions": [
                {
                    "session_id": item.get("session_id"),
                    "operator_id": item.get("operator_id"),
                    "mfa_required": item.get("mfa_required"),
                    "expires_at": item.get("expires_at"),
                    "csrf_protection": item.get("csrf_protection"),
                    "token_scope": item.get("token_scope"),
                }
                for item in sessions
                if isinstance(item, dict)
            ],
        },
    )


def _log_redaction_pattern_ids(policy):
    pattern_set = policy.get("secret_pattern_set") if isinstance(policy.get("secret_pattern_set"), dict) else {}
    patterns = pattern_set.get("patterns") if isinstance(pattern_set.get("patterns"), list) else []
    return {
        str(pattern.get("pattern_id")): str(pattern.get("regex") or "")
        for pattern in patterns
        if isinstance(pattern, dict) and pattern.get("pattern_id")
    }


def verify_secret_access_audit_contract(policy_path=DEFAULT_SECURITY_SESSION_POLICY):
    policy, policy_error = _security_session_policy(policy_path)
    if policy_error:
        return _contract("SecretAccessAuditContract", False, "secret_access_audit_missing_malformed_or_unverified", policy_error)

    log_policy_path = _resolve_project_file(policy.get("log_redaction_policy_file")) or DEFAULT_LOG_REDACTION_POLICY
    try:
        log_policy = _load_json(log_policy_path)
        redaction_patterns = _log_redaction_pattern_ids(log_policy)
        log_policy_error = None
    except Exception as exc:
        redaction_patterns = {}
        log_policy_error = {"policy_path": str(log_policy_path), "error": str(exc)}

    records = policy.get("secret_access_audit") if isinstance(policy.get("secret_access_audit"), list) else []
    malformed_records = []
    source_violations = []
    redaction_violations = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            malformed_records.append({"index": index, "secret_id": None, "missing_fields": list(SECRET_ACCESS_AUDIT_REQUIRED_FIELDS), "violations": ["secret_access_record_not_object"]})
            continue
        missing = _missing_required_fields(record, SECRET_ACCESS_AUDIT_REQUIRED_FIELDS)
        violations = []
        if _parse_iso_ts(record.get("accessed_at")) is None:
            violations.append("accessed_at_invalid")
        if record.get("store_secret_value") is not False:
            violations.append("store_secret_value_must_be_false")
        if not re.match(r"^env:[A-Z][A-Z0-9_]*$", str(record.get("secret_id") or "")):
            violations.append("secret_id_must_reference_env_name")
        source_violations.extend(
            {"index": index, "secret_id": record.get("secret_id"), **violation}
            for violation in _policy_source_anchor_violations(record)
        )
        pattern_ids = [str(item) for item in (record.get("redaction_pattern_ids") or [])]
        unknown_patterns = sorted(pattern_id for pattern_id in pattern_ids if pattern_id not in redaction_patterns)
        if unknown_patterns:
            redaction_violations.append({"index": index, "secret_id": record.get("secret_id"), "unknown_pattern_ids": unknown_patterns})
        secret_name = str(record.get("secret_id") or "").split("env:", 1)[-1].lower()
        if secret_name and not any(secret_name in regex.lower() for regex in redaction_patterns.values()):
            redaction_violations.append({"index": index, "secret_id": record.get("secret_id"), "reason": "secret_name_not_covered_by_redaction_patterns"})
        if missing or violations:
            malformed_records.append({"index": index, "secret_id": record.get("secret_id"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.security_session_policy.v1"
        and len(records) >= 3
        and not log_policy_error
        and not malformed_records
        and not source_violations
        and not redaction_violations
    )
    return _contract(
        "SecretAccessAuditContract",
        passed,
        "secret_access_audit_missing_malformed_or_unverified",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "record_count": len(records),
            "required_fields": list(SECRET_ACCESS_AUDIT_REQUIRED_FIELDS),
            "log_redaction_policy_path": str(log_policy_path),
            "log_policy_error": log_policy_error,
            "malformed_records": malformed_records,
            "source_violations": source_violations,
            "redaction_violations": redaction_violations,
            "records": [
                {
                    "secret_id": item.get("secret_id"),
                    "accessor_id": item.get("accessor_id"),
                    "access_reason": item.get("access_reason"),
                    "audit_event_id": item.get("audit_event_id"),
                    "accessed_at": item.get("accessed_at"),
                }
                for item in records
                if isinstance(item, dict)
            ],
        },
    )


def verify_telegram_session_security_contract(policy_path=DEFAULT_SECURITY_SESSION_POLICY):
    policy, policy_error = _security_session_policy(policy_path)
    if policy_error:
        return _contract("TelegramSessionSecurityContract", False, "telegram_session_security_missing_malformed_or_unenforced", policy_error)

    sessions = policy.get("telegram_sessions") if isinstance(policy.get("telegram_sessions"), list) else []
    malformed_sessions = []
    source_violations = []
    allowed_auth_states = {"required_before_ingestion", "authenticated", "disabled"}
    for index, session in enumerate(sessions):
        if not isinstance(session, dict):
            malformed_sessions.append({"index": index, "session_id": None, "missing_fields": list(TELEGRAM_SESSION_SECURITY_REQUIRED_FIELDS), "violations": ["telegram_session_not_object"]})
            continue
        missing = _missing_required_fields(session, TELEGRAM_SESSION_SECURITY_REQUIRED_FIELDS)
        violations = []
        if str(session.get("auth_state") or "") not in allowed_auth_states:
            violations.append("auth_state_invalid")
        if not re.match(r"^[0-9a-f]{64}$", str(session.get("device_fingerprint_hash") or "")):
            violations.append("device_fingerprint_hash_must_be_sha256_hex")
        if _parse_iso_ts(session.get("checked_at")) is None:
            violations.append("checked_at_invalid")
        source_violations.extend(
            {"index": index, "session_id": session.get("session_id"), **violation}
            for violation in _policy_source_anchor_violations(session, source_anchor_key="source_anchors")
        )
        source_text, source_error = _read_project_text(session.get("source_file"))
        if source_error:
            source_violations.append({"index": index, "session_id": session.get("session_id"), **source_error})
        else:
            required_runtime_fragments = ["new StringSession(sessionString)", "new TelegramClient(session", "Missing Telegram User API credentials"]
            missing_fragments = [fragment for fragment in required_runtime_fragments if fragment not in source_text]
            if missing_fragments:
                source_violations.append({"index": index, "session_id": session.get("session_id"), "reason": "telegram_runtime_guard_missing", "missing_fragments": missing_fragments})
        if missing or violations:
            malformed_sessions.append({"index": index, "session_id": session.get("session_id"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.security_session_policy.v1"
        and bool(sessions)
        and not malformed_sessions
        and not source_violations
    )
    return _contract(
        "TelegramSessionSecurityContract",
        passed,
        "telegram_session_security_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "session_count": len(sessions),
            "required_fields": list(TELEGRAM_SESSION_SECURITY_REQUIRED_FIELDS),
            "malformed_sessions": malformed_sessions,
            "source_violations": source_violations,
            "sessions": [
                {
                    "session_id": item.get("session_id"),
                    "account_id": item.get("account_id"),
                    "auth_state": item.get("auth_state"),
                    "device_fingerprint_hash": item.get("device_fingerprint_hash"),
                    "checked_at": item.get("checked_at"),
                }
                for item in sessions
                if isinstance(item, dict)
            ],
        },
    )


def _runtime_pipeline_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "runtime_pipeline_policy_not_object"}
    return policy, None


def verify_queue_ack_nack_contract(policy_path=DEFAULT_RUNTIME_PIPELINE_POLICY):
    policy, policy_error = _runtime_pipeline_policy(policy_path)
    if policy_error:
        return _contract("QueueAckNackContract", False, "queue_ack_nack_missing_malformed_or_unenforced", policy_error)

    records = policy.get("queue_ack_nack") if isinstance(policy.get("queue_ack_nack"), list) else []
    malformed_records = []
    source_violations = []
    duplicate_task_ids = []
    ack_states = set()
    seen_task_ids = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            malformed_records.append({"index": index, "task_id": None, "missing_fields": list(QUEUE_ACK_NACK_REQUIRED_FIELDS), "violations": ["queue_record_not_object"]})
            continue
        task_id = str(record.get("task_id") or "")
        missing = _missing_required_fields(record, QUEUE_ACK_NACK_REQUIRED_FIELDS)
        violations = []
        if task_id in seen_task_ids:
            duplicate_task_ids.append(task_id)
        if task_id:
            seen_task_ids.add(task_id)
        ack_state = str(record.get("ack_state") or "").strip().lower()
        ack_states.add(ack_state)
        if ack_state not in {"acked", "nacked", "retrying", "pending"}:
            violations.append("ack_state_invalid")
        if ack_state == "nacked" and str(record.get("nack_reason") or "").strip().lower() in {"", "none", "null"}:
            violations.append("nack_reason_required_for_nacked")
        if ack_state != "nacked" and str(record.get("nack_reason") or "").strip().lower() == "":
            violations.append("nack_reason_required")
        if _parse_iso_ts(record.get("recorded_at")) is None:
            violations.append("recorded_at_invalid")
        source_violations.extend(
            {"index": index, "task_id": task_id or None, **violation}
            for violation in _policy_source_anchor_violations(record)
        )
        if missing or violations:
            malformed_records.append({"index": index, "task_id": task_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_pipeline_policy.v1"
        and bool(records)
        and {"acked", "nacked"}.issubset(ack_states)
        and not malformed_records
        and not duplicate_task_ids
        and not source_violations
    )
    return _contract(
        "QueueAckNackContract",
        passed,
        "queue_ack_nack_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "record_count": len(records),
            "ack_states": sorted(ack_states),
            "required_fields": list(QUEUE_ACK_NACK_REQUIRED_FIELDS),
            "duplicate_task_ids": sorted(str(item) for item in duplicate_task_ids),
            "malformed_records": malformed_records,
            "source_violations": source_violations,
        },
    )


def verify_pipeline_progress_invariant(policy_path=DEFAULT_RUNTIME_PIPELINE_POLICY):
    policy, policy_error = _runtime_pipeline_policy(policy_path)
    if policy_error:
        return _contract("PipelineProgressInvariant", False, "pipeline_progress_missing_malformed_or_unenforced", policy_error)

    records = policy.get("pipeline_progress") if isinstance(policy.get("pipeline_progress"), list) else []
    malformed_records = []
    source_violations = []
    duplicate_stage_keys = []
    seen_stage_keys = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            malformed_records.append({"index": index, "pipeline_id": None, "missing_fields": list(PIPELINE_PROGRESS_REQUIRED_FIELDS), "violations": ["pipeline_progress_record_not_object"]})
            continue
        pipeline_id = str(record.get("pipeline_id") or "")
        stage_name = str(record.get("stage_name") or "")
        stage_key = f"{pipeline_id}:{stage_name}"
        missing = _missing_required_fields(record, PIPELINE_PROGRESS_REQUIRED_FIELDS)
        violations = []
        if stage_key in seen_stage_keys:
            duplicate_stage_keys.append(stage_key)
        if pipeline_id and stage_name:
            seen_stage_keys.add(stage_key)
        max_stall_ms = record.get("max_stall_ms")
        if isinstance(max_stall_ms, bool) or not isinstance(max_stall_ms, int) or max_stall_ms <= 0:
            violations.append("max_stall_ms_positive_int_required")
        if _parse_iso_ts(record.get("last_progress_at")) is None:
            violations.append("last_progress_at_invalid")
        if str(record.get("stall_action") or "") not in {"emit_progress_warning_and_classify_cause", "strict_smoke_fails_with_blocking_reasons"}:
            violations.append("stall_action_invalid")
        source_violations.extend(
            {"index": index, "pipeline_id": pipeline_id or None, "stage_name": stage_name or None, **violation}
            for violation in _policy_source_anchor_violations(record, source_anchor_key="source_anchors")
        )
        if missing or violations:
            malformed_records.append(
                {
                    "index": index,
                    "pipeline_id": pipeline_id or None,
                    "stage_name": stage_name or None,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_pipeline_policy.v1"
        and len(records) >= 2
        and not malformed_records
        and not duplicate_stage_keys
        and not source_violations
    )
    return _contract(
        "PipelineProgressInvariant",
        passed,
        "pipeline_progress_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "record_count": len(records),
            "required_fields": list(PIPELINE_PROGRESS_REQUIRED_FIELDS),
            "duplicate_stage_keys": sorted(str(item) for item in duplicate_stage_keys),
            "malformed_records": malformed_records,
            "source_violations": source_violations,
        },
    )


def verify_thread_pool_isolation_contract(policy_path=DEFAULT_RUNTIME_PIPELINE_POLICY):
    policy, policy_error = _runtime_pipeline_policy(policy_path)
    if policy_error:
        return _contract("ThreadPoolIsolationContract", False, "thread_pool_isolation_missing_malformed_or_unenforced", policy_error)

    pools = policy.get("thread_pools") if isinstance(policy.get("thread_pools"), list) else []
    malformed_pools = []
    source_violations = []
    duplicate_pool_names = []
    seen_pool_names = set()
    for index, pool in enumerate(pools):
        if not isinstance(pool, dict):
            malformed_pools.append({"index": index, "pool_name": None, "missing_fields": list(THREAD_POOL_ISOLATION_REQUIRED_FIELDS), "violations": ["thread_pool_record_not_object"]})
            continue
        pool_name = str(pool.get("pool_name") or "")
        missing = _missing_required_fields(pool, THREAD_POOL_ISOLATION_REQUIRED_FIELDS)
        violations = []
        if pool_name in seen_pool_names:
            duplicate_pool_names.append(pool_name)
        if pool_name:
            seen_pool_names.add(pool_name)
        for field in ("max_workers", "reserved_capacity"):
            value = pool.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                violations.append(f"{field}_positive_int_required")
        if isinstance(pool.get("max_workers"), int) and isinstance(pool.get("reserved_capacity"), int):
            if pool.get("reserved_capacity") > pool.get("max_workers"):
                violations.append("reserved_capacity_gt_max_workers")
        if _parse_iso_ts(pool.get("checked_at")) is None:
            violations.append("checked_at_invalid")
        source_violations.extend(
            {"index": index, "pool_name": pool_name or None, **violation}
            for violation in _policy_source_anchor_violations(pool)
        )
        if missing or violations:
            malformed_pools.append({"index": index, "pool_name": pool_name or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.runtime_pipeline_policy.v1"
        and len(pools) >= 3
        and not malformed_pools
        and not duplicate_pool_names
        and not source_violations
    )
    return _contract(
        "ThreadPoolIsolationContract",
        passed,
        "thread_pool_isolation_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "pool_count": len(pools),
            "pool_names": sorted(str(pool.get("pool_name")) for pool in pools if isinstance(pool, dict) and pool.get("pool_name")),
            "required_fields": list(THREAD_POOL_ISOLATION_REQUIRED_FIELDS),
            "duplicate_pool_names": sorted(str(item) for item in duplicate_pool_names),
            "malformed_pools": malformed_pools,
            "source_violations": source_violations,
        },
    )


def _ci_spec_generated_policy(policy_path):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return None, {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        return None, {"policy_path": str(policy_path), "error": "ci_spec_generated_policy_not_object"}
    return policy, None


def _file_hash_record(raw_path):
    resolved = _resolve_project_file(raw_path)
    if not resolved or not resolved.exists():
        return None, {"source_file": str(raw_path), "reason": "source_missing"}
    return _sha256_file(resolved), None


def verify_cicd_merge_gate_contract(
    policy_path=DEFAULT_CI_SPEC_GENERATED_POLICY,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
):
    policy, policy_error = _ci_spec_generated_policy(policy_path)
    if policy_error:
        return _contract("CICDMergeGateContract", False, "cicd_merge_gate_missing_malformed_or_unenforced", policy_error)

    try:
        spec_report = validate_all(manifest_path=manifest_path, catalog_path=catalog_path, registry_path=registry_path)
        spec_error = None
    except Exception as exc:
        spec_report = {}
        spec_error = {"error": str(exc)}

    spec_hash = spec_report.get("spec_hash")
    gates = policy.get("ci_merge_gates") if isinstance(policy.get("ci_merge_gates"), list) else []
    malformed_gates = []
    workflow_evidence = []
    for index, gate in enumerate(gates):
        if not isinstance(gate, dict):
            malformed_gates.append({"index": index, "merge_gate_id": None, "missing_fields": list(CICD_MERGE_GATE_REQUIRED_FIELDS), "violations": ["gate_not_object"]})
            continue
        missing = _missing_required_fields(gate, CICD_MERGE_GATE_REQUIRED_FIELDS + ("workflow_file",))
        violations = []
        workflow_file = gate.get("workflow_file")
        workflow_text, workflow_error = _read_project_text(workflow_file)
        workflow_hash = None
        if workflow_error:
            violations.append("workflow_file_missing")
        else:
            workflow_hash, _ = _file_hash_record(workflow_file)
        required_checks = gate.get("required_checks") if isinstance(gate.get("required_checks"), list) else []
        missing_checks = [str(check) for check in required_checks if str(check) not in workflow_text]
        if missing_checks:
            violations.append("required_check_missing_from_workflow")
        if gate.get("spec_hash") != spec_hash:
            violations.append("spec_hash_mismatch")
        if gate.get("gate_result") != "pass":
            violations.append("gate_result_not_pass")
        expected_artifact_hash = _sha256_json(
            {
                "workflow_file": workflow_file,
                "workflow_sha256": workflow_hash,
                "required_checks": required_checks,
                "spec_hash": spec_hash,
            }
        )
        if gate.get("artifact_hash") != expected_artifact_hash:
            violations.append("artifact_hash_mismatch")
        workflow_evidence.append(
            {
                "merge_gate_id": gate.get("merge_gate_id"),
                "workflow_file": workflow_file,
                "workflow_sha256": workflow_hash,
                "required_check_count": len(required_checks),
                "missing_checks": missing_checks,
                "artifact_hash": gate.get("artifact_hash"),
                "expected_artifact_hash": expected_artifact_hash,
            }
        )
        if missing or violations:
            malformed_gates.append({"index": index, "merge_gate_id": gate.get("merge_gate_id"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.ci_spec_generated_policy.v1"
        and not spec_error
        and bool(gates)
        and not malformed_gates
    )
    return _contract(
        "CICDMergeGateContract",
        passed,
        "cicd_merge_gate_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "spec_hash": spec_hash,
            "spec_error": spec_error,
            "gate_count": len(gates),
            "workflow_evidence": workflow_evidence,
            "malformed_gates": malformed_gates,
        },
    )


def verify_generated_client_contract(policy_path=DEFAULT_CI_SPEC_GENERATED_POLICY, catalog_path=CATALOG_PATH):
    policy, policy_error = _ci_spec_generated_policy(policy_path)
    if policy_error:
        return _contract("GeneratedClientContract", False, "generated_client_missing_malformed_or_stale", policy_error)

    try:
        catalog = _load_json(catalog_path)
        catalog_error = None
    except Exception as exc:
        catalog = {}
        catalog_error = {"catalog_path": str(catalog_path), "error": str(exc)}

    catalog_contracts = catalog.get("contracts") if isinstance(catalog, dict) else {}
    catalog_contract_ids = sorted((catalog_contracts or {}).keys())
    source_schema_hash = _sha256_json(catalog) if not catalog_error else None
    clients = policy.get("generated_clients") if isinstance(policy.get("generated_clients"), list) else []
    malformed_clients = []
    client_evidence = []
    for index, client in enumerate(clients):
        if not isinstance(client, dict):
            malformed_clients.append({"index": index, "client_name": None, "missing_fields": list(GENERATED_CLIENT_REQUIRED_FIELDS), "violations": ["client_not_object"]})
            continue
        missing = _missing_required_fields(
            client,
            GENERATED_CLIENT_REQUIRED_FIELDS + ("source_schema_file", "generated_artifact_file", "generator_script"),
        )
        violations = []
        artifact = None
        artifact_hash = None
        artifact_embedded_hash = None
        artifact_expected_embedded_hash = None
        artifact_contract_ids = []
        try:
            artifact = _load_json(_resolve_project_file(client.get("generated_artifact_file")))
        except Exception as exc:
            violations.append("generated_artifact_missing_or_invalid")
            artifact_error = str(exc)
        else:
            artifact_error = None
            artifact_hash = _sha256_json(artifact)
            artifact_embedded_hash = artifact.get("generated_artifact_hash") if isinstance(artifact, dict) else None
            artifact_expected_embedded_hash = _sha256_json(
                {key: value for key, value in artifact.items() if key != "generated_artifact_hash"}
            ) if isinstance(artifact, dict) else None
            artifact_contract_ids = sorted(str(row.get("contract_id")) for row in (artifact.get("contracts") or []) if isinstance(row, dict) and row.get("contract_id"))
        generator_hash, generator_error = _file_hash_record(client.get("generator_script"))
        if generator_error:
            violations.append("generator_script_missing")
        if client.get("source_schema_hash") != source_schema_hash:
            violations.append("source_schema_hash_mismatch")
        if client.get("generated_artifact_hash") != artifact_hash:
            violations.append("generated_artifact_hash_mismatch")
        if artifact_embedded_hash != artifact_expected_embedded_hash:
            violations.append("generated_artifact_embedded_hash_mismatch")
        if artifact and artifact.get("source_schema_hash") != source_schema_hash:
            violations.append("artifact_source_schema_hash_mismatch")
        if artifact and artifact.get("contract_count") != len(catalog_contract_ids):
            violations.append("artifact_contract_count_mismatch")
        if artifact and artifact_contract_ids != catalog_contract_ids:
            violations.append("artifact_contract_ids_mismatch")
        if artifact and artifact.get("generation_tool_version") != client.get("generation_tool_version"):
            violations.append("generation_tool_version_mismatch")
        if _parse_iso_ts(client.get("checked_at")) is None:
            violations.append("checked_at_invalid")
        client_evidence.append(
            {
                "client_name": client.get("client_name"),
                "source_schema_file": client.get("source_schema_file"),
                "generated_artifact_file": client.get("generated_artifact_file"),
                "generator_script": client.get("generator_script"),
                "source_schema_hash": client.get("source_schema_hash"),
                "expected_source_schema_hash": source_schema_hash,
                "generated_artifact_hash": client.get("generated_artifact_hash"),
                "expected_generated_artifact_hash": artifact_hash,
                "embedded_generated_artifact_hash": artifact_embedded_hash,
                "expected_embedded_generated_artifact_hash": artifact_expected_embedded_hash,
                "contract_count": len(artifact_contract_ids),
                "generator_sha256": generator_hash,
                "artifact_error": artifact_error,
            }
        )
        if missing or violations:
            malformed_clients.append({"index": index, "client_name": client.get("client_name"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.ci_spec_generated_policy.v1"
        and not catalog_error
        and bool(clients)
        and not malformed_clients
    )
    return _contract(
        "GeneratedClientContract",
        passed,
        "generated_client_missing_malformed_or_stale",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "catalog_path": str(catalog_path),
            "catalog_error": catalog_error,
            "catalog_contract_count": len(catalog_contract_ids),
            "client_count": len(clients),
            "client_evidence": client_evidence,
            "malformed_clients": malformed_clients,
        },
    )


def verify_spec_change_impact_analysis_contract(
    policy_path=DEFAULT_CI_SPEC_GENERATED_POLICY,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
):
    policy, policy_error = _ci_spec_generated_policy(policy_path)
    if policy_error:
        return _contract("SpecChangeImpactAnalysisContract", False, "spec_change_impact_missing_malformed_or_unapproved", policy_error)

    try:
        spec_report = validate_all(manifest_path=manifest_path, catalog_path=catalog_path, registry_path=registry_path)
        catalog = _load_json(catalog_path)
        spec_error = None
    except Exception as exc:
        spec_report = {}
        catalog = {}
        spec_error = {"error": str(exc)}

    spec_hash = spec_report.get("spec_hash")
    catalog_contract_ids = set((catalog.get("contracts") or {}).keys()) if isinstance(catalog, dict) else set()
    required_contracts = {"CICDMergeGateContract", "GeneratedClientContract", "SpecChangeImpactAnalysisContract"}
    allowed_modes = {"observe_only", "shadow", "ultra_tiny", "normal_tiny"}
    impacts = policy.get("spec_change_impacts") if isinstance(policy.get("spec_change_impacts"), list) else []
    malformed_impacts = []
    impact_evidence = []
    policy_file_name = "config/v27-ci-spec-generated-policy.json"
    for index, impact in enumerate(impacts):
        if not isinstance(impact, dict):
            malformed_impacts.append({"index": index, "spec_change_id": None, "missing_fields": list(SPEC_CHANGE_IMPACT_REQUIRED_FIELDS), "violations": ["impact_not_object"]})
            continue
        missing = _missing_required_fields(impact, SPEC_CHANGE_IMPACT_REQUIRED_FIELDS + ("spec_hash", "source_files", "source_hashes"))
        violations = []
        affected_contracts = [str(item) for item in (impact.get("affected_contracts") or [])]
        affected_modes = [str(item) for item in (impact.get("affected_modes") or [])]
        source_files = [str(item) for item in (impact.get("source_files") or [])]
        if not required_contracts.issubset(set(affected_contracts)):
            violations.append("required_contracts_not_covered")
        unknown_contracts = sorted(set(affected_contracts) - catalog_contract_ids)
        if unknown_contracts:
            violations.append("unknown_affected_contract")
        unknown_modes = sorted(set(affected_modes) - allowed_modes)
        if unknown_modes:
            violations.append("unknown_affected_mode")
        if _parse_iso_ts(impact.get("approved_at")) is None:
            violations.append("approved_at_invalid")
        if impact.get("spec_hash") != spec_hash:
            violations.append("spec_hash_mismatch")
        if policy_file_name in source_files:
            violations.append("policy_self_reference_not_allowed")
        source_hashes = {}
        source_errors = []
        for source_file in source_files:
            file_hash, source_error = _file_hash_record(source_file)
            if source_error:
                source_errors.append(source_error)
            else:
                source_hashes[source_file] = file_hash
        if source_errors:
            violations.append("source_file_missing")
        if impact.get("source_hashes") != source_hashes:
            violations.append("source_hashes_mismatch")
        expected_impact_hash = _sha256_json(
            {
                "spec_change_id": impact.get("spec_change_id"),
                "affected_contracts": affected_contracts,
                "affected_modes": affected_modes,
                "spec_hash": spec_hash,
                "source_hashes": source_hashes,
            }
        )
        if impact.get("impact_hash") != expected_impact_hash:
            violations.append("impact_hash_mismatch")
        impact_evidence.append(
            {
                "spec_change_id": impact.get("spec_change_id"),
                "affected_contracts": affected_contracts,
                "affected_modes": affected_modes,
                "source_file_count": len(source_files),
                "unknown_contracts": unknown_contracts,
                "unknown_modes": unknown_modes,
                "source_errors": source_errors,
                "impact_hash": impact.get("impact_hash"),
                "expected_impact_hash": expected_impact_hash,
            }
        )
        if missing or violations:
            malformed_impacts.append({"index": index, "spec_change_id": impact.get("spec_change_id"), "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.ci_spec_generated_policy.v1"
        and not spec_error
        and bool(impacts)
        and not malformed_impacts
    )
    return _contract(
        "SpecChangeImpactAnalysisContract",
        passed,
        "spec_change_impact_missing_malformed_or_unapproved",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "spec_hash": spec_hash,
            "spec_error": spec_error,
            "impact_count": len(impacts),
            "impact_evidence": impact_evidence,
            "malformed_impacts": malformed_impacts,
        },
    )


def verify_service_readiness_probe_contract(policy_path=DEFAULT_SERVICE_READINESS_PROBES):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("ServiceReadinessProbeContract", False, "service_readiness_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("ServiceReadinessProbeContract", False, "service_readiness_policy_not_object", {"policy_path": str(policy_path)})

    probes = policy.get("probes") if isinstance(policy.get("probes"), list) else []
    required_probe_ids = [str(item) for item in (policy.get("required_probe_ids") or [])]
    required_fields = [str(item) for item in (policy.get("required_fields") or [])]
    checked_at = _utc_now_iso()

    schema_violations = []
    if policy.get("schema_version") != "v2.7.0.service_readiness_probes.v1":
        schema_violations.append("schema_version_invalid")
    if policy.get("failure_action") != "service_not_ready":
        schema_violations.append("failure_action_must_be_service_not_ready")
    if set(required_fields) != set(SERVICE_READINESS_CONTRACT_FIELDS):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if not required_probe_ids:
        schema_violations.append("required_probe_ids_required")

    malformed_probes = []
    duplicate_probe_ids = []
    source_violations = []
    seen_probe_ids = set()
    probe_ids = set()
    probe_evidence = []
    for index, probe in enumerate(probes):
        if not isinstance(probe, dict):
            malformed_probes.append({"index": index, "probe_id": None, "missing_fields": list(SERVICE_READINESS_PROBE_REQUIRED_FIELDS), "violations": ["probe_not_object"]})
            continue

        service_name = str(probe.get("service_name") or "")
        probe_id = str(probe.get("probe_id") or "")
        probe_ids.add(probe_id)
        missing = _missing_required_fields(probe, SERVICE_READINESS_PROBE_REQUIRED_FIELDS)
        violations = []
        if probe_id in seen_probe_ids:
            duplicate_probe_ids.append(probe_id)
        if probe_id:
            seen_probe_ids.add(probe_id)
        if str(probe.get("health_status") or "") not in {"ready", "degraded", "blocked"}:
            violations.append("health_status_invalid")
        dependency_status = probe.get("dependency_status")
        if not isinstance(dependency_status, dict) or not dependency_status:
            violations.append("dependency_status_required")
        dependency_anchors = [str(item) for item in (probe.get("dependency_anchors") or [])]
        if not dependency_anchors:
            violations.append("dependency_anchors_required")

        source_text, source_error = _read_project_text(probe.get("source_file"))
        if source_error:
            source_violations.append({"probe_id": probe_id, **source_error})
        else:
            source_anchor = str(probe.get("source_anchor") or "")
            if source_anchor and source_anchor not in source_text:
                source_violations.append({"probe_id": probe_id, "reason": "source_anchor_missing", "source_anchor": source_anchor})
            missing_dependency_anchors = [anchor for anchor in dependency_anchors if anchor not in source_text]
            if missing_dependency_anchors:
                source_violations.append({"probe_id": probe_id, "reason": "dependency_anchor_missing", "missing_dependency_anchors": missing_dependency_anchors})
            endpoint = probe.get("endpoint")
            if endpoint:
                source_lines = source_text.splitlines()
                route_block = _dashboard_route_block(source_lines, str(endpoint))
                if not route_block:
                    source_violations.append({"probe_id": probe_id, "endpoint": endpoint, "reason": "endpoint_route_missing"})
                elif source_anchor and source_anchor not in route_block:
                    source_violations.append({"probe_id": probe_id, "endpoint": endpoint, "reason": "source_anchor_missing_in_route", "source_anchor": source_anchor})

        probe_evidence.append(
            {
                "service_name": service_name,
                "probe_id": probe_id,
                "health_status": probe.get("health_status"),
                "dependency_status": dependency_status,
                "checked_at": checked_at,
            }
        )
        if missing or violations:
            malformed_probes.append({"index": index, "service_name": service_name or None, "probe_id": probe_id or None, "missing_fields": missing, "violations": violations})

    missing_required_probe_ids = sorted(set(required_probe_ids) - probe_ids)
    unexpected_probe_ids = sorted(probe_ids - set(required_probe_ids))
    passed = (
        not schema_violations
        and bool(probes)
        and not malformed_probes
        and not duplicate_probe_ids
        and not source_violations
        and not missing_required_probe_ids
        and not unexpected_probe_ids
        and all(item.get("health_status") == "ready" for item in probe_evidence)
    )
    return _contract(
        "ServiceReadinessProbeContract",
        passed,
        "service_readiness_probe_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "required_fields": required_fields,
            "probe_count": len(probes),
            "required_probe_ids": required_probe_ids,
            "probes": probe_evidence,
            "schema_violations": schema_violations,
            "malformed_probes": malformed_probes,
            "duplicate_probe_ids": sorted(str(item) for item in duplicate_probe_ids),
            "source_violations": source_violations,
            "missing_required_probe_ids": missing_required_probe_ids,
            "unexpected_probe_ids": unexpected_probe_ids,
        },
    )


def verify_dashboard_action_separation_contract(
    policy_path=DEFAULT_DASHBOARD_ACTION_SEPARATION_POLICY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
    write_path_registry_path=DEFAULT_WRITE_PATH_REGISTRY,
):
    try:
        policy = _load_json(policy_path)
        access_policy = _load_json(access_control_policy_path)
        write_registry = _load_json(write_path_registry_path)
    except Exception as exc:
        return _contract("DashboardActionSeparationContract", False, "dashboard_action_separation_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict) or not isinstance(access_policy, dict) or not isinstance(write_registry, dict):
        return _contract(
            "DashboardActionSeparationContract",
            False,
            "dashboard_action_separation_policy_not_object",
            {
                "policy_path": str(policy_path),
                "access_control_policy_path": str(access_control_policy_path),
                "write_path_registry_path": str(write_path_registry_path),
            },
        )

    source_file = policy.get("source_file") or access_policy.get("source_file")
    lines, source_error = _source_lines(source_file)
    if source_error:
        return _contract(
            "DashboardActionSeparationContract",
            False,
            "dashboard_action_separation_missing_malformed_or_unenforced",
            {"policy_path": str(policy_path), **source_error},
        )

    routes = _extract_dashboard_routes(lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    defaults = access_policy.get("protected_defaults") if isinstance(access_policy.get("protected_defaults"), dict) else {}
    overrides = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }
    danger_requires_post = set(access_policy.get("danger_levels_requiring_post") or [])
    write_paths = {
        str(item.get("write_path_id")): item
        for item in (write_registry.get("write_paths") or [])
        if isinstance(item, dict) and item.get("write_path_id")
    }

    required_fields = [str(item) for item in (policy.get("required_fields") or [])]
    required_action_ids = [str(item) for item in (policy.get("required_action_ids") or [])]
    actions = policy.get("actions") if isinstance(policy.get("actions"), list) else []
    schema_violations = []
    if policy.get("schema_version") != "v2.7.0.dashboard_action_separation.v1":
        schema_violations.append("schema_version_invalid")
    if policy.get("failure_action") != "dashboard_mutation_blocked":
        schema_violations.append("failure_action_must_be_dashboard_mutation_blocked")
    if set(required_fields) != set(DASHBOARD_ACTION_SEPARATION_REQUIRED_FIELDS):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if not required_action_ids:
        schema_violations.append("required_action_ids_required")

    malformed_actions = []
    route_violations = []
    duplicate_action_ids = []
    seen_action_ids = set()
    action_ids = set()
    action_evidence = []
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            malformed_actions.append({"index": index, "action_id": None, "missing_fields": list(DASHBOARD_ACTION_SEPARATION_REQUIRED_FIELDS), "violations": ["action_not_object"]})
            continue

        action_id = str(action.get("action_id") or "")
        view_route = str(action.get("view_route") or "")
        mutation_route = str(action.get("mutation_route") or "")
        action_ids.add(action_id)
        missing = _missing_required_fields(action, DASHBOARD_ACTION_SEPARATION_REQUIRED_FIELDS)
        violations = []
        if action_id in seen_action_ids:
            duplicate_action_ids.append(action_id)
        if action_id:
            seen_action_ids.add(action_id)
        if action.get("separation_enforced") is not True:
            violations.append("separation_enforced_true_required")
        if action.get("audit_required") is not True:
            violations.append("audit_required_true_required")
        if view_route and mutation_route and view_route == mutation_route:
            violations.append("view_route_must_differ_from_mutation_route")

        view = route_by_endpoint.get(view_route)
        mutation = route_by_endpoint.get(mutation_route)
        view_policy = _resolve_access_policy(view_route, defaults, overrides)
        mutation_policy = _resolve_access_policy(mutation_route, defaults, overrides)
        view_anchor = str(action.get("view_anchor") or "")
        mutation_anchor = str(action.get("mutation_anchor") or "")
        view_block = _dashboard_route_block(lines, view_route) if view_route else ""
        mutation_block = _dashboard_route_block(lines, mutation_route) if mutation_route else ""
        if not view:
            route_violations.append({"action_id": action_id, "route": view_route, "reason": "view_route_missing"})
        else:
            if not view.get("has_check_auth"):
                route_violations.append({"action_id": action_id, "route": view_route, "reason": "view_route_auth_missing"})
            if view.get("has_post_guard") or view.get("has_audit_event") or view.get("mutation_markers"):
                route_violations.append({"action_id": action_id, "route": view_route, "reason": "view_route_contains_mutation_surface"})
            if view_anchor and view_anchor not in view_block:
                route_violations.append({"action_id": action_id, "route": view_route, "reason": "view_anchor_missing", "view_anchor": view_anchor})
        if not mutation:
            route_violations.append({"action_id": action_id, "route": mutation_route, "reason": "mutation_route_missing"})
        else:
            if not mutation.get("has_check_auth"):
                route_violations.append({"action_id": action_id, "route": mutation_route, "reason": "mutation_route_auth_missing"})
            if not mutation.get("has_post_guard"):
                route_violations.append({"action_id": action_id, "route": mutation_route, "reason": "mutation_route_post_guard_missing"})
            if not mutation.get("has_audit_event"):
                route_violations.append({"action_id": action_id, "route": mutation_route, "reason": "mutation_route_audit_missing"})
            if mutation_anchor and mutation_anchor not in mutation_block:
                route_violations.append({"action_id": action_id, "route": mutation_route, "reason": "mutation_anchor_missing", "mutation_anchor": mutation_anchor})

        view_danger = str(view_policy.get("danger_level") or "")
        mutation_danger = str(mutation_policy.get("danger_level") or "")
        if view_danger in danger_requires_post or view_policy.get("audit_log_required") is True:
            route_violations.append({"action_id": action_id, "route": view_route, "reason": "view_policy_must_not_be_mutation_policy", "danger_level": view_danger})
        if (
            mutation_policy.get("audit_log_required") is not True
            or mutation_danger not in danger_requires_post
            or mutation_policy.get("method_guard_required") is not True
            or "POST" not in [str(value).upper() for value in (mutation_policy.get("allowed_methods") or [])]
        ):
            route_violations.append(
                {
                    "action_id": action_id,
                    "route": mutation_route,
                    "reason": "mutation_policy_missing_post_audit_or_danger",
                    "audit_log_required": mutation_policy.get("audit_log_required"),
                    "danger_level": mutation_danger,
                    "method_guard_required": mutation_policy.get("method_guard_required"),
                    "allowed_methods": mutation_policy.get("allowed_methods"),
                }
            )

        write_path_ids = [str(item) for item in (action.get("mutation_write_path_ids") or [])]
        if not write_path_ids:
            violations.append("mutation_write_path_ids_required")
        for write_path_id in write_path_ids:
            write_path = write_paths.get(write_path_id)
            method, endpoint = _entry_point_endpoint(write_path.get("entry_point") if isinstance(write_path, dict) else None)
            if not write_path:
                route_violations.append({"action_id": action_id, "write_path_id": write_path_id, "reason": "mutation_write_path_missing"})
            elif method != "POST" or endpoint != mutation_route:
                route_violations.append(
                    {
                        "action_id": action_id,
                        "write_path_id": write_path_id,
                        "reason": "mutation_write_path_route_mismatch",
                        "entry_point": write_path.get("entry_point"),
                        "mutation_route": mutation_route,
                    }
                )

        action_evidence.append(
            {
                "action_id": action_id,
                "view_route": view_route,
                "mutation_route": mutation_route,
                "separation_enforced": action.get("separation_enforced"),
                "audit_required": action.get("audit_required"),
            }
        )
        if missing or violations:
            malformed_actions.append({"index": index, "action_id": action_id or None, "missing_fields": missing, "violations": violations})

    missing_required_action_ids = sorted(set(required_action_ids) - action_ids)
    unexpected_action_ids = sorted(action_ids - set(required_action_ids))
    passed = (
        not schema_violations
        and bool(actions)
        and not malformed_actions
        and not duplicate_action_ids
        and not route_violations
        and not missing_required_action_ids
        and not unexpected_action_ids
        and all(action.get("separation_enforced") is True and action.get("audit_required") is True for action in action_evidence)
    )
    return _contract(
        "DashboardActionSeparationContract",
        passed,
        "dashboard_action_separation_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "access_control_policy_path": str(access_control_policy_path),
            "write_path_registry_path": str(write_path_registry_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "required_fields": required_fields,
            "action_count": len(actions),
            "required_action_ids": required_action_ids,
            "actions": action_evidence,
            "schema_violations": schema_violations,
            "malformed_actions": malformed_actions,
            "duplicate_action_ids": sorted(str(item) for item in duplicate_action_ids),
            "route_violations": route_violations,
            "missing_required_action_ids": missing_required_action_ids,
            "unexpected_action_ids": unexpected_action_ids,
        },
    )


def _numeric_precision_quantize(value, *, scale, rounding_mode):
    decimal_value = Decimal(str(value))
    quant = Decimal("1").scaleb(-int(scale))
    return format(decimal_value.quantize(quant, rounding=NUMERIC_PRECISION_ROUNDING[rounding_mode]), "f")


def verify_numeric_precision_policy(policy_path=DEFAULT_NUMERIC_PRECISION_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("NumericPrecisionContract", False, "numeric_precision_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("NumericPrecisionContract", False, "numeric_precision_policy_not_object", {"policy_path": str(policy_path)})

    units = policy.get("units") if isinstance(policy.get("units"), list) else []
    malformed_units = []
    duplicate_units = []
    unit_records = {}
    seen_units = set()
    for index, unit in enumerate(units):
        if not isinstance(unit, dict):
            malformed_units.append({"index": index, "unit": None, "missing_fields": list(NUMERIC_PRECISION_REQUIRED_FIELDS), "violations": ["unit_not_object"]})
            continue
        unit_id = str(unit.get("unit") or "")
        missing = _missing_required_fields(unit, NUMERIC_PRECISION_REQUIRED_FIELDS)
        violations = []
        if unit_id in seen_units:
            duplicate_units.append(unit_id)
        if unit_id:
            seen_units.add(unit_id)
            unit_records[unit_id] = unit
        if unit_id and not re.match(r"^[a-z][a-z0-9_]*$", unit_id):
            violations.append("unit_must_be_lower_snake_case")
        scale = unit.get("decimal_scale")
        if isinstance(scale, bool) or not isinstance(scale, int) or scale < 0 or scale > 18:
            violations.append("decimal_scale_must_be_integer_0_to_18")
        rounding_mode = str(unit.get("rounding_mode") or "")
        if rounding_mode not in NUMERIC_PRECISION_ROUNDING:
            violations.append("rounding_mode_not_allowed")
        overflow_policy = str(unit.get("overflow_policy") or "")
        if overflow_policy not in NUMERIC_PRECISION_OVERFLOW_POLICIES:
            violations.append("overflow_policy_not_allowed")
        for bound in ("min_value", "max_value"):
            if bound in unit:
                try:
                    Decimal(str(unit.get(bound)))
                except (InvalidOperation, ValueError):
                    violations.append(f"{bound}_invalid_decimal")
        if "min_value" in unit and "max_value" in unit:
            try:
                if Decimal(str(unit.get("min_value"))) > Decimal(str(unit.get("max_value"))):
                    violations.append("min_value_greater_than_max_value")
            except (InvalidOperation, ValueError):
                pass
        if missing or violations:
            malformed_units.append({"index": index, "unit": unit_id or None, "missing_fields": missing, "violations": violations})

    missing_required_units = sorted(NUMERIC_PRECISION_REQUIRED_UNITS - set(unit_records))

    sample_cases = policy.get("sample_cases") if isinstance(policy.get("sample_cases"), list) else []
    malformed_sample_cases = []
    sample_results = []
    for index, case in enumerate(sample_cases):
        if not isinstance(case, dict):
            malformed_sample_cases.append({"index": index, "case_id": None, "violations": ["sample_case_not_object"]})
            continue
        case_id = str(case.get("case_id") or f"case_{index}")
        unit_id = str(case.get("unit") or "")
        input_value = case.get("input")
        expected = case.get("expected")
        violations = []
        unit = unit_records.get(unit_id)
        actual = None
        if unit is None:
            violations.append("sample_unit_unknown")
        if not isinstance(input_value, str) or not isinstance(expected, str):
            violations.append("sample_input_and_expected_must_be_strings")
        if unit is not None and not violations:
            try:
                actual = _numeric_precision_quantize(
                    input_value,
                    scale=unit.get("decimal_scale"),
                    rounding_mode=str(unit.get("rounding_mode")),
                )
                if actual != expected:
                    violations.append("sample_expected_mismatch")
            except (InvalidOperation, ValueError, KeyError) as exc:
                violations.append(f"sample_quantize_failed:{type(exc).__name__}")
        result = {
            "case_id": case_id,
            "unit": unit_id or None,
            "input": input_value,
            "expected": expected,
            "actual": actual,
            "ok": not violations,
        }
        sample_results.append(result)
        if violations:
            malformed_sample_cases.append({"index": index, "case_id": case_id, "violations": violations, "result": result})

    source_files = policy.get("source_files") if isinstance(policy.get("source_files"), list) else []
    source_checks = []
    source_errors = []
    for index, source in enumerate(source_files):
        if not isinstance(source, dict):
            source_errors.append({"index": index, "source_file": None, "reason": "source_record_not_object"})
            continue
        source_file = source.get("source_file")
        source_anchor = source.get("source_anchor")
        required_patterns = source.get("required_patterns") if isinstance(source.get("required_patterns"), list) else []
        text, error = _read_project_text(source_file)
        if error:
            source_errors.append({"index": index, **error})
            continue
        missing_patterns = [str(pattern) for pattern in required_patterns if str(pattern) not in text]
        anchor_present = bool(source_anchor and str(source_anchor) in text)
        check = {
            "source_file": source_file,
            "source_anchor": source_anchor,
            "anchor_present": anchor_present,
            "required_pattern_count": len(required_patterns),
            "missing_patterns": missing_patterns,
        }
        source_checks.append(check)
        if not anchor_present or missing_patterns:
            source_errors.append({"index": index, "source_file": source_file, "reason": "source_anchor_or_pattern_missing", **check})

    passed = (
        policy.get("schema_version") == "v2.7.0.numeric_precision_policy.v1"
        and policy.get("failure_action") == "spec_dirty"
        and bool(units)
        and bool(sample_cases)
        and bool(source_files)
        and not malformed_units
        and not duplicate_units
        and not missing_required_units
        and not malformed_sample_cases
        and not source_errors
    )
    return _contract(
        "NumericPrecisionContract",
        passed,
        "numeric_precision_policy_missing_malformed_or_unverified",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "required_fields": list(NUMERIC_PRECISION_REQUIRED_FIELDS),
            "required_units": sorted(NUMERIC_PRECISION_REQUIRED_UNITS),
            "unit_count": len(units),
            "sample_case_count": len(sample_cases),
            "source_file_count": len(source_files),
            "missing_required_units": missing_required_units,
            "duplicate_units": sorted(str(item) for item in duplicate_units),
            "malformed_units": malformed_units,
            "sample_results": sample_results,
            "malformed_sample_cases": malformed_sample_cases,
            "source_checks": source_checks,
            "source_errors": source_errors,
        },
    )


def verify_spec_consistency(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH):
    try:
        report = validate_all(manifest_path, catalog_path, registry_path)
        catalog = _load_json(catalog_path)
        contract_ids = set((catalog.get("contracts") or {}).keys())
        manifest = _load_json(manifest_path)
        required = set(manifest.get("mvp_blocking_contracts") or []) | set(manifest.get("high_risk_carry_forward_contracts") or [])
        duplicate_sections = len(manifest.get("sections") or []) != len({section.get("section_id") for section in manifest.get("sections") or []})
        missing_required = sorted(required - contract_ids)
        passed = not duplicate_sections and not missing_required
        return _contract(
            "SpecConsistencyLinterContract",
            passed,
            "spec_consistency_linter_failed",
            {
                "spec_hash": report.get("spec_hash"),
                "duplicate_sections": duplicate_sections,
                "missing_required_contracts": missing_required,
                "catalog_contract_count": len(contract_ids),
            },
        )
    except Exception as exc:
        return _contract("SpecConsistencyLinterContract", False, "spec_consistency_linter_exception", {"error": str(exc)})


def verify_paper_mode_safety(env=None, runtime_evidence_path=None):
    passed, reason, evidence = build_paper_mode_safety_boundary(
        env=env or os.environ,
        runtime_evidence_path=runtime_evidence_path,
    )
    return _contract(
        "PaperModeSafetyBoundary",
        passed,
        reason or "paper_mode_safety_unverified",
        evidence,
    )


def verify_chain_config(chain_config_path=DEFAULT_CHAIN_CONFIG, system_config_path=DEFAULT_SYSTEM_CONFIG):
    try:
        chain_config = _load_json(chain_config_path)
        system_config = _load_json(system_config_path)
    except Exception as exc:
        return _contract("ChainConfigContract", False, "chain_config_missing_or_invalid", {"error": str(exc)})
    required = {"chain", "native_unit", "quote_mint", "finality_rule", "address_validator"}
    chains = chain_config.get("chains") or {}
    supported = [str(chain).lower() for chain in system_config.get("supported_chains") or []]
    aliases = {"sol": "solana", "bsc": "bsc", "bnb": "bsc"}
    missing = []
    invalid = {}
    for chain in supported:
        normalized = aliases.get(chain, chain)
        record = chains.get(normalized)
        if not isinstance(record, dict):
            missing.append(normalized)
            continue
        missing_fields = sorted(required - set(record))
        if missing_fields:
            invalid[normalized] = missing_fields
    passed = not missing and not invalid
    return _contract(
        "ChainConfigContract",
        passed,
        "chain_config_incomplete",
        {
            "chain_config_path": str(chain_config_path),
            "supported_chains": supported,
            "missing_chains": sorted(set(missing)),
            "invalid_chains": invalid,
        },
    )


def _csv_channel_names(channels_csv):
    if not Path(channels_csv).exists():
        return []
    with Path(channels_csv).open("r", encoding="utf-8") as fh:
        return [row.get("channel_name") for row in csv.DictReader(fh) if row.get("channel_name")]


def verify_source_registry(source_registry_path=DEFAULT_SOURCE_REGISTRY, channels_csv=DEFAULT_CHANNELS_CSV):
    try:
        registry = _load_json(source_registry_path)
    except Exception as exc:
        return _contract("SourceRegistryContract", False, "source_registry_missing_or_invalid", {"error": str(exc)})
    sources = registry.get("sources") or []
    required = {"telegram_source_id", "telegram_channel_id", "allowed_modes", "source_status"}
    invalid = []
    active = 0
    for idx, source in enumerate(sources):
        missing = sorted(required - set(source))
        allowed_modes = source.get("allowed_modes")
        if missing or not isinstance(allowed_modes, list) or not allowed_modes:
            invalid.append({"index": idx, "missing_fields": missing, "allowed_modes": allowed_modes})
        if str(source.get("source_status") or "").lower() == "active":
            active += 1
    passed = bool(sources) and not invalid and active > 0
    return _contract(
        "SourceRegistryContract",
        passed,
        "source_registry_incomplete",
        {
            "source_registry_path": str(source_registry_path),
            "source_count": len(sources),
            "active_source_count": active,
            "invalid_sources": invalid,
            "channels_csv_count": len(_csv_channel_names(channels_csv)),
        },
    )


def _source_registry_index(source_registry_path):
    registry = _load_json(source_registry_path)
    sources = registry.get("sources") if isinstance(registry, dict) else []
    by_id = {}
    for source in sources if isinstance(sources, list) else []:
        if isinstance(source, dict) and source.get("telegram_source_id"):
            by_id[str(source["telegram_source_id"])] = source
    shadow_source_ids = sorted(
        source_id
        for source_id, source in by_id.items()
        if str(source.get("source_status") or "").lower() == "active"
        and "shadow" in (source.get("allowed_modes") or [])
    )
    return by_id, shadow_source_ids


def _sha256_hex_like(value):
    return bool(re.fullmatch(r"[0-9a-f]{64}", str(value or "")))


def _confidence(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0 or parsed > 1:
        return None
    return parsed


def _source_parser_policy_base(policy_path, source_registry_path, channels_csv):
    try:
        policy = _load_json(policy_path)
        policy_error = None
    except Exception as exc:
        policy = {}
        policy_error = {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        policy = {}
        policy_error = {"policy_path": str(policy_path), "error": "source_parser_auth_policy_not_object"}

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    try:
        source_by_id, shadow_source_ids = _source_registry_index(source_registry_path)
        registry_error = None
    except Exception as exc:
        source_by_id, shadow_source_ids = {}, []
        registry_error = {"source_registry_path": str(source_registry_path), "error": str(exc)}
    base = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "source_registry_path": str(source_registry_path),
        "channels_csv_count": len(_csv_channel_names(channels_csv)),
        "source_anchor_violations": source_anchor_violations,
        "source_errors": source_errors,
        "registry_error": registry_error,
        "shadow_source_ids": shadow_source_ids,
    }
    base_failed = bool(policy_error or source_anchor_violations or source_errors or registry_error)
    if policy_error:
        base["policy_error"] = policy_error
    return policy, source_by_id, shadow_source_ids, base, base_failed


def _source_parser_contract(contract_id, passed, reason, base_evidence, evidence):
    return _contract(contract_id, passed, reason, {**base_evidence, **evidence})


def _parser_ambiguity_evidence(records):
    malformed = []
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, PARSER_AMBIGUITY_REQUIRED_FIELDS)
        violations = []
        candidate_anchors = item.get("candidate_anchors") if isinstance(item, dict) else None
        selected_anchor = item.get("selected_anchor") if isinstance(item, dict) else None
        if not isinstance(candidate_anchors, list) or len(candidate_anchors) < 2:
            violations.append("candidate_anchors_need_two_or_more")
        elif selected_anchor not in candidate_anchors:
            violations.append("selected_anchor_not_in_candidates")
        if missing or violations:
            malformed.append({"index": index, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "ambiguity_case_count": len(records) if isinstance(records, list) else 0,
        "malformed_ambiguity_cases": malformed,
    }


def _parser_canary_evidence(corpus):
    if not isinstance(corpus, dict):
        corpus = {}
    cases = corpus.get("cases") if isinstance(corpus.get("cases"), list) else []
    corpus_record = {
        "corpus_id": corpus.get("corpus_id"),
        "parser_version": corpus.get("parser_version"),
        "canary_case_count": len(cases),
        "failure_count": corpus.get("failure_count"),
        "checked_at": corpus.get("checked_at"),
    }
    missing = _missing_required_fields(corpus_record, PARSER_CANARY_CORPUS_REQUIRED_FIELDS)
    malformed = []
    allowed_signal_types = {"NEW_TRENDING", "ATH"}
    for index, item in enumerate(cases):
        required = ("message_id", "raw_message_hash", "candidate_anchors", "selected_anchor", "ambiguity_reason", "expected_signal_type", "expected_chain")
        item_missing = _missing_required_fields(item, required)
        violations = []
        candidate_anchors = item.get("candidate_anchors") if isinstance(item, dict) else None
        selected_anchor = item.get("selected_anchor") if isinstance(item, dict) else None
        if not _sha256_hex_like(item.get("raw_message_hash") if isinstance(item, dict) else None):
            violations.append("raw_message_hash_not_sha256")
        if not isinstance(candidate_anchors, list) or selected_anchor not in candidate_anchors:
            violations.append("selected_anchor_not_in_candidates")
        if item.get("expected_signal_type") not in allowed_signal_types:
            violations.append("unexpected_signal_type")
        if item_missing or violations:
            malformed.append({"index": index, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": item_missing, "violations": violations})
    return {
        **corpus_record,
        "missing_corpus_fields": missing,
        "checked_at_valid": _parse_iso_ts(corpus.get("checked_at")) is not None,
        "malformed_canary_cases": malformed,
    }


def _forwarded_message_evidence(records, source_by_id):
    malformed = []
    accepted_registered = 0
    quarantined_unknown = 0
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, TELEGRAM_FORWARDED_MESSAGE_REQUIRED_FIELDS)
        violations = []
        forwarded_from = str(item.get("forwarded_from") or "") if isinstance(item, dict) else ""
        action = item.get("action") if isinstance(item, dict) else None
        known_source = forwarded_from in source_by_id
        if action not in {"accept_with_source_attribution", "message_quarantined", "source_quarantined"}:
            violations.append("unknown_forwarded_action")
        if known_source and action == "accept_with_source_attribution":
            accepted_registered += 1
        if not known_source and action in {"message_quarantined", "source_quarantined"}:
            quarantined_unknown += 1
        if not known_source and action == "accept_with_source_attribution":
            violations.append("unknown_source_accepted")
        if missing or violations:
            malformed.append({"index": index, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "forwarded_policy_count": len(records) if isinstance(records, list) else 0,
        "accepted_registered_forwarded_count": accepted_registered,
        "quarantined_unknown_forwarded_count": quarantined_unknown,
        "malformed_forwarded_policies": malformed,
    }


def _premium_source_access_evidence(records, source_by_id, shadow_source_ids):
    malformed = []
    probed = set()
    allowed_auth_states = {"credential_required_and_cache_supported", "authenticated", "cache_available"}
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, PREMIUM_SOURCE_ACCESS_REQUIRED_FIELDS)
        violations = []
        source_id = str(item.get("source_id") or "") if isinstance(item, dict) else ""
        if source_id not in source_by_id:
            violations.append("unknown_source_id")
        else:
            probed.add(source_id)
        if item.get("auth_state") not in allowed_auth_states:
            violations.append("unsupported_auth_state")
        if _parse_iso_ts(item.get("last_success_at") if isinstance(item, dict) else None) is None:
            violations.append("last_success_at_invalid")
        if item.get("failure_action") != "source_access_degraded":
            violations.append("unexpected_failure_action")
        if missing or violations:
            malformed.append({"index": index, "source_id": source_id, "missing_fields": missing, "violations": violations})
    missing_probe_source_ids = sorted(set(shadow_source_ids) - probed)
    return {
        "access_probe_count": len(records) if isinstance(records, list) else 0,
        "missing_probe_source_ids": missing_probe_source_ids,
        "malformed_access_probes": malformed,
    }


def _source_authenticity_evidence(records, source_by_id, shadow_source_ids):
    malformed = []
    verified = set()
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, SOURCE_AUTHENTICITY_REQUIRED_FIELDS)
        violations = []
        source_id = str(item.get("source_id") or "") if isinstance(item, dict) else ""
        source = source_by_id.get(source_id)
        if not source:
            violations.append("unknown_source_id")
        elif str(item.get("channel_id")) != str(source.get("telegram_channel_id")):
            violations.append("channel_id_mismatch")
        if item.get("authenticity_status") not in {"verified", "registered"}:
            violations.append("authenticity_status_not_verified")
        if not _sha256_hex_like(item.get("evidence_hash") if isinstance(item, dict) else None):
            violations.append("evidence_hash_not_sha256")
        if source and not violations:
            verified.add(source_id)
        if missing or violations:
            malformed.append({"index": index, "source_id": source_id, "missing_fields": missing, "violations": violations})
    missing_authenticity_source_ids = sorted(set(shadow_source_ids) - verified)
    return {
        "authenticity_check_count": len(records) if isinstance(records, list) else 0,
        "missing_authenticity_source_ids": missing_authenticity_source_ids,
        "malformed_authenticity_checks": malformed,
    }


def _confusables_evidence(records):
    malformed = []
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, PARSER_CONFUSABLES_REQUIRED_FIELDS)
        violations = []
        if item.get("policy_action") not in {"parser_output_quarantined", "manual_review"}:
            violations.append("unexpected_policy_action")
        if missing or violations:
            malformed.append({"index": index, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "confusable_case_count": len(records) if isinstance(records, list) else 0,
        "malformed_confusable_cases": malformed,
    }


def _image_ocr_evidence(records):
    malformed = []
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, IMAGE_OCR_SIGNAL_REQUIRED_FIELDS)
        violations = []
        confidence = _confidence(item.get("confidence") if isinstance(item, dict) else None)
        action = item.get("policy_action") if isinstance(item, dict) else None
        if confidence is None:
            violations.append("confidence_invalid")
        elif confidence < 0.8 and action != "ocr_signal_quarantined":
            violations.append("low_confidence_not_quarantined")
        elif confidence >= 0.8 and action not in {"manual_review", "ocr_signal_quarantined"}:
            violations.append("high_confidence_action_invalid")
        if not _sha256_hex_like(item.get("image_hash") if isinstance(item, dict) else None):
            violations.append("image_hash_not_sha256")
        if missing or violations:
            malformed.append({"index": index, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "ocr_policy_count": len(records) if isinstance(records, list) else 0,
        "malformed_ocr_policies": malformed,
    }


def _impersonation_evidence(records, source_by_id):
    malformed = []
    high_confidence_quarantine_count = 0
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, SOURCE_IMPERSONATION_REQUIRED_FIELDS)
        violations = []
        source_id = str(item.get("source_id") or "") if isinstance(item, dict) else ""
        confidence = _confidence(item.get("confidence") if isinstance(item, dict) else None)
        action = item.get("action") if isinstance(item, dict) else None
        if source_id not in source_by_id:
            violations.append("unknown_source_id")
        if confidence is None:
            violations.append("confidence_invalid")
        elif confidence >= 0.8:
            if action != "source_quarantined":
                violations.append("high_confidence_not_quarantined")
            else:
                high_confidence_quarantine_count += 1
        elif action not in {"manual_review", "source_quarantined"}:
            violations.append("low_confidence_action_invalid")
        if missing or violations:
            malformed.append({"index": index, "source_id": source_id, "message_id": item.get("message_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "impersonation_check_count": len(records) if isinstance(records, list) else 0,
        "high_confidence_quarantine_count": high_confidence_quarantine_count,
        "malformed_impersonation_checks": malformed,
    }


def verify_source_parser_authenticity_contracts(
    policy_path=DEFAULT_SOURCE_PARSER_AUTH_POLICY,
    source_registry_path=DEFAULT_SOURCE_REGISTRY,
    channels_csv=DEFAULT_CHANNELS_CSV,
):
    policy, source_by_id, shadow_source_ids, base, base_failed = _source_parser_policy_base(policy_path, source_registry_path, channels_csv)

    canary = _parser_canary_evidence(policy.get("parser_canary_corpus"))
    ambiguity = _parser_ambiguity_evidence(policy.get("parser_ambiguity_cases"))
    forwarded = _forwarded_message_evidence(policy.get("forwarded_message_policy"), source_by_id)
    access = _premium_source_access_evidence(policy.get("premium_source_access_probes"), source_by_id, shadow_source_ids)
    authenticity = _source_authenticity_evidence(policy.get("source_authenticity_checks"), source_by_id, shadow_source_ids)
    confusables = _confusables_evidence(policy.get("parser_confusables_cases"))
    ocr = _image_ocr_evidence(policy.get("image_ocr_signal_policy"))
    impersonation = _impersonation_evidence(policy.get("source_impersonation_checks"), source_by_id)

    return [
        _source_parser_contract(
            "ParserCanaryCorpusContract",
            not base_failed and canary["canary_case_count"] > 0 and canary["failure_count"] == 0 and canary["checked_at_valid"] and not canary["missing_corpus_fields"] and not canary["malformed_canary_cases"],
            "parser_canary_corpus_missing_malformed_or_failing",
            base,
            canary,
        ),
        _source_parser_contract(
            "ParserAmbiguityContract",
            not base_failed and ambiguity["ambiguity_case_count"] > 0 and not ambiguity["malformed_ambiguity_cases"],
            "parser_ambiguity_policy_missing_malformed_or_unenforced",
            base,
            ambiguity,
        ),
        _source_parser_contract(
            "TelegramForwardedMessagePolicy",
            not base_failed and forwarded["forwarded_policy_count"] > 0 and forwarded["accepted_registered_forwarded_count"] > 0 and forwarded["quarantined_unknown_forwarded_count"] > 0 and not forwarded["malformed_forwarded_policies"],
            "telegram_forwarded_message_policy_missing_malformed_or_bypassable",
            base,
            forwarded,
        ),
        _source_parser_contract(
            "PremiumSourceAccessHealthContract",
            not base_failed and access["access_probe_count"] > 0 and not access["missing_probe_source_ids"] and not access["malformed_access_probes"],
            "premium_source_access_missing_malformed_or_degraded",
            base,
            access,
        ),
        _source_parser_contract(
            "SourceAuthenticityContract",
            not base_failed and authenticity["authenticity_check_count"] > 0 and not authenticity["missing_authenticity_source_ids"] and not authenticity["malformed_authenticity_checks"],
            "source_authenticity_missing_malformed_or_unverified",
            base,
            authenticity,
        ),
        _source_parser_contract(
            "ParserConfusablesContract",
            not base_failed and confusables["confusable_case_count"] > 0 and not confusables["malformed_confusable_cases"],
            "parser_confusables_missing_malformed_or_unenforced",
            base,
            confusables,
        ),
        _source_parser_contract(
            "ImageOCRSignalPolicy",
            not base_failed and ocr["ocr_policy_count"] > 0 and not ocr["malformed_ocr_policies"],
            "image_ocr_policy_missing_malformed_or_unenforced",
            base,
            ocr,
        ),
        _source_parser_contract(
            "SourceImpersonationDetector",
            not base_failed and impersonation["impersonation_check_count"] > 0 and impersonation["high_confidence_quarantine_count"] > 0 and not impersonation["malformed_impersonation_checks"],
            "source_impersonation_detector_missing_malformed_or_unenforced",
            base,
            impersonation,
        ),
    ]


def _shadow_observation_identity_policy_base(policy_path, source_registry_path):
    try:
        policy = _load_json(policy_path)
        policy_error = None
    except Exception as exc:
        policy = {}
        policy_error = {"policy_path": str(policy_path), "error": str(exc)}
    if not isinstance(policy, dict):
        policy = {}
        policy_error = {"policy_path": str(policy_path), "error": "shadow_observation_identity_policy_not_object"}

    source_anchor_violations, source_errors = _verify_source_anchors(policy.get("source_anchors"))
    schema_violations = []
    if policy.get("schema_version") != "v2.7.0.shadow_observation_identity_policy.v1":
        schema_violations.append("schema_version_mismatch")
    try:
        source_by_id, shadow_source_ids = _source_registry_index(source_registry_path)
        registry_error = None
    except Exception as exc:
        source_by_id, shadow_source_ids = {}, []
        registry_error = {"source_registry_path": str(source_registry_path), "error": str(exc)}
    allowed_aliases = [str(item) for item in (policy.get("allowed_source_aliases") or []) if item]
    forbidden_future_fields = [str(item) for item in (policy.get("forbidden_future_fields") or []) if item]
    base = {
        "policy_path": str(policy_path),
        "schema_version": policy.get("schema_version"),
        "scope": policy.get("scope"),
        "source_registry_path": str(source_registry_path),
        "source_anchor_violations": source_anchor_violations,
        "source_errors": source_errors,
        "schema_violations": schema_violations,
        "registry_error": registry_error,
        "shadow_source_ids": shadow_source_ids,
        "allowed_source_aliases": allowed_aliases,
        "forbidden_future_fields": forbidden_future_fields,
    }
    base_failed = bool(policy_error or source_anchor_violations or source_errors or schema_violations or registry_error)
    if policy_error:
        base["policy_error"] = policy_error
    return policy, source_by_id, shadow_source_ids, allowed_aliases, forbidden_future_fields, base, base_failed


def _shadow_observation_identity_contract(contract_id, passed, reason, base_evidence, evidence):
    return _contract(contract_id, passed, reason, {**base_evidence, **evidence})


def _identity_merge_split_evidence(records):
    malformed = []
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, IDENTITY_MERGE_SPLIT_REQUIRED_FIELDS)
        violations = []
        old_key = item.get("old_identity_key") if isinstance(item, dict) else None
        new_key = item.get("new_identity_key") if isinstance(item, dict) else None
        if old_key and new_key and old_key == new_key:
            violations.append("identity_key_not_changed")
        if item.get("source_event_type") != "token_lifecycle_identity_resolved":
            violations.append("unexpected_source_event_type")
        if item.get("failure_action") != "identity_dirty":
            violations.append("unexpected_failure_action")
        if missing or violations:
            malformed.append({"index": index, "merge_split_id": item.get("merge_split_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "merge_split_count": len(records) if isinstance(records, list) else 0,
        "malformed_merge_split_records": malformed,
    }


def _rekeying_evidence(records):
    malformed = []
    retired_count = 0
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, REKEYING_REQUIRED_FIELDS)
        violations = []
        old_key = item.get("old_key") if isinstance(item, dict) else None
        new_key = item.get("new_key") if isinstance(item, dict) else None
        if old_key and new_key and old_key == new_key:
            violations.append("rekey_target_unchanged")
        if item.get("source_event_type") != "token_lifecycle_identity_resolved":
            violations.append("unexpected_source_event_type")
        if item.get("old_key_retired") is not True:
            violations.append("old_key_not_retired")
        else:
            retired_count += 1
        if item.get("failure_action") != "identity_dirty":
            violations.append("unexpected_failure_action")
        if missing or violations:
            malformed.append({"index": index, "old_key": old_key, "new_key": new_key, "missing_fields": missing, "violations": violations})
    return {
        "rekey_count": len(records) if isinstance(records, list) else 0,
        "old_key_retired_count": retired_count,
        "malformed_rekey_records": malformed,
    }


def _source_gap_backfill_boundary_evidence(records, source_by_id, forbidden_future_fields):
    malformed = []
    research_only_count = 0
    forbidden_set = set(forbidden_future_fields)
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, SOURCE_GAP_BACKFILL_REQUIRED_FIELDS)
        violations = []
        source_id = str(item.get("source_id") or "") if isinstance(item, dict) else ""
        gap_window = item.get("gap_window") if isinstance(item, dict) else None
        allowed_fields = item.get("allowed_fields") if isinstance(item, dict) else None
        forbidden_fields = item.get("forbidden_fields") if isinstance(item, dict) else None
        if source_id not in source_by_id:
            violations.append("unknown_source_id")
        if not isinstance(gap_window, dict):
            violations.append("gap_window_not_object")
        else:
            start_ts = _parse_iso_ts(gap_window.get("start"))
            end_ts = _parse_iso_ts(gap_window.get("end"))
            if start_ts is None or end_ts is None:
                violations.append("gap_window_invalid")
            elif start_ts > end_ts:
                violations.append("gap_window_reversed")
        if not isinstance(allowed_fields, list) or not allowed_fields:
            violations.append("allowed_fields_missing")
        elif forbidden_set.intersection(str(field) for field in allowed_fields):
            violations.append("allowed_fields_include_future_outcome")
        if not isinstance(forbidden_fields, list) or not forbidden_set.issubset(set(str(field) for field in forbidden_fields)):
            violations.append("forbidden_fields_incomplete")
        if _parse_iso_ts(item.get("backfilled_at") if isinstance(item, dict) else None) is None:
            violations.append("backfilled_at_invalid")
        if item.get("backfill_mode") != "research_only":
            violations.append("backfill_mode_not_research_only")
        else:
            research_only_count += 1
        if item.get("writes_to_shadow_only") is not True:
            violations.append("writes_to_shadow_only_not_true")
        if item.get("failure_action") != "backfill_research_only":
            violations.append("unexpected_failure_action")
        if missing or violations:
            malformed.append({"index": index, "backfill_id": item.get("backfill_id") if isinstance(item, dict) else None, "source_id": source_id, "missing_fields": missing, "violations": violations})
    return {
        "source_gap_backfill_boundary_count": len(records) if isinstance(records, list) else 0,
        "research_only_backfill_count": research_only_count,
        "malformed_source_gap_backfill_boundaries": malformed,
    }


def _observation_policy_evidence(records, source_by_id, allowed_aliases, forbidden_future_fields):
    malformed = []
    known_sources = set(source_by_id) | set(allowed_aliases)
    forbidden_set = set(forbidden_future_fields)
    rejecting_policy_count = 0
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, OBSERVATION_POLICY_REQUIRED_FIELDS)
        violations = []
        allowed_sources = item.get("allowed_sources") if isinstance(item, dict) else None
        forbidden_fields = item.get("forbidden_fields") if isinstance(item, dict) else None
        if not isinstance(allowed_sources, list) or not allowed_sources:
            violations.append("allowed_sources_missing")
        else:
            unknown_allowed = sorted(str(source) for source in allowed_sources if str(source) not in known_sources)
            if unknown_allowed:
                violations.append("unknown_allowed_source")
        if not isinstance(forbidden_fields, list) or not forbidden_set.issubset(set(str(field) for field in forbidden_fields)):
            violations.append("forbidden_fields_incomplete")
        if _parse_iso_ts(item.get("recorded_at") if isinstance(item, dict) else None) is None:
            violations.append("recorded_at_invalid")
        if item.get("policy_action") != "observation_rejected":
            violations.append("unexpected_policy_action")
        else:
            rejecting_policy_count += 1
        if missing or violations:
            malformed.append({"index": index, "observation_id": item.get("observation_id") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
    return {
        "observation_policy_count": len(records) if isinstance(records, list) else 0,
        "rejecting_observation_policy_count": rejecting_policy_count,
        "malformed_observation_policies": malformed,
    }


def _counterfactual_model_snapshot_valid(value):
    text = str(value or "")
    if text.startswith("sha256:"):
        text = text.split("sha256:", 1)[1]
    return _sha256_hex_like(text)


def _counterfactual_entry_time_evidence(records):
    malformed = []
    leak_free_count = 0
    for index, item in enumerate(records if isinstance(records, list) else []):
        missing = _missing_required_fields(item, COUNTERFACTUAL_ENTRY_TIME_REQUIRED_FIELDS)
        violations = []
        entry_ts = _parse_iso_ts(item.get("counterfactual_entry_ts") if isinstance(item, dict) else None)
        decision_ts = _parse_iso_ts(item.get("decision_available_at") if isinstance(item, dict) else None)
        feature_ts = _parse_iso_ts(item.get("feature_max_available_at") if isinstance(item, dict) else None)
        if entry_ts is None:
            violations.append("counterfactual_entry_ts_invalid")
        if decision_ts is None:
            violations.append("decision_available_at_invalid")
        elif entry_ts is not None and decision_ts > entry_ts:
            violations.append("decision_after_counterfactual_entry")
        if feature_ts is not None and entry_ts is not None and feature_ts > entry_ts:
            violations.append("feature_after_counterfactual_entry")
        if not _counterfactual_model_snapshot_valid(item.get("counterfactual_model_snapshot_id") if isinstance(item, dict) else None):
            violations.append("counterfactual_model_snapshot_id_invalid")
        used_future_peak = item.get("used_future_peak") if isinstance(item, dict) else None
        used_future_outcome = item.get("used_future_outcome") if isinstance(item, dict) else None
        used_posthoc_label = item.get("used_posthoc_label") if isinstance(item, dict) else None
        forbidden_used = item.get("forbidden_future_fields_used") if isinstance(item, dict) else None
        if used_future_peak is not False:
            violations.append("used_future_peak_not_false")
        if used_future_outcome is not False:
            violations.append("used_future_outcome_not_false")
        if used_posthoc_label is not False:
            violations.append("used_posthoc_label_not_false")
        if forbidden_used not in ([], None):
            violations.append("forbidden_future_fields_used_not_empty")
        if item.get("failure_action") != "research_only":
            violations.append("unexpected_failure_action")
        if missing or violations:
            malformed.append({"index": index, "counterfactual_policy_version": item.get("counterfactual_policy_version") if isinstance(item, dict) else None, "missing_fields": missing, "violations": violations})
        else:
            leak_free_count += 1
    return {
        "counterfactual_entry_time_count": len(records) if isinstance(records, list) else 0,
        "leak_free_counterfactual_entry_time_count": leak_free_count,
        "malformed_counterfactual_entry_times": malformed,
    }


def verify_shadow_observation_identity_contracts(
    policy_path=DEFAULT_SHADOW_OBSERVATION_IDENTITY_POLICY,
    source_registry_path=DEFAULT_SOURCE_REGISTRY,
):
    policy, source_by_id, shadow_source_ids, allowed_aliases, forbidden_future_fields, base, base_failed = _shadow_observation_identity_policy_base(policy_path, source_registry_path)

    merge_split = _identity_merge_split_evidence(policy.get("identity_merge_split_records"))
    rekeying = _rekeying_evidence(policy.get("rekey_records"))
    backfill = _source_gap_backfill_boundary_evidence(policy.get("source_gap_backfill_boundaries"), source_by_id, forbidden_future_fields)
    observation = _observation_policy_evidence(policy.get("observation_policies"), source_by_id, allowed_aliases, forbidden_future_fields)
    counterfactual = _counterfactual_entry_time_evidence(policy.get("counterfactual_entry_time_records"))

    return [
        _shadow_observation_identity_contract(
            "IdentityMergeSplitContract",
            not base_failed and merge_split["merge_split_count"] > 0 and not merge_split["malformed_merge_split_records"],
            "identity_merge_split_policy_missing_malformed_or_unenforced",
            base,
            merge_split,
        ),
        _shadow_observation_identity_contract(
            "ReKeyingContract",
            not base_failed and rekeying["rekey_count"] > 0 and rekeying["old_key_retired_count"] > 0 and not rekeying["malformed_rekey_records"],
            "rekeying_policy_missing_malformed_or_unenforced",
            base,
            rekeying,
        ),
        _shadow_observation_identity_contract(
            "SourceGapBackfillBoundary",
            not base_failed and backfill["source_gap_backfill_boundary_count"] > 0 and backfill["research_only_backfill_count"] > 0 and not backfill["malformed_source_gap_backfill_boundaries"],
            "source_gap_backfill_boundary_missing_malformed_or_live_mutating",
            base,
            backfill,
        ),
        _shadow_observation_identity_contract(
            "ObservationPolicyContract",
            not base_failed and observation["observation_policy_count"] > 0 and observation["rejecting_observation_policy_count"] > 0 and not observation["malformed_observation_policies"],
            "observation_policy_missing_malformed_or_leaky",
            base,
            observation,
        ),
        _shadow_observation_identity_contract(
            "CounterfactualEntryTime",
            not base_failed and counterfactual["counterfactual_entry_time_count"] > 0 and counterfactual["leak_free_counterfactual_entry_time_count"] > 0 and not counterfactual["malformed_counterfactual_entry_times"],
            "counterfactual_entry_time_missing_malformed_or_future_leaky",
            base,
            counterfactual,
        ),
    ]


def verify_input_sanitization():
    sample = {
        "id": 1,
        "token_ca": "TokenSanitize",
        "symbol": "SAN",
        "created_at": "2026-01-15 00:00:00",
        "parse_status": "parsed",
        "raw_message": "<script>alert('x')</script> CA TokenSanitize",
    }
    payload = _signal_payload(sample)
    legacy = payload.get("legacy_premium_signal") or {}
    raw_leaked = legacy.get("raw_message") == sample["raw_message"]
    passed = bool(payload.get("payload_schema_valid")) and payload.get("raw_message_hash") and not raw_leaked
    return _contract(
        "InputSanitizationContract",
        passed,
        "input_sanitization_unverified",
        {
            "payload_schema_valid": payload.get("payload_schema_valid"),
            "unsafe_pattern_detected": payload.get("unsafe_pattern_detected"),
            "raw_message_hash_present": bool(payload.get("raw_message_hash")),
            "raw_text_fields_redacted": payload.get("raw_text_fields_redacted"),
            "legacy_raw_message_leaked": raw_leaked,
        },
    )


def verify_safe_default(registry_path=DEFAULT_ENTRY_MODE_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("SafeDefaultContract", False, "safe_default_registry_missing_or_invalid", {"error": str(exc)})
    modes = registry.get("modes") if isinstance(registry, dict) else {}
    tiers = registry.get("tiers") if isinstance(registry, dict) else {}
    if not isinstance(modes, dict):
        modes = {}
    if not isinstance(tiers, dict):
        tiers = {}
    hard_shadow_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict)
        and entry.get("paper_enabled") is False
        and str(entry.get("tier") or "") in {"hard_shadow", "shadow_watch_only", "deprecated_shadow"}
    )
    blocked_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict) and entry.get("paper_enabled") is False
    )
    invalid_tier_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict) and str(entry.get("tier") or "") not in tiers
    )
    default_record = {
        "unknown_type": "unregistered_entry_mode_or_unproven_contract",
        "default_action": "fail_closed",
        "allowed_modes": ["observe_only", "shadow"],
        "owning_contract": "SafeDefaultContract",
    }
    passed = bool(modes) and bool(blocked_modes) and bool(hard_shadow_modes) and not invalid_tier_modes
    return _contract(
        "SafeDefaultContract",
        passed,
        "safe_default_fail_closed_unverified",
        {
            **default_record,
            "entry_mode_registry_path": str(registry_path),
            "mode_count": len(modes),
            "blocked_mode_count": len(blocked_modes),
            "hard_shadow_default_mode_count": len(hard_shadow_modes),
            "invalid_tier_modes": invalid_tier_modes,
            "blocked_modes_sample": blocked_modes[:20],
        },
    )


def verify_project_stop_loss(env=None):
    if env is None:
        env = os.environ
    auto_kill_enabled = _bool_env(env, "ENTRY_MODE_QUALITY_AUTO_KILL_SWITCH_ENABLED", True)
    window = max(5, _int_env(env, "ENTRY_MODE_QUALITY_WINDOW", 20))
    shadow_sec = max(60, _int_env(env, "ENTRY_MODE_QUALITY_SHADOW_SEC", 2 * 3600))
    stop_criteria = {
        "negative_ev_min_samples": max(1, _int_env(env, "ENTRY_MODE_QUALITY_NEGATIVE_EV_MIN_SAMPLES", 20)),
        "tail_min_samples": max(1, _int_env(env, "ENTRY_MODE_QUALITY_TAIL_MIN_SAMPLES", 8)),
        "avg_pnl_floor": _float_env(env, "ENTRY_MODE_QUALITY_AVG_PNL_FLOOR", 0.0),
        "p10_pnl_floor": _float_env(env, "ENTRY_MODE_QUALITY_P10_PNL_FLOOR", -0.30),
        "max_loss_floor": _float_env(env, "ENTRY_MODE_QUALITY_MAX_LOSS_FLOOR", -0.80),
    }
    action = {
        "action": "downgrade_to_watch_only",
        "shadow_sec": shadow_sec,
        "stop_automatic_entry": True,
    }
    invalid_criteria = []
    if stop_criteria["negative_ev_min_samples"] <= 0:
        invalid_criteria.append("negative_ev_min_samples")
    if stop_criteria["tail_min_samples"] <= 0:
        invalid_criteria.append("tail_min_samples")
    if stop_criteria["p10_pnl_floor"] >= 0:
        invalid_criteria.append("p10_pnl_floor")
    if stop_criteria["max_loss_floor"] >= 0:
        invalid_criteria.append("max_loss_floor")
    passed = auto_kill_enabled and window > 0 and shadow_sec >= 60 and not invalid_criteria
    return _contract(
        "ProjectStopLossContract",
        passed,
        "project_stop_loss_unverified_or_disabled",
        {
            "scope": "entry_mode",
            "window": {"closed_trade_window": window},
            "stop_criteria": stop_criteria,
            "action": action,
            "auto_kill_switch_enabled": auto_kill_enabled,
            "invalid_criteria": invalid_criteria,
        },
    )


def verify_evidence_eligibility_matrix(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("EvidenceEligibilityMatrix", False, "evidence_eligibility_matrix_missing_or_invalid", base_evidence)
    rows = governance.get("evidence_eligibility_matrix")
    if not isinstance(rows, list):
        rows = []
    required = ("evidence_use", "event_truth", "feature_truth", "label_truth", "replay_truth")
    malformed = []
    for index, row in enumerate(rows):
        missing = _missing_required_fields(row, required)
        truth_fields = {
            field: row.get(field)
            for field in ("event_truth", "feature_truth", "label_truth", "replay_truth")
            if isinstance(row, dict)
        }
        non_list_truth = sorted(field for field, value in truth_fields.items() if not isinstance(value, list))
        if missing or non_list_truth:
            malformed.append({"index": index, "evidence_use": row.get("evidence_use") if isinstance(row, dict) else None, "missing_fields": missing, "non_list_truth_fields": non_list_truth})
    evidence_uses = sorted({row.get("evidence_use") for row in rows if isinstance(row, dict) and row.get("evidence_use")})
    passed = bool(rows) and not malformed and "normal_tiny_promotion" in evidence_uses
    return _contract(
        "EvidenceEligibilityMatrix",
        passed,
        "evidence_eligibility_matrix_missing_malformed_or_incomplete",
        {
            **base_evidence,
            "matrix_row_count": len(rows),
            "evidence_uses": evidence_uses,
            "malformed_rows": malformed,
            "required_evidence_use_present": "normal_tiny_promotion" in evidence_uses,
        },
    )


def verify_top_fix_queue(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("TopFixQueueContract", False, "top_fix_queue_missing_or_invalid", base_evidence)
    queue = governance.get("top_fix_queue")
    if not isinstance(queue, list):
        queue = []
    required = ("fix_id", "blocker_code", "first_fix_that_would_change_decision", "owner", "acceptance_test")
    malformed = []
    seen_fix_ids = set()
    duplicate_fix_ids = []
    blocker_codes = set()
    for index, item in enumerate(queue):
        missing = _missing_required_fields(item, required)
        fix_id = item.get("fix_id") if isinstance(item, dict) else None
        blocker_code = item.get("blocker_code") if isinstance(item, dict) else None
        if fix_id in seen_fix_ids:
            duplicate_fix_ids.append(fix_id)
        if fix_id:
            seen_fix_ids.add(fix_id)
        if blocker_code:
            blocker_codes.add(str(blocker_code))
        if missing:
            malformed.append({"index": index, "fix_id": fix_id, "blocker_code": blocker_code, "missing_fields": missing})
    missing_blocker_codes = sorted(NORMAL_TINY_BLOCKING_CONTRACTS - blocker_codes)
    passed = bool(queue) and not malformed and not duplicate_fix_ids and not missing_blocker_codes
    return _contract(
        "TopFixQueueContract",
        passed,
        "top_fix_queue_missing_malformed_or_incomplete",
        {
            **base_evidence,
            "queue_count": len(queue),
            "normal_tiny_contract_count": len(NORMAL_TINY_BLOCKING_CONTRACTS),
            "covered_blocker_codes": sorted(blocker_codes),
            "missing_blocker_codes": missing_blocker_codes,
            "malformed_queue_items": malformed,
            "duplicate_fix_ids": sorted(duplicate_fix_ids),
        },
    )


def verify_safety_case(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("SafetyCaseContract", False, "safety_case_missing_or_invalid", base_evidence)
    safety_cases = governance.get("safety_cases")
    if not isinstance(safety_cases, list):
        safety_cases = []
    required = ("safety_case_id", "scope", "core_hazards", "mitigations", "evidence_links")
    required_links = {"EvidenceEligibilityMatrix", "TopFixQueueContract", "WaiverPolicyContract", "SafeDefaultContract", "ProjectStopLossContract"}
    malformed = []
    normal_tiny_cases = []
    link_coverage = set()
    for index, item in enumerate(safety_cases):
        missing = _missing_required_fields(item, required)
        safety_case_id = item.get("safety_case_id") if isinstance(item, dict) else None
        if isinstance(item, dict) and item.get("scope") == "normal_tiny":
            normal_tiny_cases.append(item)
            links = item.get("evidence_links")
            if isinstance(links, list):
                link_coverage.update(str(link) for link in links)
        if missing:
            malformed.append({"index": index, "safety_case_id": safety_case_id, "missing_fields": missing})
    missing_links = sorted(required_links - link_coverage)
    passed = bool(normal_tiny_cases) and not malformed and not missing_links
    return _contract(
        "SafetyCaseContract",
        passed,
        "safety_case_missing_malformed_or_unlinked",
        {
            **base_evidence,
            "safety_case_count": len(safety_cases),
            "normal_tiny_safety_case_count": len(normal_tiny_cases),
            "malformed_safety_cases": malformed,
            "required_evidence_links": sorted(required_links),
            "missing_evidence_links": missing_links,
        },
    )


def verify_waiver_policy(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("WaiverPolicyContract", False, "waiver_policy_missing_or_invalid", base_evidence)
    policies = governance.get("waiver_policy")
    if not isinstance(policies, list):
        policies = []
    required = ("waiver_id", "contract_id", "scope", "expires_at", "non_waivable")
    malformed = []
    non_waivable_contracts = set()
    wildcard_non_waivable = False
    now = datetime.now(timezone.utc)
    for index, item in enumerate(policies):
        missing = _missing_required_fields(item, required)
        waiver_id = item.get("waiver_id") if isinstance(item, dict) else None
        contract_id = item.get("contract_id") if isinstance(item, dict) else None
        parsed_expires_at = _parse_iso_ts(item.get("expires_at")) if isinstance(item, dict) else None
        violations = []
        if parsed_expires_at is None:
            violations.append("expires_at_parseable")
        elif parsed_expires_at <= now:
            violations.append("expires_at_future")
        if isinstance(item, dict) and item.get("scope") == "normal_tiny" and item.get("non_waivable") is True:
            if contract_id == "*":
                wildcard_non_waivable = True
            elif contract_id:
                non_waivable_contracts.add(str(contract_id))
        else:
            violations.append("normal_tiny_non_waivable_true")
        if missing or violations:
            malformed.append({"index": index, "waiver_id": waiver_id, "contract_id": contract_id, "missing_fields": missing, "violations": violations})
    missing_non_waivable = [] if wildcard_non_waivable else sorted(NORMAL_TINY_BLOCKING_CONTRACTS - non_waivable_contracts)
    passed = bool(policies) and not malformed and not missing_non_waivable
    return _contract(
        "WaiverPolicyContract",
        passed,
        "waiver_policy_missing_malformed_or_bypassable",
        {
            **base_evidence,
            "waiver_policy_count": len(policies),
            "wildcard_non_waivable": wildcard_non_waivable,
            "non_waivable_contracts": sorted(non_waivable_contracts),
            "missing_non_waivable_contracts": missing_non_waivable,
            "malformed_waiver_policies": malformed,
        },
    )


def build_basic_contract_readiness(
    *,
    chain_config_path=DEFAULT_CHAIN_CONFIG,
    source_registry_path=DEFAULT_SOURCE_REGISTRY,
    source_parser_auth_policy_path=DEFAULT_SOURCE_PARSER_AUTH_POLICY,
    shadow_observation_identity_policy_path=DEFAULT_SHADOW_OBSERVATION_IDENTITY_POLICY,
    channels_csv=DEFAULT_CHANNELS_CSV,
    system_config_path=DEFAULT_SYSTEM_CONFIG,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
    governance_path=DEFAULT_GOVERNANCE_READINESS,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
    write_path_registry_path=DEFAULT_WRITE_PATH_REGISTRY,
    direct_db_mutation_policy_path=DEFAULT_DIRECT_DB_MUTATION_POLICY,
    aggregate_boundary_policy_path=DEFAULT_AGGREGATE_BOUNDARIES,
    event_schema_compatibility_policy_path=DEFAULT_EVENT_SCHEMA_COMPATIBILITY,
    read_model_snapshot_policy_path=DEFAULT_READ_MODEL_SNAPSHOT_POLICY,
    runtime_worker_health_policy_path=DEFAULT_RUNTIME_WORKER_HEALTH_POLICY,
    db_runtime_concurrency_policy_path=DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY,
    background_job_registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY,
    entry_point_inventory_path=DEFAULT_ENTRY_POINT_INVENTORY,
    static_policy_path=DEFAULT_STATIC_POLICY_ENFORCEMENT,
    feature_flag_dependency_policy_path=DEFAULT_FEATURE_FLAG_DEPENDENCIES,
    filesystem_pressure_policy_path=DEFAULT_FILESYSTEM_PRESSURE_POLICY,
    api_response_policy_path=DEFAULT_API_RESPONSE_POLICY,
    api_response_envelope_policy_path=DEFAULT_API_RESPONSE_ENVELOPE_POLICY,
    error_taxonomy_path=DEFAULT_ERROR_TAXONOMY,
    log_redaction_policy_path=DEFAULT_LOG_REDACTION_POLICY,
    service_readiness_policy_path=DEFAULT_SERVICE_READINESS_PROBES,
    dashboard_action_separation_policy_path=DEFAULT_DASHBOARD_ACTION_SEPARATION_POLICY,
    numeric_precision_policy_path=DEFAULT_NUMERIC_PRECISION_POLICY,
    metric_definition_registry_path=DEFAULT_METRIC_DEFINITION_REGISTRY,
    threshold_catalog_path=DEFAULT_THRESHOLD_CATALOG,
    runtime_config_drift_policy_path=DEFAULT_RUNTIME_CONFIG_DRIFT_POLICY,
    environment_separation_policy_path=DEFAULT_ENVIRONMENT_SEPARATION_POLICY,
    null_value_policy_path=DEFAULT_NULL_VALUE_POLICY,
    feature_vector_snapshot_policy_path=DEFAULT_FEATURE_VECTOR_SNAPSHOT_POLICY,
    training_dataset_manifest_policy_path=DEFAULT_TRAINING_DATASET_MANIFEST_POLICY,
    data_lineage_graph_policy_path=DEFAULT_DATA_LINEAGE_GRAPH_POLICY,
    detector_shadow_calibration_policy_path=DEFAULT_DETECTOR_SHADOW_CALIBRATION_POLICY,
    capacity_load_latency_policy_path=DEFAULT_CAPACITY_LOAD_LATENCY_POLICY,
    operator_runtime_safety_policy_path=DEFAULT_OPERATOR_RUNTIME_SAFETY_POLICY,
    replay_build_model_policy_path=DEFAULT_REPLAY_BUILD_MODEL_POLICY,
    spec_governance_feasibility_policy_path=DEFAULT_SPEC_GOVERNANCE_FEASIBILITY_POLICY,
    identity_unit_provider_finality_policy_path=DEFAULT_IDENTITY_UNIT_PROVIDER_FINALITY_POLICY,
    execution_exit_safety_policy_path=DEFAULT_EXECUTION_EXIT_SAFETY_POLICY,
    delivery_traceability_policy_path=DEFAULT_DELIVERY_TRACEABILITY_POLICY,
    release_experiment_safety_policy_path=DEFAULT_RELEASE_EXPERIMENT_SAFETY_POLICY,
    markov_lifecycle_forecast_policy_path=DEFAULT_MARKOV_LIFECYCLE_FORECAST_POLICY,
    reason_taxonomy_policy_path=DEFAULT_REASON_TAXONOMY_POLICY,
    security_session_policy_path=DEFAULT_SECURITY_SESSION_POLICY,
    runtime_pipeline_policy_path=DEFAULT_RUNTIME_PIPELINE_POLICY,
    ci_spec_generated_policy_path=DEFAULT_CI_SPEC_GENERATED_POLICY,
    env=None,
):
    contracts = {
        item["contract_id"]: item
        for item in [
            verify_spec_consistency(manifest_path, catalog_path, registry_path),
            verify_numeric_precision_policy(policy_path=numeric_precision_policy_path),
            verify_metric_definition_registry(registry_path=metric_definition_registry_path),
            verify_threshold_catalog(
                threshold_catalog_path=threshold_catalog_path,
                metric_registry_path=metric_definition_registry_path,
            ),
            verify_runtime_config_drift_contract(policy_path=runtime_config_drift_policy_path, env=env),
            verify_environment_separation_contract(policy_path=environment_separation_policy_path, env=env),
            verify_null_value_policy_contract(policy_path=null_value_policy_path),
            verify_feature_availability_contract(
                policy_path=feature_vector_snapshot_policy_path,
                null_policy_path=null_value_policy_path,
            ),
            verify_feature_vector_snapshot_contract(
                policy_path=feature_vector_snapshot_policy_path,
                null_policy_path=null_value_policy_path,
            ),
            verify_data_lineage_graph_contract(policy_path=data_lineage_graph_policy_path),
            verify_training_dataset_manifest_contract(
                policy_path=training_dataset_manifest_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
                manifest_path=manifest_path,
                catalog_path=catalog_path,
                registry_path=registry_path,
            ),
            *verify_detector_shadow_calibration_contracts(
                policy_path=detector_shadow_calibration_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_capacity_load_latency_contracts(
                policy_path=capacity_load_latency_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_operator_runtime_safety_contracts(
                policy_path=operator_runtime_safety_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_replay_build_model_contracts(
                policy_path=replay_build_model_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_spec_governance_feasibility_contracts(
                policy_path=spec_governance_feasibility_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
                manifest_path=manifest_path,
                catalog_path=catalog_path,
                registry_path=registry_path,
            ),
            *verify_identity_unit_provider_finality_contracts(
                policy_path=identity_unit_provider_finality_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_execution_exit_safety_contracts(
                policy_path=execution_exit_safety_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_delivery_traceability_contracts(
                policy_path=delivery_traceability_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_release_experiment_safety_contracts(
                policy_path=release_experiment_safety_policy_path,
                metric_registry_path=metric_definition_registry_path,
                threshold_catalog_path=threshold_catalog_path,
            ),
            *verify_markov_lifecycle_forecast_contracts(policy_path=markov_lifecycle_forecast_policy_path),
            verify_human_readable_reason_contract(policy_path=reason_taxonomy_policy_path),
            verify_machine_readable_reason_contract(policy_path=reason_taxonomy_policy_path),
            verify_paper_mode_safety(env=env),
            verify_chain_config(chain_config_path, system_config_path),
            verify_source_registry(source_registry_path, channels_csv),
            *verify_source_parser_authenticity_contracts(
                policy_path=source_parser_auth_policy_path,
                source_registry_path=source_registry_path,
                channels_csv=channels_csv,
            ),
            *verify_shadow_observation_identity_contracts(
                policy_path=shadow_observation_identity_policy_path,
                source_registry_path=source_registry_path,
            ),
            verify_input_sanitization(),
            verify_safe_default(registry_path=registry_path),
            verify_project_stop_loss(env=env),
            verify_evidence_eligibility_matrix(governance_path=governance_path),
            verify_top_fix_queue(governance_path=governance_path),
            verify_safety_case(governance_path=governance_path),
            verify_waiver_policy(governance_path=governance_path),
            verify_access_control_policy(
                policy_path=access_control_policy_path,
                write_path_registry_path=write_path_registry_path,
            ),
            verify_audit_log_integrity(policy_path=access_control_policy_path),
            verify_write_path_registry(registry_path=write_path_registry_path),
            verify_direct_database_mutation_ban(
                policy_path=direct_db_mutation_policy_path,
                registry_path=write_path_registry_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_aggregate_boundary_contract(policy_path=aggregate_boundary_policy_path),
            verify_clock_rollback_guard_contract(),
            verify_event_schema_compatibility_contract(policy_path=event_schema_compatibility_policy_path),
            verify_enum_evolution_contract(policy_path=event_schema_compatibility_policy_path),
            verify_mutation_command_idempotency_contract(policy_path=event_schema_compatibility_policy_path),
            verify_projection_version_isolation_contract(policy_path=read_model_snapshot_policy_path),
            verify_snapshot_compaction_invariant_contract(policy_path=read_model_snapshot_policy_path),
            verify_snapshot_compaction_read_barrier_contract(policy_path=read_model_snapshot_policy_path),
            verify_worker_heartbeat_contract(policy_path=runtime_worker_health_policy_path),
            verify_silent_worker_death_detector_contract(
                policy_path=runtime_worker_health_policy_path,
                background_job_registry_path=background_job_registry_path,
            ),
            verify_warm_start_safety_contract(policy_path=runtime_worker_health_policy_path),
            verify_connection_pool_partition_contract(policy_path=db_runtime_concurrency_policy_path),
            verify_db_lock_contention_policy(policy_path=db_runtime_concurrency_policy_path),
            verify_database_transaction_isolation_contract(policy_path=db_runtime_concurrency_policy_path),
            verify_distributed_lock_backend_health_contract(policy_path=db_runtime_concurrency_policy_path),
            verify_background_job_registry(registry_path=background_job_registry_path),
            verify_scheduled_job_mode_gate_contract(registry_path=background_job_registry_path),
            verify_entry_point_inventory(
                inventory_path=entry_point_inventory_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_static_policy_enforcement(policy_path=static_policy_path),
            verify_feature_flag_dependency_contract(policy_path=feature_flag_dependency_policy_path, catalog_path=catalog_path),
            verify_filesystem_disk_pressure_policy(policy_path=filesystem_pressure_policy_path),
            verify_api_response_contract(
                policy_path=api_response_policy_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_api_response_envelope_contract(policy_path=api_response_envelope_policy_path),
            verify_error_taxonomy(taxonomy_path=error_taxonomy_path),
            verify_log_redaction_verification(policy_path=log_redaction_policy_path),
            verify_admin_session_security_contract(policy_path=security_session_policy_path),
            verify_secret_access_audit_contract(policy_path=security_session_policy_path),
            verify_telegram_session_security_contract(policy_path=security_session_policy_path),
            verify_queue_ack_nack_contract(policy_path=runtime_pipeline_policy_path),
            verify_pipeline_progress_invariant(policy_path=runtime_pipeline_policy_path),
            verify_thread_pool_isolation_contract(policy_path=runtime_pipeline_policy_path),
            verify_cicd_merge_gate_contract(
                policy_path=ci_spec_generated_policy_path,
                manifest_path=manifest_path,
                catalog_path=catalog_path,
                registry_path=registry_path,
            ),
            verify_generated_client_contract(policy_path=ci_spec_generated_policy_path, catalog_path=catalog_path),
            verify_spec_change_impact_analysis_contract(
                policy_path=ci_spec_generated_policy_path,
                manifest_path=manifest_path,
                catalog_path=catalog_path,
                registry_path=registry_path,
            ),
            verify_service_readiness_probe_contract(policy_path=service_readiness_policy_path),
            verify_dashboard_action_separation_contract(policy_path=dashboard_action_separation_policy_path),
        ]
    }
    blocking = [contract_id for contract_id, item in contracts.items() if item.get("status") != "pass"]
    return {
        "basic_readiness_schema_version": "v2.7.0.basic_contract_readiness.v1",
        "generated_at": _utc_now_iso(),
        "contracts": contracts,
        "blocking_contracts": blocking,
        "health": {
            "status": "basic_contract_readiness_ok" if not blocking else "basic_contract_readiness_blocked",
            "observe_only_foundation_ready": not blocking,
            "normal_tiny_ready": False,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain-config", default=str(DEFAULT_CHAIN_CONFIG))
    parser.add_argument("--source-registry", default=str(DEFAULT_SOURCE_REGISTRY))
    parser.add_argument("--source-parser-auth-policy", default=str(DEFAULT_SOURCE_PARSER_AUTH_POLICY))
    parser.add_argument("--shadow-observation-identity-policy", default=str(DEFAULT_SHADOW_OBSERVATION_IDENTITY_POLICY))
    parser.add_argument("--channels-csv", default=str(DEFAULT_CHANNELS_CSV))
    parser.add_argument("--system-config", default=str(DEFAULT_SYSTEM_CONFIG))
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--entry-mode-registry", default=str(ENTRY_MODE_REGISTRY_PATH))
    parser.add_argument("--governance-readiness", default=str(DEFAULT_GOVERNANCE_READINESS))
    parser.add_argument("--access-control-policy", default=str(DEFAULT_ACCESS_CONTROL_POLICY))
    parser.add_argument("--write-path-registry", default=str(DEFAULT_WRITE_PATH_REGISTRY))
    parser.add_argument("--direct-db-mutation-policy", default=str(DEFAULT_DIRECT_DB_MUTATION_POLICY))
    parser.add_argument("--aggregate-boundary-policy", default=str(DEFAULT_AGGREGATE_BOUNDARIES))
    parser.add_argument("--event-schema-compatibility-policy", default=str(DEFAULT_EVENT_SCHEMA_COMPATIBILITY))
    parser.add_argument("--read-model-snapshot-policy", default=str(DEFAULT_READ_MODEL_SNAPSHOT_POLICY))
    parser.add_argument("--runtime-worker-health-policy", default=str(DEFAULT_RUNTIME_WORKER_HEALTH_POLICY))
    parser.add_argument("--db-runtime-concurrency-policy", default=str(DEFAULT_DB_RUNTIME_CONCURRENCY_POLICY))
    parser.add_argument("--background-job-registry", default=str(DEFAULT_BACKGROUND_JOB_REGISTRY))
    parser.add_argument("--entry-point-inventory", default=str(DEFAULT_ENTRY_POINT_INVENTORY))
    parser.add_argument("--static-policy", default=str(DEFAULT_STATIC_POLICY_ENFORCEMENT))
    parser.add_argument("--feature-flag-dependency-policy", default=str(DEFAULT_FEATURE_FLAG_DEPENDENCIES))
    parser.add_argument("--filesystem-pressure-policy", default=str(DEFAULT_FILESYSTEM_PRESSURE_POLICY))
    parser.add_argument("--api-response-policy", default=str(DEFAULT_API_RESPONSE_POLICY))
    parser.add_argument("--api-response-envelope-policy", default=str(DEFAULT_API_RESPONSE_ENVELOPE_POLICY))
    parser.add_argument("--error-taxonomy", default=str(DEFAULT_ERROR_TAXONOMY))
    parser.add_argument("--log-redaction-policy", default=str(DEFAULT_LOG_REDACTION_POLICY))
    parser.add_argument("--service-readiness-policy", default=str(DEFAULT_SERVICE_READINESS_PROBES))
    parser.add_argument("--dashboard-action-separation-policy", default=str(DEFAULT_DASHBOARD_ACTION_SEPARATION_POLICY))
    parser.add_argument("--numeric-precision-policy", default=str(DEFAULT_NUMERIC_PRECISION_POLICY))
    parser.add_argument("--metric-definition-registry", default=str(DEFAULT_METRIC_DEFINITION_REGISTRY))
    parser.add_argument("--threshold-catalog", default=str(DEFAULT_THRESHOLD_CATALOG))
    parser.add_argument("--runtime-config-drift-policy", default=str(DEFAULT_RUNTIME_CONFIG_DRIFT_POLICY))
    parser.add_argument("--environment-separation-policy", default=str(DEFAULT_ENVIRONMENT_SEPARATION_POLICY))
    parser.add_argument("--null-value-policy", default=str(DEFAULT_NULL_VALUE_POLICY))
    parser.add_argument("--feature-vector-snapshot-policy", default=str(DEFAULT_FEATURE_VECTOR_SNAPSHOT_POLICY))
    parser.add_argument("--training-dataset-manifest-policy", default=str(DEFAULT_TRAINING_DATASET_MANIFEST_POLICY))
    parser.add_argument("--data-lineage-graph-policy", default=str(DEFAULT_DATA_LINEAGE_GRAPH_POLICY))
    parser.add_argument("--detector-shadow-calibration-policy", default=str(DEFAULT_DETECTOR_SHADOW_CALIBRATION_POLICY))
    parser.add_argument("--capacity-load-latency-policy", default=str(DEFAULT_CAPACITY_LOAD_LATENCY_POLICY))
    parser.add_argument("--operator-runtime-safety-policy", default=str(DEFAULT_OPERATOR_RUNTIME_SAFETY_POLICY))
    parser.add_argument("--replay-build-model-policy", default=str(DEFAULT_REPLAY_BUILD_MODEL_POLICY))
    parser.add_argument("--spec-governance-feasibility-policy", default=str(DEFAULT_SPEC_GOVERNANCE_FEASIBILITY_POLICY))
    parser.add_argument("--identity-unit-provider-finality-policy", default=str(DEFAULT_IDENTITY_UNIT_PROVIDER_FINALITY_POLICY))
    parser.add_argument("--execution-exit-safety-policy", default=str(DEFAULT_EXECUTION_EXIT_SAFETY_POLICY))
    parser.add_argument("--delivery-traceability-policy", default=str(DEFAULT_DELIVERY_TRACEABILITY_POLICY))
    parser.add_argument("--release-experiment-safety-policy", default=str(DEFAULT_RELEASE_EXPERIMENT_SAFETY_POLICY))
    parser.add_argument("--markov-lifecycle-forecast-policy", default=str(DEFAULT_MARKOV_LIFECYCLE_FORECAST_POLICY))
    parser.add_argument("--reason-taxonomy-policy", default=str(DEFAULT_REASON_TAXONOMY_POLICY))
    parser.add_argument("--security-session-policy", default=str(DEFAULT_SECURITY_SESSION_POLICY))
    parser.add_argument("--runtime-pipeline-policy", default=str(DEFAULT_RUNTIME_PIPELINE_POLICY))
    parser.add_argument("--ci-spec-generated-policy", default=str(DEFAULT_CI_SPEC_GENERATED_POLICY))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = build_basic_contract_readiness(
        chain_config_path=Path(args.chain_config),
        source_registry_path=Path(args.source_registry),
        source_parser_auth_policy_path=Path(args.source_parser_auth_policy),
        shadow_observation_identity_policy_path=Path(args.shadow_observation_identity_policy),
        channels_csv=Path(args.channels_csv),
        system_config_path=Path(args.system_config),
        manifest_path=Path(args.manifest),
        catalog_path=Path(args.catalog),
        registry_path=Path(args.entry_mode_registry),
        governance_path=Path(args.governance_readiness),
        access_control_policy_path=Path(args.access_control_policy),
        write_path_registry_path=Path(args.write_path_registry),
        direct_db_mutation_policy_path=Path(args.direct_db_mutation_policy),
        aggregate_boundary_policy_path=Path(args.aggregate_boundary_policy),
        event_schema_compatibility_policy_path=Path(args.event_schema_compatibility_policy),
        read_model_snapshot_policy_path=Path(args.read_model_snapshot_policy),
        runtime_worker_health_policy_path=Path(args.runtime_worker_health_policy),
        db_runtime_concurrency_policy_path=Path(args.db_runtime_concurrency_policy),
        background_job_registry_path=Path(args.background_job_registry),
        entry_point_inventory_path=Path(args.entry_point_inventory),
        static_policy_path=Path(args.static_policy),
        feature_flag_dependency_policy_path=Path(args.feature_flag_dependency_policy),
        filesystem_pressure_policy_path=Path(args.filesystem_pressure_policy),
        api_response_policy_path=Path(args.api_response_policy),
        api_response_envelope_policy_path=Path(args.api_response_envelope_policy),
        error_taxonomy_path=Path(args.error_taxonomy),
        log_redaction_policy_path=Path(args.log_redaction_policy),
        service_readiness_policy_path=Path(args.service_readiness_policy),
        dashboard_action_separation_policy_path=Path(args.dashboard_action_separation_policy),
        numeric_precision_policy_path=Path(args.numeric_precision_policy),
        metric_definition_registry_path=Path(args.metric_definition_registry),
        threshold_catalog_path=Path(args.threshold_catalog),
        runtime_config_drift_policy_path=Path(args.runtime_config_drift_policy),
        environment_separation_policy_path=Path(args.environment_separation_policy),
        null_value_policy_path=Path(args.null_value_policy),
        feature_vector_snapshot_policy_path=Path(args.feature_vector_snapshot_policy),
        training_dataset_manifest_policy_path=Path(args.training_dataset_manifest_policy),
        data_lineage_graph_policy_path=Path(args.data_lineage_graph_policy),
        detector_shadow_calibration_policy_path=Path(args.detector_shadow_calibration_policy),
        capacity_load_latency_policy_path=Path(args.capacity_load_latency_policy),
        operator_runtime_safety_policy_path=Path(args.operator_runtime_safety_policy),
        replay_build_model_policy_path=Path(args.replay_build_model_policy),
        spec_governance_feasibility_policy_path=Path(args.spec_governance_feasibility_policy),
        identity_unit_provider_finality_policy_path=Path(args.identity_unit_provider_finality_policy),
        execution_exit_safety_policy_path=Path(args.execution_exit_safety_policy),
        delivery_traceability_policy_path=Path(args.delivery_traceability_policy),
        release_experiment_safety_policy_path=Path(args.release_experiment_safety_policy),
        markov_lifecycle_forecast_policy_path=Path(args.markov_lifecycle_forecast_policy),
        reason_taxonomy_policy_path=Path(args.reason_taxonomy_policy),
        security_session_policy_path=Path(args.security_session_policy),
        runtime_pipeline_policy_path=Path(args.runtime_pipeline_policy),
        ci_spec_generated_policy_path=Path(args.ci_spec_generated_policy),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and report["blocking_contracts"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
