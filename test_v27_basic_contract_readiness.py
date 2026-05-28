import sys
import json
from pathlib import Path

sys.path.insert(0, "scripts")

from v27_basic_contract_readiness import (  # noqa: E402
    build_basic_contract_readiness,
    verify_admin_session_security_contract,
    verify_api_response_contract,
    verify_api_response_envelope_contract,
    verify_aggregate_boundary_contract,
    verify_audit_log_integrity,
    verify_background_job_registry,
    verify_capacity_load_latency_contracts,
    verify_cicd_merge_gate_contract,
    verify_clock_rollback_guard_contract,
    verify_connection_pool_partition_contract,
    verify_database_transaction_isolation_contract,
    verify_db_lock_contention_policy,
    verify_dashboard_action_separation_contract,
    verify_direct_database_mutation_ban,
    verify_distributed_lock_backend_health_contract,
    verify_detector_shadow_calibration_contracts,
    verify_enum_evolution_contract,
    verify_entry_point_inventory,
    verify_error_taxonomy,
    verify_event_schema_compatibility_contract,
    verify_feature_flag_dependency_contract,
    verify_feature_availability_contract,
    verify_feature_vector_snapshot_contract,
    verify_filesystem_disk_pressure_policy,
    verify_data_lineage_graph_contract,
    verify_delivery_traceability_contracts,
    verify_evidence_eligibility_matrix,
    verify_generated_client_contract,
    verify_human_readable_reason_contract,
    verify_identity_unit_provider_finality_contracts,
    verify_log_redaction_verification,
    verify_machine_readable_reason_contract,
    verify_markov_lifecycle_forecast_contracts,
    verify_metric_definition_registry,
    verify_mutation_command_idempotency_contract,
    verify_null_value_policy_contract,
    verify_numeric_precision_policy,
    verify_operator_runtime_safety_contracts,
    verify_pipeline_progress_invariant,
    verify_projection_version_isolation_contract,
    verify_queue_ack_nack_contract,
    verify_release_experiment_safety_contracts,
    verify_replay_build_model_contracts,
    verify_runtime_config_drift_contract,
    verify_scheduled_job_mode_gate_contract,
    verify_snapshot_compaction_invariant_contract,
    verify_snapshot_compaction_read_barrier_contract,
    verify_spec_change_impact_analysis_contract,
    verify_static_policy_enforcement,
    verify_threshold_catalog,
    verify_training_dataset_manifest_contract,
    verify_thread_pool_isolation_contract,
    verify_environment_separation_contract,
    verify_execution_exit_safety_contracts,
    verify_access_control_policy,
    verify_input_sanitization,
    verify_paper_mode_safety,
    verify_project_stop_loss,
    verify_safe_default,
    verify_safety_case,
    verify_service_readiness_probe_contract,
    verify_secret_access_audit_contract,
    verify_shadow_observation_identity_contracts,
    verify_spec_governance_feasibility_contracts,
    verify_silent_worker_death_detector_contract,
    verify_source_parser_authenticity_contracts,
    verify_top_fix_queue,
    verify_telegram_session_security_contract,
    verify_waiver_policy,
    verify_warm_start_safety_contract,
    verify_worker_heartbeat_contract,
    verify_write_path_registry,
)


def test_basic_contract_readiness_passes_seed_foundation():
    report = build_basic_contract_readiness(env={})

    assert report["health"]["status"] == "basic_contract_readiness_ok"
    assert report["blocking_contracts"] == []
    for contract_id in (
        "SpecConsistencyLinterContract",
        "NumericPrecisionContract",
        "MetricDefinitionRegistry",
        "ThresholdCatalogContract",
        "RuntimeConfigDriftContract",
        "EnvironmentSeparationContract",
        "NullValuePolicyContract",
        "FeatureAvailabilityContract",
        "FeatureVectorSnapshotContract",
        "DataLineageGraphContract",
        "TrainingDatasetManifestContract",
        "TelegramLifecycleTransitionMatrixContract",
        "LifecycleNstepForecastContract",
        "AbsorbingSemiMarkovForecastContract",
        "CompetingRiskForecastContract",
        "CensoringPolicyContract",
        "ForecastWalkForwardValidationContract",
        "HMMResearchOnlyBoundaryContract",
        "ReclaimDetector",
        "OverextensionDetector",
        "DetectorCalibrationContract",
        "CapacityPlanningContract",
        "LoadTestReplayContract",
        "LatencyAttributionContract",
        "ProviderQuotaIsolationContract",
        "EconomicCostBudgetContract",
        "OperatorAudit",
        "OperatorSafetyContract",
        "OwnershipOnCallContract",
        "AlertPolicy",
        "AlertAckEscalationPolicy",
        "KillSwitchDrillContract",
        "ReplayDeterminismCheck",
        "ReproducibleBuildContract",
        "SupplyChainSecurityContract",
        "PolicyBundleCompatibilityContract",
        "ModelExpiryContract",
        "ForecastSanityGuard",
        "RenderedSpecViewContract",
        "HealthStateEnumContract",
        "ContractLifecycleContract",
        "ObjectivePriorityContract",
        "GoalConfidenceContract",
        "FillTimeAnchorContract",
        "ExAnteVsPosthocFeasibilityContract",
        "TokenIdentityContract",
        "DataUnitContract",
        "ChainFinalityContract",
        "ProviderSchemaContract",
        "LifecycleStateMachineContract",
        "ExitExecutionStateMachine",
        "ExitPolicyContract",
        "CircuitBreakerPositionPolicy",
        "EmergencyExitJournal",
        "ExitQueueHealthContract",
        "ReconciliationPolicyContract",
        "DashboardStalenessContract",
        "SpecTraceabilityMatrix",
        "ImplementationIssueGraphContract",
        "ModuleClosureContract",
        "DecommissionPolicyContract",
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
        "HumanReadableReasonContract",
        "MachineReadableReasonContract",
        "PaperModeSafetyBoundary",
        "ChainConfigContract",
        "SourceRegistryContract",
        "ParserCanaryCorpusContract",
        "ParserAmbiguityContract",
        "TelegramForwardedMessagePolicy",
        "PremiumSourceAccessHealthContract",
        "SourceAuthenticityContract",
        "ParserConfusablesContract",
        "ImageOCRSignalPolicy",
        "SourceImpersonationDetector",
        "IdentityMergeSplitContract",
        "ReKeyingContract",
        "SourceGapBackfillBoundary",
        "ObservationPolicyContract",
        "CounterfactualEntryTime",
        "InputSanitizationContract",
        "SafeDefaultContract",
        "ProjectStopLossContract",
        "EvidenceEligibilityMatrix",
        "TopFixQueueContract",
        "SafetyCaseContract",
        "WaiverPolicyContract",
        "AccessControlContract",
        "AuditLogIntegrityContract",
        "WritePathRegistryContract",
        "DirectDatabaseMutationBan",
        "AggregateBoundaryContract",
        "ClockRollbackGuardContract",
        "EventSchemaCompatibilityContract",
        "EnumEvolutionContract",
        "MutationCommandIdempotencyContract",
        "ProjectionVersionIsolationContract",
        "SnapshotCompactionInvariantContract",
        "SnapshotCompactionReadBarrier",
        "WorkerHeartbeatContract",
        "SilentWorkerDeathDetector",
        "WarmStartSafetyContract",
        "ConnectionPoolPartitionContract",
        "DBLockContentionPolicy",
        "DatabaseTransactionIsolationContract",
        "DistributedLockBackendHealthContract",
        "BackgroundJobRegistryContract",
        "ScheduledJobModeGateContract",
        "EntryPointInventoryContract",
        "StaticPolicyEnforcementContract",
        "FeatureFlagDependencyContract",
        "FilesystemDiskPressurePolicy",
        "APIResponseContract",
        "APIResponseEnvelopeContract",
        "ErrorTaxonomyContract",
        "LogRedactionVerificationContract",
        "AdminSessionSecurityContract",
        "SecretAccessAuditContract",
        "TelegramSessionSecurityContract",
        "QueueAckNackContract",
        "PipelineProgressInvariant",
        "ThreadPoolIsolationContract",
        "CICDMergeGateContract",
        "GeneratedClientContract",
        "SpecChangeImpactAnalysisContract",
        "ServiceReadinessProbeContract",
        "DashboardActionSeparationContract",
    ):
        assert report["contracts"][contract_id]["status"] == "pass"


def test_source_parser_authenticity_policy_verifies_shadow_contracts():
    reports = {item["contract_id"]: item for item in verify_source_parser_authenticity_contracts()}

    for contract_id in (
        "ParserCanaryCorpusContract",
        "ParserAmbiguityContract",
        "TelegramForwardedMessagePolicy",
        "PremiumSourceAccessHealthContract",
        "SourceAuthenticityContract",
        "ParserConfusablesContract",
        "ImageOCRSignalPolicy",
        "SourceImpersonationDetector",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["ParserCanaryCorpusContract"]["evidence"]["failure_count"] == 0
    assert reports["ParserAmbiguityContract"]["evidence"]["malformed_ambiguity_cases"] == []
    assert reports["TelegramForwardedMessagePolicy"]["evidence"]["quarantined_unknown_forwarded_count"] == 1
    assert reports["PremiumSourceAccessHealthContract"]["evidence"]["missing_probe_source_ids"] == []
    assert reports["SourceAuthenticityContract"]["evidence"]["missing_authenticity_source_ids"] == []
    assert reports["ImageOCRSignalPolicy"]["evidence"]["malformed_ocr_policies"] == []
    assert reports["SourceImpersonationDetector"]["evidence"]["high_confidence_quarantine_count"] == 1


def test_source_parser_authenticity_policy_blocks_bad_ambiguity_case(tmp_path):
    policy = json.loads(Path("config/v27-source-parser-auth-policy.json").read_text(encoding="utf-8"))
    policy["parser_ambiguity_cases"][0]["selected_anchor"] = "missing_anchor"
    policy_path = tmp_path / "source-parser-auth-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_source_parser_authenticity_contracts(policy_path=policy_path)}

    ambiguity = reports["ParserAmbiguityContract"]
    assert ambiguity["status"] == "missing_evidence"
    assert ambiguity["blocking_reason"] == "parser_ambiguity_policy_missing_malformed_or_unenforced"
    assert ambiguity["evidence"]["malformed_ambiguity_cases"][0]["violations"] == ["selected_anchor_not_in_candidates"]


def test_shadow_observation_identity_policy_verifies_shadow_contracts():
    reports = {item["contract_id"]: item for item in verify_shadow_observation_identity_contracts()}

    for contract_id in (
        "IdentityMergeSplitContract",
        "ReKeyingContract",
        "SourceGapBackfillBoundary",
        "ObservationPolicyContract",
        "CounterfactualEntryTime",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["IdentityMergeSplitContract"]["evidence"]["malformed_merge_split_records"] == []
    assert reports["ReKeyingContract"]["evidence"]["old_key_retired_count"] == 1
    assert reports["SourceGapBackfillBoundary"]["evidence"]["research_only_backfill_count"] == 1
    assert reports["ObservationPolicyContract"]["evidence"]["rejecting_observation_policy_count"] == 1
    assert reports["CounterfactualEntryTime"]["evidence"]["leak_free_counterfactual_entry_time_count"] == 1


def test_shadow_observation_identity_policy_blocks_future_leaky_backfill(tmp_path):
    policy = json.loads(Path("config/v27-shadow-observation-identity-policy.json").read_text(encoding="utf-8"))
    policy["source_gap_backfill_boundaries"][0]["allowed_fields"].append("peak_pnl")
    policy_path = tmp_path / "shadow-observation-identity-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_shadow_observation_identity_contracts(policy_path=policy_path)}

    backfill = reports["SourceGapBackfillBoundary"]
    assert backfill["status"] == "missing_evidence"
    assert backfill["blocking_reason"] == "source_gap_backfill_boundary_missing_malformed_or_live_mutating"
    assert backfill["evidence"]["malformed_source_gap_backfill_boundaries"][0]["violations"] == ["allowed_fields_include_future_outcome"]


def test_numeric_precision_policy_verifies_units_samples_and_source_anchors():
    report = verify_numeric_precision_policy()

    assert report["status"] == "pass"
    assert report["evidence"]["missing_required_units"] == []
    assert report["evidence"]["malformed_units"] == []
    assert report["evidence"]["malformed_sample_cases"] == []
    assert {case["unit"] for case in report["evidence"]["sample_results"]} >= {
        "basis_points",
        "market_cap_usd",
        "percentage",
        "price_quote",
        "sol",
        "token_base_units",
        "unix_ms",
    }


def test_metric_definition_registry_and_threshold_catalog_verify_hashes_and_links():
    metrics = verify_metric_definition_registry()
    thresholds = verify_threshold_catalog()

    assert metrics["status"] == "pass"
    assert thresholds["status"] == "pass"
    assert metrics["evidence"]["missing_required_metrics"] == []
    assert thresholds["evidence"]["missing_required_thresholds"] == []
    assert metrics["evidence"]["malformed_metrics"] == []
    assert thresholds["evidence"]["malformed_thresholds"] == []


def test_metric_definition_registry_blocks_hash_drift(tmp_path):
    registry = json.loads(Path("config/v27-metric-definition-registry.json").read_text(encoding="utf-8"))
    registry["metrics"][0]["formula"] = "captured / hidden_denominator"
    registry_path = tmp_path / "metric-definition-registry.json"
    registry_path.write_text(json.dumps(registry), encoding="utf-8")

    report = verify_metric_definition_registry(registry_path=registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "metric_definition_registry_missing_malformed_or_drifted"
    assert "metric_hash_mismatch" in report["evidence"]["malformed_metrics"][0]["violations"]


def test_threshold_catalog_blocks_unknown_metric_and_hash_drift(tmp_path):
    catalog = json.loads(Path("config/v27-threshold-catalog.json").read_text(encoding="utf-8"))
    catalog["thresholds"][0]["applies_to_metric"] = "unregistered_metric"
    catalog["thresholds"][0]["threshold_value"] = 0.61
    catalog_path = tmp_path / "threshold-catalog.json"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = verify_threshold_catalog(threshold_catalog_path=catalog_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "threshold_catalog_missing_malformed_or_drifted"
    violations = set(report["evidence"]["malformed_thresholds"][0]["violations"])
    assert {"applies_to_metric_unknown", "threshold_hash_mismatch"} <= violations


def test_threshold_catalog_blocks_hardcoded_governance_literal(tmp_path):
    source = tmp_path / "governance.py"
    source.write_text("CAPTURE_RATE = 0.60\n", encoding="utf-8")
    catalog = json.loads(Path("config/v27-threshold-catalog.json").read_text(encoding="utf-8"))
    catalog["hardcoded_threshold_guard"]["protected_source_files"] = [str(source)]
    catalog_path = tmp_path / "threshold-catalog.json"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

    report = verify_threshold_catalog(threshold_catalog_path=catalog_path)

    assert report["status"] == "missing_evidence"
    assert "hardcoded_threshold_literal_detected" in report["evidence"]["hardcode_guard_errors"]
    assert report["evidence"]["hardcode_guard"]["violations"][0]["threshold_id"] == "thr_capture_rate_d3a_24h_min"


def test_runtime_config_and_environment_separation_verify_hashes_and_runtime_boundary():
    runtime = verify_runtime_config_drift_contract(env={})
    environment = verify_environment_separation_contract(env={})

    assert runtime["status"] == "pass"
    assert environment["status"] == "pass"
    assert runtime["evidence"]["malformed_profiles"] == []
    assert environment["evidence"]["malformed_environments"] == []
    assert environment["evidence"]["runtime_violations"] == []


def test_runtime_config_blocks_env_drift(tmp_path):
    policy = json.loads(Path("config/v27-runtime-config-drift-policy.json").read_text(encoding="utf-8"))
    policy_path = tmp_path / "runtime-config-drift-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_runtime_config_drift_contract(policy_path=policy_path, env={"V27_ENVIRONMENT_ID": "paper"})

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "runtime_config_drift_missing_malformed_or_drifted"
    violations = set(report["evidence"]["malformed_profiles"][0]["violations"])
    assert {"env_var_drift", "env_vars_hash_mismatch", "runtime_config_hash_mismatch", "expected_hash_mismatch"} <= violations


def test_environment_separation_blocks_live_capability():
    report = verify_environment_separation_contract(env={"PREMIUM_LIVE_EXECUTION_ENABLED": "true", "LIVE_PRIVATE_KEY": "secret"})

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "environment_separation_missing_malformed_or_contaminated"
    assert {"live_execution_env_enabled", "live_secret_present"} <= set(report["evidence"]["runtime_violations"])


def test_training_truth_contracts_verify_no_leakage_hashes_and_lineage():
    null_policy = verify_null_value_policy_contract()
    feature_availability = verify_feature_availability_contract()
    feature_vector = verify_feature_vector_snapshot_contract()
    lineage = verify_data_lineage_graph_contract()
    manifest = verify_training_dataset_manifest_contract()

    assert null_policy["status"] == "pass"
    assert feature_availability["status"] == "pass"
    assert feature_vector["status"] == "pass"
    assert lineage["status"] == "pass"
    assert manifest["status"] == "pass"
    assert null_policy["evidence"]["missing_required_fields"] == []
    assert feature_availability["evidence"]["missing_required_features"] == []
    assert feature_vector["evidence"]["malformed_feature_vector_snapshots"] == []
    assert lineage["evidence"]["missing_required_node_types"] == []
    assert manifest["evidence"]["malformed_manifests"] == []


def test_detector_shadow_calibration_contracts_verify_shadow_only_detectors():
    reports = {item["contract_id"]: item for item in verify_detector_shadow_calibration_contracts()}

    assert reports["ReclaimDetector"]["status"] == "pass"
    assert reports["OverextensionDetector"]["status"] == "pass"
    assert reports["DetectorCalibrationContract"]["status"] == "pass"
    assert reports["ReclaimDetector"]["evidence"]["malformed_detector_records"] == []
    assert reports["DetectorCalibrationContract"]["evidence"]["malformed_calibrations"] == []


def test_detector_shadow_calibration_blocks_reclaim_gate_drift(tmp_path):
    policy = json.loads(Path("config/v27-detector-shadow-calibration-policy.json").read_text(encoding="utf-8"))
    policy["detectors"][0]["gate_allowed"] = True
    policy_path = tmp_path / "detector-shadow-calibration-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_detector_shadow_calibration_contracts(policy_path=policy_path)}

    reclaim = reports["ReclaimDetector"]
    assert reclaim["status"] == "missing_evidence"
    assert reclaim["blocking_reason"] == "reclaim_detector_missing_malformed_or_unsafe"
    violations = set(reclaim["evidence"]["malformed_detector_records"][0]["violations"])
    assert {"gate_allowed_must_be_false", "detector_hash_mismatch"} <= violations


def test_detector_shadow_calibration_blocks_contaminated_or_failed_calibration(tmp_path):
    policy = json.loads(Path("config/v27-detector-shadow-calibration-policy.json").read_text(encoding="utf-8"))
    policy["calibrations"][0]["contaminated_sample_count"] = 1
    policy["calibrations"][1]["observed_value"] = 0.3
    policy_path = tmp_path / "detector-shadow-calibration-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_detector_shadow_calibration_contracts(policy_path=policy_path)}

    calibration = reports["DetectorCalibrationContract"]
    assert calibration["status"] == "missing_evidence"
    assert calibration["blocking_reason"] == "detector_calibration_missing_malformed_or_contaminated"
    first = set(calibration["evidence"]["malformed_calibrations"][0]["violations"])
    second = set(calibration["evidence"]["malformed_calibrations"][1]["violations"])
    assert {"contaminated_samples_not_allowed", "calibration_hash_mismatch"} <= first
    assert {"observed_value_fails_threshold", "calibration_hash_mismatch"} <= second


def test_capacity_load_latency_contracts_verify_runtime_burst_safety():
    reports = {item["contract_id"]: item for item in verify_capacity_load_latency_contracts()}

    for contract_id in (
        "CapacityPlanningContract",
        "LoadTestReplayContract",
        "LatencyAttributionContract",
        "ProviderQuotaIsolationContract",
        "EconomicCostBudgetContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["CapacityPlanningContract"]["evidence"]["missing_capacity_components"] == []
    assert reports["LoadTestReplayContract"]["evidence"]["missing_load_scenarios"] == []
    assert reports["LatencyAttributionContract"]["evidence"]["malformed_latency_attributions"] == []
    assert reports["ProviderQuotaIsolationContract"]["evidence"]["malformed_quota_records"] == []
    assert reports["EconomicCostBudgetContract"]["evidence"]["malformed_budgets"] == []


def test_capacity_planning_blocks_missing_exit_component_and_low_headroom(tmp_path):
    policy = json.loads(Path("config/v27-capacity-load-latency-policy.json").read_text(encoding="utf-8"))
    policy["capacity_plans"] = [
        item for item in policy["capacity_plans"] if item["component"] != "exit_executor"
    ]
    policy["capacity_plans"][0]["headroom_pct"] = 0.1
    policy_path = tmp_path / "capacity-load-latency-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_capacity_load_latency_contracts(policy_path=policy_path)}

    capacity = reports["CapacityPlanningContract"]
    assert capacity["status"] == "missing_evidence"
    assert capacity["blocking_reason"] == "capacity_planning_missing_malformed_or_insufficient"
    assert "exit_executor" in capacity["evidence"]["missing_capacity_components"]
    violations = set(capacity["evidence"]["malformed_capacity_plans"][0]["violations"])
    assert {"record_value_fails_threshold", "headroom_below_degradation_threshold", "capacity_hash_mismatch"} <= violations


def test_load_latency_quota_budget_block_when_runtime_safety_breaks(tmp_path):
    policy = json.loads(Path("config/v27-capacity-load-latency-policy.json").read_text(encoding="utf-8"))
    policy["load_tests"][0]["pass_fail"] = "fail"
    policy["latency_attributions"][0]["peak_ts"] = "2026-05-27T00:00:05Z"
    policy["provider_quota_isolation"][0]["priority_order"] = ["random_control_polling", "open_position_exit"]
    policy["economic_cost_budgets"][1]["reserved_for"].append("exit_safety")
    policy_path = tmp_path / "capacity-load-latency-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_capacity_load_latency_contracts(policy_path=policy_path)}

    assert reports["LoadTestReplayContract"]["status"] == "missing_evidence"
    assert "load_test_not_pass" in reports["LoadTestReplayContract"]["evidence"]["malformed_load_tests"][0]["violations"]
    assert reports["LatencyAttributionContract"]["status"] == "missing_evidence"
    assert "simulated_fill_ts_after_peak_ts" in reports["LatencyAttributionContract"]["evidence"]["malformed_latency_attributions"][0]["violations"]
    assert reports["ProviderQuotaIsolationContract"]["status"] == "missing_evidence"
    assert "exit_and_entry_priorities_not_first" in reports["ProviderQuotaIsolationContract"]["evidence"]["malformed_quota_records"][0]["violations"]
    assert reports["EconomicCostBudgetContract"]["status"] == "missing_evidence"
    assert "exploration_budget_cannot_reserve_exit_safety" in reports["EconomicCostBudgetContract"]["evidence"]["malformed_budgets"][0]["violations"]


def test_operator_runtime_safety_contracts_verify_operator_alert_and_kill_switch_safety():
    reports = {item["contract_id"]: item for item in verify_operator_runtime_safety_contracts()}

    for contract_id in (
        "OperatorAudit",
        "OperatorSafetyContract",
        "OwnershipOnCallContract",
        "AlertPolicy",
        "AlertAckEscalationPolicy",
        "KillSwitchDrillContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["OperatorAudit"]["evidence"]["malformed_operator_audits"] == []
    assert reports["OperatorSafetyContract"]["evidence"]["unsafe_allowed_count"] == 0
    assert reports["OwnershipOnCallContract"]["evidence"]["missing_ownership_components"] == []
    assert reports["AlertPolicy"]["evidence"]["missing_alert_ids"] == []
    assert reports["AlertAckEscalationPolicy"]["evidence"]["malformed_alert_acks"] == []
    assert reports["KillSwitchDrillContract"]["evidence"]["missing_kill_switch_types"] == []


def test_operator_runtime_safety_blocks_unsafe_operator_alert_and_kill_switch_breaks(tmp_path):
    policy = json.loads(Path("config/v27-operator-runtime-safety-policy.json").read_text(encoding="utf-8"))
    policy["operator_audits"][0]["approval_status"] = "pending"
    policy["operator_safety_checks"][0]["dashboard_freshness_ok"] = False
    policy["ownership_oncall"] = [
        item for item in policy["ownership_oncall"] if item["component"] != "exit_executor"
    ]
    policy["alert_policies"][0]["auto_action"] = ["operator_ack_required"]
    policy["alert_ack_escalations"][0]["acked_at"] = "2026-05-27T01:06:00Z"
    policy["kill_switch_drills"][0]["new_entry_blocked"] = False
    policy_path = tmp_path / "operator-runtime-safety-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_operator_runtime_safety_contracts(policy_path=policy_path)}

    assert reports["OperatorAudit"]["status"] == "missing_evidence"
    assert "approval_status_not_approved" in reports["OperatorAudit"]["evidence"]["malformed_operator_audits"][0]["violations"]
    assert reports["OperatorSafetyContract"]["status"] == "missing_evidence"
    safety_violations = set(reports["OperatorSafetyContract"]["evidence"]["malformed_safety_checks"][0]["violations"])
    assert {"dashboard_freshness_required_for_high_danger", "safety_hash_mismatch"} <= safety_violations
    assert reports["OwnershipOnCallContract"]["status"] == "missing_evidence"
    assert "exit_executor" in reports["OwnershipOnCallContract"]["evidence"]["missing_ownership_components"]
    assert reports["AlertPolicy"]["status"] == "missing_evidence"
    assert "p0_alert_must_disable_new_entry" in reports["AlertPolicy"]["evidence"]["malformed_alert_policies"][0]["violations"]
    assert reports["AlertAckEscalationPolicy"]["status"] == "missing_evidence"
    assert "alert_ack_chronology_invalid" in reports["AlertAckEscalationPolicy"]["evidence"]["malformed_alert_acks"][0]["violations"]
    assert reports["KillSwitchDrillContract"]["status"] == "missing_evidence"
    assert "new_entry_not_blocked" in reports["KillSwitchDrillContract"]["evidence"]["malformed_kill_switch_drills"][0]["violations"]


def test_replay_build_model_contracts_verify_replay_build_and_forecast_trust():
    reports = {item["contract_id"]: item for item in verify_replay_build_model_contracts()}

    for contract_id in (
        "ReplayDeterminismCheck",
        "ReproducibleBuildContract",
        "SupplyChainSecurityContract",
        "PolicyBundleCompatibilityContract",
        "ModelExpiryContract",
        "ForecastSanityGuard",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["ReplayDeterminismCheck"]["evidence"]["malformed_replay_checks"] == []
    assert reports["ReproducibleBuildContract"]["evidence"]["malformed_builds"] == []
    assert reports["SupplyChainSecurityContract"]["evidence"]["missing_artifact_types"] == []
    assert reports["PolicyBundleCompatibilityContract"]["evidence"]["malformed_bundles"] == []
    assert reports["ModelExpiryContract"]["evidence"]["malformed_model_expiry"] == []
    assert reports["ForecastSanityGuard"]["evidence"]["malformed_forecast_rows"] == []


def test_replay_build_model_contracts_block_nondeterministic_unsigned_expired_or_unsafe_forecast(tmp_path):
    policy = json.loads(Path("config/v27-replay-build-model-policy.json").read_text(encoding="utf-8"))
    policy["replay_determinism_checks"][0]["pass_fail"] = "fail"
    policy["reproducible_builds"][0]["code_commit_hash"] = "not-a-sha"
    policy["supply_chain_artifacts"][0]["signature_status"] = "unsigned"
    policy["policy_bundle_compatibility"][0]["compatibility_status"] = "incompatible"
    policy["model_expiry"][0]["checked_at"] = "2026-05-27T04:00:00Z"
    policy["forecast_sanity_guards"][0]["fallback_level"] = "global"
    policy["forecast_sanity_guards"][0]["sanitized_forecast"] = 0.9
    policy_path = tmp_path / "replay-build-model-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_replay_build_model_contracts(policy_path=policy_path)}

    assert reports["ReplayDeterminismCheck"]["status"] == "missing_evidence"
    assert "replay_check_not_pass" in reports["ReplayDeterminismCheck"]["evidence"]["malformed_replay_checks"][0]["violations"]
    assert reports["ReproducibleBuildContract"]["status"] == "missing_evidence"
    assert "code_commit_hash_must_be_sha256" in reports["ReproducibleBuildContract"]["evidence"]["malformed_builds"][0]["violations"]
    assert reports["SupplyChainSecurityContract"]["status"] == "missing_evidence"
    assert "signature_not_verified" in reports["SupplyChainSecurityContract"]["evidence"]["malformed_artifacts"][0]["violations"]
    assert reports["PolicyBundleCompatibilityContract"]["status"] == "missing_evidence"
    assert "policy_bundle_not_compatible" in reports["PolicyBundleCompatibilityContract"]["evidence"]["malformed_bundles"][0]["violations"]
    assert reports["ModelExpiryContract"]["status"] == "missing_evidence"
    assert "model_expired_or_checked_before_training" in reports["ModelExpiryContract"]["evidence"]["malformed_model_expiry"][0]["violations"]
    assert reports["ForecastSanityGuard"]["status"] == "missing_evidence"
    forecast_violations = set(reports["ForecastSanityGuard"]["evidence"]["malformed_forecast_rows"][0]["violations"])
    assert {"sanitized_forecast_exceeds_raw", "global_fallback_cannot_high_conviction"} <= forecast_violations


def test_markov_lifecycle_forecast_contracts_verify_shadow_only_no_lookahead_boundary():
    reports = {item["contract_id"]: item for item in verify_markov_lifecycle_forecast_contracts()}

    for contract_id in (
        "TelegramLifecycleTransitionMatrixContract",
        "LifecycleNstepForecastContract",
        "AbsorbingSemiMarkovForecastContract",
        "CompetingRiskForecastContract",
        "CensoringPolicyContract",
        "ForecastWalkForwardValidationContract",
        "HMMResearchOnlyBoundaryContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["TelegramLifecycleTransitionMatrixContract"]["evidence"]["transition_matrix_errors"] == []
    assert reports["ForecastWalkForwardValidationContract"]["evidence"]["malformed_walk_forward_validations"] == []
    assert reports["HMMResearchOnlyBoundaryContract"]["evidence"]["malformed_hmm_boundaries"] == []


def test_markov_lifecycle_forecast_contracts_block_entry_leaky_hmm_and_lookahead(tmp_path):
    policy = json.loads(Path("config/v27-markov-lifecycle-forecast-policy.json").read_text(encoding="utf-8"))
    policy["hmm_research_only_boundaries"][0]["full_sequence_viterbi_allowed"] = True
    policy["hmm_research_only_boundaries"][0]["entry_gate_allowed"] = True
    policy["walk_forward_validations"][0]["no_lookahead_proof"] = "uses full future outcome labels"
    policy["walk_forward_validations"][0]["promotion_allowed"] = True
    policy_path = tmp_path / "markov-lifecycle-forecast-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_markov_lifecycle_forecast_contracts(policy_path=policy_path)}

    assert reports["ForecastWalkForwardValidationContract"]["status"] == "missing_evidence"
    walk_forward_violations = set(
        reports["ForecastWalkForwardValidationContract"]["evidence"]["malformed_walk_forward_validations"][0]["violations"]
    )
    assert {"no_lookahead_proof_must_reference_cutoff", "promotion_allowed_must_be_false"} <= walk_forward_violations
    assert reports["HMMResearchOnlyBoundaryContract"]["status"] == "missing_evidence"
    hmm_violations = set(reports["HMMResearchOnlyBoundaryContract"]["evidence"]["malformed_hmm_boundaries"][0]["violations"])
    assert {"full_sequence_viterbi_must_be_false", "entry_gate_allowed_must_be_false"} <= hmm_violations


def test_spec_governance_feasibility_contracts_verify_spec_health_confidence_and_no_leakage():
    reports = {item["contract_id"]: item for item in verify_spec_governance_feasibility_contracts()}

    for contract_id in (
        "RenderedSpecViewContract",
        "HealthStateEnumContract",
        "ContractLifecycleContract",
        "ObjectivePriorityContract",
        "GoalConfidenceContract",
        "FillTimeAnchorContract",
        "ExAnteVsPosthocFeasibilityContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["RenderedSpecViewContract"]["evidence"]["malformed_rendered_views"] == []
    assert reports["HealthStateEnumContract"]["evidence"]["malformed_health_states"] == []
    assert reports["ContractLifecycleContract"]["evidence"]["missing_lifecycle_contracts"] == []
    assert reports["ObjectivePriorityContract"]["evidence"]["malformed_objective_conflicts"] == []
    assert reports["GoalConfidenceContract"]["evidence"]["malformed_goal_confidence"] == []
    assert reports["FillTimeAnchorContract"]["evidence"]["malformed_fill_time_anchors"] == []
    assert reports["ExAnteVsPosthocFeasibilityContract"]["evidence"]["malformed_feasibility"] == []


def test_spec_governance_feasibility_blocks_stale_unknown_inconclusive_or_leaky_rows(tmp_path):
    policy = json.loads(Path("config/v27-spec-governance-feasibility-policy.json").read_text(encoding="utf-8"))
    policy["rendered_spec_views"][0]["render_validation_status"] = "stale"
    policy["health_states"][0]["health_state"] = "UNKNOWN"
    policy["contract_lifecycle"][0]["status"] = "retired"
    policy["objective_conflicts"][0]["chosen_objective"] = "roi_expansion"
    policy["goal_confidence"][0]["denominator"] = 1
    policy["fill_time_anchors"][0]["fill_time_anchor_type"] = "decision_ts"
    policy["ex_ante_posthoc_feasibility"][0]["used_future_peak_in_ex_ante"] = True
    policy["ex_ante_posthoc_feasibility"][0]["ex_ante_source_fields"].append("future_peak_ts")
    policy_path = tmp_path / "spec-governance-feasibility-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_spec_governance_feasibility_contracts(policy_path=policy_path)}

    assert reports["RenderedSpecViewContract"]["status"] == "missing_evidence"
    assert "render_validation_not_valid" in reports["RenderedSpecViewContract"]["evidence"]["malformed_rendered_views"][0]["violations"]
    assert reports["HealthStateEnumContract"]["status"] == "missing_evidence"
    assert "unknown_state_cannot_pass_readiness" in reports["HealthStateEnumContract"]["evidence"]["malformed_health_states"][0]["violations"]
    assert reports["ContractLifecycleContract"]["status"] == "missing_evidence"
    assert "contract_not_active_gate_for_readiness" in reports["ContractLifecycleContract"]["evidence"]["malformed_lifecycles"][0]["violations"]
    assert reports["ObjectivePriorityContract"]["status"] == "missing_evidence"
    assert "chosen_objective_not_highest_priority" in reports["ObjectivePriorityContract"]["evidence"]["malformed_objective_conflicts"][0]["violations"]
    assert reports["GoalConfidenceContract"]["status"] == "missing_evidence"
    assert "denominator_below_min" in reports["GoalConfidenceContract"]["evidence"]["malformed_goal_confidence"][0]["violations"]
    assert reports["FillTimeAnchorContract"]["status"] == "missing_evidence"
    assert "fill_time_anchor_type_must_be_simulated_fill_ts" in reports["FillTimeAnchorContract"]["evidence"]["malformed_fill_time_anchors"][0]["violations"]
    assert reports["ExAnteVsPosthocFeasibilityContract"]["status"] == "missing_evidence"
    feasibility_violations = set(reports["ExAnteVsPosthocFeasibilityContract"]["evidence"]["malformed_feasibility"][0]["violations"])
    assert {"future_peak_used_in_ex_ante", "forbidden_ex_ante_source_field"} <= feasibility_violations


def test_identity_unit_provider_finality_contracts_verify_identity_units_and_schema():
    reports = {item["contract_id"]: item for item in verify_identity_unit_provider_finality_contracts()}

    for contract_id in (
        "TokenIdentityContract",
        "DataUnitContract",
        "ChainFinalityContract",
        "ProviderSchemaContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["TokenIdentityContract"]["evidence"]["malformed_identities"] == []
    assert reports["DataUnitContract"]["evidence"]["malformed_data_units"] == []
    assert reports["ChainFinalityContract"]["evidence"]["malformed_finalities"] == []
    assert reports["ProviderSchemaContract"]["evidence"]["malformed_provider_schemas"] == []


def test_identity_unit_provider_finality_blocks_dirty_identity_unit_finality_and_schema(tmp_path):
    policy = json.loads(Path("config/v27-identity-unit-provider-finality-policy.json").read_text(encoding="utf-8"))
    policy["token_identities"][0]["identity_confidence"] = 0.5
    policy["data_units"][0]["unit_validation_status"] = "invalid"
    policy["chain_finality"][0]["commitment_level"] = "confirmed"
    policy["chain_finality"][0]["chain_reorg_detected"] = True
    policy["provider_schemas"][0]["schema_drift_detected"] = True
    policy["provider_schemas"][0]["canary_parse_result"] = "fail"
    policy_path = tmp_path / "identity-unit-provider-finality-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_identity_unit_provider_finality_contracts(policy_path=policy_path)}

    assert reports["TokenIdentityContract"]["status"] == "missing_evidence"
    assert "identity_hash_mismatch" in reports["TokenIdentityContract"]["evidence"]["malformed_identities"][0]["violations"]
    assert "observed_value_must_match_identity_confidence" in reports["TokenIdentityContract"]["evidence"]["malformed_identities"][0]["violations"]
    assert reports["DataUnitContract"]["status"] == "missing_evidence"
    assert "unit_validation_status_not_valid" in reports["DataUnitContract"]["evidence"]["malformed_data_units"][0]["violations"]
    assert reports["ChainFinalityContract"]["status"] == "missing_evidence"
    finality_violations = set(reports["ChainFinalityContract"]["evidence"]["malformed_finalities"][0]["violations"])
    assert {"commitment_level_below_minimum", "chain_reorg_detected"} <= finality_violations
    assert reports["ProviderSchemaContract"]["status"] == "missing_evidence"
    schema_violations = set(reports["ProviderSchemaContract"]["evidence"]["malformed_provider_schemas"][0]["violations"])
    assert {"schema_drift_detected", "canary_parse_result_not_pass"} <= schema_violations


def test_execution_exit_safety_contracts_verify_lifecycle_exit_and_queue_safety():
    reports = {item["contract_id"]: item for item in verify_execution_exit_safety_contracts()}

    for contract_id in (
        "LifecycleStateMachineContract",
        "ExitExecutionStateMachine",
        "ExitPolicyContract",
        "CircuitBreakerPositionPolicy",
        "EmergencyExitJournal",
        "ExitQueueHealthContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["LifecycleStateMachineContract"]["evidence"]["malformed_lifecycle_machines"] == []
    assert reports["ExitExecutionStateMachine"]["evidence"]["malformed_exit_state_machines"] == []
    assert reports["ExitPolicyContract"]["evidence"]["malformed_exit_policies"] == []
    assert reports["CircuitBreakerPositionPolicy"]["evidence"]["malformed_circuit_breakers"] == []
    assert reports["EmergencyExitJournal"]["evidence"]["malformed_emergency_journals"] == []
    assert reports["ExitQueueHealthContract"]["evidence"]["malformed_exit_queue_health"] == []


def test_execution_exit_safety_blocks_unsafe_exit_policy_circuit_breaker_and_queue(tmp_path):
    policy = json.loads(Path("config/v27-execution-exit-safety-policy.json").read_text(encoding="utf-8"))
    policy["lifecycle_state_machines"][0]["state_version_fencing_required"] = False
    policy["exit_execution_state_machines"][0]["exit_safety_preserved"] = False
    policy["exit_policies"][0]["entry_outcome_separation"] = False
    policy["circuit_breaker_position_policies"][0]["new_entry_disabled"] = False
    policy["emergency_exit_journals"][0]["reconciled_to_ledger"] = False
    policy["exit_queue_health"][0]["exit_queue_status"] = "degraded"
    policy["exit_queue_health"][0]["stuck_open_position_count"] = 1
    policy_path = tmp_path / "execution-exit-safety-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_execution_exit_safety_contracts(policy_path=policy_path)}

    assert reports["LifecycleStateMachineContract"]["status"] == "missing_evidence"
    assert "state_version_fencing_required" in reports["LifecycleStateMachineContract"]["evidence"]["malformed_lifecycle_machines"][0]["violations"]
    assert reports["ExitExecutionStateMachine"]["status"] == "missing_evidence"
    assert "exit_safety_must_be_preserved" in reports["ExitExecutionStateMachine"]["evidence"]["malformed_exit_state_machines"][0]["violations"]
    assert reports["ExitPolicyContract"]["status"] == "missing_evidence"
    assert "entry_outcome_separation_required" in reports["ExitPolicyContract"]["evidence"]["malformed_exit_policies"][0]["violations"]
    assert reports["CircuitBreakerPositionPolicy"]["status"] == "missing_evidence"
    assert "new_entry_must_be_disabled" in reports["CircuitBreakerPositionPolicy"]["evidence"]["malformed_circuit_breakers"][0]["violations"]
    assert reports["EmergencyExitJournal"]["status"] == "missing_evidence"
    assert "journal_must_reconcile_to_ledger" in reports["EmergencyExitJournal"]["evidence"]["malformed_emergency_journals"][0]["violations"]
    assert reports["ExitQueueHealthContract"]["status"] == "missing_evidence"
    queue_violations = set(reports["ExitQueueHealthContract"]["evidence"]["malformed_exit_queue_health"][0]["violations"])
    assert {"exit_queue_status_not_healthy", "stuck_open_position_count_must_be_zero"} <= queue_violations


def test_delivery_traceability_contracts_verify_reconciliation_dashboard_and_delivery_chain():
    reports = {item["contract_id"]: item for item in verify_delivery_traceability_contracts()}

    for contract_id in (
        "ReconciliationPolicyContract",
        "DashboardStalenessContract",
        "SpecTraceabilityMatrix",
        "ImplementationIssueGraphContract",
        "ModuleClosureContract",
        "DecommissionPolicyContract",
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["ReconciliationPolicyContract"]["evidence"]["malformed_reconciliation_policies"] == []
    assert reports["DashboardStalenessContract"]["evidence"]["malformed_dashboard_staleness_panels"] == []
    assert reports["SpecTraceabilityMatrix"]["evidence"]["malformed_traceability_rows"] == []
    assert reports["ImplementationIssueGraphContract"]["evidence"]["malformed_issues"] == []
    assert reports["ModuleClosureContract"]["evidence"]["malformed_module_closures"] == []
    assert reports["DecommissionPolicyContract"]["evidence"]["malformed_decommission_policies"] == []


def test_delivery_traceability_blocks_unsafe_repair_stale_dashboard_and_retired_route(tmp_path):
    policy = json.loads(Path("config/v27-delivery-traceability-policy.json").read_text(encoding="utf-8"))
    policy["reconciliation_policies"][0]["auto_repair_allowed"] = True
    policy["dashboard_staleness_panels"][0]["panel_lag_sec"] = 90
    policy["dashboard_staleness_panels"][0]["operator_override_allowed"] = True
    policy["spec_traceability_matrix"][0]["status"] = "not_started"
    policy["implementation_issue_graph"][0]["acceptance_tests"] = []
    policy["module_closures"][0]["contract_tests"][0]["status"] = "fail"
    policy["decommission_policies"][0]["runtime_reference_allowed"] = True
    policy["decommission_policies"][0]["direct_entry_allowed"] = True
    policy_path = tmp_path / "delivery-traceability-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_delivery_traceability_contracts(policy_path=policy_path)}

    assert reports["ReconciliationPolicyContract"]["status"] == "missing_evidence"
    assert "ledger_mismatch_cannot_auto_repair" in reports["ReconciliationPolicyContract"]["evidence"]["malformed_reconciliation_policies"][0]["violations"]
    assert reports["DashboardStalenessContract"]["status"] == "missing_evidence"
    dashboard_violations = set(reports["DashboardStalenessContract"]["evidence"]["malformed_dashboard_staleness_panels"][0]["violations"])
    assert {"panel_lag_above_threshold", "operator_override_must_be_disabled_on_dashboard_panel"} <= dashboard_violations
    assert reports["SpecTraceabilityMatrix"]["status"] == "missing_evidence"
    assert "traceability_status_not_tested_deployed_or_validated" in reports["SpecTraceabilityMatrix"]["evidence"]["malformed_traceability_rows"][0]["violations"]
    assert reports["ImplementationIssueGraphContract"]["status"] == "missing_evidence"
    assert "acceptance_tests_required_for_done" in reports["ImplementationIssueGraphContract"]["evidence"]["malformed_issues"][0]["violations"]
    assert reports["ModuleClosureContract"]["status"] == "missing_evidence"
    assert "contract_tests_must_pass" in reports["ModuleClosureContract"]["evidence"]["malformed_module_closures"][0]["violations"]
    assert reports["DecommissionPolicyContract"]["status"] == "missing_evidence"
    decommission_violations = set(reports["DecommissionPolicyContract"]["evidence"]["malformed_decommission_policies"][0]["violations"])
    assert {"retired_artifact_runtime_reference_forbidden", "direct_entry_must_be_false"} <= decommission_violations


def test_release_experiment_safety_contracts_verify_release_blockers():
    reports = {item["contract_id"]: item for item in verify_release_experiment_safety_contracts()}

    for contract_id in (
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
    ):
        assert reports[contract_id]["status"] == "pass"

    assert reports["SecretsManagementContract"]["evidence"]["malformed_secrets"] == []
    assert reports["SystemSLO"]["evidence"]["malformed_slos"] == []
    assert reports["NoTradeRootCause"]["evidence"]["malformed_no_trade_root_causes"] == []
    assert reports["ReleaseComplexityBudget"]["evidence"]["malformed_release_complexity"] == []
    assert reports["BackpressurePolicy"]["evidence"]["malformed_backpressure_policies"] == []
    assert reports["BudgetReserveContract"]["evidence"]["malformed_budget_reserves"] == []
    assert reports["BlindedHoldoutContract"]["evidence"]["malformed_holdouts"] == []
    assert reports["ManualOverrideContract"]["evidence"]["malformed_manual_overrides"] == []
    assert reports["ContractTestSuite"]["evidence"]["malformed_contract_tests"] == []
    assert reports["AdversarialReplaySuite"]["evidence"]["malformed_adversarial_replays"] == []


def test_release_experiment_safety_blocks_contamination_and_unsafe_runtime(tmp_path):
    policy = json.loads(Path("config/v27-release-experiment-safety-policy.json").read_text(encoding="utf-8"))
    policy["secrets_management"][0]["mutation_scope_allowed"] = True
    policy["system_slos"][0]["status"] = "degraded"
    policy["system_slos"][0]["severity"] = "P1"
    policy["no_trade_root_causes"][0]["category"] = "unknown"
    policy["release_complexity_budgets"][0]["new_gates"] = 2
    policy["backpressure_policies"][0]["drops_p0_p1_allowed"] = True
    policy["budget_reserves"][0]["borrow_allowed"] = True
    policy["blinded_holdouts"][0]["access_count"] = 1
    policy["manual_overrides"][0]["training_allowed"] = True
    policy["contract_test_suite"][0]["pass_fail"] = "fail"
    policy["adversarial_replay_suite"][0]["observed_action"] = "allowed"
    policy_path = tmp_path / "release-experiment-safety-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    reports = {item["contract_id"]: item for item in verify_release_experiment_safety_contracts(policy_path=policy_path)}

    assert reports["SecretsManagementContract"]["status"] == "missing_evidence"
    assert "dashboard_token_mutation_scope_forbidden" in reports["SecretsManagementContract"]["evidence"]["malformed_secrets"][0]["violations"]
    assert reports["SystemSLO"]["status"] == "missing_evidence"
    slo_violations = set(reports["SystemSLO"]["evidence"]["malformed_slos"][0]["violations"])
    assert {"slo_status_not_healthy", "critical_slo_unresolved"} <= slo_violations
    assert reports["NoTradeRootCause"]["status"] == "missing_evidence"
    assert "d3a_zero_fill_requires_known_root_cause" in reports["NoTradeRootCause"]["evidence"]["malformed_no_trade_root_causes"][0]["violations"]
    assert reports["ReleaseComplexityBudget"]["status"] == "missing_evidence"
    assert "new_gates_exceed_release_budget" in reports["ReleaseComplexityBudget"]["evidence"]["malformed_release_complexity"][0]["violations"]
    assert reports["BackpressurePolicy"]["status"] == "missing_evidence"
    assert "p0_p1_drop_forbidden" in reports["BackpressurePolicy"]["evidence"]["malformed_backpressure_policies"][0]["violations"]
    assert reports["BudgetReserveContract"]["status"] == "missing_evidence"
    assert "p0_p1_reserve_borrow_forbidden" in reports["BudgetReserveContract"]["evidence"]["malformed_budget_reserves"][0]["violations"]
    assert reports["BlindedHoldoutContract"]["status"] == "missing_evidence"
    assert "holdout_access_count_must_be_zero" in reports["BlindedHoldoutContract"]["evidence"]["malformed_holdouts"][0]["violations"]
    assert reports["ManualOverrideContract"]["status"] == "missing_evidence"
    assert "manual_override_training_forbidden" in reports["ManualOverrideContract"]["evidence"]["malformed_manual_overrides"][0]["violations"]
    assert reports["ContractTestSuite"]["status"] == "missing_evidence"
    assert "contract_test_not_pass" in reports["ContractTestSuite"]["evidence"]["malformed_contract_tests"][0]["violations"]
    assert reports["AdversarialReplaySuite"]["status"] == "missing_evidence"
    assert "observed_action_must_match_expected_action" in reports["AdversarialReplaySuite"]["evidence"]["malformed_adversarial_replays"][0]["violations"]


def test_null_value_policy_blocks_critical_unknown_decision_default(tmp_path):
    policy = json.loads(Path("config/v27-null-value-policy.json").read_text(encoding="utf-8"))
    policy["null_policies"][0]["default_value_allowed"] = True
    policy["null_policies"][0]["decision_allowed"] = True
    policy_path = tmp_path / "null-value-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_null_value_policy_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "null_value_policy_missing_malformed_or_unsafe"
    violations = set(report["evidence"]["malformed_policies"][0]["violations"])
    assert {
        "critical_field_default_value_forbidden",
        "critical_unknown_decision_allowed",
        "policy_hash_mismatch",
    } <= violations


def test_feature_availability_blocks_future_leakage(tmp_path):
    policy = json.loads(Path("config/v27-feature-vector-snapshot-policy.json").read_text(encoding="utf-8"))
    policy["feature_availability"][1]["feature_available_at"] = "2026-05-27T00:00:20Z"
    policy_path = tmp_path / "feature-vector-snapshot-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_feature_availability_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "feature_availability_missing_malformed_or_leaky"
    violations = set(report["evidence"]["malformed_feature_availability"][0]["violations"])
    assert {"feature_available_after_decision", "availability_hash_mismatch"} <= violations


def test_feature_vector_snapshot_blocks_future_availability_and_hash_drift(tmp_path):
    policy = json.loads(Path("config/v27-feature-vector-snapshot-policy.json").read_text(encoding="utf-8"))
    policy["feature_vector_snapshots"][0]["feature_available_at_map"]["entry_quote_price"] = "2026-05-27T00:00:20Z"
    policy_path = tmp_path / "feature-vector-snapshot-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_feature_vector_snapshot_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "feature_vector_snapshot_missing_malformed_or_unreproducible"
    violations = set(report["evidence"]["malformed_feature_vector_snapshots"][0]["violations"])
    assert {"entry_quote_price_available_after_decision", "feature_vector_hash_mismatch"} <= violations


def test_data_lineage_graph_blocks_unknown_parent_and_hash_drift(tmp_path):
    policy = json.loads(Path("config/v27-data-lineage-graph-policy.json").read_text(encoding="utf-8"))
    policy["lineage_nodes"][1]["parent_node_ids"] = ["node:missing:seed"]
    policy_path = tmp_path / "data-lineage-graph-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_data_lineage_graph_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "data_lineage_graph_missing_malformed_or_broken"
    violations = set(report["evidence"]["malformed_lineage_nodes"][0]["violations"])
    assert {"unknown_parent_node_id", "lineage_hash_mismatch"} <= violations


def test_training_dataset_manifest_blocks_spec_hash_drift(tmp_path):
    policy = json.loads(Path("config/v27-training-dataset-manifest-policy.json").read_text(encoding="utf-8"))
    policy["training_dataset_manifests"][0]["spec_hash"] = "bad_spec_hash"
    policy_path = tmp_path / "training-dataset-manifest-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_training_dataset_manifest_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "training_dataset_manifest_missing_malformed_or_unlinked"
    violations = set(report["evidence"]["malformed_manifests"][0]["violations"])
    assert {"spec_hash_mismatch", "manifest_hash_mismatch"} <= violations


def test_reason_taxonomy_contracts_bind_human_and_machine_reasons():
    human = verify_human_readable_reason_contract()
    machine = verify_machine_readable_reason_contract()

    assert human["status"] == "pass"
    assert machine["status"] == "pass"
    assert human["evidence"]["reason_count"] == machine["evidence"]["reason_count"]
    assert human["evidence"]["missing_reason_codes"] == []
    assert machine["evidence"]["missing_reason_codes"] == []
    assert human["evidence"]["malformed_reasons"] == []
    assert machine["evidence"]["malformed_reasons"] == []
    sample = machine["evidence"]["sample_reasons"][0]
    assert sample["machine_code"] == sample["reason_code"].upper()
    assert sample["blocking_contract"]
    assert sample["failure_action"]


def test_reason_taxonomy_blocks_bad_locale(tmp_path):
    policy_path = tmp_path / "reason-taxonomy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.reason_taxonomy_policy.v1",
                "scope": "unit",
                "failure_action": "reason_missing",
                "human_reason_schema_version": "v2.7.0.human_reason.v1",
                "machine_reason_schema_version": "v2.7.0.machine_reason.v1",
                "human_required_fields": ["reason_code", "human_message", "operator_action", "locale", "owner"],
                "machine_required_fields": ["reason_code", "machine_code", "schema_version", "blocking_contract", "failure_action"],
                "allowed_locales": ["fr-FR"],
                "allowed_schema_versions": ["v2.7.0.machine_reason.v1"],
                "default_locale": "en-US",
                "human_message_template": "{blocking_contract} is blocked by {reason_code}; {operator_action}.",
                "coverage": {
                    "basic_readiness_source_file": "scripts/v27_basic_contract_readiness.py",
                    "contract_catalog_file": "spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json",
                    "error_taxonomy_file": "config/v27-error-taxonomy.json",
                },
                "owner_by_category": {"default": "test"},
            }
        ),
        encoding="utf-8",
    )

    report = verify_human_readable_reason_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "reason_taxonomy_policy_missing_malformed_or_incomplete"
    assert report["evidence"]["malformed_reasons"][0]["violations"] == ["locale_not_allowed"]


def test_scheduled_job_mode_gate_derives_explicit_allow_deny_rows():
    report = verify_scheduled_job_mode_gate_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["job_count"] >= 8
    assert report["evidence"]["mode_count"] == 4
    assert report["evidence"]["gate_row_count"] == report["evidence"]["job_count"] * 4
    assert report["evidence"]["denied_rows"]
    assert report["evidence"]["malformed_rows"] == []


def test_feature_flag_dependency_policy_covers_all_runtime_env_flags():
    report = verify_feature_flag_dependency_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["feature_flag_count"] == report["evidence"]["source_feature_flag_count"]
    assert report["evidence"]["uncovered_source_flags"] == []
    assert report["evidence"]["unknown_policy_flags"] == []
    assert report["evidence"]["unknown_dependencies"] == []
    assert report["evidence"]["source_anchor_violations"] == []


def test_feature_flag_dependency_blocks_unknown_contract(tmp_path):
    policy_path = tmp_path / "feature-flags.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.feature_flag_dependencies.v1",
                "scope": "unit",
                "failure_action": "feature_flag_blocked",
                "source_file": "src/index.js",
                "feature_flag_dependencies": [
                    {
                        "feature_flag": "NODE_STARTUP_PREFLIGHT_ENABLED",
                        "depends_on": ["NotAContract"],
                        "mode_scope": ["observe_only"],
                        "dependency_state": "required_pass",
                        "activation_action": "block_until_dependencies_ready",
                        "source_anchor": "envFlag('NODE_STARTUP_PREFLIGHT_ENABLED'",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_feature_flag_dependency_contract(policy_path=policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "feature_flag_dependency_missing_malformed_or_unenforced"
    assert report["evidence"]["unknown_dependencies"] == [
        {"feature_flag": "NODE_STARTUP_PREFLIGHT_ENABLED", "dependency": "NotAContract"}
    ]


def test_filesystem_disk_pressure_policy_reads_free_space_and_wal_bytes():
    report = verify_filesystem_disk_pressure_policy()

    assert report["status"] == "pass"
    assert report["evidence"]["filesystem_count"] == 1
    measurement = report["evidence"]["measurements"][0]
    assert isinstance(measurement["free_bytes"], int)
    assert measurement["free_bytes"] >= measurement["min_free_bytes"]
    assert measurement["wal_bytes"] <= measurement["max_wal_bytes"]
    assert report["evidence"]["pressure_violations"] == []


def test_aggregate_boundary_policy_verifies_patterns_and_event_log_anchors():
    report = verify_aggregate_boundary_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["boundary_count"] >= 6
    assert report["evidence"]["missing_required_types"] == []
    assert report["evidence"]["missing_source_anchors"] == []
    assert report["evidence"]["malformed_boundaries"] == []
    assert all(item["sample_matches"] for item in report["evidence"]["pattern_results"])


def test_aggregate_boundary_blocks_bad_pattern(tmp_path):
    policy_path = tmp_path / "aggregate-boundaries.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.aggregate_boundaries.v1",
                "scope": "unit",
                "failure_action": "event_log_unhealthy",
                "source_file": "scripts/v27_event_log.py",
                "source_anchors": ["aggregate_id = event.get(\"aggregate_id\")"],
                "aggregate_boundaries": [
                    {
                        "aggregate_type": "telegram_signal",
                        "aggregate_id_pattern": "[",
                        "sequence_scope": "aggregate_id",
                        "owner_store": "v27_event_log",
                        "sample_aggregate_id": "telegram_signal:sol:So11111111111111111111111111111111111111112:unknown_pool:0",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_aggregate_boundary_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "aggregate_boundary_missing_malformed_or_unenforced"
    assert "aggregate_id_pattern_invalid" in report["evidence"]["malformed_boundaries"][0]["violations"][0]


def test_clock_rollback_guard_detects_monotonic_regression():
    report = verify_clock_rollback_guard_contract(
        clock_samples=[
            {"wall_clock_ns": 200, "monotonic_ns": 200, "wall_clock_ts": "2026-05-25T00:00:00Z"},
            {"wall_clock_ns": 199, "monotonic_ns": 201, "wall_clock_ts": "2026-05-25T00:00:01Z"},
        ]
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "clock_rollback_guard_unverified_or_dirty"
    assert report["evidence"]["rollback_detected"] is True


def test_event_schema_policy_binds_producers_consumers_and_versions():
    schema = verify_event_schema_compatibility_contract()
    enums = verify_enum_evolution_contract()
    idempotency = verify_mutation_command_idempotency_contract()

    assert schema["status"] == "pass"
    assert schema["evidence"]["event_schema_count"] >= 10
    assert schema["evidence"]["consumer_gaps"] == []
    assert schema["evidence"]["source_anchor_violations"] == []
    assert enums["status"] == "pass"
    assert set(enums["evidence"]["enum_names"]) >= {
        "event_schema_version",
        "event_type",
        "mode_target",
        "entry_mode_tier",
    }
    assert idempotency["status"] == "pass"
    assert idempotency["evidence"]["command_count"] >= 2
    assert idempotency["evidence"]["malformed_commands"] == []


def test_event_schema_policy_blocks_bad_schema_version(tmp_path):
    policy_path = tmp_path / "event-schema.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.event_schema_compatibility.v1",
                "scope": "unit",
                "failure_action": "event_rejected",
                "allowed_event_schema_versions": ["v2.7.0.seed"],
                "source_anchors": [],
                "event_schemas": [
                    {
                        "event_type": "telegram_signal_seen",
                        "schema_version": "v9.bad",
                        "producer_version": "producer",
                        "consumer_version": "consumer",
                        "compatibility_result": "backward_compatible",
                    }
                ],
                "enum_evolution": [],
                "mutation_commands": [],
            }
        ),
        encoding="utf-8",
    )

    report = verify_event_schema_compatibility_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "event_schema_compatibility_missing_malformed_or_unenforced"
    assert report["evidence"]["malformed_schemas"][0]["violations"] == ["schema_version_not_allowed"]


def test_mutation_command_idempotency_blocks_missing_dedupe_material(tmp_path):
    policy_path = tmp_path / "event-schema.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.event_schema_compatibility.v1",
                "scope": "unit",
                "failure_action": "event_rejected",
                "source_anchors": [],
                "mutation_commands": [
                    {
                        "command_id": "unit",
                        "idempotency_key": "unit:1",
                        "mutation_target": "filesystem:unit",
                        "dedupe_hash_material": [],
                        "result_hash_material": ["event_id"],
                        "sample_payload": {"event_id": "evt"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_mutation_command_idempotency_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "mutation_command_idempotency_missing_malformed_or_unenforced"
    assert "dedupe_hash_material_required" in report["evidence"]["malformed_commands"][0]["violations"]


def test_read_model_snapshot_policy_binds_projection_hashes_and_barrier():
    projection = verify_projection_version_isolation_contract()
    compaction = verify_snapshot_compaction_invariant_contract()
    barrier = verify_snapshot_compaction_read_barrier_contract()

    assert projection["status"] == "pass"
    assert projection["evidence"]["projection_keys"] == ["v27_denominator_projection:v0.1"]
    assert projection["evidence"]["malformed_rows"] == []
    assert compaction["status"] == "pass"
    assert set(compaction["evidence"]["hash_fields"]) == {"projection_hash", "snapshot_hash"}
    assert compaction["evidence"]["missing_hash_fields"] == []
    assert barrier["status"] == "pass"
    assert set(barrier["evidence"]["required_unsafe_statuses"]) == {"event_log_invalid", "not_built", "seed_empty"}
    assert barrier["evidence"]["malformed_rows"] == []


def test_snapshot_read_barrier_blocks_missing_hash_check(tmp_path):
    policy_path = tmp_path / "snapshot-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.read_model_snapshot_policy.v1",
                "scope": "unit",
                "failure_action": "dashboard_snapshot_rejected",
                "source_anchors": [],
                "read_barriers": [
                    {
                        "barrier_id": "unit",
                        "consumer": "dashboard_and_mode_readiness",
                        "required_checks": ["snapshot_schema_ok"],
                        "unsafe_statuses": ["event_log_invalid", "not_built", "seed_empty"],
                        "failure_action": "dashboard_snapshot_rejected",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_snapshot_compaction_read_barrier_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "snapshot_compaction_read_barrier_missing_malformed_or_unenforced"
    assert "required_checks_incomplete" in report["evidence"]["malformed_rows"][0]["violations"][0]


def test_runtime_worker_health_policy_binds_heartbeats_death_detectors_and_warm_start():
    heartbeat = verify_worker_heartbeat_contract()
    silent_death = verify_silent_worker_death_detector_contract()
    warm_start = verify_warm_start_safety_contract()

    assert heartbeat["status"] == "pass"
    assert set(heartbeat["evidence"]["required_roles"]) == {
        "dashboard",
        "paper-trader",
        "lifecycle-tracker",
        "v27-read-model-refresh",
    }
    assert heartbeat["evidence"]["source_anchor_violations"] == []
    assert silent_death["status"] == "pass"
    assert silent_death["evidence"]["detector_count"] >= 5
    assert "premium_node_server" in silent_death["evidence"]["detected_jobs"]
    assert silent_death["evidence"]["malformed_rows"] == []
    assert warm_start["status"] == "pass"
    assert set(warm_start["evidence"]["control_ids"]) == {
        "node_restart_preflight_before_warm_rejoin",
        "volume_preflight_before_service_start",
    }
    assert warm_start["evidence"]["malformed_rows"] == []


def test_db_runtime_concurrency_policy_binds_sqlite_and_distributed_locks():
    pool = verify_connection_pool_partition_contract()
    contention = verify_db_lock_contention_policy()
    isolation = verify_database_transaction_isolation_contract()
    backend = verify_distributed_lock_backend_health_contract()

    assert pool["status"] == "pass"
    assert set(pool["evidence"]["pool_names"]) == {
        "market_data_distributed_singleflight",
        "paper_sqlite_writer_pool",
    }
    assert pool["evidence"]["source_anchor_violations"] == []
    assert contention["status"] == "pass"
    assert set(contention["evidence"]["stores"]) == {
        "sqlite:missed_attribution",
        "sqlite:paper_trades",
        "sqlite:volume_preflight",
    }
    assert contention["evidence"]["malformed_rows"] == []
    assert isolation["status"] == "pass"
    assert set(isolation["evidence"]["stores"]) == {
        "sqlite:kline_cache",
        "sqlite:paper_decision_audit",
        "sqlite:paper_trades",
    }
    assert isolation["evidence"]["malformed_rows"] == []
    assert backend["status"] == "pass"
    assert set(backend["evidence"]["backend_names"]) == {
        "redis_market_data_singleflight",
        "sqlite_file_lock_single_writer",
    }
    assert backend["evidence"]["source_violations"] == []


def test_db_lock_contention_blocks_missing_retry_policy(tmp_path):
    policy_path = tmp_path / "db-runtime-concurrency.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.db_runtime_concurrency_policy.v1",
                "scope": "unit",
                "failure_action": "storage_or_lock_backend_degraded",
                "source_anchors": [],
                "db_lock_contention_policies": [
                    {
                        "store": "sqlite:paper_trades",
                        "lock_name": "unit",
                        "contention_threshold_ms": 30000,
                        "retry_policy": {},
                        "fallback_action": "rollback_and_retry_then_raise",
                        "source_file": "scripts/sqlite_write_coordinator.py",
                        "source_anchor": "fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_db_lock_contention_policy(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "db_lock_contention_policy_missing_malformed_or_unenforced"
    assert "retry_policy_non_empty_object_required" in report["evidence"]["malformed_rows"][0]["violations"]


def test_worker_heartbeat_blocks_missing_required_role(tmp_path):
    policy_path = tmp_path / "runtime-worker-health.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.runtime_worker_health_policy.v1",
                "scope": "unit",
                "failure_action": "worker_runtime_not_ready",
                "source_anchors": [],
                "worker_heartbeats": [
                    {
                        "event_type": "worker_fleet_heartbeat_recorded",
                        "required_roles": ["dashboard"],
                        "required_payload_fields": [
                            "worker_id",
                            "role",
                            "build_hash",
                            "runtime_config_hash",
                            "policy_bundle_id",
                            "heartbeat_at",
                        ],
                        "projection_health_key": "worker_fleet_consistency_ok",
                        "max_heartbeat_lag_ms": 300000,
                        "failure_action": "block_promotion_until_fresh_heartbeat",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_worker_heartbeat_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "worker_heartbeat_missing_malformed_or_unenforced"
    assert "required_roles_incomplete" in report["evidence"]["malformed_rows"][0]["violations"][0]


def test_paper_mode_safety_blocks_live_capabilities():
    report = verify_paper_mode_safety(
        env={
            "PREMIUM_LIVE_EXECUTION_ENABLED": "true",
            "TRADE_WALLET_PRIVATE_KEY": "secret",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert report["evidence"]["premium_live_execution_enabled"] is True
    assert report["evidence"]["live_private_key_present"] is True


def test_paper_mode_safety_consumes_clean_runtime_evidence(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "pass"
    assert report["evidence"]["runtime_evidence_present"] is True
    assert report["evidence"]["runtime_evidence_valid"] is True


def test_paper_mode_safety_allows_quarantined_live_secret_marker(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_secret_quarantine_applied": True,
                "live_secret_quarantine_reason": "node_preload_before_app_import",
                "quarantined_live_secret_names": ["TRADE_WALLET_PRIVATE_KEY"],
                "live_secret_quarantine_hash": "hash",
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(
        env={
            "V27_LIVE_SECRET_QUARANTINE_APPLIED": "true",
            "V27_LIVE_SECRET_QUARANTINE_REASON": "node_preload_before_app_import",
            "V27_QUARANTINED_LIVE_SECRET_NAMES": "TRADE_WALLET_PRIVATE_KEY",
            "V27_LIVE_SECRET_QUARANTINE_HASH": "hash",
        },
        runtime_evidence_path=evidence_path,
    )

    assert report["status"] == "pass"
    assert report["evidence"]["live_private_key_present"] is False
    assert report["evidence"]["live_secret_quarantine_applied"] is True
    assert report["evidence"]["quarantined_live_secret_names"] == ["TRADE_WALLET_PRIVATE_KEY"]
    assert report["evidence"]["runtime_evidence"]["live_secret_quarantine_applied"] is True


def test_paper_mode_safety_blocks_runtime_live_component(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": True,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert "runtime_jupiter_executor_initialized" in report["evidence"]["violations"]


def test_input_sanitization_redacts_raw_telegram_text():
    report = verify_input_sanitization()

    assert report["status"] == "pass"
    assert report["evidence"]["payload_schema_valid"] is True
    assert report["evidence"]["raw_message_hash_present"] is True
    assert report["evidence"]["legacy_raw_message_leaked"] is False


def test_safe_default_requires_blocked_shadow_defaults(tmp_path):
    registry_path = tmp_path / "entry-mode-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "tiers": {"live": {"paper_enabled": True}},
                "modes": {
                    "unit_live": {
                        "tier": "live",
                        "paper_enabled": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report = verify_safe_default(registry_path=registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "safe_default_fail_closed_unverified"
    assert report["evidence"]["blocked_mode_count"] == 0
    assert report["evidence"]["default_action"] == "fail_closed"


def test_project_stop_loss_blocks_when_auto_kill_disabled():
    report = verify_project_stop_loss(
        env={
            "ENTRY_MODE_QUALITY_AUTO_KILL_SWITCH_ENABLED": "false",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "project_stop_loss_unverified_or_disabled"
    assert report["evidence"]["auto_kill_switch_enabled"] is False
    assert report["evidence"]["action"]["stop_automatic_entry"] is True


def test_project_stop_loss_passes_default_thresholds():
    report = verify_project_stop_loss(env={})

    assert report["status"] == "pass"
    assert report["evidence"]["scope"] == "entry_mode"
    assert report["evidence"]["stop_criteria"]["negative_ev_min_samples"] == 20
    assert report["evidence"]["action"]["action"] == "downgrade_to_watch_only"


def test_governance_readiness_contracts_pass_seed_artifact():
    assert verify_evidence_eligibility_matrix()["status"] == "pass"
    assert verify_top_fix_queue()["status"] == "pass"
    assert verify_safety_case()["status"] == "pass"
    assert verify_waiver_policy()["status"] == "pass"


def write_access_policy(path, source_file, overrides=None):
    path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.access_control_policy.v1",
                "source_file": str(source_file),
                "public_endpoints": ["/", "/health", "/ping", "/dashboard"],
                "protected_defaults": {
                    "required_role": "dashboard_reader",
                    "token_scope": "dashboard:read",
                    "audit_log_required": False,
                    "danger_level": "read",
                },
                "danger_levels_requiring_post": ["operator_mutation", "admin_mutation", "critical"],
                "danger_levels_requiring_audit": ["operator_mutation", "admin_mutation", "critical"],
                "endpoint_overrides": overrides or [],
                "dynamic_protected_routes": [],
            }
        ),
        encoding="utf-8",
    )


def write_write_registry(path, endpoints=None):
    path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "write_paths": [
                    {
                        "write_path_id": f"unit.write.{index}",
                        "entry_point": f"POST {endpoint}",
                    }
                    for index, endpoint in enumerate(endpoints or [], start=1)
                ],
            }
        ),
        encoding="utf-8",
    )


def test_access_control_policy_covers_dashboard_routes_and_mutations():
    report = verify_access_control_policy()

    assert report["status"] == "pass"
    assert report["evidence"]["protected_route_count"] == 58
    assert report["evidence"]["mutation_policy_count"] == 12
    assert report["evidence"]["write_path_endpoint_count"] == 6
    assert report["evidence"]["unauthenticated_routes"] == []
    assert report["evidence"]["mutation_without_post_guard"] == []
    assert report["evidence"]["write_path_policy_gaps"] == []


def test_audit_log_integrity_covers_required_mutation_routes():
    report = verify_audit_log_integrity()

    assert report["status"] == "pass"
    assert report["evidence"]["audit_required_endpoint_count"] == 12
    assert report["evidence"]["missing_audit_hooks"] == []
    assert report["evidence"]["helper_required_fragments"]["sha256_hashing"] is True
    assert report["evidence"]["chain_field_presence"]["audit_chain_hash"] is True


def test_access_control_policy_blocks_unprotected_dashboard_route(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
if (url.pathname === '/api/open') {
  res.end('ok');
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    registry_path = tmp_path / "write-registry.json"
    write_access_policy(policy_path, source_path)
    write_write_registry(registry_path)

    report = verify_access_control_policy(policy_path, registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "access_control_policy_missing_malformed_or_incomplete"
    assert report["evidence"]["unauthenticated_routes"] == [{"endpoint": "/api/open", "line": 9}]


def test_access_control_policy_blocks_mutation_without_post_guard(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
if (url.pathname === '/api/mutate') {
  if (!checkAuth(req, url, res)) return;
  db.prepare('UPDATE demo SET value = 1').run();
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    registry_path = tmp_path / "write-registry.json"
    write_access_policy(
        policy_path,
        source_path,
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "critical",
                "allowed_methods": ["POST"],
                "method_guard_required": True,
            }
        ],
    )
    write_write_registry(registry_path, ["/api/mutate"])

    report = verify_access_control_policy(policy_path, registry_path)

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["mutation_without_post_guard"] == [
        {"endpoint": "/api/mutate", "line": 9, "danger_level": "critical"}
    ]
    assert report["evidence"]["write_path_policy_gaps"][0]["endpoint"] == "/api/mutate"


def test_audit_log_integrity_blocks_missing_audit_hook(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
import { createHash } from 'crypto';
const DASHBOARD_AUDIT_SCHEMA_VERSION = 'v2.7.0.audit_log_integrity.v1';
function verifyDashboardAuditChain(events) { return events; }
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
function writeAudit(event) {
  createHash('sha256').update(event.audit_event_id + event.prev_audit_hash + event.audit_payload_hash + event.audit_chain_hash + event.created_at);
  fs.appendFileSync(auditLogPath, JSON.stringify(event));
}
function requireDashboardAuditEvent(req, res, url, input) {
  res.end('Audit log unavailable');
}
if (url.pathname === '/api/mutate') {
  if (req.method !== 'POST') return;
  if (!checkAuth(req, url, res)) return;
  db.prepare('UPDATE demo SET value = 1').run();
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    write_access_policy(
        policy_path,
        source_path,
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "critical",
            }
        ],
    )

    report = verify_audit_log_integrity(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["missing_audit_hooks"] == [
        {"endpoint": "/api/mutate", "registered_route": True, "has_audit_event": False}
    ]


def test_write_path_registry_covers_dashboard_write_paths():
    report = verify_write_path_registry()

    assert report["status"] == "pass"
    assert report["evidence"]["scan_target_count"] == 1
    assert report["evidence"]["scanned_mutation_count"] == 9
    assert report["evidence"]["registered_write_path_count"] == 9
    assert report["evidence"]["unregistered_mutation_count"] == 0
    assert "sqlite:paper_trades" in report["evidence"]["registered_targets"]


def test_direct_database_mutation_ban_covers_break_glass_paths():
    report = verify_direct_database_mutation_ban()

    assert report["status"] == "pass"
    assert report["evidence"]["direct_db_write_path_count"] == 7
    assert report["evidence"]["approved_mutation_path_count"] == 7
    assert report["evidence"]["unapproved_direct_db_mutations"] == []
    assert report["evidence"]["registry_gate_violations"] == []
    assert report["evidence"]["access_control_violations"] == []
    assert "sqlite:live_positions" in report["evidence"]["direct_db_targets"]


def test_direct_database_mutation_ban_blocks_unapproved_sqlite_mutation(tmp_path):
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "write_paths": [
                    {
                        "write_path_id": "unit.direct_sqlite_update",
                        "module": "unit",
                        "entry_point": "POST /api/mutate",
                        "target_store": "sqlite:demo",
                        "write_target": "demo",
                        "mutation_type": "update",
                        "requires_outbox": False,
                        "outbox_reason": "unit_test_break_glass_only",
                        "owner": "test",
                        "mode_gate": "admin_break_glass",
                        "source_file": "server.js",
                        "source_anchor": "UPDATE demo",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    policy_path = tmp_path / "direct-db-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.direct_database_mutation_ban.v1",
                "rules": {
                    "default_action": "ban_direct_database_mutation",
                    "required_registry_mode_gate": "admin_break_glass",
                    "require_access_control_policy": True,
                    "require_audit_log": True,
                    "require_post": True,
                    "require_outbox_rationale": True,
                },
                "approved_mutation_paths": [],
            }
        ),
        encoding="utf-8",
    )
    access_policy_path = tmp_path / "access-policy.json"
    write_access_policy(
        access_policy_path,
        "server.js",
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "admin_mutation",
                "allowed_methods": ["POST"],
                "method_guard_required": True,
            }
        ],
    )

    report = verify_direct_database_mutation_ban(policy_path, registry_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "direct_db_mutation_ban_missing_malformed_or_bypassed"
    assert report["evidence"]["unapproved_direct_db_mutations"] == [
        {
            "write_path_id": "unit.direct_sqlite_update",
            "target_store": "sqlite:demo",
            "entry_point": "POST /api/mutate",
        }
    ]


def test_background_job_registry_covers_supervised_runtime_jobs():
    report = verify_background_job_registry()

    assert report["status"] == "pass"
    assert report["evidence"]["job_count"] == 9
    assert report["evidence"]["restart_loop_job_count"] == 6
    assert report["evidence"]["missing_entry_point_files"] == []
    assert report["evidence"]["missing_source_anchors"] == []
    assert "paper_trade_monitor" in report["evidence"]["job_names"]
    assert "source_resonance_shadow" in report["evidence"]["job_names"]


def test_background_job_registry_blocks_missing_entry_point(tmp_path):
    registry_path = tmp_path / "background-jobs.json"
    source_path = tmp_path / "runner.sh"
    source_path.write_text("python3 scripts/missing_worker.py\n", encoding="utf-8")
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.background_job_registry.v1",
                "jobs": [
                    {
                        "job_name": "missing_worker",
                        "entry_point": "python3 scripts/missing_worker.py",
                        "entry_point_file": str(tmp_path / "missing_worker.py"),
                        "source_file": str(source_path),
                        "source_anchor": "python3 scripts/missing_worker.py",
                        "allowed_modes": ["observe_only"],
                        "lease_policy": {
                            "kind": "supervised_restart_loop",
                            "pid_env": "MISSING_PID",
                            "restart_delay_sec": 15,
                        },
                        "owner": "test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_background_job_registry(registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "background_job_registry_missing_malformed_or_incomplete"
    assert report["evidence"]["missing_entry_point_files"] == [
        {"job_name": "missing_worker", "entry_point_file": str(tmp_path / "missing_worker.py")}
    ]


def test_entry_point_inventory_covers_runtime_routes_scripts_and_deploy():
    report = verify_entry_point_inventory()

    assert report["status"] == "pass"
    assert report["evidence"]["entry_point_count"] == 32
    assert report["evidence"]["entry_type_counts"]["route_group"] == 5
    assert report["evidence"]["entry_type_counts"]["script"] == 18
    assert report["evidence"]["dashboard_literal_route_count"] == 64
    assert report["evidence"]["dashboard_protected_route_count"] == 58
    assert report["evidence"]["route_registry_required_count"] == 2
    assert report["evidence"]["arbiter_required_count"] == 29
    assert report["evidence"]["uncovered_audit_required_routes"] == []
    assert report["evidence"]["location_violations"] == []


def test_entry_point_inventory_blocks_missing_anchor(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text("if (url.pathname === '/health') { res.end('ok'); }\n", encoding="utf-8")
    access_policy_path = tmp_path / "access-policy.json"
    write_access_policy(access_policy_path, source_path)
    inventory_path = tmp_path / "entry-points.json"
    inventory_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.entry_point_inventory.v1",
                "entry_points": [
                    {
                        "entry_point_id": "unit_missing_anchor",
                        "entry_type": "server",
                        "code_location": {
                            "file": str(source_path),
                            "anchor": "definitely_missing_anchor",
                        },
                        "route_registry_required": False,
                        "route_registry_reason": "unit test",
                        "arbiter_required": False,
                        "arbiter_reason": "unit test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_entry_point_inventory(inventory_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "entry_point_inventory_missing_malformed_or_incomplete"
    assert report["evidence"]["location_violations"] == [
        {
            "entry_point_id": "unit_missing_anchor",
            "location": "code_location",
            "file": str(source_path),
            "anchor": "definitely_missing_anchor",
            "reason": "anchor_not_found",
        }
    ]


def test_static_policy_enforcement_passes_critical_static_scans():
    report = verify_static_policy_enforcement()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.static_policy_enforcement.v1"
    assert report["evidence"]["static_check_count"] == 8
    assert report["evidence"]["forbidden_match_count"] == 0
    assert report["evidence"]["malformed_checks"] == []
    assert report["evidence"]["scan_errors"] == []


def test_static_policy_enforcement_blocks_forbidden_pattern(tmp_path):
    source_path = tmp_path / "unsafe.js"
    source_path.write_text("export const value = eval('1 + 1');\n", encoding="utf-8")
    policy_path = tmp_path / "static-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.static_policy_enforcement.v1",
                "checks": [
                    {
                        "static_check_id": "unit_eval_forbidden",
                        "forbidden_pattern": "\\beval\\s*\\(",
                        "scan_target": str(source_path),
                        "result": "pass",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_static_policy_enforcement(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "static_policy_missing_malformed_or_violated"
    assert report["evidence"]["forbidden_match_count"] == 1
    assert report["evidence"]["forbidden_matches"] == [
        {
            "static_check_id": "unit_eval_forbidden",
            "file": str(source_path),
            "line": 1,
            "match": "export const value = eval('1 + 1');",
        }
    ]


def test_api_response_contract_covers_v27_manual_evidence_post_routes():
    report = verify_api_response_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.api_response_policy.v1"
    assert report["evidence"]["endpoint_count"] == 6
    assert report["evidence"]["v27_evidence_endpoint_count"] == 6
    assert report["evidence"]["uncovered_v27_evidence_endpoints"] == []
    assert report["evidence"]["unknown_policy_endpoints"] == []
    assert report["evidence"]["malformed_policies"] == []
    assert report["evidence"]["route_violations"] == []
    assert report["evidence"]["source_violations"] == []
    assert report["evidence"]["missing_guard_helper_fragments"] == []


def test_api_response_contract_blocks_missing_response_builder_anchor(tmp_path):
    source_path = tmp_path / "dashboard.js"
    endpoint = "/api/paper/v27-read-model-refresh"
    source_path.write_text(
        "\n".join(
            [
                "export function apiJsonHeaders(cacheControl = 'no-store') { return {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl}; }",
                "function buildV27ManualEvidenceApiResponse(responseSchemaVersion, result = {}, options = {}) {",
                "  const generatedAt = options.generatedAt || new Date().toISOString();",
                "  const payload = {generated_at: generatedAt, materialized: false, response_schema_version: responseSchemaVersion, refresh_schema_version: responseSchemaVersion, ...result};",
                "  if (payload.accepted === false && !payload.error) payload.error = payload.status || 'manual_evidence_request_rejected';",
                "  return payload;",
                "}",
                "function checkAuth(req, url, res) { res.writeHead(403, apiJsonHeaders()); res.writeHead(401, apiJsonHeaders()); return true; }",
                "function requirePost(req, res) { res.writeHead(405, apiJsonHeaders()); return true; }",
                "function requireDashboardAuditEvent(req, res, url) { res.writeHead(500, apiJsonHeaders()); return true; /* Audit log unavailable */ }",
                f"if (url.pathname === '{endpoint}') {{",
                "  if (!requirePost(req, res)) return;",
                "  if (!checkAuth(req, url, res)) return;",
                "  if (!requireDashboardAuditEvent(req, res, url)) return;",
                "  res.writeHead(refresh.accepted ? 202 : 409, apiJsonHeaders());",
                "  res.end(JSON.stringify({ response_schema_version: 'v2.7.0.manual_read_model_refresh.v1', refresh_schema_version: 'v2.7.0.manual_read_model_refresh.v1', ...refresh }));",
                "}",
            ]
        ),
        encoding="utf-8",
    )
    access_policy_path = tmp_path / "access-policy.json"
    access_policy_path.write_text(
        json.dumps(
            {
                "source_file": str(source_path),
                "endpoint_overrides": [
                    {
                        "endpoint": endpoint,
                        "token_scope": "v27:evidence_mutation",
                        "audit_log_required": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    policy_path = tmp_path / "api-response-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_policy.v1",
                "source_file": str(source_path),
                "response_policies": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "status_code_policy": {
                            "accepted": 202,
                            "rejected": 409,
                            "method_not_allowed": 405,
                            "auth_failed": [401, 403],
                            "audit_unavailable": 500,
                        },
                        "error_envelope": {
                            "required": True,
                            "error_field": "error",
                            "guard_errors": True,
                            "rejected_response_error_required": True,
                        },
                        "cache_control": "no-store",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_api_response_contract(policy_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "api_response_policy_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"] == [
        {"endpoint": endpoint, "reason": "response_builder_missing"}
    ]


def test_api_response_envelope_contract_covers_v27_manual_evidence_routes():
    report = verify_api_response_envelope_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.api_response_envelope_policy.v1"
    assert report["evidence"]["failure_action"] == "api_envelope_invalid"
    assert report["evidence"]["envelope_version"] == "v2.7.0.api_response_envelope.v1"
    assert report["evidence"]["hash_algorithm"] == "sha256_canonical_json_without_payload_hash"
    assert report["evidence"]["required_fields"] == ["endpoint", "envelope_version", "payload_hash", "error_shape", "generated_at"]
    assert report["evidence"]["endpoint_count"] == 6
    assert report["evidence"]["base_response_endpoint_count"] == 6
    assert report["evidence"]["sample_case_count"] == 2
    assert report["evidence"]["schema_violations"] == []
    assert report["evidence"]["error_shape_violations"] == []
    assert report["evidence"]["malformed_envelopes"] == []
    assert report["evidence"]["uncovered_base_response_endpoints"] == []
    assert report["evidence"]["source_violations"] == []
    assert report["evidence"]["malformed_samples"] == []
    assert report["evidence"]["missing_helper_fragments"] == []
    for sample in report["evidence"]["sample_evidence"]:
        assert sample["payload_hash"]
        assert sample["endpoint"] == "/api/paper/v27-read-model-refresh"
        assert sample["generated_at"]


def test_api_response_envelope_contract_blocks_missing_hash_generation(tmp_path):
    source_path = tmp_path / "dashboard.js"
    endpoint = "/api/paper/v27-read-model-refresh"
    source_path.write_text(
        "\n".join(
            [
                "export const V27_API_RESPONSE_ENVELOPE_VERSION = 'v2.7.0.api_response_envelope.v1';",
                "function buildApiResponseErrorShape(payload = {}) { return {}; }",
                "function apiEnvelopePayloadForHash(payload = {}) { return payload; }",
                f"if (url.pathname === '{endpoint}') {{",
                "  res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh)));",
                "}",
            ]
        ),
        encoding="utf-8",
    )
    base_policy_path = tmp_path / "api-response-policy.json"
    base_policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_policy.v1",
                "source_file": str(source_path),
                "response_policies": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    envelope_policy_path = tmp_path / "api-response-envelope-policy.json"
    envelope_policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_envelope_policy.v1",
                "source_file": str(source_path),
                "base_response_policy_path": str(base_policy_path),
                "failure_action": "api_envelope_invalid",
                "envelope_version": "v2.7.0.api_response_envelope.v1",
                "hash_algorithm": "sha256_canonical_json_without_payload_hash",
                "required_fields": ["endpoint", "envelope_version", "payload_hash", "error_shape", "generated_at"],
                "error_shape": {
                    "required_fields": ["has_error", "accepted", "error_field", "error_code", "status"],
                    "accepted_false_requires_error": True,
                    "accepted_false_requires_error_code": True,
                },
                "sample_cases": [
                    {
                        "sample_id": "accepted",
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "generated_at": "2026-05-25T00:00:00.000Z",
                        "result": {"accepted": True, "status": "started"},
                        "expected_error_shape": {
                            "has_error": False,
                            "accepted": True,
                            "error_field": None,
                            "error_code": None,
                            "status": "started",
                        },
                    }
                ],
                "response_envelopes": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "source_anchor": "buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh, { endpoint: url.pathname })",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_api_response_envelope_contract(envelope_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "api_response_envelope_policy_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"] == [
        {
            "endpoint": endpoint,
            "reason": "source_anchor_missing",
            "source_anchor": "buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh, { endpoint: url.pathname })",
        },
        {"endpoint": endpoint, "reason": "endpoint_binding_missing"},
    ]
    assert "payload_hash_assignment" in report["evidence"]["missing_helper_fragments"]


def test_error_taxonomy_covers_dashboard_and_readiness_error_codes():
    report = verify_error_taxonomy()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.error_taxonomy.v1"
    assert report["evidence"]["unclassified_error_codes"] == []
    assert report["evidence"]["unused_taxonomy_codes"] == []
    assert report["evidence"]["malformed_entries"] == []
    assert "already_running" in report["evidence"]["observed_by_source"]["dashboard_api_error_codes"]
    assert "error_taxonomy_missing_malformed_or_incomplete" in report["evidence"]["observed_by_source"]["basic_readiness_blocking_reasons"]
    assert "paper_live_capability_detected" in report["evidence"]["observed_by_source"]["paper_mode_safety_reasons"]


def test_error_taxonomy_blocks_unclassified_dashboard_error(tmp_path):
    dashboard_source = tmp_path / "dashboard.js"
    dashboard_source.write_text("res.end(JSON.stringify({ error: 'Nope', error_code: 'unclassified_unit_error' }));\n", encoding="utf-8")
    basic_source = tmp_path / "basic.py"
    basic_source.write_text("", encoding="utf-8")
    paper_source = tmp_path / "paper.py"
    paper_source.write_text("", encoding="utf-8")
    taxonomy_path = tmp_path / "taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.error_taxonomy.v1",
                "coverage": {},
                "allowed_categories": ["dashboard_request"],
                "allowed_severities": ["error"],
                "taxonomy": [
                    {
                        "error_code": "classified_unit_error",
                        "category": "dashboard_request",
                        "severity": "error",
                        "operator_action": "unit test",
                        "introduced_at": "2026-05-25T00:00:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_error_taxonomy(
        taxonomy_path=taxonomy_path,
        dashboard_source_path=dashboard_source,
        basic_readiness_source_path=basic_source,
        paper_mode_safety_source_path=paper_source,
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "error_taxonomy_missing_malformed_or_incomplete"
    assert report["evidence"]["unclassified_error_codes"] == ["unclassified_unit_error"]
    assert report["evidence"]["unused_taxonomy_codes"] == ["classified_unit_error"]


def test_error_taxonomy_blocks_malformed_entry(tmp_path):
    dashboard_source = tmp_path / "dashboard.js"
    dashboard_source.write_text("res.end(JSON.stringify({ error: 'Nope', error_code: 'classified_unit_error' }));\n", encoding="utf-8")
    basic_source = tmp_path / "basic.py"
    basic_source.write_text("", encoding="utf-8")
    paper_source = tmp_path / "paper.py"
    paper_source.write_text("", encoding="utf-8")
    taxonomy_path = tmp_path / "taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.error_taxonomy.v1",
                "coverage": {},
                "allowed_categories": ["dashboard_request"],
                "allowed_severities": ["error"],
                "taxonomy": [
                    {
                        "error_code": "classified_unit_error",
                        "category": "dashboard_request",
                        "severity": "error",
                        "introduced_at": "not-a-date",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_error_taxonomy(
        taxonomy_path=taxonomy_path,
        dashboard_source_path=dashboard_source,
        basic_readiness_source_path=basic_source,
        paper_mode_safety_source_path=paper_source,
    )

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["malformed_entries"] == [
        {
            "index": 0,
            "error_code": "classified_unit_error",
            "missing_fields": ["operator_action"],
            "violations": ["introduced_at_invalid_iso_timestamp"],
        }
    ]


def test_log_redaction_verification_covers_runtime_and_manual_evidence_logs():
    report = verify_log_redaction_verification()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.log_redaction_policy.v1"
    assert report["evidence"]["secret_pattern_set"] == "v2.7.0.secret_pattern_set.dashboard_runtime.v1"
    assert report["evidence"]["pattern_count"] == 4
    assert report["evidence"]["sample_case_count"] == 4
    assert report["evidence"]["stream_count"] == 3
    assert report["evidence"]["malformed_samples"] == []
    assert report["evidence"]["malformed_streams"] == []
    assert report["evidence"]["source_violations"] == []
    for stream in report["evidence"]["streams"]:
        assert stream["redaction_passed"] is True
        assert stream["sample_hash"]
        assert stream["checked_at"]


def test_log_redaction_verification_blocks_unredacted_sample(tmp_path):
    source_path = tmp_path / "dashboard.js"
    source_path.write_text(
        "const message = args.map(formatLogArg).join(' ');\n"
        "logBuffer.push(logLine);\n"
        "fs.appendFileSync(runtimeLogPath, message);\n",
        encoding="utf-8",
    )
    policy_path = tmp_path / "log-redaction.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.log_redaction_policy.v1",
                "secret_pattern_set": {
                    "secret_pattern_set": "unit.secret_patterns.v1",
                    "patterns": [
                        {
                            "pattern_id": "wrong_pattern",
                            "regex": "(api_key=)(safe-value-only)",
                            "replacement": "\\1[REDACTED]",
                        }
                    ],
                },
                "sample_cases": [
                    {
                        "sample_id": "leaky",
                        "raw": "api_key=unit-secret-value",
                        "expected_fragments_absent": ["unit-secret-value"],
                        "expected_fragments_present": ["api_key=[REDACTED]"],
                    }
                ],
                "streams": [
                    {
                        "log_stream": "dashboard_runtime_log",
                        "secret_pattern_set": "unit.secret_patterns.v1",
                        "source_file": str(source_path),
                        "redaction_anchor": "redactLogMessage(args.map(formatLogArg).join(' '))",
                        "write_anchor": "fs.appendFileSync(runtimeLogPath",
                        "sample_case_ids": ["leaky"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_log_redaction_verification(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "log_redaction_verification_missing_malformed_or_failed"
    assert report["evidence"]["malformed_samples"][0]["sample_id"] == "leaky"
    assert report["evidence"]["source_violations"] == [
        {
            "log_stream": "dashboard_runtime_log",
            "reason": "redaction_anchor_missing",
            "redaction_anchor": "redactLogMessage(args.map(formatLogArg).join(' '))",
        }
    ]


def test_security_session_policy_covers_admin_secret_and_telegram_sessions():
    admin = verify_admin_session_security_contract()
    secret = verify_secret_access_audit_contract()
    telegram = verify_telegram_session_security_contract()

    assert admin["status"] == "pass"
    assert admin["evidence"]["session_count"] == 1
    assert admin["evidence"]["malformed_sessions"] == []
    assert admin["evidence"]["source_violations"] == []
    assert admin["evidence"]["sessions"][0]["mfa_required"] is True
    assert admin["evidence"]["sessions"][0]["csrf_protection"] == "post_only_mutation_and_non_cookie_token"

    assert secret["status"] == "pass"
    assert secret["evidence"]["record_count"] == 3
    assert secret["evidence"]["malformed_records"] == []
    assert secret["evidence"]["redaction_violations"] == []
    assert {item["secret_id"] for item in secret["evidence"]["records"]} == {
        "env:DASHBOARD_TOKEN",
        "env:TELEGRAM_API_HASH",
        "env:TELEGRAM_SESSION",
    }

    assert telegram["status"] == "pass"
    assert telegram["evidence"]["session_count"] == 1
    assert telegram["evidence"]["malformed_sessions"] == []
    assert telegram["evidence"]["source_violations"] == []
    assert telegram["evidence"]["sessions"][0]["auth_state"] == "required_before_ingestion"


def test_security_session_policy_blocks_missing_admin_source_anchor(tmp_path):
    policy_path = tmp_path / "security-session.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.security_session_policy.v1",
                "admin_sessions": [
                    {
                        "session_id": "broken-admin",
                        "operator_id": "dashboard_token_operator",
                        "mfa_required": True,
                        "expires_at": "2026-06-25T00:00:00Z",
                        "csrf_protection": "post_only_mutation_and_non_cookie_token",
                        "source_file": "src/web/dashboard-server.js",
                        "source_anchors": ["definitely_missing_admin_session_anchor"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_admin_session_security_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "admin_session_security_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"][0]["missing_anchors"] == ["definitely_missing_admin_session_anchor"]


def test_runtime_pipeline_policy_covers_queue_progress_and_thread_pools():
    queue = verify_queue_ack_nack_contract()
    progress = verify_pipeline_progress_invariant()
    pools = verify_thread_pool_isolation_contract()

    assert queue["status"] == "pass"
    assert queue["evidence"]["record_count"] == 2
    assert queue["evidence"]["ack_states"] == ["acked", "nacked"]
    assert queue["evidence"]["malformed_records"] == []
    assert queue["evidence"]["source_violations"] == []

    assert progress["status"] == "pass"
    assert progress["evidence"]["record_count"] == 2
    assert progress["evidence"]["malformed_records"] == []
    assert progress["evidence"]["source_violations"] == []

    assert pools["status"] == "pass"
    assert pools["evidence"]["pool_count"] == 3
    assert pools["evidence"]["malformed_pools"] == []
    assert pools["evidence"]["source_violations"] == []
    assert set(pools["evidence"]["pool_names"]) == {
        "paper_fast_lane_pool",
        "smart_entry_pool",
        "timing_executor",
    }


def test_runtime_pipeline_policy_blocks_nacked_record_without_reason(tmp_path):
    source_path = tmp_path / "queue.py"
    source_path.write_text("ack_state = 'nacked'\n", encoding="utf-8")
    policy_path = tmp_path / "runtime-pipeline.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.runtime_pipeline_policy.v1",
                "queue_ack_nack": [
                    {
                        "queue_id": "unit_queue",
                        "task_id": "unit-task",
                        "ack_state": "nacked",
                        "nack_reason": "none",
                        "recorded_at": "2026-05-26T00:00:00Z",
                        "source_file": str(source_path),
                        "source_anchor": "ack_state = 'nacked'",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_queue_ack_nack_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "queue_ack_nack_missing_malformed_or_unenforced"
    assert report["evidence"]["malformed_records"][0]["violations"] == ["nack_reason_required_for_nacked"]


def test_ci_spec_generated_policy_covers_ci_client_and_impact():
    ci_gate = verify_cicd_merge_gate_contract()
    client = verify_generated_client_contract()
    impact = verify_spec_change_impact_analysis_contract()

    assert ci_gate["status"] == "pass"
    assert ci_gate["evidence"]["gate_count"] == 1
    assert ci_gate["evidence"]["malformed_gates"] == []
    assert ci_gate["evidence"]["workflow_evidence"][0]["required_check_count"] >= 10
    assert ci_gate["evidence"]["workflow_evidence"][0]["missing_checks"] == []

    assert client["status"] == "pass"
    assert client["evidence"]["client_count"] == 1
    catalog = json.loads(Path("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json").read_text(encoding="utf-8"))
    assert client["evidence"]["catalog_contract_count"] == len(catalog["contracts"])
    assert {"MetricDefinitionRegistry", "ThresholdCatalogContract"} <= set(catalog["contracts"])
    assert client["evidence"]["malformed_clients"] == []

    assert impact["status"] == "pass"
    assert impact["evidence"]["impact_count"] == 1
    assert impact["evidence"]["malformed_impacts"] == []


def test_generated_client_contract_blocks_stale_artifact_hash(tmp_path):
    policy = json.loads(Path("config/v27-ci-spec-generated-policy.json").read_text(encoding="utf-8"))
    policy["generated_clients"][0]["generated_artifact_hash"] = "0" * 64
    policy_path = tmp_path / "ci-spec-generated-policy.json"
    policy_path.write_text(json.dumps(policy), encoding="utf-8")

    report = verify_generated_client_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "generated_client_missing_malformed_or_stale"
    assert "generated_artifact_hash_mismatch" in report["evidence"]["malformed_clients"][0]["violations"]


def test_service_readiness_probe_contract_covers_health_surfaces():
    report = verify_service_readiness_probe_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.service_readiness_probes.v1"
    assert report["evidence"]["failure_action"] == "service_not_ready"
    assert report["evidence"]["required_fields"] == ["service_name", "probe_id", "health_status", "dependency_status", "checked_at"]
    assert report["evidence"]["probe_count"] == 6
    assert report["evidence"]["schema_violations"] == []
    assert report["evidence"]["malformed_probes"] == []
    assert report["evidence"]["source_violations"] == []
    assert report["evidence"]["missing_required_probe_ids"] == []
    probe_ids = {probe["probe_id"] for probe in report["evidence"]["probes"]}
    assert {
        "public_health",
        "dashboard_status_snapshot",
        "module_health_snapshot",
        "v27_read_model_health",
        "v27_mode_readiness",
        "zeabur_supervisor_boot",
    } <= probe_ids
    for probe in report["evidence"]["probes"]:
        assert probe["health_status"] == "ready"
        assert probe["dependency_status"]
        assert probe["checked_at"]


def test_service_readiness_probe_contract_blocks_missing_dependency_anchor(tmp_path):
    source_path = tmp_path / "dashboard.js"
    source_path.write_text(
        "if (url.pathname === '/health') { res.end(JSON.stringify({status: 'ok'})); }\n",
        encoding="utf-8",
    )
    policy_path = tmp_path / "service-readiness.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.service_readiness_probes.v1",
                "failure_action": "service_not_ready",
                "required_fields": ["service_name", "probe_id", "health_status", "dependency_status", "checked_at"],
                "required_probe_ids": ["public_health"],
                "probes": [
                    {
                        "service_name": "dashboard_http_server",
                        "probe_id": "public_health",
                        "health_status": "ready",
                        "endpoint": "/health",
                        "source_file": str(source_path),
                        "source_anchor": "url.pathname === '/health'",
                        "dependency_status": {"commit_fingerprint": "required"},
                        "dependency_anchors": ["commit: runtimeCommitFingerprint()"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_service_readiness_probe_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "service_readiness_probe_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"] == [
        {
            "probe_id": "public_health",
            "reason": "dependency_anchor_missing",
            "missing_dependency_anchors": ["commit: runtimeCommitFingerprint()"],
        },
    ]


def test_dashboard_action_separation_contract_covers_admin_mutations():
    report = verify_dashboard_action_separation_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.dashboard_action_separation.v1"
    assert report["evidence"]["failure_action"] == "dashboard_mutation_blocked"
    assert report["evidence"]["required_fields"] == ["action_id", "view_route", "mutation_route", "separation_enforced", "audit_required"]
    assert report["evidence"]["action_count"] == 6
    assert report["evidence"]["schema_violations"] == []
    assert report["evidence"]["malformed_actions"] == []
    assert report["evidence"]["route_violations"] == []
    assert report["evidence"]["missing_required_action_ids"] == []
    action_ids = {action["action_id"] for action in report["evidence"]["actions"]}
    assert {
        "close_position_route_split",
        "pause_trading_route_split",
        "resume_trading_route_split",
        "reset_daily_loss_route_split",
        "reset_live_data_route_split",
        "paper_cleanup_route_split",
    } <= action_ids
    for action in report["evidence"]["actions"]:
        assert action["view_route"] != action["mutation_route"]
        assert action["separation_enforced"] is True
        assert action["audit_required"] is True


def test_dashboard_action_separation_contract_blocks_disabled_separation(tmp_path):
    policy_path = tmp_path / "dashboard-action-separation.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.dashboard_action_separation.v1",
                "failure_action": "dashboard_mutation_blocked",
                "source_file": "src/web/dashboard-server.js",
                "required_fields": ["action_id", "view_route", "mutation_route", "separation_enforced", "audit_required"],
                "required_action_ids": ["pause_trading_route_split"],
                "actions": [
                    {
                        "action_id": "pause_trading_route_split",
                        "view_route": "/api/trading-status",
                        "mutation_route": "/api/pause-trading",
                        "separation_enforced": False,
                        "audit_required": True,
                        "view_anchor": "const status = rm.getStatus();",
                        "mutation_anchor": "action: 'pause_trading'",
                        "mutation_write_path_ids": ["dashboard.system_state.pause_trading"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_dashboard_action_separation_contract(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "dashboard_action_separation_missing_malformed_or_unenforced"
    assert report["evidence"]["malformed_actions"] == [
        {
            "index": 0,
            "action_id": "pause_trading_route_split",
            "missing_fields": [],
            "violations": ["separation_enforced_true_required"],
        }
    ]


def test_write_path_registry_blocks_unregistered_static_write(tmp_path):
    source_path = tmp_path / "writer.js"
    source_path.write_text("db.prepare(`UPDATE demo SET value = 1`).run();\n", encoding="utf-8")
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "static_scan": {
                    "targets": [
                        {
                            "source_file": str(source_path),
                            "include_patterns": ["UPDATE "],
                            "exclude_patterns": [],
                        }
                    ]
                },
                "write_paths": [],
            }
        ),
        encoding="utf-8",
    )

    report = verify_write_path_registry(registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "write_path_registry_missing_malformed_or_incomplete"
    assert report["evidence"]["scanned_mutation_count"] == 1
    assert report["evidence"]["unregistered_mutation_count"] == 1


def test_write_path_registry_accepts_tmp_registered_write(tmp_path):
    source_path = tmp_path / "writer.js"
    source_path.write_text("db.prepare(`UPDATE demo SET value = 1`).run();\n", encoding="utf-8")
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "static_scan": {
                    "targets": [
                        {
                            "source_file": str(source_path),
                            "include_patterns": ["UPDATE "],
                            "exclude_patterns": [],
                        }
                    ]
                },
                "write_paths": [
                    {
                        "write_path_id": "unit.demo.update",
                        "module": "unit",
                        "entry_point": "unit_test",
                        "target_store": "sqlite:demo",
                        "mutation_type": "update",
                        "requires_outbox": False,
                        "outbox_reason": "unit_test_non_production_write",
                        "owner": "test",
                        "mode_gate": "diagnostics",
                        "source_file": str(source_path),
                        "source_anchor": "UPDATE demo",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_write_path_registry(registry_path)

    assert report["status"] == "pass"
    assert report["evidence"]["registered_write_path_count"] == 1


def test_governance_readiness_blocks_incomplete_artifact(tmp_path):
    governance_path = tmp_path / "governance.json"
    governance_path.write_text(
        json.dumps(
            {
                "schema_version": "unit.bad",
                "evidence_eligibility_matrix": [
                    {
                        "evidence_use": "normal_tiny_promotion",
                        "event_truth": [],
                        "feature_truth": [],
                        "label_truth": [],
                        "replay_truth": [],
                    }
                ],
                "top_fix_queue": [
                    {
                        "fix_id": "fix-only-one",
                        "blocker_code": "RawProviderEvidenceContract",
                    }
                ],
                "safety_cases": [
                    {
                        "safety_case_id": "case-without-links",
                        "scope": "normal_tiny",
                        "core_hazards": ["hazard"],
                        "mitigations": ["mitigation"],
                        "evidence_links": [],
                    }
                ],
                "waiver_policy": [
                    {
                        "waiver_id": "expired-waiver",
                        "contract_id": "RawProviderEvidenceContract",
                        "scope": "normal_tiny",
                        "expires_at": "2020-01-01T00:00:00Z",
                        "non_waivable": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert verify_evidence_eligibility_matrix(governance_path)["status"] == "missing_evidence"
    assert verify_top_fix_queue(governance_path)["status"] == "missing_evidence"
    assert verify_safety_case(governance_path)["status"] == "missing_evidence"
    assert verify_waiver_policy(governance_path)["status"] == "missing_evidence"
