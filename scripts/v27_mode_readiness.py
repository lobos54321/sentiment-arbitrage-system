#!/usr/bin/env python3
"""Build the v2.7 ModeReadinessMatrix from current machine evidence."""

import argparse
import copy
import json
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402
from v27_basic_contract_readiness import build_basic_contract_readiness  # noqa: E402
from v27_mode_gate_scope import build_mode_gate_scope_audit  # noqa: E402
from v27_projection_consumer_evidence import CONSUMER_HEALTH_FILE, read_projection_consumer_health  # noqa: E402
from v27_read_model_freshness import validate_snapshot_file  # noqa: E402
from v27_spec_validate import CATALOG_PATH, ENTRY_MODE_REGISTRY_PATH, MANIFEST_PATH, validate_all  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_READ_MODEL_DIR = PROJECT_ROOT / "data" / "v27_read_models"
DEFAULT_SNAPSHOT_PATH = DEFAULT_READ_MODEL_DIR / "denominator_snapshot.json"
DEFAULT_CONSUMER_HEALTH_PATH = DEFAULT_READ_MODEL_DIR / CONSUMER_HEALTH_FILE
DEFAULT_OUTPUT_PATH = DEFAULT_READ_MODEL_DIR / "mode_readiness.json"

MODE_ORDER = ["observe_only", "shadow", "ultra_tiny", "normal_tiny"]

MODE_REQUIREMENTS = {
    "observe_only": [
        "CanonicalSpecIntegrityContract",
        "CanonicalSerializationContract",
        "HumanReadableReasonContract",
        "MachineReadableReasonContract",
        "NumericPrecisionContract",
        "ModeReadinessMatrix",
        "SpecConsistencyLinterContract",
        "PaperModeSafetyBoundary",
        "SafeDefaultContract",
        "ChainConfigContract",
        "SourceRegistryContract",
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
        "ServiceReadinessProbeContract",
        "DashboardActionSeparationContract",
        "RouteRegistryContract",
        "InputSanitizationContract",
        "EventSemanticsContract",
        "EventSequencerContract",
        "TieBreakOrderingContract",
    ],
    "shadow": [
        "CanonicalSpecIntegrityContract",
        "EventSemanticsContract",
        "EventSequencerContract",
        "TieBreakOrderingContract",
        "TransactionalOutboxContract",
        "ReplaySideEffectIsolationContract",
        "DeadLetterQueueContract",
        "ProjectionOrderingContract",
        "ConsumerCheckpointContract",
        "ProjectionHandlerIdempotencyContract",
        "DecisionReadModelFreshnessContract",
        "CacheInvalidationContract",
        "DenominatorDedupContract",
        "SignalCreditAssignmentContract",
        "SourceDogLabelContract",
        "ReferencePriceContract",
        "MetricsWindowContract",
    ],
    "ultra_tiny": [
        "TradeOutcomeLabelContract",
        "StandardizedStopContract",
        "ExAnteFeasibility",
        "EarliestActionableTime",
        "RealtimeCleanDetector",
        "QuoteIntentBindingContract",
        "IdempotencyContract",
        "IdempotencyKeyNamespaceContract",
        "ExecutionLeaseContract",
        "StateVersionFencing",
        "EntryExecutionStateMachine",
        "PaperPositionLedgerContract",
        "PaperCapitalLedgerContract",
        "DoubleEntryLedgerInvariantContract",
        "CapitalReservationPolicy",
        "NoFillOutcome",
        "CrashRecoveryStateMachine",
        "ResumeDrainPolicy",
    ],
    "normal_tiny": [
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
        "ProjectStopLossContract",
    ],
}


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _status(contract_id, status, reason, evidence=None):
    return {
        "contract_id": contract_id,
        "status": status,
        "blocking_reason": None if status == "pass" else reason,
        "evidence": evidence or {},
    }


def _event_log_report(event_log_dir):
    try:
        verify = V27EventLog(event_log_dir).summary()
        return {
            "ok": True,
            "verify": verify,
            "error": None,
            "has_events": int(verify.get("event_count") or 0) > 0,
        }
    except V27EventLogError as exc:
        return {
            "ok": False,
            "verify": None,
            "error": str(exc),
            "has_events": False,
        }


def _snapshot_payload(snapshot_path):
    snapshot_path = Path(snapshot_path)
    if not snapshot_path.exists():
        return None
    try:
        return _load_json(snapshot_path)
    except Exception:
        return None


def _m0_route_registry_ok(registry_path):
    try:
        registry = _load_json(registry_path)
    except Exception:
        return False, {"registry_present": False}
    modes = registry.get("modes") or {}
    frozen = {}
    for mode in ("hard_gate_pass_tiny_probe", "source_resonance_tiny_probe"):
        entry = modes.get(mode) or {}
        frozen[mode] = {
            "registered": bool(entry),
            "paper_enabled": entry.get("paper_enabled"),
            "tier": entry.get("tier"),
            "frozen": bool(entry and entry.get("paper_enabled") is False and entry.get("tier") != "live"),
        }
    return all(item["frozen"] for item in frozen.values()), {"registry_present": True, "m0_freeze": frozen}


def _consumer_contract_status(projection_consumer_health, contract_id):
    contracts = projection_consumer_health.get("contracts") if isinstance(projection_consumer_health, dict) else {}
    item = contracts.get(contract_id) if isinstance(contracts, dict) else None
    if not item:
        return _status(
            contract_id,
            "missing_evidence",
            f"{contract_id}_projection_consumer_health_missing",
            {
                "projection_consumer_health_path": projection_consumer_health.get("path") if isinstance(projection_consumer_health, dict) else None,
                "projection_consumer_status": projection_consumer_health.get("health", {}).get("status") if isinstance(projection_consumer_health, dict) else None,
            },
        )
    hash_ok = projection_consumer_health.get("consumer_health_hash_ok", True)
    passed = item.get("status") == "pass" and hash_ok
    status = "pass" if passed else item.get("status", "missing_evidence")
    if item.get("status") == "pass" and not hash_ok:
        status = "fail"
    return _status(
        contract_id,
        status,
        item.get("blocking_reason") or "projection_consumer_contract_not_proven",
        {
            **(item.get("evidence") or {}),
            "projection_consumer_health_path": projection_consumer_health.get("path"),
            "consumer_health_hash_ok": hash_ok,
        },
    )


def build_contract_statuses(
    *,
    spec_report,
    spec_error,
    event_log,
    snapshot_report,
    snapshot,
    registry_path,
    basic_readiness,
    projection_consumer_health,
):
    projection = snapshot.get("projection") if isinstance(snapshot, dict) else {}
    if not isinstance(projection, dict):
        projection = {}
    projection_health = projection.get("health") if isinstance(projection, dict) else {}
    if not isinstance(projection_health, dict):
        projection_health = {}
    metrics = projection.get("metrics") if isinstance(projection, dict) else {}
    if not isinstance(metrics, dict):
        metrics = {}
    event_log_ok = bool(event_log.get("ok") and event_log.get("has_events"))
    projection_built = bool(projection_health.get("projection_built"))
    read_model_safe = bool(snapshot_report.get("health", {}).get("dashboard_safe"))
    route_ok, route_evidence = _m0_route_registry_ok(registry_path)

    statuses = {}
    for contract_id, item in (basic_readiness.get("contracts") or {}).items():
        statuses[contract_id] = {
            "contract_id": contract_id,
            "status": item.get("status"),
            "blocking_reason": item.get("blocking_reason"),
            "evidence": item.get("evidence") or {},
        }
    statuses["CanonicalSpecIntegrityContract"] = _status(
        "CanonicalSpecIntegrityContract",
        "pass" if spec_report else "fail",
        "canonical_spec_invalid",
        spec_report or {"error": spec_error},
    )
    statuses["CanonicalSerializationContract"] = _status(
        "CanonicalSerializationContract",
        "pass" if spec_report else "fail",
        "canonical_serialization_unverified",
        {"spec_hash_algorithm": "sha256(canonical_json)", "spec_valid": bool(spec_report)},
    )
    statuses["RouteRegistryContract"] = _status("RouteRegistryContract", "pass" if route_ok else "missing_evidence", "route_registry_m0_freeze_unverified", route_evidence)
    statuses["EventSemanticsContract"] = _status(
        "EventSemanticsContract",
        "pass" if event_log_ok else "missing_evidence",
        "event_log_empty_or_invalid",
        event_log,
    )
    statuses["EventSequencerContract"] = _status(
        "EventSequencerContract",
        "pass" if event_log_ok else "missing_evidence",
        "sequencer_verify_missing",
        event_log.get("verify") or {"error": event_log.get("error")},
    )
    statuses["TieBreakOrderingContract"] = _status(
        "TieBreakOrderingContract",
        "pass" if event_log_ok else "missing_evidence",
        "deterministic_event_order_unverified",
        {"global_seq_verified": event_log_ok, "aggregate_seq_verified": event_log_ok},
    )
    statuses["ProjectionOrderingContract"] = _status(
        "ProjectionOrderingContract",
        "pass" if projection_built and event_log_ok else "missing_evidence",
        "projection_not_built_from_verified_event_log",
        projection_health,
    )
    statuses["DecisionReadModelFreshnessContract"] = _status(
        "DecisionReadModelFreshnessContract",
        "pass" if read_model_safe else "fail",
        "read_model_not_dashboard_safe",
        snapshot_report,
    )
    statuses["DenominatorDedupContract"] = _status(
        "DenominatorDedupContract",
        "pass" if projection_built and projection.get("records_hash") else "missing_evidence",
        "denominator_records_hash_missing",
        {"records_hash": projection.get("records_hash"), "denominator_seed_records": metrics.get("denominator_seed_records")},
    )
    statuses["SourceDogLabelContract"] = _status(
        "SourceDogLabelContract",
        "pass" if int(projection.get("source_dog_label_events") or 0) > 0 else "missing_evidence",
        "source_dog_label_events_missing",
        {"source_dog_label_events": projection.get("source_dog_label_events")},
    )
    contract_evidence = projection.get("contract_evidence") if isinstance(projection.get("contract_evidence"), dict) else {}
    signal_credit_evidence = contract_evidence.get("SignalCreditAssignmentContract") or {}
    statuses["SignalCreditAssignmentContract"] = _status(
        "SignalCreditAssignmentContract",
        "pass" if projection_built and projection_health.get("signal_credit_assignment_ok") else "missing_evidence",
        "signal_credit_assignment_missing_or_dirty",
        signal_credit_evidence,
    )
    reference_price_evidence = contract_evidence.get("ReferencePriceContract") or {}
    statuses["ReferencePriceContract"] = _status(
        "ReferencePriceContract",
        "pass" if projection_built and projection_health.get("reference_price_ok") else "missing_evidence",
        "reference_price_missing_or_conflicted",
        reference_price_evidence,
    )
    metrics_window_evidence = contract_evidence.get("MetricsWindowContract") or {}
    statuses["MetricsWindowContract"] = _status(
        "MetricsWindowContract",
        "pass" if projection_built and projection_health.get("metrics_window_ok") else "missing_evidence",
        "metrics_window_missing_or_invalid",
        metrics_window_evidence,
    )
    trade_outcome_evidence = contract_evidence.get("TradeOutcomeLabelContract") or {}
    statuses["TradeOutcomeLabelContract"] = _status(
        "TradeOutcomeLabelContract",
        "pass" if projection_built and projection_health.get("trade_outcome_label_ok") else "missing_evidence",
        "trade_outcome_label_missing_or_malformed",
        trade_outcome_evidence,
    )
    label_finalization_evidence = contract_evidence.get("LabelFinalizationContract") or {}
    label_finalization_eligible = int(label_finalization_evidence.get("eligible_label_finalization_records") or 0) > 0
    label_finalization_status = (
        "pass"
        if projection_built and projection_health.get("label_finalization_ok")
        else "fail"
        if projection_built and label_finalization_eligible
        else "missing_evidence"
    )
    statuses["LabelFinalizationContract"] = _status(
        "LabelFinalizationContract",
        label_finalization_status,
        "label_finalization_malformed" if label_finalization_status == "fail" else "label_finalization_missing_or_malformed",
        label_finalization_evidence,
    )
    outcome_window_close_evidence = contract_evidence.get("OutcomeWindowCloseContract") or {}
    outcome_window_close_eligible = int(outcome_window_close_evidence.get("eligible_outcome_window_close_records") or 0) > 0
    outcome_window_close_status = (
        "pass"
        if projection_built and projection_health.get("outcome_window_close_ok")
        else "fail"
        if projection_built and outcome_window_close_eligible
        else "missing_evidence"
    )
    statuses["OutcomeWindowCloseContract"] = _status(
        "OutcomeWindowCloseContract",
        outcome_window_close_status,
        "outcome_window_close_malformed_or_order_violation"
        if outcome_window_close_status == "fail"
        else "outcome_window_close_missing_or_malformed",
        outcome_window_close_evidence,
    )
    standardized_stop_evidence = contract_evidence.get("StandardizedStopContract") or {}
    statuses["StandardizedStopContract"] = _status(
        "StandardizedStopContract",
        "pass" if projection_built and projection_health.get("standardized_stop_ok") else "missing_evidence",
        "standardized_stop_missing_or_malformed",
        standardized_stop_evidence,
    )
    ex_ante_evidence = contract_evidence.get("ExAnteFeasibility") or {}
    statuses["ExAnteFeasibility"] = _status(
        "ExAnteFeasibility",
        "pass" if projection_built and projection_health.get("ex_ante_feasibility_ok") else "missing_evidence",
        "ex_ante_feasibility_missing_malformed_or_leaky",
        ex_ante_evidence,
    )
    earliest_actionable_evidence = contract_evidence.get("EarliestActionableTime") or {}
    statuses["EarliestActionableTime"] = _status(
        "EarliestActionableTime",
        "pass" if projection_built and projection_health.get("earliest_actionable_time_ok") else "missing_evidence",
        "earliest_actionable_missing_malformed_or_invariant_violation",
        earliest_actionable_evidence,
    )
    realtime_clean_evidence = contract_evidence.get("RealtimeCleanDetector") or {}
    statuses["RealtimeCleanDetector"] = _status(
        "RealtimeCleanDetector",
        "pass" if projection_built and projection_health.get("realtime_clean_detector_ok") else "missing_evidence",
        "realtime_clean_missing_malformed_or_future_leakage",
        realtime_clean_evidence,
    )
    quote_intent_evidence = contract_evidence.get("QuoteIntentBindingContract") or {}
    statuses["QuoteIntentBindingContract"] = _status(
        "QuoteIntentBindingContract",
        "pass" if projection_built and projection_health.get("quote_intent_binding_ok") else "missing_evidence",
        "quote_intent_binding_missing_malformed_mismatched_or_future_leakage",
        quote_intent_evidence,
    )
    raw_provider_evidence = contract_evidence.get("RawProviderEvidenceContract") or {}
    statuses["RawProviderEvidenceContract"] = _status(
        "RawProviderEvidenceContract",
        "pass" if projection_built and projection_health.get("raw_provider_evidence_ok") else "missing_evidence",
        "raw_provider_evidence_missing_malformed_or_untrusted",
        raw_provider_evidence,
    )
    randomness_control_evidence = contract_evidence.get("RandomnessControlContract") or {}
    statuses["RandomnessControlContract"] = _status(
        "RandomnessControlContract",
        "pass" if projection_built and projection_health.get("randomness_control_ok") else "missing_evidence",
        "randomness_control_missing_malformed_or_invalid",
        randomness_control_evidence,
    )
    deployment_rollout_evidence = contract_evidence.get("DeploymentRolloutStateMachine") or {}
    statuses["DeploymentRolloutStateMachine"] = _status(
        "DeploymentRolloutStateMachine",
        "pass" if projection_built and projection_health.get("deployment_rollout_state_machine_ok") else "missing_evidence",
        "deployment_rollout_missing_malformed_or_invalid",
        deployment_rollout_evidence,
    )
    worker_fleet_evidence = contract_evidence.get("WorkerFleetConsistencyContract") or {}
    statuses["WorkerFleetConsistencyContract"] = _status(
        "WorkerFleetConsistencyContract",
        "pass" if projection_built and projection_health.get("worker_fleet_consistency_ok") else "missing_evidence",
        "worker_fleet_missing_malformed_or_inconsistent",
        worker_fleet_evidence,
    )
    backup_restore_evidence = contract_evidence.get("BackupRestoreDrillContract") or {}
    statuses["BackupRestoreDrillContract"] = _status(
        "BackupRestoreDrillContract",
        "pass" if projection_built and projection_health.get("backup_restore_drill_ok") else "missing_evidence",
        "backup_restore_drill_missing_malformed_or_invalid",
        backup_restore_evidence,
    )
    incident_freeze_evidence = contract_evidence.get("IncidentEvidenceFreezeContract") or {}
    statuses["IncidentEvidenceFreezeContract"] = _status(
        "IncidentEvidenceFreezeContract",
        "pass" if projection_built and projection_health.get("incident_evidence_freeze_ok") else "missing_evidence",
        "incident_evidence_freeze_missing_malformed_or_invalid",
        incident_freeze_evidence,
    )
    circuit_breaker_resume_evidence = contract_evidence.get("CircuitBreakerResumeContract") or {}
    statuses["CircuitBreakerResumeContract"] = _status(
        "CircuitBreakerResumeContract",
        "pass" if projection_built and projection_health.get("circuit_breaker_resume_ok") else "missing_evidence",
        "circuit_breaker_resume_missing_malformed_or_invalid",
        circuit_breaker_resume_evidence,
    )
    queue_durability_evidence = contract_evidence.get("QueueDurabilityContract") or {}
    statuses["QueueDurabilityContract"] = _status(
        "QueueDurabilityContract",
        "pass" if projection_built and projection_health.get("queue_durability_ok") else "missing_evidence",
        "queue_durability_missing_malformed_or_invalid",
        queue_durability_evidence,
    )
    candidate_cancellation_evidence = contract_evidence.get("CandidateCancellationContract") or {}
    statuses["CandidateCancellationContract"] = _status(
        "CandidateCancellationContract",
        "pass" if projection_built and projection_health.get("candidate_cancellation_ok") else "missing_evidence",
        "candidate_cancellation_missing_malformed_or_invalid",
        candidate_cancellation_evidence,
    )
    retry_storm_evidence = contract_evidence.get("RetryStormControlContract") or {}
    statuses["RetryStormControlContract"] = _status(
        "RetryStormControlContract",
        "pass" if projection_built and projection_health.get("retry_storm_control_ok") else "missing_evidence",
        "retry_storm_control_missing_malformed_or_invalid",
        retry_storm_evidence,
    )
    provider_coverage_evidence = contract_evidence.get("ProviderCoverageMapContract") or {}
    statuses["ProviderCoverageMapContract"] = _status(
        "ProviderCoverageMapContract",
        "pass" if projection_built and projection_health.get("provider_coverage_map_ok") else "missing_evidence",
        "provider_coverage_map_missing_malformed_or_invalid",
        provider_coverage_evidence,
    )
    training_serving_skew_evidence = contract_evidence.get("TrainingServingSkewContract") or {}
    statuses["TrainingServingSkewContract"] = _status(
        "TrainingServingSkewContract",
        "pass" if projection_built and projection_health.get("training_serving_skew_ok") else "missing_evidence",
        "training_serving_skew_missing_malformed_or_invalid",
        training_serving_skew_evidence,
    )
    idempotency_evidence = contract_evidence.get("IdempotencyContract") or {}
    statuses["IdempotencyContract"] = _status(
        "IdempotencyContract",
        "pass" if projection_built and projection_health.get("idempotency_contract_ok") else "missing_evidence",
        "idempotency_missing_malformed_collision_or_duplicate_action_conflict",
        idempotency_evidence,
    )
    idempotency_namespace_evidence = contract_evidence.get("IdempotencyKeyNamespaceContract") or {}
    statuses["IdempotencyKeyNamespaceContract"] = _status(
        "IdempotencyKeyNamespaceContract",
        "pass" if projection_built and projection_health.get("idempotency_key_namespace_ok") else "missing_evidence",
        "idempotency_namespace_missing_malformed_collision_or_policy_violation",
        idempotency_namespace_evidence,
    )
    execution_lease_evidence = contract_evidence.get("ExecutionLeaseContract") or {}
    statuses["ExecutionLeaseContract"] = _status(
        "ExecutionLeaseContract",
        "pass" if projection_built and projection_health.get("execution_lease_ok") else "missing_evidence",
        "execution_lease_missing_malformed_or_invalid",
        execution_lease_evidence,
    )
    state_fencing_evidence = contract_evidence.get("StateVersionFencing") or {}
    statuses["StateVersionFencing"] = _status(
        "StateVersionFencing",
        "pass" if projection_built and projection_health.get("state_version_fencing_ok") else "missing_evidence",
        "state_version_fencing_missing_malformed_or_invalid",
        state_fencing_evidence,
    )
    entry_execution_evidence = contract_evidence.get("EntryExecutionStateMachine") or {}
    statuses["EntryExecutionStateMachine"] = _status(
        "EntryExecutionStateMachine",
        "pass" if projection_built and projection_health.get("entry_execution_state_machine_ok") else "missing_evidence",
        "entry_execution_state_machine_missing_malformed_or_invalid",
        entry_execution_evidence,
    )
    position_ledger_evidence = contract_evidence.get("PaperPositionLedgerContract") or {}
    statuses["PaperPositionLedgerContract"] = _status(
        "PaperPositionLedgerContract",
        "pass" if projection_built and projection_health.get("paper_position_ledger_ok") else "missing_evidence",
        "paper_position_ledger_missing_malformed_or_invalid",
        position_ledger_evidence,
    )
    capital_ledger_evidence = contract_evidence.get("PaperCapitalLedgerContract") or {}
    statuses["PaperCapitalLedgerContract"] = _status(
        "PaperCapitalLedgerContract",
        "pass" if projection_built and projection_health.get("paper_capital_ledger_ok") else "missing_evidence",
        "paper_capital_ledger_missing_malformed_or_invalid",
        capital_ledger_evidence,
    )
    double_entry_evidence = contract_evidence.get("DoubleEntryLedgerInvariantContract") or {}
    statuses["DoubleEntryLedgerInvariantContract"] = _status(
        "DoubleEntryLedgerInvariantContract",
        "pass" if projection_built and projection_health.get("double_entry_ledger_invariant_ok") else "missing_evidence",
        "double_entry_ledger_invariant_missing_or_violated",
        double_entry_evidence,
    )
    reservation_evidence = contract_evidence.get("CapitalReservationPolicy") or {}
    statuses["CapitalReservationPolicy"] = _status(
        "CapitalReservationPolicy",
        "pass" if projection_built and projection_health.get("capital_reservation_policy_ok") else "missing_evidence",
        "capital_reservation_policy_missing_malformed_or_violated",
        reservation_evidence,
    )
    no_fill_evidence = contract_evidence.get("NoFillOutcome") or {}
    statuses["NoFillOutcome"] = _status(
        "NoFillOutcome",
        "pass" if projection_built and projection_health.get("no_fill_outcome_ok") else "missing_evidence",
        "no_fill_outcome_missing_malformed_or_invalid",
        no_fill_evidence,
    )
    recovery_evidence = contract_evidence.get("CrashRecoveryStateMachine") or {}
    statuses["CrashRecoveryStateMachine"] = _status(
        "CrashRecoveryStateMachine",
        "pass" if projection_built and projection_health.get("crash_recovery_state_machine_ok") else "missing_evidence",
        "crash_recovery_missing_malformed_or_invalid",
        recovery_evidence,
    )
    resume_drain_evidence = contract_evidence.get("ResumeDrainPolicy") or {}
    statuses["ResumeDrainPolicy"] = _status(
        "ResumeDrainPolicy",
        "pass" if projection_built and projection_health.get("resume_drain_policy_ok") else "missing_evidence",
        "resume_drain_missing_malformed_or_invalid",
        resume_drain_evidence,
    )
    for contract_id in (
        "ReplaySideEffectIsolationContract",
        "TransactionalOutboxContract",
        "DeadLetterQueueContract",
        "ConsumerCheckpointContract",
        "ProjectionHandlerIdempotencyContract",
        "CacheInvalidationContract",
    ):
        statuses[contract_id] = _consumer_contract_status(projection_consumer_health, contract_id)

    # Seed implementation has not yet produced proof for these contracts. They
    # must remain explicitly blocked rather than inferred from adjacent checks.
    missing_contracts = [
        "SpecConsistencyLinterContract",
        "PaperModeSafetyBoundary",
        "ChainConfigContract",
        "SourceRegistryContract",
        "InputSanitizationContract",
        "TransactionalOutboxContract",
        "DeadLetterQueueContract",
        "ConsumerCheckpointContract",
        "ProjectionHandlerIdempotencyContract",
        "CacheInvalidationContract",
        "SignalCreditAssignmentContract",
        "ReferencePriceContract",
        "MetricsWindowContract",
        "TradeOutcomeLabelContract",
        "StandardizedStopContract",
        "ExAnteFeasibility",
        "EarliestActionableTime",
        "RealtimeCleanDetector",
        "QuoteIntentBindingContract",
        "IdempotencyContract",
        "IdempotencyKeyNamespaceContract",
        "ExecutionLeaseContract",
        "StateVersionFencing",
        "EntryExecutionStateMachine",
        "PaperPositionLedgerContract",
        "PaperCapitalLedgerContract",
        "DoubleEntryLedgerInvariantContract",
        "CapitalReservationPolicy",
        "RawProviderEvidenceContract",
        "LabelFinalizationContract",
        "OutcomeWindowCloseContract",
    ]
    for contract_id in missing_contracts:
        statuses.setdefault(
            contract_id,
            _status(contract_id, "missing_evidence", f"{contract_id}_not_proven", {}),
        )
    return statuses


def _expanded_requirements(mode):
    if mode == "observe_only":
        return list(MODE_REQUIREMENTS["observe_only"])
    if mode == "shadow":
        return _expanded_requirements("observe_only") + list(MODE_REQUIREMENTS["shadow"])
    if mode == "ultra_tiny":
        return _expanded_requirements("shadow") + list(MODE_REQUIREMENTS["ultra_tiny"])
    if mode == "normal_tiny":
        return _expanded_requirements("ultra_tiny") + list(MODE_REQUIREMENTS["normal_tiny"])
    raise KeyError(mode)


def _build_modes(contract_statuses):
    modes = {}
    highest_allowed = None
    for mode in MODE_ORDER:
        required = list(dict.fromkeys(_expanded_requirements(mode)))
        blocking = [
            contract_id
            for contract_id in required
            if contract_statuses.get(contract_id, {}).get("status") != "pass"
        ]
        modes[mode] = {
            "mode": mode,
            "required_contracts": required,
            "blocking_contracts": blocking,
            "status": "allowed" if not blocking else "blocked",
        }
        if not blocking:
            highest_allowed = mode
    return modes, highest_allowed


def _mode_readiness_matrix_status(modes):
    required_fields = ["mode", "required_contracts", "status", "blocking_contracts"]
    row_violations = []
    missing_modes = [mode for mode in MODE_ORDER if mode not in modes]
    for mode in MODE_ORDER:
        row = modes.get(mode)
        if not isinstance(row, dict):
            row_violations.append({"mode": mode, "reason": "mode_row_missing_or_not_object"})
            continue
        missing_fields = [field for field in required_fields if field not in row]
        required = row.get("required_contracts")
        blocking = row.get("blocking_contracts")
        violations = []
        if missing_fields:
            violations.append("required_fields_missing")
        if row.get("mode") != mode:
            violations.append("mode_field_mismatch")
        if not isinstance(required, list) or not required:
            violations.append("required_contracts_list_required")
            required_set = set()
        else:
            required_set = set(required)
        if not isinstance(blocking, list):
            violations.append("blocking_contracts_list_required")
            blocking_set = set()
        else:
            blocking_set = set(blocking)
        if blocking_set - required_set:
            violations.append("blocking_contracts_must_be_subset_of_required_contracts")
        expected_status = "allowed" if not blocking_set else "blocked"
        if row.get("status") != expected_status:
            violations.append("status_must_match_blocking_contracts")
        if violations:
            row_violations.append(
                {
                    "mode": mode,
                    "missing_fields": missing_fields,
                    "violations": violations,
                }
            )
    passed = not missing_modes and not row_violations
    return _status(
        "ModeReadinessMatrix",
        "pass" if passed else "missing_evidence",
        "mode_readiness_matrix_missing_malformed_or_inconsistent",
        {
            "matrix_schema_version": "v2.7.0.mode_readiness.v1",
            "required_fields": required_fields,
            "mode_count": len(modes),
            "modes": [
                {
                    "mode": modes.get(mode, {}).get("mode"),
                    "required_contract_count": len(modes.get(mode, {}).get("required_contracts") or []),
                    "blocking_contract_count": len(modes.get(mode, {}).get("blocking_contracts") or []),
                    "status": modes.get(mode, {}).get("status"),
                }
                for mode in MODE_ORDER
                if isinstance(modes.get(mode), dict)
            ],
            "missing_modes": missing_modes,
            "row_violations": row_violations,
        },
    )


def _with_mode_health_context(report, mode_health, *, component_ready_key=None, component_ready=None):
    if not isinstance(report, dict):
        return report
    output = copy.deepcopy(report)
    health = output.get("health")
    if not isinstance(health, dict):
        return output
    if component_ready_key is not None:
        health[component_ready_key] = bool(component_ready)
    health["normal_tiny_ready"] = bool(mode_health.get("normal_tiny_ready"))
    health["normal_tiny_ready_source"] = "mode_readiness_matrix"
    return output


def build_mode_readiness_matrix(
    *,
    event_log_dir=DEFAULT_EVENT_LOG_DIR,
    snapshot_path=DEFAULT_SNAPSHOT_PATH,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
    consumer_health_path=None,
    max_snapshot_age_ms=300_000,
):
    spec_report = None
    spec_error = None
    try:
        spec_report = validate_all(manifest_path, catalog_path, registry_path)
    except Exception as exc:
        spec_error = str(exc)

    catalog = _load_json(catalog_path)
    basic_readiness = build_basic_contract_readiness(
        manifest_path=manifest_path,
        catalog_path=catalog_path,
        registry_path=registry_path,
    )
    snapshot_report = validate_snapshot_file(snapshot_path, max_snapshot_age_ms=max_snapshot_age_ms)
    snapshot = _snapshot_payload(snapshot_path) or {}
    event_log = _event_log_report(event_log_dir)
    consumer_health_path = Path(consumer_health_path) if consumer_health_path else Path(snapshot_path).parent / CONSUMER_HEALTH_FILE
    projection_consumer_health = read_projection_consumer_health(consumer_health_path)
    contract_statuses = build_contract_statuses(
        spec_report=spec_report,
        spec_error=spec_error,
        event_log=event_log,
        snapshot_report=snapshot_report,
        snapshot=snapshot,
        registry_path=registry_path,
        basic_readiness=basic_readiness,
        projection_consumer_health=projection_consumer_health,
    )

    catalog_contracts = catalog.get("contracts") or {}
    for contract_id, record in catalog_contracts.items():
        contract_statuses.setdefault(
            contract_id,
            _status(contract_id, "missing_evidence", f"{contract_id}_not_proven", {}),
        )
        contract_statuses[contract_id]["section_id"] = record.get("section_id")
        contract_statuses[contract_id]["mode_target"] = record.get("mode_target")
        contract_statuses[contract_id]["failure_action"] = record.get("failure_action")

    matrix_catalog_record = catalog_contracts.get("ModeReadinessMatrix") or {}
    contract_statuses["ModeReadinessMatrix"] = _status(
        "ModeReadinessMatrix",
        "pass",
        "mode_readiness_matrix_missing_malformed_or_inconsistent",
        {"provisional_self_check": True},
    )
    contract_statuses["ModeReadinessMatrix"]["section_id"] = matrix_catalog_record.get("section_id")
    contract_statuses["ModeReadinessMatrix"]["mode_target"] = matrix_catalog_record.get("mode_target")
    contract_statuses["ModeReadinessMatrix"]["failure_action"] = matrix_catalog_record.get("failure_action")
    modes, _ = _build_modes(contract_statuses)
    matrix_status = _mode_readiness_matrix_status(modes)
    matrix_status["section_id"] = matrix_catalog_record.get("section_id")
    matrix_status["mode_target"] = matrix_catalog_record.get("mode_target")
    matrix_status["failure_action"] = matrix_catalog_record.get("failure_action")
    contract_statuses["ModeReadinessMatrix"] = matrix_status
    modes, highest_allowed = _build_modes(contract_statuses)

    gate_scope = build_mode_gate_scope_audit(catalog, MODE_REQUIREMENTS, MODE_ORDER)
    health = {
        "status": "mode_readiness_evaluated",
        "dashboard_safe": bool(snapshot_report.get("health", {}).get("dashboard_safe")),
        "normal_tiny_ready": modes["normal_tiny"]["status"] == "allowed",
        "current_gate_normal_tiny_ready": modes["normal_tiny"]["status"] == "allowed",
        "final_spec_normal_tiny_ready": (
            modes["normal_tiny"]["status"] == "allowed"
            and bool(gate_scope.get("health", {}).get("final_normal_tiny_blocking_scope_complete"))
        ),
        "final_spec_normal_tiny_missing_count": gate_scope.get("health", {}).get(
            "final_normal_tiny_blocking_missing_count"
        ),
        "ultra_tiny_ready": modes["ultra_tiny"]["status"] == "allowed",
        "shadow_ready": modes["shadow"]["status"] == "allowed",
        "observe_only_ready": modes["observe_only"]["status"] == "allowed",
    }

    return {
        "matrix_schema_version": "v2.7.0.mode_readiness.v1",
        "generated_at": _utc_now_iso(),
        "event_log_dir": str(event_log_dir),
        "snapshot_path": str(snapshot_path),
        "spec": spec_report or {"spec_valid": False, "error": spec_error},
        "event_log": event_log,
        "read_model": _with_mode_health_context(
            snapshot_report,
            health,
            component_ready_key="read_model_fresh",
            component_ready=snapshot_report.get("health", {}).get("dashboard_safe"),
        ),
        "basic_readiness": _with_mode_health_context(
            basic_readiness,
            health,
            component_ready_key="basic_contracts_ready",
            component_ready=not (basic_readiness.get("blocking_contracts") or []),
        ),
        "projection_consumer": _with_mode_health_context(
            projection_consumer_health,
            health,
            component_ready_key="projection_consumer_ready",
            component_ready=projection_consumer_health.get("health", {}).get("shadow_consumer_ready"),
        ),
        "gate_scope": gate_scope,
        "contract_statuses": contract_statuses,
        "modes": modes,
        "highest_allowed_mode": highest_allowed,
        "health": health,
    }


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--snapshot-path", default=str(DEFAULT_SNAPSHOT_PATH))
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--registry", default=str(ENTRY_MODE_REGISTRY_PATH))
    parser.add_argument("--consumer-health-path")
    parser.add_argument("--max-snapshot-age-ms", type=int, default=300_000)
    parser.add_argument("--output")
    parser.add_argument("--strict-mode", choices=MODE_ORDER)
    args = parser.parse_args()

    matrix = build_mode_readiness_matrix(
        event_log_dir=Path(args.event_log_dir),
        snapshot_path=Path(args.snapshot_path),
        manifest_path=Path(args.manifest),
        catalog_path=Path(args.catalog),
        registry_path=Path(args.registry),
        consumer_health_path=Path(args.consumer_health_path) if args.consumer_health_path else None,
        max_snapshot_age_ms=args.max_snapshot_age_ms,
    )
    if args.output:
        write_json(args.output, matrix)
    print(json.dumps(matrix, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict_mode and matrix["modes"][args.strict_mode]["status"] != "allowed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
