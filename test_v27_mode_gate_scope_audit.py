import sys

sys.path.insert(0, "scripts")

from v27_mode_gate_scope import build_mode_gate_scope_audit  # noqa: E402
from v27_mode_readiness import CATALOG_PATH, MODE_ORDER, MODE_REQUIREMENTS, build_mode_readiness_matrix  # noqa: E402
from v27_spec_validate import load_json  # noqa: E402


def test_mode_gate_scope_audit_reports_final_normal_tiny_scope_covered():
    audit = build_mode_gate_scope_audit(load_json(CATALOG_PATH), MODE_REQUIREMENTS, MODE_ORDER)

    normal = audit["final_scopes"]["normal_tiny_blocking"]
    phase_1 = audit["final_scopes"]["phase_1_hardening"]
    ultra = audit["final_scopes"]["ultra_tiny"]
    shadow = audit["final_scopes"]["shadow"]
    mvp = audit["final_scopes"]["mvp_blocking"]
    assert audit["health"]["current_gate_normal_tiny_contract_count"] == len(
        set(MODE_REQUIREMENTS["observe_only"])
        | set(MODE_REQUIREMENTS["shadow"])
        | set(MODE_REQUIREMENTS["ultra_tiny"])
        | set(MODE_REQUIREMENTS["normal_tiny"])
    )
    assert normal["scope_complete"] is True
    assert ultra["missing_count"] == 0
    assert shadow["missing_count"] == 0
    assert normal["missing_count"] == 0
    assert phase_1["missing_count"] == 0
    assert phase_1["scope_complete"] is True
    assert mvp["missing_count"] == 0
    assert "AccessControlContract" not in normal["missing_contracts"]
    assert "AggregateBoundaryContract" not in normal["missing_contracts"]
    assert "AuditLogIntegrityContract" not in normal["missing_contracts"]
    assert "ClockRollbackGuardContract" not in normal["missing_contracts"]
    assert "DirectDatabaseMutationBan" not in normal["missing_contracts"]
    assert "EnumEvolutionContract" not in normal["missing_contracts"]
    assert "EventSchemaCompatibilityContract" not in normal["missing_contracts"]
    assert "BackgroundJobRegistryContract" not in normal["missing_contracts"]
    assert "ScheduledJobModeGateContract" not in normal["missing_contracts"]
    assert "EntryPointInventoryContract" not in normal["missing_contracts"]
    assert "StaticPolicyEnforcementContract" not in normal["missing_contracts"]
    assert "FeatureFlagDependencyContract" not in normal["missing_contracts"]
    assert "FilesystemDiskPressurePolicy" not in normal["missing_contracts"]
    assert "APIResponseContract" not in normal["missing_contracts"]
    assert "APIResponseEnvelopeContract" not in normal["missing_contracts"]
    assert "ErrorTaxonomyContract" not in normal["missing_contracts"]
    assert "LogRedactionVerificationContract" not in normal["missing_contracts"]
    assert "AdminSessionSecurityContract" not in normal["missing_contracts"]
    assert "SecretAccessAuditContract" not in normal["missing_contracts"]
    assert "TelegramSessionSecurityContract" not in normal["missing_contracts"]
    assert "QueueAckNackContract" not in normal["missing_contracts"]
    assert "PipelineProgressInvariant" not in normal["missing_contracts"]
    assert "ThreadPoolIsolationContract" not in normal["missing_contracts"]
    assert "CICDMergeGateContract" not in normal["missing_contracts"]
    assert "GeneratedClientContract" not in normal["missing_contracts"]
    assert "SpecChangeImpactAnalysisContract" not in normal["missing_contracts"]
    assert "ServiceReadinessProbeContract" not in normal["missing_contracts"]
    assert "DashboardActionSeparationContract" not in normal["missing_contracts"]
    assert "ModeReadinessMatrix" not in normal["missing_contracts"]
    assert "MutationCommandIdempotencyContract" not in normal["missing_contracts"]
    assert "ProjectionVersionIsolationContract" not in normal["missing_contracts"]
    assert "SnapshotCompactionInvariantContract" not in normal["missing_contracts"]
    assert "SnapshotCompactionReadBarrier" not in normal["missing_contracts"]
    assert "ConfigDistributionContract" not in normal["missing_contracts"]
    assert "ConfigDistributionAckContract" not in normal["missing_contracts"]
    assert "InFlightConfigRotationPolicy" not in normal["missing_contracts"]
    assert "PolicyActivationBarrierContract" not in normal["missing_contracts"]
    assert "RetryPolicyCatalogContract" not in normal["missing_contracts"]
    assert "AlertNoiseBudgetContract" not in normal["missing_contracts"]
    assert "AlertSuppressionAuditContract" not in normal["missing_contracts"]
    assert "CanaryAbortContract" not in normal["missing_contracts"]
    assert "ModelArtifactRuntimeCompatibilityContract" not in normal["missing_contracts"]
    assert "ModelRollbackContract" not in normal["missing_contracts"]
    assert "PostReleaseMonitoringWindow" not in normal["missing_contracts"]
    assert "TrainingPoisoningGuard" not in normal["missing_contracts"]
    assert "WorkerHeartbeatContract" not in normal["missing_contracts"]
    assert "SilentWorkerDeathDetector" not in normal["missing_contracts"]
    assert "WarmStartSafetyContract" not in normal["missing_contracts"]
    assert "ConnectionPoolPartitionContract" not in normal["missing_contracts"]
    assert "DBLockContentionPolicy" not in normal["missing_contracts"]
    assert "DatabaseTransactionIsolationContract" not in normal["missing_contracts"]
    assert "DistributedLockBackendHealthContract" not in normal["missing_contracts"]
    assert "HumanReadableReasonContract" not in normal["missing_contracts"]
    assert "MachineReadableReasonContract" not in normal["missing_contracts"]
    assert "NumericPrecisionContract" not in mvp["missing_contracts"]
    assert "SafeDefaultContract" not in mvp["missing_contracts"]
    assert "ReplaySideEffectIsolationContract" not in normal["missing_contracts"]
    assert "ManualReplaySafetyContract" not in normal["missing_contracts"]
    assert "SyntheticSentinelEventContract" not in normal["missing_contracts"]
    assert "ReconciliationDiffContract" not in normal["missing_contracts"]
    assert "ClientSideCacheContract" not in normal["missing_contracts"]
    assert "ClientSideFreshnessContract" not in normal["missing_contracts"]
    assert "DashboardQueryProvenanceContract" not in normal["missing_contracts"]
    assert "DashboardComputationProvenanceContract" not in normal["missing_contracts"]
    assert "DataExportWatermarkContract" not in normal["missing_contracts"]
    assert "DataExportEnvelopeContract" not in normal["missing_contracts"]
    assert "TradeOutcomeLabelContract" not in shadow["missing_contracts"]
    assert "StandardizedStopContract" not in shadow["missing_contracts"]
    assert "ExAnteFeasibility" not in shadow["missing_contracts"]
    assert "EarliestActionableTime" not in shadow["missing_contracts"]
    assert "ParserCanaryCorpusContract" not in shadow["missing_contracts"]
    assert "ParserAmbiguityContract" not in shadow["missing_contracts"]
    assert "TelegramForwardedMessagePolicy" not in shadow["missing_contracts"]
    assert "PremiumSourceAccessHealthContract" not in shadow["missing_contracts"]
    assert "SourceAuthenticityContract" not in shadow["missing_contracts"]
    assert "ParserConfusablesContract" not in shadow["missing_contracts"]
    assert "ImageOCRSignalPolicy" not in shadow["missing_contracts"]
    assert "SourceImpersonationDetector" not in shadow["missing_contracts"]
    assert "IdentityMergeSplitContract" not in shadow["missing_contracts"]
    assert "ReKeyingContract" not in shadow["missing_contracts"]
    assert "SourceGapBackfillBoundary" not in shadow["missing_contracts"]
    assert "ObservationPolicyContract" not in shadow["missing_contracts"]
    assert "CounterfactualEntryTime" not in shadow["missing_contracts"]
    assert "DecisionAudit" not in ultra["missing_contracts"]
    assert "LedgerSnapshotHashContract" not in ultra["missing_contracts"]
    assert "WritePathRegistryContract" not in normal["missing_contracts"]
    assert "FeeScheduleSourceContract" not in normal["missing_contracts"]
    assert "FeeScheduleVersionContract" not in normal["missing_contracts"]
    assert "ProviderCredentialScopeContract" not in normal["missing_contracts"]
    assert "ProviderRequestReplayContract" not in normal["missing_contracts"]
    assert "ProviderResponseAuthenticityContract" not in normal["missing_contracts"]
    assert "RiskRevalidationAfterEntryContract" not in normal["missing_contracts"]
    assert "ProviderByzantineQuorumContract" not in normal["missing_contracts"]
    assert "ProviderCachePoisoningGuard" not in normal["missing_contracts"]
    assert "ExternalDependencyContract" not in normal["missing_contracts"]
    assert "ThirdPartyStatusCorrelationContract" not in normal["missing_contracts"]
    assert "ResourceExhaustionContract" not in normal["missing_contracts"]
    assert "FeatureStoreConsistencyContract" not in normal["missing_contracts"]
    assert "DynamicTokenAuthorityChangeContract" not in normal["missing_contracts"]
    assert "AdversarialExecutionSimulationContract" not in normal["missing_contracts"]
    assert "OpenPositionValuationContract" not in normal["missing_contracts"]
    assert "ExitPolicyMigrationContract" not in normal["missing_contracts"]
    assert "OpenPositionPolicyMigrationContract" not in normal["missing_contracts"]
    assert "PositionOwnershipTransferContract" not in normal["missing_contracts"]
    assert "RollbackVerificationContract" not in normal["missing_contracts"]
    assert "PartialRollbackPolicy" not in normal["missing_contracts"]
    assert "ReleaseReadinessReviewContract" not in normal["missing_contracts"]
    assert "ChangeFreezeContract" not in normal["missing_contracts"]
    assert "NotificationChannelIntegrityContract" not in normal["missing_contracts"]
    assert "RunbookFreshnessContract" not in normal["missing_contracts"]
    assert "MetricBackfillImpactContract" not in normal["missing_contracts"]
    assert "SelectionBiasDiagnosticContract" not in normal["missing_contracts"]
    assert "AccessReviewContract" not in normal["missing_contracts"]
    assert "ApprovalWorkflowContract" not in normal["missing_contracts"]
    assert "BreakGlassAccessContract" not in normal["missing_contracts"]
    assert "CSVSpreadsheetInjectionContract" not in normal["missing_contracts"]
    assert "EvidenceExternalAnchoringContract" not in normal["missing_contracts"]
    assert "ExperimentAssignmentImmutabilityContract" not in normal["missing_contracts"]
    assert "IncidentPostmortemContract" not in normal["missing_contracts"]
    assert "LabelDisputeResolutionContract" not in normal["missing_contracts"]
    assert "NegativeControlContract" not in normal["missing_contracts"]
    assert "OperatorTrainingCertificationContract" not in normal["missing_contracts"]
    assert audit["health"]["final_normal_tiny_blocking_scope_complete"] is True
    governance = audit["final_scopes"]["normal_tiny_governance"]
    assert audit["health"]["final_normal_tiny_governance_scope_complete"] is True
    assert audit["health"]["final_normal_tiny_governance_missing_count"] == 0
    for contract_id in (
        "ArchiveBitrotScrubContract",
        "DataDeletionLegalHoldContract",
        "DataLicenseComplianceContract",
        "ExportReimportBoundaryContract",
        "LegalHoldContract",
        "ProviderTermsComplianceContract",
    ):
        assert contract_id not in phase_1["missing_contracts"]

    for contract_id in (
        "RuntimeSpecAssertionContract",
        "MinimumViableTrustBoundary",
        "EvidenceConflictContract",
        "EvidenceAgingContract",
        "MarketRegimeInvalidatesEvidence",
        "SourceAlphaDecayExitCriteria",
        "FalseNegativeBudgetContract",
        "SmallSampleDecisionPolicy",
        "SafetyVsCaptureTradeoffContract",
        "ImplementationDriftMonitor",
        "AssumptionRegistryContract",
        "AssumptionInvalidationTrigger",
        "ContractPriorityGraph",
        "ContractConflictResolutionContract",
        "ContractFailureBlastRadius",
        "DashboardTriageWorkflowContract",
        "IssueEscalationFromMetricsContract",
        "PromotionEvidencePackageContract",
        "RegressionBudgetContract",
        "RootCauseTaxonomyVersioning",
        "CohortDriftBoundary",
        "ComplexityBudgetContract",
        "ExceptionDebtRegister",
        "GateRetirementPolicy",
        "GracefulDegradationBoundary",
        "InvariantSamplingAudit",
        "OperatorCognitiveLoadContract",
        "ResearchNotebookBoundaryContract",
        "UnknownUnknownsSamplingContract",
    ):
        assert contract_id not in governance["missing_contracts"]


def test_mode_gate_scope_audit_accepts_synthetic_complete_gate():
    catalog = {
        "contracts": {
            "CanonicalSpecIntegrityContract": {"mode_target": "all_modes"},
            "PaperModeSafetyBoundary": {"mode_target": "observe_only_blocking"},
            "EventSequencerContract": {"mode_target": "mvp_blocking"},
            "ParserAmbiguityContract": {"mode_target": "shadow_blocking"},
            "ExecutionLeaseContract": {"mode_target": "ultra_tiny_blocking"},
            "RawProviderEvidenceContract": {"mode_target": "normal_tiny_blocking"},
            "RandomnessControlContract": {"mode_target": "normal_tiny_promotion_blocking"},
        }
    }
    requirements = {
        "observe_only": ["CanonicalSpecIntegrityContract", "PaperModeSafetyBoundary", "EventSequencerContract"],
        "shadow": ["ParserAmbiguityContract"],
        "ultra_tiny": ["ExecutionLeaseContract"],
        "normal_tiny": ["RawProviderEvidenceContract", "RandomnessControlContract"],
    }

    audit = build_mode_gate_scope_audit(catalog, requirements, MODE_ORDER)

    assert audit["health"]["final_normal_tiny_blocking_scope_complete"] is True
    assert audit["health"]["final_normal_tiny_blocking_missing_count"] == 0
    assert audit["status"] == "final_scope_covered"


def test_mode_readiness_exposes_current_gate_vs_final_spec_scope(tmp_path):
    matrix = build_mode_readiness_matrix(
        event_log_dir=tmp_path / "events",
        snapshot_path=tmp_path / "missing_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["gate_scope"]["scope_audit_schema_version"] == "v2.7.0.mode_gate_scope_audit.v1"
    assert matrix["gate_scope"]["health"]["final_normal_tiny_blocking_scope_complete"] is True
    assert matrix["health"]["final_spec_normal_tiny_ready"] is False
    assert matrix["health"]["final_spec_normal_tiny_missing_count"] == 0
    assert matrix["health"]["current_gate_normal_tiny_ready"] is False
