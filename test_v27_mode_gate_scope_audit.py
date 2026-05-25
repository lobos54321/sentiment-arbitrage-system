import sys

sys.path.insert(0, "scripts")

from v27_mode_gate_scope import build_mode_gate_scope_audit  # noqa: E402
from v27_mode_readiness import CATALOG_PATH, MODE_ORDER, MODE_REQUIREMENTS, build_mode_readiness_matrix  # noqa: E402
from v27_spec_validate import load_json  # noqa: E402


def test_mode_gate_scope_audit_flags_final_normal_tiny_as_partial():
    audit = build_mode_gate_scope_audit(load_json(CATALOG_PATH), MODE_REQUIREMENTS, MODE_ORDER)

    normal = audit["final_scopes"]["normal_tiny_blocking"]
    mvp = audit["final_scopes"]["mvp_blocking"]
    assert audit["health"]["current_gate_normal_tiny_contract_count"] == len(
        set(MODE_REQUIREMENTS["observe_only"])
        | set(MODE_REQUIREMENTS["shadow"])
        | set(MODE_REQUIREMENTS["ultra_tiny"])
        | set(MODE_REQUIREMENTS["normal_tiny"])
    )
    assert normal["scope_complete"] is False
    assert normal["missing_count"] == 72
    assert mvp["missing_count"] == 3
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
    assert "WritePathRegistryContract" not in normal["missing_contracts"]
    assert "ManualReplaySafetyContract" in normal["missing_contracts"]
    assert "ProviderByzantineQuorumContract" in normal["missing_contracts"]
    assert audit["health"]["final_normal_tiny_blocking_scope_complete"] is False


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
    assert matrix["gate_scope"]["health"]["final_normal_tiny_blocking_scope_complete"] is False
    assert matrix["health"]["final_spec_normal_tiny_ready"] is False
    assert matrix["health"]["final_spec_normal_tiny_missing_count"] == 72
    assert matrix["health"]["current_gate_normal_tiny_ready"] is False
