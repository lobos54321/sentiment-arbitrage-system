import json
from pathlib import Path

import pytest

from scripts.v27_spec_validate import SpecValidationError, validate_all


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "spec"
    / "telegram_dog_regime_capture"
    / "v2.7.0"
    / "spec.manifest.json"
)
CATALOG_PATH = SPEC_PATH.parent / "contract-catalog.json"
GAP_REGISTER_PATH = SPEC_PATH.parent / "gap-register.json"
ENTRY_MODE_REGISTRY_PATH = Path(__file__).resolve().parent / "config" / "entry-mode-registry.json"


def _load_manifest():
    with SPEC_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_gap_register():
    with GAP_REGISTER_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def test_v27_manifest_has_stable_identity_and_rendered_views():
    manifest = _load_manifest()

    assert manifest["spec_id"] == "telegram_dog_regime_capture"
    assert manifest["spec_version"] == "2.7.0"
    assert manifest["gap_register_file"] == "gap-register.json"

    rendered = manifest["rendered_views"]
    assert len(rendered) == 3
    assert sum(view["lines"] for view in rendered) == 9016
    assert all(len(view["sha256"]) == 64 for view in rendered)


def test_v27_manifest_has_contiguous_section_ids():
    manifest = _load_manifest()

    expected = [f"S{idx:02d}" for idx in range(24)]
    actual = [section["section_id"] for section in manifest["sections"]]

    assert actual == expected


def test_v27_manifest_tracks_m0_freeze_modes_and_high_risk_contracts():
    manifest = _load_manifest()

    assert set(manifest["m0_freeze_modes"]) == {
        "hard_gate_pass_tiny_probe",
        "source_resonance_tiny_probe",
    }

    contracts = set(manifest["mvp_blocking_contracts"]) | set(
        manifest["high_risk_carry_forward_contracts"]
    )
    required = {
        "EventSequencerContract",
        "ConsumerCheckpointContract",
        "RawProviderEvidenceContract",
        "QuoteIntentBindingContract",
        "AuditLogIntegrityContract",
        "DoubleEntryLedgerInvariantContract",
        "RandomnessControlContract",
        "DeploymentRolloutStateMachine",
        "WorkerFleetConsistencyContract",
        "QueueDurabilityContract",
        "CandidateCancellationContract",
        "RetryStormControlContract",
        "WritePathRegistryContract",
        "EntryPointInventoryContract",
        "StaticPolicyEnforcementContract",
        "EvidenceEligibilityMatrix",
        "TopFixQueueContract",
        "SafetyCaseContract",
        "SafeDefaultContract",
    }

    assert required <= contracts


def test_v27_gap_register_carries_forward_adversarial_contracts():
    gap_register = _load_gap_register()
    gap_contracts = {
        contract_id
        for batch in gap_register["batches"]
        for contract_id in batch["contract_ids"]
    }

    assert gap_register["status"] == "machine_checkable_adversarial_gap_register"
    assert len(gap_contracts) == 175
    required = {
        "AggregateBoundaryContract",
        "ProviderByzantineQuorumContract",
        "PolicyActivationBarrierContract",
        "AdminSessionSecurityContract",
        "DatabaseTransactionIsolationContract",
        "ResourceExhaustionContract",
        "SourceImpersonationDetector",
        "ContractConflictResolutionContract",
        "RuntimeSpecAssertionContract",
        "PromotionEvidencePackageContract",
        "RollbackVerificationContract",
        "LabelDisputeResolutionContract",
    }

    assert required <= gap_contracts


def test_v27_spec_validator_computes_stable_hash_and_contract_coverage():
    result = validate_all(SPEC_PATH, CATALOG_PATH, ENTRY_MODE_REGISTRY_PATH)

    assert result["spec_id"] == "telegram_dog_regime_capture"
    assert result["spec_version"] == "2.7.0"
    assert result["section_count"] == 24
    assert result["required_contract_count"] == 77
    assert result["catalog_contract_count"] == 77
    assert result["gap_register_count"] == 175
    assert result["spec_hash"] == "575db4a61a7040bc148d575f4a9ae39436bde3d917cba93c6262500b729244a1"


def test_v27_spec_validator_rejects_reopened_m0_direct_probe_modes(tmp_path):
    registry = json.loads(ENTRY_MODE_REGISTRY_PATH.read_text(encoding="utf-8"))
    registry["modes"]["hard_gate_pass_tiny_probe"]["paper_enabled"] = True
    registry["modes"]["hard_gate_pass_tiny_probe"]["tier"] = "live"

    registry_path = tmp_path / "entry-mode-registry.json"
    registry_path.write_text(json.dumps(registry), encoding="utf-8")

    with pytest.raises(SpecValidationError, match="still paper-enabled"):
        validate_all(SPEC_PATH, CATALOG_PATH, registry_path)
