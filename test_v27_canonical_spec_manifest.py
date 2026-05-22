import json
from pathlib import Path


SPEC_PATH = (
    Path(__file__).resolve().parent
    / "spec"
    / "telegram_dog_regime_capture"
    / "v2.7.0"
    / "spec.manifest.json"
)


def _load_manifest():
    with SPEC_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def test_v27_manifest_has_stable_identity_and_rendered_views():
    manifest = _load_manifest()

    assert manifest["spec_id"] == "telegram_dog_regime_capture"
    assert manifest["spec_version"] == "2.7.0"

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
