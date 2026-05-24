import json
import sys

sys.path.insert(0, "scripts")

from v27_read_model_refresh import refresh_denominator_read_model  # noqa: E402
from v27_record_normal_tiny_ops_evidence import (  # noqa: E402
    record_normal_tiny_ops_evidence,
)


OPS_CONTRACTS = (
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
)


def test_normal_tiny_ops_evidence_unblocks_operational_contracts(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"

    report = record_normal_tiny_ops_evidence(event_log_dir, run_id="unit", strict=True)

    assert report["health"]["status"] == "normal_tiny_ops_evidence_recorded"
    assert report["planned_event_count"] == 15
    assert report["append_status_counts"] == {"appended": 15}
    assert report["event_log_summary_after"]["event_count"] == 15

    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        mode_readiness_path=out_dir / "mode_readiness.json",
        max_snapshot_age_ms=300_000,
    )
    matrix = json.loads((out_dir / "mode_readiness.json").read_text(encoding="utf-8"))
    normal_tiny_blockers = matrix["modes"]["normal_tiny"]["blocking_contracts"]

    for contract_id in OPS_CONTRACTS:
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in normal_tiny_blockers

    assert "RawProviderEvidenceContract" in normal_tiny_blockers
    assert "RandomnessControlContract" in normal_tiny_blockers


def test_normal_tiny_ops_evidence_dry_run_does_not_append(tmp_path):
    event_log_dir = tmp_path / "events"

    report = record_normal_tiny_ops_evidence(event_log_dir, run_id="dry", dry_run=True)

    assert report["health"]["status"] == "normal_tiny_ops_evidence_dry_run"
    assert report["append_status_counts"] == {}
    assert not (event_log_dir / "events.jsonl").exists()
