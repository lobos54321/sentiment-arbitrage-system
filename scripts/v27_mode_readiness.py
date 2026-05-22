#!/usr/bin/env python3
"""Build the v2.7 ModeReadinessMatrix from current machine evidence."""

import argparse
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
        "SpecConsistencyLinterContract",
        "PaperModeSafetyBoundary",
        "ChainConfigContract",
        "SourceRegistryContract",
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
        verify = V27EventLog(event_log_dir).verify()
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
    for contract_id in (
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
        "NoFillOutcome",
        "CrashRecoveryStateMachine",
        "ResumeDrainPolicy",
        "RawProviderEvidenceContract",
        "LabelFinalizationContract",
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

    return {
        "matrix_schema_version": "v2.7.0.mode_readiness.v1",
        "generated_at": _utc_now_iso(),
        "event_log_dir": str(event_log_dir),
        "snapshot_path": str(snapshot_path),
        "spec": spec_report or {"spec_valid": False, "error": spec_error},
        "event_log": event_log,
        "read_model": snapshot_report,
        "basic_readiness": basic_readiness,
        "projection_consumer": projection_consumer_health,
        "contract_statuses": contract_statuses,
        "modes": modes,
        "highest_allowed_mode": highest_allowed,
        "health": {
            "status": "mode_readiness_evaluated",
            "dashboard_safe": bool(snapshot_report.get("health", {}).get("dashboard_safe")),
            "normal_tiny_ready": modes["normal_tiny"]["status"] == "allowed",
            "ultra_tiny_ready": modes["ultra_tiny"]["status"] == "allowed",
            "shadow_ready": modes["shadow"]["status"] == "allowed",
            "observe_only_ready": modes["observe_only"]["status"] == "allowed",
        },
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
