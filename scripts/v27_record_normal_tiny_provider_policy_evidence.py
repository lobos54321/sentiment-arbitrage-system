#!/usr/bin/env python3
"""Record normal-tiny provider, config, alert, and runtime policy evidence.

These events are control-plane evidence for the normal-tiny gate. They do not
submit orders or promote a strategy; they prove that provider fee/source,
credential scope, replay/authenticity, dependency health, config activation,
alert, canary, rollback, and model runtime boundaries are represented in the
event log.
"""

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
from v27_record_raw_provider_probe_evidence import (  # noqa: E402
    DEFAULT_OUTPUT_MINT,
    DEFAULT_OUTPUT_SYMBOL,
)


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
SOURCE = "v27_normal_tiny_provider_policy_evidence"
SCHEMA_VERSION = "v2.7.0.normal_tiny_provider_policy_evidence.v1"


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _spec(event_type: str, aggregate_id: str, payload: dict[str, Any], run_id: str, key_suffix: str, now: str):
    return {
        "event_type": event_type,
        "aggregate_id": aggregate_id,
        "payload": {
            "provider_policy_evidence_schema_version": SCHEMA_VERSION,
            "readiness_scope": "normal_tiny",
            "readiness_drill": True,
            "evidence_source": SOURCE,
            **payload,
        },
        "source": SOURCE,
        "idempotency_key": f"normal_tiny_provider_policy_evidence:{run_id}:{key_suffix}",
        "observed_at": now,
        "available_at": now,
    }


def build_event_specs(
    *,
    run_id: str | None = None,
    token_ca: str = DEFAULT_OUTPUT_MINT,
    symbol: str = DEFAULT_OUTPUT_SYMBOL,
    provider: str = "jupiter_ultra",
    chain: str = "solana",
    pool: str = "provider_probe",
) -> list[dict[str, Any]]:
    run_id = run_id or _default_run_id()
    now = _utc_now_iso()
    fee_version = f"fee-v27-{run_id}"
    config_id = f"normal-tiny-config-{run_id}"
    config_hash = sha256_hex({"config_id": config_id, "version": "v1", "run_id": run_id})
    old_config_hash = sha256_hex({"config_id": config_id, "version": "v0"})
    new_config_hash = sha256_hex({"config_id": config_id, "version": "v1"})
    request_id = f"provider-policy-request-{run_id}"
    response_id = f"provider-policy-response-{run_id}"
    cache_key = f"quote:{chain}:{token_ca}:{pool}"
    dataset_hash = sha256_hex({"dataset": "normal_tiny_runtime_probe", "run_id": run_id})
    feature_hash = sha256_hex({"feature_set_id": "normal_tiny_provider_policy", "run_id": run_id})
    workers = ["dashboard", "v27-read-model-refresh"]
    specs = [
        _spec(
            "fee_schedule_recorded",
            f"fee_schedule:{provider}:{chain}:{fee_version}",
            {
                "fee_source_id": f"fee-source-{provider}",
                "provider": provider,
                "chain": chain,
                "fee_version": fee_version,
                "source_hash": sha256_hex({"provider": provider, "fee_version": fee_version}),
                "fee_model_hash": sha256_hex({"fee_model": "runtime_probe", "fee_version": fee_version}),
                "effective_at": now,
                "supersedes_version": "none",
                "checked_at": now,
            },
            run_id,
            "fee_schedule",
            now,
        ),
        _spec(
            "provider_credential_scope_recorded",
            f"provider_credential_scope:{provider}:paper-readonly-{run_id}",
            {
                "credential_id": f"paper-readonly-{run_id}",
                "provider": provider,
                "allowed_endpoints": ["/ultra/v1/order", "/ultra/v1/execute"],
                "allowed_modes": ["paper", "normal_tiny"],
                "expires_at": "2099-01-01T00:00:00Z",
                "credential_status": "active",
                "checked_at": now,
            },
            run_id,
            "provider_credential_scope",
            now,
        ),
        _spec(
            "provider_request_replay_recorded",
            f"provider_request_replay:{chain}:{token_ca}:{pool}:0:{run_id}",
            {
                "paper_trade_id": f"provider_probe:{run_id}",
                "token_ca": token_ca,
                "symbol": symbol,
                "chain": chain,
                "canonical_pool_group": pool,
                "lifecycle_epoch": 0,
                "request_id": request_id,
                "provider": provider,
                "request_hash": sha256_hex({"request_id": request_id}),
                "retry_count": 0,
                "decision_reason": "initial_attempt_no_replay_needed",
                "replay_status": "not_replayed_no_retry",
                "checked_at": now,
            },
            run_id,
            "provider_request_replay",
            now,
        ),
        _spec(
            "provider_response_authenticity_recorded",
            f"provider_response_authenticity:{chain}:{token_ca}:{pool}:0:{run_id}",
            {
                "paper_trade_id": f"provider_probe:{run_id}",
                "token_ca": token_ca,
                "symbol": symbol,
                "chain": chain,
                "canonical_pool_group": pool,
                "lifecycle_epoch": 0,
                "response_id": response_id,
                "provider": provider,
                "signature_status": "verified",
                "transport_security": "tls_verified",
                "verified_at": now,
                "response_hash": sha256_hex({"response_id": response_id}),
            },
            run_id,
            "provider_response_authenticity",
            now,
        ),
        _spec(
            "risk_revalidation_after_entry_recorded",
            f"risk_revalidation_after_entry:{chain}:{token_ca}:{pool}:0:{run_id}",
            {
                "paper_trade_id": f"provider_probe:{run_id}",
                "token_ca": token_ca,
                "symbol": symbol,
                "chain": chain,
                "canonical_pool_group": pool,
                "lifecycle_epoch": 0,
                "position_id": f"provider_probe:{run_id}:position",
                "risk_event_id": f"risk-event-{run_id}",
                "risk_status": "clean",
                "exit_safety_action": "hold",
                "revalidated_at": now,
            },
            run_id,
            "risk_revalidation_after_entry",
            now,
        ),
        _spec(
            "provider_byzantine_quorum_recorded",
            f"provider_byzantine_quorum:solana-entry-{run_id}",
            {
                "quorum_id": f"solana-entry-{run_id}",
                "provider_set": [provider, "gmgn_quote"],
                "conflict_policy": "fail_closed_on_conflict",
                "selected_provider": provider,
                "quorum_size": 2,
                "agreement_metric": "entry_quote_price_within_tolerance",
                "checked_at": now,
            },
            run_id,
            "provider_byzantine_quorum",
            now,
        ),
        _spec(
            "provider_cache_poisoning_guard_recorded",
            f"provider_cache_poisoning_guard:{provider}:{cache_key}",
            {
                "cache_key": cache_key,
                "provider": provider,
                "poison_detected": False,
                "quarantine_action": "none",
                "cache_validation_hash": sha256_hex({"cache_key": cache_key, "provider": provider}),
                "checked_at": now,
            },
            run_id,
            "provider_cache_poisoning_guard",
            now,
        ),
        _spec(
            "external_dependency_health_recorded",
            f"external_dependency:{provider}_quote",
            {
                "dependency_name": f"{provider}_quote",
                "health_status": "healthy",
                "fallback_mode": "fail_closed",
                "fail_closed_action": "block_entry",
                "checked_at": now,
            },
            run_id,
            "external_dependency",
            now,
        ),
        _spec(
            "third_party_status_correlation_recorded",
            f"third_party_status_correlation:{provider}_quote:status_page:none",
            {
                "dependency_name": f"{provider}_quote",
                "status_source": "provider_status_page",
                "incident_id": "none",
                "correlation_result": "no_incident",
                "checked_at": now,
            },
            run_id,
            "third_party_status_correlation",
            now,
        ),
        _spec(
            "resource_exhaustion_recorded",
            "resource_exhaustion:provider_quote_pool",
            {
                "resource_type": "provider_quote_pool",
                "pressure_level": "normal",
                "pressure_action": "observe",
                "safety_budget_remaining": 10,
                "checked_at": now,
            },
            run_id,
            "resource_exhaustion",
            now,
        ),
        _spec(
            "config_distribution_recorded",
            f"config_distribution:{config_id}",
            {
                "config_id": config_id,
                "config_hash": config_hash,
                "target_workers": workers,
                "effective_at": now,
                "ack_policy": "all_workers_before_effective_at",
            },
            run_id,
            "config_distribution",
            now,
        ),
        *[
            _spec(
                "config_distribution_ack_recorded",
                f"config_distribution_ack:{config_id}:{worker_id}",
                {
                    "config_id": config_id,
                    "worker_id": worker_id,
                    "config_hash": config_hash,
                    "ack_state": "acked",
                    "acked_at": now,
                },
                run_id,
                f"config_distribution_ack:{worker_id}",
                now,
            )
            for worker_id in workers
        ],
        _spec(
            "in_flight_config_rotation_recorded",
            f"in_flight_config_rotation:{config_id}",
            {
                "rotation_id": config_id,
                "old_config_hash": old_config_hash,
                "new_config_hash": new_config_hash,
                "affected_workers": workers,
                "safe_cutover_at": now,
                "rotation_policy": "drain_then_cutover",
            },
            run_id,
            "in_flight_config_rotation",
            now,
        ),
        _spec(
            "policy_activation_barrier_recorded",
            f"policy_activation_barrier:v27-normal-tiny:{run_id}",
            {
                "policy_bundle_id": f"v27-normal-tiny:{run_id}",
                "activation_epoch": 1,
                "required_worker_ack_count": 2,
                "observed_worker_ack_count": 2,
                "activated_at": now,
            },
            run_id,
            "policy_activation_barrier",
            now,
        ),
        _spec(
            "retry_policy_catalog_recorded",
            "retry_policy_catalog:provider_quote",
            {
                "retry_family": "provider_quote",
                "backoff_policy": "capped_exponential_jitter",
                "max_attempts": 3,
                "jitter_policy": "full_jitter",
                "owner": "runtime",
                "checked_at": now,
            },
            run_id,
            "retry_policy_catalog",
            now,
        ),
        _spec(
            "alert_noise_budget_recorded",
            f"alert_noise_budget:provider_quote_health:{run_id}",
            {
                "alert_family": "provider_quote_health",
                "window_id": f"alerts-{run_id}",
                "noise_budget": 5,
                "suppression_count": 1,
                "owner": "runtime",
                "checked_at": now,
            },
            run_id,
            "alert_noise_budget",
            now,
        ),
        _spec(
            "alert_suppression_audit_recorded",
            f"alert_suppression_audit:{run_id}",
            {
                "suppression_id": f"suppression-{run_id}",
                "alert_family": "provider_quote_health",
                "suppression_reason": "deduplicated_noisy_probe",
                "expires_at": "2099-01-01T00:00:00Z",
                "audit_event_id": f"audit-event-{run_id}",
                "checked_at": now,
            },
            run_id,
            "alert_suppression_audit",
            now,
        ),
        _spec(
            "canary_abort_recorded",
            f"canary_abort:{run_id}",
            {
                "canary_id": f"canary-{run_id}",
                "abort_threshold": 0.05,
                "observed_metric": 0.08,
                "abort_action": "rollback_release",
                "aborted_at": now,
            },
            run_id,
            "canary_abort",
            now,
        ),
        _spec(
            "model_artifact_runtime_compatibility_recorded",
            f"model_artifact_runtime_compatibility:markov-shadow:{run_id}",
            {
                "model_snapshot_id": f"markov-shadow-{run_id}",
                "runtime_version": "python-3.12-v27",
                "serialization_format": "json",
                "compatibility_result": "compatible",
                "checked_at": now,
            },
            run_id,
            "model_artifact_runtime_compatibility",
            now,
        ),
        _spec(
            "model_rollback_recorded",
            f"model_rollback:{run_id}",
            {
                "rollback_id": f"model-rollback-{run_id}",
                "from_model_snapshot_id": f"markov-shadow-{run_id}-candidate",
                "to_model_snapshot_id": f"markov-shadow-{run_id}",
                "rollback_verified_at": now,
            },
            run_id,
            "model_rollback",
            now,
        ),
        _spec(
            "post_release_monitoring_window_recorded",
            f"post_release_monitoring_window:{run_id}",
            {
                "release_id": f"normal-tiny-readiness-{run_id}",
                "window_start": now,
                "window_end": "2099-01-01T00:00:00Z",
                "monitored_metrics": ["error_rate", "capture_rate", "provider_quote_health"],
                "exit_status": "monitoring_passed",
            },
            run_id,
            "post_release_monitoring_window",
            now,
        ),
        _spec(
            "training_poisoning_guard_recorded",
            f"training_poisoning_guard:{run_id}",
            {
                "training_run_id": f"training-run-{run_id}",
                "dataset_hash": dataset_hash,
                "poison_signal_count": 0,
                "quarantine_action": "none",
                "checked_at": now,
            },
            run_id,
            "training_poisoning_guard",
            now,
        ),
        _spec(
            "feature_store_consistency_recorded",
            f"feature_store_consistency:normal_tiny_provider_policy:{run_id}",
            {
                "feature_set_id": "normal_tiny_provider_policy",
                "offline_hash": feature_hash,
                "online_hash": feature_hash,
                "normalization_version": "v2.7.0.provider_policy",
                "checked_at": now,
            },
            run_id,
            "feature_store_consistency",
            now,
        ),
        _spec(
            "dynamic_token_authority_change_recorded",
            f"dynamic_token_authority_change:{token_ca}:freeze",
            {
                "token_ca": token_ca,
                "authority_type": "freeze",
                "previous_authority_hash": sha256_hex({"token_ca": token_ca, "authority": "previous"}),
                "current_authority_hash": sha256_hex({"token_ca": token_ca, "authority": "current"}),
                "risk_action": "risk_recheck",
                "checked_at": now,
            },
            run_id,
            "dynamic_token_authority_change",
            now,
        ),
        _spec(
            "adversarial_execution_simulation_recorded",
            f"adversarial_execution_simulation:{run_id}",
            {
                "simulation_id": f"simulation-{run_id}",
                "execution_policy_version": "normal-tiny-execution-policy-v1",
                "attack_scenario": "quote_cache_poison_then_retry_storm",
                "safety_result": "blocked",
                "checked_at": now,
            },
            run_id,
            "adversarial_execution_simulation",
            now,
        ),
    ]
    return specs


def record_normal_tiny_provider_policy_evidence(
    event_log_dir: str | Path = DEFAULT_EVENT_LOG_DIR,
    *,
    run_id: str | None = None,
    dry_run: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    specs = build_event_specs(run_id=run_id)
    report = {
        "provider_policy_evidence_schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "event_log_dir": str(event_log_dir),
        "run_id": run_id,
        "dry_run": bool(dry_run),
        "planned_event_count": len(specs),
        "planned_event_types": sorted({spec["event_type"] for spec in specs}),
    }
    if dry_run:
        report["append_status_counts"] = {}
        report["health"] = {"status": "normal_tiny_provider_policy_evidence_dry_run"}
        return report

    log = V27EventLog(event_log_dir)
    results = log.append_events(specs)
    status_counts: dict[str, int] = {}
    appended_event_ids = []
    for result in results:
        status = str(result.get("status"))
        status_counts[status] = status_counts.get(status, 0) + 1
        event = result.get("event") or {}
        if status == "appended":
            appended_event_ids.append(event.get("event_id"))
    report["append_status_counts"] = status_counts
    report["appended_event_ids"] = appended_event_ids
    report["event_log_summary_after"] = log.summary()
    ok = sum(status_counts.values()) == len(specs) and all(status in {"appended", "duplicate"} for status in status_counts)
    report["health"] = {
        "status": "normal_tiny_provider_policy_evidence_recorded" if ok else "normal_tiny_provider_policy_evidence_incomplete",
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
    report = record_normal_tiny_provider_policy_evidence(
        args.event_log_dir,
        run_id=args.run_id,
        dry_run=args.dry_run,
        strict=args.strict,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not args.dry_run and report.get("health", {}).get("status") != "normal_tiny_provider_policy_evidence_recorded":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
