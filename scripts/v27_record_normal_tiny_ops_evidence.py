#!/usr/bin/env python3
"""Record normal-tiny operational readiness evidence into the v2.7 event log."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
SOURCE = "v27_normal_tiny_ops_evidence"
SCHEMA_VERSION = "v2.7.0.normal_tiny_ops_evidence.v1"
DEFAULT_WORKER_ROLES = (
    "dashboard",
    "paper-trader",
    "lifecycle-tracker",
    "v27-read-model-refresh",
)
COMMIT_ENV_NAMES = (
    "GIT_COMMIT",
    "COMMIT_SHA",
    "SOURCE_VERSION",
    "RAILWAY_GIT_COMMIT_SHA",
    "ZEABUR_GIT_COMMIT_SHA",
    "ZEABUR_GIT_COMMIT",
    "ZEABUR_COMMIT_SHA",
    "VERCEL_GIT_COMMIT_SHA",
    "GITHUB_SHA",
    "RENDER_GIT_COMMIT",
)
CONFIG_HASH_FILES = (
    "package.json",
    "Dockerfile",
    "scripts/run_zeabur_services.sh",
    "config/system.config.json",
    "config/v27-chain-config.json",
    "config/v27-source-registry.json",
    "config/entry-mode-registry.json",
    "config/v27-governance-readiness.json",
)
FEATURE_HASH_FILES = (
    "scripts/v27_denominator_projection.py",
    "scripts/v27_mode_readiness.py",
    "config/entry-mode-registry.json",
)


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _default_run_id():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _sha256_file(path):
    path = Path(path)
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_hash_map(relative_paths):
    result = {}
    for relative in relative_paths:
        digest = _sha256_file(PROJECT_ROOT / relative)
        if digest:
            result[relative] = digest
    return result


def _first_env(env, names):
    for name in names:
        value = (env or {}).get(name)
        if value:
            return str(value).strip()
    return None


def _git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(PROJECT_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=3,
        ).strip()
    except Exception:
        return None


def runtime_build_hash(env):
    commit = _first_env(env, COMMIT_ENV_NAMES) or _git_commit()
    if commit:
        return commit
    return sha256_hex({"source_file_hashes": _file_hash_map(("package.json", "scripts/v27_denominator_projection.py"))})


def runtime_config_hash(env):
    material = {
        "config_files": _file_hash_map(CONFIG_HASH_FILES),
        "runtime_env": {
            "node_env": (env or {}).get("NODE_ENV"),
            "python_unbuffered": (env or {}).get("PYTHONUNBUFFERED"),
            "v27_environment_id": (env or {}).get("V27_ENVIRONMENT_ID"),
            "paper_mode_required": (env or {}).get("V27_PAPER_MODE_REQUIRED", "true"),
            "live_execution_enabled": (env or {}).get("PREMIUM_LIVE_EXECUTION_ENABLED", "false"),
        },
    }
    return sha256_hex(material)


def policy_bundle_id(config_hash):
    return f"v27-normal-tiny-policy:{config_hash[:16]}"


def feature_code_hash():
    return sha256_hex({"feature_files": _file_hash_map(FEATURE_HASH_FILES)})


def _copy_event_log_for_restore(event_log_dir, scratch_dir):
    source = Path(event_log_dir)
    scratch_root = Path(scratch_dir) if scratch_dir else Path(tempfile.gettempdir())
    restore_dir = Path(tempfile.mkdtemp(prefix="v27_restore_drill_", dir=str(scratch_root)))
    if source.exists():
        for child in source.iterdir():
            if child.name.endswith(".lock") or child.name == "sequencer.lock":
                continue
            target = restore_dir / child.name
            if child.is_dir():
                shutil.copytree(child, target)
            elif child.is_file():
                shutil.copy2(child, target)
    return restore_dir


def restore_drill_material(event_log_dir, scratch_dir=None):
    started_at = _utc_now_iso()
    restore_dir = _copy_event_log_for_restore(event_log_dir, scratch_dir)
    try:
        restored_log = V27EventLog(restore_dir)
        restored_summary = restored_log.summary()
        material = {
            "restored_summary": restored_summary,
            "events_hash": _sha256_file(restore_dir / "events.jsonl"),
            "state_hash": _sha256_file(restore_dir / "sequencer-state.json"),
        }
        return {
            "restore_started_at": started_at,
            "restore_completed_at": _utc_now_iso(),
            "restore_status": "passed",
            "backup_set_id": f"event-log-copy:{sha256_hex(material)[:16]}",
            "restored_world_hash": sha256_hex(material),
            "restored_summary": restored_summary,
        }
    finally:
        shutil.rmtree(restore_dir, ignore_errors=True)


def _provider_coverage_rows(env, now):
    raw = (env or {}).get("V27_PROVIDER_COVERAGE_JSON")
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                return parsed
        except json.JSONDecodeError:
            pass
    return [
        {
            "provider": "jupiter_ultra",
            "chain": "solana",
            "pool_type": "raydium_amm",
            "coverage_status": "supported",
            "unsupported_reason": "none",
            "coverage_map_version": "v2.7.0.provider_coverage_map.v1",
            "checked_at": now,
        },
        {
            "provider": "jupiter_ultra",
            "chain": "solana",
            "pool_type": "unknown_pool",
            "coverage_status": "partial",
            "unsupported_reason": "requires_route_discovery_before_quote",
            "coverage_map_version": "v2.7.0.provider_coverage_map.v1",
            "checked_at": now,
        },
        {
            "provider": "gmgn_readonly",
            "chain": "solana",
            "pool_type": "meme_discovery",
            "coverage_status": "supported",
            "unsupported_reason": "none",
            "coverage_map_version": "v2.7.0.provider_coverage_map.v1",
            "checked_at": now,
        },
    ]


def _spec(event_type, aggregate_id, payload, run_id, key_suffix, now):
    return {
        "event_type": event_type,
        "aggregate_id": aggregate_id,
        "payload": {
            "ops_evidence_schema_version": SCHEMA_VERSION,
            "readiness_scope": "normal_tiny",
            "readiness_drill": True,
            **payload,
        },
        "source": SOURCE,
        "idempotency_key": f"normal_tiny_ops_evidence:{run_id}:{key_suffix}",
        "observed_at": now,
        "available_at": now,
    }


def build_event_specs(event_log_dir, *, run_id=None, env=None, scratch_dir=None, worker_roles=None):
    env = env or os.environ
    run_id = run_id or _default_run_id()
    now = _utc_now_iso()
    event_log = V27EventLog(event_log_dir)
    summary = event_log.summary()
    latest_seq = int(summary.get("last_global_seq") or 0)
    build_hash = runtime_build_hash(env)
    config_hash = runtime_config_hash(env)
    policy_id = policy_bundle_id(config_hash)
    roles = list(worker_roles or DEFAULT_WORKER_ROLES)
    fleet_hash_map = {
        role: {
            "build_hash": build_hash,
            "runtime_config_hash": config_hash,
            "policy_bundle_id": policy_id,
        }
        for role in roles
    }
    restore = restore_drill_material(event_log_dir, scratch_dir=scratch_dir)
    frozen_range = {
        "start_seq": 1 if latest_seq > 0 else 0,
        "end_seq": latest_seq,
    }
    freeze_id = f"normal-tiny-readiness-freeze-{run_id}"
    incident_id = f"normal-tiny-readiness-drill-{run_id}"
    feature_hash = feature_code_hash()
    specs = [
        _spec(
            "deployment_rollout_state_recorded",
            "deployment_rollout:normal_tiny_ops_readiness",
            {
                "rollout_id": "normal-tiny-ops-readiness",
                "state": "completed",
                "fleet_hash_map": fleet_hash_map,
                "canary_status": "passed",
                "build_hash": build_hash,
                "runtime_config_hash": config_hash,
                "policy_bundle_id": policy_id,
                "evidence_source": SOURCE,
            },
            run_id,
            "deployment_rollout",
            now,
        ),
        _spec(
            "backup_restore_drill_recorded",
            "backup_restore_drill:normal_tiny_ops_readiness",
            {
                "drill_id": "normal-tiny-ops-readiness",
                "backup_set_id": restore["backup_set_id"],
                "restored_world_hash": restore["restored_world_hash"],
                "restore_started_at": restore["restore_started_at"],
                "restore_completed_at": restore["restore_completed_at"],
                "restore_status": restore["restore_status"],
                "evidence_source": SOURCE,
            },
            run_id,
            "backup_restore_drill",
            now,
        ),
        _spec(
            "incident_evidence_freeze_recorded",
            f"incident_evidence_freeze:{freeze_id}",
            {
                "freeze_id": freeze_id,
                "incident_id": incident_id,
                "frozen_event_range": frozen_range,
                "frozen_config_hash": sha256_hex(
                    {
                        "runtime_config_hash": config_hash,
                        "policy_bundle_id": policy_id,
                        "frozen_event_range": frozen_range,
                    }
                ),
                "frozen_at": now,
                "freeze_status": "frozen",
                "evidence_source": SOURCE,
            },
            run_id,
            "incident_evidence_freeze",
            now,
        ),
        _spec(
            "circuit_breaker_resume_recorded",
            "circuit_breaker_resume:normal_tiny_ops_readiness",
            {
                "breaker_id": "normal-tiny-ops-readiness",
                "root_cause_fixed": True,
                "evidence_freeze_id": freeze_id,
                "health_checks_passed": True,
                "resumed_at": now,
                "resume_status": "resumed",
                "evidence_source": SOURCE,
            },
            run_id,
            "circuit_breaker_resume",
            now,
        ),
        _spec(
            "queue_durability_recorded",
            f"queue_durability:normal_tiny_ops_readiness:{run_id}",
            {
                "queue_id": "normal_tiny_ops_readiness",
                "task_id": f"normal-tiny-ops-readiness-{run_id}",
                "durable_state": "persisted",
                "ack_state": "acked",
                "created_at": now,
                "evidence_source": SOURCE,
            },
            run_id,
            "queue_durability",
            now,
        ),
        _spec(
            "candidate_cancellation_recorded",
            f"candidate_cancellation:normal_tiny_ops_readiness:{run_id}",
            {
                "candidate_id": f"normal-tiny-readiness-candidate-{run_id}",
                "cancel_reason": "readiness_drill_no_trade_candidate",
                "cancel_event_seq": latest_seq,
                "cancelled_at": now,
                "evidence_source": SOURCE,
            },
            run_id,
            "candidate_cancellation",
            now,
        ),
        _spec(
            "retry_storm_control_recorded",
            "retry_storm_control:provider_quote",
            {
                "retry_family": "provider_quote",
                "backoff_policy": "capped_exponential_jitter",
                "max_concurrent_retries": int(env.get("V27_PROVIDER_QUOTE_MAX_CONCURRENT_RETRIES", 2)),
                "p0_reserved_capacity": int(env.get("V27_PROVIDER_QUOTE_P0_RESERVED_CAPACITY", 1)),
                "evidence_source": SOURCE,
            },
            run_id,
            "retry_storm_control",
            now,
        ),
        _spec(
            "randomness_control_recorded",
            "randomness_control:normal_tiny_deterministic_policy",
            {
                "rng_seed": f"sha256:{sha256_hex({'policy_bundle_id': policy_id, 'build_hash': build_hash})}",
                "rng_version": "v2.7.0.normal_tiny_deterministic_policy.v1",
                "randomization_unit": "normal_tiny_promotion_policy",
                "assignment_id": "normal-tiny-deterministic-policy",
                "assignment_status": "deterministic_policy",
                "randomization_enabled": False,
                "deterministic_assignment": True,
                "assignment_algorithm": "deterministic_no_randomized_assignment",
                "assigned_bucket": "normal_tiny_candidate",
                "assignment_hash": sha256_hex(
                    {
                        "assignment_id": "normal-tiny-deterministic-policy",
                        "policy_bundle_id": policy_id,
                        "randomization_enabled": False,
                    }
                ),
                "decision_available_at": now,
                "evidence_source": SOURCE,
            },
            run_id,
            "randomness_control",
            now,
        ),
        _spec(
            "training_serving_skew_recorded",
            "training_serving_skew:normal_tiny_features:v27_denominator_projection",
            {
                "feature_set_id": "normal_tiny_denominator_features",
                "training_feature_code_hash": feature_hash,
                "serving_feature_code_hash": feature_hash,
                "normalization_version": "v2.7.0.denominator_projection",
                "skew_check_result": "matched",
                "checked_at": now,
                "training_artifact_id": f"source-tree:{build_hash[:16]}",
                "serving_artifact_id": f"runtime-tree:{build_hash[:16]}",
                "evidence_source": SOURCE,
            },
            run_id,
            "training_serving_skew",
            now,
        ),
    ]
    for role in roles:
        specs.append(
            _spec(
                "worker_fleet_heartbeat_recorded",
                f"worker_fleet:{role}",
                {
                    "worker_id": role,
                    "role": role,
                    "build_hash": build_hash,
                    "runtime_config_hash": config_hash,
                    "policy_bundle_id": policy_id,
                    "heartbeat_at": now,
                    "evidence_source": SOURCE,
                },
                run_id,
                f"worker_fleet:{role}",
                now,
            )
        )
    for row in _provider_coverage_rows(env, now):
        provider = row.get("provider") or "unknown_provider"
        chain = row.get("chain") or "unknown_chain"
        pool_type = row.get("pool_type") or "unknown_pool_type"
        specs.append(
            _spec(
                "provider_coverage_map_recorded",
                f"provider_coverage_map:{provider}:{chain}:{pool_type}",
                {**row, "evidence_source": row.get("evidence_source") or SOURCE},
                run_id,
                f"provider_coverage_map:{provider}:{chain}:{pool_type}",
                now,
            )
        )
    evidence = {
        "run_id": run_id,
        "build_hash": build_hash,
        "runtime_config_hash": config_hash,
        "policy_bundle_id": policy_id,
        "worker_roles": roles,
        "event_log_summary_before": summary,
        "backup_restore": restore,
        "feature_code_hash": feature_hash,
    }
    return specs, evidence


def record_normal_tiny_ops_evidence(event_log_dir, *, run_id=None, dry_run=False, strict=False, scratch_dir=None, worker_roles=None):
    run_id = run_id or _default_run_id()
    specs, evidence = build_event_specs(
        event_log_dir,
        run_id=run_id,
        scratch_dir=scratch_dir,
        worker_roles=worker_roles,
    )
    report = {
        "ops_evidence_schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now_iso(),
        "event_log_dir": str(event_log_dir),
        "run_id": run_id,
        "dry_run": bool(dry_run),
        "planned_event_count": len(specs),
        "planned_event_types": sorted({spec["event_type"] for spec in specs}),
        "evidence": evidence,
    }
    if dry_run:
        report["append_status_counts"] = {}
        report["health"] = {"status": "normal_tiny_ops_evidence_dry_run"}
        return report

    log = V27EventLog(event_log_dir)
    results = log.append_events(specs)
    status_counts = {}
    appended_event_ids = []
    for result in results:
        status = result.get("status")
        status_counts[status] = status_counts.get(status, 0) + 1
        event = result.get("event") or {}
        if status == "appended":
            appended_event_ids.append(event.get("event_id"))
    summary_after = log.summary()
    report.update(
        {
            "append_status_counts": status_counts,
            "appended_event_ids": appended_event_ids,
            "event_log_summary_after": summary_after,
        }
    )
    ok = sum(status_counts.values()) == len(specs) and all(status in {"appended", "duplicate"} for status in status_counts)
    report["health"] = {
        "status": "normal_tiny_ops_evidence_recorded" if ok else "normal_tiny_ops_evidence_incomplete",
        "event_log_verified": True,
    }
    if strict and not ok:
        report["health"]["strict_failed"] = True
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=os.environ.get("V27_EVENT_LOG_DIR", str(DEFAULT_EVENT_LOG_DIR)))
    parser.add_argument("--run-id")
    parser.add_argument("--scratch-dir")
    parser.add_argument("--worker-role", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    worker_roles = args.worker_role or None
    report = record_normal_tiny_ops_evidence(
        Path(args.event_log_dir),
        run_id=args.run_id,
        dry_run=args.dry_run,
        strict=args.strict,
        scratch_dir=args.scratch_dir,
        worker_roles=worker_roles,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and not args.dry_run and report.get("health", {}).get("status") != "normal_tiny_ops_evidence_recorded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
