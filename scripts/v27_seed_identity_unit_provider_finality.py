#!/usr/bin/env python3
"""Seed v2.7 identity, unit, chain-finality, and provider-schema policy."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = ROOT / "scripts"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_basic_contract_readiness import (  # noqa: E402
    CATALOG_PATH,
    ENTRY_MODE_REGISTRY_PATH,
    MANIFEST_PATH,
    _file_hash_record,
    _hash_record_without,
    _runtime_config_component_hashes,
    _sha256_json,
    validate_all,
)


NEW_CONTRACTS = {
    "TokenIdentityContract": (
        "observe_only_blocking",
        "S05",
        ["chain", "token_ca", "normalized_ca", "checksum", "identity_confidence"],
        "shadow_only",
    ),
    "DataUnitContract": (
        "observe_only_blocking",
        "S05",
        ["token_decimals", "quote_decimals", "price_unit", "normalized_price", "unit_validation_status"],
        "shadow_only",
    ),
    "ChainFinalityContract": (
        "observe_only_blocking",
        "S08",
        ["chain", "slot", "commitment_level", "rpc_consistency_check", "chain_reorg_detected"],
        "shadow_only",
    ),
    "ProviderSchemaContract": (
        "observe_only_blocking",
        "S08",
        ["provider_name", "schema_version", "required_fields", "canary_parse_result", "schema_drift_detected"],
        "provider_degraded",
    ),
}


METRIC_SPECS = [
    (
        "token_identity_confidence_min",
        "token_identity_confidence_min",
        "minimum token identity confidence across readiness evidence",
        "minimum identity_confidence for valid token identity rows",
        "token identity readiness rows",
        "S05",
    ),
    (
        "data_unit_validation_rate",
        "data_unit_validation_rate",
        "valid data unit rows / data unit rows",
        "count rows with unit_validation_status valid",
        "count data unit rows",
        "S05",
    ),
    (
        "chain_finality_health_rate",
        "chain_finality_health_rate",
        "healthy finality rows / finality rows",
        "count rows with finalized commitment, RPC consistency pass, low lag, and no reorg",
        "count chain finality rows",
        "S08",
    ),
    (
        "provider_schema_canary_pass_rate",
        "provider_schema_canary_pass_rate",
        "provider schema canary pass rows / provider schema rows",
        "count provider schema rows with canary pass and no schema drift",
        "count provider schema rows",
        "S08",
    ),
]


THRESHOLD_SPECS = [
    ("thr_token_identity_confidence_min", "token_identity_confidence_min", "token_identity_confidence_min", 0.99),
    ("thr_data_unit_validation_rate_min", "data_unit_validation_rate_min", "data_unit_validation_rate", 1.0),
    ("thr_chain_finality_health_rate_min", "chain_finality_health_rate_min", "chain_finality_health_rate", 1.0),
    ("thr_provider_schema_canary_pass_rate_min", "provider_schema_canary_pass_rate_min", "provider_schema_canary_pass_rate", 1.0),
]


ERROR_CODES = [
    "identity_unit_provider_finality_policy_missing_or_invalid",
    "identity_unit_provider_finality_policy_not_object",
    "token_identity_missing_malformed_or_low_confidence",
    "data_unit_missing_malformed_or_invalid",
    "chain_finality_missing_malformed_or_dirty",
    "provider_schema_missing_malformed_or_drifted",
]


def load_json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def write_json(path: str, payload: dict) -> None:
    (ROOT / path).write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_policy() -> dict:
    identity = {
        "identity_id": "identity:solana:so11111111111111111111111111111111111111112",
        "chain": "solana",
        "token_ca": "So11111111111111111111111111111111111111112",
        "normalized_ca": "So11111111111111111111111111111111111111112",
        "checksum": "sha256:4f8c4c6f1fb6c3fe6d309f053f26c4d6bc9560cfa68af1e0fc1e9a72f5366f5b",
        "symbol": "SOL",
        "symbol_conflict_count": 0,
        "pool_address": "pool:raydium:sol-usdc-seed",
        "pool_authority": "raydium_cpmm",
        "quote_mint": "So11111111111111111111111111111111111111112",
        "liquidity_pair_valid": True,
        "identity_confidence": 1.0,
        "metric_id": "token_identity_confidence_min",
        "threshold_id": "thr_token_identity_confidence_min",
        "observed_value": 1.0,
    }
    identity["identity_hash"] = _hash_record_without(identity, "identity_hash")

    data_unit = {
        "unit_id": "unit:solana:sol-usdc-seed",
        "token_decimals": 9,
        "quote_mint": "So11111111111111111111111111111111111111112",
        "quote_decimals": 9,
        "price_unit": "quote_per_token",
        "liquidity_unit": "quote_mint_base_units",
        "market_cap_unit": "usd",
        "quote_size_sol": 0.05,
        "normalized_price": 0.000001,
        "unit_validation_status": "valid",
        "unit_conversion_version": "v2.7.0.unit.seed",
        "metric_id": "data_unit_validation_rate",
        "threshold_id": "thr_data_unit_validation_rate_min",
        "observed_value": 1.0,
    }
    data_unit["unit_hash"] = _hash_record_without(data_unit, "unit_hash")

    finality = {
        "finality_id": "finality:solana:seed-finalized",
        "chain": "solana",
        "slot": 1,
        "block_time": "2026-05-27T03:00:00Z",
        "commitment_level": "finalized",
        "finalized_at": "2026-05-27T03:00:01Z",
        "rpc_provider": "helius",
        "rpc_consistency_check": "pass",
        "indexer_lag_sec": 1,
        "chain_reorg_detected": False,
        "metric_id": "chain_finality_health_rate",
        "threshold_id": "thr_chain_finality_health_rate_min",
        "observed_value": 1.0,
    }
    finality["finality_hash"] = _hash_record_without(finality, "finality_hash")

    provider_schema = {
        "provider_name": "gmgn",
        "schema_version": "v2.7.0.gmgn.seed",
        "required_fields": ["token_ca", "price", "liquidity", "updated_at"],
        "optional_fields": ["holders", "top10", "bundler", "toxic"],
        "field_type_contract": {
            "token_ca": "string",
            "price": "decimal_string",
            "liquidity": "decimal_string",
            "updated_at": "iso_timestamp",
        },
        "canary_parse_result": "pass",
        "schema_drift_detected": False,
        "last_schema_check_at": "2026-05-27T03:00:00Z",
        "missing_required_field_rate": 0.0,
        "field_type_error_rate": 0.0,
        "unexpected_enum_rate": 0.0,
        "null_spike_rate": 0.0,
        "value_range_anomaly": False,
        "metric_id": "provider_schema_canary_pass_rate",
        "threshold_id": "thr_provider_schema_canary_pass_rate_min",
        "observed_value": 1.0,
    }
    provider_schema["schema_hash"] = _hash_record_without(provider_schema, "schema_hash")

    return {
        "schema_version": "v2.7.0.identity_unit_provider_finality_policy.v1",
        "scope": "token_identity_data_unit_chain_finality_provider_schema_truth",
        "failure_action": "identity_unit_provider_finality_blocked",
        "min_commitment_level": "finalized",
        "max_indexer_lag_sec": 5,
        "token_identities": [identity],
        "data_units": [data_unit],
        "chain_finality": [finality],
        "provider_schemas": [provider_schema],
        "source_files": [
            {
                "source_file": "scripts/v27_basic_contract_readiness.py",
                "source_anchor": "def verify_identity_unit_provider_finality_contracts",
                "required_patterns": list(NEW_CONTRACTS),
            },
            {
                "source_file": "scripts/v27_mode_readiness.py",
                "source_anchor": "\"TokenIdentityContract\"",
                "required_patterns": [
                    "\"DataUnitContract\"",
                    "\"ChainFinalityContract\"",
                    "\"ProviderSchemaContract\"",
                ],
            },
        ],
    }


def upsert_metrics() -> None:
    registry = load_json("config/v27-metric-definition-registry.json")
    metrics = registry.setdefault("metrics", [])
    by_id = {item.get("metric_id"): item for item in metrics if isinstance(item, dict)}
    for metric_id, name, formula, numerator, denominator, section in METRIC_SPECS:
        record = {
            "metric_id": metric_id,
            "metric_name": name,
            "formula": formula,
            "numerator_definition": numerator,
            "denominator_definition": denominator,
            "window_id": "readiness_seed_window",
            "event_time_basis": "decision_available_at",
            "inclusion_criteria": ["v27_identity_unit_provider_finality_readiness_seed"],
            "exclusion_criteria": ["identity_low_confidence", "unit_dirty", "provider_schema_drift", "chain_finality_dirty"],
            "late_event_policy": "append_only_recompute_next_metric_version",
            "partial_window_policy": "mark_incomplete_no_promotion",
            "unit": "ratio",
            "owner": "system",
            "spec_section_id": section,
            "metric_version": "v2.7.0.seed",
        }
        record["metric_hash"] = _hash_record_without(record, "metric_hash")
        if metric_id in by_id:
            by_id[metric_id].update(record)
        else:
            metrics.append(record)
    registry["metrics"] = sorted(metrics, key=lambda item: item.get("metric_id", ""))
    write_json("config/v27-metric-definition-registry.json", registry)


def upsert_thresholds() -> None:
    catalog = load_json("config/v27-threshold-catalog.json")
    thresholds = catalog.setdefault("thresholds", [])
    by_id = {item.get("threshold_id"): item for item in thresholds if isinstance(item, dict)}
    for threshold_id, name, metric_id, value in THRESHOLD_SPECS:
        record = {
            "threshold_id": threshold_id,
            "threshold_name": name,
            "threshold_value": value,
            "unit": "ratio",
            "comparison_operator": ">=",
            "scope": "identity_unit_provider_finality_readiness",
            "applies_to_metric": metric_id,
            "applies_to_mode": "observe_only",
            "owner": "system",
            "source_spec_section_id": "S05",
            "policy_bundle_id": "v2.7.0_seed_policy_bundle",
            "effective_from": "2026-05-27T00:00:00Z",
            "effective_to": "open",
            "change_reason": "v2.6.13 token identity, data unit, provider schema, and chain finality truth closure",
            "approval_id": "canonical_spec_seed",
        }
        record["threshold_hash"] = _hash_record_without(record, "threshold_hash")
        if threshold_id in by_id:
            by_id[threshold_id].update(record)
        else:
            thresholds.append(record)
    catalog["thresholds"] = sorted(thresholds, key=lambda item: item.get("threshold_id", ""))
    write_json("config/v27-threshold-catalog.json", catalog)


def upsert_contract_catalog() -> None:
    catalog = load_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json")
    contracts = catalog.setdefault("contracts", {})
    for contract_id, (mode_target, section_id, required_fields, failure_action) in NEW_CONTRACTS.items():
        contracts[contract_id] = {
            "failure_action": failure_action,
            "mode_target": mode_target,
            "required_fields": required_fields,
            "section_id": section_id,
        }
    catalog["contracts"] = dict(sorted(contracts.items()))
    write_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json", catalog)


def upsert_gap_register() -> None:
    register = load_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json")
    batches = [
        batch
        for batch in register.setdefault("batches", [])
        if not isinstance(batch, dict) or batch.get("batch_id") != "v2.6.13_identity_unit_provider_finality"
    ]
    batches.append(
        {
            "batch_id": "v2.6.13_identity_unit_provider_finality",
            "contract_ids": list(NEW_CONTRACTS),
            "theme": "Token identity, data unit, chain finality, and provider schema truth must be machine-checkable before denominator, label, or training evidence can be trusted.",
        }
    )
    register["batches"] = sorted(batches, key=lambda item: str(item.get("batch_id", "")) if isinstance(item, dict) else "")
    write_json("spec/telegram_dog_regime_capture/v2.7.0/gap-register.json", register)


def upsert_error_taxonomy() -> None:
    taxonomy_file = load_json("config/v27-error-taxonomy.json")
    taxonomy = taxonomy_file.setdefault("taxonomy", [])
    existing = {item.get("error_code"): item for item in taxonomy if isinstance(item, dict)}
    for code in ERROR_CODES:
        if code in existing:
            existing[code]["category"] = "source_integrity"
            existing[code]["severity"] = "critical"
            existing[code]["operator_action"] = "repair identity/unit/provider/finality policy and rerun v27 readiness"
            continue
        taxonomy.append(
            {
                "error_code": code,
                "category": "source_integrity",
                "severity": "critical",
                "operator_action": "repair identity/unit/provider/finality policy and rerun v27 readiness",
                "introduced_at": "2026-05-27T00:00:00Z",
            }
        )
    taxonomy_file["taxonomy"] = sorted(taxonomy, key=lambda item: item.get("error_code", ""))
    write_json("config/v27-error-taxonomy.json", taxonomy_file)


def refresh_runtime_config_hashes() -> None:
    runtime = load_json("config/v27-runtime-config-drift-policy.json")
    for profile in runtime.get("profiles", []):
        if not isinstance(profile, dict):
            continue
        files = profile.setdefault("policy_bundle_files", [])
        if "config/v27-identity-unit-provider-finality-policy.json" not in files:
            files.append("config/v27-identity-unit-provider-finality-policy.json")
        computed = _runtime_config_component_hashes(profile, {})
        for key, value in computed["component_hashes"].items():
            profile[key] = value
        profile["runtime_config_hash"] = computed["runtime_config_hash"]
        profile["expected_hash"] = computed["expected_hash"]
    write_json("config/v27-runtime-config-drift-policy.json", runtime)


def refresh_ci_policy_hashes(spec_report: dict) -> None:
    ci = load_json("config/v27-ci-spec-generated-policy.json")
    required_check = "python3 -m json.tool config/v27-identity-unit-provider-finality-policy.json"
    for gate in ci.get("ci_merge_gates", []):
        if not isinstance(gate, dict):
            continue
        checks = gate.get("required_checks") if isinstance(gate.get("required_checks"), list) else []
        if required_check not in checks:
            anchor = "python3 -m json.tool config/v27-spec-governance-feasibility-policy.json"
            if anchor in checks:
                checks.insert(checks.index(anchor) + 1, required_check)
            else:
                checks.append(required_check)
        workflow_hash, workflow_error = _file_hash_record(gate.get("workflow_file"))
        if workflow_error:
            raise SystemExit(workflow_error)
        gate["required_checks"] = checks
        gate["workflow_sha256"] = workflow_hash
        gate["spec_hash"] = spec_report["spec_hash"]
        gate["artifact_hash"] = _sha256_json(
            {
                "workflow_file": gate.get("workflow_file"),
                "workflow_sha256": workflow_hash,
                "required_checks": checks,
                "spec_hash": spec_report["spec_hash"],
            }
        )

    catalog = load_json("spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json")
    source_schema_hash = _sha256_json(catalog)
    for client in ci.get("generated_clients", []):
        if not isinstance(client, dict):
            continue
        artifact = load_json(client["generated_artifact_file"])
        client["source_schema_hash"] = source_schema_hash
        client["generated_artifact_hash"] = _sha256_json(artifact)

    for impact in ci.get("spec_change_impacts", []):
        if not isinstance(impact, dict):
            continue
        affected = impact.get("affected_contracts") if isinstance(impact.get("affected_contracts"), list) else []
        for contract_id in NEW_CONTRACTS:
            if contract_id not in affected:
                affected.append(contract_id)
        source_files = impact.get("source_files") if isinstance(impact.get("source_files"), list) else []
        for source in (
            "config/v27-identity-unit-provider-finality-policy.json",
            "scripts/v27_seed_identity_unit_provider_finality.py",
        ):
            if source not in source_files:
                source_files.append(source)
        source_hashes = {}
        for source in source_files:
            file_hash, source_error = _file_hash_record(source)
            if source_error:
                raise SystemExit(source_error)
            source_hashes[source] = file_hash
        impact["affected_contracts"] = affected
        impact["source_files"] = source_files
        impact["source_hashes"] = source_hashes
        impact["spec_hash"] = spec_report["spec_hash"]
        impact["impact_hash"] = _sha256_json(
            {
                "spec_change_id": impact.get("spec_change_id"),
                "affected_contracts": [str(item) for item in affected],
                "affected_modes": [str(item) for item in (impact.get("affected_modes") or [])],
                "spec_hash": spec_report["spec_hash"],
                "source_hashes": source_hashes,
            }
        )
    write_json("config/v27-ci-spec-generated-policy.json", ci)


def main() -> int:
    spec_report = validate_all(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH)
    write_json("config/v27-identity-unit-provider-finality-policy.json", build_policy())
    upsert_metrics()
    upsert_thresholds()
    upsert_contract_catalog()
    upsert_gap_register()
    upsert_error_taxonomy()
    refresh_runtime_config_hashes()
    refresh_ci_policy_hashes(spec_report)
    print(json.dumps({"status": "ok", "contracts": list(NEW_CONTRACTS)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
