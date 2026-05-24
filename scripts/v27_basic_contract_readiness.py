#!/usr/bin/env python3
"""Verify v2.7 observe-only foundation contracts from local machine evidence."""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_mirror_telegram_signals import _signal_payload  # noqa: E402
from v27_paper_mode_safety import build_paper_mode_safety_boundary  # noqa: E402
from v27_spec_validate import CATALOG_PATH, ENTRY_MODE_REGISTRY_PATH, MANIFEST_PATH, validate_all  # noqa: E402


DEFAULT_CHAIN_CONFIG = PROJECT_ROOT / "config" / "v27-chain-config.json"
DEFAULT_SOURCE_REGISTRY = PROJECT_ROOT / "config" / "v27-source-registry.json"
DEFAULT_CHANNELS_CSV = PROJECT_ROOT / "config" / "channels.csv"
DEFAULT_SYSTEM_CONFIG = PROJECT_ROOT / "config" / "system.config.json"
DEFAULT_ENTRY_MODE_REGISTRY = PROJECT_ROOT / "config" / "entry-mode-registry.json"


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_json(path):
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _contract(contract_id, passed, reason, evidence):
    return {
        "contract_id": contract_id,
        "status": "pass" if passed else "missing_evidence",
        "blocking_reason": None if passed else reason,
        "evidence": evidence,
    }


def _bool_env(env, name, default):
    value = (env or {}).get(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _int_env(env, name, default):
    try:
        return int((env or {}).get(name, default))
    except (TypeError, ValueError):
        return default


def _float_env(env, name, default):
    try:
        return float((env or {}).get(name, default))
    except (TypeError, ValueError):
        return default


def verify_spec_consistency(manifest_path=MANIFEST_PATH, catalog_path=CATALOG_PATH, registry_path=ENTRY_MODE_REGISTRY_PATH):
    try:
        report = validate_all(manifest_path, catalog_path, registry_path)
        catalog = _load_json(catalog_path)
        contract_ids = set((catalog.get("contracts") or {}).keys())
        manifest = _load_json(manifest_path)
        required = set(manifest.get("mvp_blocking_contracts") or []) | set(manifest.get("high_risk_carry_forward_contracts") or [])
        duplicate_sections = len(manifest.get("sections") or []) != len({section.get("section_id") for section in manifest.get("sections") or []})
        missing_required = sorted(required - contract_ids)
        passed = not duplicate_sections and not missing_required
        return _contract(
            "SpecConsistencyLinterContract",
            passed,
            "spec_consistency_linter_failed",
            {
                "spec_hash": report.get("spec_hash"),
                "duplicate_sections": duplicate_sections,
                "missing_required_contracts": missing_required,
                "catalog_contract_count": len(contract_ids),
            },
        )
    except Exception as exc:
        return _contract("SpecConsistencyLinterContract", False, "spec_consistency_linter_exception", {"error": str(exc)})


def verify_paper_mode_safety(env=None, runtime_evidence_path=None):
    passed, reason, evidence = build_paper_mode_safety_boundary(
        env=env or os.environ,
        runtime_evidence_path=runtime_evidence_path,
    )
    return _contract(
        "PaperModeSafetyBoundary",
        passed,
        reason or "paper_mode_safety_unverified",
        evidence,
    )


def verify_chain_config(chain_config_path=DEFAULT_CHAIN_CONFIG, system_config_path=DEFAULT_SYSTEM_CONFIG):
    try:
        chain_config = _load_json(chain_config_path)
        system_config = _load_json(system_config_path)
    except Exception as exc:
        return _contract("ChainConfigContract", False, "chain_config_missing_or_invalid", {"error": str(exc)})
    required = {"chain", "native_unit", "quote_mint", "finality_rule", "address_validator"}
    chains = chain_config.get("chains") or {}
    supported = [str(chain).lower() for chain in system_config.get("supported_chains") or []]
    aliases = {"sol": "solana", "bsc": "bsc", "bnb": "bsc"}
    missing = []
    invalid = {}
    for chain in supported:
        normalized = aliases.get(chain, chain)
        record = chains.get(normalized)
        if not isinstance(record, dict):
            missing.append(normalized)
            continue
        missing_fields = sorted(required - set(record))
        if missing_fields:
            invalid[normalized] = missing_fields
    passed = not missing and not invalid
    return _contract(
        "ChainConfigContract",
        passed,
        "chain_config_incomplete",
        {
            "chain_config_path": str(chain_config_path),
            "supported_chains": supported,
            "missing_chains": sorted(set(missing)),
            "invalid_chains": invalid,
        },
    )


def _csv_channel_names(channels_csv):
    if not Path(channels_csv).exists():
        return []
    with Path(channels_csv).open("r", encoding="utf-8") as fh:
        return [row.get("channel_name") for row in csv.DictReader(fh) if row.get("channel_name")]


def verify_source_registry(source_registry_path=DEFAULT_SOURCE_REGISTRY, channels_csv=DEFAULT_CHANNELS_CSV):
    try:
        registry = _load_json(source_registry_path)
    except Exception as exc:
        return _contract("SourceRegistryContract", False, "source_registry_missing_or_invalid", {"error": str(exc)})
    sources = registry.get("sources") or []
    required = {"telegram_source_id", "telegram_channel_id", "allowed_modes", "source_status"}
    invalid = []
    active = 0
    for idx, source in enumerate(sources):
        missing = sorted(required - set(source))
        allowed_modes = source.get("allowed_modes")
        if missing or not isinstance(allowed_modes, list) or not allowed_modes:
            invalid.append({"index": idx, "missing_fields": missing, "allowed_modes": allowed_modes})
        if str(source.get("source_status") or "").lower() == "active":
            active += 1
    passed = bool(sources) and not invalid and active > 0
    return _contract(
        "SourceRegistryContract",
        passed,
        "source_registry_incomplete",
        {
            "source_registry_path": str(source_registry_path),
            "source_count": len(sources),
            "active_source_count": active,
            "invalid_sources": invalid,
            "channels_csv_count": len(_csv_channel_names(channels_csv)),
        },
    )


def verify_input_sanitization():
    sample = {
        "id": 1,
        "token_ca": "TokenSanitize",
        "symbol": "SAN",
        "created_at": "2026-01-15 00:00:00",
        "parse_status": "parsed",
        "raw_message": "<script>alert('x')</script> CA TokenSanitize",
    }
    payload = _signal_payload(sample)
    legacy = payload.get("legacy_premium_signal") or {}
    raw_leaked = legacy.get("raw_message") == sample["raw_message"]
    passed = bool(payload.get("payload_schema_valid")) and payload.get("raw_message_hash") and not raw_leaked
    return _contract(
        "InputSanitizationContract",
        passed,
        "input_sanitization_unverified",
        {
            "payload_schema_valid": payload.get("payload_schema_valid"),
            "unsafe_pattern_detected": payload.get("unsafe_pattern_detected"),
            "raw_message_hash_present": bool(payload.get("raw_message_hash")),
            "raw_text_fields_redacted": payload.get("raw_text_fields_redacted"),
            "legacy_raw_message_leaked": raw_leaked,
        },
    )


def verify_safe_default(registry_path=DEFAULT_ENTRY_MODE_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("SafeDefaultContract", False, "safe_default_registry_missing_or_invalid", {"error": str(exc)})
    modes = registry.get("modes") if isinstance(registry, dict) else {}
    tiers = registry.get("tiers") if isinstance(registry, dict) else {}
    if not isinstance(modes, dict):
        modes = {}
    if not isinstance(tiers, dict):
        tiers = {}
    hard_shadow_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict)
        and entry.get("paper_enabled") is False
        and str(entry.get("tier") or "") in {"hard_shadow", "shadow_watch_only", "deprecated_shadow"}
    )
    blocked_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict) and entry.get("paper_enabled") is False
    )
    invalid_tier_modes = sorted(
        mode
        for mode, entry in modes.items()
        if isinstance(entry, dict) and str(entry.get("tier") or "") not in tiers
    )
    default_record = {
        "unknown_type": "unregistered_entry_mode_or_unproven_contract",
        "default_action": "fail_closed",
        "allowed_modes": ["observe_only", "shadow"],
        "owning_contract": "SafeDefaultContract",
    }
    passed = bool(modes) and bool(blocked_modes) and bool(hard_shadow_modes) and not invalid_tier_modes
    return _contract(
        "SafeDefaultContract",
        passed,
        "safe_default_fail_closed_unverified",
        {
            **default_record,
            "entry_mode_registry_path": str(registry_path),
            "mode_count": len(modes),
            "blocked_mode_count": len(blocked_modes),
            "hard_shadow_default_mode_count": len(hard_shadow_modes),
            "invalid_tier_modes": invalid_tier_modes,
            "blocked_modes_sample": blocked_modes[:20],
        },
    )


def verify_project_stop_loss(env=None):
    if env is None:
        env = os.environ
    auto_kill_enabled = _bool_env(env, "ENTRY_MODE_QUALITY_AUTO_KILL_SWITCH_ENABLED", True)
    window = max(5, _int_env(env, "ENTRY_MODE_QUALITY_WINDOW", 20))
    shadow_sec = max(60, _int_env(env, "ENTRY_MODE_QUALITY_SHADOW_SEC", 2 * 3600))
    stop_criteria = {
        "negative_ev_min_samples": max(1, _int_env(env, "ENTRY_MODE_QUALITY_NEGATIVE_EV_MIN_SAMPLES", 20)),
        "tail_min_samples": max(1, _int_env(env, "ENTRY_MODE_QUALITY_TAIL_MIN_SAMPLES", 8)),
        "avg_pnl_floor": _float_env(env, "ENTRY_MODE_QUALITY_AVG_PNL_FLOOR", 0.0),
        "p10_pnl_floor": _float_env(env, "ENTRY_MODE_QUALITY_P10_PNL_FLOOR", -0.30),
        "max_loss_floor": _float_env(env, "ENTRY_MODE_QUALITY_MAX_LOSS_FLOOR", -0.80),
    }
    action = {
        "action": "downgrade_to_watch_only",
        "shadow_sec": shadow_sec,
        "stop_automatic_entry": True,
    }
    invalid_criteria = []
    if stop_criteria["negative_ev_min_samples"] <= 0:
        invalid_criteria.append("negative_ev_min_samples")
    if stop_criteria["tail_min_samples"] <= 0:
        invalid_criteria.append("tail_min_samples")
    if stop_criteria["p10_pnl_floor"] >= 0:
        invalid_criteria.append("p10_pnl_floor")
    if stop_criteria["max_loss_floor"] >= 0:
        invalid_criteria.append("max_loss_floor")
    passed = auto_kill_enabled and window > 0 and shadow_sec >= 60 and not invalid_criteria
    return _contract(
        "ProjectStopLossContract",
        passed,
        "project_stop_loss_unverified_or_disabled",
        {
            "scope": "entry_mode",
            "window": {"closed_trade_window": window},
            "stop_criteria": stop_criteria,
            "action": action,
            "auto_kill_switch_enabled": auto_kill_enabled,
            "invalid_criteria": invalid_criteria,
        },
    )


def build_basic_contract_readiness(
    *,
    chain_config_path=DEFAULT_CHAIN_CONFIG,
    source_registry_path=DEFAULT_SOURCE_REGISTRY,
    channels_csv=DEFAULT_CHANNELS_CSV,
    system_config_path=DEFAULT_SYSTEM_CONFIG,
    manifest_path=MANIFEST_PATH,
    catalog_path=CATALOG_PATH,
    registry_path=ENTRY_MODE_REGISTRY_PATH,
    env=None,
):
    contracts = {
        item["contract_id"]: item
        for item in [
            verify_spec_consistency(manifest_path, catalog_path, registry_path),
            verify_paper_mode_safety(env=env),
            verify_chain_config(chain_config_path, system_config_path),
            verify_source_registry(source_registry_path, channels_csv),
            verify_input_sanitization(),
            verify_safe_default(registry_path=DEFAULT_ENTRY_MODE_REGISTRY),
            verify_project_stop_loss(env=env),
        ]
    }
    blocking = [contract_id for contract_id, item in contracts.items() if item.get("status") != "pass"]
    return {
        "basic_readiness_schema_version": "v2.7.0.basic_contract_readiness.v1",
        "generated_at": _utc_now_iso(),
        "contracts": contracts,
        "blocking_contracts": blocking,
        "health": {
            "status": "basic_contract_readiness_ok" if not blocking else "basic_contract_readiness_blocked",
            "observe_only_foundation_ready": not blocking,
            "normal_tiny_ready": False,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain-config", default=str(DEFAULT_CHAIN_CONFIG))
    parser.add_argument("--source-registry", default=str(DEFAULT_SOURCE_REGISTRY))
    parser.add_argument("--channels-csv", default=str(DEFAULT_CHANNELS_CSV))
    parser.add_argument("--system-config", default=str(DEFAULT_SYSTEM_CONFIG))
    parser.add_argument("--manifest", default=str(MANIFEST_PATH))
    parser.add_argument("--catalog", default=str(CATALOG_PATH))
    parser.add_argument("--entry-mode-registry", default=str(ENTRY_MODE_REGISTRY_PATH))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    report = build_basic_contract_readiness(
        chain_config_path=Path(args.chain_config),
        source_registry_path=Path(args.source_registry),
        channels_csv=Path(args.channels_csv),
        system_config_path=Path(args.system_config),
        manifest_path=Path(args.manifest),
        catalog_path=Path(args.catalog),
        registry_path=Path(args.entry_mode_registry),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and report["blocking_contracts"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
