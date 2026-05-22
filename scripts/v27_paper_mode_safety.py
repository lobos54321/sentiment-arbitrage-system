#!/usr/bin/env python3
"""Paper/live boundary evidence for v2.7 mode readiness."""

import json
import os
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

RUNTIME_EVIDENCE_SCHEMA_VERSION = "v2.7.0.paper_mode_safety_runtime.v1"
BOUNDARY_SCHEMA_VERSION = "v2.7.0.paper_mode_safety_boundary.v1"
DEFAULT_RUNTIME_EVIDENCE_PATH = PROJECT_ROOT / "data" / "v27_read_models" / "paper_mode_safety.json"

TRUE_VALUES = {"1", "true", "yes", "on"}

LIVE_SECRET_NAMES = (
    "TRADE_WALLET_PRIVATE_KEY",
    "LIVE_PRIVATE_KEY",
    "WALLET_PRIVATE_KEY",
    "SOLANA_PRIVATE_KEY",
    "BSC_PRIVATE_KEY",
)

LIVE_SWITCH_NAMES = (
    "PREMIUM_LIVE_EXECUTION_ENABLED",
    "LIVE_SWAP_ENDPOINT_ENABLED",
    "REAL_ORDER_ROUTER_ENABLED",
    "NETWORK_TRANSACTION_SIGNING_ENABLED",
)

RUNTIME_LIVE_COMPONENT_FIELDS = (
    "jupiter_executor_initialized",
    "live_execution_executor_initialized",
    "live_position_monitor_initialized",
    "real_order_router_enabled",
    "live_swap_endpoint_enabled",
    "network_transaction_signing_enabled",
)


def utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def bool_flag(value, default=False):
    if value is None or value == "":
        return default
    return str(value).strip().lower() in TRUE_VALUES


def resolve_runtime_evidence_path(env=None, path=None):
    env = env or os.environ
    raw = path or env.get("V27_PAPER_MODE_SAFETY_PATH")
    if not raw:
        read_model_dir = env.get("V27_READ_MODEL_DIR")
        raw = str(Path(read_model_dir) / "paper_mode_safety.json") if read_model_dir else str(DEFAULT_RUNTIME_EVIDENCE_PATH)
    evidence_path = Path(raw)
    if not evidence_path.is_absolute():
        evidence_path = PROJECT_ROOT / evidence_path
    return evidence_path


def env_boundary_evidence(env=None):
    env = env or os.environ
    present_live_secret_names = [name for name in LIVE_SECRET_NAMES if env.get(name)]
    switches = {name: bool_flag(env.get(name), False) for name in LIVE_SWITCH_NAMES}
    violations = []
    if switches["PREMIUM_LIVE_EXECUTION_ENABLED"]:
        violations.append("premium_live_execution_enabled")
    if present_live_secret_names:
        violations.append("live_private_key_present")
    if switches["LIVE_SWAP_ENDPOINT_ENABLED"]:
        violations.append("live_swap_endpoint_enabled")
    if switches["REAL_ORDER_ROUTER_ENABLED"]:
        violations.append("real_order_router_enabled")
    if switches["NETWORK_TRANSACTION_SIGNING_ENABLED"]:
        violations.append("network_transaction_signing_enabled")
    return {
        "paper_mode_required": True,
        "premium_live_execution_enabled": switches["PREMIUM_LIVE_EXECUTION_ENABLED"],
        "live_private_key_present": bool(present_live_secret_names),
        "present_live_secret_names": present_live_secret_names,
        "live_swap_endpoint_enabled": switches["LIVE_SWAP_ENDPOINT_ENABLED"],
        "real_order_router_enabled": switches["REAL_ORDER_ROUTER_ENABLED"],
        "network_transaction_signing_enabled": switches["NETWORK_TRANSACTION_SIGNING_ENABLED"],
        "checked_live_switch_names": list(LIVE_SWITCH_NAMES),
        "checked_live_secret_names": list(LIVE_SECRET_NAMES),
        "violations": violations,
    }


def read_runtime_evidence(path):
    path = Path(path)
    if not path.exists():
        return {
            "runtime_evidence_present": False,
            "runtime_evidence_path": str(path),
            "runtime_evidence_valid": False,
            "runtime_evidence_error": "runtime_evidence_missing",
            "runtime_violations": [],
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "runtime_evidence_present": True,
            "runtime_evidence_path": str(path),
            "runtime_evidence_valid": False,
            "runtime_evidence_error": str(exc),
            "runtime_violations": ["runtime_evidence_parse_failed"],
        }

    runtime_violations = []
    schema_ok = payload.get("runtime_evidence_schema_version") == RUNTIME_EVIDENCE_SCHEMA_VERSION
    if not schema_ok:
        runtime_violations.append("runtime_evidence_schema_mismatch")
    if payload.get("premium_live_execution_enabled"):
        runtime_violations.append("runtime_premium_live_execution_enabled")
    if payload.get("paper_only_mode") is False:
        runtime_violations.append("runtime_paper_only_mode_false")
    if payload.get("live_private_key_present"):
        runtime_violations.append("runtime_live_private_key_present")
    for field in RUNTIME_LIVE_COMPONENT_FIELDS:
        if payload.get(field):
            runtime_violations.append(f"runtime_{field}")
    return {
        "runtime_evidence_present": True,
        "runtime_evidence_path": str(path),
        "runtime_evidence_valid": schema_ok,
        "runtime_evidence_error": None if schema_ok else "runtime_evidence_schema_mismatch",
        "runtime_violations": runtime_violations,
        "runtime_evidence": {
            key: value
            for key, value in payload.items()
            if key not in {"environment", "secret_values"}
        },
    }


def build_paper_mode_safety_boundary(env=None, runtime_evidence_path=None):
    env = env or os.environ
    env_evidence = env_boundary_evidence(env)
    path = resolve_runtime_evidence_path(env=env, path=runtime_evidence_path)
    runtime = read_runtime_evidence(path)
    runtime_required = bool_flag(env.get("V27_PAPER_MODE_RUNTIME_EVIDENCE_REQUIRED"), False)

    violations = list(env_evidence["violations"]) + list(runtime.get("runtime_violations") or [])
    if runtime_required and not runtime.get("runtime_evidence_present"):
        violations.append("runtime_evidence_required_missing")

    passed = not violations
    blocking_reason = None
    if not passed:
        blocking_reason = (
            "paper_mode_runtime_evidence_missing"
            if violations == ["runtime_evidence_required_missing"]
            else "paper_live_capability_detected"
        )

    evidence = {
        "boundary_schema_version": BOUNDARY_SCHEMA_VERSION,
        "checked_at": utc_now_iso(),
        **env_evidence,
        "runtime_evidence_required": runtime_required,
        **runtime,
        "violations": violations,
    }
    return passed, blocking_reason, evidence
