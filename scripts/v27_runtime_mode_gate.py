#!/usr/bin/env python3
"""Runtime cap for v2.7 entry modes.

The read-model readiness matrix is the runtime source of truth for whether a
mode tier may open paper entries. Forecasts and research features can inform a
candidate, but they cannot bypass this cap.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

try:
    from entry_mode_registry import normalize_entry_mode
except Exception:  # pragma: no cover - fallback for standalone bootstrap
    def normalize_entry_mode(raw_entry_mode=None, route=None, detail=None):
        raw = str(raw_entry_mode or "").lower()
        if "a_class" in raw or "fastlane" in raw or "fast_lane" in raw:
            return "A_CLASS_FASTLANE"
        if "lotto" in raw:
            return "LOTTO_TINY_SCOUT"
        if "reclaim" in raw or "revival" in raw or "canary" in raw:
            return "RECLAIM_REVIVAL"
        return "ATH_CONTINUATION"


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_READ_MODEL_DIR = PROJECT_ROOT / "data" / "v27_read_models"
DEFAULT_MODE_READINESS_PATH = DEFAULT_READ_MODEL_DIR / "mode_readiness.json"
DEFAULT_CONTROLLER_ACTIONS_PATH = DEFAULT_READ_MODEL_DIR / "strategy_goal_controller_actions.json"
MODE_ORDER = ("observe_only", "shadow", "ultra_tiny", "normal_tiny")
RUNTIME_MODE_GATE_SCHEMA_VERSION = "v2.7.0.runtime_mode_gate.v1"
MODE_READINESS_SCHEMA_VERSION = "v2.7.0.mode_readiness.v1"
CONTROLLER_ACTIONS_SCHEMA_VERSION = "v1.strategy_goal_controller.runtime_overlay"

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}


def _bool_env(name: str, default: bool = True, env: Mapping[str, str] | None = None) -> bool:
    env = env or os.environ
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    value = str(raw).strip().lower()
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    return default


def runtime_mode_gate_enabled(env: Mapping[str, str] | None = None) -> bool:
    return _bool_env("V27_RUNTIME_MODE_GATE_ENABLED", True, env=env)


def strategy_goal_controller_runtime_enabled(env: Mapping[str, str] | None = None) -> bool:
    return _bool_env("STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED", False, env=env)


def strategy_goal_controller_human_approved(env: Mapping[str, str] | None = None) -> bool:
    return _bool_env("STRATEGY_GOAL_CONTROLLER_HUMAN_APPROVED", False, env=env)


def resolve_mode_readiness_path(path: str | os.PathLike[str] | None = None, env: Mapping[str, str] | None = None) -> Path:
    env = env or os.environ
    raw = path or env.get("V27_MODE_READINESS_PATH")
    if not raw:
        read_model_dir = env.get("V27_READ_MODEL_DIR")
        raw = str(Path(read_model_dir) / "mode_readiness.json") if read_model_dir else str(DEFAULT_MODE_READINESS_PATH)
    resolved = Path(raw)
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved


def resolve_controller_actions_path(path: str | os.PathLike[str] | None = None, env: Mapping[str, str] | None = None) -> Path:
    env = env or os.environ
    raw = path or env.get("STRATEGY_GOAL_CONTROLLER_ACTIONS_PATH")
    if not raw:
        read_model_dir = env.get("V27_READ_MODEL_DIR")
        raw = str(Path(read_model_dir) / "strategy_goal_controller_actions.json") if read_model_dir else str(DEFAULT_CONTROLLER_ACTIONS_PATH)
    resolved = Path(raw)
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved


def _mode_rank(mode: str | None) -> int:
    try:
        return MODE_ORDER.index(str(mode or ""))
    except ValueError:
        return -1


def required_runtime_mode_for_entry(
    *,
    entry_mode: str | None = None,
    entry_branch: str | None = None,
    position_size_sol: float | None = None,
    default_required_mode: str = "ultra_tiny",
    env: Mapping[str, str] | None = None,
) -> str:
    """Return the minimum v2.7 mode required before this entry may fill."""
    env = env or os.environ
    override = env.get("V27_RUNTIME_MODE_GATE_MIN_MODE")
    if override:
        return str(override).strip()
    monitor_override = env.get("V27_PAPER_MONITOR_RUNTIME_MODE_GATE_MIN_MODE")
    if monitor_override and entry_branch != "paper_fast_lane":
        return str(monitor_override).strip()
    fast_override = env.get("V27_FAST_LANE_RUNTIME_MODE_GATE_MIN_MODE")
    if fast_override and entry_branch == "paper_fast_lane":
        return str(fast_override).strip()
    try:
        size = float(position_size_sol) if position_size_sol is not None else None
    except (TypeError, ValueError):
        size = None
    mode_text = str(entry_mode or "").lower()
    if size is not None and size > 0.01 and "tiny" not in mode_text:
        return "normal_tiny"
    return default_required_mode


def _read_mode_readiness(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "v27_mode_readiness_missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return None, f"v27_mode_readiness_unreadable:{exc}"
    if not isinstance(payload, dict):
        return None, "v27_mode_readiness_not_object"
    return payload, None


def _read_controller_actions(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "strategy_goal_controller_actions_missing"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return None, f"strategy_goal_controller_actions_unreadable:{exc}"
    if not isinstance(payload, dict):
        return None, "strategy_goal_controller_actions_not_object"
    return payload, None


def _controller_mode_keys(entry_mode: str | None, entry_branch: str | None = None) -> set[str]:
    raw = str(entry_mode or "").strip()
    normalized = normalize_entry_mode(
        raw_entry_mode=raw,
        detail={
            "entry_branch": entry_branch,
        },
    )
    keys = {"ALL", "ALL_LIVE_RISK"}
    if raw:
        keys.add(raw)
        keys.add(raw.lower())
        keys.add(raw.upper())
    if normalized:
        keys.add(str(normalized))
        keys.add(str(normalized).lower())
        keys.add(str(normalized).upper())
    return keys


def _matching_controller_actions(payload: dict[str, Any], *, entry_mode: str | None, entry_branch: str | None) -> list[dict[str, Any]]:
    actions = payload.get("actions") if isinstance(payload.get("actions"), list) else []
    keys = _controller_mode_keys(entry_mode, entry_branch=entry_branch)
    matches = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        mode = str(action.get("mode") or "").strip()
        if mode in keys or mode.lower() in keys or mode.upper() in keys:
            matches.append(action)
    return matches


def _apply_controller_overlay(
    result: dict[str, Any],
    *,
    entry_mode: str | None,
    entry_branch: str | None,
    position_size_sol: float | None,
    required_mode: str,
    controller_actions_path: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    env = env or os.environ
    enabled = strategy_goal_controller_runtime_enabled(env=env)
    path = resolve_controller_actions_path(controller_actions_path, env=env)
    result["strategy_goal_controller_enforcement_enabled"] = enabled
    result["strategy_goal_controller_actions_path"] = str(path)
    if not enabled:
        result["strategy_goal_controller_decision"] = "NOT_ENFORCED"
        return result

    payload, error = _read_controller_actions(path)
    if error:
        result.update(
            {
                "pass": False,
                "decision": "BLOCK",
                "reason": error,
                "strategy_goal_controller_decision": "BLOCK",
                "strategy_goal_controller_error": error,
            }
        )
        return result

    schema = payload.get("schema_version")
    if schema != CONTROLLER_ACTIONS_SCHEMA_VERSION:
        result.update(
            {
                "pass": False,
                "decision": "BLOCK",
                "reason": "strategy_goal_controller_actions_schema_mismatch",
                "strategy_goal_controller_decision": "BLOCK",
                "strategy_goal_controller_schema_version": schema,
            }
        )
        return result

    if payload.get("enforcement_enabled") is False:
        result["strategy_goal_controller_decision"] = "OVERLAY_PRESENT_NOT_ENFORCING"
        result["strategy_goal_controller_schema_version"] = schema
        return result

    matches = _matching_controller_actions(payload, entry_mode=entry_mode, entry_branch=entry_branch)
    result["strategy_goal_controller_schema_version"] = schema
    result["strategy_goal_controller_matching_actions"] = matches
    if not matches:
        result["strategy_goal_controller_decision"] = "ALLOW"
        return result

    try:
        size = float(position_size_sol) if position_size_sol is not None else None
    except (TypeError, ValueError):
        size = None

    for action in matches:
        action_name = str(action.get("action") or "").upper()
        if action_name in {"DISABLE", "BLOCK", "CIRCUIT_BREAK", "SHADOW"}:
            result.update(
                {
                    "pass": False,
                    "decision": "BLOCK",
                    "reason": f"strategy_goal_controller_{action_name.lower()}",
                    "strategy_goal_controller_decision": "BLOCK",
                    "strategy_goal_controller_blocking_action": action,
                }
            )
            return result
        if action_name in {"TINY_CANARY", "TINY_ONLY"}:
            if action.get("requires_human_approval") and not strategy_goal_controller_human_approved(env=env):
                result.update(
                    {
                        "pass": False,
                        "decision": "BLOCK",
                        "reason": "strategy_goal_controller_human_approval_required",
                        "strategy_goal_controller_decision": "BLOCK",
                        "strategy_goal_controller_blocking_action": action,
                    }
                )
                return result
            max_size = action.get("size_sol", action.get("max_size_sol"))
            try:
                max_size = float(max_size) if max_size is not None else None
            except (TypeError, ValueError):
                max_size = None
            if max_size is not None and size is not None and size > max_size:
                result.update(
                    {
                        "pass": False,
                        "decision": "BLOCK",
                        "reason": "strategy_goal_controller_size_cap_exceeded",
                        "strategy_goal_controller_decision": "BLOCK",
                        "strategy_goal_controller_blocking_action": action,
                    }
                )
                return result
        if action_name == "ALLOW_A_CLASS_ONLY" and normalize_entry_mode(entry_mode) != "A_CLASS_FASTLANE":
            result.update(
                {
                    "pass": False,
                    "decision": "BLOCK",
                    "reason": "strategy_goal_controller_allow_a_class_only",
                    "strategy_goal_controller_decision": "BLOCK",
                    "strategy_goal_controller_blocking_action": action,
                }
            )
            return result

    result["strategy_goal_controller_decision"] = "ALLOW"
    return result


def evaluate_runtime_mode_gate(
    *,
    required_mode: str | None = None,
    entry_mode: str | None = None,
    entry_branch: str | None = None,
    position_size_sol: float | None = None,
    mode_readiness_path: str | os.PathLike[str] | None = None,
    controller_actions_path: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    env = env or os.environ
    path = resolve_mode_readiness_path(mode_readiness_path, env=env)
    required = required_mode or required_runtime_mode_for_entry(
        entry_mode=entry_mode,
        entry_branch=entry_branch,
        position_size_sol=position_size_sol,
        env=env,
    )
    if required not in MODE_ORDER:
        return {
            "runtime_mode_gate_schema_version": RUNTIME_MODE_GATE_SCHEMA_VERSION,
            "pass": False,
            "decision": "BLOCK",
            "reason": "v27_runtime_mode_gate_required_mode_invalid",
            "required_mode": required,
            "mode_readiness_path": str(path),
        }
    if not runtime_mode_gate_enabled(env=env):
        return _apply_controller_overlay({
            "runtime_mode_gate_schema_version": RUNTIME_MODE_GATE_SCHEMA_VERSION,
            "pass": True,
            "decision": "ALLOW",
            "reason": "v27_runtime_mode_gate_disabled",
            "required_mode": required,
            "mode_readiness_path": str(path),
        },
            entry_mode=entry_mode,
            entry_branch=entry_branch,
            position_size_sol=position_size_sol,
            required_mode=required,
            controller_actions_path=controller_actions_path,
            env=env,
        )

    readiness, error = _read_mode_readiness(path)
    if error:
        return {
            "runtime_mode_gate_schema_version": RUNTIME_MODE_GATE_SCHEMA_VERSION,
            "pass": False,
            "decision": "BLOCK",
            "reason": error,
            "required_mode": required,
            "mode_readiness_path": str(path),
            "readiness_present": False,
        }

    schema_ok = readiness.get("matrix_schema_version") == MODE_READINESS_SCHEMA_VERSION
    health = readiness.get("health") if isinstance(readiness.get("health"), dict) else {}
    modes = readiness.get("modes") if isinstance(readiness.get("modes"), dict) else {}
    required_status = modes.get(required) if isinstance(modes.get(required), dict) else {}
    highest_allowed_mode = str(readiness.get("highest_allowed_mode") or "")
    mode_allowed = required_status.get("status") == "allowed"
    highest_covers_required = _mode_rank(highest_allowed_mode) >= _mode_rank(required)
    pass_gate = bool(schema_ok and mode_allowed and highest_covers_required)

    reason = "v27_runtime_mode_gate_allowed" if pass_gate else "v27_runtime_mode_not_allowed"
    if not schema_ok:
        reason = "v27_mode_readiness_schema_mismatch"
    elif not highest_allowed_mode:
        reason = "v27_mode_readiness_highest_allowed_missing"
    elif not mode_allowed:
        reason = "v27_runtime_mode_required_status_blocked"
    elif not highest_covers_required:
        reason = "v27_runtime_mode_highest_allowed_below_required"

    result = {
        "runtime_mode_gate_schema_version": RUNTIME_MODE_GATE_SCHEMA_VERSION,
        "pass": pass_gate,
        "decision": "ALLOW" if pass_gate else "BLOCK",
        "reason": reason,
        "required_mode": required,
        "entry_mode": entry_mode,
        "entry_branch": entry_branch,
        "position_size_sol": position_size_sol,
        "mode_readiness_path": str(path),
        "readiness_present": True,
        "matrix_schema_version": readiness.get("matrix_schema_version"),
        "highest_allowed_mode": highest_allowed_mode,
        "required_mode_status": required_status.get("status"),
        "required_mode_blocking_contracts": required_status.get("blocking_contracts") or [],
        "health_status": health.get("status"),
        "normal_tiny_ready": bool(health.get("normal_tiny_ready")),
        "ultra_tiny_ready": bool(health.get("ultra_tiny_ready")),
    }
    if pass_gate:
        return _apply_controller_overlay(
            result,
            entry_mode=entry_mode,
            entry_branch=entry_branch,
            position_size_sol=position_size_sol,
            required_mode=required,
            controller_actions_path=controller_actions_path,
            env=env,
        )
    result["strategy_goal_controller_enforcement_enabled"] = strategy_goal_controller_runtime_enabled(env=env)
    result["strategy_goal_controller_actions_path"] = str(resolve_controller_actions_path(controller_actions_path, env=env))
    result["strategy_goal_controller_decision"] = "SKIPPED_BASE_GATE_BLOCKED"
    return result
