import json
import sys

sys.path.insert(0, "scripts")

from strategy_goal_controller import write_strategy_goal_runtime_overlay  # noqa: E402
from v27_runtime_mode_gate import evaluate_runtime_mode_gate, required_runtime_mode_for_entry  # noqa: E402


def write_matrix(path, *, highest="ultra_tiny", blocked=()):
    blocked = set(blocked)
    modes = {}
    for mode in ("observe_only", "shadow", "ultra_tiny", "normal_tiny"):
        modes[mode] = {
            "status": "blocked" if mode in blocked else "allowed",
            "blocking_contracts": ["UnitContract"] if mode in blocked else [],
        }
    path.write_text(
        json.dumps(
            {
                "matrix_schema_version": "v2.7.0.mode_readiness.v1",
                "highest_allowed_mode": highest,
                "health": {
                    "status": "mode_readiness_evaluated",
                    "ultra_tiny_ready": highest in ("ultra_tiny", "normal_tiny"),
                    "normal_tiny_ready": highest == "normal_tiny",
                },
                "modes": modes,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_runtime_mode_gate_allows_when_required_mode_is_allowed(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="normal_tiny")

    gate = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="pre_pass_resonance_tiny_probe",
        mode_readiness_path=path,
    )

    assert gate["pass"] is True
    assert gate["reason"] == "v27_runtime_mode_gate_allowed"
    assert gate["highest_allowed_mode"] == "normal_tiny"


def test_runtime_mode_gate_blocks_when_highest_mode_is_too_low(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="shadow")

    gate = evaluate_runtime_mode_gate(required_mode="ultra_tiny", mode_readiness_path=path)

    assert gate["pass"] is False
    assert gate["reason"] == "v27_runtime_mode_highest_allowed_below_required"


def test_runtime_mode_gate_blocks_missing_readiness(tmp_path):
    gate = evaluate_runtime_mode_gate(required_mode="ultra_tiny", mode_readiness_path=tmp_path / "missing.json")

    assert gate["pass"] is False
    assert gate["reason"] == "v27_mode_readiness_missing"


def test_runtime_mode_gate_promotes_non_tiny_large_entries_to_normal_tiny():
    required = required_runtime_mode_for_entry(
        entry_mode="smart_entry_pullback_bounce",
        position_size_sol=0.04,
    )

    assert required == "normal_tiny"


def test_runtime_mode_gate_ignores_controller_overlay_until_enabled(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="normal_tiny")
    overlay = write_strategy_goal_runtime_overlay(
        {"actions": [{"mode": "A_CLASS_FASTLANE", "action": "DISABLE", "reason": "unit"}]},
        path=tmp_path / "controller.json",
        enforcement_enabled=True,
        generated_at=1_700_000_000,
    )

    gate = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="a_class_fastlane",
        mode_readiness_path=path,
        controller_actions_path=overlay["path"],
        env={"STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "false"},
    )

    assert gate["pass"] is True
    assert gate["strategy_goal_controller_decision"] == "NOT_ENFORCED"


def test_runtime_mode_gate_blocks_matching_controller_disable(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="normal_tiny")
    overlay = write_strategy_goal_runtime_overlay(
        {"actions": [{"mode": "A_CLASS_FASTLANE", "action": "DISABLE", "reason": "unit"}]},
        path=tmp_path / "controller.json",
        enforcement_enabled=True,
        generated_at=1_700_000_000,
    )

    gate = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="source_resonance_a_class_fastlane",
        mode_readiness_path=path,
        controller_actions_path=overlay["path"],
        env={"STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true"},
    )

    assert gate["pass"] is False
    assert gate["reason"] == "strategy_goal_controller_disable"
    assert gate["strategy_goal_controller_decision"] == "BLOCK"


def test_runtime_mode_gate_enforces_tiny_canary_size_cap(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="normal_tiny")
    overlay = write_strategy_goal_runtime_overlay(
        {"actions": [{"mode": "A_CLASS_FASTLANE", "action": "TINY_CANARY", "size_sol": 0.001}]},
        path=tmp_path / "controller.json",
        enforcement_enabled=True,
        generated_at=1_700_000_000,
    )

    blocked = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="a_class_fastlane",
        position_size_sol=0.003,
        mode_readiness_path=path,
        controller_actions_path=overlay["path"],
        env={"STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true"},
    )
    allowed = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="a_class_fastlane",
        position_size_sol=0.001,
        mode_readiness_path=path,
        controller_actions_path=overlay["path"],
        env={"STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true"},
    )

    assert blocked["pass"] is False
    assert blocked["reason"] == "strategy_goal_controller_size_cap_exceeded"
    assert allowed["pass"] is True
    assert allowed["strategy_goal_controller_decision"] == "ALLOW"


def test_runtime_mode_gate_fails_closed_when_controller_overlay_missing_and_enabled(tmp_path):
    path = write_matrix(tmp_path / "mode_readiness.json", highest="normal_tiny")

    gate = evaluate_runtime_mode_gate(
        required_mode="ultra_tiny",
        entry_mode="a_class_fastlane",
        mode_readiness_path=path,
        controller_actions_path=tmp_path / "missing-controller.json",
        env={"STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED": "true"},
    )

    assert gate["pass"] is False
    assert gate["reason"] == "strategy_goal_controller_actions_missing"
