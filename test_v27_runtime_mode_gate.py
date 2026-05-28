import json
import sys

sys.path.insert(0, "scripts")

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
