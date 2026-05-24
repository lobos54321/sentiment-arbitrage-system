#!/usr/bin/env python3
"""Compare the implemented v2.7 mode gate against the final catalog scope."""

MODE_STAGE_ORDER = [
    "mvp_blocking",
    "shadow",
    "ultra_tiny",
    "normal_tiny_blocking",
    "normal_tiny_governance",
    "phase_1_hardening",
]

MODE_STAGE_TARGETS = {
    "mvp_blocking": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
    ],
    "shadow": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
        "shadow_blocking",
    ],
    "ultra_tiny": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
        "shadow_blocking",
        "ultra_tiny_blocking",
    ],
    "normal_tiny_blocking": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
        "shadow_blocking",
        "ultra_tiny_blocking",
        "normal_tiny_blocking",
        "normal_tiny_promotion_blocking",
    ],
    "normal_tiny_governance": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
        "shadow_blocking",
        "ultra_tiny_blocking",
        "normal_tiny_blocking",
        "normal_tiny_promotion_blocking",
        "normal_tiny_governance",
    ],
    "phase_1_hardening": [
        "all_modes",
        "observe_only_blocking",
        "mvp_blocking",
        "shadow_blocking",
        "ultra_tiny_blocking",
        "normal_tiny_blocking",
        "normal_tiny_promotion_blocking",
        "normal_tiny_governance",
        "phase_1_hardening",
    ],
}


def _expanded_requirements(mode, current_mode_requirements, mode_order):
    if mode not in current_mode_requirements:
        raise KeyError(mode)
    result = []
    for item in mode_order:
        result.extend(current_mode_requirements.get(item) or [])
        if item == mode:
            break
    return list(dict.fromkeys(result))


def _catalog_contracts_by_target(catalog):
    contracts = catalog.get("contracts") if isinstance(catalog, dict) else {}
    by_target = {}
    for contract_id, record in contracts.items():
        target = record.get("mode_target") if isinstance(record, dict) else None
        target = target or "unknown"
        by_target.setdefault(target, set()).add(contract_id)
    return by_target


def _catalog_scope(by_target, targets):
    result = set()
    for target in targets:
        result.update(by_target.get(target, set()))
    return result


def _coverage(expected, covered):
    expected = set(expected)
    covered = set(covered)
    missing = sorted(expected - covered)
    extra = sorted(covered - expected)
    pct = 100.0 if not expected else round(((len(expected) - len(missing)) / len(expected)) * 100, 2)
    return {
        "expected_count": len(expected),
        "covered_count": len(expected) - len(missing),
        "coverage_pct": pct,
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing_contracts": missing,
        "extra_contracts": extra,
        "scope_complete": not missing,
    }


def build_mode_gate_scope_audit(catalog, current_mode_requirements, mode_order):
    by_target = _catalog_contracts_by_target(catalog)
    current_gate = {
        mode: set(_expanded_requirements(mode, current_mode_requirements, mode_order))
        for mode in mode_order
    }
    final_scopes = {}
    for stage in MODE_STAGE_ORDER:
        expected = _catalog_scope(by_target, MODE_STAGE_TARGETS[stage])
        current_mode = "normal_tiny" if stage.startswith("normal_tiny") or stage == "phase_1_hardening" else stage
        if current_mode == "mvp_blocking":
            covered = current_gate.get("observe_only", set()) | current_gate.get("shadow", set())
        else:
            covered = current_gate.get(current_mode, set())
        final_scopes[stage] = {
            "stage": stage,
            "catalog_mode_targets": MODE_STAGE_TARGETS[stage],
            **_coverage(expected, covered),
        }

    normal = final_scopes["normal_tiny_blocking"]
    governance = final_scopes["normal_tiny_governance"]
    catalog_by_target = {
        target: {
            "count": len(ids),
            "contracts": sorted(ids),
        }
        for target, ids in sorted(by_target.items())
    }
    current_gate_summary = {
        mode: {
            "required_count": len(ids),
            "contracts": sorted(ids),
        }
        for mode, ids in current_gate.items()
    }
    return {
        "scope_audit_schema_version": "v2.7.0.mode_gate_scope_audit.v1",
        "status": "final_scope_gaps_present" if not normal["scope_complete"] else "final_scope_covered",
        "catalog_contract_count": sum(len(ids) for ids in by_target.values()),
        "catalog_by_mode_target": catalog_by_target,
        "current_gate": current_gate_summary,
        "final_scopes": final_scopes,
        "health": {
            "status": "final_scope_gaps_present" if not normal["scope_complete"] else "final_scope_covered",
            "current_gate_normal_tiny_contract_count": len(current_gate.get("normal_tiny", set())),
            "final_normal_tiny_blocking_contract_count": normal["expected_count"],
            "final_normal_tiny_blocking_missing_count": normal["missing_count"],
            "final_normal_tiny_blocking_scope_complete": normal["scope_complete"],
            "final_normal_tiny_governance_missing_count": governance["missing_count"],
            "final_normal_tiny_governance_scope_complete": governance["scope_complete"],
        },
    }
