#!/usr/bin/env python3
"""Verify v2.7 observe-only foundation contracts from local machine evidence."""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
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
DEFAULT_GOVERNANCE_READINESS = PROJECT_ROOT / "config" / "v27-governance-readiness.json"
DEFAULT_ACCESS_CONTROL_POLICY = PROJECT_ROOT / "config" / "v27-access-control-policy.json"
DEFAULT_WRITE_PATH_REGISTRY = PROJECT_ROOT / "config" / "v27-write-path-registry.json"
ACCESS_CONTROL_REQUIRED_FIELDS = (
    "endpoint",
    "required_role",
    "token_scope",
    "audit_log_required",
    "danger_level",
)
WRITE_PATH_REQUIRED_FIELDS = (
    "write_path_id",
    "module",
    "target_store",
    "requires_outbox",
    "owner",
)
WRITE_PATH_SOURCE_FIELDS = (
    "entry_point",
    "mutation_type",
    "mode_gate",
    "source_file",
    "source_anchor",
)
WRITE_PATH_ALLOWED_MODE_GATES = {
    "observe_only",
    "shadow",
    "ultra_tiny",
    "normal_tiny",
    "admin_break_glass",
    "diagnostics",
}
NORMAL_TINY_BLOCKING_CONTRACTS = {
    "RawProviderEvidenceContract",
    "LabelFinalizationContract",
    "OutcomeWindowCloseContract",
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
}


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


def _missing_required_fields(record, fields):
    missing = []
    for field in fields:
        value = record.get(field) if isinstance(record, dict) else None
        if value is None or value == "" or value == [] or value == {}:
            missing.append(field)
    return missing


def _parse_iso_ts(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_governance_readiness(governance_path):
    try:
        payload = _load_json(governance_path)
    except Exception as exc:
        return None, {"governance_path": str(governance_path), "error": str(exc)}
    if not isinstance(payload, dict):
        return None, {"governance_path": str(governance_path), "error": "governance_readiness_not_object"}
    return payload, {"governance_path": str(governance_path), "schema_version": payload.get("schema_version"), "updated_at": payload.get("updated_at")}


def _resolve_source_file(source_file):
    path = Path(str(source_file or ""))
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _source_lines(source_file):
    source_path = _resolve_source_file(source_file)
    if not source_path.exists():
        return None, {"source_file": str(source_file), "error": "source_file_missing"}
    return source_path.read_text(encoding="utf-8").splitlines(), None


def _extract_dashboard_routes(lines):
    route_line_indexes = []
    endpoints_by_line = {}
    for index, line in enumerate(lines):
        endpoints = re.findall(r"url\.pathname\s*===\s*['\"]([^'\"]+)['\"]", line)
        if not endpoints:
            continue
        route_line_indexes.append(index)
        endpoints_by_line[index] = endpoints

    routes = []
    for route_index, line_index in enumerate(route_line_indexes):
        next_line_index = route_line_indexes[route_index + 1] if route_index + 1 < len(route_line_indexes) else len(lines)
        block = lines[line_index:next_line_index]
        check_auth_line = None
        post_guard_line = None
        mutation_markers = []
        for offset, text in enumerate(block):
            if check_auth_line is None and "checkAuth(req, url, res)" in text:
                check_auth_line = line_index + offset + 1
            if post_guard_line is None and ("req.method !== 'POST'" in text or "requirePost(req, res)" in text):
                post_guard_line = line_index + offset + 1
            if (
                "triggerV27" in text
                or "cleanupOpenPaperPositions(" in text
                or ".run(" in text
                or "manualPause(" in text
                or "resumeTrading(" in text
                or "resetDailyLoss(" in text
            ):
                mutation_markers.append({"line": line_index + offset + 1, "text": text.strip()})
        for endpoint in endpoints_by_line.get(line_index, []):
            routes.append(
                {
                    "endpoint": endpoint,
                    "line": line_index + 1,
                    "has_check_auth": check_auth_line is not None,
                    "check_auth_line": check_auth_line,
                    "has_post_guard": post_guard_line is not None,
                    "post_guard_line": post_guard_line,
                    "mutation_markers": mutation_markers[:5],
                }
            )
    return routes


def _resolve_access_policy(endpoint, defaults, overrides):
    policy = {"endpoint": endpoint, **(defaults or {})}
    policy.update(overrides.get(endpoint) or {})
    policy["endpoint"] = endpoint
    return policy


def _write_registry_post_endpoints(write_path_registry_path):
    try:
        registry = _load_json(write_path_registry_path)
    except Exception:
        return set(), False
    endpoints = set()
    for item in registry.get("write_paths") or []:
        if not isinstance(item, dict):
            continue
        entry_point = str(item.get("entry_point") or "")
        if entry_point.startswith("POST "):
            endpoints.add(entry_point.removeprefix("POST ").strip())
    return endpoints, True


def verify_access_control_policy(policy_path=DEFAULT_ACCESS_CONTROL_POLICY, write_path_registry_path=DEFAULT_WRITE_PATH_REGISTRY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("AccessControlContract", False, "access_control_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("AccessControlContract", False, "access_control_policy_not_object", {"policy_path": str(policy_path)})

    lines, source_error = _source_lines(policy.get("source_file"))
    if source_error:
        return _contract("AccessControlContract", False, "access_control_source_missing", {"policy_path": str(policy_path), **source_error})

    source_text = "\n".join(lines)
    auth_boundary = {
        "dashboard_token_required": "if (!DASHBOARD_TOKEN)" in source_text and "writeHead(403" in source_text,
        "invalid_token_rejected": "token !== DASHBOARD_TOKEN" in source_text and "writeHead(401" in source_text,
        "token_sources": sorted(
            item
            for item, present in {
                "query_token": "url.searchParams.get('token')" in source_text,
                "x_dashboard_token_header": "x-dashboard-token" in source_text,
            }.items()
            if present
        ),
    }
    routes = _extract_dashboard_routes(lines)
    public_endpoints = set(policy.get("public_endpoints") or [])
    defaults = policy.get("protected_defaults") if isinstance(policy.get("protected_defaults"), dict) else {}
    overrides_list = policy.get("endpoint_overrides") if isinstance(policy.get("endpoint_overrides"), list) else []
    overrides = {}
    malformed_policies = []
    duplicate_policy_endpoints = []
    for index, item in enumerate(overrides_list):
        if not isinstance(item, dict):
            malformed_policies.append({"index": index, "endpoint": None, "missing_fields": list(ACCESS_CONTROL_REQUIRED_FIELDS), "violations": ["policy_not_object"]})
            continue
        endpoint = item.get("endpoint")
        if endpoint in overrides:
            duplicate_policy_endpoints.append(endpoint)
        overrides[endpoint] = item

    danger_requires_post = set(policy.get("danger_levels_requiring_post") or [])
    danger_requires_audit = set(policy.get("danger_levels_requiring_audit") or [])
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    literal_endpoints = set(route_by_endpoint)
    protected_routes = [route for route in routes if route["endpoint"] not in public_endpoints]
    unauthenticated_routes = [
        {"endpoint": route["endpoint"], "line": route["line"]}
        for route in protected_routes
        if not route["has_check_auth"]
    ]

    resolved_endpoint_policies = []
    missing_policy_fields = []
    mutation_without_post_guard = []
    mutation_without_audit_requirement = []
    mutation_like_routes_without_mutation_policy = []
    for route in protected_routes:
        endpoint = route["endpoint"]
        resolved = _resolve_access_policy(endpoint, defaults, overrides)
        missing = _missing_required_fields(resolved, ACCESS_CONTROL_REQUIRED_FIELDS)
        violations = []
        if not isinstance(resolved.get("audit_log_required"), bool):
            violations.append("audit_log_required_bool")
        danger = str(resolved.get("danger_level") or "")
        if danger in danger_requires_post and not route["has_post_guard"]:
            mutation_without_post_guard.append({"endpoint": endpoint, "line": route["line"], "danger_level": danger})
        if danger in danger_requires_audit and resolved.get("audit_log_required") is not True:
            mutation_without_audit_requirement.append({"endpoint": endpoint, "danger_level": danger})
        if route["mutation_markers"] and danger not in danger_requires_post:
            mutation_like_routes_without_mutation_policy.append(
                {
                    "endpoint": endpoint,
                    "danger_level": danger,
                    "markers": route["mutation_markers"],
                }
            )
        if missing or violations:
            missing_policy_fields.append({"endpoint": endpoint, "missing_fields": missing, "violations": violations})
        resolved_endpoint_policies.append(
            {
                "endpoint": endpoint,
                "required_role": resolved.get("required_role"),
                "token_scope": resolved.get("token_scope"),
                "audit_log_required": resolved.get("audit_log_required"),
                "danger_level": resolved.get("danger_level"),
            }
        )

    unknown_policy_endpoints = sorted(endpoint for endpoint in overrides if endpoint not in literal_endpoints)
    write_path_endpoints, write_registry_loaded = _write_registry_post_endpoints(write_path_registry_path)
    write_path_policy_gaps = []
    for endpoint in sorted(write_path_endpoints):
        resolved = _resolve_access_policy(endpoint, defaults, overrides)
        route = route_by_endpoint.get(endpoint)
        if (
            endpoint not in literal_endpoints
            or resolved.get("audit_log_required") is not True
            or str(resolved.get("danger_level") or "") not in danger_requires_post
            or not route
            or not route.get("has_post_guard")
            or not route.get("has_check_auth")
        ):
            write_path_policy_gaps.append(
                {
                    "endpoint": endpoint,
                    "registered_route": endpoint in literal_endpoints,
                    "audit_log_required": resolved.get("audit_log_required"),
                    "danger_level": resolved.get("danger_level"),
                    "has_post_guard": route.get("has_post_guard") if route else False,
                    "has_check_auth": route.get("has_check_auth") if route else False,
                }
            )

    dynamic_failures = []
    for index, item in enumerate(policy.get("dynamic_protected_routes") or []):
        if not isinstance(item, dict):
            dynamic_failures.append({"index": index, "endpoint": None, "error": "dynamic_policy_not_object"})
            continue
        missing = _missing_required_fields(item, ACCESS_CONTROL_REQUIRED_FIELDS + ("source_anchor",))
        anchor = str(item.get("source_anchor") or "")
        anchor_indexes = [idx for idx, text in enumerate(lines) if anchor and anchor in text]
        check_auth_near_anchor = any(
            "checkAuth(req, url, res)" in text
            for anchor_index in anchor_indexes
            for text in lines[anchor_index:min(anchor_index + 12, len(lines))]
        )
        if missing or not anchor_indexes or not check_auth_near_anchor:
            dynamic_failures.append(
                {
                    "index": index,
                    "endpoint": item.get("endpoint"),
                    "missing_fields": missing,
                    "anchor_found": bool(anchor_indexes),
                    "check_auth_near_anchor": check_auth_near_anchor,
                }
            )

    passed = (
        policy.get("schema_version") == "v2.7.0.access_control_policy.v1"
        and all(auth_boundary.values())
        and bool(routes)
        and not malformed_policies
        and not duplicate_policy_endpoints
        and not unauthenticated_routes
        and not missing_policy_fields
        and not mutation_without_post_guard
        and not mutation_without_audit_requirement
        and not mutation_like_routes_without_mutation_policy
        and not unknown_policy_endpoints
        and write_registry_loaded
        and not write_path_policy_gaps
        and not dynamic_failures
    )
    return _contract(
        "AccessControlContract",
        passed,
        "access_control_policy_missing_malformed_or_incomplete",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "source_file": policy.get("source_file"),
            "auth_boundary": auth_boundary,
            "literal_route_count": len(routes),
            "public_route_count": len([route for route in routes if route["endpoint"] in public_endpoints]),
            "protected_route_count": len(protected_routes),
            "resolved_policy_count": len(resolved_endpoint_policies),
            "mutation_policy_count": len(
                [item for item in resolved_endpoint_policies if str(item.get("danger_level") or "") in danger_requires_post]
            ),
            "write_path_endpoint_count": len(write_path_endpoints),
            "unauthenticated_routes": unauthenticated_routes,
            "missing_policy_fields": missing_policy_fields,
            "malformed_policies": malformed_policies,
            "duplicate_policy_endpoints": sorted(str(item) for item in duplicate_policy_endpoints),
            "unknown_policy_endpoints": unknown_policy_endpoints,
            "mutation_without_post_guard": mutation_without_post_guard,
            "mutation_without_audit_requirement": mutation_without_audit_requirement,
            "mutation_like_routes_without_mutation_policy": mutation_like_routes_without_mutation_policy[:20],
            "write_path_policy_gaps": write_path_policy_gaps,
            "dynamic_failures": dynamic_failures,
            "sample_resolved_policies": resolved_endpoint_policies[:20],
        },
    )


def _scan_write_path_target(target):
    source_file = target.get("source_file") if isinstance(target, dict) else None
    source_path = _resolve_source_file(source_file)
    include_patterns = target.get("include_patterns") if isinstance(target, dict) else None
    exclude_patterns = target.get("exclude_patterns") if isinstance(target, dict) else None
    include_patterns = include_patterns if isinstance(include_patterns, list) else []
    exclude_patterns = exclude_patterns if isinstance(exclude_patterns, list) else []
    if not source_file or not include_patterns:
        return [], [{"source_file": source_file, "error": "scan_target_missing_source_file_or_include_patterns"}]
    if not source_path.exists():
        return [], [{"source_file": source_file, "error": "scan_target_source_file_missing"}]

    occurrences = []
    lines = source_path.read_text(encoding="utf-8").splitlines()
    for line_no, line in enumerate(lines, start=1):
        if not any(str(pattern) in line for pattern in include_patterns):
            continue
        if any(str(pattern) in line for pattern in exclude_patterns):
            continue
        occurrences.append(
            {
                "source_file": str(source_file),
                "line": line_no,
                "text": line.strip(),
            }
        )
    return occurrences, []


def _find_anchor_occurrences(source_file, source_anchor, scanned_occurrences):
    return [
        item
        for item in scanned_occurrences
        if item.get("source_file") == source_file and str(source_anchor or "") in item.get("text", "")
    ]


def verify_write_path_registry(registry_path=DEFAULT_WRITE_PATH_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("WritePathRegistryContract", False, "write_path_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("WritePathRegistryContract", False, "write_path_registry_not_object", {"registry_path": str(registry_path)})

    static_scan = registry.get("static_scan") if isinstance(registry.get("static_scan"), dict) else {}
    scan_targets = static_scan.get("targets") if isinstance(static_scan, dict) else []
    scan_targets = scan_targets if isinstance(scan_targets, list) else []
    write_paths = registry.get("write_paths") if isinstance(registry.get("write_paths"), list) else []

    scanned_occurrences = []
    scan_errors = []
    for target in scan_targets:
        occurrences, errors = _scan_write_path_target(target if isinstance(target, dict) else {})
        scanned_occurrences.extend(occurrences)
        scan_errors.extend(errors)

    malformed = []
    duplicate_write_path_ids = []
    duplicate_source_bindings = []
    seen_write_path_ids = set()
    seen_source_bindings = set()
    registered_anchors = {}
    for index, item in enumerate(write_paths):
        if not isinstance(item, dict):
            malformed.append({"index": index, "write_path_id": None, "missing_fields": list(WRITE_PATH_REQUIRED_FIELDS), "violations": ["write_path_not_object"]})
            continue
        write_path_id = item.get("write_path_id")
        missing = _missing_required_fields(item, WRITE_PATH_REQUIRED_FIELDS + WRITE_PATH_SOURCE_FIELDS)
        violations = []
        if write_path_id in seen_write_path_ids:
            duplicate_write_path_ids.append(write_path_id)
        if write_path_id:
            seen_write_path_ids.add(write_path_id)
        if not isinstance(item.get("requires_outbox"), bool):
            violations.append("requires_outbox_bool")
        if item.get("requires_outbox") is False and not item.get("outbox_reason"):
            violations.append("outbox_reason_required_when_requires_outbox_false")
        if str(item.get("mode_gate") or "") not in WRITE_PATH_ALLOWED_MODE_GATES:
            violations.append("mode_gate_invalid")
        try:
            source_anchor_occurrence = int(item.get("source_anchor_occurrence") or 1)
        except (TypeError, ValueError):
            source_anchor_occurrence = 0
        if source_anchor_occurrence <= 0:
            violations.append("source_anchor_occurrence_positive_int")
        source_file = str(item.get("source_file") or "")
        source_anchor = str(item.get("source_anchor") or "")
        source_binding = (source_file, source_anchor, source_anchor_occurrence)
        if source_binding in seen_source_bindings:
            duplicate_source_bindings.append(
                {
                    "source_file": source_file,
                    "source_anchor": source_anchor,
                    "source_anchor_occurrence": source_anchor_occurrence,
                    "write_path_id": write_path_id,
                }
            )
        seen_source_bindings.add(source_binding)
        if source_file and source_anchor:
            registered_anchors.setdefault(source_file, set()).add(source_anchor)
            anchor_occurrences = _find_anchor_occurrences(source_file, source_anchor, scanned_occurrences)
            if len(anchor_occurrences) < source_anchor_occurrence:
                violations.append("source_anchor_not_found")
        if missing or violations:
            malformed.append(
                {
                    "index": index,
                    "write_path_id": write_path_id,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    unregistered_occurrences = []
    for occurrence in scanned_occurrences:
        source_file = occurrence.get("source_file")
        anchors = registered_anchors.get(source_file, set())
        if not any(anchor in occurrence.get("text", "") for anchor in anchors):
            unregistered_occurrences.append(occurrence)

    passed = (
        registry.get("schema_version") == "v2.7.0.write_path_registry.v1"
        and bool(scan_targets)
        and bool(write_paths)
        and not scan_errors
        and not malformed
        and not duplicate_write_path_ids
        and not duplicate_source_bindings
        and not unregistered_occurrences
    )
    return _contract(
        "WritePathRegistryContract",
        passed,
        "write_path_registry_missing_malformed_or_incomplete",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "scope": registry.get("scope"),
            "scan_target_count": len(scan_targets),
            "scanned_mutation_count": len(scanned_occurrences),
            "registered_write_path_count": len(write_paths),
            "duplicate_write_path_ids": sorted(str(item) for item in duplicate_write_path_ids),
            "duplicate_source_bindings": duplicate_source_bindings,
            "scan_errors": scan_errors,
            "malformed_write_paths": malformed,
            "unregistered_mutation_count": len(unregistered_occurrences),
            "unregistered_mutations": unregistered_occurrences[:20],
            "registered_targets": sorted(
                {
                    str(item.get("target_store"))
                    for item in write_paths
                    if isinstance(item, dict) and item.get("target_store")
                }
            ),
        },
    )


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


def verify_evidence_eligibility_matrix(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("EvidenceEligibilityMatrix", False, "evidence_eligibility_matrix_missing_or_invalid", base_evidence)
    rows = governance.get("evidence_eligibility_matrix")
    if not isinstance(rows, list):
        rows = []
    required = ("evidence_use", "event_truth", "feature_truth", "label_truth", "replay_truth")
    malformed = []
    for index, row in enumerate(rows):
        missing = _missing_required_fields(row, required)
        truth_fields = {
            field: row.get(field)
            for field in ("event_truth", "feature_truth", "label_truth", "replay_truth")
            if isinstance(row, dict)
        }
        non_list_truth = sorted(field for field, value in truth_fields.items() if not isinstance(value, list))
        if missing or non_list_truth:
            malformed.append({"index": index, "evidence_use": row.get("evidence_use") if isinstance(row, dict) else None, "missing_fields": missing, "non_list_truth_fields": non_list_truth})
    evidence_uses = sorted({row.get("evidence_use") for row in rows if isinstance(row, dict) and row.get("evidence_use")})
    passed = bool(rows) and not malformed and "normal_tiny_promotion" in evidence_uses
    return _contract(
        "EvidenceEligibilityMatrix",
        passed,
        "evidence_eligibility_matrix_missing_malformed_or_incomplete",
        {
            **base_evidence,
            "matrix_row_count": len(rows),
            "evidence_uses": evidence_uses,
            "malformed_rows": malformed,
            "required_evidence_use_present": "normal_tiny_promotion" in evidence_uses,
        },
    )


def verify_top_fix_queue(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("TopFixQueueContract", False, "top_fix_queue_missing_or_invalid", base_evidence)
    queue = governance.get("top_fix_queue")
    if not isinstance(queue, list):
        queue = []
    required = ("fix_id", "blocker_code", "first_fix_that_would_change_decision", "owner", "acceptance_test")
    malformed = []
    seen_fix_ids = set()
    duplicate_fix_ids = []
    blocker_codes = set()
    for index, item in enumerate(queue):
        missing = _missing_required_fields(item, required)
        fix_id = item.get("fix_id") if isinstance(item, dict) else None
        blocker_code = item.get("blocker_code") if isinstance(item, dict) else None
        if fix_id in seen_fix_ids:
            duplicate_fix_ids.append(fix_id)
        if fix_id:
            seen_fix_ids.add(fix_id)
        if blocker_code:
            blocker_codes.add(str(blocker_code))
        if missing:
            malformed.append({"index": index, "fix_id": fix_id, "blocker_code": blocker_code, "missing_fields": missing})
    missing_blocker_codes = sorted(NORMAL_TINY_BLOCKING_CONTRACTS - blocker_codes)
    passed = bool(queue) and not malformed and not duplicate_fix_ids and not missing_blocker_codes
    return _contract(
        "TopFixQueueContract",
        passed,
        "top_fix_queue_missing_malformed_or_incomplete",
        {
            **base_evidence,
            "queue_count": len(queue),
            "normal_tiny_contract_count": len(NORMAL_TINY_BLOCKING_CONTRACTS),
            "covered_blocker_codes": sorted(blocker_codes),
            "missing_blocker_codes": missing_blocker_codes,
            "malformed_queue_items": malformed,
            "duplicate_fix_ids": sorted(duplicate_fix_ids),
        },
    )


def verify_safety_case(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("SafetyCaseContract", False, "safety_case_missing_or_invalid", base_evidence)
    safety_cases = governance.get("safety_cases")
    if not isinstance(safety_cases, list):
        safety_cases = []
    required = ("safety_case_id", "scope", "core_hazards", "mitigations", "evidence_links")
    required_links = {"EvidenceEligibilityMatrix", "TopFixQueueContract", "WaiverPolicyContract", "SafeDefaultContract", "ProjectStopLossContract"}
    malformed = []
    normal_tiny_cases = []
    link_coverage = set()
    for index, item in enumerate(safety_cases):
        missing = _missing_required_fields(item, required)
        safety_case_id = item.get("safety_case_id") if isinstance(item, dict) else None
        if isinstance(item, dict) and item.get("scope") == "normal_tiny":
            normal_tiny_cases.append(item)
            links = item.get("evidence_links")
            if isinstance(links, list):
                link_coverage.update(str(link) for link in links)
        if missing:
            malformed.append({"index": index, "safety_case_id": safety_case_id, "missing_fields": missing})
    missing_links = sorted(required_links - link_coverage)
    passed = bool(normal_tiny_cases) and not malformed and not missing_links
    return _contract(
        "SafetyCaseContract",
        passed,
        "safety_case_missing_malformed_or_unlinked",
        {
            **base_evidence,
            "safety_case_count": len(safety_cases),
            "normal_tiny_safety_case_count": len(normal_tiny_cases),
            "malformed_safety_cases": malformed,
            "required_evidence_links": sorted(required_links),
            "missing_evidence_links": missing_links,
        },
    )


def verify_waiver_policy(governance_path=DEFAULT_GOVERNANCE_READINESS):
    governance, base_evidence = _load_governance_readiness(governance_path)
    if governance is None:
        return _contract("WaiverPolicyContract", False, "waiver_policy_missing_or_invalid", base_evidence)
    policies = governance.get("waiver_policy")
    if not isinstance(policies, list):
        policies = []
    required = ("waiver_id", "contract_id", "scope", "expires_at", "non_waivable")
    malformed = []
    non_waivable_contracts = set()
    wildcard_non_waivable = False
    now = datetime.now(timezone.utc)
    for index, item in enumerate(policies):
        missing = _missing_required_fields(item, required)
        waiver_id = item.get("waiver_id") if isinstance(item, dict) else None
        contract_id = item.get("contract_id") if isinstance(item, dict) else None
        parsed_expires_at = _parse_iso_ts(item.get("expires_at")) if isinstance(item, dict) else None
        violations = []
        if parsed_expires_at is None:
            violations.append("expires_at_parseable")
        elif parsed_expires_at <= now:
            violations.append("expires_at_future")
        if isinstance(item, dict) and item.get("scope") == "normal_tiny" and item.get("non_waivable") is True:
            if contract_id == "*":
                wildcard_non_waivable = True
            elif contract_id:
                non_waivable_contracts.add(str(contract_id))
        else:
            violations.append("normal_tiny_non_waivable_true")
        if missing or violations:
            malformed.append({"index": index, "waiver_id": waiver_id, "contract_id": contract_id, "missing_fields": missing, "violations": violations})
    missing_non_waivable = [] if wildcard_non_waivable else sorted(NORMAL_TINY_BLOCKING_CONTRACTS - non_waivable_contracts)
    passed = bool(policies) and not malformed and not missing_non_waivable
    return _contract(
        "WaiverPolicyContract",
        passed,
        "waiver_policy_missing_malformed_or_bypassable",
        {
            **base_evidence,
            "waiver_policy_count": len(policies),
            "wildcard_non_waivable": wildcard_non_waivable,
            "non_waivable_contracts": sorted(non_waivable_contracts),
            "missing_non_waivable_contracts": missing_non_waivable,
            "malformed_waiver_policies": malformed,
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
    governance_path=DEFAULT_GOVERNANCE_READINESS,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
    write_path_registry_path=DEFAULT_WRITE_PATH_REGISTRY,
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
            verify_safe_default(registry_path=registry_path),
            verify_project_stop_loss(env=env),
            verify_evidence_eligibility_matrix(governance_path=governance_path),
            verify_top_fix_queue(governance_path=governance_path),
            verify_safety_case(governance_path=governance_path),
            verify_waiver_policy(governance_path=governance_path),
            verify_access_control_policy(
                policy_path=access_control_policy_path,
                write_path_registry_path=write_path_registry_path,
            ),
            verify_write_path_registry(registry_path=write_path_registry_path),
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
    parser.add_argument("--governance-readiness", default=str(DEFAULT_GOVERNANCE_READINESS))
    parser.add_argument("--access-control-policy", default=str(DEFAULT_ACCESS_CONTROL_POLICY))
    parser.add_argument("--write-path-registry", default=str(DEFAULT_WRITE_PATH_REGISTRY))
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
        governance_path=Path(args.governance_readiness),
        access_control_policy_path=Path(args.access_control_policy),
        write_path_registry_path=Path(args.write_path_registry),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and report["blocking_contracts"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
