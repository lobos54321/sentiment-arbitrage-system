#!/usr/bin/env python3
"""Verify v2.7 observe-only foundation contracts from local machine evidence."""

import argparse
import csv
import hashlib
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
DEFAULT_DIRECT_DB_MUTATION_POLICY = PROJECT_ROOT / "config" / "v27-direct-database-mutation-policy.json"
DEFAULT_BACKGROUND_JOB_REGISTRY = PROJECT_ROOT / "config" / "v27-background-job-registry.json"
DEFAULT_ENTRY_POINT_INVENTORY = PROJECT_ROOT / "config" / "v27-entry-point-inventory.json"
DEFAULT_STATIC_POLICY_ENFORCEMENT = PROJECT_ROOT / "config" / "v27-static-policy-enforcement.json"
DEFAULT_API_RESPONSE_POLICY = PROJECT_ROOT / "config" / "v27-api-response-policy.json"
DEFAULT_API_RESPONSE_ENVELOPE_POLICY = PROJECT_ROOT / "config" / "v27-api-response-envelope-policy.json"
DEFAULT_ERROR_TAXONOMY = PROJECT_ROOT / "config" / "v27-error-taxonomy.json"
DEFAULT_LOG_REDACTION_POLICY = PROJECT_ROOT / "config" / "v27-log-redaction-policy.json"
DEFAULT_SERVICE_READINESS_PROBES = PROJECT_ROOT / "config" / "v27-service-readiness-probes.json"
ACCESS_CONTROL_REQUIRED_FIELDS = (
    "endpoint",
    "required_role",
    "token_scope",
    "audit_log_required",
    "danger_level",
)
AUDIT_LOG_REQUIRED_FIELDS = (
    "audit_event_id",
    "prev_audit_hash",
    "audit_payload_hash",
    "audit_chain_hash",
    "created_at",
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
DIRECT_DB_MUTATION_REQUIRED_FIELDS = (
    "write_path_id",
    "target_store",
    "approved_mutation_path",
    "break_glass_id",
)
BACKGROUND_JOB_REQUIRED_FIELDS = (
    "job_name",
    "entry_point",
    "allowed_modes",
    "lease_policy",
    "owner",
)
BACKGROUND_JOB_ALLOWED_MODES = {"observe_only", "shadow", "ultra_tiny", "normal_tiny"}
ENTRY_POINT_REQUIRED_FIELDS = (
    "entry_point_id",
    "code_location",
    "route_registry_required",
    "arbiter_required",
)
ENTRY_POINT_ALLOWED_TYPES = {"route_group", "server", "script", "cron", "deploy"}
STATIC_POLICY_REQUIRED_FIELDS = (
    "static_check_id",
    "forbidden_pattern",
    "scan_target",
    "result",
)
API_RESPONSE_REQUIRED_FIELDS = (
    "endpoint",
    "response_schema_version",
    "status_code_policy",
    "error_envelope",
    "cache_control",
)
API_RESPONSE_ENVELOPE_REQUIRED_FIELDS = (
    "endpoint",
    "response_schema_version",
    "source_anchor",
)
API_RESPONSE_ENVELOPE_SPEC_FIELDS = (
    "endpoint",
    "envelope_version",
    "payload_hash",
    "error_shape",
    "generated_at",
)
ERROR_TAXONOMY_REQUIRED_FIELDS = (
    "error_code",
    "category",
    "severity",
    "operator_action",
    "introduced_at",
)
LOG_REDACTION_STREAM_REQUIRED_FIELDS = (
    "log_stream",
    "secret_pattern_set",
    "source_file",
    "redaction_anchor",
    "write_anchor",
    "sample_case_ids",
)
SERVICE_READINESS_PROBE_REQUIRED_FIELDS = (
    "service_name",
    "probe_id",
    "health_status",
    "dependency_status",
    "source_file",
    "source_anchor",
)
SERVICE_READINESS_CONTRACT_FIELDS = (
    "service_name",
    "probe_id",
    "health_status",
    "dependency_status",
    "checked_at",
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


def _sha256_json(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


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
        audit_event_line = None
        mutation_markers = []
        for offset, text in enumerate(block):
            if check_auth_line is None and "checkAuth(req, url, res)" in text:
                check_auth_line = line_index + offset + 1
            if post_guard_line is None and ("req.method !== 'POST'" in text or "requirePost(req, res)" in text):
                post_guard_line = line_index + offset + 1
            if audit_event_line is None and "requireDashboardAuditEvent(req, res, url" in text:
                audit_event_line = line_index + offset + 1
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
                    "has_audit_event": audit_event_line is not None,
                    "audit_event_line": audit_event_line,
                    "mutation_markers": mutation_markers[:5],
                }
            )
    return routes


def _dashboard_route_block(lines, endpoint):
    route_line_indexes = []
    endpoints_by_line = {}
    for index, line in enumerate(lines):
        endpoints = re.findall(r"url\.pathname\s*===\s*['\"]([^'\"]+)['\"]", line)
        if not endpoints:
            continue
        route_line_indexes.append(index)
        endpoints_by_line[index] = endpoints
    for route_index, line_index in enumerate(route_line_indexes):
        if endpoint not in endpoints_by_line.get(line_index, []):
            continue
        next_line_index = route_line_indexes[route_index + 1] if route_index + 1 < len(route_line_indexes) else len(lines)
        return "\n".join(lines[line_index:next_line_index])
    return ""


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


def verify_audit_log_integrity(policy_path=DEFAULT_ACCESS_CONTROL_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("AuditLogIntegrityContract", False, "audit_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("AuditLogIntegrityContract", False, "audit_policy_not_object", {"policy_path": str(policy_path)})

    lines, source_error = _source_lines(policy.get("source_file"))
    if source_error:
        return _contract("AuditLogIntegrityContract", False, "audit_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(lines)
    routes = _extract_dashboard_routes(lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    overrides = [
        item
        for item in (policy.get("endpoint_overrides") or [])
        if isinstance(item, dict)
    ]
    audit_required_endpoints = sorted(
        str(item.get("endpoint"))
        for item in overrides
        if item.get("audit_log_required") is True and item.get("endpoint")
    )
    missing_audit_hooks = []
    for endpoint in audit_required_endpoints:
        route = route_by_endpoint.get(endpoint)
        if not route or not route.get("has_audit_event"):
            missing_audit_hooks.append(
                {
                    "endpoint": endpoint,
                    "registered_route": bool(route),
                    "has_audit_event": bool(route and route.get("has_audit_event")),
                }
            )

    helper_required_fragments = {
        "schema_version": "DASHBOARD_AUDIT_SCHEMA_VERSION" in source_text and "v2.7.0.audit_log_integrity.v1" in source_text,
        "sha256_hashing": "createHash('sha256')" in source_text,
        "append_only_jsonl": "fs.appendFileSync(auditLogPath" in source_text,
        "chain_verifier": "verifyDashboardAuditChain" in source_text,
        "fail_closed_response": "Audit log unavailable" in source_text,
    }
    chain_field_presence = {
        field: field in source_text
        for field in AUDIT_LOG_REQUIRED_FIELDS
    }
    passed = (
        bool(audit_required_endpoints)
        and all(helper_required_fragments.values())
        and all(chain_field_presence.values())
        and not missing_audit_hooks
    )
    return _contract(
        "AuditLogIntegrityContract",
        passed,
        "audit_log_integrity_missing_malformed_or_incomplete",
        {
            "policy_path": str(policy_path),
            "source_file": policy.get("source_file"),
            "schema_version": "v2.7.0.audit_log_integrity.v1",
            "audit_required_endpoint_count": len(audit_required_endpoints),
            "audit_required_endpoints": audit_required_endpoints,
            "helper_required_fragments": helper_required_fragments,
            "chain_field_presence": chain_field_presence,
            "missing_audit_hooks": missing_audit_hooks,
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


def _entry_point_endpoint(entry_point):
    parts = str(entry_point or "").split()
    if len(parts) >= 2 and parts[0].upper() in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        return parts[0].upper(), parts[1]
    return None, None


def verify_direct_database_mutation_ban(
    policy_path=DEFAULT_DIRECT_DB_MUTATION_POLICY,
    registry_path=DEFAULT_WRITE_PATH_REGISTRY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        policy = _load_json(policy_path)
        registry = _load_json(registry_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("DirectDatabaseMutationBan", False, "direct_db_mutation_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict) or not isinstance(registry, dict) or not isinstance(access_policy, dict):
        return _contract(
            "DirectDatabaseMutationBan",
            False,
            "direct_db_mutation_policy_not_object",
            {
                "policy_path": str(policy_path),
                "registry_path": str(registry_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    write_paths = registry.get("write_paths") if isinstance(registry.get("write_paths"), list) else []
    direct_db_paths = [
        item for item in write_paths
        if isinstance(item, dict) and str(item.get("target_store") or "").startswith("sqlite:")
    ]
    direct_by_id = {str(item.get("write_path_id")): item for item in direct_db_paths if item.get("write_path_id")}
    approved_paths = policy.get("approved_mutation_paths") if isinstance(policy.get("approved_mutation_paths"), list) else []
    rules = policy.get("rules") if isinstance(policy.get("rules"), dict) else {}
    required_mode_gate = str(rules.get("required_registry_mode_gate") or "admin_break_glass")
    access_by_endpoint = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }

    malformed_policy_rows = []
    duplicate_policy_write_path_ids = []
    seen_policy_ids = set()
    approved_by_id = {}
    for index, item in enumerate(approved_paths):
        if not isinstance(item, dict):
            malformed_policy_rows.append({"index": index, "write_path_id": None, "missing_fields": list(DIRECT_DB_MUTATION_REQUIRED_FIELDS), "violations": ["policy_row_not_object"]})
            continue
        write_path_id = str(item.get("write_path_id") or "")
        missing = _missing_required_fields(item, DIRECT_DB_MUTATION_REQUIRED_FIELDS)
        violations = []
        if write_path_id in seen_policy_ids:
            duplicate_policy_write_path_ids.append(write_path_id)
        if write_path_id:
            seen_policy_ids.add(write_path_id)
            approved_by_id[write_path_id] = item
        method, endpoint = _entry_point_endpoint(item.get("approved_mutation_path"))
        if method != "POST" or not endpoint:
            violations.append("approved_mutation_path_must_be_post_endpoint")
        if item.get("break_glass_id") and not str(item.get("break_glass_id")).startswith("BG-DDB-"):
            violations.append("break_glass_id_prefix_invalid")
        registry_item = direct_by_id.get(write_path_id)
        if registry_item:
            if item.get("target_store") != registry_item.get("target_store"):
                violations.append("target_store_mismatch_registry")
            if item.get("approved_mutation_path") != registry_item.get("entry_point"):
                violations.append("approved_mutation_path_mismatch_registry")
        elif write_path_id:
            violations.append("approved_path_not_in_direct_db_registry")
        if missing or violations:
            malformed_policy_rows.append(
                {
                    "index": index,
                    "write_path_id": write_path_id or None,
                    "missing_fields": missing,
                    "violations": violations,
                }
            )

    unapproved_direct_db_mutations = [
        {
            "write_path_id": item.get("write_path_id"),
            "target_store": item.get("target_store"),
            "entry_point": item.get("entry_point"),
        }
        for item in direct_db_paths
        if str(item.get("write_path_id")) not in approved_by_id
    ]
    registry_gate_violations = []
    access_control_violations = []
    outbox_rationale_violations = []
    for item in direct_db_paths:
        write_path_id = str(item.get("write_path_id") or "")
        if str(item.get("mode_gate") or "") != required_mode_gate:
            registry_gate_violations.append(
                {
                    "write_path_id": write_path_id,
                    "mode_gate": item.get("mode_gate"),
                    "required_mode_gate": required_mode_gate,
                }
            )
        if rules.get("require_outbox_rationale", True) and item.get("requires_outbox") is False and not item.get("outbox_reason"):
            outbox_rationale_violations.append({"write_path_id": write_path_id})
        method, endpoint = _entry_point_endpoint(item.get("entry_point"))
        endpoint_policy = access_by_endpoint.get(endpoint)
        if not endpoint_policy:
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "missing_access_policy"})
            continue
        if rules.get("require_post", True) and (
            method != "POST"
            or endpoint_policy.get("method_guard_required") is not True
            or "POST" not in [str(value).upper() for value in (endpoint_policy.get("allowed_methods") or [])]
        ):
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "post_guard_missing"})
        if rules.get("require_audit_log", True) and endpoint_policy.get("audit_log_required") is not True:
            access_control_violations.append({"write_path_id": write_path_id, "endpoint": endpoint, "reason": "audit_requirement_missing"})

    passed = (
        policy.get("schema_version") == "v2.7.0.direct_database_mutation_ban.v1"
        and bool(direct_db_paths)
        and len(approved_paths) == len(direct_db_paths)
        and not malformed_policy_rows
        and not duplicate_policy_write_path_ids
        and not unapproved_direct_db_mutations
        and not registry_gate_violations
        and not access_control_violations
        and not outbox_rationale_violations
    )
    return _contract(
        "DirectDatabaseMutationBan",
        passed,
        "direct_db_mutation_ban_missing_malformed_or_bypassed",
        {
            "policy_path": str(policy_path),
            "registry_path": str(registry_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": policy.get("schema_version"),
            "default_action": rules.get("default_action"),
            "required_registry_mode_gate": required_mode_gate,
            "direct_db_write_path_count": len(direct_db_paths),
            "approved_mutation_path_count": len(approved_paths),
            "direct_db_targets": sorted({str(item.get("target_store")) for item in direct_db_paths if item.get("target_store")}),
            "malformed_policy_rows": malformed_policy_rows,
            "duplicate_policy_write_path_ids": sorted(str(item) for item in duplicate_policy_write_path_ids),
            "unapproved_direct_db_mutations": unapproved_direct_db_mutations,
            "registry_gate_violations": registry_gate_violations,
            "access_control_violations": access_control_violations,
            "outbox_rationale_violations": outbox_rationale_violations,
        },
    )


def _resolve_project_file(raw_path):
    if not raw_path:
        return None
    path = Path(str(raw_path))
    return path if path.is_absolute() else PROJECT_ROOT / path


def verify_background_job_registry(registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY):
    try:
        registry = _load_json(registry_path)
    except Exception as exc:
        return _contract("BackgroundJobRegistryContract", False, "background_job_registry_missing_or_invalid", {"error": str(exc)})
    if not isinstance(registry, dict):
        return _contract("BackgroundJobRegistryContract", False, "background_job_registry_not_object", {"registry_path": str(registry_path)})

    jobs = registry.get("jobs") if isinstance(registry.get("jobs"), list) else []
    malformed_jobs = []
    duplicate_job_names = []
    missing_entry_point_files = []
    missing_source_anchors = []
    seen_job_names = set()
    restart_loop_jobs = 0
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            malformed_jobs.append({"index": index, "job_name": None, "missing_fields": list(BACKGROUND_JOB_REQUIRED_FIELDS), "violations": ["job_not_object"]})
            continue
        job_name = str(job.get("job_name") or "")
        missing = _missing_required_fields(job, BACKGROUND_JOB_REQUIRED_FIELDS)
        violations = []
        if job_name in seen_job_names:
            duplicate_job_names.append(job_name)
        if job_name:
            seen_job_names.add(job_name)
        allowed_modes = job.get("allowed_modes")
        if not isinstance(allowed_modes, list) or not allowed_modes:
            violations.append("allowed_modes_non_empty_list_required")
        else:
            invalid_modes = sorted(str(mode) for mode in allowed_modes if str(mode) not in BACKGROUND_JOB_ALLOWED_MODES)
            if invalid_modes:
                violations.append(f"allowed_modes_invalid:{','.join(invalid_modes)}")
        lease_policy = job.get("lease_policy")
        if not isinstance(lease_policy, dict) or not lease_policy.get("kind"):
            violations.append("lease_policy_kind_required")
        elif str(lease_policy.get("kind")) == "supervised_restart_loop":
            restart_loop_jobs += 1
            if not lease_policy.get("pid_env"):
                violations.append("supervised_restart_loop_pid_env_required")
            try:
                if int(lease_policy.get("restart_delay_sec", 0)) <= 0:
                    violations.append("supervised_restart_loop_restart_delay_positive")
            except (TypeError, ValueError):
                violations.append("supervised_restart_loop_restart_delay_positive")
        entry_point_file = _resolve_project_file(job.get("entry_point_file"))
        if entry_point_file and not entry_point_file.exists():
            missing_entry_point_files.append({"job_name": job_name, "entry_point_file": job.get("entry_point_file")})
        source_file = _resolve_project_file(job.get("source_file"))
        source_anchor = str(job.get("source_anchor") or "")
        if not source_file or not source_anchor:
            violations.append("source_file_and_anchor_required")
        elif not source_file.exists():
            missing_source_anchors.append({"job_name": job_name, "source_file": job.get("source_file"), "source_anchor": source_anchor, "reason": "source_file_missing"})
        else:
            source_text = source_file.read_text(encoding="utf-8")
            if source_anchor not in source_text:
                missing_source_anchors.append({"job_name": job_name, "source_file": job.get("source_file"), "source_anchor": source_anchor, "reason": "source_anchor_missing"})
        if missing or violations:
            malformed_jobs.append({"index": index, "job_name": job_name or None, "missing_fields": missing, "violations": violations})

    passed = (
        registry.get("schema_version") == "v2.7.0.background_job_registry.v1"
        and bool(jobs)
        and restart_loop_jobs >= 5
        and not malformed_jobs
        and not duplicate_job_names
        and not missing_entry_point_files
        and not missing_source_anchors
    )
    return _contract(
        "BackgroundJobRegistryContract",
        passed,
        "background_job_registry_missing_malformed_or_incomplete",
        {
            "registry_path": str(registry_path),
            "schema_version": registry.get("schema_version"),
            "scope": registry.get("scope"),
            "job_count": len(jobs),
            "restart_loop_job_count": restart_loop_jobs,
            "job_names": sorted(str(job.get("job_name")) for job in jobs if isinstance(job, dict) and job.get("job_name")),
            "duplicate_job_names": sorted(str(item) for item in duplicate_job_names),
            "malformed_jobs": malformed_jobs,
            "missing_entry_point_files": missing_entry_point_files,
            "missing_source_anchors": missing_source_anchors,
        },
    )


def _verify_code_location(location, label):
    violations = []
    if not isinstance(location, dict):
        return [{"location": label, "reason": "location_not_object"}]
    raw_file = location.get("file")
    anchor = location.get("anchor")
    if not raw_file:
        violations.append({"location": label, "reason": "file_missing"})
        return violations
    path = _resolve_project_file(raw_file)
    if not path.exists():
        violations.append({"location": label, "file": str(raw_file), "reason": "file_not_found"})
        return violations
    if anchor:
        text = path.read_text(encoding="utf-8")
        if str(anchor) not in text:
            violations.append({"location": label, "file": str(raw_file), "anchor": str(anchor), "reason": "anchor_not_found"})
    return violations


def verify_entry_point_inventory(
    inventory_path=DEFAULT_ENTRY_POINT_INVENTORY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        inventory = _load_json(inventory_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("EntryPointInventoryContract", False, "entry_point_inventory_missing_or_invalid", {"error": str(exc)})
    if not isinstance(inventory, dict) or not isinstance(access_policy, dict):
        return _contract(
            "EntryPointInventoryContract",
            False,
            "entry_point_inventory_not_object",
            {
                "inventory_path": str(inventory_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    source_lines, source_error = _source_lines(access_policy.get("source_file"))
    if source_error:
        return _contract("EntryPointInventoryContract", False, "entry_point_source_missing", {"inventory_path": str(inventory_path), **source_error})
    routes = _extract_dashboard_routes(source_lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    public_endpoints = set(str(item) for item in (access_policy.get("public_endpoints") or []))
    protected_route_count = sum(1 for route in routes if route.get("endpoint") not in public_endpoints)
    overrides = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }
    audit_required_endpoints = {
        endpoint
        for endpoint, item in overrides.items()
        if item.get("audit_log_required") is True
    }
    dynamic_source_anchors = {
        str(item.get("source_anchor"))
        for item in (access_policy.get("dynamic_protected_routes") or [])
        if isinstance(item, dict) and item.get("source_anchor")
    }

    entries = inventory.get("entry_points") if isinstance(inventory.get("entry_points"), list) else []
    malformed_entries = []
    duplicate_entry_point_ids = []
    location_violations = []
    route_group_violations = []
    dynamic_route_violations = []
    seen_ids = set()
    covered_route_endpoints = set()
    route_registry_required_count = 0
    arbiter_required_count = 0
    entry_type_counts = {}

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            malformed_entries.append({"index": index, "entry_point_id": None, "missing_fields": list(ENTRY_POINT_REQUIRED_FIELDS), "violations": ["entry_not_object"]})
            continue
        entry_id = str(entry.get("entry_point_id") or "")
        entry_type = str(entry.get("entry_type") or "")
        missing = _missing_required_fields(entry, ENTRY_POINT_REQUIRED_FIELDS)
        violations = []
        if entry_id in seen_ids:
            duplicate_entry_point_ids.append(entry_id)
        if entry_id:
            seen_ids.add(entry_id)
        if entry_type not in ENTRY_POINT_ALLOWED_TYPES:
            violations.append("entry_type_invalid_or_missing")
        else:
            entry_type_counts[entry_type] = entry_type_counts.get(entry_type, 0) + 1
        for bool_field in ("route_registry_required", "arbiter_required"):
            if not isinstance(entry.get(bool_field), bool):
                violations.append(f"{bool_field}_must_be_bool")
        if entry.get("route_registry_required") is True:
            route_registry_required_count += 1
            if not entry.get("route_registry_reason"):
                violations.append("route_registry_reason_required")
        if entry.get("arbiter_required") is True:
            arbiter_required_count += 1
            if not entry.get("arbiter_reason"):
                violations.append("arbiter_reason_required")

        location_violations.extend(
            {"entry_point_id": entry_id, **violation}
            for violation in _verify_code_location(entry.get("code_location"), "code_location")
        )
        for optional_location in ("launcher_location", "target_location"):
            if optional_location in entry:
                location_violations.extend(
                    {"entry_point_id": entry_id, **violation}
                    for violation in _verify_code_location(entry.get(optional_location), optional_location)
                )

        route_group = entry.get("route_group") if isinstance(entry.get("route_group"), dict) else None
        if route_group:
            endpoints = [str(endpoint) for endpoint in (route_group.get("endpoints") or [])]
            covered_route_endpoints.update(endpoints)
            for endpoint in endpoints:
                if endpoint not in route_by_endpoint:
                    route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "route_not_found"})
            expected_literal = route_group.get("expected_literal_route_count")
            if expected_literal is not None and int(expected_literal) != len(routes):
                route_group_violations.append(
                    {
                        "entry_point_id": entry_id,
                        "expected_literal_route_count": expected_literal,
                        "actual_literal_route_count": len(routes),
                        "reason": "literal_route_count_mismatch",
                    }
                )
            expected_protected = route_group.get("expected_protected_route_count")
            if expected_protected is not None and int(expected_protected) != protected_route_count:
                route_group_violations.append(
                    {
                        "entry_point_id": entry_id,
                        "expected_protected_route_count": expected_protected,
                        "actual_protected_route_count": protected_route_count,
                        "reason": "protected_route_count_mismatch",
                    }
                )
            if route_group.get("require_access_control") or route_group.get("require_post") or route_group.get("require_audit"):
                for endpoint in endpoints:
                    policy = overrides.get(endpoint)
                    if not policy:
                        route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "access_policy_override_missing"})
                        continue
                    if route_group.get("require_post"):
                        allowed = [str(value).upper() for value in (policy.get("allowed_methods") or [])]
                        if policy.get("method_guard_required") is not True or "POST" not in allowed:
                            route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "post_guard_missing"})
                    if route_group.get("require_audit") and policy.get("audit_log_required") is not True:
                        route_group_violations.append({"entry_point_id": entry_id, "endpoint": endpoint, "reason": "audit_requirement_missing"})

        dynamic_group = entry.get("dynamic_route_group") if isinstance(entry.get("dynamic_route_group"), dict) else None
        if dynamic_group:
            source_anchor = str(dynamic_group.get("source_anchor") or "")
            if dynamic_group.get("require_access_control") and source_anchor not in dynamic_source_anchors:
                dynamic_route_violations.append({"entry_point_id": entry_id, "source_anchor": source_anchor, "reason": "dynamic_access_policy_missing"})

        if missing or violations:
            malformed_entries.append({"index": index, "entry_point_id": entry_id or None, "missing_fields": missing, "violations": violations})

    uncovered_audit_required_routes = sorted(audit_required_endpoints - covered_route_endpoints)
    passed = (
        inventory.get("schema_version") == "v2.7.0.entry_point_inventory.v1"
        and bool(entries)
        and len(routes) >= 60
        and protected_route_count >= 50
        and route_registry_required_count >= 2
        and arbiter_required_count >= 20
        and not malformed_entries
        and not duplicate_entry_point_ids
        and not location_violations
        and not route_group_violations
        and not dynamic_route_violations
        and not uncovered_audit_required_routes
    )
    return _contract(
        "EntryPointInventoryContract",
        passed,
        "entry_point_inventory_missing_malformed_or_incomplete",
        {
            "inventory_path": str(inventory_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": inventory.get("schema_version"),
            "entry_point_count": len(entries),
            "entry_type_counts": entry_type_counts,
            "dashboard_literal_route_count": len(routes),
            "dashboard_protected_route_count": protected_route_count,
            "route_registry_required_count": route_registry_required_count,
            "arbiter_required_count": arbiter_required_count,
            "duplicate_entry_point_ids": sorted(str(item) for item in duplicate_entry_point_ids),
            "malformed_entries": malformed_entries,
            "location_violations": location_violations,
            "route_group_violations": route_group_violations,
            "dynamic_route_violations": dynamic_route_violations,
            "audit_required_route_count": len(audit_required_endpoints),
            "uncovered_audit_required_routes": uncovered_audit_required_routes,
        },
    )


def _static_policy_scan_files(scan_target):
    raw_targets = scan_target if isinstance(scan_target, list) else [scan_target]
    files = []
    for raw_target in raw_targets:
        if not raw_target:
            continue
        raw_text = str(raw_target)
        if any(char in raw_text for char in "*?[]") and not Path(raw_text).is_absolute():
            files.extend(sorted(path for path in PROJECT_ROOT.glob(raw_text) if path.is_file()))
            continue
        files.append(_resolve_project_file(raw_text))
    unique = []
    seen = set()
    for path in files:
        if not path:
            continue
        resolved = str(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _static_policy_line(text, offset):
    line_no = text.count("\n", 0, offset) + 1
    line_start = text.rfind("\n", 0, offset) + 1
    line_end = text.find("\n", offset)
    if line_end == -1:
        line_end = len(text)
    return line_no, text[line_start:line_end].strip()[:180]


def verify_static_policy_enforcement(policy_path=DEFAULT_STATIC_POLICY_ENFORCEMENT):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("StaticPolicyEnforcementContract", False, "static_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("StaticPolicyEnforcementContract", False, "static_policy_not_object", {"policy_path": str(policy_path)})

    checks = policy.get("checks") if isinstance(policy.get("checks"), list) else []
    malformed_checks = []
    duplicate_static_check_ids = []
    scan_errors = []
    forbidden_matches = []
    seen_ids = set()
    scan_target_files = set()

    for index, check in enumerate(checks):
        if not isinstance(check, dict):
            malformed_checks.append({"index": index, "static_check_id": None, "missing_fields": list(STATIC_POLICY_REQUIRED_FIELDS), "violations": ["check_not_object"]})
            continue
        check_id = str(check.get("static_check_id") or "")
        missing = _missing_required_fields(check, STATIC_POLICY_REQUIRED_FIELDS)
        violations = []
        if check_id in seen_ids:
            duplicate_static_check_ids.append(check_id)
        if check_id:
            seen_ids.add(check_id)
        if check.get("result") != "pass":
            violations.append("result_must_be_pass")
        try:
            pattern = re.compile(str(check.get("forbidden_pattern") or ""))
        except re.error as exc:
            pattern = None
            violations.append("forbidden_pattern_invalid")
            scan_errors.append({"static_check_id": check_id, "reason": "forbidden_pattern_invalid", "error": str(exc)})

        target_files = _static_policy_scan_files(check.get("scan_target"))
        if not target_files:
            scan_errors.append({"static_check_id": check_id, "scan_target": check.get("scan_target"), "reason": "scan_target_empty"})
        for path in target_files:
            scan_target_files.add(str(path))
            if not path.exists():
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_missing"})
                continue
            if not path.is_file():
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_not_file"})
                continue
            if pattern is None:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                scan_errors.append({"static_check_id": check_id, "file": str(path), "reason": "scan_target_decode_failed", "error": str(exc)})
                continue
            for match in pattern.finditer(text):
                line_no, line_text = _static_policy_line(text, match.start())
                forbidden_matches.append(
                    {
                        "static_check_id": check_id,
                        "file": str(path),
                        "line": line_no,
                        "match": line_text,
                    }
                )

        if missing or violations:
            malformed_checks.append({"index": index, "static_check_id": check_id or None, "missing_fields": missing, "violations": violations})

    passed = (
        policy.get("schema_version") == "v2.7.0.static_policy_enforcement.v1"
        and bool(checks)
        and not malformed_checks
        and not duplicate_static_check_ids
        and not scan_errors
        and not forbidden_matches
    )
    return _contract(
        "StaticPolicyEnforcementContract",
        passed,
        "static_policy_missing_malformed_or_violated",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "static_check_count": len(checks),
            "scan_target_file_count": len(scan_target_files),
            "duplicate_static_check_ids": sorted(str(item) for item in duplicate_static_check_ids),
            "malformed_checks": malformed_checks,
            "scan_errors": scan_errors,
            "forbidden_match_count": len(forbidden_matches),
            "forbidden_matches": forbidden_matches,
        },
    )


def verify_api_response_contract(
    policy_path=DEFAULT_API_RESPONSE_POLICY,
    access_control_policy_path=DEFAULT_ACCESS_CONTROL_POLICY,
):
    try:
        policy = _load_json(policy_path)
        access_policy = _load_json(access_control_policy_path)
    except Exception as exc:
        return _contract("APIResponseContract", False, "api_response_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict) or not isinstance(access_policy, dict):
        return _contract(
            "APIResponseContract",
            False,
            "api_response_policy_not_object",
            {
                "policy_path": str(policy_path),
                "access_control_policy_path": str(access_control_policy_path),
            },
        )

    source_lines, source_error = _source_lines(policy.get("source_file") or access_policy.get("source_file"))
    if source_error:
        return _contract("APIResponseContract", False, "api_response_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(source_lines)
    routes = _extract_dashboard_routes(source_lines)
    route_by_endpoint = {route["endpoint"]: route for route in routes}
    overrides = {
        str(item.get("endpoint")): item
        for item in (access_policy.get("endpoint_overrides") or [])
        if isinstance(item, dict) and item.get("endpoint")
    }
    v27_evidence_endpoints = {
        endpoint
        for endpoint, item in overrides.items()
        if item.get("token_scope") == "v27:evidence_mutation"
    }

    response_policies = policy.get("response_policies") if isinstance(policy.get("response_policies"), list) else []
    malformed_policies = []
    duplicate_endpoints = []
    route_violations = []
    source_violations = []
    seen_endpoints = set()

    for index, item in enumerate(response_policies):
        if not isinstance(item, dict):
            malformed_policies.append({"index": index, "endpoint": None, "missing_fields": list(API_RESPONSE_REQUIRED_FIELDS), "violations": ["policy_not_object"]})
            continue
        endpoint = str(item.get("endpoint") or "")
        missing = _missing_required_fields(item, API_RESPONSE_REQUIRED_FIELDS)
        violations = []
        if endpoint in seen_endpoints:
            duplicate_endpoints.append(endpoint)
        if endpoint:
            seen_endpoints.add(endpoint)

        status_policy = item.get("status_code_policy")
        if not isinstance(status_policy, dict):
            violations.append("status_code_policy_not_object")
        else:
            if status_policy.get("accepted") != 202:
                violations.append("accepted_status_must_be_202")
            if status_policy.get("rejected") != 409:
                violations.append("rejected_status_must_be_409")
            if status_policy.get("method_not_allowed") != 405:
                violations.append("method_not_allowed_status_must_be_405")
            auth_failed = status_policy.get("auth_failed")
            if not isinstance(auth_failed, list) or sorted(int(value) for value in auth_failed) != [401, 403]:
                violations.append("auth_failed_statuses_must_be_401_403")
            if status_policy.get("audit_unavailable") != 500:
                violations.append("audit_unavailable_status_must_be_500")

        error_envelope = item.get("error_envelope")
        if not isinstance(error_envelope, dict):
            violations.append("error_envelope_not_object")
        else:
            if error_envelope.get("required") is not True:
                violations.append("error_envelope_required_must_be_true")
            if error_envelope.get("error_field") != "error":
                violations.append("error_field_must_be_error")
            if error_envelope.get("guard_errors") is not True:
                violations.append("guard_errors_must_be_true")
            if error_envelope.get("rejected_response_error_required") is not True:
                violations.append("rejected_response_error_required_must_be_true")

        if item.get("cache_control") != "no-store":
            violations.append("cache_control_must_be_no_store")

        route = route_by_endpoint.get(endpoint)
        if not route:
            route_violations.append({"endpoint": endpoint, "reason": "route_not_found"})
        else:
            if route.get("has_post_guard") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "post_guard_missing"})
            if route.get("has_check_auth") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "auth_guard_missing"})
            if route.get("has_audit_event") is not True:
                route_violations.append({"endpoint": endpoint, "reason": "audit_guard_missing"})
        access_override = overrides.get(endpoint)
        if not access_override:
            route_violations.append({"endpoint": endpoint, "reason": "access_policy_override_missing"})
        elif access_override.get("audit_log_required") is not True:
            route_violations.append({"endpoint": endpoint, "reason": "access_policy_audit_required_missing"})

        route_block = _dashboard_route_block(source_lines, endpoint)
        source_anchor = str(item.get("source_anchor") or "")
        response_schema_version = str(item.get("response_schema_version") or "")
        if source_anchor and source_anchor not in source_text:
            source_violations.append({"endpoint": endpoint, "reason": "source_anchor_missing", "source_anchor": source_anchor})
        if response_schema_version and response_schema_version not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "response_schema_version_missing_in_route"})
        if "buildV27ManualEvidenceApiResponse(" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "response_builder_missing"})
        if "apiJsonHeaders()" not in route_block and "apiJsonHeaders('no-store')" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "no_store_header_missing"})
        if "? 202 : 409" not in route_block:
            source_violations.append({"endpoint": endpoint, "reason": "accepted_rejected_status_branch_missing"})

        if missing or violations:
            malformed_policies.append({"index": index, "endpoint": endpoint or None, "missing_fields": missing, "violations": violations})

    policy_endpoints = {
        str(item.get("endpoint"))
        for item in response_policies
        if isinstance(item, dict) and item.get("endpoint")
    }
    uncovered_v27_evidence_endpoints = sorted(v27_evidence_endpoints - policy_endpoints)
    unknown_policy_endpoints = sorted(policy_endpoints - v27_evidence_endpoints)
    guard_helper_fragments = {
        "api_json_headers_default_no_store": "apiJsonHeaders(cacheControl = 'no-store')" in source_text,
        "api_json_headers_cache_control": "'Cache-Control': cacheControl" in source_text,
        "response_generated_at": "generated_at: generatedAt" in source_text,
        "response_schema_version": "response_schema_version: responseSchemaVersion" in source_text,
        "response_legacy_refresh_schema_version": "refresh_schema_version: responseSchemaVersion" in source_text,
        "response_materialized_false": "materialized: false" in source_text,
        "auth_403_no_store": "res.writeHead(403, apiJsonHeaders())" in source_text,
        "auth_401_no_store": "res.writeHead(401, apiJsonHeaders())" in source_text,
        "method_405_no_store": "res.writeHead(405, apiJsonHeaders())" in source_text,
        "audit_500_no_store": "res.writeHead(500, apiJsonHeaders())" in source_text and "Audit log unavailable" in source_text,
        "rejected_response_error": "payload.accepted === false && !payload.error" in source_text,
    }
    missing_guard_helper_fragments = sorted(key for key, present in guard_helper_fragments.items() if not present)

    passed = (
        policy.get("schema_version") == "v2.7.0.api_response_policy.v1"
        and bool(response_policies)
        and len(response_policies) >= 6
        and not malformed_policies
        and not duplicate_endpoints
        and not uncovered_v27_evidence_endpoints
        and not unknown_policy_endpoints
        and not route_violations
        and not source_violations
        and not missing_guard_helper_fragments
    )
    return _contract(
        "APIResponseContract",
        passed,
        "api_response_policy_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "access_control_policy_path": str(access_control_policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "source_file": policy.get("source_file") or access_policy.get("source_file"),
            "endpoint_count": len(response_policies),
            "endpoints": sorted(str(item.get("endpoint")) for item in response_policies if isinstance(item, dict) and item.get("endpoint")),
            "v27_evidence_endpoint_count": len(v27_evidence_endpoints),
            "uncovered_v27_evidence_endpoints": uncovered_v27_evidence_endpoints,
            "unknown_policy_endpoints": unknown_policy_endpoints,
            "duplicate_endpoints": sorted(str(item) for item in duplicate_endpoints),
            "malformed_policies": malformed_policies,
            "route_violations": route_violations,
            "source_violations": source_violations,
            "guard_helper_fragments": guard_helper_fragments,
            "missing_guard_helper_fragments": missing_guard_helper_fragments,
        },
    )


def _api_response_error_shape(payload):
    has_error = bool(payload.get("error") or payload.get("error_code") or payload.get("accepted") is False)
    return {
        "has_error": has_error,
        "accepted": None if "accepted" not in payload else bool(payload.get("accepted")),
        "error_field": "error" if payload.get("error") else None,
        "error_code": payload.get("error_code") or None,
        "status": payload.get("status") or None,
    }


def _build_api_response_envelope_sample(sample, envelope_version):
    result = sample.get("result") if isinstance(sample.get("result"), dict) else {}
    response_schema_version = str(sample.get("response_schema_version") or "")
    payload = {
        "generated_at": sample.get("generated_at"),
        "materialized": False,
        "endpoint": sample.get("endpoint"),
        "envelope_version": envelope_version,
        "response_schema_version": response_schema_version,
        "refresh_schema_version": response_schema_version,
        **result,
    }
    if payload.get("accepted") is False and not payload.get("error"):
        payload["error"] = payload.get("status") or "manual_evidence_request_rejected"
    if payload.get("accepted") is False and not payload.get("error_code"):
        payload["error_code"] = payload.get("error") or "manual_evidence_request_rejected"
    payload["error_shape"] = _api_response_error_shape(payload)
    payload["payload_hash"] = _sha256_json({key: value for key, value in payload.items() if key != "payload_hash"})
    return payload


def verify_api_response_envelope_contract(
    policy_path=DEFAULT_API_RESPONSE_ENVELOPE_POLICY,
):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_policy_not_object", {"policy_path": str(policy_path)})

    base_policy_path = _resolve_project_file(policy.get("base_response_policy_path")) or DEFAULT_API_RESPONSE_POLICY
    try:
        base_policy = _load_json(base_policy_path)
    except Exception as exc:
        return _contract(
            "APIResponseEnvelopeContract",
            False,
            "api_response_envelope_policy_missing_or_invalid",
            {
                "policy_path": str(policy_path),
                "base_response_policy_path": str(base_policy_path),
                "error": str(exc),
            },
        )
    if not isinstance(base_policy, dict):
        return _contract(
            "APIResponseEnvelopeContract",
            False,
            "api_response_envelope_policy_not_object",
            {
                "policy_path": str(policy_path),
                "base_response_policy_path": str(base_policy_path),
            },
        )

    source_lines, source_error = _source_lines(policy.get("source_file") or base_policy.get("source_file"))
    if source_error:
        return _contract("APIResponseEnvelopeContract", False, "api_response_envelope_source_missing", {"policy_path": str(policy_path), **source_error})
    source_text = "\n".join(source_lines)

    base_response_policies = base_policy.get("response_policies") if isinstance(base_policy.get("response_policies"), list) else []
    base_endpoints = {
        str(item.get("endpoint"))
        for item in base_response_policies
        if isinstance(item, dict) and item.get("endpoint")
    }
    response_envelopes = policy.get("response_envelopes") if isinstance(policy.get("response_envelopes"), list) else []
    envelope_version = str(policy.get("envelope_version") or "")
    required_fields = [str(item) for item in (policy.get("required_fields") or [])]

    schema_violations = []
    if set(required_fields) != set(API_RESPONSE_ENVELOPE_SPEC_FIELDS):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if policy.get("failure_action") != "api_envelope_invalid":
        schema_violations.append("failure_action_must_be_api_envelope_invalid")
    if policy.get("hash_algorithm") != "sha256_canonical_json_without_payload_hash":
        schema_violations.append("hash_algorithm_must_exclude_payload_hash")
    if not envelope_version:
        schema_violations.append("envelope_version_required")

    malformed_envelopes = []
    duplicate_endpoints = []
    source_violations = []
    seen_endpoints = set()
    for index, item in enumerate(response_envelopes):
        if not isinstance(item, dict):
            malformed_envelopes.append({"index": index, "endpoint": None, "missing_fields": list(API_RESPONSE_ENVELOPE_REQUIRED_FIELDS), "violations": ["envelope_policy_not_object"]})
            continue
        endpoint = str(item.get("endpoint") or "")
        missing = _missing_required_fields(item, API_RESPONSE_ENVELOPE_REQUIRED_FIELDS)
        violations = []
        if endpoint in seen_endpoints:
            duplicate_endpoints.append(endpoint)
        if endpoint:
            seen_endpoints.add(endpoint)
        if endpoint not in base_endpoints:
            violations.append("endpoint_not_in_base_response_policy")
        route_block = _dashboard_route_block(source_lines, endpoint)
        if not route_block:
            source_violations.append({"endpoint": endpoint, "reason": "route_not_found"})
        else:
            response_schema_version = str(item.get("response_schema_version") or "")
            source_anchor = str(item.get("source_anchor") or "")
            if response_schema_version and response_schema_version not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "response_schema_version_missing_in_route"})
            if source_anchor and source_anchor not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "source_anchor_missing", "source_anchor": source_anchor})
            if "{ endpoint: url.pathname }" not in route_block:
                source_violations.append({"endpoint": endpoint, "reason": "endpoint_binding_missing"})
        if missing or violations:
            malformed_envelopes.append({"index": index, "endpoint": endpoint or None, "missing_fields": missing, "violations": violations})

    policy_endpoints = {
        str(item.get("endpoint"))
        for item in response_envelopes
        if isinstance(item, dict) and item.get("endpoint")
    }
    uncovered_base_response_endpoints = sorted(base_endpoints - policy_endpoints)
    unknown_envelope_endpoints = sorted(policy_endpoints - base_endpoints)

    error_shape_policy = policy.get("error_shape") if isinstance(policy.get("error_shape"), dict) else {}
    error_shape_required_fields = [str(item) for item in (error_shape_policy.get("required_fields") or [])]
    error_shape_violations = []
    if set(error_shape_required_fields) != {"has_error", "accepted", "error_field", "error_code", "status"}:
        error_shape_violations.append("error_shape_required_fields_incomplete")
    if error_shape_policy.get("accepted_false_requires_error") is not True:
        error_shape_violations.append("accepted_false_requires_error_must_be_true")
    if error_shape_policy.get("accepted_false_requires_error_code") is not True:
        error_shape_violations.append("accepted_false_requires_error_code_must_be_true")

    malformed_samples = []
    sample_evidence = []
    sample_cases = policy.get("sample_cases") if isinstance(policy.get("sample_cases"), list) else []
    for index, sample in enumerate(sample_cases):
        if not isinstance(sample, dict):
            malformed_samples.append({"index": index, "sample_id": None, "violations": ["sample_not_object"]})
            continue
        sample_id = str(sample.get("sample_id") or "")
        violations = []
        missing = _missing_required_fields(sample, ("sample_id", "endpoint", "response_schema_version", "generated_at", "result", "expected_error_shape"))
        payload = _build_api_response_envelope_sample(sample, envelope_version)
        payload_missing_fields = [field for field in API_RESPONSE_ENVELOPE_SPEC_FIELDS if payload.get(field) in (None, "", [], {})]
        if payload_missing_fields:
            violations.append("payload_missing_required_fields")
        expected_error_shape = sample.get("expected_error_shape")
        if expected_error_shape != payload.get("error_shape"):
            violations.append("expected_error_shape_mismatch")
        if not re.fullmatch(r"[a-f0-9]{64}", str(payload.get("payload_hash") or "")):
            violations.append("payload_hash_invalid")
        if payload.get("payload_hash") != _sha256_json({key: value for key, value in payload.items() if key != "payload_hash"}):
            violations.append("payload_hash_mismatch")
        sample_evidence.append(
            {
                "sample_id": sample_id,
                "endpoint": payload.get("endpoint"),
                "envelope_version": payload.get("envelope_version"),
                "payload_hash": payload.get("payload_hash"),
                "error_shape": payload.get("error_shape"),
                "generated_at": payload.get("generated_at"),
            }
        )
        if missing or violations:
            malformed_samples.append({"index": index, "sample_id": sample_id or None, "missing_fields": missing, "violations": violations, "payload_missing_fields": payload_missing_fields})

    helper_fragments = {
        "envelope_version_constant": f"V27_API_RESPONSE_ENVELOPE_VERSION = '{envelope_version}'" in source_text,
        "endpoint_field": "endpoint: options.endpoint || null" in source_text,
        "envelope_version_field": "envelope_version: V27_API_RESPONSE_ENVELOPE_VERSION" in source_text,
        "error_shape_helper": "function buildApiResponseErrorShape(payload = {})" in source_text,
        "payload_hash_helper": "function apiEnvelopePayloadForHash(payload = {})" in source_text,
        "payload_hash_excludes_self": "const { payload_hash, ...unsignedPayload } = payload || {};" in source_text,
        "payload_hash_assignment": "payload.payload_hash = auditSha256Hex(apiEnvelopePayloadForHash(payload));" in source_text,
    }
    missing_helper_fragments = sorted(key for key, present in helper_fragments.items() if not present)

    passed = (
        policy.get("schema_version") == "v2.7.0.api_response_envelope_policy.v1"
        and bool(response_envelopes)
        and bool(sample_cases)
        and not schema_violations
        and not error_shape_violations
        and not malformed_envelopes
        and not duplicate_endpoints
        and not uncovered_base_response_endpoints
        and not unknown_envelope_endpoints
        and not source_violations
        and not malformed_samples
        and not missing_helper_fragments
    )
    return _contract(
        "APIResponseEnvelopeContract",
        passed,
        "api_response_envelope_policy_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "base_response_policy_path": str(base_policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "envelope_version": envelope_version,
            "hash_algorithm": policy.get("hash_algorithm"),
            "required_fields": required_fields,
            "endpoint_count": len(response_envelopes),
            "base_response_endpoint_count": len(base_endpoints),
            "sample_case_count": len(sample_cases),
            "sample_evidence": sample_evidence,
            "schema_violations": schema_violations,
            "error_shape_violations": error_shape_violations,
            "duplicate_endpoints": sorted(str(item) for item in duplicate_endpoints),
            "malformed_envelopes": malformed_envelopes,
            "uncovered_base_response_endpoints": uncovered_base_response_endpoints,
            "unknown_envelope_endpoints": unknown_envelope_endpoints,
            "source_violations": source_violations,
            "malformed_samples": malformed_samples,
            "helper_fragments": helper_fragments,
            "missing_helper_fragments": missing_helper_fragments,
        },
    )


def _read_project_text(path):
    resolved = _resolve_project_file(path)
    if not resolved or not resolved.exists():
        return "", {"source_file": str(path), "reason": "source_missing"}
    try:
        return resolved.read_text(encoding="utf-8"), None
    except UnicodeDecodeError as exc:
        return "", {"source_file": str(path), "reason": "source_decode_failed", "error": str(exc)}


def _extract_basic_readiness_error_codes(source_text):
    codes = set(
        re.findall(
            r"_contract\(\s*['\"][^'\"]+['\"]\s*,\s*[^,]+,\s*['\"]([^'\"]+)['\"]",
            source_text,
            flags=re.S,
        )
    )
    codes.update(re.findall(r"reason\s+or\s+['\"]([^'\"]+)['\"]", source_text))
    return codes


def _extract_dashboard_error_codes(source_text):
    codes = set(re.findall(r"error_code\s*:\s*['\"]([^'\"]+)['\"]", source_text))
    if "manual_evidence_request_rejected" in source_text:
        codes.add("manual_evidence_request_rejected")
    codes.update(
        re.findall(
            r"accepted\s*:\s*false\s*,[\s\S]{0,180}?status\s*:\s*['\"]([^'\"]+)['\"]",
            source_text,
        )
    )
    return codes


def _extract_paper_mode_error_codes(source_text):
    return set(
        re.findall(
            r"['\"](paper_[a-z0-9_]*(?:detected|missing|unverified))['\"]",
            source_text,
        )
    )


def verify_error_taxonomy(
    taxonomy_path=DEFAULT_ERROR_TAXONOMY,
    dashboard_source_path=None,
    basic_readiness_source_path=None,
    paper_mode_safety_source_path=None,
):
    try:
        taxonomy = _load_json(taxonomy_path)
    except Exception as exc:
        return _contract("ErrorTaxonomyContract", False, "error_taxonomy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(taxonomy, dict):
        return _contract("ErrorTaxonomyContract", False, "error_taxonomy_not_object", {"taxonomy_path": str(taxonomy_path)})

    coverage = taxonomy.get("coverage") if isinstance(taxonomy.get("coverage"), dict) else {}
    dashboard_source_path = dashboard_source_path or coverage.get("dashboard_source_file") or "src/web/dashboard-server.js"
    basic_readiness_source_path = basic_readiness_source_path or coverage.get("basic_readiness_source_file") or "scripts/v27_basic_contract_readiness.py"
    paper_mode_safety_source_path = paper_mode_safety_source_path or coverage.get("paper_mode_safety_source_file") or "scripts/v27_paper_mode_safety.py"

    source_errors = []
    dashboard_text, source_error = _read_project_text(dashboard_source_path)
    if source_error:
        source_errors.append({"source": "dashboard", **source_error})
    basic_text, source_error = _read_project_text(basic_readiness_source_path)
    if source_error:
        source_errors.append({"source": "basic_readiness", **source_error})
    paper_text, source_error = _read_project_text(paper_mode_safety_source_path)
    if source_error:
        source_errors.append({"source": "paper_mode_safety", **source_error})

    observed_by_source = {
        "dashboard_api_error_codes": sorted(_extract_dashboard_error_codes(dashboard_text)),
        "basic_readiness_blocking_reasons": sorted(_extract_basic_readiness_error_codes(basic_text)),
        "paper_mode_safety_reasons": sorted(_extract_paper_mode_error_codes(paper_text)),
    }
    required_codes = set()
    for codes in observed_by_source.values():
        required_codes.update(codes)

    allowed_categories = set(str(item) for item in (taxonomy.get("allowed_categories") or []))
    allowed_severities = set(str(item) for item in (taxonomy.get("allowed_severities") or []))
    entries = taxonomy.get("taxonomy") if isinstance(taxonomy.get("taxonomy"), list) else []
    malformed_entries = []
    duplicate_error_codes = []
    taxonomy_codes = set()
    seen_codes = set()
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            malformed_entries.append({"index": index, "error_code": None, "missing_fields": list(ERROR_TAXONOMY_REQUIRED_FIELDS), "violations": ["entry_not_object"]})
            continue
        error_code = str(entry.get("error_code") or "")
        missing = _missing_required_fields(entry, ERROR_TAXONOMY_REQUIRED_FIELDS)
        violations = []
        if error_code in seen_codes:
            duplicate_error_codes.append(error_code)
        if error_code:
            seen_codes.add(error_code)
            taxonomy_codes.add(error_code)
        if error_code and not re.match(r"^[a-z][a-z0-9_]*$", error_code):
            violations.append("error_code_must_be_lower_snake_case")
        if str(entry.get("category") or "") not in allowed_categories:
            violations.append("category_not_allowed")
        if str(entry.get("severity") or "") not in allowed_severities:
            violations.append("severity_not_allowed")
        if entry.get("introduced_at") and _parse_iso_ts(entry.get("introduced_at")) is None:
            violations.append("introduced_at_invalid_iso_timestamp")
        if missing or violations:
            malformed_entries.append({"index": index, "error_code": error_code or None, "missing_fields": missing, "violations": violations})

    unclassified_error_codes = sorted(required_codes - taxonomy_codes)
    unused_taxonomy_codes = sorted(taxonomy_codes - required_codes)
    passed = (
        taxonomy.get("schema_version") == "v2.7.0.error_taxonomy.v1"
        and bool(entries)
        and bool(allowed_categories)
        and bool(allowed_severities)
        and not source_errors
        and not malformed_entries
        and not duplicate_error_codes
        and not unclassified_error_codes
        and not unused_taxonomy_codes
    )
    return _contract(
        "ErrorTaxonomyContract",
        passed,
        "error_taxonomy_missing_malformed_or_incomplete",
        {
            "taxonomy_path": str(taxonomy_path),
            "schema_version": taxonomy.get("schema_version"),
            "scope": taxonomy.get("scope"),
            "failure_action": taxonomy.get("failure_action"),
            "taxonomy_entry_count": len(entries),
            "required_error_code_count": len(required_codes),
            "observed_by_source": observed_by_source,
            "duplicate_error_codes": sorted(str(item) for item in duplicate_error_codes),
            "malformed_entries": malformed_entries,
            "source_errors": source_errors,
            "unclassified_error_codes": unclassified_error_codes,
            "unused_taxonomy_codes": unused_taxonomy_codes,
        },
    )


def _apply_log_redaction_patterns(raw, patterns):
    text = str(raw)
    for pattern in patterns:
        regex = pattern.get("regex") if isinstance(pattern, dict) else None
        replacement = pattern.get("replacement") if isinstance(pattern, dict) else None
        if not regex or replacement is None:
            continue
        text = re.sub(str(regex), str(replacement), text, flags=re.IGNORECASE)
    return text


def verify_log_redaction_verification(policy_path=DEFAULT_LOG_REDACTION_POLICY):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("LogRedactionVerificationContract", False, "log_redaction_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("LogRedactionVerificationContract", False, "log_redaction_policy_not_object", {"policy_path": str(policy_path)})

    pattern_set = policy.get("secret_pattern_set") if isinstance(policy.get("secret_pattern_set"), dict) else {}
    secret_pattern_set = str(pattern_set.get("secret_pattern_set") or "")
    patterns = pattern_set.get("patterns") if isinstance(pattern_set.get("patterns"), list) else []
    sample_cases = {
        str(item.get("sample_id")): item
        for item in (policy.get("sample_cases") or [])
        if isinstance(item, dict) and item.get("sample_id")
    }
    streams = policy.get("streams") if isinstance(policy.get("streams"), list) else []

    malformed_patterns = []
    duplicate_pattern_ids = []
    seen_pattern_ids = set()
    for index, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            malformed_patterns.append({"index": index, "pattern_id": None, "violations": ["pattern_not_object"]})
            continue
        pattern_id = str(pattern.get("pattern_id") or "")
        violations = []
        if pattern_id in seen_pattern_ids:
            duplicate_pattern_ids.append(pattern_id)
        if pattern_id:
            seen_pattern_ids.add(pattern_id)
        if not pattern_id:
            violations.append("pattern_id_required")
        if not pattern.get("regex"):
            violations.append("regex_required")
        else:
            try:
                re.compile(str(pattern.get("regex")), flags=re.IGNORECASE)
            except re.error as exc:
                violations.append(f"regex_invalid:{exc}")
        if pattern.get("replacement") is None:
            violations.append("replacement_required")
        if violations:
            malformed_patterns.append({"index": index, "pattern_id": pattern_id or None, "violations": violations})

    malformed_samples = []
    sample_results = {}
    for sample_id, sample in sample_cases.items():
        raw = str(sample.get("raw") or "")
        redacted = _apply_log_redaction_patterns(raw, patterns)
        absent_failures = [
            fragment for fragment in (sample.get("expected_fragments_absent") or [])
            if str(fragment) and str(fragment) in redacted
        ]
        present_failures = [
            fragment for fragment in (sample.get("expected_fragments_present") or [])
            if str(fragment) and str(fragment) not in redacted
        ]
        redaction_passed = not absent_failures and not present_failures and redacted != raw
        if not raw or not redaction_passed:
            malformed_samples.append(
                {
                    "sample_id": sample_id,
                    "absent_failures": absent_failures,
                    "present_failures": present_failures,
                    "raw_present": bool(raw),
                    "redaction_changed_sample": redacted != raw,
                }
            )
        sample_results[sample_id] = {
            "sample_hash": _sha256_json({"sample_id": sample_id, "raw": raw}),
            "redaction_passed": redaction_passed,
        }

    checked_at = _utc_now_iso()
    malformed_streams = []
    source_violations = []
    stream_evidence = []
    for index, stream in enumerate(streams):
        if not isinstance(stream, dict):
            malformed_streams.append({"index": index, "log_stream": None, "missing_fields": list(LOG_REDACTION_STREAM_REQUIRED_FIELDS), "violations": ["stream_not_object"]})
            continue
        log_stream = str(stream.get("log_stream") or "")
        missing = _missing_required_fields(stream, LOG_REDACTION_STREAM_REQUIRED_FIELDS)
        violations = []
        if stream.get("secret_pattern_set") != secret_pattern_set:
            violations.append("secret_pattern_set_mismatch")
        sample_case_ids = [str(item) for item in (stream.get("sample_case_ids") or [])]
        unknown_samples = sorted(sample_id for sample_id in sample_case_ids if sample_id not in sample_cases)
        if unknown_samples:
            violations.append("unknown_sample_case_ids")
        stream_sample_passed = all(sample_results.get(sample_id, {}).get("redaction_passed") for sample_id in sample_case_ids)
        sample_hash = _sha256_json(
            {
                "log_stream": log_stream,
                "secret_pattern_set": stream.get("secret_pattern_set"),
                "sample_hashes": [sample_results.get(sample_id, {}).get("sample_hash") for sample_id in sample_case_ids],
            }
        )

        source_text, source_error = _read_project_text(stream.get("source_file"))
        if source_error:
            source_violations.append({"log_stream": log_stream, **source_error})
        else:
            redaction_anchor = str(stream.get("redaction_anchor") or "")
            write_anchor = str(stream.get("write_anchor") or "")
            if redaction_anchor and redaction_anchor not in source_text:
                source_violations.append({"log_stream": log_stream, "reason": "redaction_anchor_missing", "redaction_anchor": redaction_anchor})
            if write_anchor and write_anchor not in source_text:
                source_violations.append({"log_stream": log_stream, "reason": "write_anchor_missing", "write_anchor": write_anchor})
            if log_stream == "v27_manual_evidence_child_process_logs":
                raw_write_count = source_text.count("logStream.write(")
                if raw_write_count != 1 or "logStream.write(redactLogMessage(chunk));" not in source_text:
                    source_violations.append({"log_stream": log_stream, "reason": "raw_log_stream_write_bypass", "raw_log_stream_write_count": raw_write_count})

        redaction_passed = stream_sample_passed and not unknown_samples and not missing and not violations
        stream_evidence.append(
            {
                "log_stream": log_stream,
                "secret_pattern_set": stream.get("secret_pattern_set"),
                "sample_hash": sample_hash,
                "redaction_passed": redaction_passed,
                "checked_at": checked_at,
                "sample_case_ids": sample_case_ids,
            }
        )
        if missing or violations:
            malformed_streams.append({"index": index, "log_stream": log_stream or None, "missing_fields": missing, "violations": violations, "unknown_sample_case_ids": unknown_samples})

    passed = (
        policy.get("schema_version") == "v2.7.0.log_redaction_policy.v1"
        and bool(secret_pattern_set)
        and bool(patterns)
        and bool(sample_cases)
        and bool(streams)
        and not malformed_patterns
        and not duplicate_pattern_ids
        and not malformed_samples
        and not malformed_streams
        and not source_violations
        and all(item.get("redaction_passed") for item in stream_evidence)
    )
    return _contract(
        "LogRedactionVerificationContract",
        passed,
        "log_redaction_verification_missing_malformed_or_failed",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "secret_pattern_set": secret_pattern_set,
            "pattern_count": len(patterns),
            "sample_case_count": len(sample_cases),
            "stream_count": len(streams),
            "streams": stream_evidence,
            "malformed_patterns": malformed_patterns,
            "duplicate_pattern_ids": sorted(str(item) for item in duplicate_pattern_ids),
            "malformed_samples": malformed_samples,
            "malformed_streams": malformed_streams,
            "source_violations": source_violations,
        },
    )


def verify_service_readiness_probe_contract(policy_path=DEFAULT_SERVICE_READINESS_PROBES):
    try:
        policy = _load_json(policy_path)
    except Exception as exc:
        return _contract("ServiceReadinessProbeContract", False, "service_readiness_policy_missing_or_invalid", {"error": str(exc)})
    if not isinstance(policy, dict):
        return _contract("ServiceReadinessProbeContract", False, "service_readiness_policy_not_object", {"policy_path": str(policy_path)})

    probes = policy.get("probes") if isinstance(policy.get("probes"), list) else []
    required_probe_ids = [str(item) for item in (policy.get("required_probe_ids") or [])]
    required_fields = [str(item) for item in (policy.get("required_fields") or [])]
    checked_at = _utc_now_iso()

    schema_violations = []
    if policy.get("schema_version") != "v2.7.0.service_readiness_probes.v1":
        schema_violations.append("schema_version_invalid")
    if policy.get("failure_action") != "service_not_ready":
        schema_violations.append("failure_action_must_be_service_not_ready")
    if set(required_fields) != set(SERVICE_READINESS_CONTRACT_FIELDS):
        schema_violations.append("required_fields_must_match_contract_catalog")
    if not required_probe_ids:
        schema_violations.append("required_probe_ids_required")

    malformed_probes = []
    duplicate_probe_ids = []
    source_violations = []
    seen_probe_ids = set()
    probe_ids = set()
    probe_evidence = []
    for index, probe in enumerate(probes):
        if not isinstance(probe, dict):
            malformed_probes.append({"index": index, "probe_id": None, "missing_fields": list(SERVICE_READINESS_PROBE_REQUIRED_FIELDS), "violations": ["probe_not_object"]})
            continue

        service_name = str(probe.get("service_name") or "")
        probe_id = str(probe.get("probe_id") or "")
        probe_ids.add(probe_id)
        missing = _missing_required_fields(probe, SERVICE_READINESS_PROBE_REQUIRED_FIELDS)
        violations = []
        if probe_id in seen_probe_ids:
            duplicate_probe_ids.append(probe_id)
        if probe_id:
            seen_probe_ids.add(probe_id)
        if str(probe.get("health_status") or "") not in {"ready", "degraded", "blocked"}:
            violations.append("health_status_invalid")
        dependency_status = probe.get("dependency_status")
        if not isinstance(dependency_status, dict) or not dependency_status:
            violations.append("dependency_status_required")
        dependency_anchors = [str(item) for item in (probe.get("dependency_anchors") or [])]
        if not dependency_anchors:
            violations.append("dependency_anchors_required")

        source_text, source_error = _read_project_text(probe.get("source_file"))
        if source_error:
            source_violations.append({"probe_id": probe_id, **source_error})
        else:
            source_anchor = str(probe.get("source_anchor") or "")
            if source_anchor and source_anchor not in source_text:
                source_violations.append({"probe_id": probe_id, "reason": "source_anchor_missing", "source_anchor": source_anchor})
            missing_dependency_anchors = [anchor for anchor in dependency_anchors if anchor not in source_text]
            if missing_dependency_anchors:
                source_violations.append({"probe_id": probe_id, "reason": "dependency_anchor_missing", "missing_dependency_anchors": missing_dependency_anchors})
            endpoint = probe.get("endpoint")
            if endpoint:
                source_lines = source_text.splitlines()
                route_block = _dashboard_route_block(source_lines, str(endpoint))
                if not route_block:
                    source_violations.append({"probe_id": probe_id, "endpoint": endpoint, "reason": "endpoint_route_missing"})
                elif source_anchor and source_anchor not in route_block:
                    source_violations.append({"probe_id": probe_id, "endpoint": endpoint, "reason": "source_anchor_missing_in_route", "source_anchor": source_anchor})

        probe_evidence.append(
            {
                "service_name": service_name,
                "probe_id": probe_id,
                "health_status": probe.get("health_status"),
                "dependency_status": dependency_status,
                "checked_at": checked_at,
            }
        )
        if missing or violations:
            malformed_probes.append({"index": index, "service_name": service_name or None, "probe_id": probe_id or None, "missing_fields": missing, "violations": violations})

    missing_required_probe_ids = sorted(set(required_probe_ids) - probe_ids)
    unexpected_probe_ids = sorted(probe_ids - set(required_probe_ids))
    passed = (
        not schema_violations
        and bool(probes)
        and not malformed_probes
        and not duplicate_probe_ids
        and not source_violations
        and not missing_required_probe_ids
        and not unexpected_probe_ids
        and all(item.get("health_status") == "ready" for item in probe_evidence)
    )
    return _contract(
        "ServiceReadinessProbeContract",
        passed,
        "service_readiness_probe_missing_malformed_or_unenforced",
        {
            "policy_path": str(policy_path),
            "schema_version": policy.get("schema_version"),
            "scope": policy.get("scope"),
            "failure_action": policy.get("failure_action"),
            "required_fields": required_fields,
            "probe_count": len(probes),
            "required_probe_ids": required_probe_ids,
            "probes": probe_evidence,
            "schema_violations": schema_violations,
            "malformed_probes": malformed_probes,
            "duplicate_probe_ids": sorted(str(item) for item in duplicate_probe_ids),
            "source_violations": source_violations,
            "missing_required_probe_ids": missing_required_probe_ids,
            "unexpected_probe_ids": unexpected_probe_ids,
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
    direct_db_mutation_policy_path=DEFAULT_DIRECT_DB_MUTATION_POLICY,
    background_job_registry_path=DEFAULT_BACKGROUND_JOB_REGISTRY,
    entry_point_inventory_path=DEFAULT_ENTRY_POINT_INVENTORY,
    static_policy_path=DEFAULT_STATIC_POLICY_ENFORCEMENT,
    api_response_policy_path=DEFAULT_API_RESPONSE_POLICY,
    api_response_envelope_policy_path=DEFAULT_API_RESPONSE_ENVELOPE_POLICY,
    error_taxonomy_path=DEFAULT_ERROR_TAXONOMY,
    log_redaction_policy_path=DEFAULT_LOG_REDACTION_POLICY,
    service_readiness_policy_path=DEFAULT_SERVICE_READINESS_PROBES,
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
            verify_audit_log_integrity(policy_path=access_control_policy_path),
            verify_write_path_registry(registry_path=write_path_registry_path),
            verify_direct_database_mutation_ban(
                policy_path=direct_db_mutation_policy_path,
                registry_path=write_path_registry_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_background_job_registry(registry_path=background_job_registry_path),
            verify_entry_point_inventory(
                inventory_path=entry_point_inventory_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_static_policy_enforcement(policy_path=static_policy_path),
            verify_api_response_contract(
                policy_path=api_response_policy_path,
                access_control_policy_path=access_control_policy_path,
            ),
            verify_api_response_envelope_contract(policy_path=api_response_envelope_policy_path),
            verify_error_taxonomy(taxonomy_path=error_taxonomy_path),
            verify_log_redaction_verification(policy_path=log_redaction_policy_path),
            verify_service_readiness_probe_contract(policy_path=service_readiness_policy_path),
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
    parser.add_argument("--direct-db-mutation-policy", default=str(DEFAULT_DIRECT_DB_MUTATION_POLICY))
    parser.add_argument("--background-job-registry", default=str(DEFAULT_BACKGROUND_JOB_REGISTRY))
    parser.add_argument("--entry-point-inventory", default=str(DEFAULT_ENTRY_POINT_INVENTORY))
    parser.add_argument("--static-policy", default=str(DEFAULT_STATIC_POLICY_ENFORCEMENT))
    parser.add_argument("--api-response-policy", default=str(DEFAULT_API_RESPONSE_POLICY))
    parser.add_argument("--api-response-envelope-policy", default=str(DEFAULT_API_RESPONSE_ENVELOPE_POLICY))
    parser.add_argument("--error-taxonomy", default=str(DEFAULT_ERROR_TAXONOMY))
    parser.add_argument("--log-redaction-policy", default=str(DEFAULT_LOG_REDACTION_POLICY))
    parser.add_argument("--service-readiness-policy", default=str(DEFAULT_SERVICE_READINESS_PROBES))
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
        direct_db_mutation_policy_path=Path(args.direct_db_mutation_policy),
        background_job_registry_path=Path(args.background_job_registry),
        entry_point_inventory_path=Path(args.entry_point_inventory),
        static_policy_path=Path(args.static_policy),
        api_response_policy_path=Path(args.api_response_policy),
        api_response_envelope_policy_path=Path(args.api_response_envelope_policy),
        error_taxonomy_path=Path(args.error_taxonomy),
        log_redaction_policy_path=Path(args.log_redaction_policy),
        service_readiness_policy_path=Path(args.service_readiness_policy),
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    if args.strict and report["blocking_contracts"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
