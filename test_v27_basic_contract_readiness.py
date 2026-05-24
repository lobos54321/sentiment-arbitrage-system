import sys
import json

sys.path.insert(0, "scripts")

from v27_basic_contract_readiness import (  # noqa: E402
    build_basic_contract_readiness,
    verify_api_response_contract,
    verify_api_response_envelope_contract,
    verify_audit_log_integrity,
    verify_background_job_registry,
    verify_direct_database_mutation_ban,
    verify_entry_point_inventory,
    verify_error_taxonomy,
    verify_evidence_eligibility_matrix,
    verify_log_redaction_verification,
    verify_static_policy_enforcement,
    verify_access_control_policy,
    verify_input_sanitization,
    verify_paper_mode_safety,
    verify_project_stop_loss,
    verify_safe_default,
    verify_safety_case,
    verify_top_fix_queue,
    verify_waiver_policy,
    verify_write_path_registry,
)


def test_basic_contract_readiness_passes_seed_foundation():
    report = build_basic_contract_readiness(env={})

    assert report["health"]["status"] == "basic_contract_readiness_ok"
    assert report["blocking_contracts"] == []
    for contract_id in (
        "SpecConsistencyLinterContract",
        "PaperModeSafetyBoundary",
        "ChainConfigContract",
        "SourceRegistryContract",
        "InputSanitizationContract",
        "SafeDefaultContract",
        "ProjectStopLossContract",
        "EvidenceEligibilityMatrix",
        "TopFixQueueContract",
        "SafetyCaseContract",
        "WaiverPolicyContract",
        "AccessControlContract",
        "AuditLogIntegrityContract",
        "WritePathRegistryContract",
        "DirectDatabaseMutationBan",
        "BackgroundJobRegistryContract",
        "EntryPointInventoryContract",
        "StaticPolicyEnforcementContract",
        "APIResponseContract",
        "APIResponseEnvelopeContract",
        "ErrorTaxonomyContract",
        "LogRedactionVerificationContract",
    ):
        assert report["contracts"][contract_id]["status"] == "pass"


def test_paper_mode_safety_blocks_live_capabilities():
    report = verify_paper_mode_safety(
        env={
            "PREMIUM_LIVE_EXECUTION_ENABLED": "true",
            "TRADE_WALLET_PRIVATE_KEY": "secret",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert report["evidence"]["premium_live_execution_enabled"] is True
    assert report["evidence"]["live_private_key_present"] is True


def test_paper_mode_safety_consumes_clean_runtime_evidence(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "pass"
    assert report["evidence"]["runtime_evidence_present"] is True
    assert report["evidence"]["runtime_evidence_valid"] is True


def test_paper_mode_safety_allows_quarantined_live_secret_marker(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_secret_quarantine_applied": True,
                "live_secret_quarantine_reason": "node_preload_before_app_import",
                "quarantined_live_secret_names": ["TRADE_WALLET_PRIVATE_KEY"],
                "live_secret_quarantine_hash": "hash",
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(
        env={
            "V27_LIVE_SECRET_QUARANTINE_APPLIED": "true",
            "V27_LIVE_SECRET_QUARANTINE_REASON": "node_preload_before_app_import",
            "V27_QUARANTINED_LIVE_SECRET_NAMES": "TRADE_WALLET_PRIVATE_KEY",
            "V27_LIVE_SECRET_QUARANTINE_HASH": "hash",
        },
        runtime_evidence_path=evidence_path,
    )

    assert report["status"] == "pass"
    assert report["evidence"]["live_private_key_present"] is False
    assert report["evidence"]["live_secret_quarantine_applied"] is True
    assert report["evidence"]["quarantined_live_secret_names"] == ["TRADE_WALLET_PRIVATE_KEY"]
    assert report["evidence"]["runtime_evidence"]["live_secret_quarantine_applied"] is True


def test_paper_mode_safety_blocks_runtime_live_component(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": True,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert "runtime_jupiter_executor_initialized" in report["evidence"]["violations"]


def test_input_sanitization_redacts_raw_telegram_text():
    report = verify_input_sanitization()

    assert report["status"] == "pass"
    assert report["evidence"]["payload_schema_valid"] is True
    assert report["evidence"]["raw_message_hash_present"] is True
    assert report["evidence"]["legacy_raw_message_leaked"] is False


def test_safe_default_requires_blocked_shadow_defaults(tmp_path):
    registry_path = tmp_path / "entry-mode-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "tiers": {"live": {"paper_enabled": True}},
                "modes": {
                    "unit_live": {
                        "tier": "live",
                        "paper_enabled": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report = verify_safe_default(registry_path=registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "safe_default_fail_closed_unverified"
    assert report["evidence"]["blocked_mode_count"] == 0
    assert report["evidence"]["default_action"] == "fail_closed"


def test_project_stop_loss_blocks_when_auto_kill_disabled():
    report = verify_project_stop_loss(
        env={
            "ENTRY_MODE_QUALITY_AUTO_KILL_SWITCH_ENABLED": "false",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "project_stop_loss_unverified_or_disabled"
    assert report["evidence"]["auto_kill_switch_enabled"] is False
    assert report["evidence"]["action"]["stop_automatic_entry"] is True


def test_project_stop_loss_passes_default_thresholds():
    report = verify_project_stop_loss(env={})

    assert report["status"] == "pass"
    assert report["evidence"]["scope"] == "entry_mode"
    assert report["evidence"]["stop_criteria"]["negative_ev_min_samples"] == 20
    assert report["evidence"]["action"]["action"] == "downgrade_to_watch_only"


def test_governance_readiness_contracts_pass_seed_artifact():
    assert verify_evidence_eligibility_matrix()["status"] == "pass"
    assert verify_top_fix_queue()["status"] == "pass"
    assert verify_safety_case()["status"] == "pass"
    assert verify_waiver_policy()["status"] == "pass"


def write_access_policy(path, source_file, overrides=None):
    path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.access_control_policy.v1",
                "source_file": str(source_file),
                "public_endpoints": ["/", "/health", "/ping", "/dashboard"],
                "protected_defaults": {
                    "required_role": "dashboard_reader",
                    "token_scope": "dashboard:read",
                    "audit_log_required": False,
                    "danger_level": "read",
                },
                "danger_levels_requiring_post": ["operator_mutation", "admin_mutation", "critical"],
                "danger_levels_requiring_audit": ["operator_mutation", "admin_mutation", "critical"],
                "endpoint_overrides": overrides or [],
                "dynamic_protected_routes": [],
            }
        ),
        encoding="utf-8",
    )


def write_write_registry(path, endpoints=None):
    path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "write_paths": [
                    {
                        "write_path_id": f"unit.write.{index}",
                        "entry_point": f"POST {endpoint}",
                    }
                    for index, endpoint in enumerate(endpoints or [], start=1)
                ],
            }
        ),
        encoding="utf-8",
    )


def test_access_control_policy_covers_dashboard_routes_and_mutations():
    report = verify_access_control_policy()

    assert report["status"] == "pass"
    assert report["evidence"]["protected_route_count"] == 58
    assert report["evidence"]["mutation_policy_count"] == 12
    assert report["evidence"]["write_path_endpoint_count"] == 6
    assert report["evidence"]["unauthenticated_routes"] == []
    assert report["evidence"]["mutation_without_post_guard"] == []
    assert report["evidence"]["write_path_policy_gaps"] == []


def test_audit_log_integrity_covers_required_mutation_routes():
    report = verify_audit_log_integrity()

    assert report["status"] == "pass"
    assert report["evidence"]["audit_required_endpoint_count"] == 12
    assert report["evidence"]["missing_audit_hooks"] == []
    assert report["evidence"]["helper_required_fragments"]["sha256_hashing"] is True
    assert report["evidence"]["chain_field_presence"]["audit_chain_hash"] is True


def test_access_control_policy_blocks_unprotected_dashboard_route(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
if (url.pathname === '/api/open') {
  res.end('ok');
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    registry_path = tmp_path / "write-registry.json"
    write_access_policy(policy_path, source_path)
    write_write_registry(registry_path)

    report = verify_access_control_policy(policy_path, registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "access_control_policy_missing_malformed_or_incomplete"
    assert report["evidence"]["unauthenticated_routes"] == [{"endpoint": "/api/open", "line": 9}]


def test_access_control_policy_blocks_mutation_without_post_guard(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
if (url.pathname === '/api/mutate') {
  if (!checkAuth(req, url, res)) return;
  db.prepare('UPDATE demo SET value = 1').run();
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    registry_path = tmp_path / "write-registry.json"
    write_access_policy(
        policy_path,
        source_path,
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "critical",
                "allowed_methods": ["POST"],
                "method_guard_required": True,
            }
        ],
    )
    write_write_registry(registry_path, ["/api/mutate"])

    report = verify_access_control_policy(policy_path, registry_path)

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["mutation_without_post_guard"] == [
        {"endpoint": "/api/mutate", "line": 9, "danger_level": "critical"}
    ]
    assert report["evidence"]["write_path_policy_gaps"][0]["endpoint"] == "/api/mutate"


def test_audit_log_integrity_blocks_missing_audit_hook(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text(
        """
import { createHash } from 'crypto';
const DASHBOARD_AUDIT_SCHEMA_VERSION = 'v2.7.0.audit_log_integrity.v1';
function verifyDashboardAuditChain(events) { return events; }
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) { res.writeHead(403); return false; }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) { res.writeHead(401); return false; }
  return true;
}
function writeAudit(event) {
  createHash('sha256').update(event.audit_event_id + event.prev_audit_hash + event.audit_payload_hash + event.audit_chain_hash + event.created_at);
  fs.appendFileSync(auditLogPath, JSON.stringify(event));
}
function requireDashboardAuditEvent(req, res, url, input) {
  res.end('Audit log unavailable');
}
if (url.pathname === '/api/mutate') {
  if (req.method !== 'POST') return;
  if (!checkAuth(req, url, res)) return;
  db.prepare('UPDATE demo SET value = 1').run();
}
""",
        encoding="utf-8",
    )
    policy_path = tmp_path / "access-policy.json"
    write_access_policy(
        policy_path,
        source_path,
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "critical",
            }
        ],
    )

    report = verify_audit_log_integrity(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["missing_audit_hooks"] == [
        {"endpoint": "/api/mutate", "registered_route": True, "has_audit_event": False}
    ]


def test_write_path_registry_covers_dashboard_write_paths():
    report = verify_write_path_registry()

    assert report["status"] == "pass"
    assert report["evidence"]["scan_target_count"] == 1
    assert report["evidence"]["scanned_mutation_count"] == 9
    assert report["evidence"]["registered_write_path_count"] == 9
    assert report["evidence"]["unregistered_mutation_count"] == 0
    assert "sqlite:paper_trades" in report["evidence"]["registered_targets"]


def test_direct_database_mutation_ban_covers_break_glass_paths():
    report = verify_direct_database_mutation_ban()

    assert report["status"] == "pass"
    assert report["evidence"]["direct_db_write_path_count"] == 7
    assert report["evidence"]["approved_mutation_path_count"] == 7
    assert report["evidence"]["unapproved_direct_db_mutations"] == []
    assert report["evidence"]["registry_gate_violations"] == []
    assert report["evidence"]["access_control_violations"] == []
    assert "sqlite:live_positions" in report["evidence"]["direct_db_targets"]


def test_direct_database_mutation_ban_blocks_unapproved_sqlite_mutation(tmp_path):
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "write_paths": [
                    {
                        "write_path_id": "unit.direct_sqlite_update",
                        "module": "unit",
                        "entry_point": "POST /api/mutate",
                        "target_store": "sqlite:demo",
                        "write_target": "demo",
                        "mutation_type": "update",
                        "requires_outbox": False,
                        "outbox_reason": "unit_test_break_glass_only",
                        "owner": "test",
                        "mode_gate": "admin_break_glass",
                        "source_file": "server.js",
                        "source_anchor": "UPDATE demo",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    policy_path = tmp_path / "direct-db-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.direct_database_mutation_ban.v1",
                "rules": {
                    "default_action": "ban_direct_database_mutation",
                    "required_registry_mode_gate": "admin_break_glass",
                    "require_access_control_policy": True,
                    "require_audit_log": True,
                    "require_post": True,
                    "require_outbox_rationale": True,
                },
                "approved_mutation_paths": [],
            }
        ),
        encoding="utf-8",
    )
    access_policy_path = tmp_path / "access-policy.json"
    write_access_policy(
        access_policy_path,
        "server.js",
        overrides=[
            {
                "endpoint": "/api/mutate",
                "required_role": "dashboard_admin",
                "token_scope": "dashboard:admin_mutation",
                "audit_log_required": True,
                "danger_level": "admin_mutation",
                "allowed_methods": ["POST"],
                "method_guard_required": True,
            }
        ],
    )

    report = verify_direct_database_mutation_ban(policy_path, registry_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "direct_db_mutation_ban_missing_malformed_or_bypassed"
    assert report["evidence"]["unapproved_direct_db_mutations"] == [
        {
            "write_path_id": "unit.direct_sqlite_update",
            "target_store": "sqlite:demo",
            "entry_point": "POST /api/mutate",
        }
    ]


def test_background_job_registry_covers_supervised_runtime_jobs():
    report = verify_background_job_registry()

    assert report["status"] == "pass"
    assert report["evidence"]["job_count"] == 9
    assert report["evidence"]["restart_loop_job_count"] == 6
    assert report["evidence"]["missing_entry_point_files"] == []
    assert report["evidence"]["missing_source_anchors"] == []
    assert "paper_trade_monitor" in report["evidence"]["job_names"]
    assert "source_resonance_shadow" in report["evidence"]["job_names"]


def test_background_job_registry_blocks_missing_entry_point(tmp_path):
    registry_path = tmp_path / "background-jobs.json"
    source_path = tmp_path / "runner.sh"
    source_path.write_text("python3 scripts/missing_worker.py\n", encoding="utf-8")
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.background_job_registry.v1",
                "jobs": [
                    {
                        "job_name": "missing_worker",
                        "entry_point": "python3 scripts/missing_worker.py",
                        "entry_point_file": str(tmp_path / "missing_worker.py"),
                        "source_file": str(source_path),
                        "source_anchor": "python3 scripts/missing_worker.py",
                        "allowed_modes": ["observe_only"],
                        "lease_policy": {
                            "kind": "supervised_restart_loop",
                            "pid_env": "MISSING_PID",
                            "restart_delay_sec": 15,
                        },
                        "owner": "test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_background_job_registry(registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "background_job_registry_missing_malformed_or_incomplete"
    assert report["evidence"]["missing_entry_point_files"] == [
        {"job_name": "missing_worker", "entry_point_file": str(tmp_path / "missing_worker.py")}
    ]


def test_entry_point_inventory_covers_runtime_routes_scripts_and_deploy():
    report = verify_entry_point_inventory()

    assert report["status"] == "pass"
    assert report["evidence"]["entry_point_count"] == 32
    assert report["evidence"]["entry_type_counts"]["route_group"] == 5
    assert report["evidence"]["entry_type_counts"]["script"] == 18
    assert report["evidence"]["dashboard_literal_route_count"] == 63
    assert report["evidence"]["dashboard_protected_route_count"] == 58
    assert report["evidence"]["route_registry_required_count"] == 2
    assert report["evidence"]["arbiter_required_count"] == 29
    assert report["evidence"]["uncovered_audit_required_routes"] == []
    assert report["evidence"]["location_violations"] == []


def test_entry_point_inventory_blocks_missing_anchor(tmp_path):
    source_path = tmp_path / "server.js"
    source_path.write_text("if (url.pathname === '/health') { res.end('ok'); }\n", encoding="utf-8")
    access_policy_path = tmp_path / "access-policy.json"
    write_access_policy(access_policy_path, source_path)
    inventory_path = tmp_path / "entry-points.json"
    inventory_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.entry_point_inventory.v1",
                "entry_points": [
                    {
                        "entry_point_id": "unit_missing_anchor",
                        "entry_type": "server",
                        "code_location": {
                            "file": str(source_path),
                            "anchor": "definitely_missing_anchor",
                        },
                        "route_registry_required": False,
                        "route_registry_reason": "unit test",
                        "arbiter_required": False,
                        "arbiter_reason": "unit test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_entry_point_inventory(inventory_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "entry_point_inventory_missing_malformed_or_incomplete"
    assert report["evidence"]["location_violations"] == [
        {
            "entry_point_id": "unit_missing_anchor",
            "location": "code_location",
            "file": str(source_path),
            "anchor": "definitely_missing_anchor",
            "reason": "anchor_not_found",
        }
    ]


def test_static_policy_enforcement_passes_critical_static_scans():
    report = verify_static_policy_enforcement()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.static_policy_enforcement.v1"
    assert report["evidence"]["static_check_count"] == 8
    assert report["evidence"]["forbidden_match_count"] == 0
    assert report["evidence"]["malformed_checks"] == []
    assert report["evidence"]["scan_errors"] == []


def test_static_policy_enforcement_blocks_forbidden_pattern(tmp_path):
    source_path = tmp_path / "unsafe.js"
    source_path.write_text("export const value = eval('1 + 1');\n", encoding="utf-8")
    policy_path = tmp_path / "static-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.static_policy_enforcement.v1",
                "checks": [
                    {
                        "static_check_id": "unit_eval_forbidden",
                        "forbidden_pattern": "\\beval\\s*\\(",
                        "scan_target": str(source_path),
                        "result": "pass",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_static_policy_enforcement(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "static_policy_missing_malformed_or_violated"
    assert report["evidence"]["forbidden_match_count"] == 1
    assert report["evidence"]["forbidden_matches"] == [
        {
            "static_check_id": "unit_eval_forbidden",
            "file": str(source_path),
            "line": 1,
            "match": "export const value = eval('1 + 1');",
        }
    ]


def test_api_response_contract_covers_v27_manual_evidence_post_routes():
    report = verify_api_response_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.api_response_policy.v1"
    assert report["evidence"]["endpoint_count"] == 6
    assert report["evidence"]["v27_evidence_endpoint_count"] == 6
    assert report["evidence"]["uncovered_v27_evidence_endpoints"] == []
    assert report["evidence"]["unknown_policy_endpoints"] == []
    assert report["evidence"]["malformed_policies"] == []
    assert report["evidence"]["route_violations"] == []
    assert report["evidence"]["source_violations"] == []
    assert report["evidence"]["missing_guard_helper_fragments"] == []


def test_api_response_contract_blocks_missing_response_builder_anchor(tmp_path):
    source_path = tmp_path / "dashboard.js"
    endpoint = "/api/paper/v27-read-model-refresh"
    source_path.write_text(
        "\n".join(
            [
                "export function apiJsonHeaders(cacheControl = 'no-store') { return {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': cacheControl}; }",
                "function buildV27ManualEvidenceApiResponse(responseSchemaVersion, result = {}, options = {}) {",
                "  const generatedAt = options.generatedAt || new Date().toISOString();",
                "  const payload = {generated_at: generatedAt, materialized: false, response_schema_version: responseSchemaVersion, refresh_schema_version: responseSchemaVersion, ...result};",
                "  if (payload.accepted === false && !payload.error) payload.error = payload.status || 'manual_evidence_request_rejected';",
                "  return payload;",
                "}",
                "function checkAuth(req, url, res) { res.writeHead(403, apiJsonHeaders()); res.writeHead(401, apiJsonHeaders()); return true; }",
                "function requirePost(req, res) { res.writeHead(405, apiJsonHeaders()); return true; }",
                "function requireDashboardAuditEvent(req, res, url) { res.writeHead(500, apiJsonHeaders()); return true; /* Audit log unavailable */ }",
                f"if (url.pathname === '{endpoint}') {{",
                "  if (!requirePost(req, res)) return;",
                "  if (!checkAuth(req, url, res)) return;",
                "  if (!requireDashboardAuditEvent(req, res, url)) return;",
                "  res.writeHead(refresh.accepted ? 202 : 409, apiJsonHeaders());",
                "  res.end(JSON.stringify({ response_schema_version: 'v2.7.0.manual_read_model_refresh.v1', refresh_schema_version: 'v2.7.0.manual_read_model_refresh.v1', ...refresh }));",
                "}",
            ]
        ),
        encoding="utf-8",
    )
    access_policy_path = tmp_path / "access-policy.json"
    access_policy_path.write_text(
        json.dumps(
            {
                "source_file": str(source_path),
                "endpoint_overrides": [
                    {
                        "endpoint": endpoint,
                        "token_scope": "v27:evidence_mutation",
                        "audit_log_required": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    policy_path = tmp_path / "api-response-policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_policy.v1",
                "source_file": str(source_path),
                "response_policies": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "status_code_policy": {
                            "accepted": 202,
                            "rejected": 409,
                            "method_not_allowed": 405,
                            "auth_failed": [401, 403],
                            "audit_unavailable": 500,
                        },
                        "error_envelope": {
                            "required": True,
                            "error_field": "error",
                            "guard_errors": True,
                            "rejected_response_error_required": True,
                        },
                        "cache_control": "no-store",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_api_response_contract(policy_path, access_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "api_response_policy_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"] == [
        {"endpoint": endpoint, "reason": "response_builder_missing"}
    ]


def test_api_response_envelope_contract_covers_v27_manual_evidence_routes():
    report = verify_api_response_envelope_contract()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.api_response_envelope_policy.v1"
    assert report["evidence"]["failure_action"] == "api_envelope_invalid"
    assert report["evidence"]["envelope_version"] == "v2.7.0.api_response_envelope.v1"
    assert report["evidence"]["hash_algorithm"] == "sha256_canonical_json_without_payload_hash"
    assert report["evidence"]["required_fields"] == ["endpoint", "envelope_version", "payload_hash", "error_shape", "generated_at"]
    assert report["evidence"]["endpoint_count"] == 6
    assert report["evidence"]["base_response_endpoint_count"] == 6
    assert report["evidence"]["sample_case_count"] == 2
    assert report["evidence"]["schema_violations"] == []
    assert report["evidence"]["error_shape_violations"] == []
    assert report["evidence"]["malformed_envelopes"] == []
    assert report["evidence"]["uncovered_base_response_endpoints"] == []
    assert report["evidence"]["source_violations"] == []
    assert report["evidence"]["malformed_samples"] == []
    assert report["evidence"]["missing_helper_fragments"] == []
    for sample in report["evidence"]["sample_evidence"]:
        assert sample["payload_hash"]
        assert sample["endpoint"] == "/api/paper/v27-read-model-refresh"
        assert sample["generated_at"]


def test_api_response_envelope_contract_blocks_missing_hash_generation(tmp_path):
    source_path = tmp_path / "dashboard.js"
    endpoint = "/api/paper/v27-read-model-refresh"
    source_path.write_text(
        "\n".join(
            [
                "export const V27_API_RESPONSE_ENVELOPE_VERSION = 'v2.7.0.api_response_envelope.v1';",
                "function buildApiResponseErrorShape(payload = {}) { return {}; }",
                "function apiEnvelopePayloadForHash(payload = {}) { return payload; }",
                f"if (url.pathname === '{endpoint}') {{",
                "  res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh)));",
                "}",
            ]
        ),
        encoding="utf-8",
    )
    base_policy_path = tmp_path / "api-response-policy.json"
    base_policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_policy.v1",
                "source_file": str(source_path),
                "response_policies": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    envelope_policy_path = tmp_path / "api-response-envelope-policy.json"
    envelope_policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.api_response_envelope_policy.v1",
                "source_file": str(source_path),
                "base_response_policy_path": str(base_policy_path),
                "failure_action": "api_envelope_invalid",
                "envelope_version": "v2.7.0.api_response_envelope.v1",
                "hash_algorithm": "sha256_canonical_json_without_payload_hash",
                "required_fields": ["endpoint", "envelope_version", "payload_hash", "error_shape", "generated_at"],
                "error_shape": {
                    "required_fields": ["has_error", "accepted", "error_field", "error_code", "status"],
                    "accepted_false_requires_error": True,
                    "accepted_false_requires_error_code": True,
                },
                "sample_cases": [
                    {
                        "sample_id": "accepted",
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "generated_at": "2026-05-25T00:00:00.000Z",
                        "result": {"accepted": True, "status": "started"},
                        "expected_error_shape": {
                            "has_error": False,
                            "accepted": True,
                            "error_field": None,
                            "error_code": None,
                            "status": "started",
                        },
                    }
                ],
                "response_envelopes": [
                    {
                        "endpoint": endpoint,
                        "response_schema_version": "v2.7.0.manual_read_model_refresh.v1",
                        "source_anchor": "buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh, { endpoint: url.pathname })",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_api_response_envelope_contract(envelope_policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "api_response_envelope_policy_missing_malformed_or_unenforced"
    assert report["evidence"]["source_violations"] == [
        {
            "endpoint": endpoint,
            "reason": "source_anchor_missing",
            "source_anchor": "buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh, { endpoint: url.pathname })",
        },
        {"endpoint": endpoint, "reason": "endpoint_binding_missing"},
    ]
    assert "payload_hash_assignment" in report["evidence"]["missing_helper_fragments"]


def test_error_taxonomy_covers_dashboard_and_readiness_error_codes():
    report = verify_error_taxonomy()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.error_taxonomy.v1"
    assert report["evidence"]["unclassified_error_codes"] == []
    assert report["evidence"]["unused_taxonomy_codes"] == []
    assert report["evidence"]["malformed_entries"] == []
    assert "already_running" in report["evidence"]["observed_by_source"]["dashboard_api_error_codes"]
    assert "error_taxonomy_missing_malformed_or_incomplete" in report["evidence"]["observed_by_source"]["basic_readiness_blocking_reasons"]
    assert "paper_live_capability_detected" in report["evidence"]["observed_by_source"]["paper_mode_safety_reasons"]


def test_error_taxonomy_blocks_unclassified_dashboard_error(tmp_path):
    dashboard_source = tmp_path / "dashboard.js"
    dashboard_source.write_text("res.end(JSON.stringify({ error: 'Nope', error_code: 'unclassified_unit_error' }));\n", encoding="utf-8")
    basic_source = tmp_path / "basic.py"
    basic_source.write_text("", encoding="utf-8")
    paper_source = tmp_path / "paper.py"
    paper_source.write_text("", encoding="utf-8")
    taxonomy_path = tmp_path / "taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.error_taxonomy.v1",
                "coverage": {},
                "allowed_categories": ["dashboard_request"],
                "allowed_severities": ["error"],
                "taxonomy": [
                    {
                        "error_code": "classified_unit_error",
                        "category": "dashboard_request",
                        "severity": "error",
                        "operator_action": "unit test",
                        "introduced_at": "2026-05-25T00:00:00Z",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_error_taxonomy(
        taxonomy_path=taxonomy_path,
        dashboard_source_path=dashboard_source,
        basic_readiness_source_path=basic_source,
        paper_mode_safety_source_path=paper_source,
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "error_taxonomy_missing_malformed_or_incomplete"
    assert report["evidence"]["unclassified_error_codes"] == ["unclassified_unit_error"]
    assert report["evidence"]["unused_taxonomy_codes"] == ["classified_unit_error"]


def test_error_taxonomy_blocks_malformed_entry(tmp_path):
    dashboard_source = tmp_path / "dashboard.js"
    dashboard_source.write_text("res.end(JSON.stringify({ error: 'Nope', error_code: 'classified_unit_error' }));\n", encoding="utf-8")
    basic_source = tmp_path / "basic.py"
    basic_source.write_text("", encoding="utf-8")
    paper_source = tmp_path / "paper.py"
    paper_source.write_text("", encoding="utf-8")
    taxonomy_path = tmp_path / "taxonomy.json"
    taxonomy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.error_taxonomy.v1",
                "coverage": {},
                "allowed_categories": ["dashboard_request"],
                "allowed_severities": ["error"],
                "taxonomy": [
                    {
                        "error_code": "classified_unit_error",
                        "category": "dashboard_request",
                        "severity": "error",
                        "introduced_at": "not-a-date",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_error_taxonomy(
        taxonomy_path=taxonomy_path,
        dashboard_source_path=dashboard_source,
        basic_readiness_source_path=basic_source,
        paper_mode_safety_source_path=paper_source,
    )

    assert report["status"] == "missing_evidence"
    assert report["evidence"]["malformed_entries"] == [
        {
            "index": 0,
            "error_code": "classified_unit_error",
            "missing_fields": ["operator_action"],
            "violations": ["introduced_at_invalid_iso_timestamp"],
        }
    ]


def test_log_redaction_verification_covers_runtime_and_manual_evidence_logs():
    report = verify_log_redaction_verification()

    assert report["status"] == "pass"
    assert report["evidence"]["schema_version"] == "v2.7.0.log_redaction_policy.v1"
    assert report["evidence"]["secret_pattern_set"] == "v2.7.0.secret_pattern_set.dashboard_runtime.v1"
    assert report["evidence"]["pattern_count"] == 4
    assert report["evidence"]["sample_case_count"] == 3
    assert report["evidence"]["stream_count"] == 3
    assert report["evidence"]["malformed_samples"] == []
    assert report["evidence"]["malformed_streams"] == []
    assert report["evidence"]["source_violations"] == []
    for stream in report["evidence"]["streams"]:
        assert stream["redaction_passed"] is True
        assert stream["sample_hash"]
        assert stream["checked_at"]


def test_log_redaction_verification_blocks_unredacted_sample(tmp_path):
    source_path = tmp_path / "dashboard.js"
    source_path.write_text(
        "const message = args.map(formatLogArg).join(' ');\n"
        "logBuffer.push(logLine);\n"
        "fs.appendFileSync(runtimeLogPath, message);\n",
        encoding="utf-8",
    )
    policy_path = tmp_path / "log-redaction.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.log_redaction_policy.v1",
                "secret_pattern_set": {
                    "secret_pattern_set": "unit.secret_patterns.v1",
                    "patterns": [
                        {
                            "pattern_id": "wrong_pattern",
                            "regex": "(api_key=)(safe-value-only)",
                            "replacement": "\\1[REDACTED]",
                        }
                    ],
                },
                "sample_cases": [
                    {
                        "sample_id": "leaky",
                        "raw": "api_key=unit-secret-value",
                        "expected_fragments_absent": ["unit-secret-value"],
                        "expected_fragments_present": ["api_key=[REDACTED]"],
                    }
                ],
                "streams": [
                    {
                        "log_stream": "dashboard_runtime_log",
                        "secret_pattern_set": "unit.secret_patterns.v1",
                        "source_file": str(source_path),
                        "redaction_anchor": "redactLogMessage(args.map(formatLogArg).join(' '))",
                        "write_anchor": "fs.appendFileSync(runtimeLogPath",
                        "sample_case_ids": ["leaky"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_log_redaction_verification(policy_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "log_redaction_verification_missing_malformed_or_failed"
    assert report["evidence"]["malformed_samples"][0]["sample_id"] == "leaky"
    assert report["evidence"]["source_violations"] == [
        {
            "log_stream": "dashboard_runtime_log",
            "reason": "redaction_anchor_missing",
            "redaction_anchor": "redactLogMessage(args.map(formatLogArg).join(' '))",
        }
    ]


def test_write_path_registry_blocks_unregistered_static_write(tmp_path):
    source_path = tmp_path / "writer.js"
    source_path.write_text("db.prepare(`UPDATE demo SET value = 1`).run();\n", encoding="utf-8")
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "static_scan": {
                    "targets": [
                        {
                            "source_file": str(source_path),
                            "include_patterns": ["UPDATE "],
                            "exclude_patterns": [],
                        }
                    ]
                },
                "write_paths": [],
            }
        ),
        encoding="utf-8",
    )

    report = verify_write_path_registry(registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "write_path_registry_missing_malformed_or_incomplete"
    assert report["evidence"]["scanned_mutation_count"] == 1
    assert report["evidence"]["unregistered_mutation_count"] == 1


def test_write_path_registry_accepts_tmp_registered_write(tmp_path):
    source_path = tmp_path / "writer.js"
    source_path.write_text("db.prepare(`UPDATE demo SET value = 1`).run();\n", encoding="utf-8")
    registry_path = tmp_path / "write-path-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "schema_version": "v2.7.0.write_path_registry.v1",
                "static_scan": {
                    "targets": [
                        {
                            "source_file": str(source_path),
                            "include_patterns": ["UPDATE "],
                            "exclude_patterns": [],
                        }
                    ]
                },
                "write_paths": [
                    {
                        "write_path_id": "unit.demo.update",
                        "module": "unit",
                        "entry_point": "unit_test",
                        "target_store": "sqlite:demo",
                        "mutation_type": "update",
                        "requires_outbox": False,
                        "outbox_reason": "unit_test_non_production_write",
                        "owner": "test",
                        "mode_gate": "diagnostics",
                        "source_file": str(source_path),
                        "source_anchor": "UPDATE demo",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = verify_write_path_registry(registry_path)

    assert report["status"] == "pass"
    assert report["evidence"]["registered_write_path_count"] == 1


def test_governance_readiness_blocks_incomplete_artifact(tmp_path):
    governance_path = tmp_path / "governance.json"
    governance_path.write_text(
        json.dumps(
            {
                "schema_version": "unit.bad",
                "evidence_eligibility_matrix": [
                    {
                        "evidence_use": "normal_tiny_promotion",
                        "event_truth": [],
                        "feature_truth": [],
                        "label_truth": [],
                        "replay_truth": [],
                    }
                ],
                "top_fix_queue": [
                    {
                        "fix_id": "fix-only-one",
                        "blocker_code": "RawProviderEvidenceContract",
                    }
                ],
                "safety_cases": [
                    {
                        "safety_case_id": "case-without-links",
                        "scope": "normal_tiny",
                        "core_hazards": ["hazard"],
                        "mitigations": ["mitigation"],
                        "evidence_links": [],
                    }
                ],
                "waiver_policy": [
                    {
                        "waiver_id": "expired-waiver",
                        "contract_id": "RawProviderEvidenceContract",
                        "scope": "normal_tiny",
                        "expires_at": "2020-01-01T00:00:00Z",
                        "non_waivable": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert verify_evidence_eligibility_matrix(governance_path)["status"] == "missing_evidence"
    assert verify_top_fix_queue(governance_path)["status"] == "missing_evidence"
    assert verify_safety_case(governance_path)["status"] == "missing_evidence"
    assert verify_waiver_policy(governance_path)["status"] == "missing_evidence"
