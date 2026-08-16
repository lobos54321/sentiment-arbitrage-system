import fcntl
import json
from pathlib import Path
import sqlite3
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"

import sys

sys.path.insert(0, str(SCRIPTS))

from evaluator_db_contract import (  # noqa: E402
    evaluator_db_source_status,
    evaluator_snapshot_bundle_lease,
    evaluator_snapshot_bundle_status,
    evaluator_snapshot_provenance,
    json_numeric_evidence_contract_sha256,
    json_numeric_evidence_types_valid,
    require_evaluator_db_source,
    require_evaluator_snapshot_bundle,
    sha256_file,
)
from cross_db_evaluator_snapshot import (  # noqa: E402
    build_snapshot_bundle,
    shared_stage_budget_evidence_sha256,
    shared_stage_budget_plan_sha256,
)
from evaluator_evidence_schema import (  # noqa: E402
    EVIDENCE_SCHEMA,
    EVIDENCE_SCHEMA_SHA256,
    EVIDENCE_SCHEMA_VERSION,
    is_decimal_identifier,
    is_evidence_timestamp,
    is_iso8601_timestamp,
    numeric_evidence_rule,
    validate_numeric_evidence_schema,
    validate_numeric_evidence_value,
)


def create_live_sources(root):
    root.mkdir(exist_ok=True)
    definitions = {
        "signal": ("sentiment_arb.db", "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)"),
        "paper": (
            "paper_trades.db",
            "CREATE TABLE candidate_shadow_observations("
            "id INTEGER PRIMARY KEY, signal_id INTEGER, candidate_id TEXT, "
            "observed_at INTEGER, payload_json TEXT);"
            "CREATE INDEX idx_candidate_shadow_obs_observed "
            "ON candidate_shadow_observations(observed_at);"
            "CREATE INDEX idx_candidate_shadow_obs_signal "
            "ON candidate_shadow_observations(signal_id);"
            "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
            "CREATE INDEX idx_candidate_shadow_virtual_observed "
            "ON candidate_shadow_virtual_trades(observed_at);"
            "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);"
            "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_a_class_decision_recent ON a_class_decision_events(event_ts);"
            "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
            "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
            "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER);"
            "CREATE INDEX idx_opportunity_events_recent ON opportunity_events(event_ts);"
            "CREATE TABLE opportunity_event_path_samples("
            "id INTEGER PRIMARY KEY, opportunity_key TEXT, sample_ts REAL, "
            "raw_payload_json TEXT, created_at REAL, updated_at REAL);"
            "CREATE INDEX idx_opportunity_path_samples_key_ts "
            "ON opportunity_event_path_samples(opportunity_key, sample_ts);"
            "CREATE INDEX idx_opportunity_path_samples_sample_ts "
            "ON opportunity_event_path_samples(sample_ts)",
        ),
        "raw": ("raw_signal_outcomes.db", "CREATE TABLE raw_signal_outcomes(id INTEGER, signal_id TEXT, updated_at INTEGER)"),
        "kline": ("kline_cache.db", "CREATE TABLE kline_1m(token_ca TEXT, timestamp INTEGER)"),
    }
    sources = {}
    for name, (filename, ddl) in definitions.items():
        path = root / filename
        db = sqlite3.connect(path)
        db.executescript(ddl)
        db.commit()
        db.close()
        sources[name] = str(path)
    return sources


def create_valid_bundle(
    tmp_path,
    monkeypatch,
    *,
    include_optional_path_samples: bool = True,
    seed_shared_stage_rows: bool = False,
    seed_all_database_rows: bool = False,
):
    live = tmp_path / "live"
    sources = create_live_sources(live)
    if not include_optional_path_samples:
        paper = sqlite3.connect(sources["paper"])
        paper.execute("DROP TABLE opportunity_event_path_samples")
        paper.commit()
        paper.close()
    if seed_shared_stage_rows or seed_all_database_rows:
        now = int(time.time())
        paper = sqlite3.connect(sources["paper"])
        paper.execute(
            "INSERT INTO candidate_shadow_observations("
            "id, signal_id, candidate_id, observed_at, payload_json"
            ") VALUES (1, 1, 'candidate-1', ?, '{}')",
            (now,),
        )
        paper.execute(
            "INSERT INTO paper_decision_events(id, event_ts) VALUES (1, ?)",
            (now,),
        )
        paper.execute(
            "INSERT INTO a_class_decision_events(id, event_ts) VALUES (1, ?)",
            (now,),
        )
        paper.execute(
            "INSERT INTO opportunity_events(id, event_ts) VALUES (1, ?)",
            (now,),
        )
        if seed_all_database_rows:
            paper.execute(
                "INSERT INTO candidate_shadow_virtual_trades(signal_id, observed_at) "
                "VALUES (1, ?)",
                (now,),
            )
            paper.execute(
                "INSERT INTO a_class_mode_runtime_state(id, updated_at) VALUES (1, ?)",
                (now,),
            )
            paper.execute(
                "INSERT INTO paper_trades(id, entry_time) VALUES (1, ?)",
                (now,),
            )
        if include_optional_path_samples:
            paper.execute(
                "INSERT INTO opportunity_event_path_samples("
                "id, opportunity_key, sample_ts, raw_payload_json, "
                "created_at, updated_at"
                ") VALUES (1, 'opp-1', ?, '{}', ?, ?)",
                (now, now, now),
            )
        paper.commit()
        paper.close()
    if seed_all_database_rows:
        now = int(time.time())
        signal = sqlite3.connect(sources["signal"])
        signal.execute(
            "INSERT INTO premium_signals(id, source_message_ts) VALUES (1, ?)",
            (now,),
        )
        signal.commit()
        signal.close()
        raw = sqlite3.connect(sources["raw"])
        raw.execute(
            "INSERT INTO raw_signal_outcomes(id, signal_id, updated_at) "
            "VALUES (1, 1, ?)",
            (now,),
        )
        raw.commit()
        raw.close()
        kline = sqlite3.connect(sources["kline"])
        kline.execute(
            "INSERT INTO kline_1m(token_ca, timestamp) VALUES ('token-1', ?)",
            (now,),
        )
        kline.commit()
        kline.close()
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)
    out = live / "agent_evidence"
    manifest = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    manifest_path = (
        out / "snapshots" / str(manifest["snapshot_id"]) / "manifest.json"
    ).resolve()
    (out / "snapshot_status.json").write_text(
        json.dumps(
            {
                "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
                "status": "completed",
                "accepted": True,
                "snapshot_id": manifest["snapshot_id"],
                "last_success_at": "2026-01-01T00:00:00Z",
                "last_failure_at": None,
                "last_failure_code": None,
                "last_accepted_snapshot": {
                    "snapshot_id": manifest["snapshot_id"],
                    "manifest_path": str(manifest_path),
                    "manifest_sha256": sha256_file(manifest_path),
                    "numeric_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
                    "numeric_evidence_schema_sha256": EVIDENCE_SCHEMA_SHA256,
                    "numeric_evidence_schema_validated_before_publish": True,
                },
                "promotion_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    return live, sources, out


def synchronize_shared_budget_copies(manifest):
    shared = manifest["shared_stage_budget"]
    shared["plan_sha256"] = shared_stage_budget_plan_sha256(shared)
    shared["evidence_sha256"] = shared_stage_budget_evidence_sha256(shared)
    manifest["disk_preflight"]["shared_stage_budget"] = json.loads(
        json.dumps(shared)
    )


def synchronize_producer_manifest_sha(out: Path, manifest_path: Path) -> None:
    status_path = out / "snapshot_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["last_accepted_snapshot"]["manifest_sha256"] = sha256_file(
        manifest_path
    )
    status_path.write_text(json.dumps(status), encoding="utf-8")


def write_manifest_with_converged_sizes(
    manifest: dict,
    manifest_path: Path,
) -> None:
    for _attempt in range(8):
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        manifest_size = manifest_path.stat().st_size
        bundle_size = sum(
            item.stat().st_size
            for item in manifest_path.parent.iterdir()
            if item.is_file()
        )
        if (
            int(manifest["manifest_size_bytes"]) == manifest_size
            and int(manifest["output_size_bytes"]) == bundle_size
        ):
            return
        manifest["manifest_size_bytes"] = manifest_size
        manifest["output_size_bytes"] = bundle_size
    raise AssertionError("manifest size evidence did not converge")


def test_missing_evidence_db_is_rejected(tmp_path):
    status = evaluator_db_source_status(
        str(tmp_path / "agent_evidence" / "paper_evidence.db"),
        str(tmp_path),
    )

    assert status["accepted"] is False
    assert status["blockers"] == ["evaluator_db_missing"]
    assert status["promotion_allowed"] is False


def test_numeric_evidence_contract_matches_dashboard_golden_hash():
    assert json_numeric_evidence_contract_sha256() == (
        "e111584ff5368a54ba03ad938ce7f136409a0dc5438c89694e54a13e0bf234f3"
    )


def test_numeric_evidence_timestamp_union_has_strict_cross_runtime_shape():
    for value in (
        "2026-08-08T03:59:00Z",
        "2026-08-08T03:59:00.123456Z",
        "2026-08-08T13:59:00+10:00",
    ):
        assert is_iso8601_timestamp(value) is True
        assert is_evidence_timestamp(value) is True
    for value in (
        "2026-08-08 03:59:00",
        "2026-08-08 03:59:00.123456",
    ):
        assert is_iso8601_timestamp(value) is False
        assert is_evidence_timestamp(value) is True
    for value in (
        "123",
        "2026-02-31T00:00:00Z",
        "2026-08-08T25:00:00Z",
        "2026-08-08T03:59:00",
        "2026-02-31 00:00:00",
        "2026-08-08 25:00:00",
        "1969-12-31T23:59:59Z",
    ):
        assert is_evidence_timestamp(value) is False
        assert is_iso8601_timestamp(value) is False

    for value in ("0", "1", "47959", str(2**53 - 1)):
        assert is_decimal_identifier(value) is True
    for value in ("", "01", "+1", "-1", "1.0", str(2**53)):
        assert is_decimal_identifier(value) is False


def test_declarative_schema_preserves_strict_production_scalar_variants():
    payload = {
        "numeric_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "numeric_evidence_schema_sha256": EVIDENCE_SCHEMA_SHA256,
        "numeric_evidence_schema_validated_before_publish": True,
        "databases": {
            "paper": {
                "selected_tables": {
                    "paper_decision_events": {
                        "rows_copied": 1,
                        "time_column": "event_ts",
                        "time_columns": ["event_ts", "created_at"],
                    }
                },
                "source_upper_watermarks": {
                    "paper_decision_events": {
                        "event_ts": 1_786_766_144.125,
                    }
                },
                "upper_watermarks": {
                    "paper_decision_events": {
                        "id": 1,
                        "event_ts": 1_786_766_144.125,
                        "created_at": "2026-08-08 03:59:00",
                    }
                },
            },
            "raw": {
                "selected_tables": {
                    "raw_signal_outcomes": {
                        "rows_copied": 1,
                        "time_column": "updated_at",
                    }
                },
                "source_upper_watermarks": {"raw_signal_outcomes": {}},
                "upper_watermarks": {
                    "raw_signal_outcomes": {
                        "id": 1,
                        "signal_id": "47959",
                        "updated_at": 1_786_766_144,
                    }
                },
            },
        },
    }
    assert validate_numeric_evidence_schema(
        payload,
        require_binding=True,
    )["accepted"] is True

    payload["databases"]["paper"]["upper_watermarks"][
        "paper_decision_events"
    ]["event_ts"] = 0.5
    assert validate_numeric_evidence_schema(payload)["accepted"] is False
    payload["databases"]["paper"]["upper_watermarks"][
        "paper_decision_events"
    ]["event_ts"] = 1_786_766_144.125
    payload["databases"]["raw"]["upper_watermarks"][
        "raw_signal_outcomes"
    ]["signal_id"] = "047959"
    assert validate_numeric_evidence_schema(payload)["accepted"] is False


def test_every_current_manifest_numeric_leaf_is_type_guarded(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    assert json_numeric_evidence_types_valid(manifest) is True

    numeric_slots = []

    def collect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if isinstance(child, (int, float)) and not isinstance(
                    child, bool
                ):
                    numeric_slots.append((value, key, child))
                else:
                    collect(child)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                if isinstance(child, (int, float)) and not isinstance(
                    child, bool
                ):
                    numeric_slots.append((value, index, child))
                else:
                    collect(child)

    collect(manifest)
    assert len(numeric_slots) >= 900
    for container, key, original in numeric_slots:
        container[key] = "numeric-type-tamper"
        assert json_numeric_evidence_types_valid(manifest) is False
        container[key] = original
    assert json_numeric_evidence_types_valid(manifest) is True


def test_declarative_schema_covers_every_all_database_numeric_leaf(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    report = validate_numeric_evidence_schema(
        manifest,
        require_binding=True,
    )

    assert report["accepted"] is True
    assert report["error_count"] == 0
    assert report["numeric_leaf_count"] >= 1000
    assert (
        report["declared_numeric_leaf_count"]
        == report["numeric_leaf_count"]
    )
    assert report["schema_sha256"] == EVIDENCE_SCHEMA_SHA256
    assert (
        report["rule_match_counts"]["nullable_watermark_values"]
        + report["rule_match_counts"]["nullable_epoch_time_watermarks"]
        >= 10
    )
    assert report["rule_match_counts"]["nullable_watermark_identifiers"] >= 5


def test_every_numeric_leaf_obeys_declarative_type_null_and_range_rules(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    numeric_leaves = []

    def collect(value, path=""):
        if isinstance(value, dict):
            for field, child in value.items():
                child_path = f"{path}.{field}" if path else str(field)
                if isinstance(child, (int, float)) and not isinstance(
                    child, bool
                ):
                    numeric_leaves.append((child_path, child))
                else:
                    collect(child, child_path)
        elif isinstance(value, list):
            for child in value:
                child_path = f"{path}[]"
                if isinstance(child, (int, float)) and not isinstance(
                    child, bool
                ):
                    numeric_leaves.append((child_path, child))
                else:
                    collect(child, child_path)

    collect(manifest)
    assert len(numeric_leaves) >= 1000
    for path, original in numeric_leaves:
        baseline = validate_numeric_evidence_value(manifest, path, original)
        assert baseline["accepted"] is True, (path, original, baseline)
        rule_match = numeric_evidence_rule(path)
        if rule_match is None:
            parent = path[:-2] if path.endswith("[]") else path.rsplit(".", 1)[0]
            rule_match = numeric_evidence_rule(parent)
            assert rule_match is not None and rule_match[0] == "container"
            rule = rule_match[1]
            prefix = "element_"
        else:
            rule = rule_match[1]
            prefix = ""
        kind = rule[f"{prefix}kind"]
        invalid_values = [
            False,
            {},
            [],
            float("nan"),
            float("inf"),
        ]
        if kind == "safe_integer_or_decimal_identifier":
            assert validate_numeric_evidence_value(
                manifest,
                path,
                "123",
            )["accepted"] is True
            invalid_values.extend(("01", "+1", "1.0"))
        else:
            invalid_values.append("123")
        for invalid in invalid_values:
            result = validate_numeric_evidence_value(manifest, path, invalid)
            assert result["accepted"] is False, (path, invalid, result)
        if kind.startswith("safe_integer"):
            fractional = validate_numeric_evidence_value(
                manifest,
                path,
                0.5,
            )
            assert fractional["accepted"] is False, path
            unsafe = validate_numeric_evidence_value(
                manifest,
                path,
                2**53,
            )
            assert unsafe["accepted"] is False, path
        minimum = rule.get(f"{prefix}minimum")
        if minimum is not None:
            below = validate_numeric_evidence_value(
                manifest,
                path,
                minimum - 1,
            )
            assert below["accepted"] is False, path
        maximum = rule.get(f"{prefix}maximum")
        if maximum is not None:
            above = validate_numeric_evidence_value(
                manifest,
                path,
                maximum + 1,
            )
            assert above["accepted"] is False, path
        null_result = validate_numeric_evidence_value(manifest, path, None)
        if rule.get(f"{prefix}nullable") is False:
            assert null_result["accepted"] is False, path
        elif rule.get("null_policy") is None:
            assert null_result["accepted"] is True, path


def test_all_nonempty_dynamic_watermarks_reject_every_invalid_numeric_shape(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    watermark_slots = []
    for database_name, report in manifest["databases"].items():
        for copy_name in ("source_upper_watermarks", "upper_watermarks"):
            for table, watermarks in report[copy_name].items():
                for field, value in watermarks.items():
                    if isinstance(value, (int, float)) and not isinstance(
                        value, bool
                    ):
                        watermark_slots.append(
                            (
                                f"databases.{database_name}.{copy_name}.{table}.{field}",
                                watermarks,
                                field,
                                value,
                            )
                        )

    assert len(watermark_slots) >= 20
    invalid_values = (
        "1700000000",
        False,
        {},
        [],
        -1,
        2**53,
        float("inf"),
    )
    for path, container, field, original in watermark_slots:
        for invalid in invalid_values:
            container[field] = invalid
            report = validate_numeric_evidence_schema(manifest)
            assert report["accepted"] is False, (path, invalid)
            assert any(
                error["path"] == path for error in report["errors"]
            ), (path, invalid, report["errors"])
            container[field] = original
        rule = numeric_evidence_rule(path, field)
        assert rule is not None and rule[0] == "scalar"
        container[field] = 0.5
        fractional_report = validate_numeric_evidence_schema(manifest)
        assert fractional_report["accepted"] is False, path
        container[field] = original

    manifest["undeclared_attacker_numeric_evidence"] = 1
    report = validate_numeric_evidence_schema(manifest)
    assert report["accepted"] is False
    assert report["errors"][0] == {
        "path": "undeclared_attacker_numeric_evidence",
        "code": "undeclared_numeric_evidence",
    }
    del manifest["undeclared_attacker_numeric_evidence"]
    manifest["undeclared_attacker_count"] = 1
    report = validate_numeric_evidence_schema(manifest)
    assert report["accepted"] is False
    assert report["errors"][0] == {
        "path": "undeclared_attacker_count",
        "code": "undeclared_numeric_evidence",
    }


def test_indexed_count_timeout_advisory_numeric_schema_is_strict_and_complete():
    target_names = (
        "candidate_shadow_observations",
        "paper_decision_events",
        "a_class_decision_events",
        "opportunity_events",
    )

    def targets():
        return {
            target: {
                "advisory_evidence": {
                    "selected_row_count": None,
                    "sample_row_count_advisory_basis": 256,
                }
            }
            for target in target_names
        }

    payload = {
        "disk_preflight": {
            "shared_stage_budget": {"targets": targets()}
        },
        "shared_stage_budget": {"targets": targets()},
    }
    report = validate_numeric_evidence_schema(payload)
    assert report["accepted"] is True, report["errors"]
    assert report["numeric_leaf_count"] == 8
    assert report["declared_numeric_leaf_count"] == 8

    selected_rule = numeric_evidence_rule(
        "disk_preflight.shared_stage_budget.targets."
        "candidate_shadow_observations.advisory_evidence."
        "selected_row_count",
        "selected_row_count",
    )
    sample_rule = numeric_evidence_rule(
        "disk_preflight.shared_stage_budget.targets."
        "candidate_shadow_observations.advisory_evidence."
        "sample_row_count_advisory_basis",
        "sample_row_count_advisory_basis",
    )
    assert selected_rule is not None
    assert selected_rule[1]["id"] == (
        "nullable_shared_advisory_row_count_fields"
    )
    assert selected_rule[1]["nullable"] is True
    assert sample_rule is not None
    assert sample_rule[1]["id"] == "nullable_shared_advisory_row_count_fields"
    assert sample_rule[1]["nullable"] is True

    parent_targets = (
        (
            "disk_preflight.shared_stage_budget.targets",
            payload["disk_preflight"]["shared_stage_budget"]["targets"],
        ),
        ("shared_stage_budget.targets", payload["shared_stage_budget"]["targets"]),
    )
    invalid_selected = (-1, 0.5, "256", False, None, {}, [], 2**53)
    invalid_sample = (-1, 0.5, "256", False, None, {}, [], 2**53)
    for parent_path, target_map in parent_targets:
        for target, target_payload in target_map.items():
            evidence = target_payload["advisory_evidence"]
            selected_path = (
                f"{parent_path}.{target}.advisory_evidence.selected_row_count"
            )
            sample_path = (
                f"{parent_path}.{target}.advisory_evidence."
                "sample_row_count_advisory_basis"
            )
            for invalid in invalid_selected:
                evidence["selected_row_count"] = invalid
                selected_report = validate_numeric_evidence_schema(payload)
                if invalid is None:
                    assert selected_report["accepted"] is True, selected_path
                else:
                    assert selected_report["accepted"] is False, (
                        selected_path,
                        invalid,
                    )
            evidence["selected_row_count"] = None
            for invalid in invalid_sample:
                evidence["sample_row_count_advisory_basis"] = invalid
                sample_report = validate_numeric_evidence_schema(payload)
                if invalid is None:
                    assert sample_report["accepted"] is True, sample_path
                else:
                    assert sample_report["accepted"] is False, (
                        sample_path,
                        invalid,
                    )
            evidence["sample_row_count_advisory_basis"] = 256

    assert validate_numeric_evidence_schema(payload)["accepted"] is True


def test_every_field_selector_is_bound_to_declared_parent_paths(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    field_rules = [
        rule
        for category in ("container_rules", "scalar_rules")
        for rule in EVIDENCE_SCHEMA[category]
        if rule.get("fields")
    ]
    assert len(field_rules) >= 10

    attack_paths = set()
    manifest["attacker"] = {
        "rows_copied": 1,
        "output_size_bytes": 1,
        "duration_sec": 1,
    }
    attack_paths.update(
        {
            "attacker.rows_copied",
            "attacker.output_size_bytes",
            "attacker.duration_sec",
        }
    )
    wrong_path_values = (1, 0.5, "1", False, None, {}, [])
    for index, attack_value in enumerate(wrong_path_values):
        manifest["attacker"][f"shape_{index}"] = {}
        for rule in field_rules:
            attack_container = {}
            manifest["attacker"][f"shape_{index}"][rule["id"]] = (
                attack_container
            )
            for field in rule["fields"]:
                attack_container[field] = attack_value
                attack_path = (
                    f"attacker.shape_{index}.{rule['id']}.{field}"
                )
                attack_paths.add(attack_path)
                assert numeric_evidence_rule(attack_path, field) is None
                assert validate_numeric_evidence_value(
                    manifest,
                    attack_path,
                    attack_value,
                )["accepted"] is False
    for rule in field_rules:
        parent_patterns = rule.get("parent_path_patterns")
        assert isinstance(parent_patterns, list) and parent_patterns
        assert "*" not in parent_patterns
        attack_container = {}
        manifest["attacker"][rule["id"]] = attack_container
        for field in rule["fields"]:
            attack_container[field] = 1
            root_attack = f"attacker.{rule['id']}.{field}"
            attack_paths.add(root_attack)
            assert numeric_evidence_rule(root_attack, field) is None
            assert numeric_evidence_rule(
                f"databases.paper.attacker.{rule['id']}.{field}",
                field,
            ) is None
            assert numeric_evidence_rule(
                f"shared_stage_budget.attacker.{rule['id']}.{field}",
                field,
            ) is None

    report = validate_numeric_evidence_schema(manifest, max_errors=4096)
    assert report["accepted"] is False
    observed = {
        error["path"]
        for error in report["errors"]
        if error["code"]
        in {
            "undeclared_numeric_evidence",
            "declared_numeric_field_parent_path_mismatch",
        }
    }
    assert attack_paths <= observed
    assert numeric_evidence_rule(
        "attacker.rows_copied",
        "output_size_bytes",
    ) is None
    del manifest["attacker"]

    known_wrong_locations = (
        (manifest["databases"]["paper"], "databases.paper", "rows_copied"),
        (
            manifest["databases"]["paper"],
            "databases.paper",
            "output_size_bytes",
        ),
        (manifest["selection_contract"], "selection_contract", "output_size_bytes"),
        (manifest, "", "duration_sec"),
        (manifest["shared_stage_budget"], "shared_stage_budget", "duration_sec"),
        (manifest["disk_preflight"], "disk_preflight", "rows_copied"),
        (manifest["database_budget_plan"], "database_budget_plan", "stage_size_bytes"),
        (
            manifest["databases"]["paper"]["selected_tables"]
            ["a_class_decision_events"],
            "databases.paper.selected_tables.a_class_decision_events",
            "output_size_bytes",
        ),
        (manifest["shared_stage_budget"], "shared_stage_budget", "page_count"),
        (manifest["disk_preflight"], "disk_preflight", "utilization_ratio"),
    )
    for container, parent_path, field in known_wrong_locations:
        path = f"{parent_path}.{field}" if parent_path else field
        assert numeric_evidence_rule(path, field) is None
        container[field] = 1
        location_report = validate_numeric_evidence_schema(manifest)
        assert location_report["accepted"] is False, path
        assert any(
            error["path"] == path
            and error["code"]
            == "declared_numeric_field_parent_path_mismatch"
            for error in location_report["errors"]
        ), (path, location_report["errors"])
        del container[field]


def test_watermark_nullability_is_explicit_and_bound_to_empty_selected_table(
    tmp_path,
    monkeypatch,
):
    _live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest = json.loads(
        (out / "current" / "manifest.json").read_text(encoding="utf-8")
    )
    source_watermark = manifest["databases"]["paper"][
        "source_upper_watermarks"
    ]["a_class_decision_events"]
    destination_watermark = manifest["databases"]["paper"][
        "upper_watermarks"
    ]["a_class_decision_events"]

    source_original = source_watermark["event_ts"]
    destination_original = destination_watermark["event_ts"]
    source_watermark["event_ts"] = None
    destination_watermark["event_ts"] = None
    assert validate_numeric_evidence_schema(manifest)["accepted"] is False

    manifest["databases"]["paper"]["selected_tables"][
        "a_class_decision_events"
    ]["rows_copied"] = 0
    assert validate_numeric_evidence_schema(manifest)["accepted"] is True

    source_watermark["event_ts"] = source_original
    destination_watermark["event_ts"] = destination_original
    manifest["databases"]["paper"]["upper_watermarks"][
        "a_class_decision_events"
    ]["id"] = None
    assert validate_numeric_evidence_schema(manifest)["accepted"] is True


@pytest.mark.parametrize("tampered_value", ["1700000000", 0.5, None])
def test_coherent_watermark_tamper_is_rejected_by_authoritative_consumer(
    tmp_path,
    monkeypatch,
    tampered_value,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["databases"]["paper"]["source_upper_watermarks"][
        "a_class_decision_events"
    ]["event_ts"] = tampered_value
    manifest["databases"]["paper"]["upper_watermarks"][
        "a_class_decision_events"
    ]["event_ts"] = tampered_value
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_numeric_evidence_type_invalid" in status["blockers"]


def test_coherent_known_field_wrong_path_tamper_is_rejected_by_authority(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_all_database_rows=True,
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["attacker"] = {
        "rows_copied": 1,
        "output_size_bytes": 1,
        "duration_sec": 1,
        "rules": {
        rule["id"]: {field: 1 for field in rule["fields"]}
        for category in ("container_rules", "scalar_rules")
        for rule in EVIDENCE_SCHEMA[category]
        if rule.get("fields")
        },
    }
    write_manifest_with_converged_sizes(manifest, manifest_path)
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_numeric_evidence_type_invalid" in status["blockers"]


def test_coherent_schema_binding_spoof_is_rejected_by_authoritative_consumer(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["numeric_evidence_schema_sha256"] = "f" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)
    producer_path = out / "snapshot_status.json"
    producer = json.loads(producer_path.read_text(encoding="utf-8"))
    producer["last_accepted_snapshot"]["numeric_evidence_schema_sha256"] = (
        "f" * 64
    )
    producer_path.write_text(json.dumps(producer), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_numeric_evidence_schema_invalid" in status["blockers"]
    assert (
        "evaluator_snapshot_producer_numeric_evidence_schema_invalid"
        in status["blockers"]
    )


def test_active_paper_db_is_rejected_by_default(tmp_path):
    live = tmp_path / "paper_trades.db"
    live.touch()

    status = evaluator_db_source_status(str(live), str(tmp_path))

    assert status["accepted"] is False
    assert status["is_live_paper_db"] is True
    assert "active_paper_db_forbidden_for_evaluator" in status["blockers"]
    with pytest.raises(RuntimeError, match="active_paper_db_forbidden_for_evaluator"):
        require_evaluator_db_source(str(live), str(tmp_path))


def test_active_paper_db_hardlink_alias_is_rejected(tmp_path):
    live = tmp_path / "paper_trades.db"
    live.touch()
    alias = tmp_path / "research" / "paper_evidence.db"
    alias.parent.mkdir()
    alias.hardlink_to(live)

    status = evaluator_db_source_status(str(alias), str(tmp_path))

    assert status["accepted"] is False
    assert status["is_live_paper_db"] is True
    assert "active_paper_db_forbidden_for_evaluator" in status["blockers"]


def test_separate_evidence_db_is_accepted(tmp_path):
    evidence = tmp_path / "agent_evidence" / "current" / "paper_evidence.db"
    evidence.parent.mkdir(parents=True)
    evidence.touch()

    status = require_evaluator_db_source(str(evidence), str(tmp_path))

    assert status["accepted"] is True
    assert status["is_live_paper_db"] is False


def test_valid_cross_db_snapshot_bundle_is_required(tmp_path, monkeypatch):
    live, sources, out = create_valid_bundle(tmp_path, monkeypatch)

    status = require_evaluator_snapshot_bundle(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is True
    assert status["snapshot_id"] == "20260101T000000Z-1234abcd"
    assert status["manifest_sha256"] == sha256_file(out / "current" / "manifest.json")
    assert status["numeric_evidence_schema_version"] == EVIDENCE_SCHEMA_VERSION
    assert status["numeric_evidence_schema_sha256"] == EVIDENCE_SCHEMA_SHA256
    assert status["numeric_evidence_schema_binding_valid"] is True
    provenance = evaluator_snapshot_provenance(status)
    assert provenance["schema_version"] == "evaluator_snapshot_provenance.v1"
    assert provenance["accepted"] is True
    assert provenance["snapshot_id"] == status["snapshot_id"]
    assert provenance["manifest_sha256"] == status["manifest_sha256"]
    assert provenance["producer_manifest_sha256"] == status["manifest_sha256"]
    assert provenance["numeric_evidence_schema_version"] == EVIDENCE_SCHEMA_VERSION
    assert provenance["numeric_evidence_schema_sha256"] == EVIDENCE_SCHEMA_SHA256
    assert provenance["numeric_evidence_schema_binding_valid"] is True
    assert provenance["producer_status_path"] == str(out / "snapshot_status.json")
    assert provenance["databases"]["paper"]["sha256_matches_manifest"] is True
    assert provenance["databases"]["paper"]["quick_check"] == ["ok"]
    assert provenance["promotion_allowed"] is False
    assert provenance["strategy_change_allowed"] is False
    assert provenance["automatic_runtime_change_allowed"] is False
    assert provenance["paper_enablement_allowed"] is False

    stale = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
        max_age_sec=28800,
        now_ts=float(status["snapshot_ts"]) + 28801,
    )
    assert stale["accepted"] is False
    assert "evaluator_snapshot_stale" in stale["blockers"]

    rejected = evaluator_snapshot_bundle_status(
        signal_db=sources["signal"],
        paper_db=sources["paper"],
        raw_db=sources["raw"],
        kline_db=sources["kline"],
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )
    assert rejected["accepted"] is False
    assert "active_paper_db_forbidden_for_evaluator" in rejected["blockers"]


def test_optional_path_stage_absence_is_accepted_by_authoritative_consumer(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        include_optional_path_samples=False,
    )

    status = require_evaluator_snapshot_bundle(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    manifest = json.loads((out / "current" / "manifest.json").read_text())
    paper_report = manifest["databases"]["paper"]
    assert status["accepted"] is True
    assert manifest["parallel_paper_stage_inventory_passed"] is True
    assert manifest["parallel_paper_stage_tables"] == [
        "paper_decision_events",
        "a_class_decision_events",
        "opportunity_events",
    ]
    assert paper_report["selected_tables"]["opportunity_event_path_samples"] == {
        "included": False,
        "required": False,
        "reason": "optional_source_table_missing",
    }
    assert "opportunity_event_path_samples" not in paper_report[
        "parallel_paper_stages"
    ]
    disk = manifest["disk_preflight"]
    assert disk["parallel_paper_stage_tables"] == manifest[
        "parallel_paper_stage_tables"
    ]
    assert disk["omitted_optional_parallel_paper_stage_tables"] == [
        "opportunity_event_path_samples"
    ]
    assert "opportunity_event_path_samples" not in disk[
        "temporary_parallel_paper_stage_cap_bytes"
    ]
    assert (
        disk["temporary_candidate_stage_cap_bytes"]
        + sum(disk["temporary_parallel_paper_stage_cap_bytes"].values())
        == disk["temporary_stage_total_cap_bytes"]
    )


def test_optional_absent_stage_cannot_retain_hidden_disk_cap(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        include_optional_path_samples=False,
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["disk_preflight"][
        "temporary_parallel_paper_stage_cap_bytes"
    ]["opportunity_event_path_samples"] = 12288
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in status["blockers"]
    )


def test_unknown_parallel_stage_name_is_rejected_without_consumer_crash(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parallel_paper_stage_tables"].append("unknown_parallel_stage")
    manifest["parallel_paper_stage_count"] = len(
        manifest["parallel_paper_stage_tables"]
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_inventory_invalid"
        in status["blockers"]
    )


def test_non_list_report_stage_inventory_is_rejected_without_consumer_crash(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["databases"]["paper"]["parallel_paper_stage_tables"] = 123
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


def test_required_parallel_stage_cannot_be_removed_from_manifest_inventory(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parallel_paper_stage_tables"] = [
        table
        for table in manifest["parallel_paper_stage_tables"]
        if table != "opportunity_events"
    ]
    manifest["parallel_paper_stage_count"] = len(
        manifest["parallel_paper_stage_tables"]
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_inventory_invalid"
        in status["blockers"]
    )


def test_missing_producer_acceptance_status_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    (out / "snapshot_status.json").unlink()

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_producer_status_missing" in status["blockers"]


def test_manifest_rehash_cannot_bypass_producer_acceptance_anchor(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["git_commit"] = "b" * 40
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_producer_manifest_sha256_mismatch" in status["blockers"]


def test_producer_acceptance_identity_and_path_must_match_bundle(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    status_path = out / "snapshot_status.json"
    producer = json.loads(status_path.read_text(encoding="utf-8"))
    producer["last_accepted_snapshot"]["snapshot_id"] = "different-snapshot"
    producer["last_accepted_snapshot"]["manifest_path"] = str(
        out / "snapshots" / "different-snapshot" / "manifest.json"
    )
    status_path.write_text(json.dumps(producer), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_producer_snapshot_id_mismatch" in status["blockers"]
    assert "evaluator_snapshot_producer_manifest_path_mismatch" in status["blockers"]


def test_all_active_database_hardlink_aliases_are_rejected(tmp_path, monkeypatch):
    live, sources, out = create_valid_bundle(tmp_path, monkeypatch)
    aliases = tmp_path / "aliases"
    aliases.mkdir()
    alias_paths = {}
    for name, source in sources.items():
        alias = aliases / Path(source).name
        alias.hardlink_to(Path(source))
        alias_paths[name] = str(alias)

    status = evaluator_snapshot_bundle_status(
        signal_db=alias_paths["signal"],
        paper_db=alias_paths["paper"],
        raw_db=alias_paths["raw"],
        kline_db=alias_paths["kline"],
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    for name in ("signal", "paper", "raw", "kline"):
        assert f"active_{name}_db_forbidden_for_evaluator" in status["blockers"]


def test_cross_role_active_database_hardlink_alias_is_rejected(tmp_path, monkeypatch):
    live, sources, out = create_valid_bundle(tmp_path, monkeypatch)
    alias = tmp_path / "aliases" / "signal.db"
    alias.parent.mkdir()
    alias.hardlink_to(Path(sources["paper"]))

    status = evaluator_snapshot_bundle_status(
        signal_db=str(alias),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "active_paper_db_forbidden_for_signal_evaluator" in status["blockers"]


@pytest.mark.parametrize("payload", [{}, None, []])
def test_falsy_or_non_object_manifest_is_rejected(tmp_path, monkeypatch, payload):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_manifest_invalid_structure" in status["blockers"]


def test_same_size_snapshot_corruption_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    paper = (out / "current" / "paper_evidence.db").resolve()
    with paper.open("r+b") as handle:
        handle.seek(-1, 2)
        original = handle.read(1)
        handle.seek(-1, 2)
        handle.write(bytes([original[0] ^ 0x01]))

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_paper_sha256_mismatch" in status["blockers"]


def test_selection_contract_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["selection_contract"]["future_rows_excluded"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_future_row_contract_invalid" in status["blockers"]


def test_indexed_time_selection_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    selection = manifest["databases"]["paper"]["selected_tables"][
        "candidate_shadow_observations"
    ]
    selection["predicate_strategy"] = "normalized_timestamp"
    selection["indexed_time_anchor"] = None
    selection["source_index_name"] = None
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_paper_indexed_time_selection_invalid:"
        "candidate_shadow_observations"
    ) in status["blockers"]


def test_indexed_query_plan_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    selection = manifest["databases"]["paper"]["selected_tables"][
        "candidate_shadow_observations"
    ]
    selection["source_query_plan"] = [
        "SCAN src.candidate_shadow_observations"
    ]
    selection["source_query_plan_uses_index"] = False
    selection["source_query_plan_uses_range_search"] = False
    selection["source_query_plan_full_table_scan_detected"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_paper_indexed_query_plan_invalid:"
        "candidate_shadow_observations"
    ) in status["blockers"]


def test_indexed_source_watermark_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    watermark = manifest["databases"]["paper"]["source_watermark_query_evidence"][
        "candidate_shadow_observations"
    ]
    watermark["strategy"] = "aggregate_max"
    watermark["query_plan"] = ["SCAN src.candidate_shadow_observations"]
    watermark["uses_declared_index"] = False
    watermark["full_table_scan_detected"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_paper_indexed_watermark_invalid:"
        "candidate_shadow_observations"
    ) in status["blockers"]


def test_deferred_source_watermark_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    deferred = manifest["databases"]["paper"]["source_watermark_query_evidence"][
        "paper_trades"
    ]
    deferred["strategy"] = "aggregate_max"
    deferred["source_query_executed"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_paper_source_watermark_not_deferred:paper_trades"
        in status["blockers"]
    )


def test_source_read_lock_contract_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_read_lock_budget_passed"] = False
    manifest["databases"]["paper"]["source_read_lock_duration_sec"] = 9999
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_source_read_lock_budget_failed" in status["blockers"]
    assert "evaluator_snapshot_paper_source_read_lock_contract_invalid" in status["blockers"]


def test_candidate_projection_lock_order_or_stage_cleanup_tampering_is_rejected(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["candidate_projection_after_source_read_lock_release"] = False
    manifest["candidate_stage_removed_before_publish"] = False
    paper_report = manifest["databases"]["paper"]
    paper_report["candidate_projection_after_source_read_lock_release"] = False
    paper_report["temporary_candidate_stage_removed_before_publish"] = False
    projection = paper_report["selected_tables"]["candidate_shadow_observations"][
        "storage_projection"
    ]
    projection["applied"] = False
    projection["projection_started_after_source_read_view_release"] = False
    projection["stage_query_plan_uses_order_index"] = False
    projection["stage_query_plan_temp_btree_detected"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_candidate_projection_lock_order_invalid" in status["blockers"]
    assert "evaluator_snapshot_candidate_stage_cleanup_invalid" in status["blockers"]
    assert "evaluator_snapshot_paper_candidate_projection_lock_order_invalid" in status["blockers"]
    assert "evaluator_snapshot_paper_candidate_stage_cleanup_invalid" in status["blockers"]
    assert "evaluator_snapshot_candidate_payload_projection_required" in status["blockers"]
    assert "evaluator_snapshot_candidate_stage_projection_contract_invalid" in status["blockers"]


def test_parallel_paper_decision_stage_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["paper_decision_parallel_read_view_pinned"] = False
    manifest[
        "paper_decision_parallel_stage_merged_after_source_read_lock_release"
    ] = False
    manifest["paper_decision_parallel_stage_removed_before_publish"] = False
    paper_report = manifest["databases"]["paper"]
    paper_report["paper_decision_parallel_read_view_pinned"] = False
    paper_report[
        "paper_decision_parallel_stage_merged_after_source_read_lock_release"
    ] = False
    paper_report["paper_decision_parallel_stage_removed_before_publish"] = False
    paper_report["paper_decision_parallel_stage_rows_merged"] += 1
    paper_report["pinned_read_views"] = paper_report["pinned_read_views"][:1]
    parallel_stage = paper_report["selected_tables"]["paper_decision_events"][
        "parallel_stage"
    ]
    parallel_stage["full_fidelity_row_copy"] = False
    parallel_stage["row_count_matched"] = False
    parallel_stage["merge_started_after_source_read_view_release"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_paper_decision_parallel_pin_invalid" in status["blockers"]
    assert "evaluator_snapshot_paper_decision_merge_lock_order_invalid" in status["blockers"]
    assert "evaluator_snapshot_paper_decision_stage_cleanup_invalid" in status["blockers"]
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


@pytest.mark.parametrize(
    "stage_table",
    (
        "paper_decision_events",
        "a_class_decision_events",
        "opportunity_events",
        "opportunity_event_path_samples",
    ),
)
def test_each_parallel_paper_stage_nested_tampering_is_rejected(
    tmp_path,
    monkeypatch,
    stage_table,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    stage_report = paper_report["parallel_paper_stages"][stage_table]
    nested = paper_report["selected_tables"][stage_table]["parallel_stage"]
    stage_report["rows_merged"] = int(stage_report["rows_merged"]) + 1
    stage_report["removed_before_publish"] = False
    nested["row_count_matched"] = False
    nested["payload_semantics_preserved"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


@pytest.mark.parametrize(
    ("field", "tampered_value"),
        (
            ("stage_schema_mode", "source_schema_with_constraints"),
            ("source_create_sql_sha256", "0" * 64),
            ("destination_create_sql_sha256", "0" * 64),
            ("source_column_contract_sha256", "0" * 64),
            ("destination_column_contract_sha256", "0" * 64),
            ("stage_storage_contract_sha256", "0" * 64),
            ("stage_storage_contract_passed", False),
            ("stage_codec_schema_version", "unknown-codec"),
            ("stage_compression", "lossy"),
            ("stage_chunk_target_bytes", 1),
            ("stage_chunk_count", 999),
            ("stage_raw_size_bytes", -1),
            ("stage_compressed_payload_size_bytes", -1),
            ("stage_rows_sha256", "0" * 64),
            ("hydrated_rows_sha256", "0" * 64),
            ("stage_chunk_integrity_passed", False),
            ("stage_row_digest_matched", False),
            ("compressed_during_source_read_lock", False),
            ("hydrated_after_source_read_lock_release", False),
            ("stage_column_count", 999),
            ("stage_index_count", 1),
        ("source_constraints_deferred_off_source_lock", False),
        (
            "destination_schema_restored_after_source_read_lock_release",
            False,
        ),
        (
            "source_constraints_rebuilt_after_source_read_lock_release",
            False,
        ),
    ),
)
def test_parallel_stage_schema_contract_tampering_is_rejected(
    tmp_path,
    monkeypatch,
    field,
    tampered_value,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    stage_table = "opportunity_events"
    paper_report["parallel_paper_stages"][stage_table][field] = tampered_value
    paper_report["selected_tables"][stage_table]["parallel_stage"][
        field
    ] = tampered_value
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


def test_parallel_stage_fractional_integer_evidence_is_rejected(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    stage_table = "opportunity_events"
    stage_report = paper_report["parallel_paper_stages"][stage_table]
    nested_stage = paper_report["selected_tables"][stage_table][
        "parallel_stage"
    ]
    for field in (
        "stage_chunk_count",
        "stage_raw_size_bytes",
        "stage_compressed_payload_size_bytes",
        "stage_index_count",
    ):
        stage_report[field] = 0.5
        nested_stage[field] = 0.5
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


@pytest.mark.parametrize(
    ("evidence_layer", "tampered_value"),
    tuple(
        pytest.param(layer, value, id=f"{layer}-{label}")
        for layer in (
            "parallel_stage_copies",
            "shared_budget_copies",
            "disk_preflight",
            "stage_inventory",
            "output_budget",
        )
        for value, label in (
            (0.5, "fractional"),
            ("0", "numeric-string"),
            (False, "boolean"),
            (None, "null"),
            (2**53, "unsafe-integer"),
            ({}, "object"),
            ([], "array"),
        )
    )
    + tuple(
        pytest.param(layer, value, id=f"{layer}-{label}")
        for layer in ("duration_copies", "paper_alias_copies")
        for value, label in (
            ("0", "numeric-string"),
            (False, "boolean"),
            (None, "null"),
            (10**400, "non-finite-after-json-number-conversion"),
            ({}, "object"),
            ([], "array"),
        )
    ),
)
def test_numeric_evidence_type_tamper_matrix_is_rejected(
    tmp_path,
    monkeypatch,
    evidence_layer,
    tampered_value,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    stage_table = "opportunity_events"

    if evidence_layer == "parallel_stage_copies":
        stage_report = paper_report["parallel_paper_stages"][stage_table]
        selection_report = paper_report["selected_tables"][stage_table]
        nested_stage = selection_report["parallel_stage"]
        for field in (
            "stage_chunk_count",
            "stage_raw_size_bytes",
            "stage_compressed_payload_size_bytes",
            "stage_index_count",
        ):
            stage_report[field] = tampered_value
            selection_report[field] = tampered_value
            nested_stage[field] = tampered_value
    elif evidence_layer == "shared_budget_copies":
        manifest["shared_stage_budget"]["advisory_miss_count"] = (
            tampered_value
        )
        try:
            synchronize_shared_budget_copies(manifest)
        except ValueError:
            manifest["disk_preflight"]["shared_stage_budget"] = json.loads(
                json.dumps(manifest["shared_stage_budget"])
            )
    elif evidence_layer == "disk_preflight":
        manifest["disk_preflight"]["temporary_full_backup_bytes"] = (
            tampered_value
        )
    elif evidence_layer == "stage_inventory":
        manifest["parallel_paper_stage_count"] = tampered_value
        paper_report["parallel_paper_stage_count"] = tampered_value
    elif evidence_layer == "duration_copies":
        manifest["max_source_read_lock_sec"] = tampered_value
        for report in manifest["databases"].values():
            report["source_read_lock_limit_sec"] = tampered_value
            for pinned_view in report["pinned_read_views"]:
                pinned_view["source_read_lock_limit_sec"] = tampered_value
    elif evidence_layer == "paper_alias_copies":
        stage_report = paper_report["parallel_paper_stages"][
            "paper_decision_events"
        ]
        nested_stage = paper_report["selected_tables"][
            "paper_decision_events"
        ]["parallel_stage"]
        paper_report[
            "paper_decision_parallel_stage_merge_duration_sec"
        ] = tampered_value
        stage_report["merge_duration_sec"] = tampered_value
        nested_stage["merge_duration_sec"] = tampered_value
    elif evidence_layer == "output_budget":
        manifest["output_size_bytes"] = tampered_value
    else:  # pragma: no cover - parametrization is the complete inventory
        raise AssertionError(evidence_layer)

    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)
    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    if tampered_value is not None:
        assert (
            "evaluator_snapshot_numeric_evidence_type_invalid"
            in status["blockers"]
        )


def test_integral_json_float_integer_evidence_remains_cross_runtime_valid(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    stage_table = "opportunity_events"
    stage_report = paper_report["parallel_paper_stages"][stage_table]
    selection_report = paper_report["selected_tables"][stage_table]
    nested_stage = selection_report["parallel_stage"]
    for field in (
        "stage_chunk_count",
        "stage_raw_size_bytes",
        "stage_compressed_payload_size_bytes",
        "stage_index_count",
    ):
        stage_report[field] = 0.0
        selection_report[field] = 0.0
        nested_stage[field] = 0.0
    manifest["parallel_paper_stage_count"] = float(
        manifest["parallel_paper_stage_count"]
    )
    paper_report["parallel_paper_stage_count"] = float(
        paper_report["parallel_paper_stage_count"]
    )
    manifest["pinned_read_view_count"] = float(
        manifest["pinned_read_view_count"]
    )
    manifest["output_size_bytes"] = float(manifest["output_size_bytes"])
    manifest["disk_preflight"]["temporary_full_backup_bytes"] = 0.0
    manifest["shared_stage_budget"]["advisory_miss_count"] = float(
        manifest["shared_stage_budget"]["advisory_miss_count"]
    )
    synchronize_shared_budget_copies(manifest)
    for _attempt in range(8):
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        actual_bundle_size = sum(
            item.stat().st_size
            for item in manifest_path.parent.iterdir()
            if item.is_file()
        )
        if int(manifest["output_size_bytes"]) == actual_bundle_size:
            break
        manifest["output_size_bytes"] = float(actual_bundle_size)
    else:  # pragma: no cover - decimal width converges in at most two writes
        raise AssertionError("manifest output size did not converge")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is True, status["blockers"]


def test_final_snapshot_schema_drift_is_rejected_even_if_manifest_claims_restored(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    paper_path = (out / "current" / "paper_evidence.db").resolve()
    paper = sqlite3.connect(paper_path)
    paper.execute("ALTER TABLE opportunity_events ADD COLUMN injected TEXT")
    paper.commit()
    paper.close()

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(paper_path),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


def test_pinned_read_view_lineage_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["pinned_read_view_count"] = 4
    paper_views = manifest["databases"]["paper"]["pinned_read_views"]
    paper_views[1]["role"] = "paper_main_selective_copy"
    paper_views[1]["pinned_midpoint_epoch"] = (
        float(manifest["snapshot_ts"]) + 60.0
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_pinned_read_view_lineage_invalid" in status["blockers"]


@pytest.mark.parametrize(
    "mutation",
    (
        "legacy_fixed_share",
        "grant_sum_exceeds_global_cap",
        "actual_exceeds_grant",
        "cleanup_incomplete",
        "stage_files_not_removed",
        "unregistered_stage_file",
        "optional_target_inventory_drift",
        "baseline_below_history_requirement",
        "negative_actual",
        "null_grant",
        "advisory_sample_used",
        "advisory_scaled_physical_tamper",
        "advisory_physical_bytes_tamper",
        "advisory_formula_tamper",
        "advisory_claims_physical_upper",
        "allocation_weight_tamper",
        "advisory_miss_inventory_tamper",
        "storage_schema_version_tamper",
        "history_storage_compatibility_tamper",
        "history_storage_schema_tamper",
        "plan_hash_mismatch",
    ),
)
def test_shared_stage_budget_tampering_is_rejected(
    tmp_path,
    monkeypatch,
    mutation,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shared = manifest["shared_stage_budget"]
    disk = manifest["disk_preflight"]
    p9 = shared["targets"]["paper_decision_events"]
    if mutation == "legacy_fixed_share":
        disk["candidate_stage_residual_share"] = 0.12
    elif mutation == "grant_sum_exceeds_global_cap":
        p9["granted_cap_bytes"] += 4096
        p9["borrowed_shared_pool_bytes"] += 4096
        shared["total_granted_bytes"] += 4096
        disk["temporary_parallel_paper_stage_cap_bytes"][
            "paper_decision_events"
        ] += 4096
        disk["temporary_paper_decision_stage_cap_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "actual_exceeds_grant":
        delta = int(p9["granted_cap_bytes"]) + 1 - int(
            p9["actual_usage_bytes"]
        )
        p9["actual_usage_bytes"] += delta
        p9["high_water_bytes"] += delta
        p9["utilization_ratio"] = p9["actual_usage_bytes"] / p9[
            "granted_cap_bytes"
        ]
        shared["actual_total_bytes"] += delta
        shared["unconsumed_bytes"] -= delta
        synchronize_shared_budget_copies(manifest)
    elif mutation == "cleanup_incomplete":
        shared["cleanup_completed"] = False
        synchronize_shared_budget_copies(manifest)
    elif mutation == "stage_files_not_removed":
        shared["stage_files_removed"] = False
        synchronize_shared_budget_copies(manifest)
    elif mutation == "unregistered_stage_file":
        shared["no_unregistered_stage_files"] = False
        shared["unregistered_stage_files"] = [".rogue-stage.db"]
        synchronize_shared_budget_copies(manifest)
    elif mutation == "optional_target_inventory_drift":
        shared["active_targets"].remove("opportunity_event_path_samples")
        synchronize_shared_budget_copies(manifest)
    elif mutation == "baseline_below_history_requirement":
        p9["baseline_required_bytes"] -= 4096
        p9["borrowed_shared_pool_bytes"] += 4096
        shared["baseline_required_total_bytes"] -= 4096
        shared["residual_pool_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "negative_actual":
        delta = int(p9["actual_usage_bytes"]) + 1
        p9["actual_usage_bytes"] = -1
        p9["high_water_bytes"] = -1
        shared["actual_total_bytes"] -= delta
        shared["unconsumed_bytes"] += delta
        synchronize_shared_budget_copies(manifest)
    elif mutation == "null_grant":
        p9["granted_cap_bytes"] = None
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_sample_used":
        p9["advisory_evidence"]["capacity_sample_used"] = True
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_scaled_physical_tamper":
        p9["advisory_evidence"][
            "table_scaled_physical_advisory_bytes"
        ] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_physical_bytes_tamper":
        p9["advisory_evidence"]["source_dbstat_physical_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_formula_tamper":
        p9["advisory_evidence"]["advisory_formula"] = (
            "edge_sample_average_times_selected_rows"
        )
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_claims_physical_upper":
        p9["physical_upper_bound_claimed"] = True
        p9["advisory_evidence"]["physical_upper_bound_claimed"] = True
        synchronize_shared_budget_copies(manifest)
    elif mutation == "allocation_weight_tamper":
        p9["allocation_weight_bytes"] += 4096
        shared["allocation_weight_total_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_miss_inventory_tamper":
        shared["targets_exceeding_advisory"] = ["paper_decision_events"]
        shared["advisory_miss_count"] = 1
        synchronize_shared_budget_copies(manifest)
    elif mutation == "storage_schema_version_tamper":
        p9["storage_schema_version"] = "parallel_paper_event_stage.v2"
        synchronize_shared_budget_copies(manifest)
    elif mutation == "history_storage_compatibility_tamper":
        p9["history_storage_compatible"] = True
        synchronize_shared_budget_copies(manifest)
    elif mutation == "history_storage_schema_tamper":
        p9["history_storage_schema_version"] = p9[
            "storage_schema_version"
        ]
        synchronize_shared_budget_copies(manifest)
    elif mutation == "plan_hash_mismatch":
        shared["plan_sha256"] = "0" * 64
        disk["shared_stage_budget"] = json.loads(json.dumps(shared))
    else:
        raise AssertionError(mutation)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in status["blockers"]
    )


@pytest.mark.parametrize(
    "mutation",
    (
        "advisory_read_view_id_mismatch",
        "advisory_read_view_role_mismatch",
        "manifest_binding_flag_false",
        "paper_binding_flag_false",
        "pinned_view_id_mismatch",
    ),
)
def test_shared_stage_read_view_binding_tampering_is_rejected(
    tmp_path,
    monkeypatch,
    mutation,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shared = manifest["shared_stage_budget"]
    p9 = shared["targets"]["paper_decision_events"]
    candidate = shared["targets"]["candidate_shadow_observations"]
    paper_report = manifest["databases"]["paper"]
    if mutation == "advisory_read_view_id_mismatch":
        p9["advisory_evidence"]["pinned_read_view_id"] = candidate[
            "advisory_evidence"
        ]["pinned_read_view_id"]
        synchronize_shared_budget_copies(manifest)
    elif mutation == "advisory_read_view_role_mismatch":
        p9["advisory_evidence"]["pinned_read_view_role"] = (
            "paper_main_selective_copy"
        )
        synchronize_shared_budget_copies(manifest)
    elif mutation == "manifest_binding_flag_false":
        manifest[
            "shared_stage_estimates_bound_to_copy_read_views"
        ] = False
    elif mutation == "paper_binding_flag_false":
        paper_report[
            "shared_stage_estimates_bound_to_copy_read_views"
        ] = False
    elif mutation == "pinned_view_id_mismatch":
        for view in paper_report["pinned_read_views"]:
            if view.get("role") == "paper_decision_events_parallel_stage":
                view["read_view_id"] = "f" * 32
                break
        else:
            raise AssertionError("paper decision pinned view missing")
    else:
        raise AssertionError(mutation)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert {
        "evaluator_snapshot_shared_stage_budget_contract_invalid",
        "evaluator_snapshot_parallel_paper_stage_contract_invalid",
        "evaluator_snapshot_pinned_read_view_lineage_invalid",
    }.intersection(status["blockers"])


def test_frozen_row_count_mismatch_is_rejected_after_sha_reanchoring(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_shared_stage_rows=True,
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    paper_path = (out / "current" / "paper_evidence.db").resolve()

    paper = sqlite3.connect(paper_path)
    paper.execute("DELETE FROM paper_decision_events")
    paper.commit()
    paper.close()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paper_report = manifest["databases"]["paper"]
    paper_report["snapshot_sha256"] = sha256_file(paper_path)
    paper_report["snapshot_size_bytes"] = paper_path.stat().st_size
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(paper_path),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


def test_candidate_selected_and_projection_row_counts_are_bound(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(
        tmp_path,
        monkeypatch,
        seed_shared_stage_rows=True,
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    candidate = manifest["databases"]["paper"]["selected_tables"][
        "candidate_shadow_observations"
    ]
    candidate["rows_copied"] = 2
    candidate["storage_projection"]["rows_copied"] = 2
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    synchronize_producer_manifest_sha(out, manifest_path)

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
        in status["blockers"]
    )


def test_candidate_stage_budget_formula_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    disk = manifest["disk_preflight"]
    disk["candidate_stage_budget_mode"] = "fixed_output_fraction"
    disk["temporary_candidate_stage_cap_bytes"] -= 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in status["blockers"]
    )


def test_paper_decision_stage_budget_formula_tampering_is_rejected(
    tmp_path,
    monkeypatch,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    disk = manifest["disk_preflight"]
    disk["temporary_paper_decision_stage_cap_bytes"] -= 4096
    manifest["databases"]["paper"][
        "paper_decision_parallel_stage_budget_bytes"
    ] -= 4096
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in status["blockers"]
    )


@pytest.mark.parametrize(
    "stage_table",
    (
        "paper_decision_events",
        "a_class_decision_events",
        "opportunity_events",
    ),
)
def test_each_parallel_paper_stage_budget_formula_tampering_is_rejected(
    tmp_path,
    monkeypatch,
    stage_table,
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    disk = manifest["disk_preflight"]
    disk["temporary_parallel_paper_stage_cap_bytes"][stage_table] -= 4096
    manifest["databases"]["paper"]["parallel_paper_stages"][stage_table][
        "stage_budget_bytes"
    ] -= 4096
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert (
        "evaluator_snapshot_shared_stage_budget_contract_invalid"
        in status["blockers"]
    )


def test_disk_preflight_tampering_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["disk_preflight"]["accepted"] = False
    manifest["disk_preflight"]["estimated_free_after_bytes"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_disk_preflight_failed" in status["blockers"]


def test_candidate_payload_projection_tampering_is_rejected(tmp_path, monkeypatch):
    live, sources, out = create_valid_bundle(tmp_path, monkeypatch)
    paper = sqlite3.connect(sources["paper"])
    paper.execute("DROP TABLE candidate_shadow_observations")
    paper.execute(
        """
        CREATE TABLE candidate_shadow_observations(
          id INTEGER PRIMARY KEY,
          signal_id INTEGER NOT NULL,
          candidate_id TEXT NOT NULL,
          observed_at INTEGER NOT NULL,
          payload_json TEXT NOT NULL,
          UNIQUE(signal_id, candidate_id)
        )
        """
    )
    paper.execute(
        "CREATE INDEX idx_candidate_shadow_obs_observed "
        "ON candidate_shadow_observations(observed_at)"
    )
    paper.execute(
        "CREATE INDEX idx_candidate_shadow_obs_signal "
        "ON candidate_shadow_observations(signal_id)"
    )
    paper.execute(
        "INSERT INTO candidate_shadow_observations VALUES (1,1,'current_all',?,?)",
        (int(time.time()), '{"context":true}'),
    )
    paper.commit()
    paper.close()
    out = live / "projected_evidence"
    build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    projection = manifest["databases"]["paper"]["selected_tables"][
        "candidate_shadow_observations"
    ]["storage_projection"]
    assert projection["applied"] is True
    projection["payload_semantics_preserved"] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_candidate_payload_projection_invalid" in status["blockers"]


def test_partial_artifact_inside_published_bundle_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    snapshot_dir = (out / "current").resolve()
    (snapshot_dir / ".paper_evidence.db.tmp").write_text("partial", encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_partial_artifacts_present" in status["blockers"]


@pytest.mark.parametrize(
    "side_name",
    [
        "paper_evidence.db-shm",
        "paper_evidence.db-wal",
        "paper_evidence.db-journal",
        ".paper_evidence.db.tmp",
        "unexpected.bin",
    ],
)
def test_side_or_unknown_file_is_rejected_and_counted_outside_manifest(
    tmp_path, monkeypatch, side_name
):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    snapshot_dir = (out / "current").resolve()
    (snapshot_dir / side_name).write_bytes(b"unexpected-side-file")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(out / "current" / "manifest.json"),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_partial_artifacts_present" in status["blockers"]
    assert "evaluator_snapshot_bundle_size_mismatch" in status["blockers"]


def test_time_bearing_selection_without_future_bound_is_rejected(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["databases"]["paper"]["selected_tables"]["paper_trades"][
        "future_bound_enforced"
    ] = False
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(out / "current" / "paper_evidence.db"),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_paper_future_bound_missing:paper_trades" in status["blockers"]


def test_consumer_recomputes_temporal_maxima_instead_of_trusting_manifest(
    tmp_path, monkeypatch
):
    live = tmp_path / "live"
    sources = create_live_sources(live)
    paper_source = sqlite3.connect(sources["paper"])
    paper_source.execute("ALTER TABLE paper_trades ADD COLUMN exit_ts INTEGER")
    paper_source.execute(
        "INSERT INTO paper_trades(id, entry_time, exit_ts) VALUES (1, ?, NULL)",
        (1,),
    )
    paper_source.commit()
    paper_source.close()
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)
    out = live / "agent_evidence"
    report = build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    paper = (out / "current" / "paper_evidence.db").resolve()
    mutated = sqlite3.connect(paper)
    mutated.execute(
        "UPDATE paper_trades SET entry_time=?, exit_ts=? WHERE id=1",
        (int(report["snapshot_ts"]) - 60, int(report["snapshot_ts"]) + 3600),
    )
    mutated.commit()
    mutated.close()
    manifest_path = (out / "current" / "manifest.json").resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["databases"]["paper"]["snapshot_sha256"] = sha256_file(paper)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    status = evaluator_snapshot_bundle_status(
        signal_db=str(out / "current" / "signal.db"),
        paper_db=str(paper),
        raw_db=str(out / "current" / "raw.db"),
        kline_db=str(out / "current" / "kline.db"),
        data_dir=str(live),
        manifest_path=str(manifest_path),
    )

    assert status["accepted"] is False
    assert "evaluator_snapshot_paper_future_rows_detected:paper_trades" in status["blockers"]


def test_snapshot_lease_pins_immutable_paths_and_blocks_publish_lock(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    lock_file = tmp_path / "snapshot.lock"
    kwargs = {
        "signal_db": str(out / "current" / "signal.db"),
        "paper_db": str(out / "current" / "paper_evidence.db"),
        "raw_db": str(out / "current" / "raw.db"),
        "kline_db": str(out / "current" / "kline.db"),
        "data_dir": str(live),
        "manifest_path": str(out / "current" / "manifest.json"),
    }

    with evaluator_snapshot_bundle_lease(lock_file=str(lock_file), **kwargs) as status:
        assert all("/current/" not in path for path in status["databases"].values())
        competing = lock_file.open("a+")
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            competing.close()

    competing = lock_file.open("a+")
    try:
        fcntl.flock(competing.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(competing.fileno(), fcntl.LOCK_UN)
    finally:
        competing.close()


def test_snapshot_lease_detects_consumer_mutation_before_release(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    lock_file = tmp_path / "snapshot.lock"
    kwargs = {
        "signal_db": str(out / "current" / "signal.db"),
        "paper_db": str(out / "current" / "paper_evidence.db"),
        "raw_db": str(out / "current" / "raw.db"),
        "kline_db": str(out / "current" / "kline.db"),
        "data_dir": str(live),
        "manifest_path": str(out / "current" / "manifest.json"),
    }

    with pytest.raises(RuntimeError, match="evaluator_snapshot_paper_sha256_mismatch"):
        with evaluator_snapshot_bundle_lease(lock_file=str(lock_file), **kwargs) as status:
            paper = Path(status["databases"]["paper"])
            with paper.open("r+b") as handle:
                handle.seek(-1, 2)
                original = handle.read(1)
                handle.seek(-1, 2)
                handle.write(bytes([original[0] ^ 0x01]))


def test_snapshot_lease_revalidates_after_evaluator_exception(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    lock_file = tmp_path / "snapshot.lock"
    kwargs = {
        "signal_db": str(out / "current" / "signal.db"),
        "paper_db": str(out / "current" / "paper_evidence.db"),
        "raw_db": str(out / "current" / "raw.db"),
        "kline_db": str(out / "current" / "kline.db"),
        "data_dir": str(live),
        "manifest_path": str(out / "current" / "manifest.json"),
    }

    with pytest.raises(RuntimeError, match="evaluator_snapshot_paper_sha256_mismatch"):
        with evaluator_snapshot_bundle_lease(lock_file=str(lock_file), **kwargs) as status:
            paper = Path(status["databases"]["paper"])
            with paper.open("r+b") as handle:
                handle.seek(-1, 2)
                original = handle.read(1)
                handle.seek(-1, 2)
                handle.write(bytes([original[0] ^ 0x01]))
            raise ValueError("evaluator_failed_after_mutation")


def test_snapshot_lease_preserves_original_exception_when_integrity_is_clean(tmp_path, monkeypatch):
    live, _sources, out = create_valid_bundle(tmp_path, monkeypatch)
    lock_file = tmp_path / "snapshot.lock"
    kwargs = {
        "signal_db": str(out / "current" / "signal.db"),
        "paper_db": str(out / "current" / "paper_evidence.db"),
        "raw_db": str(out / "current" / "raw.db"),
        "kline_db": str(out / "current" / "kline.db"),
        "data_dir": str(live),
        "manifest_path": str(out / "current" / "manifest.json"),
    }

    with pytest.raises(ValueError, match="ordinary_evaluator_failure"):
        with evaluator_snapshot_bundle_lease(lock_file=str(lock_file), **kwargs):
            raise ValueError("ordinary_evaluator_failure")
