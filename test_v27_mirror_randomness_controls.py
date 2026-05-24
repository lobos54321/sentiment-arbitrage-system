import json
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_mirror_randomness_controls import (  # noqa: E402
    DEFAULT_AUDIT_VERSION,
    mirror_randomness_controls,
    verify_randomness_control_mirror_parity,
)


def new_experiment_db(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    db.execute(
        """
        CREATE TABLE strategy_experiments (
            candidate_id TEXT PRIMARY KEY,
            parent_id TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            config_version INTEGER NOT NULL,
            mutation_set_json TEXT,
            dataset_refs_json TEXT,
            metrics_json TEXT,
            guardrail_results_json TEXT,
            strategy_config_json TEXT,
            notes TEXT,
            promoted_at TEXT,
            retired_at TEXT,
            qualified_at TEXT,
            activated_at TEXT,
            paused_at TEXT
        )
        """
    )
    db.commit()
    return db


def insert_experiment(
    db,
    *,
    candidate_id="candidate-rng-1",
    status="evaluating",
    created_by="autoresearch-loop",
    notes=None,
    strategy_config=None,
):
    db.execute(
        """
        INSERT OR REPLACE INTO strategy_experiments (
            candidate_id, parent_id, status, created_at, created_by, config_version,
            mutation_set_json, dataset_refs_json, metrics_json, guardrail_results_json,
            strategy_config_json, notes, promoted_at, retired_at, qualified_at, activated_at, paused_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate_id,
            "baseline-v1",
            status,
            "2026-04-02T21:23:57.020Z",
            created_by,
            2,
            json.dumps([{"path": "scoreThresholds.buy", "nextValue": 77}]),
            json.dumps(["zeabur-export"]),
            json.dumps({"sampleSize": 0}),
            json.dumps({}),
            json.dumps(strategy_config or {"scoreThresholds": {"buy": 77}}),
            json.dumps(notes) if isinstance(notes, dict) else notes,
            None,
            None,
            None,
            None,
            None,
        ),
    )
    db.commit()


def test_randomness_control_mirror_records_missing_rng_as_invalid_evidence(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(db)

    result = mirror_randomness_controls(db_path, event_log_dir)
    parity = verify_randomness_control_mirror_parity(db_path, event_log_dir)
    projection = build_denominator_projection(event_log_dir)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert result["read_rows"] == 1
    assert result["eligible_randomness_controls"] == 1
    assert result["valid_randomness_controls"] == 0
    assert result["malformed_randomness_controls"] == 1
    assert result["appended"] == 1
    assert parity["parity_ok"] is True
    assert projection["health"]["randomness_control_ok"] is False
    assert evidence["eligible_randomness_control_records"] == 1
    assert evidence["malformed_randomness_controls"][0]["assignment_id"] == "candidate-rng-1"
    assert evidence["malformed_randomness_controls"][0]["missing_fields"] == ["rng_seed", "rng_version"]

    event = next(V27EventLog(event_log_dir).iter_events())
    assert event["event_type"] == "randomness_control_recorded"
    assert event["idempotency_key"].startswith("randomness_control:candidate-rng-1:")
    assert event["payload"]["randomization_unit"] == "strategy_experiment_candidate"
    assert event["payload"]["randomness_control_audit_version"] == DEFAULT_AUDIT_VERSION
    assert event["payload"]["randomness_control_proof_level"] == "strategy_experiment_without_explicit_rng_control"


def test_randomness_control_mirror_records_explicit_rng_material(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(
            db,
            notes={
                "rng_seed": "sha256:unit-seed",
                "rng_version": "unit-rng-v1",
                "assignment_algorithm": "seeded_sha256_bucket",
                "assigned_bucket": "candidate",
            },
        )

    result = mirror_randomness_controls(db_path, event_log_dir)
    projection = build_denominator_projection(event_log_dir)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert result["valid_randomness_controls"] == 1
    assert result["malformed_randomness_controls"] == 0
    assert projection["health"]["randomness_control_ok"] is True
    assert evidence["valid_randomness_control_count"] == 1
    assert evidence["rng_versions"] == ["unit-rng-v1"]
    assert evidence["evidence_sources"] == ["strategy_experiments"]


def test_randomness_control_projection_uses_latest_assignment_event(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(db)

    first = mirror_randomness_controls(db_path, event_log_dir, new_only=True)
    with sqlite3.connect(str(db_path)) as db:
        db.execute(
            "UPDATE strategy_experiments SET notes = ? WHERE candidate_id = ?",
            (
                json.dumps({"rng_seed": "sha256:repaired-seed", "rng_version": "unit-rng-v2"}),
                "candidate-rng-1",
            ),
        )
        db.commit()
    second = mirror_randomness_controls(db_path, event_log_dir, new_only=True)
    parity = verify_randomness_control_mirror_parity(db_path, event_log_dir)
    projection = build_denominator_projection(event_log_dir)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert first["appended"] == 1
    assert second["appended"] == 1
    assert parity["parity_ok"] is True
    assert parity["superseded_mirrored_fingerprint_count"] == 1
    assert projection["randomness_control_recorded_events"] == 2
    assert projection["health"]["randomness_control_ok"] is True
    assert evidence["randomness_control_observation_count"] == 2
    assert evidence["current_randomness_control_count"] == 1
    assert evidence["superseded_randomness_control_event_count"] == 1
    assert evidence["valid_randomness_control_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["rng_versions"] == ["unit-rng-v2"]


def test_randomness_control_new_only_skips_unchanged_rows(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(db)

    first = mirror_randomness_controls(db_path, event_log_dir, new_only=True)
    second = mirror_randomness_controls(db_path, event_log_dir, new_only=True)

    assert first["appended"] == 1
    assert second["unchanged"] == 1
    assert second["appended"] == 0
    assert len(list(V27EventLog(event_log_dir).iter_events())) == 1


def test_randomness_control_new_only_limit_advances_past_unchanged_rows(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(db, candidate_id="candidate-rng-1")
        insert_experiment(db, candidate_id="candidate-rng-2")
        insert_experiment(db, candidate_id="candidate-rng-3")

    first = mirror_randomness_controls(db_path, event_log_dir, new_only=True, limit=2)
    second = mirror_randomness_controls(db_path, event_log_dir, new_only=True, limit=2)
    third = mirror_randomness_controls(db_path, event_log_dir, new_only=True, limit=2)

    assert first["appended"] == 2
    assert second["unchanged"] == 2
    assert second["appended"] == 1
    assert third["unchanged"] == 3
    assert third["appended"] == 0
    assert len(list(V27EventLog(event_log_dir).iter_events())) == 3


def test_randomness_control_dry_run_cli_exits_cleanly_without_verify(tmp_path):
    db_path = tmp_path / "experiments.db"
    event_log_dir = tmp_path / "events"
    with new_experiment_db(db_path) as db:
        insert_experiment(db)

    script = Path(__file__).resolve().parent / "scripts" / "v27_mirror_randomness_controls.py"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--db",
            str(db_path),
            "--event-log-dir",
            str(event_log_dir),
            "--dry-run",
            "--strict",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["mirror"]["dry_run"] is True
    assert payload["mirror"]["read_rows"] == 1
    assert payload["mirror"]["appended"] == 0
    assert payload["verify"] is None
