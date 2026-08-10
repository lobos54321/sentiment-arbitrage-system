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
    require_evaluator_db_source,
    require_evaluator_snapshot_bundle,
    sha256_file,
)
from cross_db_evaluator_snapshot import (  # noqa: E402
    build_snapshot_bundle,
    shared_stage_budget_evidence_sha256,
    shared_stage_budget_plan_sha256,
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
            "ON opportunity_event_path_samples(opportunity_key, sample_ts)",
        ),
        "raw": ("raw_signal_outcomes.db", "CREATE TABLE raw_signal_outcomes(id INTEGER, signal_id INTEGER, updated_at INTEGER)"),
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
):
    live = tmp_path / "live"
    sources = create_live_sources(live)
    if not include_optional_path_samples:
        paper = sqlite3.connect(sources["paper"])
        paper.execute("DROP TABLE opportunity_event_path_samples")
        paper.commit()
        paper.close()
    if seed_shared_stage_rows:
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


def test_missing_evidence_db_is_rejected(tmp_path):
    status = evaluator_db_source_status(
        str(tmp_path / "agent_evidence" / "paper_evidence.db"),
        str(tmp_path),
    )

    assert status["accepted"] is False
    assert status["blockers"] == ["evaluator_db_missing"]
    assert status["promotion_allowed"] is False


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
    provenance = evaluator_snapshot_provenance(status)
    assert provenance["schema_version"] == "evaluator_snapshot_provenance.v1"
    assert provenance["accepted"] is True
    assert provenance["snapshot_id"] == status["snapshot_id"]
    assert provenance["manifest_sha256"] == status["manifest_sha256"]
    assert provenance["producer_manifest_sha256"] == status["manifest_sha256"]
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
        ("stage_create_sql_sha256", "0" * 64),
        ("destination_create_sql_sha256", "0" * 64),
        ("source_column_contract_sha256", "0" * 64),
        ("stage_column_contract_sha256", "0" * 64),
        ("destination_column_contract_sha256", "0" * 64),
        ("stage_column_count", 999),
        ("stage_column_contract_passed", False),
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
        "baseline_below_estimate",
        "negative_actual",
        "null_grant",
        "estimate_sample_used",
        "estimate_payload_upper_tamper",
        "estimate_physical_bytes_tamper",
        "estimate_formula_tamper",
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
    elif mutation == "baseline_below_estimate":
        p9["baseline_required_bytes"] = p9["estimated_required_bytes"] - 4096
        p9["borrowed_shared_pool_bytes"] = (
            p9["granted_cap_bytes"] - p9["baseline_required_bytes"]
        )
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
    elif mutation == "estimate_sample_used":
        p9["estimate_evidence"]["capacity_sample_used"] = True
        synchronize_shared_budget_copies(manifest)
    elif mutation == "estimate_payload_upper_tamper":
        p9["estimate_evidence"]["selected_payload_upper_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "estimate_physical_bytes_tamper":
        p9["estimate_evidence"]["source_dbstat_physical_bytes"] += 4096
        synchronize_shared_budget_copies(manifest)
    elif mutation == "estimate_formula_tamper":
        p9["estimate_evidence"]["upper_bound_formula"] = (
            "edge_sample_average_times_selected_rows"
        )
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
        "estimate_read_view_id_mismatch",
        "estimate_read_view_role_mismatch",
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
    if mutation == "estimate_read_view_id_mismatch":
        p9["estimate_evidence"]["pinned_read_view_id"] = candidate[
            "estimate_evidence"
        ]["pinned_read_view_id"]
        synchronize_shared_budget_copies(manifest)
    elif mutation == "estimate_read_view_role_mismatch":
        p9["estimate_evidence"]["pinned_read_view_role"] = (
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
