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
    require_evaluator_db_source,
    require_evaluator_snapshot_bundle,
    sha256_file,
)
from cross_db_evaluator_snapshot import build_snapshot_bundle  # noqa: E402


def create_live_sources(root):
    root.mkdir(exist_ok=True)
    definitions = {
        "signal": ("sentiment_arb.db", "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)"),
        "paper": (
            "paper_trades.db",
            "CREATE TABLE candidate_shadow_observations(signal_id INTEGER, observed_at INTEGER);"
            "CREATE INDEX idx_candidate_shadow_obs_observed "
            "ON candidate_shadow_observations(observed_at);"
            "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
            "CREATE INDEX idx_candidate_shadow_virtual_observed "
            "ON candidate_shadow_virtual_trades(observed_at);"
            "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
            "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
            "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
            "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER)",
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


def create_valid_bundle(tmp_path, monkeypatch):
    live = tmp_path / "live"
    sources = create_live_sources(live)
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "a" * 40)
    out = live / "agent_evidence"
    build_snapshot_bundle(
        sources=sources,
        out_root=str(out),
        repo_root=str(ROOT),
        max_skew_sec=30,
        min_free_after_gib=0,
        snapshot_id="20260101T000000Z-1234abcd",
    )
    return live, sources, out


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
