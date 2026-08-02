import fcntl
from pathlib import Path
import sqlite3

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
)
from cross_db_evaluator_snapshot import build_snapshot_bundle  # noqa: E402


def create_live_sources(root):
    root.mkdir(exist_ok=True)
    definitions = {
        "signal": ("sentiment_arb.db", "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)"),
        "paper": (
            "paper_trades.db",
            "CREATE TABLE candidate_shadow_observations(signal_id INTEGER, observed_at INTEGER);"
            "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
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
