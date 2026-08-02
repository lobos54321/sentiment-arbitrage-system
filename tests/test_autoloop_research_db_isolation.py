import os
from pathlib import Path
import sqlite3
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from agent_capture_discovery_loop import (  # noqa: E402
    SHADOW_DECISION_BRIDGE_TABLE,
    resolve_evaluator_research_db,
    persist_shadow_decision_bridge_events,
)


def bridge_audit():
    return {
        "mirror_events": [
            {
                "schema_version": "shadow_decision_bridge_event.v1",
                "event_ts": 1,
                "signal_id": "signal-1",
                "token_ca": "token-1",
                "root_cause": "shadow_entry_hypotheses_matched_no_decision_bridge",
                "matched_entry_hypothesis_count": 1,
                "matched_entry_hypothesis_sample": ["candidate-a"],
                "source_artifact": "raw_gold_silver_funnel_audit",
            }
        ]
    }


def test_bridge_persistence_refuses_evaluator_snapshot_path(tmp_path):
    snapshot = tmp_path / "snapshot.db"
    connection = sqlite3.connect(snapshot)
    connection.execute("CREATE TABLE immutable_marker(value TEXT)")
    connection.commit()
    connection.close()
    before = snapshot.read_bytes()

    result = persist_shadow_decision_bridge_events(
        snapshot,
        bridge_audit(),
        forbidden_db_paths=(snapshot,),
        allowed_root=tmp_path,
    )

    assert result["available"] is False
    assert result["reason"] == "evaluator_database_write_forbidden"
    assert snapshot.read_bytes() == before


def test_bridge_persistence_uses_separate_research_database(tmp_path):
    research = tmp_path / "run" / "autoloop_research.db"

    result = persist_shadow_decision_bridge_events(
        research,
        bridge_audit(),
        allowed_root=research.parent,
    )

    assert result["available"] is True
    assert result["upserted_event_count"] == 1
    connection = sqlite3.connect(research)
    try:
        assert connection.execute(
            f"SELECT COUNT(*) FROM {SHADOW_DECISION_BRIDGE_TABLE}"
        ).fetchone()[0] == 1
    finally:
        connection.close()


def test_custom_research_database_must_remain_inside_run_directory(tmp_path):
    run_dir = tmp_path / "agent_runs" / "run-1"
    run_dir.mkdir(parents=True)
    live_paper = tmp_path / "paper_trades.db"

    try:
        resolve_evaluator_research_db(live_paper, run_dir)
    except RuntimeError as exc:
        assert "evaluator_research_db_outside_run_dir" in str(exc)
    else:
        raise AssertionError("active database path was accepted as evaluator research DB")


def test_bridge_persistence_rejects_active_db_even_if_allowed_root_is_broad(tmp_path):
    live_paper = tmp_path / "paper_trades.db"
    connection = sqlite3.connect(live_paper)
    connection.execute("CREATE TABLE live_marker(value TEXT)")
    connection.commit()
    connection.close()
    before = live_paper.read_bytes()

    result = persist_shadow_decision_bridge_events(
        live_paper,
        bridge_audit(),
        forbidden_db_paths=(live_paper,),
        allowed_root=tmp_path,
    )

    assert result["available"] is False
    assert result["reason"] == "evaluator_database_write_forbidden"
    assert live_paper.read_bytes() == before


def test_bridge_persistence_rejects_hard_link_alias_to_active_database(tmp_path):
    live_paper = tmp_path / "paper_trades.db"
    connection = sqlite3.connect(live_paper)
    connection.execute("CREATE TABLE live_marker(value TEXT)")
    connection.commit()
    connection.close()
    run_dir = tmp_path / "agent_runs" / "run-1"
    run_dir.mkdir(parents=True)
    alias = run_dir / "autoloop_research.db"
    os.link(live_paper, alias)
    before = live_paper.read_bytes()

    result = persist_shadow_decision_bridge_events(
        alias,
        bridge_audit(),
        forbidden_db_paths=(live_paper,),
        allowed_root=run_dir,
    )

    assert result["available"] is False
    assert result["reason"] == "evaluator_database_inode_forbidden"
    assert live_paper.read_bytes() == before
