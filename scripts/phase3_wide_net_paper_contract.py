#!/usr/bin/env python3
"""Materialize the Phase 3 wide-net paper experiment contract.

This script is governance-only by default. It never enables paper trading. It can
optionally create an independent ledger schema for a future human-approved paper
experiment, but no experiment rows are inserted here.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "phase3_wide_net_paper_contract.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RUN_DIR = DEFAULT_DATA_DIR / "agent_runs/latest"
DEFAULT_APPROVAL_MARKER = DEFAULT_DATA_DIR / "phase3_wide_net_paper_approval.json"
P7_CHAMPION_POLICY_ID = "trail_a50_dd15_stop20"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def load_json(path):
    try:
        if not path or not Path(path).exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def truthy(value):
    return str(value).lower() in {"1", "true", "yes", "approved", "enable", "enabled"}


def write_approval_marker(path, approval_text):
    marker = {
        "schema_version": "phase3_wide_net_paper_approval.v1",
        "approved_at": utc_now(),
        "approval_text": str(approval_text or "").strip(),
        "human_approved": True,
        "enable_requested": True,
        "allowed_scope": "independent_phase3_paper_experiment_ledger_only",
        "promotion_allowed": False,
        "production_strategy_change_allowed": False,
        "entry_policy_change_allowed": False,
        "gate_change_allowed": False,
        "final_entry_contract_change_allowed": False,
        "executor_change_allowed": False,
        "canary_or_risk_change_allowed": False,
    }
    write_json(path, marker)
    return marker


def load_approval_marker(path):
    marker = load_json(path)
    if not marker:
        return {}
    return marker if isinstance(marker, dict) else {}


def init_contract_db(path, contract):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(target)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS phase3_wide_net_paper_contracts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          generated_at TEXT NOT NULL,
          contract_version TEXT NOT NULL,
          status TEXT NOT NULL,
          champion_policy_id TEXT NOT NULL,
          paper_size_sol REAL NOT NULL,
          payload_json TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS phase3_wide_net_paper_ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id TEXT,
          token_ca TEXT,
          signal_ts INTEGER,
          intended_size_sol REAL,
          quote_executable INTEGER,
          entry_intent_ts INTEGER,
          entry_result TEXT,
          exit_policy_id TEXT,
          hard_stop_pct REAL,
          fees_sol REAL,
          slippage_pct REAL,
          failed_quote_reason TEXT,
          no_fill_reason TEXT,
          timeout_reason TEXT,
          realized_pnl_pct REAL,
          payload_json TEXT,
          created_at INTEGER NOT NULL
        )
        """
    )
    db.execute(
        """
        INSERT INTO phase3_wide_net_paper_contracts (
          generated_at, contract_version, status, champion_policy_id, paper_size_sol, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            contract["generated_at"],
            contract["schema_version"],
            contract["classification"],
            contract["experiment_defaults"]["exit_policy_id"],
            float(contract["experiment_defaults"]["paper_size_sol"]),
            json.dumps(contract, sort_keys=True),
        ),
    )
    db.commit()
    db.close()


def build_contract(args):
    p7 = load_json(args.p7)
    approval_marker = load_approval_marker(args.approval_marker)
    p7_open = p7.get("classification") == "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN"
    human_approved = (
        truthy(args.human_approved)
        or truthy(os.environ.get("PHASE3_WIDE_NET_PAPER_HUMAN_APPROVED", "0"))
        or bool(approval_marker.get("human_approved"))
    )
    enable_requested = (
        truthy(args.enable_requested)
        or truthy(os.environ.get("PHASE3_WIDE_NET_PAPER_ENABLED", "0"))
        or bool(approval_marker.get("enable_requested"))
    )
    enablement_allowed = bool(p7_open and human_approved and enable_requested)
    if enablement_allowed:
        classification = "WIDE_NET_PAPER_ENABLED_BY_HUMAN_APPROVAL"
        next_action = "run_phase3_wide_net_paper_worker"
    elif p7_open:
        classification = "WIDE_NET_PAPER_READY_BUT_DISABLED"
        next_action = "wait_for_explicit_human_enablement"
    else:
        classification = "WIDE_NET_PAPER_BLOCKED_WAITING_FOR_P7_CHECKPOINT"
        next_action = "wait_for_p7_paper_proposal_checkpoint"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": classification,
        "proposal_source": {
            "p7_checkpoint_path": str(args.p7),
            "p7_checkpoint_open": p7_open,
            "p7_classification": p7.get("classification"),
        },
        "experiment_defaults": {
            "entry_scope": "all_eligible_signals",
            "paper_size_sol": 0.001,
            "quote_executable_required": True,
            "hard_stop_pct_range": [-20.0, -10.0],
            "exit_policy_id": P7_CHAMPION_POLICY_ID,
            "minimum_run_days": 14,
            "include_fees_slippage_failed_quotes_no_fills_timeouts": True,
            "independent_ledger_path": str(args.contract_db),
        },
        "enablement": {
            "human_approval_required": True,
            "human_approved": human_approved,
            "enable_requested": enable_requested,
            "paper_experiment_enablement_allowed": enablement_allowed,
            "approval_marker_path": str(args.approval_marker),
            "approval_marker_present": bool(approval_marker),
            "approval_marker_approved_at": approval_marker.get("approved_at"),
            "approval_marker_allowed_scope": approval_marker.get("allowed_scope"),
            "required_env_flags": [
                "PHASE3_WIDE_NET_PAPER_HUMAN_APPROVED=1",
                "PHASE3_WIDE_NET_PAPER_ENABLED=1",
            ],
            "note": (
                "This script never enables production paper trading. When enabled, it only authorizes "
                "the independent Phase 3 paper experiment ledger worker."
            ),
        },
        "guardrails": {
            "promotion_allowed": False,
            "production_strategy_change_allowed": False,
            "entry_policy_change_allowed": False,
            "gate_change_allowed": False,
            "final_entry_contract_change_allowed": False,
            "executor_change_allowed": False,
            "canary_or_risk_change_allowed": False,
        },
        "next_action": next_action,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        p7 = root / "p7.json"
        p7.write_text(json.dumps({"classification": "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN"}), encoding="utf-8")
        out = root / "contract.json"
        db_path = root / "contract.db"
        args = argparse.Namespace(
            p7=str(p7),
            contract_db=str(db_path),
            out=str(out),
            approval_marker=str(root / "approval.json"),
            materialize_contract_db=True,
            human_approved="0",
            enable_requested="0",
        )
        contract = build_contract(args)
        write_json(out, contract)
        init_contract_db(db_path, contract)
        assert contract["classification"] == "WIDE_NET_PAPER_READY_BUT_DISABLED"
        assert contract["enablement"]["paper_experiment_enablement_allowed"] is False
        assert contract["guardrails"]["promotion_allowed"] is False
        assert db_path.exists()
        marker = write_approval_marker(root / "approval.json", "approved in self-test")
        assert marker["human_approved"] is True
        approved = build_contract(args)
        assert approved["classification"] == "WIDE_NET_PAPER_ENABLED_BY_HUMAN_APPROVAL"
        assert approved["enablement"]["paper_experiment_enablement_allowed"] is True
    print("SELF_TEST_PASS phase3_wide_net_paper_contract")


def parse_args():
    parser = argparse.ArgumentParser(description="Materialize Phase 3 wide-net paper contract.")
    parser.add_argument("--p7", default=str(DEFAULT_RUN_DIR / "p7_paper_proposal_checkpoint.json"))
    parser.add_argument("--contract-db", default=str(DEFAULT_DATA_DIR / "phase3_wide_net_paper_contract.db"))
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "phase3_wide_net_paper_contract.json"))
    parser.add_argument("--approval-marker", default=str(DEFAULT_APPROVAL_MARKER))
    parser.add_argument("--record-approval", default="")
    parser.add_argument("--human-approved", default="0")
    parser.add_argument("--enable-requested", default="0")
    parser.add_argument("--materialize-contract-db", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.record_approval:
        write_approval_marker(args.approval_marker, args.record_approval)
    contract = build_contract(args)
    write_json(args.out, contract)
    if args.materialize_contract_db:
        init_contract_db(args.contract_db, contract)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "classification": contract["classification"],
        "paper_experiment_enablement_allowed": contract["enablement"]["paper_experiment_enablement_allowed"],
        "promotion_allowed": False,
        "next_action": contract["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
