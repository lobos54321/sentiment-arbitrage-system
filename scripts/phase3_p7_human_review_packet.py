#!/usr/bin/env python3
"""Build the human review packet for the P7 paper-level experiment checkpoint.

Read-only governance artifact. It does not enable paper trading or modify runtime behavior.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "phase3_p7_human_review_packet.v1"
DEFAULT_RUN_DIR = Path("/app/data/agent_runs/latest")
APPROVAL_PHRASE = "\u6279\u51c6 P7 paper-level \u5b9e\u9a8c"
REJECTION_PHRASE = "\u4e0d\u6279\u51c6\uff0c\u7ee7\u7eed shadow-only"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    try:
        if not path or not Path(path).exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def write_json(path, payload):
    write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def build_packet(p7, contract, phase3):
    p7_open = p7.get("classification") == "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN"
    contract_ready = contract.get("classification") == "WIDE_NET_PAPER_READY_BUT_DISABLED"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": "P7_HUMAN_REVIEW_PACKET_READY" if p7_open and contract_ready else "P7_HUMAN_REVIEW_PACKET_INCOMPLETE",
        "decision_required": "approve_or_reject_p7_paper_level_experiment",
        "champion_policy_id": p7.get("champion_policy_id") or "trail_a50_dd15_stop20",
        "evidence_summary": {
            "p7_checkpoint_classification": p7.get("classification"),
            "two_windows_complete": (p7.get("p7_oos") or {}).get("two_windows_complete"),
            "two_windows_same_positive_direction": (p7.get("p7_oos") or {}).get("two_windows_same_positive_direction"),
            "stop_fill_stress_champion_stable": (p7.get("p7_oos") or {}).get("stop_fill_stress_champion_stable"),
            "real_paper_evidence_separate_from_replay": True,
            "production_promotion_evidence": False,
        },
        "experiment_contract": contract.get("experiment_defaults") or {},
        "required_human_decision_options": [
            {
                "decision": "APPROVE_P7_PAPER_LEVEL_EXPERIMENT",
                "effect": "permits a later explicit paper enablement step using the recorded contract",
                "does_not_allow": ["production_promotion", "live_size_increase", "gate_relaxation"],
            },
            {
                "decision": "REJECT_OR_CONTINUE_SHADOW_ONLY",
                "effect": "keeps P7 in shadow/read-only evidence accumulation",
                "does_not_allow": ["paper_enablement"],
            },
        ],
        "approval_requirements": {
            "explicit_user_approval_text_required": True,
            "required_phrase": APPROVAL_PHRASE,
            "env_flags_still_required_for_enablement": (contract.get("enablement") or {}).get("required_env_flags") or [],
            "paper_experiment_enablement_allowed_now": False,
        },
        "guardrails": {
            "promotion_allowed": False,
            "production_exit_policy_change_allowed": False,
            "production_strategy_change_allowed": False,
            "entry_policy_change_allowed": False,
            "gate_change_allowed": False,
            "final_entry_contract_change_allowed": False,
            "executor_change_allowed": False,
            "canary_or_risk_change_allowed": False,
        },
        "phase3_context": {
            "phase": phase3.get("phase"),
            "next_action": phase3.get("next_action"),
            "task_status": {item.get("task_id"): item.get("status") for item in phase3.get("tasks") or []},
        },
        "next_action": "wait_for_human_approval_or_rejection",
    }


def markdown(packet):
    evidence = packet.get("evidence_summary") or {}
    contract = packet.get("experiment_contract") or {}
    lines = [
        "# P7 Human Review Packet",
        "",
        f"generated_at: `{packet.get('generated_at')}`",
        f"classification: `{packet.get('classification')}`",
        f"decision_required: `{packet.get('decision_required')}`",
        f"champion_policy_id: `{packet.get('champion_policy_id')}`",
        "",
        "## Evidence",
        "",
        f"- two_windows_complete: `{evidence.get('two_windows_complete')}`",
        f"- two_windows_same_positive_direction: `{evidence.get('two_windows_same_positive_direction')}`",
        f"- stop_fill_stress_champion_stable: `{evidence.get('stop_fill_stress_champion_stable')}`",
        "- real paper evidence is separate from replay and is not production promotion evidence",
        "",
        "## Paper Experiment Contract",
        "",
        f"- entry_scope: `{contract.get('entry_scope')}`",
        f"- paper_size_sol: `{contract.get('paper_size_sol')}`",
        f"- quote_executable_required: `{contract.get('quote_executable_required')}`",
        f"- exit_policy_id: `{contract.get('exit_policy_id')}`",
        f"- hard_stop_pct_range: `{contract.get('hard_stop_pct_range')}`",
        f"- minimum_run_days: `{contract.get('minimum_run_days')}`",
        "",
        "## Guardrails",
        "",
        "- No production promotion.",
        "- No production exit-policy change.",
        "- No gate, final_entry_contract, executor, canary, wallet, or risk change.",
        "- Paper enablement still requires explicit approval and enablement flags.",
        "",
        "## Decision",
        "",
        "Approve with exact text:",
        "",
        "```text",
        APPROVAL_PHRASE,
        "```",
        "",
        "Or reject/continue shadow-only:",
        "",
        "```text",
        REJECTION_PHRASE,
        "```",
        "",
    ]
    return "\n".join(lines)


def run(args):
    p7 = load_json(Path(args.p7))
    contract = load_json(Path(args.contract))
    phase3 = load_json(Path(args.phase3))
    packet = build_packet(p7, contract, phase3)
    write_json(args.out, packet)
    write_text(args.out_md, markdown(packet))
    return packet


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        p7 = {
            "classification": "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN",
            "champion_policy_id": "trail_a50_dd15_stop20",
            "p7_oos": {
                "two_windows_complete": True,
                "two_windows_same_positive_direction": True,
                "stop_fill_stress_champion_stable": True,
            },
        }
        contract = {
            "classification": "WIDE_NET_PAPER_READY_BUT_DISABLED",
            "experiment_defaults": {"entry_scope": "all_eligible_signals", "paper_size_sol": 0.001},
            "enablement": {"required_env_flags": ["A=1"]},
        }
        phase3 = {"phase": "phase3_real_quote_tail_validation", "tasks": [{"task_id": "P3.3", "status": "READY"}]}
        for name, data in [("p7.json", p7), ("contract.json", contract), ("phase3.json", phase3)]:
            (root / name).write_text(json.dumps(data), encoding="utf-8")
        args = argparse.Namespace(
            p7=str(root / "p7.json"),
            contract=str(root / "contract.json"),
            phase3=str(root / "phase3.json"),
            out=str(root / "packet.json"),
            out_md=str(root / "packet.md"),
        )
        packet = run(args)
        assert packet["classification"] == "P7_HUMAN_REVIEW_PACKET_READY"
        assert packet["approval_requirements"]["required_phrase"] == APPROVAL_PHRASE
        assert packet["guardrails"]["promotion_allowed"] is False
    print("SELF_TEST_PASS phase3_p7_human_review_packet")


def parse_args():
    parser = argparse.ArgumentParser(description="Build P7 human review packet.")
    parser.add_argument("--p7", default=str(DEFAULT_RUN_DIR / "p7_paper_proposal_checkpoint.json"))
    parser.add_argument("--contract", default=str(DEFAULT_RUN_DIR / "phase3_wide_net_paper_contract.json"))
    parser.add_argument("--phase3", default=str(DEFAULT_RUN_DIR / "phase3_goal_loop.json"))
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "phase3_p7_human_review_packet.json"))
    parser.add_argument("--out-md", default=str(DEFAULT_RUN_DIR / "phase3_p7_human_review_packet.md"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    packet = run(args)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "classification": packet["classification"],
        "decision_required": packet["decision_required"],
        "promotion_allowed": False,
        "next_action": packet["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
