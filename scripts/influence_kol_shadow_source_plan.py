#!/usr/bin/env python3
"""Generate the Phase 3 Influence/KOL shadow-source acquisition plan.

This script does not fetch social data by default. It records which agent-reach X/Twitter
backend is available and emits read-only acquisition commands for a later shadow worker.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "influence_kol_shadow_source_plan.v1"
DEFAULT_RUN_DIR = Path("/app/data/agent_runs/latest")


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


def run_doctor(timeout_sec):
    try:
        proc = subprocess.run(
            ["agent-reach", "doctor", "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=int(timeout_sec),
        )
        if proc.returncode != 0:
            return {"error": "agent_reach_doctor_failed", "returncode": proc.returncode, "stderr": proc.stderr[-1000:]}
        return json.loads(proc.stdout)
    except FileNotFoundError:
        return {"error": "agent_reach_not_installed"}
    except Exception as exc:
        return {"error": str(exc)}


def split_csv(value):
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def twitter_backend(doctor):
    tw = doctor.get("twitter") if isinstance(doctor, dict) else None
    if not isinstance(tw, dict):
        return {"status": "unknown", "active_backend": None, "message": "doctor_twitter_section_missing"}
    return {
        "status": tw.get("status"),
        "active_backend": tw.get("active_backend"),
        "message": tw.get("message"),
        "backends": tw.get("backends") or [],
    }


def command_for_query(query, backend):
    active = (backend.get("active_backend") or "").lower()
    if "opencli" in active:
        return f"opencli twitter search {json.dumps(query)} -f yaml"
    if "twitter-cli" in active or active == "twitter-cli":
        return f"twitter search {json.dumps(query)} -n 50"
    return f"# backend unavailable; queue query only: {query}"


def command_for_handle(handle, backend):
    clean = handle if str(handle).startswith("@") else f"@{handle}"
    active = (backend.get("active_backend") or "").lower()
    if "opencli" in active:
        return f"opencli twitter user-posts {clean} -f yaml"
    if "twitter-cli" in active or active == "twitter-cli":
        return f"twitter user-posts {clean} -n 50"
    return f"# backend unavailable; queue handle only: {clean}"


def build_report(args):
    doctor = load_json(args.doctor_json)
    if args.run_doctor and not doctor:
        doctor = run_doctor(args.doctor_timeout_sec)
    backend = twitter_backend(doctor)
    queries = split_csv(args.queries)
    handles = split_csv(args.handles)
    commands = []
    for query in queries:
        commands.append({"type": "query", "value": query, "command": command_for_query(query, backend)})
    for handle in handles:
        commands.append({"type": "handle", "value": handle, "command": command_for_handle(handle, backend)})
    if not commands:
        commands = [
            {
                "type": "template",
                "value": "token_or_narrative_query",
                "command": command_for_query("$TOKEN OR narrative keyword", backend),
            },
            {
                "type": "template",
                "value": "@kol_handle",
                "command": command_for_handle("@kol_handle", backend),
            },
        ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": "INFLUENCE_KOL_SHADOW_SOURCE_PLAN_READY",
        "acquisition_backend": "agent_reach_x_twitter",
        "twitter_backend": backend,
        "shadow_only": True,
        "promotion_allowed": False,
        "production_impact": "zero",
        "runtime_dependency_on_live_x_allowed": False,
        "write_actions_allowed": False,
        "cache_contract": {
            "raw_snapshot_dir": "/app/data/influence_kol/raw",
            "normalized_artifact": "/app/data/agent_runs/latest/influence_kol_shadow_features_24h.json",
            "ttl_hours": 24,
            "missing_social_data_must_not_block_runtime": True,
            "features_allowed_use": "shadow_only_context",
        },
        "read_only_commands": commands,
        "blocked_actions": ["post", "like", "reply", "follow", "dm", "runtime_trade_decision_dependency"],
        "next_action": (
            "run_shadow_collection_with_agent_reach"
            if backend.get("status") == "ok"
            else "configure_agent_reach_twitter_backend"
        ),
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        doctor = {
            "twitter": {
                "status": "ok",
                "active_backend": "OpenCLI",
                "message": "OpenCLI available",
                "backends": ["twitter-cli", "OpenCLI"],
            }
        }
        doctor_path = root / "doctor.json"
        out = root / "out.json"
        doctor_path.write_text(json.dumps(doctor), encoding="utf-8")
        args = argparse.Namespace(
            doctor_json=str(doctor_path),
            run_doctor=False,
            doctor_timeout_sec=10,
            queries="pump fun,solana meme",
            handles="@example",
            out=str(out),
        )
        report = build_report(args)
        write_json(out, report)
        assert report["twitter_backend"]["active_backend"] == "OpenCLI"
        assert report["promotion_allowed"] is False
        assert report["write_actions_allowed"] is False
        assert any("opencli twitter search" in row["command"] for row in report["read_only_commands"])
    print("SELF_TEST_PASS influence_kol_shadow_source_plan")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Influence/KOL shadow-source plan.")
    parser.add_argument("--doctor-json")
    parser.add_argument("--run-doctor", action="store_true")
    parser.add_argument("--doctor-timeout-sec", default="10")
    parser.add_argument("--queries", default="")
    parser.add_argument("--handles", default="")
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "influence_kol_shadow_source_plan.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "classification": report["classification"],
        "twitter_backend": report["twitter_backend"].get("active_backend"),
        "promotion_allowed": False,
        "next_action": report["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
