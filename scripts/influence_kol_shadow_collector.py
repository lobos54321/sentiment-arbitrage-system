#!/usr/bin/env python3
"""Collect or normalize Influence/KOL shadow-source snapshots.

Default mode is offline normalization of cached files. `--execute` may run read-only
commands from the plan, but only whitelisted Twitter/X read commands are allowed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "influence_kol_shadow_collector.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RUN_DIR = DEFAULT_DATA_DIR / "agent_runs/latest"
DEFAULT_RAW_DIR = DEFAULT_DATA_DIR / "influence_kol/raw"


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


def safe_command(command):
    command = str(command or "").strip()
    allowed_prefixes = (
        "opencli twitter search ",
        "opencli twitter user-posts ",
        "opencli twitter article ",
        "twitter search ",
        "twitter user-posts ",
        "twitter tweet ",
        "twitter article ",
    )
    blocked_terms = (" post ", " like ", " follow ", " dm ", " reply ", " delete ")
    return command.startswith(allowed_prefixes) and not any(term in f" {command} " for term in blocked_terms)


def run_command(command, raw_dir, timeout_sec):
    raw_dir.mkdir(parents=True, exist_ok=True)
    if not safe_command(command):
        return {"command": command, "ok": False, "reason": "command_not_whitelisted"}
    name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", command)[:120] or "command"
    out_path = raw_dir / f"{int(time.time())}_{name}.txt"
    proc = subprocess.run(
        command,
        shell=True,
        check=False,
        capture_output=True,
        text=True,
        timeout=int(timeout_sec),
    )
    out_path.write_text(proc.stdout + ("\nSTDERR:\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")
    return {
        "command": command,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "raw_path": str(out_path),
    }


def iter_snapshot_texts(raw_dir):
    root = Path(raw_dir)
    if not root.exists():
        return []
    texts = []
    for path in sorted(root.glob("*")):
        if path.is_dir() or path.suffix.lower() not in {".json", ".yaml", ".yml", ".txt"}:
            continue
        try:
            texts.append({"path": str(path), "text": path.read_text(encoding="utf-8", errors="replace")})
        except Exception:
            continue
    return texts


def normalize_features(raw_dir, queries, handles):
    snapshots = iter_snapshot_texts(raw_dir)
    combined = "\n".join(item["text"] for item in snapshots)
    lower = combined.lower()
    query_rows = []
    for query in queries:
        tokens = [tok.lower() for tok in re.split(r"\W+", query) if len(tok) >= 3]
        count = sum(lower.count(tok) for tok in tokens)
        query_rows.append({"query": query, "token_count": len(tokens), "mention_count": count})
    handle_rows = []
    for handle in handles:
        clean = handle.lower().lstrip("@")
        handle_rows.append({"handle": handle, "mention_count": lower.count(clean)})
    return {
        "snapshot_file_count": len(snapshots),
        "snapshot_paths": [item["path"] for item in snapshots[-20:]],
        "query_features": query_rows,
        "handle_features": handle_rows,
        "total_text_bytes": sum(len(item["text"].encode("utf-8", errors="ignore")) for item in snapshots),
    }


def split_csv(value):
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def build_report(args):
    plan = load_json(args.plan)
    commands = [row.get("command") for row in plan.get("read_only_commands") or [] if row.get("command")]
    raw_dir = Path(args.raw_dir)
    command_results = []
    if args.execute:
        for command in commands[: int(args.max_commands)]:
            command_results.append(run_command(command, raw_dir, int(args.timeout_sec)))
    queries = split_csv(args.queries)
    handles = split_csv(args.handles)
    if not queries:
        queries = [row.get("value") for row in plan.get("read_only_commands") or [] if row.get("type") == "query" and row.get("value")]
    if not handles:
        handles = [row.get("value") for row in plan.get("read_only_commands") or [] if row.get("type") == "handle" and row.get("value")]
    features = normalize_features(raw_dir, queries, handles)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": "INFLUENCE_KOL_SHADOW_FEATURES_MATERIALIZED",
        "allowed_use": "shadow_only_context",
        "promotion_allowed": False,
        "runtime_dependency_on_live_x_allowed": False,
        "write_actions_allowed": False,
        "execute_requested": bool(args.execute),
        "command_results": command_results,
        "features": features,
        "next_action": (
            "review_shadow_features_after_social_snapshots_accumulate"
            if features["snapshot_file_count"] > 0
            else "collect_agent_reach_twitter_snapshots_offline"
        ),
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_dir = root / "raw"
        raw_dir.mkdir()
        (raw_dir / "sample.json").write_text(json.dumps([
            {"text": "KOL says pump fun Solana meme is moving"},
            {"text": "another pump fun mention"},
        ]), encoding="utf-8")
        plan = root / "plan.json"
        plan.write_text(json.dumps({"read_only_commands": []}), encoding="utf-8")
        out = root / "out.json"
        args = argparse.Namespace(
            plan=str(plan),
            raw_dir=str(raw_dir),
            queries="pump fun,solana meme",
            handles="@kol",
            execute=False,
            max_commands=5,
            timeout_sec=20,
            out=str(out),
        )
        report = build_report(args)
        write_json(out, report)
        assert report["features"]["snapshot_file_count"] == 1
        assert report["features"]["query_features"][0]["mention_count"] >= 2
        assert report["promotion_allowed"] is False
    print("SELF_TEST_PASS influence_kol_shadow_collector")


def parse_args():
    parser = argparse.ArgumentParser(description="Collect or normalize Influence/KOL shadow snapshots.")
    parser.add_argument("--plan", default=str(DEFAULT_RUN_DIR / "influence_kol_shadow_source_plan.json"))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--queries", default="")
    parser.add_argument("--handles", default="")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-commands", default="5")
    parser.add_argument("--timeout-sec", default="20")
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "influence_kol_shadow_features_24h.json"))
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
        "snapshot_file_count": report["features"]["snapshot_file_count"],
        "promotion_allowed": False,
        "next_action": report["next_action"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
