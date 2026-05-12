#!/usr/bin/env python3
"""Collect GMGN read-only candidate tokens into JSONL for paper review."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

from external_alpha_shadow import (
    connect_external_alpha_db,
    record_external_alpha_candidates,
    record_external_alpha_health,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = PROJECT_ROOT / "data" / "gmgn_candidates.jsonl"
GMGN_DEMO_API_KEY = "gmgn_solbscbaseethmonadtron"


def env_flag(name, default="false"):
    return os.environ.get(name, default).strip().lower() == "true"


def gmgn_cli_path():
    local_cli = PROJECT_ROOT / "node_modules" / ".bin" / "gmgn-cli"
    if local_cli.exists():
        return str(local_cli)
    return shutil.which("gmgn-cli") or "gmgn-cli"


def gmgn_scout_runtime_status():
    api_key = os.environ.get("GMGN_API_KEY", "").strip()
    allow_demo = env_flag("GMGN_SCOUT_ALLOW_DEMO_KEY") or env_flag("GMGN_ALLOW_DEMO_KEY")
    return {
        "gmgn_cli": gmgn_cli_path(),
        "api_key_present": bool(api_key),
        "api_key_prefix": api_key[:8] if api_key else "",
        "allow_demo_key": allow_demo,
    }


def run_gmgn(args, timeout=20):
    env = dict(os.environ)
    if (
        not env.get("GMGN_API_KEY")
        and (env_flag("GMGN_SCOUT_ALLOW_DEMO_KEY") or env_flag("GMGN_ALLOW_DEMO_KEY"))
    ):
        env["GMGN_API_KEY"] = GMGN_DEMO_API_KEY
    completed = subprocess.run(
        [gmgn_cli_path(), *args, "--raw"],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "gmgn-cli failed").strip())
    return json.loads(completed.stdout)


def as_list(payload, *paths):
    if isinstance(payload, list):
        return payload
    for path in paths:
        cur = payload
        for key in path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(key)
        if isinstance(cur, list):
            return cur
    return []


def normalize_token(raw, *, source, category=None, captured_at=None):
    raw = raw or {}
    base_token = raw.get("base_token") if isinstance(raw.get("base_token"), dict) else {}
    camel_base_token = raw.get("baseToken") if isinstance(raw.get("baseToken"), dict) else {}
    token = raw.get("token") if isinstance(raw.get("token"), dict) else {}
    address = (
        raw.get("ca")
        or raw.get("token_ca")
        or raw.get("address")
        or raw.get("base_address")
        or raw.get("token_address")
        or base_token.get("address")
        or camel_base_token.get("address")
        or token.get("address")
    )
    return {
        "captured_at": captured_at or int(time.time()),
        "source": source,
        "category": category,
        "chain": raw.get("chain") or "sol",
        "ca": address,
        "symbol": raw.get("symbol") or base_token.get("symbol") or camel_base_token.get("symbol") or token.get("symbol"),
        "name": raw.get("name") or base_token.get("name") or camel_base_token.get("name") or token.get("name"),
        "market_cap": raw.get("market_cap") or raw.get("usd_market_cap"),
        "liquidity": raw.get("liquidity"),
        "volume": raw.get("volume") or raw.get("volume_24h"),
        "price_change_1m": raw.get("price_change_percent1m"),
        "price_change_5m": raw.get("price_change_percent5m"),
        "price_change_1h": raw.get("price_change_percent1h"),
        "swaps": raw.get("swaps") or raw.get("swaps_24h"),
        "buys": raw.get("buys") or raw.get("buys_24h"),
        "sells": raw.get("sells") or raw.get("sells_24h"),
        "holder_count": raw.get("holder_count"),
        "top10_holder_rate": raw.get("top_10_holder_rate") or raw.get("top10_holder_rate"),
        "smart_degen_count": raw.get("smart_degen_count"),
        "renowned_count": raw.get("renowned_count"),
        "sniper_count": raw.get("sniper_count"),
        "bundler_rate": raw.get("bundler_rate"),
        "rat_trader_amount_rate": raw.get("rat_trader_amount_rate"),
        "entrapment_ratio": raw.get("entrapment_ratio"),
        "rug_ratio": raw.get("rug_ratio"),
        "is_wash_trading": raw.get("is_wash_trading"),
        "creator_token_status": raw.get("creator_token_status"),
        "creator_close": raw.get("creator_close"),
        "launchpad": raw.get("launchpad"),
        "launchpad_platform": raw.get("launchpad_platform"),
        "creation_timestamp": raw.get("creation_timestamp") or base_token.get("token_create_time") or camel_base_token.get("token_create_time"),
        "open_timestamp": raw.get("open_timestamp") or base_token.get("token_open_time") or camel_base_token.get("token_open_time"),
    }


def collect_candidates_with_errors(chain="sol", limit=50):
    captured_at = int(time.time())
    candidates = []
    errors = []

    try:
        trending = run_gmgn([
            "market", "trending",
            "--chain", chain,
            "--interval", "1m",
            "--order-by", "volume",
            "--limit", str(limit),
        ])
        for item in as_list(trending, ("data", "rank"), ("rank",)):
            candidates.append(normalize_token(item, source="gmgn_trending_1m", captured_at=captured_at))
    except Exception as exc:
        errors.append(f"gmgn_trending_1m:{exc}")

    try:
        signals = run_gmgn(["market", "signal", "--chain", chain])
        for group_name in ("data", "list", "rank"):
            for item in as_list(signals, (group_name,)):
                candidates.append(normalize_token(item, source="gmgn_signal", captured_at=captured_at))
        if isinstance(signals, list):
            for item in signals:
                candidates.append(normalize_token(item, source="gmgn_signal", captured_at=captured_at))
        elif isinstance(signals.get("data"), dict):
            for category, items in signals["data"].items():
                if isinstance(items, list):
                    for item in items:
                        candidates.append(normalize_token(item, source="gmgn_signal", category=category, captured_at=captured_at))
    except Exception as exc:
        errors.append(f"gmgn_signal:{exc}")

    try:
        trenches = run_gmgn([
            "market", "trenches",
            "--chain", chain,
            "--type", "new_creation", "near_completion", "completed",
            "--limit", str(min(limit, 80)),
            "--filter-preset", "safe",
        ])
        data = trenches.get("data") if isinstance(trenches.get("data"), dict) else {}
        for category, items in data.items():
            if isinstance(items, list):
                for item in items:
                    candidates.append(normalize_token(item, source="gmgn_trenches", category=category, captured_at=captured_at))
    except Exception as exc:
        errors.append(f"gmgn_trenches:{exc}")

    seen = set()
    deduped = []
    for cand in candidates:
        key = (cand.get("source"), cand.get("category"), cand.get("ca"))
        if not cand.get("ca") or key in seen:
            continue
        seen.add(key)
        deduped.append(cand)
    return deduped, errors


def collect_candidates(chain="sol", limit=50):
    candidates, errors = collect_candidates_with_errors(chain=chain, limit=limit)
    if errors and not candidates:
        raise RuntimeError("; ".join(errors))
    return candidates


def main():
    parser = argparse.ArgumentParser(description="Collect GMGN read-only candidate tokens into JSONL")
    parser.add_argument("--chain", default="sol")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--state-db", default=os.environ.get("EXTERNAL_ALPHA_DB") or os.environ.get("PAPER_DB"))
    parser.add_argument("--loop", action="store_true", help="Run continuously and update external alpha shadow state")
    parser.add_argument("--interval", type=float, default=60.0)
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    db = connect_external_alpha_db(args.state_db)
    try:
        startup_ts = int(time.time())
        runtime_status = gmgn_scout_runtime_status()
        record_external_alpha_health(
            db,
            run_ts=startup_ts,
            success=False,
            error=f"startup_pending_first_collection runtime={json.dumps(runtime_status, sort_keys=True)}",
        )
        print(f"gmgn candidate scout starting: {json.dumps(runtime_status, sort_keys=True)}", flush=True)
        while True:
            captured_at = int(time.time())
            try:
                candidates, partial_errors = collect_candidates_with_errors(chain=args.chain, limit=args.limit)
                if partial_errors and not candidates:
                    raise RuntimeError("; ".join(partial_errors))
                with out_path.open("a", encoding="utf-8") as fh:
                    for cand in candidates:
                        fh.write(json.dumps(cand, ensure_ascii=False, sort_keys=True) + "\n")
                state = record_external_alpha_candidates(db, candidates, captured_at=captured_at)
                record_external_alpha_health(
                    db,
                    run_ts=captured_at,
                    success=True,
                    candidate_count=len(candidates),
                    recorded_count=state["recorded"],
                    momentum_confirmed_count=state["momentum_confirmed"],
                )
                print(
                    f"wrote {len(candidates)} GMGN candidates to {out_path}; "
                    f"state_recorded={state['recorded']} momentum_confirmed={state['momentum_confirmed']} "
                    f"partial_errors={len(partial_errors)}",
                    flush=True,
                )
                for err in partial_errors[:3]:
                    print(f"gmgn candidate scout partial error: {err}", flush=True)
            except Exception as exc:
                record_external_alpha_health(db, run_ts=captured_at, success=False, error=exc)
                print(f"gmgn candidate scout error: {exc}", flush=True)
                if not args.loop:
                    raise
            if not args.loop:
                break
            time.sleep(max(5.0, args.interval))
    finally:
        db.close()


if __name__ == "__main__":
    main()
