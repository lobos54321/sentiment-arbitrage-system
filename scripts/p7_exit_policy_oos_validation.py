#!/usr/bin/env python3
"""Validate frozen P7 exit-policy finalists on forward-only OOS data.

This is read-only shadow validation. It never changes live exits, strategy,
gates, final_entry_contract, A_CLASS, executor, wallet, canary, or risk.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import statistics
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

import exit_policy_shadow_lab as lab


SCHEMA_VERSION = "p7_exit_policy_oos_validation.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_FREEZE_REGISTRY = Path("docs/agents/P7_EXIT_POLICY_OOS_FREEZE_REGISTRY.json")
PRIMARY_DELAY_GRID_SEC = (5, 10, 20, 30)
STOP_FILL_STRESS_FLOORS_PCT = (-25.0, -30.0)


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_from_ts(ts):
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return None


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def canonical_policy(policy):
    return {
        "policy_id": policy.get("policy_id"),
        "policy_family": policy.get("policy_family") or policy.get("family"),
        "params": policy.get("params") or {},
    }


def policy_fingerprint(policy):
    raw = json.dumps(canonical_policy(policy), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_registry(path):
    registry = load_json(path)
    policies = []
    for item in registry.get("frozen_policies") or []:
        policy = {
            "policy_id": item.get("policy_id"),
            "family": item.get("policy_family"),
            "policy_family": item.get("policy_family"),
            "params": item.get("params") or {},
            "role": item.get("role"),
            "source_rank": item.get("source_rank"),
            "expected_fingerprint": item.get("fingerprint"),
        }
        policy["fingerprint"] = policy_fingerprint(policy)
        policies.append(policy)
    registry["normalized_frozen_policies"] = policies
    return registry


def validate_registry(registry):
    errors = []
    if registry.get("promotion_allowed") is not False:
        errors.append("freeze_registry_promotion_allowed_not_false")
    if not registry.get("freeze_ts"):
        errors.append("freeze_registry_missing_freeze_ts")
    if not registry.get("freeze_commit"):
        errors.append("freeze_registry_missing_freeze_commit")
    for policy in registry.get("normalized_frozen_policies") or []:
        if policy.get("fingerprint") != policy.get("expected_fingerprint"):
            errors.append(f"fingerprint_mismatch:{policy.get('policy_id')}")
    return errors


def table_count_between(db, table, ts_col, since_ts, until_ts):
    if db is None or not lab.table_exists(db, table):
        return None
    cols = lab.table_columns(db, table)
    if ts_col not in cols:
        return None
    try:
        return int(db.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {ts_col} >= ? AND {ts_col} < ?",
            (int(since_ts), int(until_ts)),
        ).fetchone()[0])
    except sqlite3.Error:
        return None


def paper_evidence_summary(paper_db_path, since_ts, until_ts):
    db = lab.connect_readonly(paper_db_path)
    if db is None:
        return {"available": False, "reason": "paper_db_unavailable"}
    summary = {"available": True, "since_ts": int(since_ts), "until_ts": int(until_ts)}
    for table, columns in {
        "paper_decision_events": ("event_ts", "ts", "created_at"),
        "paper_trades": ("entry_time", "entry_ts", "created_at"),
        "trades": ("entry_time", "entry_ts", "created_at"),
    }.items():
        if not lab.table_exists(db, table):
            summary[f"{table}_available"] = False
            continue
        cols = lab.table_columns(db, table)
        ts_col = next((col for col in columns if col in cols), None)
        summary[f"{table}_available"] = True
        summary[f"{table}_ts_col"] = ts_col
        summary[f"{table}_rows"] = table_count_between(db, table, ts_col, since_ts, until_ts) if ts_col else None
    db.close()
    return summary


def build_samples_between(raw_db, paper_db_path, since_ts, until_ts, limit):
    raw_rows, raw_meta = lab.load_raw_rows(raw_db, since_ts, until_ts, limit)
    decision_prices, decision_meta = lab.load_decision_prices(paper_db_path, since_ts, until_ts)
    samples = []
    skipped = Counter()
    source_counts = Counter()
    for raw in raw_rows:
        if not raw.get("signal_ts") or not raw.get("token_ca"):
            skipped["missing_signal_ts_or_token"] += 1
            continue
        if raw.get("signal_ts") < int(since_ts):
            skipped["pre_freeze_signal_ts"] += 1
            continue
        if not lab.table_exists(raw_db, "raw_price_bars_1m"):
            skipped["missing_raw_price_bars_1m"] += 1
            continue
        bars = lab.load_bars_for_signal(raw_db, raw, raw["signal_ts"], raw["signal_ts"] + lab.HORIZON_SEC)
        if not bars:
            skipped["missing_0_2h_bars"] += 1
            continue
        anchor = lab.choose_entry_anchor(raw, decision_prices, bars)
        if lab.safe_float(anchor.get("entry_price")) is None:
            skipped["missing_entry_price"] += 1
            continue
        source_counts[anchor.get("entry_price_source")] += 1
        samples.append({"raw": raw, "bars": bars, "anchor": anchor})
    return samples, {
        "raw_meta": raw_meta,
        "decision_price_meta": decision_meta,
        "raw_rows_considered": len(raw_rows),
        "sample_count": len(samples),
        "skipped_counts": dict(skipped),
        "entry_price_source_counts": dict(source_counts),
    }


def explicit_cluster_key(raw):
    payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    for key in (
        "cluster_id",
        "source_cluster_id",
        "event_cluster_id",
        "lifecycle_id",
        "token_cluster_id",
    ):
        value = raw.get(key) or payload.get(key)
        if value:
            return str(value)
    token = str(raw.get("token_ca") or "").lower()
    ts = safe_int(raw.get("signal_ts"), 0) or 0
    return f"token_15m:{token}:{ts // 900}"


def dedupe_samples(samples, view):
    if view == "all_samples":
        return list(samples), {"dedupe_view": view, "input_count": len(samples), "output_count": len(samples)}
    seen = set()
    out = []
    for sample in sorted(samples, key=lambda item: safe_int(item["raw"].get("signal_ts"), 0) or 0):
        raw = sample["raw"]
        if view == "unique_token":
            key = str(raw.get("token_ca") or "").lower()
        elif view == "token_time_cluster":
            key = explicit_cluster_key(raw)
        else:
            key = f"unknown:{len(out)}"
        if key in seen:
            continue
        seen.add(key)
        out.append(sample)
    return out, {"dedupe_view": view, "input_count": len(samples), "output_count": len(out), "dropped_count": len(samples) - len(out)}


def compounded_cumulative_roi_pct(results):
    equity = 1.0
    used = 0
    for row in results:
        roi = safe_float(row.get("capital_roi_pct"))
        if roi is None:
            continue
        equity *= max(0.0, 1.0 + roi / 100.0)
        used += 1
        if equity > 1e12:
            break
    return None if used == 0 else round((equity - 1.0) * 100.0, 6)


def drop_top_winners(results, count):
    valid = [row for row in results if safe_float(row.get("capital_roi_pct")) is not None]
    winners = sorted(valid, key=lambda row: safe_float(row.get("capital_roi_pct"), -1e18), reverse=True)
    removed_ids = {id(row) for row in winners[: int(count)]}
    return [row for row in results if id(row) not in removed_ids]


def sensitivity_summary(results, drop_count):
    kept = drop_top_winners(results, drop_count)
    rolling = lab.rolling_24h_distribution([r for r in kept if r.get("net_pnl_pct") is not None])
    return {
        "drop_count": int(drop_count),
        "remaining_trade_count": len([r for r in kept if r.get("net_pnl_pct") is not None]),
        "rolling_24h_roi_median_pct": rolling.get("median_pct"),
        "rolling_24h_roi_distribution_pct": rolling,
        "compound_cumulative_roi_pct_reference": compounded_cumulative_roi_pct(kept),
    }


def has_hard_stop_exit(row):
    reason = str(row.get("exit_reason") or "").lower()
    if "hard_stop" in reason:
        return True
    for item in row.get("exits") or []:
        if "hard_stop" in str(item.get("reason") or "").lower():
            return True
    return False


def apply_stop_fill_stress(results, floor_pct):
    stressed = []
    hard_stop_count = 0
    for row in results:
        item = dict(row)
        exits = []
        changed = False
        weighted = 0.0
        if item.get("exits"):
            for exit_row in item.get("exits") or []:
                exit_item = dict(exit_row)
                fraction = safe_float(exit_item.get("fraction"), 0.0) or 0.0
                if "hard_stop" in str(exit_item.get("reason") or "").lower():
                    exit_item["unstressed_net_pnl_pct"] = safe_float(exit_item.get("net_pnl_pct"))
                    exit_item["net_pnl_pct"] = float(floor_pct)
                    exit_item["stop_fill_stress_floor_pct"] = float(floor_pct)
                    changed = True
                weighted += (safe_float(exit_item.get("net_pnl_pct"), 0.0) or 0.0) * fraction
                exits.append(exit_item)
        elif has_hard_stop_exit(item):
            weighted = float(floor_pct)
            changed = True
        if changed:
            hard_stop_count += 1
            item["unstressed_net_pnl_pct"] = safe_float(item.get("net_pnl_pct"))
            item["net_pnl_pct"] = round(weighted, 6)
            item["capital_roi_pct"] = round(weighted * lab.POSITION_FRACTION_OF_RISK_CAPITAL, 6)
            item["win"] = weighted > 0
            item["stop_fill_stressed"] = True
            item["stop_fill_stress_floor_pct"] = float(floor_pct)
            if exits:
                item["exits"] = exits
        stressed.append(item)
    return stressed, hard_stop_count


def stop_fill_stress_summary(results):
    summaries = {}
    for floor in STOP_FILL_STRESS_FLOORS_PCT:
        stressed, hard_stop_count = apply_stop_fill_stress(results, floor)
        valid = [row for row in stressed if row.get("net_pnl_pct") is not None]
        rolling = lab.rolling_24h_distribution(valid)
        summaries[str(int(floor))] = {
            "stress_floor_net_pnl_pct": float(floor),
            "hard_stop_exit_count": hard_stop_count,
            "simulated_trade_count": len(valid),
            "rolling_24h_roi_median_pct": rolling.get("median_pct"),
            "rolling_24h_roi_distribution_pct": rolling,
            "compound_cumulative_roi_pct_reference": compounded_cumulative_roi_pct(stressed),
            "median_net_pnl_pct": None if not valid else round(statistics.median(row["net_pnl_pct"] for row in valid), 6),
        }
    return summaries


def summarize_oos_cell(policy, slippage, delay_sec, results):
    base = lab.summarize_variant(policy, slippage, delay_sec, results)
    rolling = base.get("rolling_24h_realized_net_roi_distribution_pct") or {}
    base.update({
        "primary_metric_name": "rolling_24h_realized_net_roi_median_pct",
        "primary_metric_pct": rolling.get("median_pct"),
        "compound_cumulative_roi_pct_reference": compounded_cumulative_roi_pct(results),
        "drop_top_1_winner_sensitivity": sensitivity_summary(results, 1),
        "drop_top_3_winners_sensitivity": sensitivity_summary(results, 3),
        "stop_fill_stress_tests": stop_fill_stress_summary(results),
    })
    return base


def simulate_cells(samples, policies):
    cells = []
    for policy in policies:
        for delay_sec in lab.ENTRY_DELAY_GRID_SEC:
            for slippage in lab.SLIPPAGE_GRID:
                results = []
                for sample in samples:
                    raw = sample["raw"]
                    entry = lab.delayed_entry(sample["anchor"], sample["bars"], delay_sec)
                    result = lab.simulate_trade(raw, sample["bars"], entry, policy, slippage, lookahead=False)
                    result.update({
                        "signal_id": raw.get("signal_id_key"),
                        "token_ca": raw.get("token_ca"),
                        "signal_ts": raw.get("signal_ts"),
                        "raw_primary_tier": raw.get("raw_primary_tier"),
                    })
                    results.append(result)
                cell = summarize_oos_cell(policy, slippage, delay_sec, results)
                cell["policy_fingerprint"] = policy.get("fingerprint")
                cell["source_rank"] = policy.get("source_rank")
                cell["role"] = policy.get("role")
                cells.append(cell)
    return cells


def cell_primary_metric(row, *, stop_fill_floor_pct=None):
    if stop_fill_floor_pct is None:
        return safe_float(row.get("primary_metric_pct"))
    tests = row.get("stop_fill_stress_tests") or {}
    key = str(int(stop_fill_floor_pct))
    return safe_float((tests.get(key) or {}).get("rolling_24h_roi_median_pct"))


def rank_cells(cells, *, stop_fill_floor_pct=None):
    ranked = sorted(
        cells,
        key=lambda row: (
            cell_primary_metric(row, stop_fill_floor_pct=stop_fill_floor_pct) is not None,
            cell_primary_metric(row, stop_fill_floor_pct=stop_fill_floor_pct) or -1e18,
            safe_float(row.get("win_rate"), -1e18),
            -safe_float(row.get("max_drawdown_pct"), 1e18),
        ),
        reverse=True,
    )
    for idx, row in enumerate(ranked, start=1):
        row["primary_rank"] = idx
    return ranked


def aggregate_policy_primary(cells, *, delays=PRIMARY_DELAY_GRID_SEC, stop_fill_floor_pct=None):
    by_policy = defaultdict(list)
    for row in cells:
        if int(row.get("entry_delay_sec") or 0) not in set(delays):
            continue
        value = cell_primary_metric(row, stop_fill_floor_pct=stop_fill_floor_pct)
        if value is None:
            continue
        by_policy[row.get("policy_id")].append(value)
    out = []
    for policy_id, values in by_policy.items():
        out.append({
            "policy_id": policy_id,
            "primary_delay_grid_sec": list(delays),
            "stop_fill_stress_floor_pct": stop_fill_floor_pct,
            "cell_count": len(values),
            "median_primary_metric_pct": round(statistics.median(values), 6),
            "mean_primary_metric_pct": round(sum(values) / len(values), 6),
            "positive_cell_count": sum(1 for v in values if v > 0),
        })
    out.sort(key=lambda row: row["median_primary_metric_pct"], reverse=True)
    for idx, row in enumerate(out, start=1):
        row["rank"] = idx
    return out


def ranking_stability(cells, champion_policy_id, *, stop_fill_floor_pct=None):
    groups = defaultdict(list)
    for row in cells:
        delay = int(row.get("entry_delay_sec") or 0)
        if delay not in PRIMARY_DELAY_GRID_SEC:
            continue
        groups[(delay, row.get("slippage_pct"))].append(row)
    rows = []
    champion_top = 0
    for (delay, slippage), group in sorted(groups.items()):
        ranked = rank_cells(list(group), stop_fill_floor_pct=stop_fill_floor_pct)
        top = ranked[0] if ranked else {}
        is_champion = top.get("policy_id") == champion_policy_id
        champion_top += 1 if is_champion else 0
        rows.append({
            "entry_delay_sec": delay,
            "slippage_pct": slippage,
            "top_policy_id": top.get("policy_id"),
            "top_primary_metric_pct": cell_primary_metric(top, stop_fill_floor_pct=stop_fill_floor_pct),
            "champion_is_top": is_champion,
        })
    return {
        "delay_zero_excluded": True,
        "primary_delay_grid_sec": list(PRIMARY_DELAY_GRID_SEC),
        "stop_fill_stress_floor_pct": stop_fill_floor_pct,
        "cell_count": len(rows),
        "champion_top_count": champion_top,
        "champion_top_rate": None if not rows else round(champion_top / len(rows), 6),
        "strictly_stable": bool(rows) and champion_top == len(rows),
        "rows": rows,
    }


def stop_fill_stress_ranking_panels(cells, champion_policy_id):
    out = {}
    for floor in STOP_FILL_STRESS_FLOORS_PCT:
        policy_primary = aggregate_policy_primary(cells, stop_fill_floor_pct=floor)
        top_policy = policy_primary[0].get("policy_id") if policy_primary else None
        champion_row = next((row for row in policy_primary if row.get("policy_id") == champion_policy_id), None)
        out[str(int(floor))] = {
            "stress_floor_net_pnl_pct": float(floor),
            "policy_primary_ranking_delay_5_30": policy_primary,
            "ranking_stability_delay_5_30": ranking_stability(
                cells,
                champion_policy_id,
                stop_fill_floor_pct=floor,
            ),
            "top_policy_id_delay_5_30": top_policy,
            "champion_primary_delay_5_30": champion_row,
            "direction_positive_for_champion": bool(
                champion_row
                and top_policy == champion_policy_id
                and safe_float(champion_row.get("median_primary_metric_pct"), -1e18) > 0
            ),
        }
    return out


def build_window_report(raw_db, paper_db, registry, window_index, window_start, window_end, now_ts, limit):
    complete = now_ts >= window_end
    samples, sample_meta = build_samples_between(raw_db, paper_db, window_start, min(window_end, now_ts), limit)
    dedupe_reports = []
    champion_policy_id = next((p.get("policy_id") for p in registry.get("normalized_frozen_policies") or [] if p.get("role") == "champion"), None)
    for view in registry.get("validation_contract", {}).get("dedupe_views") or ["all_samples", "unique_token", "token_time_cluster"]:
        view_samples, dedupe_meta = dedupe_samples(samples, view)
        cells = simulate_cells(view_samples, registry.get("normalized_frozen_policies") or [])
        ranked_cells = rank_cells(cells)
        policy_primary = aggregate_policy_primary(cells)
        top_policy = policy_primary[0].get("policy_id") if policy_primary else None
        champion_row = next((row for row in policy_primary if row.get("policy_id") == champion_policy_id), None)
        stop_stress = stop_fill_stress_ranking_panels(cells, champion_policy_id)
        dedupe_reports.append({
            "dedupe_meta": dedupe_meta,
            "policy_primary_ranking_delay_5_30": policy_primary,
            "ranking_stability_delay_5_30": ranking_stability(cells, champion_policy_id),
            "top_policy_id_delay_5_30": top_policy,
            "champion_primary_delay_5_30": champion_row,
            "stop_fill_stress_ranking_delay_5_30": stop_stress,
            "stop_fill_stress_passed_for_champion": all(
                bool((panel or {}).get("direction_positive_for_champion"))
                for panel in stop_stress.values()
            ) if stop_stress else False,
            "direction_positive_for_champion": bool(
                champion_row
                and top_policy == champion_policy_id
                and safe_float(champion_row.get("median_primary_metric_pct"), -1e18) > 0
            ),
            "ranked_cells": ranked_cells[:120],
        })
    all_view = next((row for row in dedupe_reports if row.get("dedupe_meta", {}).get("dedupe_view") == "all_samples"), {})
    return {
        "window_index": int(window_index),
        "window_start_ts": int(window_start),
        "window_start_iso": iso_from_ts(window_start),
        "window_end_ts": int(window_end),
        "window_end_iso": iso_from_ts(window_end),
        "complete": bool(complete),
        "sample_meta": sample_meta,
        "paper_evidence": paper_evidence_summary(paper_db, window_start, min(window_end, now_ts)),
        "dedupe_reports": dedupe_reports,
        "primary_all_samples_direction_positive": bool(all_view.get("direction_positive_for_champion")),
        "stop_fill_stress_all_samples_passed_for_champion": bool(all_view.get("stop_fill_stress_passed_for_champion")),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    registry = load_registry(args.freeze_registry)
    registry_errors = validate_registry(registry)
    freeze_ts = safe_int(registry.get("freeze_ts"), 0) or 0
    window_hours = safe_float(args.window_hours, registry.get("validation_contract", {}).get("default_window_hours") or 24)
    window_sec = int(window_hours * 3600)
    raw_db = lab.connect_readonly(args.raw_db)
    if raw_db is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "classification": "P7_EXIT_POLICY_OOS_BLOCKED_DATA",
            "blockers": ["raw_db_unavailable"],
            "promotion_allowed": False,
        }
    windows = []
    for idx in (1, 2):
        start = freeze_ts + (idx - 1) * window_sec
        end = start + window_sec
        windows.append(build_window_report(raw_db, args.paper_db, registry, idx, start, end, now_ts, args.limit))
    raw_db.close()
    completed = [w for w in windows if w.get("complete")]
    two_complete = len(completed) == 2
    same_direction = two_complete and all(w.get("primary_all_samples_direction_positive") for w in windows)
    stop_stress_passed = two_complete and all(w.get("stop_fill_stress_all_samples_passed_for_champion") for w in windows)
    blockers = list(registry_errors)
    if not two_complete:
        blockers.append("waiting_for_two_non_overlapping_forward_windows")
    elif not same_direction:
        blockers.append("two_forward_windows_not_same_positive_direction")
    elif not stop_stress_passed:
        blockers.append("stop_fill_stress_champion_not_stable")
    classification = (
        "P7_EXIT_POLICY_OOS_BLOCKED_REGISTRY"
        if registry_errors
        else "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW"
        if same_direction and stop_stress_passed
        else "P7_EXIT_POLICY_OOS_WAITING_FOR_FORWARD_DATA"
        if not two_complete
        else "P7_EXIT_POLICY_OOS_NOT_PASSED"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso_from_ts(now_ts),
        "now_ts": now_ts,
        "freeze_registry": registry,
        "freeze_ts": freeze_ts,
        "freeze_iso": iso_from_ts(freeze_ts),
        "forward_only": True,
        "only_signal_ts_gte_freeze_ts": True,
        "window_hours": window_hours,
        "requires_two_non_overlapping_windows": True,
        "primary_metric": "rolling_24h_realized_net_roi_median_pct",
        "compound_cumulative_roi_is_reference_only": True,
        "ranking_stability_delay_grid_sec": list(PRIMARY_DELAY_GRID_SEC),
        "delay_zero_excluded_from_ranking_stability": True,
        "stop_fill_stress_contract": {
            "enabled": True,
            "hard_stop_fill_floors_net_pnl_pct": list(STOP_FILL_STRESS_FLOORS_PCT),
            "champion_must_remain_top_delay_5_30": True,
            "basis": "paper_trade_id_71_probe_quote_guard_stop_filled_near_-29.75pct",
        },
        "windows": windows,
        "two_windows_complete": two_complete,
        "two_windows_same_positive_direction": same_direction,
        "stop_fill_stress_champion_stable": stop_stress_passed,
        "classification": classification,
        "blockers": blockers,
        "promotion_allowed": False,
        "paper_proposal_allowed": classification == "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW",
        "human_checkpoint_required": classification == "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW",
        "allowed_use": "shadow_only_oos_validation",
        "strategy_change_allowed": False,
        "live_exit_policy_changed": False,
        "production_files_touched": [],
        "next_action": (
            "wait_for_forward_oos_windows"
            if classification == "P7_EXIT_POLICY_OOS_WAITING_FOR_FORWARD_DATA"
            else "human_review_paper_proposal_checkpoint"
            if classification == "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW"
            else "review_failed_oos_direction"
        ),
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path = root / "raw.db"
        paper_path = root / "paper.db"
        freeze_ts = int(time.time()) - 3 * 3600
        now_ts = freeze_ts + 3 * 3600
        registry_path = root / "registry.json"
        policies = [
            {
                "policy_id": "trail_a50_dd15_stop20",
                "policy_family": "trailing_drawdown_stop",
                "params": {"activation_pct": 50.0, "trail_drawdown_pct": 15.0, "stop_loss_pct": -20.0},
                "role": "champion",
                "source_rank": 1,
            },
            {
                "policy_id": "trail_a30_dd15_stop20",
                "policy_family": "trailing_drawdown_stop",
                "params": {"activation_pct": 30.0, "trail_drawdown_pct": 15.0, "stop_loss_pct": -20.0},
                "role": "top3_ranked_challenger",
                "source_rank": 2,
            },
        ]
        for policy in policies:
            policy["fingerprint"] = policy_fingerprint(policy)
        write_json(registry_path, {
            "schema_version": "p7_exit_policy_oos_freeze_registry.v1",
            "freeze_ts": freeze_ts,
            "freeze_commit": "selftest",
            "promotion_allowed": False,
            "frozen_policies": policies,
            "validation_contract": {
                "default_window_hours": 1,
                "dedupe_views": ["all_samples", "unique_token", "token_time_cluster"],
            },
        })
        db = sqlite3.connect(raw_path)
        db.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, raw_primary_tier TEXT, observation_status TEXT,
              kline_covered INTEGER, baseline_price REAL, baseline_ts INTEGER,
              baseline_pool_address TEXT, path_pool_address TEXT,
              path_price_unit TEXT, max_sustained_peak_pct REAL,
              time_to_sustained_peak_sec INTEGER, payload_json TEXT
            )
            """
        )
        db.execute(
            """
            CREATE TABLE raw_price_bars_1m(
              token_ca TEXT, pool_address TEXT, timestamp INTEGER,
              open REAL, high REAL, low REAL, close REAL, volume REAL,
              provider TEXT, source_kind TEXT, source_family TEXT, price_unit TEXT
            )
            """
        )
        signal_id = 1
        for offset in (300, 900, 3900, 4500):
            signal_ts = freeze_ts + offset
            token = f"T{signal_id}"
            db.execute(
                "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(signal_id), token, token, signal_ts, "ATH", "gold", "matured", 1, 1.0, signal_ts, "pool", "pool", "native", 180.0, 600, "{}"),
            )
            for minute in range(0, 121):
                ts = signal_ts + minute * 60
                high = 1.0 + min(minute, 50) * 0.025
                low = max(0.90, high * 0.86)
                close = high * 0.95
                db.execute(
                    "INSERT INTO raw_price_bars_1m VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (token, "pool", ts, 1.0, high, low, close, 100.0, "selftest", "raw_swaps", "selftest", "native"),
                )
            signal_id += 1
        db.commit()
        db.close()
        db = sqlite3.connect(paper_path)
        db.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER PRIMARY KEY, event_ts INTEGER, signal_id TEXT,
              event_type TEXT, decision TEXT, action TEXT, reason TEXT,
              payload_json TEXT
            )
            """
        )
        for idx in range(1, signal_id):
            db.execute(
                "INSERT INTO paper_decision_events(event_ts, signal_id, event_type, decision, action, reason, payload_json) VALUES (?,?,?,?,?,?,?)",
                (freeze_ts + idx * 300, str(idx), "entry_decision", "PASS", "would_enter", "self_test", json.dumps({"decision_price": 1.0})),
            )
        db.commit()
        db.close()
        args = argparse.Namespace(
            raw_db=str(raw_path),
            paper_db=str(paper_path),
            freeze_registry=str(registry_path),
            window_hours=1.0,
            now_ts=now_ts,
            limit=1000,
            out=str(root / "out.json"),
            self_test=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["only_signal_ts_gte_freeze_ts"] is True
        assert report["two_windows_complete"] is True
        assert len(report["windows"]) == 2
        assert report["windows"][0]["dedupe_reports"][0]["ranking_stability_delay_5_30"]["delay_zero_excluded"] is True
        assert report["stop_fill_stress_contract"]["enabled"] is True
        assert report["windows"][0]["dedupe_reports"][0]["stop_fill_stress_ranking_delay_5_30"]
        first_cell = report["windows"][0]["dedupe_reports"][0]["ranked_cells"][0]
        assert first_cell["stop_fill_stress_tests"]["-25"]["stress_floor_net_pnl_pct"] == -25.0
        assert report["classification"] in {
            "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW",
            "P7_EXIT_POLICY_OOS_NOT_PASSED",
        }
    print("SELF_TEST_PASS p7_exit_policy_oos_validation")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--paper-db", default=str(DEFAULT_DATA_DIR / "paper_trades.db"))
    parser.add_argument("--freeze-registry", default=str(DEFAULT_FREEZE_REGISTRY))
    parser.add_argument("--window-hours", type=float, default=None)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "agent_runs/latest/p7_exit_policy_oos_validation.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    registry = load_registry(args.freeze_registry)
    if args.window_hours is None:
        args.window_hours = safe_float(
            registry.get("validation_contract", {}).get("default_window_hours"),
            24,
        )
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "out": args.out,
        "classification": report.get("classification"),
        "two_windows_complete": report.get("two_windows_complete"),
        "two_windows_same_positive_direction": report.get("two_windows_same_positive_direction"),
        "promotion_allowed": False,
        "paper_proposal_allowed": report.get("paper_proposal_allowed"),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
