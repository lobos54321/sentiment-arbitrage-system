#!/usr/bin/env python3
"""Score recent paper-trading evidence against the strategy goal.

This is the first piece of the strategy reflection layer. It is deliberately
read-only: it inspects paper trades, missed attribution, and Markov decision
events, then returns a compact scoreboard that a runner can turn into a
single-variable hypothesis.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "paper_trades.db"
DEFAULT_GOAL = ROOT / "config" / "strategy-goal.yaml"

GOLD_PNL = 1.00
SILVER_PNL = 0.50
BRONZE_PNL = 0.25


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def utc_iso(ts: float | None = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), tz=timezone.utc).isoformat()


def parse_scalar(raw: str) -> Any:
    raw = raw.strip()
    if raw == "":
        return ""
    if raw in {"true", "True"}:
        return True
    if raw in {"false", "False"}:
        return False
    if raw in {"null", "None", "~"}:
        return None
    if (raw.startswith('"') and raw.endswith('"')) or (raw.startswith("'") and raw.endswith("'")):
        return raw[1:-1]
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [parse_scalar(part.strip()) for part in inner.split(",")]
    try:
        if "." not in raw:
            return int(raw)
        return float(raw)
    except ValueError:
        return raw


def load_simple_yaml(path: str | Path) -> dict[str, Any]:
    """Load the small YAML subset used by config/strategy-goal.yaml.

    The repo does not otherwise depend on PyYAML, so this parser supports the
    intentionally simple shape we need: nested mappings plus inline lists.
    """
    path = Path(path)
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if ":" not in stripped:
            raise ValueError(f"Invalid goal config line: {raw_line}")
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = parse_scalar(value)
    return root


def table_exists(db: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    )


def columns_for(db: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(db, table_name):
        return set()
    return {str(row["name"]) for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()}


def load_json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def dog_tier(peak_pnl: Any) -> str:
    peak = safe_float(peak_pnl, 0.0) or 0.0
    if peak >= GOLD_PNL:
        return "gold"
    if peak >= SILVER_PNL:
        return "silver"
    if peak >= BRONZE_PNL:
        return "bronze"
    return "sub25"


def is_medal(peak_pnl: Any) -> bool:
    return dog_tier(peak_pnl) in {"gold", "silver", "bronze"}


def row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def unique_token(row: sqlite3.Row) -> str:
    token = row_get(row, "token_ca") or row_get(row, "symbol") or row_get(row, "id")
    return str(token)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "tradable", "clean"}
    return False


def quote_clean_from_missed(row: sqlite3.Row) -> bool:
    status = str(row_get(row, "tradability_status", "") or "").lower()
    reason = str(row_get(row, "tradability_reason", "") or "").lower()
    payload = load_json(row_get(row, "payload_json"))
    if status in {"tradable", "quote_clean", "clean"}:
        return True
    for key in ("quote_clean", "quote_clean_seen", "quote_executable", "entry_quote_success_seen"):
        if truthy(payload.get(key)):
            return True
    gap = safe_float(payload.get("quote_gap_pct"), None)
    if gap is None:
        gap = safe_float(payload.get("gap_pct"), None)
    if gap is not None and gap <= 10.0:
        return True
    return "quote" in reason and "clean" in reason


def connect_db(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return db


def fetch_trade_rows(db: sqlite3.Connection, since_ts: float) -> list[sqlite3.Row]:
    if not table_exists(db, "paper_trades"):
        return []
    return db.execute(
        """
        SELECT *
        FROM paper_trades
        WHERE entry_ts >= ?
        ORDER BY entry_ts ASC
        """,
        (since_ts,),
    ).fetchall()


def fetch_missed_rows(db: sqlite3.Connection, since_ts: float) -> list[sqlite3.Row]:
    if not table_exists(db, "paper_missed_signal_attribution"):
        return []
    return db.execute(
        """
        SELECT *
        FROM paper_missed_signal_attribution
        WHERE created_event_ts >= ?
        ORDER BY created_event_ts ASC
        """,
        (since_ts,),
    ).fetchall()


def fetch_markov_advice_rows(db: sqlite3.Connection, since_ts: float) -> list[sqlite3.Row]:
    if not table_exists(db, "paper_decision_events"):
        return []
    return db.execute(
        """
        SELECT *
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND component = 'markov_position_advice'
        ORDER BY event_ts ASC
        """,
        (since_ts,),
    ).fetchall()


def summarize_trades(rows: list[sqlite3.Row], goal: dict[str, Any]) -> dict[str, Any]:
    closed = [row for row in rows if row_get(row, "exit_ts") is not None]
    open_rows = [row for row in rows if row_get(row, "exit_ts") is None]
    finals = [safe_float(row_get(row, "pnl_pct"), 0.0) or 0.0 for row in closed]
    peaks = [safe_float(row_get(row, "peak_pnl"), 0.0) or 0.0 for row in closed]
    medal_rows = [row for row in closed if is_medal(row_get(row, "peak_pnl"))]
    medal_tokens = {unique_token(row) for row in medal_rows}
    tier_counts = Counter(dog_tier(row_get(row, "peak_pnl")) for row in medal_rows)
    by_entry_mode: dict[str, dict[str, Any]] = {}
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in closed:
        mode = str(row_get(row, "entry_mode") or row_get(row, "signal_type") or "unknown")
        grouped[mode].append(row)
    for mode, group in sorted(grouped.items()):
        group_finals = [safe_float(row_get(row, "pnl_pct"), 0.0) or 0.0 for row in group]
        group_peaks = [safe_float(row_get(row, "peak_pnl"), 0.0) or 0.0 for row in group]
        by_entry_mode[mode] = {
            "closed_n": len(group),
            "avg_pnl_pct": sum(group_finals) / len(group_finals) if group_finals else None,
            "avg_peak_pnl": sum(group_peaks) / len(group_peaks) if group_peaks else None,
            "medal_n": sum(1 for row in group if is_medal(row_get(row, "peak_pnl"))),
            "loss_n": sum(1 for value in group_finals if value < 0),
        }
    return {
        "total_n": len(rows),
        "closed_n": len(closed),
        "open_n": len(open_rows),
        "total_pnl_pct_points": sum(finals),
        "avg_pnl_pct": sum(finals) / len(finals) if finals else None,
        "avg_peak_pnl": sum(peaks) / len(peaks) if peaks else None,
        "captured_medal_unique": len(medal_tokens),
        "captured_gold_unique": tier_counts.get("gold", 0),
        "captured_silver_unique": tier_counts.get("silver", 0),
        "captured_bronze_unique": tier_counts.get("bronze", 0),
        "by_entry_mode": by_entry_mode,
    }


def summarize_missed(rows: list[sqlite3.Row]) -> dict[str, Any]:
    medal_rows = [row for row in rows if is_medal(row_get(row, "tradable_peak_pnl") or row_get(row, "max_pnl_recorded"))]
    clean_medal_rows = [row for row in medal_rows if quote_clean_from_missed(row)]
    medal_tokens = {unique_token(row) for row in medal_rows}
    clean_medal_tokens = {unique_token(row) for row in clean_medal_rows}
    tier_tokens: dict[str, set[str]] = {"gold": set(), "silver": set(), "bronze": set()}
    clean_tier_tokens: dict[str, set[str]] = {"gold": set(), "silver": set(), "bronze": set()}
    blockers = Counter()
    components = Counter()
    clean_blockers = Counter()
    for row in medal_rows:
        peak = row_get(row, "tradable_peak_pnl") or row_get(row, "max_pnl_recorded")
        tier = dog_tier(peak)
        tier_tokens.setdefault(tier, set()).add(unique_token(row))
        blocker = str(row_get(row, "reject_reason") or row_get(row, "decision") or "unknown")
        component = str(row_get(row, "component") or "unknown")
        blockers[blocker] += 1
        components[component] += 1
        if quote_clean_from_missed(row):
            clean_tier_tokens.setdefault(tier, set()).add(unique_token(row))
            clean_blockers[blocker] += 1
    return {
        "missed_medal_unique": len(medal_tokens),
        "missed_clean_medal_unique": len(clean_medal_tokens),
        "missed_gold_unique": len(tier_tokens["gold"]),
        "missed_silver_unique": len(tier_tokens["silver"]),
        "missed_bronze_unique": len(tier_tokens["bronze"]),
        "missed_clean_gold_unique": len(clean_tier_tokens["gold"]),
        "missed_clean_silver_unique": len(clean_tier_tokens["silver"]),
        "missed_clean_bronze_unique": len(clean_tier_tokens["bronze"]),
        "top_blockers": blockers.most_common(10),
        "top_clean_blockers": clean_blockers.most_common(10),
        "by_component": components.most_common(10),
    }


def summarize_markov_advice(rows: list[sqlite3.Row]) -> dict[str, Any]:
    by_decision = Counter()
    by_bucket = Counter()
    p30_by_bucket: dict[str, list[float]] = defaultdict(list)
    pstop_by_bucket: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        payload = load_json(row_get(row, "payload_json"))
        decision = str(row_get(row, "decision") or payload.get("decision") or "unknown")
        bucket = str(payload.get("markov_bucket") or (payload.get("forecast") or {}).get("gate", {}).get("markov_bucket") or "unknown")
        by_decision[decision] += 1
        by_bucket[bucket] += 1
        p30 = safe_float(payload.get("p_absorb_peak30"), None)
        pstop = safe_float(payload.get("p_absorb_stop_before_peak"), None)
        if p30 is not None:
            p30_by_bucket[bucket].append(p30)
        if pstop is not None:
            pstop_by_bucket[bucket].append(pstop)
    bucket_stats = {}
    for bucket in sorted(by_bucket):
        p30_values = p30_by_bucket.get(bucket, [])
        pstop_values = pstop_by_bucket.get(bucket, [])
        bucket_stats[bucket] = {
            "n": by_bucket[bucket],
            "avg_p30": sum(p30_values) / len(p30_values) if p30_values else None,
            "avg_pstop": sum(pstop_values) / len(pstop_values) if pstop_values else None,
        }
    return {
        "event_n": len(rows),
        "by_decision": dict(by_decision),
        "by_bucket": dict(by_bucket),
        "bucket_stats": bucket_stats,
    }


def compute_drawdown(finals: list[float]) -> float:
    equity = 0.0
    high = 0.0
    max_dd = 0.0
    for pnl in finals:
        equity += pnl
        high = max(high, equity)
        max_dd = max(max_dd, high - equity)
    return max_dd


def score_components(
    trades: dict[str, Any],
    missed: dict[str, Any],
    goal: dict[str, Any],
    closed_rows: list[sqlite3.Row],
) -> dict[str, Any]:
    business = goal.get("business_goal", {})
    risk_limits = goal.get("risk_limits", {})
    target_capture = safe_float(business.get("target_capture_rate_gold_silver"), 0.55) or 0.55
    target_recall = safe_float(business.get("target_winner_clean_quote_recall"), 0.60) or 0.60
    target_peak30 = safe_float(business.get("target_peak30_capture"), 0.30) or 0.30
    captured_gs = int(trades.get("captured_gold_unique") or 0) + int(trades.get("captured_silver_unique") or 0)
    missed_clean_gs = int(missed.get("missed_clean_gold_unique") or 0) + int(missed.get("missed_clean_silver_unique") or 0)
    captured_medal = int(trades.get("captured_medal_unique") or 0)
    missed_clean_medal = int(missed.get("missed_clean_medal_unique") or 0)
    gs_denom = captured_gs + missed_clean_gs
    medal_denom = captured_medal + missed_clean_medal
    capture_rate_gs = (captured_gs / gs_denom) if gs_denom else None
    clean_recall = (captured_medal / medal_denom) if medal_denom else None
    peak30_capture = (captured_medal / medal_denom) if medal_denom else None
    finals = [safe_float(row_get(row, "pnl_pct"), 0.0) or 0.0 for row in closed_rows]
    avg_pnl = sum(finals) / len(finals) if finals else None
    drawdown = compute_drawdown(finals)
    max_drawdown = (safe_float(risk_limits.get("max_drawdown_pct"), 8.0) or 8.0) / 100.0
    risk_score = None if not finals else 1.0
    if finals and drawdown > max_drawdown:
        risk_score = clamp((max_drawdown - drawdown) / max(max_drawdown, 0.001))
    elif finals and avg_pnl is not None and avg_pnl < 0:
        risk_score = clamp(avg_pnl / 0.10)
    ev_score = None if avg_pnl is None else clamp(avg_pnl / 0.10)
    components = {
        "gold_silver_capture": {
            "value": capture_rate_gs,
            "target": target_capture,
            "score": None if capture_rate_gs is None else clamp((capture_rate_gs - target_capture) / target_capture),
            "denominator": gs_denom,
        },
        "clean_medal_recall": {
            "value": clean_recall,
            "target": target_recall,
            "score": None if clean_recall is None else clamp((clean_recall - target_recall) / target_recall),
            "denominator": medal_denom,
        },
        "peak30_capture": {
            "value": peak30_capture,
            "target": target_peak30,
            "score": None if peak30_capture is None else clamp((peak30_capture - target_peak30) / target_peak30),
            "denominator": medal_denom,
        },
        "entry_ev": {
            "value": avg_pnl,
            "target": 0.0,
            "score": ev_score,
            "denominator": len(finals),
        },
        "risk": {
            "value": drawdown,
            "target": max_drawdown,
            "score": risk_score,
            "denominator": len(finals),
        },
    }
    weights = {
        "gold_silver_capture": 0.30,
        "clean_medal_recall": 0.25,
        "peak30_capture": 0.15,
        "entry_ev": 0.15,
        "risk": 0.15,
    }
    weighted = 0.0
    used_weight = 0.0
    for name, item in components.items():
        score = item.get("score")
        if score is None:
            continue
        weight = weights[name]
        weighted += score * weight
        used_weight += weight
    overall = weighted / used_weight if used_weight else 0.0
    return {
        "overall_score": clamp(overall),
        "components": components,
        "weights": weights,
        "risk_breached": drawdown > max_drawdown,
    }


def scoreboard_status(score: dict[str, Any], trades: dict[str, Any], missed: dict[str, Any], goal: dict[str, Any]) -> str:
    min_samples = int((goal.get("risk_limits", {}) or {}).get("min_sample_n_before_promotion") or 30)
    closed_n = int(trades.get("closed_n") or 0)
    if closed_n < min(5, min_samples):
        return "insufficient_closed_trades"
    if score.get("risk_breached"):
        return "risk_breached"
    if int(missed.get("missed_clean_medal_unique") or 0) > int(trades.get("captured_medal_unique") or 0):
        return "missed_clean_winners_dominate"
    overall = safe_float(score.get("overall_score"), 0.0) or 0.0
    if overall >= 0.20:
        return "promising"
    if overall < -0.25:
        return "under_target"
    return "watch"


def evaluate_strategy_window(
    db: sqlite3.Connection,
    goal: dict[str, Any],
    *,
    hours: float = 24,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now_ts = float(now_ts or time.time())
    since_ts = now_ts - float(hours) * 3600.0
    table_names = {
        "paper_trades": table_exists(db, "paper_trades"),
        "paper_missed_signal_attribution": table_exists(db, "paper_missed_signal_attribution"),
        "paper_decision_events": table_exists(db, "paper_decision_events"),
    }
    trade_rows = fetch_trade_rows(db, since_ts)
    missed_rows = fetch_missed_rows(db, since_ts)
    markov_rows = fetch_markov_advice_rows(db, since_ts)
    closed_rows = [row for row in trade_rows if row_get(row, "exit_ts") is not None]
    trades = summarize_trades(trade_rows, goal)
    missed = summarize_missed(missed_rows)
    markov = summarize_markov_advice(markov_rows)
    score = score_components(trades, missed, goal, closed_rows)
    status = scoreboard_status(score, trades, missed, goal)
    return {
        "schema_version": "v1.strategy_reflection_score",
        "generated_at": utc_iso(now_ts),
        "window": {
            "hours": hours,
            "since_ts": since_ts,
            "until_ts": now_ts,
            "since": utc_iso(since_ts),
            "until": utc_iso(now_ts),
        },
        "tables": table_names,
        "status": status,
        "score": score,
        "trades": trades,
        "missed_winners": missed,
        "markov_position_advice": markov,
    }


def evaluate_from_paths(db_path: str | Path, goal_path: str | Path, *, hours: float = 24, now_ts: float | None = None) -> dict[str, Any]:
    goal = load_simple_yaml(goal_path)
    with connect_db(db_path) as db:
        return evaluate_strategy_window(db, goal, hours=hours, now_ts=now_ts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Score strategy reflection evidence.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--goal", default=str(DEFAULT_GOAL))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=float, default=None)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    result = evaluate_from_paths(args.db, args.goal, hours=args.hours, now_ts=args.now_ts)
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
