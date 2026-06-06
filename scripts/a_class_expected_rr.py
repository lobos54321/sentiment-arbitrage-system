"""A_CLASS P0 shadow-discovery denominator and would-RR engine.

The functions here are read-only over the supplied SQLite handle. They build
the quote-clean gold/silver denominator and advisory evidence for the P0
shadow layer without creating trades or changing execution state.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STRATEGY_GOAL_PATH = PROJECT_ROOT / "config" / "strategy-goal.yaml"
WOULD_ENTER_ACTION = "WOULD_ENTER"

DEFAULT_EXIT_CONFIG = {
    "window_hours": 24,
    "evaluation_cadence_minutes": 15,
    "consecutive_windows_required": 4,
    "min_quote_clean_gold_silver_seen_24h": 8,
    "min_quote_clean_gold_silver_would_enter_count": 5,
    "min_quote_clean_gold_silver_would_enter_count_72h": 30,
    "max_would_enter_no_route_rate": 0.10,
    "max_would_enter_trapped_rate": 0.10,
    "max_unknown_data_rate": 0.05,
    "min_outlier_trimmed_would_rr": 2.0,
    "advisory_action_promote": "PROMOTE_TINY_CANARY",
    "advisory_action_insufficient": "INVESTIGATE_SOURCING",
    "requires_human_approval": True,
    "canary_size_sol": 0.001,
    "canary_max_concurrent": 1,
    "canary_max_per_hour": 1,
    "gold_pnl_threshold": 1.00,
    "silver_pnl_threshold": 0.50,
    "defined_fast_stop_pct": 0.15,
    "hard_loss_cap_pct": 0.20,
}


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def _safe_int(value: Any, default: int = 0) -> int:
    number = _safe_float(value, None)
    return default if number is None else int(number)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "tradable"}
    return bool(value)


def _json_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value or not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _table_exists(db, table: str) -> bool:
    try:
        row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    except Exception:
        return False
    return row is not None


def _columns(db, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _rows_as_dicts(rows) -> list[dict]:
    out = []
    for row in rows:
        if row is None:
            continue
        if isinstance(row, dict):
            out.append(dict(row))
            continue
        keys = getattr(row, "keys", None)
        if callable(keys):
            out.append({key: row[key] for key in keys()})
        else:
            out.append(dict(row))
    return out


def _coalesce_expr(cols: set[str], names: list[str], fallback: str = "0", qualifier: str = "") -> str:
    prefix = f"{qualifier}." if qualifier else ""
    available = [f"{prefix}{name}" for name in names if name in cols]
    return f"COALESCE({', '.join([*available, fallback])})"


def _optional_select(cols: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in cols else f"{fallback} AS {name}"


def _scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        return [item.strip().strip("'\"") for item in value[1:-1].split(",") if item.strip()]
    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value.strip("'\"")


def _load_strategy_goal_yaml(path: Path) -> dict:
    data: dict[str, dict] = {}
    current = None
    if not path.exists():
        return data
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if indent == 0 and stripped.endswith(":"):
            current = stripped[:-1]
            data.setdefault(current, {})
            continue
        if current and indent > 0 and ":" in stripped:
            key, value = stripped.split(":", 1)
            data.setdefault(current, {})[key.strip()] = _scalar(value)
    return data


def load_discovery_config(path: str | Path | None = None, overrides: dict | None = None) -> dict:
    """Load the A_CLASS P0 discovery config with stdlib-only YAML fallback."""
    config = dict(DEFAULT_EXIT_CONFIG)
    goal_path = Path(path) if path is not None else DEFAULT_STRATEGY_GOAL_PATH
    sections = _load_strategy_goal_yaml(goal_path)
    config.update(sections.get("a_class_p0_discovery_exit", {}) or {})
    business_goal = sections.get("business_goal", {}) or {}
    risk_limits = sections.get("risk_limits", {}) or {}
    if "gold_pnl_threshold" in business_goal:
        config["gold_pnl_threshold"] = _safe_float(business_goal.get("gold_pnl_threshold"), config["gold_pnl_threshold"])
    if "silver_pnl_threshold" in business_goal:
        config["silver_pnl_threshold"] = _safe_float(business_goal.get("silver_pnl_threshold"), config["silver_pnl_threshold"])
    if "max_single_trade_loss_pct" in risk_limits:
        loss = abs(_safe_float(risk_limits.get("max_single_trade_loss_pct"), 20.0) or 20.0)
        config["hard_loss_cap_pct"] = loss / 100.0 if loss > 1.0 else loss
    if overrides:
        if "a_class_p0_discovery_exit" in overrides and isinstance(overrides["a_class_p0_discovery_exit"], dict):
            config.update(overrides["a_class_p0_discovery_exit"])
        else:
            config.update(overrides)
    return config


def trim10_mean(values: list[float]) -> tuple[float | None, dict]:
    clean = sorted(float(value) for value in values if _safe_float(value, None) is not None)
    detail = {
        "sample_n": len(clean),
        "trimmed_n": 0,
        "small_sample": len(clean) < 10,
        "trim_fraction": 0.10,
    }
    if not clean:
        return None, detail
    trimmed = clean
    if len(clean) >= 10:
        trim_n = int(len(clean) * 0.10)
        if trim_n > 0:
            trimmed = clean[trim_n:-trim_n]
        detail["trimmed_n"] = len(clean) - len(trimmed)
        detail["small_sample"] = False
    return sum(trimmed) / len(trimmed), detail


def _paper_peak_expr(cols: set[str]) -> str:
    if "trusted_peak_pnl" in cols and "quote_peak_pnl" in cols:
        return "COALESCE(NULLIF(trusted_peak_pnl, 0), NULLIF(quote_peak_pnl, 0), 0)"
    if "trusted_peak_pnl" in cols:
        return "COALESCE(NULLIF(trusted_peak_pnl, 0), 0)"
    if "quote_peak_pnl" in cols:
        return "COALESCE(NULLIF(quote_peak_pnl, 0), 0)"
    return "0"


def _missed_peak_expr(cols: set[str]) -> str:
    names = [name for name in ("executable_peak_pnl", "quote_clean_peak_pnl", "tradable_peak_pnl") if name in cols]
    if not names:
        return "NULL"
    return f"COALESCE({', '.join(names)})"


def _paper_quote_clean_known(row: dict, cols: set[str]) -> tuple[bool, bool]:
    if "quote_clean" in cols:
        quote_clean = _truthy(row.get("quote_clean"))
        if "quote_executable" in cols:
            quote_clean = quote_clean and _truthy(row.get("quote_executable"))
        return True, quote_clean
    if "entry_quote_executable" in cols:
        return True, _truthy(row.get("entry_quote_executable"))
    for json_col in ("entry_execution_audit_json", "monitor_state_json", "lotto_state_json"):
        payload = _json_object(row.get(json_col))
        if not payload:
            continue
        if any(
            _truthy(payload.get(key))
            for key in (
                "quote_clean",
                "quote_clean_seen",
                "two_quote_clean_snapshots",
                "recovery_quote_clean",
                "final_reclaim_quote_executable",
                "routeAvailable",
                "success",
            )
        ):
            return True, True
    return False, False


def _fetch_canonical_records(db, since_ts: float, until_ts: float) -> tuple[list[dict], list[str]]:
    if not _table_exists(db, "canonical_trade_ledger"):
        return [], ["canonical_trade_ledger_missing"]
    cols = _columns(db, "canonical_trade_ledger")
    if "token_ca" not in cols:
        return [], ["canonical_trade_ledger_token_ca_missing"]
    if "peak_quote_pnl_pct" not in cols:
        return [], ["canonical_peak_quote_pnl_pct_missing"]
    ts_expr = _coalesce_expr(cols, ["entry_ts", "exit_ts", "created_at", "updated_at"], "0")
    query_cols = [
        "token_ca",
        f"{ts_expr} AS event_ts",
        "peak_quote_pnl_pct AS raw_peak",
        _optional_select(cols, "realized_pnl_pct"),
        _optional_select(cols, "entry_quote_executable", "1"),
        _optional_select(cols, "no_route_flag", "0"),
        _optional_select(cols, "trapped_flag", "0"),
        _optional_select(cols, "outlier_flag", "0"),
    ]
    rows = _rows_as_dicts(db.execute(
        f"""
        SELECT {', '.join(query_cols)}
        FROM canonical_trade_ledger
        WHERE {ts_expr} >= :since_ts
          AND {ts_expr} <= :until_ts
          AND COALESCE(token_ca, '') != ''
        """,
        {"since_ts": since_ts, "until_ts": until_ts},
    ).fetchall())
    records = []
    for row in rows:
        raw_peak = _safe_float(row.get("raw_peak"), None)
        records.append({
            "source": "canonical_trade_ledger",
            "source_rank": 1,
            "token_ca": str(row.get("token_ca") or ""),
            "event_ts": _safe_float(row.get("event_ts"), 0.0) or 0.0,
            "raw_peak": raw_peak,
            "adjusted_peak": max(0.0, raw_peak or 0.0),
            "quote_clean_known": True,
            "quote_clean": _truthy(row.get("entry_quote_executable")),
            "would_stop_before_peak": False,
            "no_route_flag": _truthy(row.get("no_route_flag")),
            "trapped_flag": _truthy(row.get("trapped_flag")),
            "outlier_flag": _truthy(row.get("outlier_flag")),
        })
    return records, []


def _fetch_paper_records(db, since_ts: float, until_ts: float) -> tuple[list[dict], list[str]]:
    if not _table_exists(db, "paper_trades"):
        return [], ["paper_trades_missing"]
    cols = _columns(db, "paper_trades")
    if "token_ca" not in cols:
        return [], ["paper_trades_token_ca_missing"]
    ts_expr = _coalesce_expr(cols, ["entry_ts", "signal_ts", "exit_ts", "created_at"], "0")
    peak_expr = _paper_peak_expr(cols)
    query_cols = [
        "token_ca",
        f"{ts_expr} AS event_ts",
        f"{peak_expr} AS raw_peak",
        _optional_select(cols, "quote_clean", "NULL"),
        _optional_select(cols, "quote_executable", "NULL"),
        _optional_select(cols, "entry_quote_executable", "NULL"),
        _optional_select(cols, "entry_execution_audit_json"),
        _optional_select(cols, "monitor_state_json"),
        _optional_select(cols, "lotto_state_json"),
        _optional_select(cols, "no_route_flag", "0"),
        _optional_select(cols, "trapped_flag", "0"),
        _optional_select(cols, "outlier_flag", "0"),
    ]
    rows = _rows_as_dicts(db.execute(
        f"""
        SELECT {', '.join(query_cols)}
        FROM paper_trades
        WHERE {ts_expr} >= :since_ts
          AND {ts_expr} <= :until_ts
          AND COALESCE(token_ca, '') != ''
        """,
        {"since_ts": since_ts, "until_ts": until_ts},
    ).fetchall())
    records = []
    for row in rows:
        known, clean = _paper_quote_clean_known(row, cols)
        raw_peak = _safe_float(row.get("raw_peak"), 0.0)
        records.append({
            "source": "paper_trades",
            "source_rank": 2,
            "token_ca": str(row.get("token_ca") or ""),
            "event_ts": _safe_float(row.get("event_ts"), 0.0) or 0.0,
            "raw_peak": raw_peak,
            "adjusted_peak": max(0.0, raw_peak or 0.0),
            "quote_clean_known": known,
            "quote_clean": clean,
            "would_stop_before_peak": False,
            "no_route_flag": _truthy(row.get("no_route_flag")),
            "trapped_flag": _truthy(row.get("trapped_flag")),
            "outlier_flag": _truthy(row.get("outlier_flag")),
        })
    return records, []


def _fetch_opportunity_path_records(db, since_ts: float, until_ts: float) -> tuple[list[dict], list[str]]:
    if not _table_exists(db, "opportunity_events"):
        return [], ["opportunity_events_missing"]
    if not _table_exists(db, "opportunity_event_path_samples"):
        return [], ["opportunity_event_path_samples_missing"]
    event_cols = _columns(db, "opportunity_events")
    sample_cols = _columns(db, "opportunity_event_path_samples")
    required_event = {"opportunity_key", "token_ca"}
    required_sample = {"opportunity_key", "quote_pnl_pct"}
    if not required_event.issubset(event_cols):
        return [], ["opportunity_events_core_columns_missing"]
    if not required_sample.issubset(sample_cols):
        return [], ["opportunity_path_samples_core_columns_missing"]
    ts_expr = "event_ts" if "event_ts" in event_cols else ("created_at" if "created_at" in event_cols else "0")
    sample_clean_expr = "COALESCE(s.quote_clean, 0) = 1" if "quote_clean" in sample_cols else "1 = 1"
    sample_exec_expr = "COALESCE(s.quote_executable, 0) = 1" if "quote_executable" in sample_cols else "1 = 1"
    sample_route_expr = "COALESCE(s.route_available, 0) = 1" if "route_available" in sample_cols else "1 = 1"
    event_quote_clean_expr = "COALESCE(e.quote_clean, 0) = 1" if "quote_clean" in event_cols else "0 = 1"
    event_quote_exec_expr = "COALESCE(e.quote_executable, 0) = 1" if "quote_executable" in event_cols else "0 = 1"
    event_route_expr = "COALESCE(e.route_available, 0) = 1" if "route_available" in event_cols else "0 = 1"
    would_expr = "COALESCE(e.would_enter_a_class, 0) = 1" if "would_enter_a_class" in event_cols else "0 = 1"
    did_expr = "COALESCE(e.did_enter, 0) = 1" if "did_enter" in event_cols else "0 = 1"
    no_route_expr = "MAX(COALESCE(s.no_route_flag, 0))" if "no_route_flag" in sample_cols else "0"
    trapped_expr = "MAX(COALESCE(s.trapped_flag, 0))" if "trapped_flag" in sample_cols else "0"
    rows = _rows_as_dicts(db.execute(
        f"""
        SELECT
          e.token_ca,
          {ts_expr} AS event_ts,
          {_optional_select(event_cols, "source_type", "'opportunity_events'")},
          {_optional_select(event_cols, "source_component", "'unknown'")},
          {_optional_select(event_cols, "hydrate_outcome", "'not_recorded'")},
          {_optional_select(event_cols, "would_enter_a_class", "0")},
          {_optional_select(event_cols, "did_enter", "0")},
          CASE
            WHEN {event_quote_clean_expr} AND {event_quote_exec_expr} AND {event_route_expr}
            THEN 1 ELSE 0
          END AS event_quote_clean,
          MAX(
            CASE
              WHEN {sample_clean_expr} AND {sample_exec_expr} AND {sample_route_expr}
              THEN 1 ELSE 0
            END
          ) AS sample_quote_clean,
          MAX(
            CASE
              WHEN {sample_clean_expr} AND {sample_exec_expr} AND {sample_route_expr}
              THEN s.quote_pnl_pct
              ELSE NULL
            END
          ) AS raw_peak,
          COUNT(s.id) AS sample_count,
          {no_route_expr} AS no_route_flag,
          {trapped_expr} AS trapped_flag
        FROM opportunity_events e
        LEFT JOIN opportunity_event_path_samples s
          ON s.opportunity_key = e.opportunity_key
        WHERE {ts_expr} >= :since_ts
          AND {ts_expr} <= :until_ts
          AND COALESCE(e.token_ca, '') != ''
          AND ({would_expr} OR {did_expr} OR ({event_quote_clean_expr} AND {event_quote_exec_expr} AND {event_route_expr}))
        GROUP BY e.opportunity_key
        """,
        {"since_ts": since_ts, "until_ts": until_ts},
    ).fetchall())
    records = []
    for row in rows:
        raw_peak = _safe_float(row.get("raw_peak"), None)
        sample_count = _safe_int(row.get("sample_count"), 0)
        quote_clean = _truthy(row.get("event_quote_clean")) or _truthy(row.get("sample_quote_clean"))
        records.append({
            "source": "opportunity_events",
            "source_rank": 3,
            "token_ca": str(row.get("token_ca") or ""),
            "event_ts": _safe_float(row.get("event_ts"), 0.0) or 0.0,
            "raw_peak": raw_peak,
            "adjusted_peak": None if raw_peak is None else max(0.0, raw_peak),
            "quote_clean_known": _truthy(row.get("event_quote_clean")) or sample_count > 0,
            "quote_clean": quote_clean,
            "would_stop_before_peak": False,
            "no_route_flag": _truthy(row.get("no_route_flag")),
            "trapped_flag": _truthy(row.get("trapped_flag")),
            "outlier_flag": False,
            "source_component": row.get("source_component") or "unknown",
            "hydrate_outcome": row.get("hydrate_outcome") or "not_recorded",
            "would_enter_a_class": _truthy(row.get("would_enter_a_class")) or _truthy(row.get("did_enter")),
            "path_sample_count": sample_count,
        })
    return records, []


def _fetch_missed_records(db, since_ts: float, until_ts: float) -> tuple[list[dict], list[str]]:
    if not _table_exists(db, "paper_missed_signal_attribution"):
        return [], ["paper_missed_signal_attribution_missing"]
    cols = _columns(db, "paper_missed_signal_attribution")
    if "token_ca" not in cols:
        return [], ["paper_missed_signal_attribution_token_ca_missing"]
    ts_expr = _coalesce_expr(cols, ["created_event_ts", "signal_ts", "baseline_ts", "updated_at"], "0")
    peak_expr = _missed_peak_expr(cols)
    query_cols = [
        "token_ca",
        f"{ts_expr} AS event_ts",
        f"{peak_expr} AS raw_peak",
        _optional_select(cols, "tradable_missed", "0"),
        _optional_select(cols, "would_stop_before_peak", "0"),
        _optional_select(cols, "route", "'-'"),
        _optional_select(cols, "component", "'-'"),
        _optional_select(cols, "reject_reason", "'-'"),
        _optional_select(cols, "outlier_flag", "0"),
    ]
    rows = _rows_as_dicts(db.execute(
        f"""
        SELECT {', '.join(query_cols)}
        FROM paper_missed_signal_attribution
        WHERE {ts_expr} >= :since_ts
          AND {ts_expr} <= :until_ts
          AND COALESCE(token_ca, '') != ''
        """,
        {"since_ts": since_ts, "until_ts": until_ts},
    ).fetchall())
    records = []
    for row in rows:
        raw_peak = _safe_float(row.get("raw_peak"), None)
        adjusted = None if raw_peak is None else max(0.0, raw_peak * 0.80 - 0.05)
        stopped = _truthy(row.get("would_stop_before_peak"))
        records.append({
            "source": "paper_missed_signal_attribution",
            "source_rank": 4,
            "token_ca": str(row.get("token_ca") or ""),
            "event_ts": _safe_float(row.get("event_ts"), 0.0) or 0.0,
            "raw_peak": raw_peak,
            "adjusted_peak": adjusted,
            "quote_clean_known": "tradable_missed" in cols,
            "quote_clean": _truthy(row.get("tradable_missed")) and not stopped,
            "would_stop_before_peak": stopped,
            "no_route_flag": False,
            "trapped_flag": False,
            "outlier_flag": _truthy(row.get("outlier_flag")),
            "route": row.get("route") or "-",
            "component": row.get("component") or "-",
            "reject_reason": row.get("reject_reason") or "-",
        })
    return records, []


def _would_enter_tokens(db, since_ts: float, until_ts: float) -> tuple[set[str], list[str]]:
    tokens: set[str] = set()
    issues: list[str] = []
    if _table_exists(db, "a_class_decision_events"):
        cols = _columns(db, "a_class_decision_events")
        if "token_ca" not in cols or "event_ts" not in cols:
            issues.append("a_class_decision_events_core_columns_missing")
        else:
            if "would_action" in cols and "action" in cols:
                action_expr = "COALESCE(would_action, action)"
            elif "would_action" in cols:
                action_expr = "would_action"
            else:
                action_expr = "action"
            rows = db.execute(
                f"""
                SELECT DISTINCT token_ca
                FROM a_class_decision_events
                WHERE event_ts >= :since_ts
                  AND event_ts <= :until_ts
                  AND COALESCE(token_ca, '') != ''
                  AND {action_expr} = :would_enter
                """,
                {"since_ts": since_ts, "until_ts": until_ts, "would_enter": WOULD_ENTER_ACTION},
            ).fetchall()
            tokens.update(str(row[0]) for row in rows if row[0])
    else:
        issues.append("a_class_decision_events_missing")

    if _table_exists(db, "opportunity_events"):
        cols = _columns(db, "opportunity_events")
        if {"token_ca", "event_ts"}.issubset(cols):
            would_expr = "COALESCE(would_enter_a_class, 0) = 1" if "would_enter_a_class" in cols else "0 = 1"
            did_expr = "COALESCE(did_enter, 0) = 1" if "did_enter" in cols else "0 = 1"
            rows = db.execute(
                f"""
                SELECT DISTINCT token_ca
                FROM opportunity_events
                WHERE event_ts >= :since_ts
                  AND event_ts <= :until_ts
                  AND COALESCE(token_ca, '') != ''
                  AND ({would_expr} OR {did_expr})
                """,
                {"since_ts": since_ts, "until_ts": until_ts},
            ).fetchall()
            tokens.update(str(row[0]) for row in rows if row[0])
        else:
            issues.append("opportunity_events_core_columns_missing")
    return tokens, sorted(set(issues))


def _caught_tokens(db, since_ts: float, until_ts: float) -> set[str]:
    caught = set()
    for table, ts_names in (
        ("canonical_trade_ledger", ["entry_ts", "exit_ts", "created_at", "updated_at"]),
        ("paper_trades", ["entry_ts", "signal_ts", "exit_ts", "created_at"]),
    ):
        if not _table_exists(db, table):
            continue
        cols = _columns(db, table)
        if "token_ca" not in cols:
            continue
        ts_expr = _coalesce_expr(cols, ts_names, "0")
        rows = db.execute(
            f"""
            SELECT DISTINCT token_ca
            FROM {table}
            WHERE {ts_expr} >= :since_ts
              AND {ts_expr} <= :until_ts
              AND COALESCE(token_ca, '') != ''
            """,
            {"since_ts": since_ts, "until_ts": until_ts},
        ).fetchall()
        caught.update(str(row[0]) for row in rows if row[0])
    return caught


def _best_source_per_token(records: list[dict]) -> dict[str, dict]:
    grouped: dict[str, list[dict]] = {}
    for record in records:
        token = record.get("token_ca")
        if token:
            grouped.setdefault(str(token), []).append(record)
    selected = {}
    for token, token_records in grouped.items():
        selected[token] = sorted(
            token_records,
            key=lambda row: (
                int(row.get("source_rank") or 99),
                -(_safe_float(row.get("adjusted_peak"), -1.0) or -1.0),
                -(_safe_float(row.get("event_ts"), 0.0) or 0.0),
            ),
        )[0]
    return selected


def _defined_risk(config: dict) -> float:
    fast_stop = abs(_safe_float(config.get("defined_fast_stop_pct"), 0.15) or 0.15)
    hard_cap = abs(_safe_float(config.get("hard_loss_cap_pct"), 0.20) or 0.20)
    return max(fast_stop, hard_cap, 0.15)


def _denominator_exclusion_reason(record: dict, silver_threshold: float) -> str:
    adjusted = _safe_float(record.get("adjusted_peak"), None)
    if not record.get("quote_clean_known"):
        return "quote_clean_unknown"
    if not record.get("quote_clean"):
        return "quote_not_clean"
    if adjusted is None:
        return "path_peak_missing"
    if record.get("would_stop_before_peak"):
        return "would_stop_before_peak"
    if adjusted < silver_threshold:
        return "below_silver_threshold"
    return "eligible"


def _missed_blocker_ranking(db, since_ts: float, until_ts: float, silver_threshold: float) -> list[dict]:
    records, _issues = _fetch_missed_records(db, since_ts, until_ts)
    caught = _caught_tokens(db, since_ts, until_ts)
    best: dict[str, dict] = {}
    for record in records:
        token = record.get("token_ca")
        if not token or token in caught:
            continue
        if record.get("would_stop_before_peak"):
            continue
        adjusted = _safe_float(record.get("adjusted_peak"), None)
        if adjusted is None or adjusted < silver_threshold:
            continue
        current = best.get(token)
        if current is None or (
            adjusted,
            _safe_float(record.get("event_ts"), 0.0) or 0.0,
        ) > (
            _safe_float(current.get("adjusted_peak"), 0.0) or 0.0,
            _safe_float(current.get("event_ts"), 0.0) or 0.0,
        ):
            best[token] = record
    groups: dict[tuple[str, str, str], dict] = {}
    for record in best.values():
        key = (
            str(record.get("route") or "-"),
            str(record.get("component") or "-"),
            str(record.get("reject_reason") or "-"),
        )
        group = groups.setdefault(
            key,
            {
                "route": key[0],
                "component": key[1],
                "reject_reason": key[2],
                "unique_tokens": 0,
                "gold_n": 0,
                "silver_n": 0,
                "max_adjusted_peak": 0.0,
            },
        )
        adjusted = _safe_float(record.get("adjusted_peak"), 0.0) or 0.0
        group["unique_tokens"] += 1
        if adjusted >= 1.0:
            group["gold_n"] += 1
        elif adjusted >= silver_threshold:
            group["silver_n"] += 1
        group["max_adjusted_peak"] = max(group["max_adjusted_peak"], adjusted)
    rows = list(groups.values())
    rows.sort(
        key=lambda row: (
            row["gold_n"],
            row["silver_n"],
            row["unique_tokens"],
            row["max_adjusted_peak"],
        ),
        reverse=True,
    )
    for row in rows:
        row["max_adjusted_peak"] = round(row["max_adjusted_peak"], 6)
    return rows


def calculate_a_class_expected_rr(
    db,
    *,
    since_ts: float | None = None,
    until_ts: float | None = None,
    window_hours: float | None = None,
    config: dict | None = None,
) -> dict:
    """Calculate one P0 discovery window without side effects."""
    cfg = load_discovery_config(overrides=config or {})
    until_ts = float(until_ts if until_ts is not None else time.time())
    if since_ts is None:
        hours = float(window_hours if window_hours is not None else cfg.get("window_hours", 24))
        since_ts = until_ts - hours * 3600.0
    since_ts = float(since_ts)
    silver_threshold = _safe_float(cfg.get("silver_pnl_threshold"), 0.50) or 0.50
    gold_threshold = _safe_float(cfg.get("gold_pnl_threshold"), 1.00) or 1.00
    risk = _defined_risk(cfg)

    source_issues: list[str] = []
    records: list[dict] = []
    for fetcher in (_fetch_canonical_records, _fetch_paper_records, _fetch_opportunity_path_records, _fetch_missed_records):
        source_records, issues = fetcher(db, since_ts, until_ts)
        records.extend(source_records)
        source_issues.extend(issues)

    selected = _best_source_per_token(records)
    would_enter_tokens, action_issues = _would_enter_tokens(db, since_ts, until_ts)
    source_issues.extend(action_issues)

    unknown_tokens = {
        token
        for token, record in selected.items()
        if not record.get("quote_clean_known") or _safe_float(record.get("adjusted_peak"), None) is None
    }
    eligible = []
    for token, record in selected.items():
        adjusted = _safe_float(record.get("adjusted_peak"), None)
        if adjusted is None:
            continue
        if not record.get("quote_clean_known") or not record.get("quote_clean"):
            continue
        if record.get("would_stop_before_peak"):
            continue
        if adjusted >= silver_threshold:
            eligible.append((token, record))

    would_enter = [(token, record) for token, record in eligible if token in would_enter_tokens]
    rr_samples = [
        _safe_float(record.get("adjusted_peak"), 0.0) or 0.0
        for _token, record in would_enter
        if not record.get("outlier_flag")
    ]
    trimmed_mean, trim_detail = trim10_mean(rr_samples)
    would_rr = None if trimmed_mean is None else trimmed_mean / risk
    would_count = len(would_enter)
    all_selected_n = len(selected)
    no_route_n = sum(1 for _token, record in would_enter if record.get("no_route_flag"))
    trapped_n = sum(1 for _token, record in would_enter if record.get("trapped_flag"))

    source_breakdown: dict[str, int] = {}
    source_component_breakdown: dict[str, int] = {}
    hydrate_outcome_breakdown: dict[str, int] = {}
    for _token, record in eligible:
        source = str(record.get("source") or "unknown")
        source_breakdown[source] = source_breakdown.get(source, 0) + 1
        component = str(record.get("source_component") or source)
        source_component_breakdown[component] = source_component_breakdown.get(component, 0) + 1
        hydrate = str(record.get("hydrate_outcome") or "not_recorded")
        hydrate_outcome_breakdown[hydrate] = hydrate_outcome_breakdown.get(hydrate, 0) + 1

    denominator_exclusion_breakdown: dict[str, int] = {}
    unknown_reason_breakdown: dict[str, int] = {}
    observed_hydrate_outcome_breakdown: dict[str, int] = {}
    hydrate_outcome_exclusion_breakdown: dict[str, int] = {}
    for _token, record in selected.items():
        reason = _denominator_exclusion_reason(record, silver_threshold)
        denominator_exclusion_breakdown[reason] = denominator_exclusion_breakdown.get(reason, 0) + 1
        if reason in {"quote_clean_unknown", "path_peak_missing"}:
            unknown_reason_breakdown[reason] = unknown_reason_breakdown.get(reason, 0) + 1
        hydrate = str(record.get("hydrate_outcome") or "not_recorded")
        observed_hydrate_outcome_breakdown[hydrate] = observed_hydrate_outcome_breakdown.get(hydrate, 0) + 1
        outcome_key = f"{hydrate}:{reason}"
        hydrate_outcome_exclusion_breakdown[outcome_key] = hydrate_outcome_exclusion_breakdown.get(outcome_key, 0) + 1

    result = {
        "available": True,
        "status": "shadow_ready",
        "denominator_key": f"quote_clean_gold_silver_unique:{int(since_ts)}:{int(until_ts)}",
        "since_ts": since_ts,
        "until_ts": until_ts,
        "window_hours": round((until_ts - since_ts) / 3600.0, 4),
        "source_precedence": [
            "canonical_trade_ledger",
            "paper_trades",
            "opportunity_events",
            "paper_missed_signal_attribution",
        ],
        "source_issues": sorted(set(source_issues)),
        "source_breakdown": source_breakdown,
        "source_component_breakdown": source_component_breakdown,
        "hydrate_outcome_breakdown": hydrate_outcome_breakdown,
        "observed_hydrate_outcome_breakdown": observed_hydrate_outcome_breakdown,
        "denominator_exclusion_breakdown": denominator_exclusion_breakdown,
        "hydrate_outcome_exclusion_breakdown": hydrate_outcome_exclusion_breakdown,
        "unknown_reason_breakdown": unknown_reason_breakdown,
        "quote_clean_gold_silver_seen_count": len(eligible),
        "quote_clean_gold_silver_gold_count": sum(
            1 for _token, record in eligible if (_safe_float(record.get("adjusted_peak"), 0.0) or 0.0) >= gold_threshold
        ),
        "quote_clean_gold_silver_silver_count": sum(
            1
            for _token, record in eligible
            if silver_threshold <= (_safe_float(record.get("adjusted_peak"), 0.0) or 0.0) < gold_threshold
        ),
        "quote_clean_gold_silver_would_enter_count": would_count,
        "would_enter_no_route_count": no_route_n,
        "would_enter_trapped_count": trapped_n,
        "would_enter_no_route_rate": (no_route_n / would_count) if would_count else 0.0,
        "would_enter_trapped_rate": (trapped_n / would_count) if would_count else 0.0,
        "unknown_data_count": len(unknown_tokens),
        "observed_unique_count": all_selected_n,
        "unknown_data_rate": (len(unknown_tokens) / all_selected_n) if all_selected_n else 0.0,
        "outlier_excluded_count": sum(1 for _token, record in would_enter if record.get("outlier_flag")),
        "trim_detail": trim_detail,
        "defined_risk_pct": risk,
        "outlier_trimmed_adjusted_peak_mean": trimmed_mean,
        "outlier_trimmed_would_rr": would_rr,
        "would_rr": would_rr,
        "missed_blockers": _missed_blocker_ranking(db, since_ts, until_ts, silver_threshold),
        "evidence_notes": [
            "counterfactual peaks use max(0, peak*0.80-0.05)",
            "paper_trades peak fallback is trusted_peak_pnl/quote_peak_pnl/0; peak_pnl is not used",
            "risk is max(defined_fast_stop_pct, hard_loss_cap_pct, 0.15)",
            "API-safe blocker output excludes token identifiers",
        ],
    }
    return result


def _window_thresholds(summary: dict, config: dict) -> list[str]:
    blockers = []
    if summary.get("quote_clean_gold_silver_seen_count", 0) < int(config.get("min_quote_clean_gold_silver_seen_24h", 8)):
        blockers.append("quote_clean_gold_silver_seen_below_min")
    if summary.get("quote_clean_gold_silver_would_enter_count", 0) < int(
        config.get("min_quote_clean_gold_silver_would_enter_count", 5)
    ):
        blockers.append("quote_clean_gold_silver_would_enter_below_min")
    if (summary.get("would_enter_no_route_rate") or 0.0) > float(config.get("max_would_enter_no_route_rate", 0.10)):
        blockers.append("would_enter_no_route_rate_above_max")
    if (summary.get("would_enter_trapped_rate") or 0.0) > float(config.get("max_would_enter_trapped_rate", 0.10)):
        blockers.append("would_enter_trapped_rate_above_max")
    if (summary.get("unknown_data_rate") or 0.0) > float(config.get("max_unknown_data_rate", 0.05)):
        blockers.append("unknown_data_rate_above_max")
    rr = _safe_float(summary.get("outlier_trimmed_would_rr"), None)
    if rr is None or rr < float(config.get("min_outlier_trimmed_would_rr", 2.0)):
        blockers.append("outlier_trimmed_would_rr_below_min")
    return blockers


def evaluate_discovery_exit(db, current: dict, *, config: dict | None = None) -> dict:
    cfg = load_discovery_config(overrides=config or {})
    blockers = _window_thresholds(current, cfg)
    min_seen = int(cfg.get("min_quote_clean_gold_silver_seen_24h", 8))
    if current.get("quote_clean_gold_silver_seen_count", 0) < min_seen:
        return {
            "available": True,
            "advisory": cfg.get("advisory_action_insufficient", "INVESTIGATE_SOURCING"),
            "advisory_action": cfg.get("advisory_action_insufficient", "INVESTIGATE_SOURCING"),
            "advisory_only": True,
            "requires_human_approval": True,
            "reason": "quote_clean_gold_silver_seen_below_min",
            "pass": False,
            "blockers": blockers,
            "min_seen": min_seen,
            "seen_count": current.get("quote_clean_gold_silver_seen_count", 0),
        }

    until_ts = float(current.get("until_ts") or time.time())
    seventy_two = calculate_a_class_expected_rr(
        db,
        since_ts=until_ts - 72 * 3600.0,
        until_ts=until_ts,
        config=cfg,
    )
    min_72h = int(cfg.get("min_quote_clean_gold_silver_would_enter_count_72h", 30))
    if seventy_two.get("quote_clean_gold_silver_would_enter_count", 0) < min_72h:
        blockers.append("quote_clean_gold_silver_would_enter_72h_below_min")

    window_hours = float(cfg.get("window_hours", 24) or 24)
    cadence_sec = float(cfg.get("evaluation_cadence_minutes", 15) or 15) * 60.0
    consecutive_required = max(1, int(cfg.get("consecutive_windows_required", 4) or 4))
    consecutive = []
    for idx in range(consecutive_required):
        window_until = until_ts - idx * cadence_sec
        summary = current if idx == 0 else calculate_a_class_expected_rr(
            db,
            since_ts=window_until - window_hours * 3600.0,
            until_ts=window_until,
            config=cfg,
        )
        window_blockers = _window_thresholds(summary, cfg)
        consecutive.append({
            "offset": idx,
            "until_ts": window_until,
            "pass": not window_blockers,
            "blockers": window_blockers,
            "seen_count": summary.get("quote_clean_gold_silver_seen_count", 0),
            "would_enter_count": summary.get("quote_clean_gold_silver_would_enter_count", 0),
            "outlier_trimmed_would_rr": summary.get("outlier_trimmed_would_rr"),
        })
    if any(not item["pass"] for item in consecutive):
        blockers.append("consecutive_windows_not_met")

    blockers = sorted(set(blockers))
    passed = not blockers
    advisory = cfg.get("advisory_action_promote") if passed else "SHADOW_CONTINUE"
    return {
        "available": True,
        "advisory": advisory,
        "advisory_action": advisory,
        "advisory_only": True,
        "requires_human_approval": bool(cfg.get("requires_human_approval", True)),
        "pass": passed,
        "blockers": blockers,
        "current_window": {
            "seen_count": current.get("quote_clean_gold_silver_seen_count", 0),
            "would_enter_count": current.get("quote_clean_gold_silver_would_enter_count", 0),
            "outlier_trimmed_would_rr": current.get("outlier_trimmed_would_rr"),
        },
        "seventy_two_hour": {
            "would_enter_count": seventy_two.get("quote_clean_gold_silver_would_enter_count", 0),
            "min_would_enter_count": min_72h,
        },
        "consecutive_windows": consecutive,
        "canary_size_sol": cfg.get("canary_size_sol"),
        "canary_max_concurrent": cfg.get("canary_max_concurrent"),
        "canary_max_per_hour": cfg.get("canary_max_per_hour"),
    }


def build_a_class_p0_discovery(
    db,
    *,
    since_ts: float | None = None,
    until_ts: float | None = None,
    window_hours: float | None = None,
    config: dict | None = None,
) -> dict:
    """Build the full P0 discovery read model, including advisory text."""
    cfg = load_discovery_config(overrides=config or {})
    summary = calculate_a_class_expected_rr(
        db,
        since_ts=since_ts,
        until_ts=until_ts,
        window_hours=window_hours,
        config=cfg,
    )
    summary["discovery_exit"] = evaluate_discovery_exit(db, summary, config=cfg)
    summary["expected_rr_detail"] = {
        "denominator_key": summary["denominator_key"],
        "seen_count": summary["quote_clean_gold_silver_seen_count"],
        "would_enter_count": summary["quote_clean_gold_silver_would_enter_count"],
        "defined_risk_pct": summary["defined_risk_pct"],
        "outlier_trimmed_would_rr": summary["outlier_trimmed_would_rr"],
        "trim_detail": summary["trim_detail"],
    }
    return summary
