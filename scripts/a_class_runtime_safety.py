"""Runtime safety state for A_CLASS live tiny canary modes.

Canonical ledger already records realized loss-cap breaches.  This module turns
those immutable trade facts into a small mode-level state that the final-entry
contract can consume before the next live entry.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any


A_CLASS_RUNTIME_MODE_KEY = "A_CLASS_FASTLANE"
DEFAULT_LOSS_CAP_BREACH_COOLDOWN_SEC = 24 * 60 * 60
_BREACH_REASON = "realized_loss_cap_breach"
BREACH_CLASS_DATA_INFRA = "DATA_INFRA"
BREACH_CLASS_PAPER_MARKET = "PAPER_MARKET"
BREACH_CLASS_LIVE_MARKET = "LIVE_MARKET"
BREACH_DETAIL_SCHEMA_VERSION = "a_class_runtime_breach_detail.v2.paper_market_recovery"


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    number = _safe_float(value, None)
    return default if number is None else int(number)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "live", "available"}
    return bool(value)


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except Exception:
        if isinstance(row, dict):
            return row.get(key, default)
        return default


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _table_exists(db, table: str) -> bool:
    try:
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
    except Exception:
        return False


def _table_columns(db, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    try:
        return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def normalize_runtime_mode_key(mode: Any = None, detail: dict | None = None) -> str:
    """Collapse A_CLASS aliases to one runtime key.

    The entry layer has several raw names (A_GRADE_RESONANCE_FASTLANE,
    a_class_fastlane_tiny_canary, etc.).  Runtime safety must affect all of
    them together, otherwise a breach in one alias can be bypassed by another.
    """
    detail = detail if isinstance(detail, dict) else {}
    values = [
        mode,
        detail.get("mode_key"),
        detail.get("normalized_mode"),
        detail.get("normalizedEntryMode"),
        detail.get("entry_mode"),
        detail.get("entryMode"),
    ]
    text = " ".join(str(value or "") for value in values).lower()
    if "a_class" in text or "fastlane" in text or "a_grade_resonance" in text:
        return A_CLASS_RUNTIME_MODE_KEY
    raw = next((str(value).strip() for value in values if str(value or "").strip()), "UNKNOWN")
    return raw.upper()


def is_a_class_runtime_mode(mode: Any = None, detail: dict | None = None) -> bool:
    return normalize_runtime_mode_key(mode, detail) == A_CLASS_RUNTIME_MODE_KEY


def loss_cap_breach_cooldown_sec(config: Any = None) -> int:
    value = None
    if config is not None:
        value = getattr(config, "loss_cap_breach_cooldown_sec", None)
    if value is None:
        value = os.environ.get("A_CLASS_LOSS_CAP_BREACH_COOLDOWN_SEC")
    return max(0, _safe_int(value, DEFAULT_LOSS_CAP_BREACH_COOLDOWN_SEC))


def init_runtime_safety(db) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS a_class_mode_runtime_state (
            mode_key TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            action TEXT,
            circuit_broken INTEGER DEFAULT 0,
            reason TEXT,
            source_trade_id TEXT,
            token_ca TEXT,
            symbol TEXT,
            last_realized_pnl_pct REAL,
            last_realized_pnl_sol REAL,
            loss_cap_pct REAL,
            breach_count INTEGER DEFAULT 0,
            last_breach_ts REAL,
            cooldown_until_ts REAL,
            clean_windows_required INTEGER DEFAULT 4,
            detail_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_a_class_mode_runtime_state_updated
        ON a_class_mode_runtime_state(updated_at DESC)
        """
    )
    try:
        db.commit()
    except Exception:
        pass


def _default_live_state(mode_key: str, now_ts: float) -> dict:
    return {
        "mode_key": mode_key,
        "status": "LIVE",
        "action": "LIVE",
        "circuit_broken": False,
        "reason": None,
        "source_trade_id": None,
        "cooldown_until_ts": None,
        "cooldown_remaining_sec": 0,
        "recovery_required": False,
        "clean_windows_required": 0,
        "evaluated_at": now_ts,
    }


def _effective_state(row: Any, *, mode_key: str, now_ts: float) -> dict:
    if row is None:
        return _default_live_state(mode_key, now_ts)
    cooldown_until = _safe_float(_row_value(row, "cooldown_until_ts"), None)
    stored_status = str(_row_value(row, "status", "LIVE") or "LIVE").upper()
    stored_circuit = _truthy(_row_value(row, "circuit_broken", False))
    in_cooldown = cooldown_until is not None and cooldown_until > now_ts
    effective_status = stored_status
    effective_action = str(_row_value(row, "action", stored_status) or stored_status).upper()
    effective_circuit = stored_circuit
    recovery_required = False
    reason = _row_value(row, "reason")
    if stored_circuit and in_cooldown:
        effective_status = "CIRCUIT_BROKEN"
        effective_action = "SHADOW"
        effective_circuit = True
    elif stored_circuit:
        effective_status = "SHADOW"
        effective_action = "SHADOW"
        effective_circuit = False
        recovery_required = True
        reason = "cooldown_elapsed_requires_clean_windows"
    return {
        "mode_key": mode_key,
        "status": effective_status,
        "action": effective_action,
        "circuit_broken": bool(effective_circuit),
        "stored_status": stored_status,
        "stored_circuit_broken": bool(stored_circuit),
        "reason": reason,
        "source_trade_id": _row_value(row, "source_trade_id"),
        "token_ca": _row_value(row, "token_ca"),
        "symbol": _row_value(row, "symbol"),
        "last_realized_pnl_pct": _safe_float(_row_value(row, "last_realized_pnl_pct"), None),
        "last_realized_pnl_sol": _safe_float(_row_value(row, "last_realized_pnl_sol"), None),
        "loss_cap_pct": _safe_float(_row_value(row, "loss_cap_pct"), None),
        "breach_count": _safe_int(_row_value(row, "breach_count"), 0),
        "last_breach_ts": _safe_float(_row_value(row, "last_breach_ts"), None),
        "cooldown_until_ts": cooldown_until,
        "cooldown_remaining_sec": max(0.0, (cooldown_until or 0.0) - now_ts) if cooldown_until is not None else 0,
        "recovery_required": recovery_required,
        "clean_windows_required": _safe_int(_row_value(row, "clean_windows_required"), 4),
        "detail": _parse_json(_row_value(row, "detail_json"), {}),
        "evaluated_at": now_ts,
    }


def _parse_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _paper_trade_lookup(db, trade_id: str) -> dict:
    if not _table_exists(db, "paper_trades"):
        return {"available": False, "reason": "paper_trades_missing"}
    cols = _table_columns(db, "paper_trades")
    if "id" not in cols:
        return {"available": False, "reason": "paper_trades_missing_id"}
    select = [
        "id",
        "paper_only" if "paper_only" in cols else "NULL AS paper_only",
        "token_ca" if "token_ca" in cols else "NULL AS token_ca",
        "symbol" if "symbol" in cols else "NULL AS symbol",
        "premium_signal_id" if "premium_signal_id" in cols else "NULL AS premium_signal_id",
        "entry_ts" if "entry_ts" in cols else "NULL AS entry_ts",
        "exit_ts" if "exit_ts" in cols else "NULL AS exit_ts",
        "entry_price" if "entry_price" in cols else "NULL AS entry_price",
        "exit_price" if "exit_price" in cols else "NULL AS exit_price",
        "pnl_pct" if "pnl_pct" in cols else "NULL AS pnl_pct",
        "exit_reason" if "exit_reason" in cols else "NULL AS exit_reason",
    ]
    try:
        row = db.execute(f"SELECT {', '.join(select)} FROM paper_trades WHERE id = ?", (trade_id,)).fetchone()
    except Exception as exc:
        return {"available": False, "reason": "paper_trade_lookup_failed", "error": str(exc)}
    if row is None:
        return {"available": False, "reason": "paper_trade_not_found"}
    return {
        "available": True,
        "paper_trade_id": str(_row_value(row, "id")),
        "paper_only": bool(_truthy(_row_value(row, "paper_only", True))),
        "token_ca": _row_value(row, "token_ca"),
        "symbol": _row_value(row, "symbol"),
        "premium_signal_id": _row_value(row, "premium_signal_id"),
        "entry_ts": _safe_float(_row_value(row, "entry_ts"), None),
        "exit_ts": _safe_float(_row_value(row, "exit_ts"), None),
        "entry_price": _safe_float(_row_value(row, "entry_price"), None),
        "exit_price": _safe_float(_row_value(row, "exit_price"), None),
        "pnl_pct": _safe_float(_row_value(row, "pnl_pct"), None),
        "exit_reason": _row_value(row, "exit_reason"),
    }


def _is_paper_only_breach(row: Any, detail: dict, paper_trade: dict) -> tuple[bool, list[str]]:
    evidence = []
    if paper_trade.get("available") and bool(paper_trade.get("paper_only")):
        evidence.append("paper_trades.paper_only")
    metadata = _parse_json(_row_value(row, "metadata_json"), {})
    for source, payload in (("loss_cap_detail_json", detail), ("canonical_metadata_json", metadata)):
        if not isinstance(payload, dict):
            continue
        if _truthy(payload.get("paper_only")):
            evidence.append(f"{source}.paper_only")
        if _truthy(payload.get("paper_only_scout")):
            evidence.append(f"{source}.paper_only_scout")
        scope = str(payload.get("execution_scope") or payload.get("executionScope") or "").lower()
        if scope == "paper_only":
            evidence.append(f"{source}.execution_scope")
    return bool(evidence), sorted(set(evidence))


def _classify_breach(row: Any, detail: dict, paper_trade: dict) -> dict:
    paper_only, paper_evidence = _is_paper_only_breach(row, detail, paper_trade)
    evidence = list(paper_evidence)
    if _truthy(_row_value(row, "no_route_flag")):
        evidence.append("canonical_ledger.no_route_flag")
    if _truthy(_row_value(row, "trapped_flag")):
        evidence.append("canonical_ledger.trapped_flag")
    if not paper_only and any(item.endswith(("no_route_flag", "trapped_flag")) for item in evidence):
        return {
            "breach_class": BREACH_CLASS_DATA_INFRA,
            "source": "canonical_ledger_runtime_flags",
            "evidence": sorted(set(evidence)),
            "paper_only": False,
        }
    if paper_only:
        evidence.append("paper_only_loss_cap_breach")
        return {
            "breach_class": BREACH_CLASS_PAPER_MARKET,
            "source": "paper_trade_runtime_lookup",
            "evidence": sorted(set(evidence)),
            "paper_only": True,
        }
    evidence.append("a_class_loss_cap_breach_without_paper_only_marker")
    return {
        "breach_class": BREACH_CLASS_LIVE_MARKET,
        "source": "canonical_ledger_default_live_market",
        "evidence": sorted(set(evidence)),
        "paper_only": False,
    }


def fetch_mode_runtime_state(db, mode: Any = None, *, now_ts: float | None = None) -> dict:
    now_ts = float(now_ts if now_ts is not None else time.time())
    mode_key = normalize_runtime_mode_key(mode)
    if mode_key != A_CLASS_RUNTIME_MODE_KEY:
        return _default_live_state(mode_key, now_ts)
    init_runtime_safety(db)
    row = db.execute(
        "SELECT * FROM a_class_mode_runtime_state WHERE mode_key = ?",
        (mode_key,),
    ).fetchone()
    return _effective_state(row, mode_key=mode_key, now_ts=now_ts)


def record_loss_cap_breach_reaction(
    db,
    trade_id: Any,
    *,
    mode: Any = None,
    now_ts: float | None = None,
    cooldown_sec: int | None = None,
    clean_windows_required: int = 4,
) -> dict:
    """Downgrade A_CLASS live modes after a realized SOL loss-cap breach.

    Returns a payload suitable for paper_decision_events.  The write is
    idempotent per source trade id so close retries do not keep extending the
    cooldown or incrementing the breach count.
    """
    now_ts = float(now_ts if now_ts is not None else time.time())
    trade_id = str(trade_id)
    init_runtime_safety(db)
    if not _table_exists(db, "canonical_trade_ledger"):
        return {"breach": False, "reason": "canonical_trade_ledger_missing", "should_record_event": False}
    row = db.execute(
        """
        SELECT trade_id, token_ca, symbol, normalized_mode, entry_mode,
               COALESCE(is_a_class_fastlane, 0) AS is_a_class_fastlane,
               loss_cap_breach, loss_cap_pct, loss_cap_detail_json,
               realized_pnl_pct, realized_pnl_sol, entry_size_sol, exit_ts,
               exit_reason, no_route_flag, trapped_flag, metadata_json
        FROM canonical_trade_ledger
        WHERE trade_id = ?
        """,
        (trade_id,),
    ).fetchone()
    if row is None:
        return {"breach": False, "reason": "trade_not_found", "trade_id": trade_id, "should_record_event": False}
    row_mode = mode or _row_value(row, "normalized_mode") or _row_value(row, "entry_mode")
    mode_key = normalize_runtime_mode_key(
        row_mode,
        {
            "normalized_mode": _row_value(row, "normalized_mode"),
            "entry_mode": _row_value(row, "entry_mode"),
        },
    )
    if mode_key != A_CLASS_RUNTIME_MODE_KEY and not _truthy(_row_value(row, "is_a_class_fastlane")):
        return {
            "breach": bool(_truthy(_row_value(row, "loss_cap_breach"))),
            "reason": "non_a_class_mode_ignored",
            "mode_key": mode_key,
            "trade_id": trade_id,
            "should_record_event": False,
        }
    if not _truthy(_row_value(row, "loss_cap_breach")):
        return {
            "breach": False,
            "reason": "loss_cap_not_breached",
            "mode_key": mode_key,
            "trade_id": trade_id,
            "should_record_event": False,
        }

    cooldown_sec = loss_cap_breach_cooldown_sec() if cooldown_sec is None else max(0, int(cooldown_sec))
    event_ts = _safe_float(_row_value(row, "exit_ts"), now_ts) or now_ts
    cooldown_until = max(now_ts, event_ts) + cooldown_sec
    loss_cap_detail = _parse_json(_row_value(row, "loss_cap_detail_json"), {})
    paper_trade = _paper_trade_lookup(db, trade_id)
    breach_classification = _classify_breach(row, loss_cap_detail, paper_trade)
    existing = db.execute(
        "SELECT source_trade_id, breach_count FROM a_class_mode_runtime_state WHERE mode_key = ?",
        (mode_key,),
    ).fetchone()
    duplicate = existing is not None and str(_row_value(existing, "source_trade_id") or "") == trade_id
    breach_count = _safe_int(_row_value(existing, "breach_count"), 0) if duplicate else _safe_int(_row_value(existing, "breach_count"), 0) + 1
    paper_only = bool(breach_classification.get("paper_only"))
    detail = {
        "schema_version": BREACH_DETAIL_SCHEMA_VERSION,
        "breach": True,
        "reason": _BREACH_REASON,
        "breach_class": breach_classification.get("breach_class"),
        "breach_class_source": breach_classification.get("source"),
        "breach_class_evidence": breach_classification.get("evidence") or [],
        "mode_key": mode_key,
        "trade_id": trade_id,
        "token_ca": _row_value(row, "token_ca"),
        "symbol": _row_value(row, "symbol"),
        "normalized_mode": _row_value(row, "normalized_mode"),
        "entry_mode": _row_value(row, "entry_mode"),
        "realized_pnl_pct": _safe_float(_row_value(row, "realized_pnl_pct"), None),
        "realized_pnl_sol": _safe_float(_row_value(row, "realized_pnl_sol"), None),
        "entry_size_sol": _safe_float(_row_value(row, "entry_size_sol"), None),
        "loss_cap_pct": _safe_float(_row_value(row, "loss_cap_pct"), 0.20),
        "exit_reason": _row_value(row, "exit_reason"),
        "no_route_flag": bool(_truthy(_row_value(row, "no_route_flag"))),
        "trapped_flag": bool(_truthy(_row_value(row, "trapped_flag"))),
        "paper_only": paper_only,
        "paper_trade_lookup": paper_trade,
        "paper_recovery_contract": {
            "paper_auto_resume_after_clean_windows_allowed": paper_only,
            "paper_auto_recovery_counter_started": paper_only,
            "paper_only_loss_records_recap_required": paper_only,
            "paper_only_loss_requires_human_live_reenable": True,
            "live_reenable_requires_human_operator": True,
            "changes_strategy_or_gates": False,
        },
        "cooldown_sec": cooldown_sec,
        "cooldown_until_ts": cooldown_until,
        "clean_windows_required": int(clean_windows_required),
        "idempotent_duplicate": duplicate,
    }
    db.execute(
        """
        INSERT INTO a_class_mode_runtime_state (
            mode_key, status, action, circuit_broken, reason, source_trade_id,
            token_ca, symbol, last_realized_pnl_pct, last_realized_pnl_sol,
            loss_cap_pct, breach_count, last_breach_ts, cooldown_until_ts,
            clean_windows_required, detail_json, created_at, updated_at
        )
        VALUES (?, 'CIRCUIT_BROKEN', 'SHADOW', 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mode_key) DO UPDATE SET
            status='CIRCUIT_BROKEN',
            action='SHADOW',
            circuit_broken=1,
            reason=excluded.reason,
            source_trade_id=excluded.source_trade_id,
            token_ca=excluded.token_ca,
            symbol=excluded.symbol,
            last_realized_pnl_pct=excluded.last_realized_pnl_pct,
            last_realized_pnl_sol=excluded.last_realized_pnl_sol,
            loss_cap_pct=excluded.loss_cap_pct,
            breach_count=excluded.breach_count,
            last_breach_ts=excluded.last_breach_ts,
            cooldown_until_ts=excluded.cooldown_until_ts,
            clean_windows_required=excluded.clean_windows_required,
            detail_json=excluded.detail_json,
            updated_at=excluded.updated_at
        """,
        (
            mode_key,
            _BREACH_REASON,
            trade_id,
            _row_value(row, "token_ca"),
            _row_value(row, "symbol"),
            _safe_float(_row_value(row, "realized_pnl_pct"), None),
            _safe_float(_row_value(row, "realized_pnl_sol"), None),
            _safe_float(_row_value(row, "loss_cap_pct"), 0.20),
            breach_count,
            event_ts,
            cooldown_until,
            int(clean_windows_required),
            _json_dumps(detail),
            now_ts,
            now_ts,
        ),
    )
    try:
        db.commit()
    except Exception:
        pass
    return {
        **detail,
        "breach_count": breach_count,
        "status": "CIRCUIT_BROKEN",
        "action": "SHADOW",
        "circuit_broken": True,
        "should_record_event": not duplicate,
    }


def summarize_runtime_safety(db, *, since_ts: float | None = None, now_ts: float | None = None) -> dict:
    now_ts = float(now_ts if now_ts is not None else time.time())
    init_runtime_safety(db)
    loss_cap_breach_n = 0
    recent_breaches = []
    ledger_cols = _table_columns(db, "canonical_trade_ledger")
    if ledger_cols and "loss_cap_breach" in ledger_cols:
        where = "WHERE COALESCE(loss_cap_breach, 0) = 1"
        params: tuple[Any, ...] = ()
        if since_ts is not None:
            where += " AND COALESCE(exit_ts, updated_at, created_at, 0) >= ?"
            params = (float(since_ts),)
        row = db.execute(f"SELECT COUNT(*) AS n FROM canonical_trade_ledger {where}", params).fetchone()
        loss_cap_breach_n = _safe_int(_row_value(row, "n"), 0)
        recent_breaches = [
            dict(item)
            for item in db.execute(
                f"""
                SELECT trade_id, token_ca, symbol, normalized_mode, entry_mode,
                       exit_ts, realized_pnl_pct, realized_pnl_sol, loss_cap_pct,
                       exit_reason, no_route_flag, trapped_flag, loss_cap_detail_json
                FROM canonical_trade_ledger
                {where}
                ORDER BY COALESCE(exit_ts, updated_at, created_at, 0) DESC
                LIMIT 20
                """,
                params,
            ).fetchall()
        ]
    state_rows = db.execute("SELECT * FROM a_class_mode_runtime_state ORDER BY updated_at DESC").fetchall()
    mode_states = [
        _effective_state(row, mode_key=str(_row_value(row, "mode_key") or A_CLASS_RUNTIME_MODE_KEY), now_ts=now_ts)
        for row in state_rows
    ]
    downgraded_modes = [
        state for state in mode_states
        if str(state.get("status") or "LIVE").upper() != "LIVE" or bool(state.get("recovery_required"))
    ]
    mode_circuit_broken = any(bool(state.get("circuit_broken")) for state in mode_states)
    if mode_circuit_broken:
        next_safe_action = "keep_breached_modes_shadow_until_cooldown"
    elif downgraded_modes:
        next_safe_action = "keep_breached_modes_shadow_until_clean_windows"
    else:
        next_safe_action = "continue_a_class_observation"
    return {
        "available": True,
        "schema_version": "v1.a_class_runtime_safety",
        "loss_cap_breach_n": loss_cap_breach_n,
        "mode_circuit_broken": mode_circuit_broken,
        "downgraded_modes": downgraded_modes,
        "mode_states": mode_states,
        "recent_breaches": recent_breaches,
        "next_safe_action": next_safe_action,
        "generated_at_ts": now_ts,
    }
