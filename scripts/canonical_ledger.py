"""Canonical SOL-accounting ledger for strategy evaluation.

P0 creates the schema plus A_CLASS decision events. Shadow decisions are kept
out of canonical_trade_ledger so EV is not polluted by non-trades.
"""

import json
import time


def _json_default(value):
    try:
        return value.to_dict()
    except AttributeError:
        return str(value)


def _json_dumps(value):
    return json.dumps(value, sort_keys=True, default=_json_default)


def _get(value, key, default=None):
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _safe_float(value, default=None):
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None):
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _as_dict(value):
    if value is None:
        return {}
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return dict(value)
    try:
        return dict(value)
    except (TypeError, ValueError):
        return dict(vars(value))


def _table_columns(db, table):
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _index_exists(db, index_name):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1",
        (index_name,),
    ).fetchone()
    return row is not None


def init_canonical_ledger(db):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS a_class_decision_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts REAL NOT NULL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            route_bucket TEXT,
            normalized_mode TEXT,
            source_table TEXT,
            source_id INTEGER,
            source_component TEXT,
            source_reason TEXT,
            opportunity_key TEXT,
            is_duplicate INTEGER DEFAULT 0,
            duplicate_of_id INTEGER,
            signal_ts REAL,
            opportunity_ts REAL,
            action TEXT NOT NULL,
            grade TEXT,
            size_sol REAL DEFAULT 0,
            score REAL DEFAULT 0,
            reason TEXT,
            hard_blockers_json TEXT,
            soft_notes_json TEXT,
            freshness_json TEXT,
            budget_json TEXT,
            risk_json TEXT,
            source_dedup_key TEXT,
            would_action TEXT,
            expected_rr REAL,
            expected_upside_pct REAL,
            defined_risk_pct REAL,
            bottom_ticket_size_sol REAL,
            expected_rr_detail_json TEXT,
            matrix_json TEXT,
            ai_review_json TEXT,
            controller_action_json TEXT,
            denominator_key TEXT,
            discovery_exit_json TEXT,
            principal_recovery_plan_json TEXT,
            moonbag_plan_json TEXT,
            candidate_json TEXT,
            created_at REAL NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_a_class_decision_source
        ON a_class_decision_events(source_table, source_id)
        WHERE source_table IS NOT NULL AND source_id IS NOT NULL
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_a_class_decision_recent
        ON a_class_decision_events(event_ts DESC, action, route_bucket)
        """
    )
    for col_name, col_def in (
        ("opportunity_key", "TEXT"),
        ("is_duplicate", "INTEGER DEFAULT 0"),
        ("duplicate_of_id", "INTEGER"),
        ("source_dedup_key", "TEXT"),
        ("would_action", "TEXT"),
        ("expected_rr", "REAL"),
        ("expected_upside_pct", "REAL"),
        ("defined_risk_pct", "REAL"),
        ("bottom_ticket_size_sol", "REAL"),
        ("expected_rr_detail_json", "TEXT"),
        ("matrix_json", "TEXT"),
        ("ai_review_json", "TEXT"),
        ("controller_action_json", "TEXT"),
        ("denominator_key", "TEXT"),
        ("discovery_exit_json", "TEXT"),
        ("principal_recovery_plan_json", "TEXT"),
        ("moonbag_plan_json", "TEXT"),
    ):
        if col_name not in _table_columns(db, "a_class_decision_events"):
            try:
                db.execute(f"ALTER TABLE a_class_decision_events ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass
    if not _index_exists(db, "idx_a_class_decision_dedup"):
        db.execute(
            """
            UPDATE a_class_decision_events
               SET source_dedup_key = CASE
                 WHEN source_table IS NOT NULL AND source_id IS NOT NULL THEN source_table||':'||source_id
                 WHEN opportunity_key IS NOT NULL THEN 'opportunity:'||opportunity_key
                 ELSE 'token:'||COALESCE(token_ca,'')||':'||COALESCE(route_bucket,'')||':'||CAST(CAST(event_ts/300 AS INT) AS TEXT)
               END
             WHERE source_dedup_key IS NULL
            """
        )
        db.execute(
            """
            WITH ranked AS (
              SELECT id,
                     ROW_NUMBER() OVER (PARTITION BY source_dedup_key ORDER BY id) AS rn
              FROM a_class_decision_events
              WHERE source_dedup_key IS NOT NULL
            )
            UPDATE a_class_decision_events
               SET source_dedup_key = source_dedup_key || '#legacy:' || id
             WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
            """
        )
        db.execute("DROP INDEX IF EXISTS idx_a_class_decision_source")
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_a_class_decision_source
            ON a_class_decision_events(source_table, source_id)
            WHERE source_table IS NOT NULL AND source_id IS NOT NULL
            """
        )
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_a_class_decision_dedup
            ON a_class_decision_events(source_dedup_key)
            WHERE source_dedup_key IS NOT NULL
            """
        )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_a_class_decision_opportunity
        ON a_class_decision_events(opportunity_key, action, event_ts DESC)
        WHERE opportunity_key IS NOT NULL
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS canonical_trade_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT UNIQUE,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            route_bucket TEXT,
            entry_mode TEXT,
            normalized_mode TEXT,
            strategy_family TEXT,
            source_component TEXT,
            source_reason TEXT,

            entry_ts REAL,
            entry_size_sol REAL,
            entry_price REAL,
            entry_quote_out REAL,
            entry_quote_out_raw TEXT,
            entry_quote_source TEXT,
            entry_route_available INTEGER,
            entry_quote_executable INTEGER,
            entry_quote_age_sec REAL,
            entry_spread_pct REAL,
            entry_liquidity_usd REAL,
            entry_market_cap REAL,
            entry_data_confidence TEXT,

            exit_ts REAL,
            exit_price REAL,
            exit_quote_out_sol REAL,
            exit_quote_source TEXT,
            exit_route_available INTEGER,
            exit_quote_executable INTEGER,
            exit_quote_age_sec REAL,
            exit_reason TEXT,

            realized_exit_sol REAL,
            realized_pnl_sol REAL,
            realized_pnl_pct REAL,
            total_fees_sol REAL DEFAULT 0,
            slippage_bps REAL,
            accounting_source TEXT,

            peak_quote_pnl_pct REAL,
            peak_quote_pnl_sol REAL,
            max_drawdown_pct REAL,
            time_to_peak_sec REAL,
            time_held_sec REAL,
            positive_feedback_seen INTEGER,
            first_positive_feedback_sec REAL,

            trapped_flag INTEGER DEFAULT 0,
            no_route_flag INTEGER DEFAULT 0,
            stale_flag INTEGER DEFAULT 0,
            hard_blocker_override_flag INTEGER DEFAULT 0,
            outlier_flag INTEGER DEFAULT 0,
            outlier_reason TEXT,
            security_flags_json TEXT,
            gmgn_policy_json TEXT,

            is_a_class_fastlane INTEGER DEFAULT 0,
            a_class_grade TEXT,
            a_class_score REAL,
            a_class_size_rule TEXT,
            a_class_freshness_sources_json TEXT,
            a_class_hard_prefilter_json TEXT,
            a_class_budget_state_json TEXT,
            expected_rr REAL,
            expected_upside_pct REAL,
            defined_risk_pct REAL,
            bottom_ticket_size_sol REAL,
            principal_recovery_plan_json TEXT,
            moonbag_plan_json TEXT,
            a_class_matrix_json TEXT,
            ai_review_json TEXT,
            controller_action_json TEXT,

            metadata_json TEXT,
            code_version TEXT,
            deploy_version TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    for col_name, col_def in (
        ("expected_rr", "REAL"),
        ("expected_upside_pct", "REAL"),
        ("defined_risk_pct", "REAL"),
        ("bottom_ticket_size_sol", "REAL"),
        ("principal_recovery_plan_json", "TEXT"),
        ("moonbag_plan_json", "TEXT"),
        ("a_class_matrix_json", "TEXT"),
        ("ai_review_json", "TEXT"),
        ("controller_action_json", "TEXT"),
    ):
        if col_name not in _table_columns(db, "canonical_trade_ledger"):
            try:
                db.execute(f"ALTER TABLE canonical_trade_ledger ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_canonical_trade_mode_recent
        ON canonical_trade_ledger(normalized_mode, entry_ts DESC)
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_canonical_trade_a_class
        ON canonical_trade_ledger(is_a_class_fastlane, entry_ts DESC)
        """
    )
    try:
        db.commit()
    except Exception:
        pass


def record_a_class_decision_event(
    db,
    *,
    candidate,
    decision,
    stored_action=None,
    source_table=None,
    source_id=None,
    now_ts=None,
):
    init_canonical_ledger(db)
    now_ts = float(now_ts if now_ts is not None else time.time())
    action = stored_action or _get(decision, "action", "SHADOW")
    candidate_dict = _as_dict(candidate)
    decision_dict = _as_dict(decision)
    opportunity_key = _get(candidate, "opportunity_key")
    source_dedup_key = _get(decision, "source_dedup_key", None) or _get(candidate, "source_dedup_key", None)
    if not source_dedup_key:
        if source_table is not None and source_id is not None:
            source_dedup_key = f"{source_table}:{source_id}"
        elif opportunity_key:
            source_dedup_key = f"opportunity:{opportunity_key}"
        else:
            bucket = int(now_ts / 300)
            # Deliberately merges within 5 minutes; denominators dedup by token_ca at read time.
            source_dedup_key = f"token:{_get(candidate, 'token_ca') or ''}:{_get(candidate, 'route_bucket') or ''}:{bucket}"
    would_action = _get(decision, "would_action", None)
    if would_action is None and action == "WOULD_ENTER":
        would_action = "WOULD_ENTER"
    expected_rr_detail = _get(decision, "expected_rr_detail", {}) or {}
    expected_rr = _safe_float(
        _get(decision, "expected_rr", None)
        if _get(decision, "expected_rr", None) is not None
        else expected_rr_detail.get("outlier_trimmed_would_rr"),
        None,
    )
    expected_upside_pct = _safe_float(
        _get(decision, "expected_upside_pct", None)
        if _get(decision, "expected_upside_pct", None) is not None
        else expected_rr_detail.get("expected_upside_pct"),
        None,
    )
    defined_risk_pct = _safe_float(
        _get(decision, "defined_risk_pct", None)
        if _get(decision, "defined_risk_pct", None) is not None
        else expected_rr_detail.get("defined_risk_pct"),
        None,
    )
    bottom_ticket_size_sol = _safe_float(
        _get(decision, "bottom_ticket_size_sol", None)
        if _get(decision, "bottom_ticket_size_sol", None) is not None
        else expected_rr_detail.get("bottom_ticket_size_sol"),
        None,
    )
    denominator_key = _get(decision, "denominator_key", None) or expected_rr_detail.get("denominator_key")
    discovery_exit = _get(decision, "discovery_exit", None) or expected_rr_detail.get("discovery_exit")
    matrix_detail = _get(decision, "matrix_detail", {}) or {}
    ai_review = _get(decision, "ai_review", {}) or {}
    controller_action = _get(decision, "controller_action", {}) or {}
    principal_recovery_plan = (
        _get(decision, "principal_recovery_plan", {}) or expected_rr_detail.get("principal_recovery_plan") or {}
    )
    moonbag_plan = _get(decision, "moonbag_plan", {}) or expected_rr_detail.get("moonbag_plan") or {}
    db.execute(
        """
        INSERT INTO a_class_decision_events (
            event_ts, token_ca, symbol, lifecycle_id, route_bucket, normalized_mode,
            source_table, source_id, source_component, source_reason,
            opportunity_key, source_dedup_key, is_duplicate, duplicate_of_id, signal_ts,
            opportunity_ts, action, grade, size_sol, score, reason,
            hard_blockers_json, soft_notes_json, freshness_json, budget_json,
            risk_json, would_action, expected_rr, expected_upside_pct,
            defined_risk_pct, bottom_ticket_size_sol, expected_rr_detail_json,
            matrix_json, ai_review_json, controller_action_json, denominator_key,
            discovery_exit_json, principal_recovery_plan_json, moonbag_plan_json,
            candidate_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_dedup_key) WHERE source_dedup_key IS NOT NULL DO UPDATE SET
            would_action=excluded.would_action,
            expected_rr=excluded.expected_rr,
            expected_upside_pct=excluded.expected_upside_pct,
            defined_risk_pct=excluded.defined_risk_pct,
            bottom_ticket_size_sol=excluded.bottom_ticket_size_sol,
            expected_rr_detail_json=excluded.expected_rr_detail_json,
            matrix_json=excluded.matrix_json,
            ai_review_json=excluded.ai_review_json,
            controller_action_json=excluded.controller_action_json,
            denominator_key=excluded.denominator_key,
            discovery_exit_json=excluded.discovery_exit_json,
            principal_recovery_plan_json=excluded.principal_recovery_plan_json,
            moonbag_plan_json=excluded.moonbag_plan_json
        """,
        (
            now_ts,
            _get(candidate, "token_ca"),
            _get(candidate, "symbol"),
            _get(candidate, "lifecycle_id"),
            _get(candidate, "route_bucket"),
            "A_GRADE_RESONANCE_FASTLANE",
            source_table,
            source_id,
            _get(candidate, "source_component"),
            _get(candidate, "source_reason"),
            opportunity_key,
            source_dedup_key,
            1 if _truthy(_get(candidate, "is_duplicate", False)) else 0,
            _safe_int(_get(candidate, "duplicate_of_event_id"), None),
            _safe_float(_get(candidate, "signal_ts"), None),
            _safe_float(decision_dict.get("freshness_detail", {}).get("opportunity_ts"), None)
            or _safe_float(_get(candidate, "opportunity_ts"), None),
            action,
            _get(decision, "grade"),
            _safe_float(_get(decision, "size_sol"), 0.0) or 0.0,
            _safe_float(_get(decision, "score"), 0.0) or 0.0,
            _get(decision, "reason"),
            _json_dumps(_get(decision, "hard_blockers", [])),
            _json_dumps(_get(decision, "soft_notes", [])),
            _json_dumps(_get(decision, "freshness_detail", {})),
            _json_dumps(_get(decision, "budget_detail", {})),
            _json_dumps(_get(decision, "risk_detail", {})),
            would_action,
            expected_rr,
            expected_upside_pct,
            defined_risk_pct,
            bottom_ticket_size_sol,
            _json_dumps(expected_rr_detail),
            _json_dumps(matrix_detail),
            _json_dumps(ai_review),
            _json_dumps(controller_action),
            denominator_key,
            _json_dumps(discovery_exit) if discovery_exit is not None else None,
            _json_dumps(principal_recovery_plan),
            _json_dumps(moonbag_plan),
            _json_dumps(candidate_dict),
            now_ts,
        ),
    )
    try:
        db.commit()
    except Exception:
        pass


def record_canonical_trade_entry(db, trade):
    init_canonical_ledger(db)
    now_ts = float(_get(trade, "created_at", None) or time.time())
    trade_id = str(_get(trade, "trade_id"))
    if not trade_id or trade_id == "None":
        raise ValueError("trade_id is required")
    entry_size_sol = _safe_float(_get(trade, "entry_size_sol"), None)
    db.execute(
        """
        INSERT INTO canonical_trade_ledger (
            trade_id, token_ca, symbol, lifecycle_id, route_bucket, entry_mode,
            normalized_mode, strategy_family, source_component, source_reason,
            entry_ts, entry_size_sol, entry_price, entry_quote_out,
            entry_quote_out_raw, entry_quote_source, entry_route_available,
            entry_quote_executable, entry_quote_age_sec, entry_spread_pct,
            entry_liquidity_usd, entry_market_cap, entry_data_confidence,
            trapped_flag, no_route_flag, stale_flag, outlier_flag, outlier_reason,
            is_a_class_fastlane, a_class_grade, a_class_score, a_class_size_rule,
            a_class_freshness_sources_json, a_class_hard_prefilter_json,
            a_class_budget_state_json, expected_rr, expected_upside_pct,
            defined_risk_pct, bottom_ticket_size_sol, principal_recovery_plan_json,
            moonbag_plan_json, a_class_matrix_json, ai_review_json,
            controller_action_json, security_flags_json, gmgn_policy_json,
            metadata_json, code_version, deploy_version, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_id) DO UPDATE SET
            updated_at=excluded.updated_at
        """
        ,
        (
            trade_id,
            _get(trade, "token_ca"),
            _get(trade, "symbol"),
            _get(trade, "lifecycle_id"),
            _get(trade, "route_bucket"),
            _get(trade, "entry_mode"),
            _get(trade, "normalized_mode"),
            _get(trade, "strategy_family"),
            _get(trade, "source_component"),
            _get(trade, "source_reason"),
            _safe_float(_get(trade, "entry_ts"), now_ts),
            entry_size_sol,
            _safe_float(_get(trade, "entry_price"), None),
            _safe_float(_get(trade, "entry_quote_out"), None),
            str(_get(trade, "entry_quote_out_raw", "")) if _get(trade, "entry_quote_out_raw", None) is not None else None,
            _get(trade, "entry_quote_source"),
            1 if _truthy(_get(trade, "entry_route_available", False)) else 0,
            1 if _truthy(_get(trade, "entry_quote_executable", False)) else 0,
            _safe_float(_get(trade, "entry_quote_age_sec"), None),
            _safe_float(_get(trade, "entry_spread_pct"), None),
            _safe_float(_get(trade, "entry_liquidity_usd"), None),
            _safe_float(_get(trade, "entry_market_cap"), None),
            _get(trade, "entry_data_confidence"),
            1 if _truthy(_get(trade, "trapped_flag", False)) else 0,
            1 if _truthy(_get(trade, "no_route_flag", False)) else 0,
            1 if _truthy(_get(trade, "stale_flag", False)) else 0,
            1 if _truthy(_get(trade, "outlier_flag", False)) else 0,
            _get(trade, "outlier_reason"),
            1 if _truthy(_get(trade, "is_a_class_fastlane", False)) else 0,
            _get(trade, "a_class_grade"),
            _safe_float(_get(trade, "a_class_score"), None),
            _get(trade, "a_class_size_rule"),
            _json_dumps(_get(trade, "a_class_freshness_sources", [])),
            _json_dumps(_get(trade, "a_class_hard_prefilter", {})),
            _json_dumps(_get(trade, "a_class_budget_state", {})),
            _safe_float(_get(trade, "expected_rr"), None),
            _safe_float(_get(trade, "expected_upside_pct"), None),
            _safe_float(_get(trade, "defined_risk_pct"), None),
            _safe_float(_get(trade, "bottom_ticket_size_sol"), None),
            _json_dumps(_get(trade, "principal_recovery_plan", {})),
            _json_dumps(_get(trade, "moonbag_plan", {})),
            _json_dumps(_get(trade, "a_class_matrix", {})),
            _json_dumps(_get(trade, "ai_review", {})),
            _json_dumps(_get(trade, "controller_action", {})),
            _json_dumps(_get(trade, "security_flags", [])),
            _json_dumps(_get(trade, "gmgn_policy", {})),
            _json_dumps(_get(trade, "metadata", {})),
            _get(trade, "code_version"),
            _get(trade, "deploy_version"),
            now_ts,
            now_ts,
        ),
    )
    try:
        db.commit()
    except Exception:
        pass


def record_canonical_trade_exit(db, trade_id, exit_data):
    init_canonical_ledger(db)
    now_ts = float(_get(exit_data, "updated_at", None) or time.time())
    row = db.execute(
        "SELECT entry_size_sol, entry_ts FROM canonical_trade_ledger WHERE trade_id = ?",
        (str(trade_id),),
    ).fetchone()
    if row is None:
        raise ValueError(f"trade_id not found: {trade_id}")
    entry_size_sol = _safe_float(row[0], 0.0) or 0.0
    realized_exit_sol = _safe_float(_get(exit_data, "realized_exit_sol"), None)
    if realized_exit_sol is None:
        realized_exit_sol = _safe_float(_get(exit_data, "exit_quote_out_sol"), None)
    realized_pnl_sol = None if realized_exit_sol is None else realized_exit_sol - entry_size_sol
    realized_pnl_pct = None
    if realized_pnl_sol is not None and entry_size_sol:
        realized_pnl_pct = realized_pnl_sol / entry_size_sol
    entry_ts = _safe_float(row[1], None)
    exit_ts = _safe_float(_get(exit_data, "exit_ts"), now_ts)
    time_held_sec = None if entry_ts is None or exit_ts is None else max(0.0, exit_ts - entry_ts)
    db.execute(
        """
        UPDATE canonical_trade_ledger
        SET exit_ts = ?, exit_price = ?, exit_quote_out_sol = ?, exit_quote_source = ?,
            exit_route_available = ?, exit_quote_executable = ?, exit_quote_age_sec = ?,
            exit_reason = ?, realized_exit_sol = ?, realized_pnl_sol = ?,
            realized_pnl_pct = ?, total_fees_sol = ?, slippage_bps = ?,
            accounting_source = ?, peak_quote_pnl_pct = ?, peak_quote_pnl_sol = ?,
            max_drawdown_pct = ?, time_to_peak_sec = ?, time_held_sec = ?,
            positive_feedback_seen = ?, first_positive_feedback_sec = ?,
            trapped_flag = ?, no_route_flag = ?, stale_flag = ?, outlier_flag = ?,
            outlier_reason = COALESCE(?, outlier_reason), updated_at = ?
        WHERE trade_id = ?
        """,
        (
            exit_ts,
            _safe_float(_get(exit_data, "exit_price"), None),
            _safe_float(_get(exit_data, "exit_quote_out_sol"), realized_exit_sol),
            _get(exit_data, "exit_quote_source"),
            1 if _truthy(_get(exit_data, "exit_route_available", False)) else 0,
            1 if _truthy(_get(exit_data, "exit_quote_executable", False)) else 0,
            _safe_float(_get(exit_data, "exit_quote_age_sec"), None),
            _get(exit_data, "exit_reason"),
            realized_exit_sol,
            realized_pnl_sol,
            realized_pnl_pct,
            _safe_float(_get(exit_data, "total_fees_sol"), 0.0) or 0.0,
            _safe_float(_get(exit_data, "slippage_bps"), None),
            _get(exit_data, "accounting_source", "sol_accounting"),
            _safe_float(_get(exit_data, "peak_quote_pnl_pct"), None),
            _safe_float(_get(exit_data, "peak_quote_pnl_sol"), None),
            _safe_float(_get(exit_data, "max_drawdown_pct"), None),
            _safe_float(_get(exit_data, "time_to_peak_sec"), None),
            time_held_sec,
            1 if _truthy(_get(exit_data, "positive_feedback_seen", False)) else 0,
            _safe_float(_get(exit_data, "first_positive_feedback_sec"), None),
            1 if _truthy(_get(exit_data, "trapped_flag", False)) else 0,
            1 if _truthy(_get(exit_data, "no_route_flag", False)) else 0,
            1 if _truthy(_get(exit_data, "stale_flag", False)) else 0,
            1 if _truthy(_get(exit_data, "outlier_flag", False)) else 0,
            _get(exit_data, "outlier_reason"),
            now_ts,
            str(trade_id),
        ),
    )
    try:
        db.commit()
    except Exception:
        pass


def record_canonical_trade_path_update(db, trade_id, path_data):
    """Update quote-trusted path metrics without closing the trade.

    The ledger remains SOL-accounting-primary.  Path values are evidence for
    convexity capture, DOA, and stop-before-peak analysis; they should never
    replace realized SOL PnL.
    """
    init_canonical_ledger(db)
    now_ts = float(_get(path_data, "updated_at", None) or time.time())
    row = db.execute(
        """
        SELECT entry_size_sol, entry_ts, peak_quote_pnl_pct, max_drawdown_pct,
               first_positive_feedback_sec
        FROM canonical_trade_ledger
        WHERE trade_id = ?
        """,
        (str(trade_id),),
    ).fetchone()
    if row is None:
        raise ValueError(f"trade_id not found: {trade_id}")

    entry_size_sol = _safe_float(row[0], 0.0) or 0.0
    entry_ts = _safe_float(row[1], None)
    previous_peak = _safe_float(row[2], None)
    previous_drawdown = _safe_float(row[3], None)
    previous_first_positive = _safe_float(row[4], None)

    observed_peak = _safe_float(_get(path_data, "peak_quote_pnl_pct"), None)
    current_quote_pnl = _safe_float(_get(path_data, "current_quote_pnl_pct"), None)
    if observed_peak is None:
        observed_peak = current_quote_pnl
    peak_candidates = [value for value in (previous_peak, observed_peak) if value is not None and value == value]
    new_peak = max(peak_candidates) if peak_candidates else None
    peak_sol = entry_size_sol * new_peak if new_peak is not None else None

    observed_drawdown = _safe_float(_get(path_data, "max_drawdown_pct"), None)
    if observed_drawdown is None and current_quote_pnl is not None:
        observed_drawdown = current_quote_pnl
    drawdown_candidates = [
        value for value in (previous_drawdown, observed_drawdown) if value is not None and value == value
    ]
    new_drawdown = min(drawdown_candidates) if drawdown_candidates else None

    positive_feedback_seen = _truthy(_get(path_data, "positive_feedback_seen", False))
    if new_peak is not None and new_peak > 0:
        positive_feedback_seen = True
    if current_quote_pnl is not None and current_quote_pnl > 0:
        positive_feedback_seen = True

    first_positive = previous_first_positive
    if positive_feedback_seen and first_positive is None and entry_ts is not None:
        first_positive = max(0.0, now_ts - entry_ts)

    time_to_peak = _safe_float(_get(path_data, "time_to_peak_sec"), None)
    if (
        time_to_peak is None
        and entry_ts is not None
        and observed_peak is not None
        and new_peak is not None
        and observed_peak >= new_peak
    ):
        time_to_peak = max(0.0, now_ts - entry_ts)

    db.execute(
        """
        UPDATE canonical_trade_ledger
        SET peak_quote_pnl_pct = COALESCE(?, peak_quote_pnl_pct),
            peak_quote_pnl_sol = COALESCE(?, peak_quote_pnl_sol),
            max_drawdown_pct = COALESCE(?, max_drawdown_pct),
            time_to_peak_sec = COALESCE(?, time_to_peak_sec),
            positive_feedback_seen = CASE
                WHEN ? THEN 1
                ELSE COALESCE(positive_feedback_seen, 0)
            END,
            first_positive_feedback_sec = COALESCE(first_positive_feedback_sec, ?),
            updated_at = ?
        WHERE trade_id = ?
        """,
        (
            new_peak,
            peak_sol,
            new_drawdown,
            time_to_peak,
            1 if positive_feedback_seen else 0,
            first_positive,
            now_ts,
            str(trade_id),
        ),
    )
    try:
        db.commit()
    except Exception:
        pass


def fetch_a_class_status(db, since_ts=None, limit=20):
    init_canonical_ledger(db)
    params = {}
    where = ""
    if since_ts is not None:
        where = "WHERE event_ts >= @since_ts"
        params["since_ts"] = float(since_ts)
    action_rows = db.execute(
        f"""
        SELECT action, COUNT(*) AS n, MAX(event_ts) AS latest_event_ts
        FROM a_class_decision_events
        {where}
        GROUP BY action
        ORDER BY n DESC
        """,
        params,
    ).fetchall()
    blocker_rows = db.execute(
        f"""
        SELECT hard_blockers_json
        FROM a_class_decision_events
        {where}
        """,
        params,
    ).fetchall()
    blockers = {}
    for row in blocker_rows:
        try:
            values = json.loads(row[0] or "[]")
        except Exception:
            values = []
        for blocker in values:
            blockers[blocker] = blockers.get(blocker, 0) + 1
    recent = fetch_a_class_events(db, since_ts=since_ts, limit=limit)
    return {
        "action_summary": [dict(row) if hasattr(row, "keys") else {"action": row[0], "n": row[1], "latest_event_ts": row[2]} for row in action_rows],
        "hard_blockers": sorted(blockers.items(), key=lambda item: item[1], reverse=True),
        "recent_events": recent,
    }


def fetch_a_class_events(db, since_ts=None, limit=50):
    init_canonical_ledger(db)
    params = {"limit": int(limit)}
    where = ""
    if since_ts is not None:
        where = "WHERE event_ts >= @since_ts"
        params["since_ts"] = float(since_ts)
    rows = db.execute(
        f"""
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, route_bucket,
               normalized_mode, source_table, source_id, source_component,
               source_reason, action, grade, size_sol, score, reason,
               hard_blockers_json, freshness_json, budget_json, risk_json,
               would_action, expected_rr, expected_upside_pct, defined_risk_pct,
               bottom_ticket_size_sol, expected_rr_detail_json, matrix_json,
               ai_review_json, controller_action_json, denominator_key,
               discovery_exit_json, principal_recovery_plan_json,
               moonbag_plan_json, source_dedup_key
        FROM a_class_decision_events
        {where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
        """,
        params,
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row) if hasattr(row, "keys") else {
            "id": row[0],
            "event_ts": row[1],
            "token_ca": row[2],
            "symbol": row[3],
            "lifecycle_id": row[4],
            "route_bucket": row[5],
            "normalized_mode": row[6],
            "source_table": row[7],
            "source_id": row[8],
            "source_component": row[9],
            "source_reason": row[10],
            "action": row[11],
            "grade": row[12],
            "size_sol": row[13],
            "score": row[14],
            "reason": row[15],
            "hard_blockers_json": row[16],
            "freshness_json": row[17],
            "budget_json": row[18],
            "risk_json": row[19],
            "would_action": row[20],
            "expected_rr": row[21],
            "expected_upside_pct": row[22],
            "defined_risk_pct": row[23],
            "bottom_ticket_size_sol": row[24],
            "expected_rr_detail_json": row[25],
            "matrix_json": row[26],
            "ai_review_json": row[27],
            "controller_action_json": row[28],
            "denominator_key": row[29],
            "discovery_exit_json": row[30],
            "principal_recovery_plan_json": row[31],
            "moonbag_plan_json": row[32],
            "source_dedup_key": row[33],
        }
        for key in (
            "hard_blockers_json",
            "freshness_json",
            "budget_json",
            "risk_json",
            "expected_rr_detail_json",
            "matrix_json",
            "ai_review_json",
            "controller_action_json",
            "discovery_exit_json",
            "principal_recovery_plan_json",
            "moonbag_plan_json",
        ):
            try:
                item[key.replace("_json", "")] = json.loads(item.get(key) or "{}")
            except Exception:
                item[key.replace("_json", "")] = [] if key == "hard_blockers_json" else {}
            item.pop(key, None)
        result.append(item)
    return result
