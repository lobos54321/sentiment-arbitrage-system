#!/usr/bin/env python3
"""Shared registry helpers for Strategy Memory Mining.

Everything in this module is research-only. Historical notes can seed
hypotheses, labels, and shadow reports, but they are never promotion evidence
and never modify entry, exit, gate, executor, wallet, or risk behavior.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import os
import re
import sqlite3
import tempfile
import time
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET


SCHEMA_VERSION = "strategy_memory_registry.v1"
EVIDENCE_LEVEL = "historical_notes_shadow_only"
DEFAULT_DATA_DIR = Path(os.environ.get("STRATEGY_MEMORY_DATA_DIR", "/app/data"))
DEFAULT_STRATEGY_DOCX = Path("/Users/lobos/Desktop/策略记录.docx")
GS_TIERS = {"gold", "silver"}
PROMOTION_ALLOWED = False
ALLOWED_USE = "shadow_only"

MC_BUCKETS = [
    ("15K-30K", 15_000, 30_000),
    ("30K-50K", 30_000, 50_000),
    ("50K-80K", 50_000, 80_000),
    ("80K-100K", 80_000, 100_000),
    ("100K-150K", 100_000, 150_000),
    ("150K-200K", 150_000, 200_000),
    ("200K-300K", 200_000, 300_000),
    ("300K+", 300_000, None),
]

FORBIDDEN_PRODUCTION_FILES = {
    "src/engines/premium-signal-engine.js",
    "src/execution/live-position-monitor.js",
    "scripts/final_entry_contract.py",
    "src/gates/hard-gates.js",
    "src/gates/exit-gates.js",
    "src/execution/gmgn-executor.js",
    "src/execution/jupiter-ultra-executor.js",
    "src/risk/risk-manager.js",
}

INDEX_KEYS = ("super", "ai", "trade", "security", "address", "viral", "media")


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def normalize_ts(value) -> int | None:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(ts) or ts <= 0:
        return None
    if ts > 1_000_000_000_000:
        ts /= 1000.0
    return int(ts)


def iso_from_ts(value) -> str | None:
    ts = normalize_ts(value)
    if ts is None:
        return None
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "pass", "enter", "would_enter"}


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def pct(num, den):
    return None if not den else round(float(num) / float(den) * 100.0, 4)


def jloads(raw, default=None):
    default = {} if default is None else default
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def load_json(path, default=None):
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def table_exists(db, table) -> bool:
    try:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
    except sqlite3.Error:
        return False


def columns(db, table) -> set[str]:
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def connect_sqlite(path):
    if not path or not Path(path).exists():
        return None
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def signal_id_key(value) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
        if math.isfinite(number) and number.is_integer():
            return str(int(number))
    except Exception:
        pass
    return text


def compact_number(raw):
    if raw is None:
        return None
    text = str(raw).strip().replace(",", "").replace("$", "")
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*([KMB])?", text, re.IGNORECASE)
    if not m:
        return None
    value = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    if suffix == "K":
        value *= 1_000
    elif suffix == "M":
        value *= 1_000_000
    elif suffix == "B":
        value *= 1_000_000_000
    return value


def market_cap_bucket(value) -> str:
    mc = safe_float(value)
    if mc is None or mc <= 0:
        return "UNKNOWN"
    for label, lo, hi in MC_BUCKETS:
        if mc >= lo and (hi is None or mc < hi):
            return label
    if mc < 15_000:
        return "LT15K"
    return "UNKNOWN"


def docx_text(path) -> str:
    """Extract text from a DOCX without requiring python-docx."""
    source = Path(path)
    if not source.exists():
        return ""
    with zipfile.ZipFile(source) as zf:
        xml = zf.read("word/document.xml")
    root = ET.fromstring(xml)
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    lines = []
    for para in root.findall(".//w:p", ns):
        parts = [node.text for node in para.findall(".//w:t", ns) if node.text]
        if parts:
            lines.append("".join(parts))
    return "\n".join(lines)


def load_source_text(source_docx=None, source_text=None) -> tuple[str, dict]:
    if source_text and Path(source_text).exists():
        text = Path(source_text).read_text(encoding="utf-8", errors="ignore")
        return text, {"source_type": "text", "source_path": str(source_text), "bytes": Path(source_text).stat().st_size}
    docx = Path(source_docx) if source_docx else DEFAULT_STRATEGY_DOCX
    text = docx_text(docx) if docx.exists() else ""
    return text, {"source_type": "docx", "source_path": str(docx), "bytes": docx.stat().st_size if docx.exists() else 0}


def text_presence(text: str, patterns) -> dict:
    haystack = text or ""
    matches = []
    for pattern in patterns:
        if re.search(pattern, haystack, re.IGNORECASE):
            matches.append(pattern)
    return {"found": bool(matches), "patterns": matches}


def excerpts_for(text: str, patterns, limit=3) -> list[str]:
    if not text:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    out = []
    for line in lines:
        if any(re.search(pattern, line, re.IGNORECASE) for pattern in patterns):
            cleaned = re.sub(r"\s+", " ", line)
            out.append(cleaned[:320])
            if len(out) >= limit:
                break
    return out


def hypothesis(
    hid,
    name,
    source_section,
    strategy_family,
    entry_definition,
    exit_definition,
    required_features,
    time_legal_features,
    future_or_posthoc_features,
    evidence_source,
    known_risks,
    next_validation_required,
    patterns,
    priority=50,
):
    return {
        "id": hid,
        "name": name,
        "source_section": source_section,
        "strategy_family": strategy_family,
        "entry_definition": entry_definition,
        "exit_definition": exit_definition,
        "required_features": required_features,
        "time_legal_features": time_legal_features,
        "future_or_posthoc_features": future_or_posthoc_features,
        "evidence_source": evidence_source,
        "known_risks": known_risks,
        "allowed_use": ALLOWED_USE,
        "promotion_allowed": PROMOTION_ALLOWED,
        "next_validation_required": next_validation_required,
        "source_patterns": patterns,
        "priority": priority,
    }


def seed_hypotheses(include_x_placeholder=True) -> list[dict]:
    seeds = [
        hypothesis(
            "SM-ATH1-EARLY-SCOUT-V17D4",
            "ATH#1 early scout with delta confirmation",
            "V17.3/V17.4 ATH#1 delta notes",
            "ATH1 early scout",
            "ATH#1, MC 20K-75K, Super(signal)>=80, SuperDelta>=15, TradeDelta>=1.",
            "Shadow only: TP100 fast-exit variants and no live exit change.",
            ["ath_stage", "market_cap", "super_index_signal", "super_delta", "trade_delta"],
            ["ath_stage_at_signal", "mc_at_signal", "index_delta_available_before_decision"],
            ["future_ath_count", "max_peak_pct", "held_to_tp_label"],
            "historical_strategy_doc: V17.4 backtest and live failure notes",
            ["ATH#1 is execution-delay sensitive", "MC peak estimates overstate real kline fills", "same-window PnL is not promotion evidence"],
            "Run execution-delay-adjusted replay at 0/5/10/20/30/60s before any candidate proposal.",
            [r"V17\.4", r"ATH#1", r"Sup.?Δ", r"T.?Δ", r"延迟"],
            88,
        ),
        hypothesis(
            "SM-ATH2-CONFIRMATION-V18-MC50-100",
            "ATH#2 confirmation with MC bucket and Security Delta",
            "V18 ATH#2 delta/MC bucket notes",
            "ATH2 confirmation",
            "ATH#2, TradeDelta>=2, MC in 50K-100K or 50K-150K, SecurityDelta<8/10.",
            "Shadow only: compare TP75/100/150/200 with SL25/30; PnL secondary.",
            ["ath_stage", "trade_delta", "security_delta", "market_cap_bucket"],
            ["ath_stage_at_signal", "index_deltas_before_decision", "mc_at_signal_or_quote"],
            ["top50_label", "max_ath", "future_peak_mc"],
            "historical_strategy_doc: V18 sensitivity and MC sweet-zone analysis",
            ["20K-50K low-MC bucket was described as noisy", "needs real kline/quote validation", "depends on clean delta extraction"],
            "Evaluate by MC bucket and delay; reject future Top50/max_ath features as labels only.",
            [r"V18", r"ATH#2", r"T.?Δ", r"Security Delta", r"MC ?50"],
            90,
        ),
        hypothesis(
            "SM-ATH3-CONTINUATION-SEC20-ADDR15",
            "ATH#3 continuation with low Security and Address confirmation",
            "ATH#3 continuation / v14.9C notes",
            "ATH3 continuation",
            "ATH#3, Security<=20, Address>=15, cumulative MCG>=1.5/2.0 when time-legal.",
            "Shadow only: DynSL 15/35/55 and DynSL 20/40/60 plus TP/SL comparisons.",
            ["ath_stage", "security_index", "address_index", "cumulative_mc_gain"],
            ["ath_stage_at_signal", "security_current_at_signal", "address_current_at_signal", "mc_gain_from_prior_ath_if_observed"],
            ["future_ath_count", "future_peak", "posthoc_winner_bucket"],
            "historical_strategy_doc: ATH#3 Sec<=20 + Addr>=15 scans",
            ["sample sizes were small in several runs", "ATH#3/4 conclusions changed across windows", "MC comparison can overstate kline exit"],
            "Use index lifecycle snapshot plus real kline replay; keep as shadow-only until repeated OOS windows agree.",
            [r"ATH#3", r"Sec.?≤?20", r"Addr.?≥?15", r"DynSL"],
            86,
        ),
        hypothesis(
            "SM-ATH4-LATE-CONTINUATION-AUDIT",
            "ATH#4+ late continuation lifecycle audit",
            "ATH#4/5 late continuation notes",
            "ATH4+ late continuation",
            "ATH#4+ late continuation; compare cumulative MCG and maturity rather than one-size thresholds.",
            "Shadow only: no entry; measure if late ATH stages are posthoc labels or viable time-legal contexts.",
            ["ath_stage", "cumulative_mc_gain", "trade_index", "security_index", "address_index"],
            ["ath_stage_at_signal", "prior_ath_snapshots_available_before_decision"],
            ["knowing_later_ath_count", "max_ath", "matured_snapshot_after_entry"],
            "historical_strategy_doc: conflicting ATH#4 findings and late continuation cautions",
            ["high future-data risk", "late entries may be near exhaustion", "matured snapshots must not leak into entry"],
            "Classify ATH#4+ features into time-legal vs posthoc labels before any candidate conversion.",
            [r"ATH#4", r"ATH#5", r"late", r"后续"],
            60,
        ),
        hypothesis(
            "SM-SUPER200-HIGH-CONSENSUS",
            "Super200 high consensus continuation",
            "S13 Super200 / high consensus notes",
            "Super200 / high consensus",
            "ATH#3+ with Super(current)>=200 or stronger high-consensus bucket.",
            "Shadow only: TP75/SL20, TP75/SL25, DynSL grids.",
            ["ath_stage", "super_index", "market_cap", "trade_index"],
            ["super_index_available_at_signal", "ath_stage_at_signal"],
            ["future_peak_mc", "future_best_exit"],
            "historical_strategy_doc: S13_Super200 and Super 200+ analysis",
            ["Super can be a composite of AI/Trade and may duplicate features", "same-window discovery is not promotion evidence"],
            "Map to current candidate context and 2D slices; require OOS kline evidence.",
            [r"Super ?200", r"Super>=200", r"S13_Super200", r"high consensus"],
            84,
        ),
        hypothesis(
            "SM-INDEX-DELTA-COMPOSITE",
            "Index delta composite context",
            "Super/AI/Trade/Security/Address/Viral/Media delta notes",
            "Index delta strategies",
            "Use SuperDelta, TradeDelta, AddressDelta, ViralDelta, AIDelta, SecurityDelta as 2D context, not production rules.",
            "Not an exit policy; pair with existing shadow candidate virtual trades.",
            ["super_delta", "ai_delta", "trade_delta", "security_delta", "address_delta", "viral_delta", "media_delta"],
            ["delta_computed_between_signal_and_decision_or_prior_snapshot"],
            ["delta_to_peak", "delta_to_matured_snapshot", "winner_after_the_fact"],
            "historical_strategy_doc: index lifecycle and delta analysis",
            ["delta windows can be posthoc", "different indexes mature at different ATH stages", "missing snapshot timing creates bias"],
            "Emit index lifecycle snapshots with feature_available_at_ts and time_legal flags.",
            [r"Super.?Δ", r"T.?Δ", r"Addr.?Δ", r"Viral.?Δ", r"Index"],
            82,
        ),
        hypothesis(
            "SM-MC-BUCKET-LIFECYCLE",
            "MC bucket lifecycle sweet-zone audit",
            "MC bucket / black-hole zone notes",
            "MC bucket strategies",
            "Evaluate MC buckets 15K-30K, 30K-50K, 50K-80K, 80K-100K, 100K-150K, 150K-200K, 200K-300K, 300K+ by ATH stage.",
            "Not an exit policy; bucket is candidate context only.",
            ["market_cap", "ath_stage", "signal_type"],
            ["mc_at_signal_or_executable_quote"],
            ["future_peak_mc", "current_mc_after_signal"],
            "historical_strategy_doc: MC sweet-zone and skipped winner analysis",
            ["MC parser bugs were documented", "signal MC and executable quote MC must be separated", "bucket conclusions differed by phase"],
            "Produce per-bucket recall/precision and mark MC peak estimates separately from real kline/quote.",
            [r"MC", r"50K", r"80K", r"甜点", r"黑洞"],
            80,
        ),
        hypothesis(
            "SM-FILTERED-WINNER-RELAXATION",
            "Filtered winner relaxation dossier",
            "SKIP/filtered winner notes",
            "Filtered winner relaxation strategies",
            "Find tokens filtered out but later gold/silver or 100%+; classify top blocker and downstream dropoff.",
            "Not an exit policy; dossier only.",
            ["raw_tier", "candidate_matches", "decision_status", "pending_status", "final_entry_status"],
            ["filter_reason_at_decision", "candidate_match_at_signal"],
            ["later_peak", "later_gold_silver_label"],
            "historical_strategy_doc: MC>=50K and SKIP wrong-kill analysis",
            ["relaxation can increase noise", "missed winners are labels not entry rules", "needs denominator discipline"],
            "Generate filtered_winner_dossier_24h/72h and compare blockers against current 84-candidate mesh.",
            [r"SKIP", r"错杀", r"filtered", r"MC>=50K"],
            92,
        ),
        hypothesis(
            "SM-EXIT-V4-BREAKEVEN-AFTER-TP1",
            "V4 breakeven after TP1",
            "V4 vs V5 exit notes",
            "Exit policy variants",
            "Entry unchanged; after TP1, move remaining stop to breakeven in shadow simulation only.",
            "V4: TP1 then breakeven/0% stop on remainder.",
            ["entry_price", "peak_pct", "time_path_or_kline"],
            ["post_entry_kline_only_for_exit_simulation"],
            ["best_exit_after_peak"],
            "historical_strategy_doc: V4 beat V5 in later exit scan",
            ["exit-only simulation must not alter live-position-monitor", "peak-only proxy can overstate capture"],
            "Run shadow simulator on real kline when available; keep live monitor untouched.",
            [r"V4", r"保本", r"breakeven", r"TP1"],
            76,
        ),
        hypothesis(
            "SM-EXIT-V5-MULTI-TP",
            "V5 multi-TP with original hard stop",
            "V5 multi-TP notes",
            "Exit policy variants",
            "Entry unchanged; simulate TP1 60 sell50, TP2 100 sell20, TP3 150 sell10, TP4 200 sell10, TP5 500 sell10 with SL -30.",
            "V5 multi-TP shadow simulator only.",
            ["entry_price", "peak_pct", "time_path_or_kline"],
            ["post_entry_kline_only_for_exit_simulation"],
            ["peak_capture_after_full_path"],
            "historical_strategy_doc: V5 120h backtest and V4/V5/Hybrid comparison",
            ["V5 later lost to V4 in expanded scan", "multi-TP needs path ordering not just peak"],
            "Compare against V4/Hybrid with same entries and real kline order.",
            [r"V5", r"TP1", r"TP2", r"TP5", r"永保"],
            72,
        ),
        hypothesis(
            "SM-EXIT-DYNSL-GRID",
            "DynSL grid variants",
            "DynSL 15/35/55 and 20/40/60 notes",
            "Exit policy variants",
            "Entry unchanged; simulate DynSL 15/35/55, DynSL 20/40/60, no trailing, and time-stop variants.",
            "Shadow exit simulator only.",
            ["entry_price", "peak_pct", "time_path_or_kline"],
            ["post_entry_kline_only_for_exit_simulation"],
            ["future_best_floor"],
            "historical_strategy_doc: v14.9/v17 exit scans",
            ["trail can damage DynSL breakeven behavior", "needs path not just peak", "live-position-monitor must remain unchanged"],
            "Emit variant metrics; do not recommend live exit changes from same-window data.",
            [r"DynSL", r"15/35/55", r"20/40/60", r"no trailing"],
            74,
        ),
        hypothesis(
            "SM-EXECUTION-DELAY-SENSITIVITY",
            "Execution delay adjusted replay",
            "v17 execution delay failure notes",
            "Execution delay sensitivity tests",
            "Replay each historical entry definition with entry_delay in 0,5,10,20,30,60 seconds.",
            "Exit policy paired per hypothesis; PnL secondary only.",
            ["signal_ts", "decision_ts", "quote_ts", "kline_or_quote_path"],
            ["entry_delay_applied_after_signal", "price_available_at_delayed_ts"],
            ["instant_entry_fill", "future_best_price"],
            "historical_strategy_doc: Telegram parse/quote/signing/balance delay problem",
            ["0s-only strategies are not executable", "ATH#1 is most delay sensitive", "quote/kline coverage can block replay"],
            "Reject or keep posthoc-only any strategy that only works at 0s.",
            [r"延迟", r"delay", r"10-30秒", r"Telegram", r"quote"],
            94,
        ),
    ]
    if include_x_placeholder:
        seeds.append(
            hypothesis(
                "SM-X-NARRATIVE-PLACEHOLDER",
                "X narrative variant placeholder",
                "X narrative shadow context placeholder",
                "X narrative variants placeholder",
                "Join historical families with x_narrative_stage, mention velocity, KOL score, rug warning, and source quality only if x_narrative_context exists.",
                "Not an exit policy.",
                ["x_narrative_stage", "x_mention_velocity", "x_kol_score", "x_rug_warning", "x_source_quality_bucket"],
                ["x_context_query_end_lte_decision_ts", "is_time_legal_for_decision"],
                ["post_decision_social_mentions", "viral_after_peak"],
                "placeholder: x_narrative_context_24h.json if produced by X observer",
                ["X data must be read-only", "ambiguous tickers create false positives", "posthoc social activity must be excluded"],
                "Join only after X observer emits time-legal coverage with promotion_allowed=false.",
                [r"X narrative", r"Twitter", r"KOL", r"叙事"],
                52,
            )
        )
    return seeds


def build_hypothesis_registry(source_text: str, source_meta: dict, include_x_placeholder=True) -> dict:
    hypotheses = []
    for seed in seed_hypotheses(include_x_placeholder=include_x_placeholder):
        patterns = seed.pop("source_patterns")
        presence = text_presence(source_text, patterns)
        seed["source_presence"] = presence
        seed["source_excerpts"] = excerpts_for(source_text, patterns)
        seed["historical_pnl_is_promotion_evidence"] = False
        seed["same_window_discovery_is_promotion_evidence"] = False
        seed["machine_action_allowed"] = "shadow_report_only"
        seed["promotion_allowed"] = False
        hypotheses.append(seed)
    rejected_future = [
        h for h in hypotheses
        if any("future" in str(item).lower() or "max_ath" in str(item).lower() or "peak" in str(item).lower() for item in h["future_or_posthoc_features"])
    ]
    return {
        "schema_version": "strategy_memory_hypotheses.v1",
        "registry_schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "phase": "discovery_readiness_shadow_only",
        "evidence_level": EVIDENCE_LEVEL,
        "source": source_meta,
        "hypotheses_count": len(hypotheses),
        "rejected_future_data_hypotheses_count": len(rejected_future),
        "historical_pnl_is_promotion_evidence": False,
        "same_window_discovery_is_promotion_evidence": False,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "allowed_use": ALLOWED_USE,
        "mc_buckets": [{"label": label, "min": lo, "max": hi} for label, lo, hi in MC_BUCKETS],
        "hypotheses": hypotheses,
    }


def load_hypotheses(path=None, source_docx=None, source_text=None) -> dict:
    if path and Path(path).exists():
        return load_json(path, {})
    text, meta = load_source_text(source_docx=source_docx, source_text=source_text)
    return build_hypothesis_registry(text, meta)


def raw_tier(row: dict) -> str | None:
    for key in ("raw_sustained_tier", "raw_primary_tier", "tier", "dog_tier"):
        value = str(row.get(key) or "").lower()
        if value:
            return value
    return None


def is_gold_silver(row: dict) -> bool:
    return raw_tier(row) in GS_TIERS


def optional_select(cols, name, fallback="NULL"):
    return name if name in cols else f"{fallback} AS {name}"


def load_raw_signal_outcomes(raw_db_path, hours=24, now_ts=None, limit=5000, gold_silver_only=False) -> tuple[list[dict], dict]:
    meta = {"db_path": raw_db_path, "available": False, "table": "raw_signal_outcomes", "missing_reason": None}
    db = connect_sqlite(raw_db_path)
    if not db:
        meta["missing_reason"] = "raw_db_missing"
        return [], meta
    try:
        if not table_exists(db, "raw_signal_outcomes"):
            meta["missing_reason"] = "raw_signal_outcomes_table_missing"
            return [], meta
        cols = columns(db, "raw_signal_outcomes")
        wanted = (
            "signal_id", "token_ca", "symbol", "signal_ts", "signal_type", "source",
            "raw_sustained_tier", "raw_primary_tier", "max_sustained_peak_pct",
            "max_wick_peak_pct", "time_to_sustained_peak_sec", "did_enter",
            "raw_dog_entered", "raw_dog_realized", "held_to_silver", "held_to_gold",
            "observation_status", "kline_covered", "coverage_reason",
            "payload_json", "source_kind", "source_family",
        )
        select = [optional_select(cols, name) for name in wanted]
        now_ts = int(now_ts or time.time())
        since_ts = int(now_ts - float(hours) * 3600)
        filters = ["COALESCE(signal_ts, 0) >= ?"]
        params = [since_ts]
        if gold_silver_only:
            tier_exprs = []
            if "raw_sustained_tier" in cols:
                tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
            if "raw_primary_tier" in cols:
                tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
            if tier_exprs:
                filters.append(f"({' OR '.join(tier_exprs)})")
        rows = db.execute(
            f"""
            SELECT {", ".join(select)}
            FROM raw_signal_outcomes
            WHERE {' AND '.join(filters)}
            ORDER BY signal_ts DESC, signal_id DESC
            LIMIT ?
            """,
            [*params, int(limit)],
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["signal_id_key"] = signal_id_key(item.get("signal_id"))
            item["signal_ts_norm"] = normalize_ts(item.get("signal_ts"))
            item["signal_ts_iso"] = iso_from_ts(item.get("signal_ts"))
            item["tier"] = raw_tier(item)
            item["payload"] = jloads(item.get("payload_json"))
            item["peak_pct"] = safe_float(item.get("max_sustained_peak_pct"), safe_float(item.get("max_wick_peak_pct")))
            out.append(item)
        meta.update({"available": True, "rows": len(out), "since_ts": since_ts, "until_ts": now_ts})
        return out, meta
    finally:
        db.close()


def load_candidate_observations(paper_db_path, signal_ids=None, hours=24, now_ts=None, limit=200000) -> tuple[list[dict], dict]:
    meta = {"db_path": paper_db_path, "available": False, "table": "candidate_shadow_observations", "missing_reason": None}
    db = connect_sqlite(paper_db_path)
    if not db:
        meta["missing_reason"] = "paper_db_missing"
        return [], meta
    try:
        if not table_exists(db, "candidate_shadow_observations"):
            meta["missing_reason"] = "candidate_shadow_observations_table_missing"
            return [], meta
        now_ts = int(now_ts or time.time())
        since_ts = int(now_ts - float(hours) * 3600)
        filters = ["observed_at >= ?"]
        params = [since_ts]
        signal_ids = [sid for sid in (signal_ids or []) if sid is not None]
        if signal_ids:
            placeholders = ",".join("?" for _ in signal_ids[:900])
            filters.append(f"CAST(signal_id AS TEXT) IN ({placeholders})")
            params.extend([str(sid) for sid in signal_ids[:900]])
        rows = db.execute(
            f"""
            SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json
            FROM candidate_shadow_observations
            WHERE {' AND '.join(filters)}
            ORDER BY observed_at DESC
            LIMIT ?
            """,
            [*params, int(limit)],
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["signal_id_key"] = signal_id_key(item.get("signal_id"))
            item["matched_bool"] = truthy(item.get("matched"))
            item["payload"] = jloads(item.get("payload_json"))
            out.append(item)
        meta.update({"available": True, "rows": len(out), "since_ts": since_ts, "until_ts": now_ts})
        return out, meta
    finally:
        db.close()


def group_candidate_matches(observations: list[dict]) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for row in observations:
        if row.get("matched_bool"):
            grouped[row.get("signal_id_key")].append(row)
    return dict(grouped)


def extract_index_snapshot(payload: dict) -> dict:
    payload = payload or {}
    aliases = {
        "super": ("super_index", "super", "super_current", "si"),
        "ai": ("ai_index", "ai_confidence", "ai_current"),
        "trade": ("trade_index", "trade_current"),
        "security": ("security_index", "security_current"),
        "address": ("address_index", "address_current", "addr_index", "addr_current"),
        "viral": ("viral_index", "viral_current"),
        "media": ("media_index", "media_current"),
    }
    out = {}
    for key, names in aliases.items():
        value = None
        for name in names:
            if payload.get(name) not in (None, ""):
                value = safe_float(payload.get(name))
                break
        out[f"{key}_index"] = value
        sig = safe_float(payload.get(f"{key}_signal"), safe_float(payload.get(f"{key}_index_signal")))
        out[f"{key}_signal"] = sig
        out[f"{key}_delta"] = None if value is None or sig is None else round(value - sig, 6)
    return out


def row_market_cap(row: dict) -> float | None:
    payload = row.get("payload") or {}
    for key in ("market_cap", "market_cap_usd", "mc", "signal_market_cap", "current_market_cap"):
        value = row.get(key, None)
        if value in (None, ""):
            value = payload.get(key)
        parsed = safe_float(value)
        if parsed is not None:
            return parsed
    text = json.dumps(payload, ensure_ascii=False)
    m = re.search(r"(?:MC|MarketCap)[^\d$-]*\$?\s*(\d+(?:\.\d+)?\s*[KMB]?)", text, re.IGNORECASE)
    return compact_number(m.group(1)) if m else None


def ath_stage_from(row: dict, payload=None) -> str:
    payload = payload or row.get("payload") or {}
    for key in ("ath_stage", "ath_count", "ath_index", "ath_number"):
        value = payload.get(key, row.get(key))
        if value not in (None, ""):
            try:
                n = int(float(value))
                return f"ATH#{n}" if n > 0 else "UNKNOWN"
            except Exception:
                text = str(value)
                return text if text else "UNKNOWN"
    signal_type = str(row.get("signal_type") or payload.get("signal_type") or "").upper()
    if "ATH" in signal_type:
        return "ATH_UNKNOWN"
    if "NEW" in signal_type:
        return "NEW_TRENDING"
    return "UNKNOWN"


def load_x_context(path) -> dict:
    data = load_json(path, {}) if path else {}
    records = data.get("records") if isinstance(data, dict) else []
    by_signal = {}
    for row in records or []:
        key = signal_id_key(row.get("signal_id") or row.get("signal_id_key"))
        if key:
            by_signal[key] = row
    return {
        "available": bool(records),
        "path": str(path) if path else None,
        "records": records or [],
        "by_signal": by_signal,
        "promotion_allowed": False,
    }


def stage_sets_from_paper(paper_db_path, signal_ids, hours=24, now_ts=None) -> tuple[dict, dict]:
    sets = {
        "decision": set(),
        "pass_allow": set(),
        "pending": set(),
        "final_entry": set(),
        "paper": set(),
        "realized": set(),
    }
    blockers = defaultdict(Counter)
    meta = {"available": False, "tables_scanned": []}
    db = connect_sqlite(paper_db_path)
    if not db:
        meta["missing_reason"] = "paper_db_missing"
        return sets, meta
    try:
        ids = {str(sid) for sid in signal_ids if sid is not None}
        now_ts = int(now_ts or time.time())
        since_ts = int(now_ts - float(hours) * 3600) - 3600
        for table in ("a_class_decision_events", "paper_decision_events", "entry_decision_events", "entry_decisions"):
            if not table_exists(db, table):
                continue
            cols = columns(db, table)
            sig_col = next((c for c in ("signal_id", "source_signal_id", "premium_signal_id") if c in cols), None)
            ts_col = next((c for c in ("created_at", "observed_at", "decision_ts", "signal_ts") if c in cols), None)
            if not sig_col:
                continue
            meta["tables_scanned"].append(table)
            filters = []
            params = []
            if ts_col:
                filters.append(f"COALESCE({ts_col}, 0) >= ?")
                params.append(since_ts)
            rows = db.execute(
                f"SELECT * FROM {table}" + (f" WHERE {' AND '.join(filters)}" if filters else "") + " LIMIT 200000",
                params,
            ).fetchall()
            for row in rows:
                item = dict(row)
                sid = signal_id_key(item.get(sig_col))
                if ids and sid not in ids:
                    continue
                if not sid:
                    continue
                sets["decision"].add(sid)
                payload = jloads(item.get("payload_json"), {})
                decision_text = " ".join(str(item.get(k) or "") for k in ("decision", "status", "verdict", "action", "reason"))
                if re.search(r"pass|allow|enter|buy|would", decision_text, re.IGNORECASE):
                    sets["pass_allow"].add(sid)
                if re.search(r"pending|enqueue|final", decision_text, re.IGNORECASE) or payload.get("pending_entry"):
                    sets["pending"].add(sid)
                if "final" in table or payload.get("final_entry_contract") or payload.get("reached_final_entry_contract"):
                    sets["final_entry"].add(sid)
                hard_blockers = payload.get("hard_blockers")
                if hard_blockers:
                    if isinstance(hard_blockers, str):
                        hard_blockers = [hard_blockers]
                    for blocker in hard_blockers:
                        blockers[sid][str(blocker)] += 1
        for table in ("paper_trades", "trade_intents", "paper_trade_intents"):
            if not table_exists(db, table):
                continue
            cols = columns(db, table)
            sig_col = next((c for c in ("signal_id", "source_signal_id", "premium_signal_id") if c in cols), None)
            if not sig_col:
                continue
            meta["tables_scanned"].append(table)
            for row in db.execute(f"SELECT * FROM {table} LIMIT 200000").fetchall():
                item = dict(row)
                sid = signal_id_key(item.get(sig_col))
                if ids and sid not in ids:
                    continue
                if sid:
                    sets["paper"].add(sid)
                    if truthy(item.get("closed")) or item.get("exit_ts") or item.get("realized_pnl") not in (None, ""):
                        sets["realized"].add(sid)
        meta["available"] = True
        meta["blockers_by_signal"] = {sid: dict(counter) for sid, counter in blockers.items()}
        return sets, meta
    finally:
        db.close()


def candidate_catalog_from_code(registry_path="config/entry-mode-registry.json") -> tuple[list[dict], dict]:
    meta = {"source": "fallback", "expected_candidates": 84, "error": None}
    try:
        import sys
        root = Path(__file__).resolve().parent
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        import candidate_shadow_observer as cso

        registry_modes = {}
        if registry_path and Path(registry_path).exists():
            _, registry_modes = cso.load_registry(registry_path)
        catalog = cso.build_candidate_catalog(registry_modes)
        meta.update({"source": "candidate_shadow_observer", "count": len(catalog)})
        return catalog, meta
    except Exception as exc:
        meta["error"] = str(exc)
        base = [
            "current_all", "notath_mc_lt_30k", "notath_mc_5k_30k",
            "old_filter:mc_si_ai_velocity_utc_pre3m", "lifecycle:stage1_notath_selective_v1",
            "historical:smart_backtest_76wr", "historical:walk_forward_68wr",
            "historical:peak_exit_72wr", "historical:take_profit_30_72wr",
            "historical:take_profit_50_72wr", "historical:strategy_c_exit_shape",
            "kline:first_bar_return_filters", "kline:volume_profile", "kline:candle_pattern",
            "markov_yellow_or_green",
        ]
        return [{"candidate_id": cid, "family": cid.split(":", 1)[0] if ":" in cid else "base"} for cid in base], meta


def historical_match_to_candidates(hypothesis_row: dict, catalog: list[dict]) -> dict:
    text = " ".join(
        str(hypothesis_row.get(k) or "")
        for k in ("id", "name", "strategy_family", "entry_definition", "exit_definition")
    ).lower()
    ids = [row["candidate_id"] for row in catalog]
    matches = []
    blockers = []
    if "mc" in text or "market" in text:
        matches.extend([cid for cid in ids if "mc" in cid or "market" in cid])
    if "super" in text or "index" in text:
        matches.extend([cid for cid in ids if "old_filter" in cid or "historical" in cid or "lifecycle:stage1" in cid])
    if "kline" in text or "dynsl" in text or "tp" in text or "exit" in text:
        matches.extend([cid for cid in ids if cid.startswith("kline:") or cid.startswith("historical:")])
        blockers.append("kline_or_exit_path_required")
    if "ath" in text:
        matches.extend([cid for cid in ids if "lifecycle" in cid or "historical" in cid])
    if "x narrative" in text or "x_" in text:
        blockers.append("x_narrative_context_required")
    if "delay" in text or "延迟" in text:
        blockers.extend(["quote_path_required", "execution_delay_replay_required"])
    if "future" in " ".join(map(str, hypothesis_row.get("future_or_posthoc_features") or [])).lower():
        blockers.append("future_data_features_must_be_labels_only")
    unique = []
    for cid in matches:
        if cid not in unique:
            unique.append(cid)
    status = "missing_shadow_candidate"
    if unique:
        status = "partial_existing_context"
    if hypothesis_row.get("strategy_family") == "Exit policy variants":
        status = "missing_exit_shadow_sim_only"
    if hypothesis_row.get("strategy_family") == "Execution delay sensitivity tests":
        status = "missing_delay_replay_only"
    return {
        "hypothesis_id": hypothesis_row["id"],
        "strategy_family": hypothesis_row["strategy_family"],
        "mapping_status": status,
        "existing_candidate_ids": unique[:12],
        "could_become_shadow_only_candidate": status.startswith("missing") or bool(unique),
        "blocked_contexts": sorted(set(blockers)),
        "requires_future_data_conversion": bool(
            [f for f in hypothesis_row.get("future_or_posthoc_features") or [] if re.search(r"future|max_ath|peak|top50", str(f), re.IGNORECASE)]
        ),
        "allowed_use": "shadow_only",
        "promotion_allowed": False,
    }


def classify_final_blocker(raw_row, candidate_matches, stage_sets, stage_meta, x_available=False) -> str:
    sid = raw_row.get("signal_id_key")
    if not candidate_matches:
        return "no_candidate_match"
    if sid not in stage_sets.get("decision", set()):
        return "candidate_matched_no_decision"
    if sid not in stage_sets.get("pass_allow", set()):
        blockers = (stage_meta.get("blockers_by_signal") or {}).get(sid) or {}
        return next(iter(blockers), "decision_not_pass_allow")
    if sid not in stage_sets.get("pending", set()):
        return "decision_pass_no_pending"
    if sid not in stage_sets.get("final_entry", set()):
        return "pending_no_final_entry"
    if sid not in stage_sets.get("paper", set()):
        return "final_entry_no_paper_trade"
    return "paper_or_realized_seen"


def summarize_counts(rows, key):
    return dict(Counter(str(row.get(key) or "UNKNOWN") for row in rows))


def simulation_peak_pct(row: dict) -> float | None:
    for key in ("peak_pct", "max_sustained_peak_pct", "max_wick_peak_pct"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    payload = row.get("payload") or {}
    for key in ("peak_pct", "max_sustained_peak_pct", "max_wick_peak_pct", "max_peak_pct"):
        value = safe_float(payload.get(key))
        if value is not None:
            return value
    return None


def simulate_exit_variant(peak_pct, variant):
    if peak_pct is None:
        return {"status": "missing_peak", "net_pnl_pct": None, "capture_pct": None}
    peak = float(peak_pct)
    name = variant["id"]
    sl = variant.get("sl_pct", -25.0)
    if peak <= 0:
        return {"status": "stop", "net_pnl_pct": sl, "capture_pct": None}
    if name == "no_trailing":
        return {"status": "timeout_proxy", "net_pnl_pct": round(min(peak, 25.0), 4), "capture_pct": pct(min(peak, 25.0), peak)}
    if name.startswith("tp"):
        tp = variant.get("tp_pct", 75.0)
        return {
            "status": "tp" if peak >= tp else "stop_or_timeout_proxy",
            "net_pnl_pct": tp if peak >= tp else sl,
            "capture_pct": pct(tp, peak) if peak >= tp else None,
        }
    if name == "v5_multi_tp":
        schedule = [(60, 0.5), (100, 0.2), (150, 0.1), (200, 0.1), (500, 0.1)]
        realized = 0.0
        sold = 0.0
        for threshold, weight in schedule:
            if peak >= threshold:
                realized += threshold * weight
                sold += weight
        if sold < 1.0:
            realized += (1.0 - sold) * (sl if peak < 60 else max(sl, min(peak, 60)))
        return {"status": "multi_tp_proxy", "net_pnl_pct": round(realized, 4), "capture_pct": pct(realized, peak)}
    if name == "v4_breakeven_after_tp1":
        if peak >= 50:
            realized = 50 * 0.8
            return {"status": "tp1_breakeven_proxy", "net_pnl_pct": round(realized, 4), "capture_pct": pct(realized, peak)}
        return {"status": "stop", "net_pnl_pct": variant.get("sl_pct", -50.0), "capture_pct": None}
    if name == "hybrid_tp1_floor":
        if peak >= 50:
            realized = 50 * 0.8 + 0.2 * -15
            return {"status": "tp1_floor_proxy", "net_pnl_pct": round(realized, 4), "capture_pct": pct(realized, peak)}
        return {"status": "stop", "net_pnl_pct": variant.get("sl_pct", -50.0), "capture_pct": None}
    if name.startswith("dynsl"):
        floors = variant.get("floors") or [(20, 0), (40, 15), (60, 30)]
        floor = sl
        for threshold, value in floors:
            if peak >= threshold:
                floor = value
        return {"status": "dynsl_floor_proxy", "net_pnl_pct": round(floor, 4), "capture_pct": pct(floor, peak) if floor > 0 else None}
    if name.startswith("time_stop"):
        cap = variant.get("cap_pct", 30.0)
        value = min(peak, cap) if peak > 0 else sl
        return {"status": "time_stop_proxy", "net_pnl_pct": round(value, 4), "capture_pct": pct(value, peak)}
    return {"status": "unknown_variant", "net_pnl_pct": None, "capture_pct": None}


EXIT_VARIANTS = [
    {"id": "v4_breakeven_after_tp1", "name": "V4 TP1 breakeven", "sl_pct": -50.0},
    {"id": "v5_multi_tp", "name": "V5 multi-TP", "sl_pct": -30.0},
    {"id": "hybrid_tp1_floor", "name": "Hybrid TP1 floor", "sl_pct": -50.0},
    {"id": "tp60_sell50_protect", "name": "TP60 sell50 protect", "tp_pct": 60.0, "sl_pct": -25.0},
    {"id": "tp75_sl25", "name": "TP75/SL25", "tp_pct": 75.0, "sl_pct": -25.0},
    {"id": "tp100_sl25", "name": "TP100/SL25", "tp_pct": 100.0, "sl_pct": -25.0},
    {"id": "dynsl_15_35_55", "name": "DynSL 15/35/55", "sl_pct": -25.0, "floors": [(15, 0), (35, 15), (55, 30)]},
    {"id": "dynsl_20_40_60", "name": "DynSL 20/40/60", "sl_pct": -25.0, "floors": [(20, 0), (40, 15), (60, 30)]},
    {"id": "no_trailing", "name": "No trailing", "sl_pct": -25.0},
    {"id": "time_stop_1h", "name": "Time stop 1h proxy", "sl_pct": -25.0, "cap_pct": 30.0},
    {"id": "time_stop_4h", "name": "Time stop 4h proxy", "sl_pct": -25.0, "cap_pct": 50.0},
]


def hypothesis_match_proxy(hypothesis_row: dict, raw_row: dict, candidate_payload=None) -> bool:
    family = hypothesis_row.get("strategy_family")
    text = (hypothesis_row.get("entry_definition") or "").lower()
    payload = candidate_payload or raw_row.get("payload") or {}
    stage = ath_stage_from(raw_row, payload).upper()
    mc = row_market_cap(raw_row)
    bucket = market_cap_bucket(mc)
    idx = extract_index_snapshot(payload)
    super_idx = idx.get("super_index")
    trade_delta = idx.get("trade_delta")
    security_delta = idx.get("security_delta")
    security = idx.get("security_index")
    address = idx.get("address_index")
    if family == "ATH1 early scout":
        return "ATH#1" in stage or (stage == "ATH_UNKNOWN" and "ath#1" in text)
    if family == "ATH2 confirmation":
        return "ATH#2" in stage or (mc is not None and 50_000 <= mc < 150_000 and (trade_delta is None or trade_delta >= 2) and (security_delta is None or security_delta < 10))
    if family == "ATH3 continuation":
        return "ATH#3" in stage or ((security is None or security <= 20) and (address is None or address >= 15))
    if family == "ATH4+ late continuation":
        return bool(re.search(r"ATH#([4-9]|\d{2,})", stage))
    if family == "Super200 / high consensus":
        return super_idx is not None and super_idx >= 200
    if family == "MC bucket strategies":
        return bucket not in {"UNKNOWN", "LT15K"}
    if family == "Index delta strategies":
        return any(idx.get(f"{key}_delta") is not None for key in INDEX_KEYS)
    if family == "Filtered winner relaxation strategies":
        return is_gold_silver(raw_row) or (simulation_peak_pct(raw_row) or 0) >= 100
    if family in {"Exit policy variants", "Execution delay sensitivity tests"}:
        return is_gold_silver(raw_row) or simulation_peak_pct(raw_row) is not None
    return False


def self_test():
    text = "\n".join([
        "V17.4入场: ATH#1 + MC$20-75K + SupΔ≥15 + TΔ≥1",
        "V18 ATH#2 + TΔ≥2 + MC 50K-150K + SecΔ<8",
        "ATH#3 + Sec≤20 + Addr≥15 + DynSL 15/35/55",
        "SKIP但涨100%+，MC>=50K错杀",
    ])
    registry = build_hypothesis_registry(text, {"source_type": "self_test"})
    assert registry["promotion_allowed"] is False
    assert registry["hypotheses_count"] >= 12
    assert any(row["id"] == "SM-ATH2-CONFIRMATION-V18-MC50-100" for row in registry["hypotheses"])
    assert market_cap_bucket(75_000) == "50K-80K"
    assert simulate_exit_variant(100, {"id": "tp75_sl25", "tp_pct": 75, "sl_pct": -25})["net_pnl_pct"] == 75
    print("SELF_TEST_PASS strategy_hypothesis_registry")


if __name__ == "__main__":
    self_test()
