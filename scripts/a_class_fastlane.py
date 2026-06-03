"""A_CLASS_FASTLANE tiny-probe evaluator.

This module is deliberately independent from the normal entry committee. It
can bypass slow confirmation only after hard execution/security gates pass.
"""

from dataclasses import asdict, dataclass, field
import json
import time
from typing import Optional

from fastlane_config import AClassFastlaneConfig, load_a_class_config
from opportunity_freshness import FreshnessDecision, evaluate_opportunity_freshness


def _get(value, key, default=None):
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "tradable"}
    return bool(value)


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


def _first_present(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _normalize_ts_sec(value):
    ts = _safe_float(value, None)
    if ts is None or ts <= 0:
        return None
    if ts > 1_000_000_000_000:
        return ts / 1000.0
    return ts


def _json_loads(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _row_to_dict(row):
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {key: row[key] for key in keys()}
    return dict(row)


def _short_token(token_ca):
    token = str(token_ca or "")
    if len(token) <= 12:
        return token
    return f"{token[:6]}...{token[-4:]}"


@dataclass
class AClassCandidate:
    token_ca: str
    symbol: Optional[str] = None
    lifecycle_id: Optional[str] = None
    route_bucket: str = "A_GRADE"
    source_component: Optional[str] = None
    source_reason: Optional[str] = None
    signal_ts: Optional[float] = None
    opportunity_ts: Optional[float] = None
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
    liquidity_usd: Optional[float] = None
    spread_pct: Optional[float] = None
    quote_available: bool = False
    quote_executable: bool = False
    quote_clean: bool = True
    quote_source: Optional[str] = None
    quote_age_sec: Optional[float] = None
    quote_ts: Optional[float] = None
    quote_clean_verified: bool = False
    route_available: bool = False
    route_failure_reason: Optional[str] = None
    route_stable_recent: bool = False
    gmgn_pre_seen: bool = False
    gmgn_activity_fresh: bool = False
    gmgn_last_seen_age_sec: Optional[float] = None
    source_resonance: bool = False
    fresh_momentum: bool = False
    momentum_age_sec: Optional[float] = None
    momentum_pct: Optional[float] = None
    fresh_reclaim: bool = False
    fresh_ath_refresh: bool = False
    fresh_source_hit: bool = False
    premium_source_repeat_hit: bool = False
    missed_dog_cohort_strong: bool = False
    ath_continuation: bool = False
    lotto_early_momentum: bool = False
    reclaim_resonance: bool = False
    top10_pct: Optional[float] = None
    bundler_rate: Optional[float] = None
    rat_trader_rate: Optional[float] = None
    entrapment_ratio: Optional[float] = None
    spread_verified: bool = False
    creator_close: bool = False
    risk_flags: list = field(default_factory=list)
    recent_hard_loss: bool = False
    prior_fastlane_in_lifecycle: bool = False
    active_fastlane_count: int = 0
    daily_loss_budget_breached: bool = False
    mode_circuit_broken: bool = False
    data_confidence: str = "unknown"
    raw_payload: dict = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, data):
        data = _row_to_dict(data)
        payload = _json_loads(data.get("payload_json"))
        merged = {**payload, **data}
        risk_flags = merged.get("risk_flags")
        if isinstance(risk_flags, str):
            risk_flags = [part.strip() for part in risk_flags.split(",") if part.strip()]
        if not isinstance(risk_flags, list):
            risk_flags = []
        route = merged.get("route_bucket") or merged.get("route") or "A_GRADE"
        if str(route).upper() == "NOT_ATH":
            route = "RECLAIM"
        return cls(
            token_ca=str(merged.get("token_ca") or ""),
            symbol=merged.get("symbol"),
            lifecycle_id=merged.get("lifecycle_id"),
            route_bucket=str(route or "A_GRADE").upper(),
            source_component=merged.get("source_component") or merged.get("component"),
            source_reason=merged.get("source_reason") or merged.get("reject_reason"),
            signal_ts=_safe_float(merged.get("signal_ts"), None),
            opportunity_ts=_safe_float(merged.get("opportunity_ts"), None),
            current_price=_safe_float(merged.get("current_price") or merged.get("baseline_price"), None),
            market_cap=_safe_float(merged.get("market_cap"), None),
            liquidity_usd=_safe_float(merged.get("liquidity_usd"), None),
            spread_pct=_safe_float(merged.get("spread_pct") or merged.get("quote_spread_pct"), None),
            quote_available=_truthy(merged.get("quote_available", merged.get("tradable_missed", False))),
            quote_executable=_truthy(merged.get("quote_executable", merged.get("tradable_missed", False))),
            quote_clean=_truthy(merged.get("quote_clean", True)),
            quote_source=merged.get("quote_source") or merged.get("executable_peak_source"),
            quote_age_sec=_safe_float(merged.get("quote_age_sec"), None),
            quote_ts=_safe_float(merged.get("quote_ts") or merged.get("first_tradable_ts"), None),
            quote_clean_verified=_truthy(merged.get("quote_clean_verified", False)),
            route_available=_truthy(merged.get("route_available", merged.get("tradable_missed", False))),
            route_failure_reason=merged.get("route_failure_reason") or merged.get("tradability_reason"),
            route_stable_recent=_truthy(merged.get("route_stable_recent", False)),
            gmgn_pre_seen=_truthy(merged.get("gmgn_pre_seen", False)),
            gmgn_activity_fresh=_truthy(merged.get("gmgn_activity_fresh", False)),
            gmgn_last_seen_age_sec=_safe_float(merged.get("gmgn_last_seen_age_sec"), None),
            source_resonance=_truthy(merged.get("source_resonance", False)),
            fresh_momentum=_truthy(merged.get("fresh_momentum", False)),
            momentum_age_sec=_safe_float(merged.get("momentum_age_sec"), None),
            momentum_pct=_safe_float(merged.get("momentum_pct"), None),
            fresh_reclaim=_truthy(merged.get("fresh_reclaim", False)),
            fresh_ath_refresh=_truthy(merged.get("fresh_ath_refresh", False)),
            fresh_source_hit=_truthy(merged.get("fresh_source_hit", False)),
            premium_source_repeat_hit=_truthy(merged.get("premium_source_repeat_hit", False)),
            missed_dog_cohort_strong=_truthy(merged.get("missed_dog_cohort_strong", False)),
            ath_continuation=_truthy(merged.get("ath_continuation", str(route).upper() == "ATH")),
            lotto_early_momentum=_truthy(merged.get("lotto_early_momentum", str(route).upper() == "LOTTO")),
            reclaim_resonance=_truthy(merged.get("reclaim_resonance", str(route).upper() in {"RECLAIM", "A_GRADE"})),
            top10_pct=_safe_float(merged.get("top10_pct"), None),
            bundler_rate=_safe_float(merged.get("bundler_rate"), None),
            rat_trader_rate=_safe_float(merged.get("rat_trader_rate"), None),
            entrapment_ratio=_safe_float(merged.get("entrapment_ratio"), None),
            spread_verified=_truthy(merged.get("spread_verified", False)),
            creator_close=_truthy(merged.get("creator_close", False)),
            risk_flags=risk_flags,
            recent_hard_loss=_truthy(merged.get("recent_hard_loss", False)),
            prior_fastlane_in_lifecycle=_truthy(merged.get("prior_fastlane_in_lifecycle", False)),
            active_fastlane_count=_safe_int(merged.get("active_fastlane_count"), 0) or 0,
            daily_loss_budget_breached=_truthy(merged.get("daily_loss_budget_breached", False)),
            mode_circuit_broken=_truthy(merged.get("mode_circuit_broken", False)),
            data_confidence=str(merged.get("data_confidence") or "unknown"),
            raw_payload=merged,
        )

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class AClassDecision:
    action: str
    grade: str
    size_sol: float
    reason: str
    hard_blockers: list
    soft_notes: list
    score: float
    freshness_detail: dict
    budget_detail: dict
    risk_detail: dict

    def to_dict(self):
        return asdict(self)


def hard_prefilter(candidate, context=None, config=None):
    config = config or load_a_class_config()
    candidate = candidate if isinstance(candidate, AClassCandidate) else AClassCandidate.from_mapping(candidate)
    context = context or {}
    blockers = []
    detail = {}

    if not candidate.quote_available:
        blockers.append("quote_not_available")
    if not candidate.quote_executable:
        blockers.append("quote_not_executable")
    if not candidate.quote_source:
        blockers.append("quote_source_missing")
    if candidate.quote_age_sec is None:
        blockers.append("quote_age_unknown")
    elif candidate.quote_age_sec > config.quote_max_age_sec:
        blockers.append("quote_stale")

    if not candidate.route_available:
        blockers.append("route_unavailable")
    route_failure = str(candidate.route_failure_reason or "").lower()
    if any(token in route_failure for token in ("no_route", "route_unavailable", "trapped")):
        blockers.append("route_failure_red_flag")

    min_liq = config.min_liquidity_for_route(candidate.route_bucket)
    detail["min_liquidity_usd"] = min_liq
    if candidate.liquidity_usd is None:
        blockers.append("liquidity_unknown")
    elif candidate.liquidity_usd < min_liq:
        blockers.append("liquidity_below_min")

    max_spread = config.max_spread_for_route(candidate.route_bucket)
    detail["max_spread_pct"] = max_spread
    if candidate.spread_pct is None and not candidate.spread_verified:
        blockers.append("spread_unknown")
    elif candidate.spread_pct is not None and abs(candidate.spread_pct) > config.extreme_spread_block_pct:
        blockers.append("spread_extreme")
    elif candidate.spread_pct is not None and abs(candidate.spread_pct) > max_spread:
        blockers.append("spread_too_high")

    if candidate.creator_close:
        blockers.append("creator_close")
    risk_flag_text = " ".join(str(flag).lower() for flag in candidate.risk_flags)
    if any(token in risk_flag_text for token in ("rug", "honeypot", "blacklist", "trapped", "no_route", "creator_dump")):
        blockers.append("security_red_flag")
    if candidate.top10_pct is not None and candidate.top10_pct > config.top10_hard_max_pct:
        blockers.append("top10_too_high")
    if candidate.bundler_rate is not None and candidate.bundler_rate > config.bundler_hard_max:
        blockers.append("bundler_red_flag")
    if candidate.rat_trader_rate is not None and candidate.rat_trader_rate > config.rat_trader_hard_max:
        blockers.append("rat_trader_red_flag")
    if candidate.entrapment_ratio is not None and candidate.entrapment_ratio > config.entrapment_hard_max:
        blockers.append("entrapment_red_flag")

    if candidate.recent_hard_loss:
        blockers.append("recent_hard_loss")
    if candidate.prior_fastlane_in_lifecycle:
        blockers.append("prior_fastlane_in_lifecycle")
    active_count = _safe_int(context.get("active_fastlane_count"), candidate.active_fastlane_count) or 0
    if active_count >= config.max_concurrent:
        blockers.append("fastlane_concurrency_cap")
    if _truthy(context.get("daily_loss_budget_breached", candidate.daily_loss_budget_breached)):
        blockers.append("daily_loss_budget_breached")
    if _truthy(context.get("mode_circuit_broken", candidate.mode_circuit_broken)):
        blockers.append("mode_circuit_broken")

    detail.update({
        "quote_age_sec": candidate.quote_age_sec,
        "quote_source": candidate.quote_source,
        "liquidity_usd": candidate.liquidity_usd,
        "spread_pct": candidate.spread_pct,
        "spread_verified": candidate.spread_verified,
        "quote_clean_verified": candidate.quote_clean_verified,
        "route_bucket": candidate.route_bucket,
        "active_fastlane_count": active_count,
    })
    return len(blockers) == 0, blockers, detail


def score_a_class(candidate, freshness_decision: FreshnessDecision, config=None):
    candidate = candidate if isinstance(candidate, AClassCandidate) else AClassCandidate.from_mapping(candidate)
    score = 0.0
    detail = {
        "source_strength": 0.0,
        "execution_quality": 0.0,
        "freshness": 0.0,
        "structure_cleanliness": 0.0,
        "bucket_edge": 0.0,
    }

    if candidate.gmgn_pre_seen:
        detail["source_strength"] += 10
    if candidate.source_resonance:
        detail["source_strength"] += 10
    if candidate.fresh_ath_refresh or candidate.premium_source_repeat_hit:
        detail["source_strength"] += 10
    if candidate.missed_dog_cohort_strong:
        detail["source_strength"] += 5
    detail["source_strength"] = min(30, detail["source_strength"])

    if candidate.quote_available and candidate.quote_executable and candidate.quote_clean:
        detail["execution_quality"] += 10
    max_spread = (config or load_a_class_config()).max_spread_for_route(candidate.route_bucket)
    if candidate.spread_verified or (candidate.spread_pct is not None and abs(candidate.spread_pct) <= max_spread / 2):
        detail["execution_quality"] += 5
    if candidate.liquidity_usd is not None and candidate.liquidity_usd >= (config or load_a_class_config()).min_liquidity_for_route(candidate.route_bucket):
        detail["execution_quality"] += 5
    if candidate.route_stable_recent or candidate.route_available:
        detail["execution_quality"] += 5

    if freshness_decision.opportunity_age_sec is not None and freshness_decision.opportunity_age_sec <= 30:
        detail["freshness"] += 10
    if "fresh_momentum" in freshness_decision.freshness_sources or candidate.fresh_momentum:
        detail["freshness"] += 5
    if any(src in freshness_decision.freshness_sources for src in ("fresh_gmgn_activity", "fresh_reclaim")):
        detail["freshness"] += 5

    if candidate.top10_pct is None or candidate.top10_pct <= 50:
        detail["structure_cleanliness"] += 5
    if (
        (candidate.bundler_rate is None or candidate.bundler_rate <= 0.20)
        and (candidate.rat_trader_rate is None or candidate.rat_trader_rate <= 0.08)
        and (candidate.entrapment_ratio is None or candidate.entrapment_ratio <= 0.05)
    ):
        detail["structure_cleanliness"] += 5
    if not candidate.creator_close and not candidate.risk_flags:
        detail["structure_cleanliness"] += 5

    route = str(candidate.route_bucket or "").upper()
    if route in {"ATH", "LOTTO", "RECLAIM", "A_GRADE", "A_GRADE_RESONANCE_FASTLANE"}:
        detail["bucket_edge"] += 5
    if candidate.ath_continuation or candidate.lotto_early_momentum or candidate.reclaim_resonance:
        detail["bucket_edge"] += 5

    score = sum(detail.values())
    return min(100.0, score), detail


def apply_budget_guard(candidate, score, context=None, config=None):
    config = config or load_a_class_config()
    context = context or {}
    blockers = []
    detail = {
        "daily_loss_budget_sol": config.daily_loss_budget_sol,
        "max_concurrent": config.max_concurrent,
        "max_size_sol": config.max_size_sol,
    }
    active_count = _safe_int(context.get("active_fastlane_count"), _get(candidate, "active_fastlane_count", 0)) or 0
    daily_loss_used = _safe_float(context.get("daily_loss_used_sol"), 0.0) or 0.0
    if active_count >= config.max_concurrent:
        blockers.append("fastlane_concurrency_cap")
    if daily_loss_used >= config.daily_loss_budget_sol:
        blockers.append("daily_loss_budget_breached")
    if _truthy(context.get("global_circuit_broken", False)):
        blockers.append("global_circuit_broken")
    detail["active_fastlane_count"] = active_count
    detail["daily_loss_used_sol"] = daily_loss_used
    detail["score"] = score
    return len(blockers) == 0, blockers, detail


def decide_size(score, config=None):
    config = config or load_a_class_config()
    if score >= 92:
        return "A_PLUS", min(config.size_a_plus_sol, config.max_size_sol)
    if score >= 82:
        return "STRONG_A", min(config.size_strong_a_sol, config.max_size_sol)
    if score >= 70:
        return "A", min(config.size_a_sol, config.max_size_sol)
    return "REJECT", 0.0


def evaluate_a_class_fastlane(candidate, context=None, config=None, now_ts=None):
    config = config or load_a_class_config()
    candidate = candidate if isinstance(candidate, AClassCandidate) else AClassCandidate.from_mapping(candidate)
    passed, blockers, risk_detail = hard_prefilter(candidate, context=context, config=config)
    if not passed:
        return AClassDecision(
            action="BLOCK",
            grade="REJECT",
            size_sol=0.0,
            reason="hard_prefilter_failed",
            hard_blockers=sorted(set(blockers)),
            soft_notes=[],
            score=0.0,
            freshness_detail={},
            budget_detail={},
            risk_detail=risk_detail,
        )

    freshness = evaluate_opportunity_freshness(candidate, now_ts=now_ts, config=config)
    freshness_detail = freshness.to_dict()
    if not freshness.fresh:
        return AClassDecision(
            action="SHADOW",
            grade="REJECT",
            size_sol=0.0,
            reason=freshness.reason,
            hard_blockers=[],
            soft_notes=["no_live_entry_without_fresh_opportunity"],
            score=0.0,
            freshness_detail=freshness_detail,
            budget_detail={},
            risk_detail=risk_detail,
        )

    score, score_detail = score_a_class(candidate, freshness, config=config)
    if score < 70:
        return AClassDecision(
            action="SHADOW",
            grade="REJECT",
            size_sol=0.0,
            reason="score_below_a_threshold",
            hard_blockers=[],
            soft_notes=[f"score_detail={score_detail}"],
            score=score,
            freshness_detail=freshness_detail,
            budget_detail={},
            risk_detail=risk_detail,
        )

    budget_ok, budget_blockers, budget_detail = apply_budget_guard(candidate, score, context=context, config=config)
    if not budget_ok:
        return AClassDecision(
            action="BLOCK",
            grade="REJECT",
            size_sol=0.0,
            reason="budget_guard_failed",
            hard_blockers=sorted(set(budget_blockers)),
            soft_notes=[],
            score=score,
            freshness_detail=freshness_detail,
            budget_detail=budget_detail,
            risk_detail=risk_detail,
        )

    grade, size = decide_size(score, config=config)
    return AClassDecision(
        action="ENTER",
        grade=grade,
        size_sol=size,
        reason="a_class_fastlane_pass",
        hard_blockers=[],
        soft_notes=[f"score_detail={score_detail}"],
        score=score,
        freshness_detail=freshness_detail,
        budget_detail=budget_detail,
        risk_detail=risk_detail,
    )


def candidate_from_missed_row(row, now_ts=None):
    data = _row_to_dict(row)
    payload = _json_loads(data.get("payload_json"))
    merged = {**payload, **data}
    tradable = _truthy(merged.get("tradable_missed", False))
    if tradable and not merged.get("quote_source"):
        merged["quote_source"] = merged.get("executable_peak_source") or merged.get("tradability_status") or "missed_attribution"
    if tradable and merged.get("quote_age_sec") is None:
        first_tradable_ts = _safe_float(merged.get("first_tradable_ts"), None)
        if first_tradable_ts is not None and now_ts is not None:
            merged["quote_age_sec"] = max(0.0, float(now_ts) - first_tradable_ts)
        elif merged.get("fresh_quote"):
            merged["quote_age_sec"] = 0
    if tradable and merged.get("route_available") is None:
        merged["route_available"] = True
    if tradable and merged.get("quote_available") is None:
        merged["quote_available"] = True
    if tradable and merged.get("quote_executable") is None:
        merged["quote_executable"] = True
    if merged.get("liquidity_usd") is None:
        merged["liquidity_usd"] = payload.get("liquidity") or payload.get("liquidityUsd")
    if merged.get("spread_pct") is None:
        merged["spread_pct"] = payload.get("spread") or payload.get("spreadPct")
    if merged.get("data_confidence") is None:
        merged["data_confidence"] = "missed_attribution"
    return AClassCandidate.from_mapping(merged)


def _table_exists(db, table_name):
    try:
        return db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone() is not None
    except Exception:
        return False


def _text_contains_any(text, needles):
    haystack = str(text or "").lower()
    return any(str(needle).lower() in haystack for needle in needles)


def _candidate_route_from_text(*values):
    text = " ".join(str(value or "") for value in values).lower()
    if "lotto" in text:
        return "LOTTO"
    if "not_ath" in text or "reclaim" in text or "revival" in text:
        return "RECLAIM"
    if "ath" in text:
        return "ATH"
    return "A_GRADE"


def _truthy_first(*values):
    for value in values:
        if value is None:
            continue
        if _truthy(value):
            return True
    return False


def _nested_dicts(payload):
    if not isinstance(payload, dict):
        return []
    names = (
        "quote",
        "entry_quote",
        "execution_quote",
        "quote_snapshot",
        "latest_snapshot",
        "latest_watch_snapshot",
        "quote_shadow",
        "quote_audit",
        "guard",
        "entry_guard",
        "entry_latency_audit",
        "entry_execution_eligibility",
        "clean_dog_reclaim_eligibility",
        "recovery_quote",
        "confirmation",
    )
    out = [payload]
    for name in names:
        value = payload.get(name)
        if isinstance(value, dict):
            out.append(value)
    confirmation = payload.get("confirmation")
    if isinstance(confirmation, dict):
        confirming = confirmation.get("confirming_snapshots")
        if isinstance(confirming, list):
            out.extend(item for item in confirming if isinstance(item, dict))
    return out


def _first_from_dicts(dicts, *keys):
    for data in dicts:
        for key in keys:
            if key in data and data.get(key) not in (None, ""):
                return data.get(key)
    return None


def _first_bool_from_dicts(dicts, *keys):
    for data in dicts:
        for key in keys:
            if key in data and data.get(key) not in (None, ""):
                return _truthy(data.get(key))
    return None


def _age_from_any_ts(merged, payload_dicts, now_ts, *keys):
    if now_ts is None:
        return None, None
    ts_value = _first_present(
        *(merged.get(key) for key in keys),
        _first_from_dicts(payload_dicts, *keys),
    )
    ts = _normalize_ts_sec(ts_value)
    if ts is None:
        return None, None
    return max(0.0, float(now_ts) - ts), ts


def _extract_quote_fields(merged, payload, now_ts=None):
    payload_dicts = _nested_dicts(payload)
    quote = payload.get("quote")
    if not isinstance(quote, dict):
        quote = payload.get("entry_quote")
    if not isinstance(quote, dict):
        quote = payload.get("execution_quote")
    if not isinstance(quote, dict):
        quote = payload.get("quote_snapshot")
    if not isinstance(quote, dict):
        quote = {}
    quote_clean_evidence = _truthy_first(
        merged.get("quote_clean"),
        merged.get("quote_clean_seen"),
        merged.get("two_quote_clean_snapshots"),
        merged.get("recovery_quote_clean"),
        merged.get("final_reclaim_quote_executable"),
        merged.get("tradable_missed"),
        _first_from_dicts(
            payload_dicts,
            "quote_clean",
            "quoteClean",
            "quote_clean_seen",
            "two_quote_clean_snapshots",
            "recovery_quote_clean",
            "final_reclaim_quote_executable",
            "tradable_missed",
            "direct_entry_ok",
        ),
    )
    for key in (
        "quote_available",
        "quote_executable",
        "quote_clean",
        "quote_source",
        "quote_age_sec",
        "quote_ts",
        "route_available",
        "route_stable_recent",
        "liquidity_usd",
        "spread_pct",
        "market_cap",
    ):
        if merged.get(key) is None and quote.get(key) is not None:
            merged[key] = quote.get(key)
    if merged.get("quote_available") is None:
        value = _first_bool_from_dicts(
            payload_dicts,
            "quote_available",
            "quoteAvailable",
            "quote_success",
            "quoteSuccess",
            "success",
            "direct_entry_ok",
            "quote_clean_seen",
            "recovery_quote_clean",
            "final_reclaim_quote_executable",
            "tradable_missed",
        )
        if value is not None:
            merged["quote_available"] = value
    if merged.get("quote_executable") is None:
        value = _first_bool_from_dicts(
            payload_dicts,
            "quote_executable",
            "quoteExecutable",
            "routeAvailable",
            "route_available",
            "success",
            "direct_entry_ok",
            "quote_clean_seen",
            "recovery_quote_clean",
            "final_reclaim_quote_executable",
            "tradable_missed",
        )
        if value is not None:
            merged["quote_executable"] = value
    if merged.get("route_available") is None:
        value = _first_bool_from_dicts(
            payload_dicts,
            "route_available",
            "routeAvailable",
            "route_clean",
            "direct_entry_ok",
            "success",
            "quote_clean_seen",
            "recovery_quote_clean",
            "final_reclaim_quote_executable",
            "tradable_missed",
        )
        if value is not None:
            merged["route_available"] = value
    if merged.get("quote_clean") is None and quote_clean_evidence:
        merged["quote_clean"] = True
    if quote_clean_evidence:
        merged["quote_clean_verified"] = True
    if merged.get("quote_source") is None:
        merged["quote_source"] = (
            quote.get("source")
            or quote.get("provider")
            or _first_from_dicts(payload_dicts, "quote_source", "quoteSource", "source", "provider")
            or ("quote_clean_verified" if quote_clean_evidence else None)
        )
    if merged.get("liquidity_usd") is None:
        merged["liquidity_usd"] = (
            payload.get("liquidity_usd")
            or payload.get("liquidityUsd")
            or payload.get("liquidity")
            or payload.get("gmgn_last_liquidity")
            or payload.get("last_liquidity")
            or quote.get("liquidity_usd")
            or _first_from_dicts(
                payload_dicts,
                "liquidity_usd",
                "liquidityUsd",
                "liquidity",
                "gmgn_last_liquidity",
                "last_liquidity",
            )
        )
    if merged.get("spread_pct") is None:
        merged["spread_pct"] = _first_present(
            payload.get("spread_pct"),
            payload.get("quote_spread_pct"),
            payload.get("quote_gap_pct"),
            quote.get("spread_pct"),
            _first_from_dicts(payload_dicts, "spread_pct", "quote_spread_pct", "quote_gap_pct", "spreadPct"),
        )
    if merged.get("spread_verified") is None and quote_clean_evidence and merged.get("spread_pct") is None:
        # quote_clean_seen / final_reclaim_quote_executable are upstream
        # policy-level checks that already rejected extreme spread. Preserve
        # that as verified evidence without inventing a numeric spread.
        merged["spread_verified"] = True
    if merged.get("market_cap") is None:
        merged["market_cap"] = (
            payload.get("market_cap")
            or payload.get("marketCap")
            or payload.get("trigger_mc")
            or payload.get("gmgn_last_market_cap")
            or _first_from_dicts(payload_dicts, "market_cap", "marketCap", "trigger_mc", "gmgn_last_market_cap")
        )
    if merged.get("quote_ts") is None:
        merged["quote_ts"] = _first_present(
            _first_from_dicts(
                payload_dicts,
                "quote_ts",
                "quoteTs",
                "snapshot_ts",
                "last_clean_quote_ts",
                "last_tradable_ts",
                "missed_updated_at",
                "updated_at",
                "source_updated_ts",
            ),
            merged.get("last_clean_quote_ts"),
            merged.get("last_tradable_ts"),
        )
    if merged.get("quote_age_sec") is None and merged.get("quote_ts") is not None and now_ts is not None:
        quote_ts = _normalize_ts_sec(merged.get("quote_ts"))
        if quote_ts is not None:
            merged["quote_age_sec"] = max(0.0, float(now_ts) - quote_ts)
    if merged.get("quote_age_sec") is None and quote_clean_evidence:
        age, ts = _age_from_any_ts(
            merged,
            payload_dicts,
            now_ts,
            "last_clean_quote_ts",
            "last_tradable_ts",
            "snapshot_ts",
            "missed_updated_at",
            "updated_at",
            "source_updated_ts",
            "source_updated_at",
        )
        if age is not None:
            merged["quote_age_sec"] = age
            merged["quote_ts"] = ts
    if merged.get("route_stable_recent") is None and _truthy_first(
        merged.get("two_quote_clean_snapshots"),
        _first_from_dicts(payload_dicts, "two_quote_clean_snapshots"),
    ):
        merged["route_stable_recent"] = True
    if merged.get("gmgn_pre_seen") is None:
        value = _first_bool_from_dicts(payload_dicts, "gmgn_pre_seen")
        if value is not None:
            merged["gmgn_pre_seen"] = value
    if merged.get("gmgn_activity_fresh") is None:
        value = _first_bool_from_dicts(
            payload_dicts,
            "gmgn_activity_fresh",
            "gmgn_momentum_confirmed",
            "gmgn_volume_confirmed",
            "activity_confirmed",
        )
        if value is not None:
            merged["gmgn_activity_fresh"] = value
    if merged.get("fresh_reclaim") is None and _truthy_first(
        merged.get("recovery_quote_clean"),
        merged.get("final_reclaim_quote_executable"),
        _first_from_dicts(payload_dicts, "recovery_quote_clean", "final_reclaim_quote_executable"),
    ):
        merged["fresh_reclaim"] = True
    if merged.get("fresh_momentum") is None and _truthy_first(
        merged.get("gmgn_momentum_confirmed"),
        _first_from_dicts(payload_dicts, "gmgn_momentum_confirmed", "momentum_confirmed"),
    ):
        merged["fresh_momentum"] = True
    return merged


def candidate_from_decision_event_row(row, now_ts=None):
    data = _row_to_dict(row)
    payload = _json_loads(data.get("payload_json"))
    route = data.get("route") or payload.get("route") or payload.get("signal_route")
    component = data.get("component")
    reason = data.get("reason")
    text = " ".join(str(value or "") for value in (route, component, reason, data.get("event_type"), data.get("decision"), data.get("data_source")))
    merged = {**payload, **data}
    merged.update({
        "route_bucket": route or _candidate_route_from_text(text),
        "source_component": component or "paper_decision_events",
        "source_reason": reason or data.get("decision") or data.get("event_type"),
        "opportunity_ts": data.get("event_ts"),
        "fresh_source_hit": True,
        "source_resonance": _text_contains_any(text, ("resonance", "fast", "hard_gate_fast", "premium")),
        "fresh_reclaim": _text_contains_any(text, ("reclaim", "revival", "canary")),
        "fresh_ath_refresh": _text_contains_any(text, ("ath", "uncertainty_scout")),
        "gmgn_pre_seen": _truthy(payload.get("gmgn_pre_seen")) or _text_contains_any(text, ("gmgn",)),
        "gmgn_activity_fresh": _truthy(payload.get("gmgn_activity_fresh")) or _text_contains_any(text, ("gmgn",)),
        "missed_dog_cohort_strong": _text_contains_any(
            text,
            (
                "scout_quality_volume_low",
                "scout_quality_buy_pressure_weak",
                "scout_quality_negative_trend",
                "scout_quality_tx_low",
                "tracking_ttl_expired",
                "not_ath_prebuy_kline_block",
            ),
        ),
        "ath_continuation": str(route or "").upper() == "ATH" or _text_contains_any(text, ("ath_uncertainty", "ath_recovery")),
        "lotto_early_momentum": str(route or "").upper() == "LOTTO" or _text_contains_any(text, ("lotto",)),
        "reclaim_resonance": _text_contains_any(text, ("reclaim", "resonance", "revival")),
        "data_confidence": payload.get("data_confidence") or "decision_event_counterfactual",
    })
    _extract_quote_fields(merged, payload, now_ts=now_ts)
    return AClassCandidate.from_mapping(merged)


def candidate_from_fast_queue_row(row, now_ts=None):
    data = _row_to_dict(row)
    payload = _json_loads(data.get("payload_json"))
    first_error = data.get("first_error") or data.get("last_error")
    source_type = data.get("source_type")
    entry_branch = data.get("entry_branch")
    entry_mode = data.get("entry_mode_hint")
    text = " ".join(str(value or "") for value in (source_type, entry_branch, entry_mode, first_error, data.get("status")))
    merged = {**payload, **data}
    merged.update({
        "route_bucket": payload.get("route_bucket") or _candidate_route_from_text(text),
        "source_component": "paper_fast_lane",
        "source_reason": first_error or data.get("status"),
        "signal_ts": data.get("source_signal_ts") or data.get("signal_receive_ts") or data.get("signal_recorded_ts"),
        "opportunity_ts": data.get("updated_at") or data.get("created_at"),
        "fresh_source_hit": True,
        "source_resonance": _text_contains_any(text, ("source_resonance", "hard_gate_fast", "fast")),
        "gmgn_pre_seen": _truthy(payload.get("gmgn_pre_seen")) or _text_contains_any(text, ("gmgn",)),
        "gmgn_activity_fresh": _truthy(payload.get("gmgn_activity_fresh")) or _text_contains_any(text, ("gmgn",)),
        "fresh_reclaim": _text_contains_any(text, ("reclaim", "revival")),
        "fresh_ath_refresh": _text_contains_any(text, ("ath", "hard_gate_fast")),
        "ath_continuation": _text_contains_any(text, ("ath", "hard_gate_fast")),
        "lotto_early_momentum": _text_contains_any(text, ("lotto",)),
        "reclaim_resonance": _text_contains_any(text, ("reclaim", "resonance", "revival")),
        "data_confidence": payload.get("data_confidence") or "paper_fast_entry_queue",
    })
    _extract_quote_fields(merged, payload, now_ts=now_ts)
    return AClassCandidate.from_mapping(merged)


def candidate_from_source_resonance_row(row, now_ts=None):
    data = _row_to_dict(row)
    payload = _json_loads(data.get("payload_json"))
    updated_ts = _safe_float(data.get("updated_ts"), None)
    gmgn_last_seen_ts = _safe_float(data.get("gmgn_last_seen_ts"), None)
    gmgn_last_seen_age_sec = None
    if gmgn_last_seen_ts is not None and now_ts is not None:
        gmgn_last_seen_age_sec = max(0.0, float(now_ts) - gmgn_last_seen_ts)
    quote_clean = _truthy(data.get("quote_clean_seen")) or _safe_int(data.get("two_quote_clean_snapshots"), 0) >= 1
    quote_shadow = payload.get("quote_shadow") if isinstance(payload.get("quote_shadow"), dict) else {}
    last_clean_quote_ts = _normalize_ts_sec(
        quote_shadow.get("last_clean_quote_ts")
        or quote_shadow.get("snapshot_ts")
        or payload.get("last_clean_quote_ts")
    )
    quote_age_sec = None
    if quote_clean and now_ts is not None:
        quote_ref_ts = last_clean_quote_ts or updated_ts
        if quote_ref_ts is not None:
            quote_age_sec = max(0.0, float(now_ts) - quote_ref_ts)
    signal_type = data.get("signal_type")
    route_bucket = "ATH" if str(signal_type or "").upper() == "ATH" else "RECLAIM"
    merged = {**payload, **data}
    merged.update({
        "route_bucket": route_bucket,
        "source_component": "source_resonance_shadow",
        "source_reason": data.get("cohort") or f"resonance_level_{data.get('resonance_level') or 1}",
        "opportunity_ts": updated_ts or data.get("signal_ts"),
        "market_cap": data.get("gmgn_last_market_cap"),
        "liquidity_usd": data.get("gmgn_last_liquidity"),
        "quote_available": quote_clean,
        "quote_executable": quote_clean,
        "quote_clean": quote_clean,
        "quote_clean_verified": quote_clean,
        "quote_source": "source_resonance_quote_clean" if quote_clean else None,
        "quote_age_sec": quote_age_sec,
        "quote_ts": last_clean_quote_ts or updated_ts,
        "route_available": quote_clean,
        "route_stable_recent": _safe_int(data.get("two_quote_clean_snapshots"), 0) >= 2,
        "gmgn_pre_seen": _truthy(data.get("gmgn_pre_seen")),
        "gmgn_activity_fresh": gmgn_last_seen_age_sec is not None and gmgn_last_seen_age_sec <= 60,
        "gmgn_last_seen_age_sec": gmgn_last_seen_age_sec,
        "source_resonance": True,
        "fresh_source_hit": True,
        "fresh_reclaim": route_bucket == "RECLAIM",
        "fresh_ath_refresh": route_bucket == "ATH",
        "premium_source_repeat_hit": _safe_int(data.get("source_count"), 1) >= 2,
        "ath_continuation": route_bucket == "ATH",
        "reclaim_resonance": route_bucket == "RECLAIM",
        "spread_verified": quote_clean and (quote_shadow.get("spread_pct") is None and quote_shadow.get("quote_gap_pct") is None),
        "data_confidence": payload.get("data_confidence") or "source_resonance_shadow",
    })
    _extract_quote_fields(merged, payload, now_ts=now_ts)
    return AClassCandidate.from_mapping(merged)


def _evaluate_and_record_candidate(
    db,
    *,
    candidate,
    source_table,
    source_id,
    now_ts,
    config,
    logger,
    record_a_class_decision_event,
):
    if not candidate.token_ca:
        return None
    if logger:
        logger.info(
            f"  [A_CLASS_CANDIDATE] symbol={candidate.symbol or 'UNKNOWN'} "
            f"token={_short_token(candidate.token_ca)} lifecycle={candidate.lifecycle_id or 'n/a'} "
            f"route={candidate.route_bucket} source={candidate.source_component or source_table or 'unknown'} "
            f"reason={candidate.source_reason or 'unknown'}"
        )
    decision = evaluate_a_class_fastlane(candidate, config=config, now_ts=now_ts)
    stored_action = "WOULD_ENTER" if decision.action == "ENTER" and not config.enabled else decision.action
    record_a_class_decision_event(
        db,
        candidate=candidate,
        decision=decision,
        stored_action=stored_action,
        source_table=source_table,
        source_id=source_id,
        now_ts=now_ts,
    )
    if stored_action == "WOULD_ENTER":
        marker = "A_CLASS_WOULD_ENTER"
    elif stored_action == "BLOCK":
        marker = "A_CLASS_HARD_BLOCK"
    else:
        marker = "A_CLASS_STALE" if "stale" in str(decision.reason).lower() or "fresh" in str(decision.reason).lower() else "A_CLASS_SHADOW"
    if logger:
        if decision.score:
            logger.info(
                f"  [A_CLASS_SCORE] symbol={candidate.symbol or 'UNKNOWN'} "
                f"token={_short_token(candidate.token_ca)} score={decision.score:.1f} "
                f"grade={decision.grade} size={decision.size_sol:.3f}SOL"
            )
        logger.info(
            f"  [{marker}] symbol={candidate.symbol or 'UNKNOWN'} "
            f"token={_short_token(candidate.token_ca)} lifecycle={candidate.lifecycle_id or 'n/a'} "
            f"route={candidate.route_bucket} score={decision.score:.1f} "
            f"size={decision.size_sol:.3f} reason={decision.reason} "
            f"blockers={decision.hard_blockers} "
            f"sources={decision.freshness_detail.get('freshness_sources', [])}"
        )
    return stored_action


def _update_summary(summary, source_table, stored_action):
    summary["candidates"] += 1
    source_summary = summary.setdefault("sources", {}).setdefault(
        source_table,
        {"candidates": 0, "would_enter": 0, "shadow": 0, "block": 0},
    )
    source_summary["candidates"] += 1
    if stored_action == "WOULD_ENTER":
        summary["would_enter"] += 1
        source_summary["would_enter"] += 1
    elif stored_action == "BLOCK":
        summary["block"] += 1
        source_summary["block"] += 1
    else:
        summary["shadow"] += 1
        source_summary["shadow"] += 1


def _query_recent_missed_attribution(db, now_ts, config, limit):
    if not _table_exists(db, "paper_missed_signal_attribution"):
        return []
    return db.execute(
        """
        SELECT m.*
        FROM paper_missed_signal_attribution m
        WHERE COALESCE(m.created_event_ts, m.updated_at, 0) >= ?
          AND COALESCE(m.token_ca, '') != ''
          AND COALESCE(m.status, '') != 'tracking'
          AND NOT EXISTS (
              SELECT 1
              FROM a_class_decision_events e
              WHERE e.source_table = 'paper_missed_signal_attribution'
                AND e.source_id = m.id
          )
        ORDER BY COALESCE(m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                 COALESCE(m.updated_at, m.created_event_ts, 0) DESC
        LIMIT ?
        """,
        (now_ts - config.shadow_scan_window_sec, int(limit)),
    ).fetchall()


def _query_recent_decision_events(db, now_ts, config, limit):
    if not _table_exists(db, "paper_decision_events"):
        return []
    return db.execute(
        """
        SELECT e.*
        FROM paper_decision_events e
        WHERE COALESCE(e.event_ts, 0) >= ?
          AND COALESCE(e.token_ca, '') != ''
          AND NOT EXISTS (
              SELECT 1
              FROM a_class_decision_events ace
              WHERE ace.source_table = 'paper_decision_events'
                AND ace.source_id = e.id
          )
          AND (
              LOWER(COALESCE(e.component, '')) LIKE '%shadow%'
              OR LOWER(COALESCE(e.component, '')) LIKE '%probe%'
              OR LOWER(COALESCE(e.component, '')) LIKE '%canary%'
              OR LOWER(COALESCE(e.component, '')) LIKE '%reclaim%'
              OR LOWER(COALESCE(e.component, '')) LIKE '%fast%'
              OR LOWER(COALESCE(e.reason, '')) LIKE '%shadow%'
              OR LOWER(COALESCE(e.reason, '')) LIKE '%probe%'
              OR LOWER(COALESCE(e.reason, '')) LIKE '%canary%'
              OR LOWER(COALESCE(e.reason, '')) LIKE '%reclaim%'
              OR LOWER(COALESCE(e.reason, '')) LIKE '%markov%'
              OR COALESCE(e.reason, '') IN (
                  'scout_quality_volume_low',
                  'scout_quality_buy_pressure_weak',
                  'scout_quality_negative_trend',
                  'scout_quality_tx_low',
                  'tracking_ttl_expired',
                  'not_ath_prebuy_kline_block'
              )
          )
        ORDER BY e.event_ts DESC, e.id DESC
        LIMIT ?
        """,
        (now_ts - config.shadow_scan_window_sec, int(limit)),
    ).fetchall()


def _query_recent_fast_queue(db, now_ts, config, limit):
    if not _table_exists(db, "paper_fast_entry_queue"):
        return []
    return db.execute(
        """
        SELECT q.*
        FROM paper_fast_entry_queue q
        WHERE COALESCE(q.updated_at, q.created_at, 0) >= ?
          AND COALESCE(q.token_ca, '') != ''
          AND COALESCE(q.status, '') IN (
              'watch_only',
              'counterfactual_only',
              'rejected',
              'queued',
              'quote_failed',
              'expired',
              'rate_limited'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM a_class_decision_events ace
              WHERE ace.source_table = 'paper_fast_entry_queue'
                AND ace.source_id = q.id
          )
        ORDER BY COALESCE(q.updated_at, q.created_at, 0) DESC, q.id DESC
        LIMIT ?
        """,
        (now_ts - config.shadow_scan_window_sec, int(limit)),
    ).fetchall()


def _query_recent_source_resonance(db, now_ts, config, limit):
    if not _table_exists(db, "source_resonance_candidates"):
        return []
    return db.execute(
        """
        SELECT s.*,
               CASE
                 WHEN typeof(s.updated_at) IN ('integer', 'real') THEN s.updated_at
                 ELSE CAST(strftime('%s', s.updated_at) AS REAL)
               END AS updated_ts
        FROM source_resonance_candidates s
        WHERE COALESCE(
                CASE
                  WHEN typeof(s.updated_at) IN ('integer', 'real') THEN s.updated_at
                  ELSE CAST(strftime('%s', s.updated_at) AS REAL)
                END,
                s.signal_ts / 1000,
                0
              ) >= ?
          AND COALESCE(s.token_ca, '') != ''
          AND (
              COALESCE(s.gmgn_pre_seen, 0) = 1
              OR COALESCE(s.source_count, 0) >= 2
              OR COALESCE(s.quote_clean_seen, 0) = 1
              OR COALESCE(s.two_quote_clean_snapshots, 0) >= 1
              OR COALESCE(s.resonance_score, 0) > 0
          )
          AND NOT EXISTS (
              SELECT 1
              FROM a_class_decision_events ace
              WHERE ace.source_table = 'source_resonance_candidates'
                AND ace.source_id = s.id
          )
        ORDER BY COALESCE(s.resonance_score, 0) DESC,
                 COALESCE(s.gmgn_pre_seen, 0) DESC,
                 COALESCE(s.quote_clean_seen, 0) DESC,
                 updated_ts DESC
        LIMIT ?
        """,
        (now_ts - config.shadow_scan_window_sec, int(limit)),
    ).fetchall()


def record_a_class_fastlane_shadow_candidates(db, *, now_ts=None, limit=50, config=None, logger=None):
    """Evaluate shadow/counterfactual A-class candidates and record evidence.

    This creates evidence only. It never creates a paper position.
    """
    config = config or load_a_class_config()
    if not config.shadow_eval_enabled:
        return {"candidates": 0, "would_enter": 0, "shadow": 0, "block": 0, "sources": {}}

    from canonical_ledger import init_canonical_ledger, record_a_class_decision_event

    init_canonical_ledger(db)
    now_ts = float(now_ts if now_ts is not None else time.time())
    per_source_limit = max(1, int(limit))
    summary = {"candidates": 0, "would_enter": 0, "shadow": 0, "block": 0, "sources": {}}
    sources = (
        ("paper_missed_signal_attribution", _query_recent_missed_attribution, candidate_from_missed_row),
        ("paper_fast_entry_queue", _query_recent_fast_queue, candidate_from_fast_queue_row),
        ("source_resonance_candidates", _query_recent_source_resonance, candidate_from_source_resonance_row),
        ("paper_decision_events", _query_recent_decision_events, candidate_from_decision_event_row),
    )
    errors = {}
    for source_table, query_func, builder in sources:
        try:
            rows = query_func(db, now_ts, config, per_source_limit)
        except Exception as exc:
            errors[source_table] = str(exc)
            if logger:
                logger.debug(f"  [A_CLASS_CANDIDATE] {source_table} query failed: {exc}")
            continue
        for row in rows:
            row_dict = _row_to_dict(row)
            try:
                candidate = builder(row, now_ts=now_ts)
                stored_action = _evaluate_and_record_candidate(
                    db,
                    candidate=candidate,
                    source_table=source_table,
                    source_id=row_dict.get("id"),
                    now_ts=now_ts,
                    config=config,
                    logger=logger,
                    record_a_class_decision_event=record_a_class_decision_event,
                )
            except Exception as exc:
                errors[f"{source_table}:{row_dict.get('id')}"] = str(exc)
                if logger:
                    logger.debug(f"  [A_CLASS_CANDIDATE] {source_table} id={row_dict.get('id')} failed: {exc}")
                continue
            if stored_action:
                _update_summary(summary, source_table, stored_action)
    if errors:
        summary["errors"] = errors
    return summary
