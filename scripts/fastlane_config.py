"""Configuration for A_CLASS_FASTLANE tiny-probe experiments.

The first rollout is intentionally conservative: live entry is disabled by
default while shadow evaluation is enabled so the monitor can build an
auditable candidate stream before creating paper positions.
"""

from dataclasses import dataclass
import os


def _env_bool(env, name, default):
    value = env.get(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(env, name, default):
    value = env.get(name)
    if value is None or str(value).strip() == "":
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _env_int(env, name, default):
    value = env.get(name)
    if value is None or str(value).strip() == "":
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _env_str(env, name, default):
    value = env.get(name)
    if value is None:
        return str(default)
    text = str(value).strip()
    return text or str(default)


@dataclass(frozen=True)
class AClassFastlaneConfig:
    enabled: bool = False
    shadow_eval_enabled: bool = True

    size_a_sol: float = 0.001
    size_strong_a_sol: float = 0.002
    size_a_plus_sol: float = 0.003
    max_size_sol: float = 0.003

    max_concurrent: int = 2
    max_per_token_lifecycle: int = 1
    daily_loss_budget_sol: float = 0.02
    live_max_size_sol: float = 0.001
    live_max_concurrent: int = 1
    live_daily_loss_budget_sol: float = 0.005
    live_max_enqueues_per_scan: int = 1
    live_max_per_hour: int = 1
    consecutive_loss_circuit_breaker: int = 5
    mode_consecutive_loss_circuit_breaker: int = 3
    mode_min_sample_to_upgrade: int = 30

    quote_max_age_sec: float = 10.0
    opportunity_max_age_sec: float = 60.0
    opportunity_shadow_max_age_sec: float = 180.0
    opportunity_dedup_sec: float = 300.0

    lotto_min_liquidity_usd: float = 15000.0
    ath_min_liquidity_usd: float = 10000.0
    reclaim_min_liquidity_usd: float = 10000.0

    max_spread_pct: float = 3.0
    lotto_max_spread_pct: float = 2.0
    extreme_spread_block_pct: float = 5.0

    fast_stop_loss_pct: float = -0.15
    no_positive_feedback_sec: float = 60.0
    breakeven_peak_pct: float = 0.20
    recover_principal_peak_pct: float = 1.00
    moonbag_peak_pct: float = 3.00

    top10_hard_max_pct: float = 70.0
    bundler_hard_max: float = 0.50
    rat_trader_hard_max: float = 0.20
    entrapment_hard_max: float = 0.12

    shadow_scan_window_sec: float = 2 * 60 * 60

    provider_hydrate_enabled: bool = True
    provider_hydrate_max_per_scan: int = 30
    provider_hydrate_source_budget_json: str = (
        '{"paper_missed_signal_attribution":6,'
        '"paper_fast_entry_queue":8,'
        '"source_resonance_candidates":10,'
        '"external_alpha_state":10,'
        '"paper_decision_events":4}'
    )
    provider_hydrate_cache_ttl_sec: float = 20.0
    provider_hydrate_failure_cache_ttl_sec: float = 8.0
    provider_hydrate_rate_limit_backoff_sec: float = 60.0
    provider_hydrate_backoff_max_sec: float = 300.0
    provider_hydrate_timeout_sec: float = 4.0
    provider_hydrate_amount_raw: str = "1000000"
    provider_hydrate_slippage_bps: int = 500
    provider_hydrate_endpoint_base: str = "https://api.jup.ag/ultra/v1/order"

    def min_liquidity_for_route(self, route_bucket):
        route = str(route_bucket or "").upper()
        if route == "LOTTO":
            return self.lotto_min_liquidity_usd
        if route in {"RECLAIM", "A_GRADE", "A_GRADE_RESONANCE_FASTLANE"}:
            return self.reclaim_min_liquidity_usd
        return self.ath_min_liquidity_usd

    def max_spread_for_route(self, route_bucket):
        route = str(route_bucket or "").upper()
        if route == "LOTTO":
            return self.lotto_max_spread_pct
        return self.max_spread_pct


def load_a_class_config(env=None):
    explicit_env = env is not None
    if env is None:
        env = os.environ
    safe_canary_force = _env_bool(env, "A_CLASS_SAFE_CANARY_FORCE", not explicit_env)
    enabled = True if safe_canary_force else _env_bool(env, "A_CLASS_ENABLED", False)
    return AClassFastlaneConfig(
        enabled=enabled,
        shadow_eval_enabled=_env_bool(env, "A_CLASS_SHADOW_EVAL_ENABLED", True),
        size_a_sol=_env_float(env, "A_CLASS_SIZE_A_SOL", 0.001),
        size_strong_a_sol=_env_float(env, "A_CLASS_SIZE_STRONG_A_SOL", 0.002),
        size_a_plus_sol=_env_float(env, "A_CLASS_SIZE_A_PLUS_SOL", 0.003),
        max_size_sol=_env_float(env, "A_CLASS_MAX_SIZE_SOL", 0.003),
        max_concurrent=_env_int(env, "A_CLASS_MAX_CONCURRENT", 2),
        max_per_token_lifecycle=_env_int(env, "A_CLASS_MAX_PER_TOKEN_LIFECYCLE", 1),
        daily_loss_budget_sol=_env_float(env, "A_CLASS_DAILY_LOSS_BUDGET_SOL", 0.02),
        live_max_size_sol=_env_float(env, "A_CLASS_LIVE_MAX_SIZE_SOL", 0.001),
        live_max_concurrent=_env_int(env, "A_CLASS_LIVE_MAX_CONCURRENT", 1),
        live_daily_loss_budget_sol=_env_float(env, "A_CLASS_LIVE_DAILY_LOSS_BUDGET_SOL", 0.005),
        live_max_enqueues_per_scan=_env_int(env, "A_CLASS_LIVE_MAX_ENQUEUES_PER_SCAN", 1),
        live_max_per_hour=_env_int(env, "A_CLASS_LIVE_MAX_PER_HOUR", _env_int(env, "A_CLASS_CANARY_MAX_PER_HOUR", 1)),
        consecutive_loss_circuit_breaker=_env_int(env, "A_CLASS_CONSECUTIVE_LOSS_CIRCUIT_BREAKER", 5),
        mode_consecutive_loss_circuit_breaker=_env_int(env, "A_CLASS_MODE_CONSECUTIVE_LOSS_CIRCUIT_BREAKER", 3),
        mode_min_sample_to_upgrade=_env_int(env, "A_CLASS_MODE_MIN_SAMPLE_TO_UPGRADE", 30),
        quote_max_age_sec=_env_float(env, "A_CLASS_QUOTE_MAX_AGE_SEC", 10.0),
        opportunity_max_age_sec=_env_float(env, "A_CLASS_OPPORTUNITY_MAX_AGE_SEC", 60.0),
        opportunity_shadow_max_age_sec=_env_float(env, "A_CLASS_OPPORTUNITY_SHADOW_MAX_AGE_SEC", 180.0),
        opportunity_dedup_sec=_env_float(env, "A_CLASS_OPPORTUNITY_DEDUP_SEC", 300.0),
        lotto_min_liquidity_usd=_env_float(env, "A_CLASS_LOTTO_MIN_LIQUIDITY_USD", 15000.0),
        ath_min_liquidity_usd=_env_float(env, "A_CLASS_ATH_MIN_LIQUIDITY_USD", 10000.0),
        reclaim_min_liquidity_usd=_env_float(env, "A_CLASS_RECLAIM_MIN_LIQUIDITY_USD", 10000.0),
        max_spread_pct=_env_float(env, "A_CLASS_MAX_SPREAD_PCT", 3.0),
        lotto_max_spread_pct=_env_float(env, "A_CLASS_LOTTO_MAX_SPREAD_PCT", 2.0),
        extreme_spread_block_pct=_env_float(env, "A_CLASS_EXTREME_SPREAD_BLOCK_PCT", 5.0),
        fast_stop_loss_pct=_env_float(env, "A_CLASS_FAST_STOP_LOSS_PCT", -0.15),
        no_positive_feedback_sec=_env_float(env, "A_CLASS_NO_POSITIVE_FEEDBACK_SEC", 60.0),
        breakeven_peak_pct=_env_float(env, "A_CLASS_BREAKEVEN_PEAK_PCT", 0.20),
        recover_principal_peak_pct=_env_float(env, "A_CLASS_RECOVER_PRINCIPAL_PEAK_PCT", 1.00),
        moonbag_peak_pct=_env_float(env, "A_CLASS_MOONBAG_PEAK_PCT", 3.00),
        top10_hard_max_pct=_env_float(env, "A_CLASS_TOP10_HARD_MAX_PCT", 70.0),
        bundler_hard_max=_env_float(env, "A_CLASS_BUNDLER_HARD_MAX", 0.50),
        rat_trader_hard_max=_env_float(env, "A_CLASS_RAT_TRADER_HARD_MAX", 0.20),
        entrapment_hard_max=_env_float(env, "A_CLASS_ENTRAPMENT_HARD_MAX", 0.12),
        shadow_scan_window_sec=_env_float(env, "A_CLASS_SHADOW_SCAN_WINDOW_SEC", 2 * 60 * 60),
        provider_hydrate_enabled=_env_bool(env, "A_CLASS_PROVIDER_HYDRATE_ENABLED", True),
        provider_hydrate_max_per_scan=_env_int(env, "A_CLASS_PROVIDER_HYDRATE_MAX_PER_SCAN", 30),
        provider_hydrate_source_budget_json=_env_str(
            env,
            "A_CLASS_PROVIDER_HYDRATE_SOURCE_BUDGET_JSON",
            '{"paper_missed_signal_attribution":6,'
            '"paper_fast_entry_queue":8,'
            '"source_resonance_candidates":10,'
            '"external_alpha_state":10,'
            '"paper_decision_events":4}',
        ),
        provider_hydrate_cache_ttl_sec=_env_float(env, "A_CLASS_PROVIDER_HYDRATE_CACHE_TTL_SEC", 20.0),
        provider_hydrate_failure_cache_ttl_sec=_env_float(env, "A_CLASS_PROVIDER_HYDRATE_FAILURE_CACHE_TTL_SEC", 8.0),
        provider_hydrate_rate_limit_backoff_sec=_env_float(env, "A_CLASS_PROVIDER_HYDRATE_RATE_LIMIT_BACKOFF_SEC", 60.0),
        provider_hydrate_backoff_max_sec=_env_float(env, "A_CLASS_PROVIDER_HYDRATE_BACKOFF_MAX_SEC", 300.0),
        provider_hydrate_timeout_sec=_env_float(env, "A_CLASS_PROVIDER_HYDRATE_TIMEOUT_SEC", 4.0),
        provider_hydrate_amount_raw=_env_str(env, "A_CLASS_PROVIDER_HYDRATE_AMOUNT_RAW", "1000000"),
        provider_hydrate_slippage_bps=_env_int(env, "A_CLASS_PROVIDER_HYDRATE_SLIPPAGE_BPS", 500),
        provider_hydrate_endpoint_base=_env_str(
            env,
            "A_CLASS_PROVIDER_HYDRATE_ENDPOINT_BASE",
            "https://api.jup.ag/ultra/v1/order",
        ),
    )
