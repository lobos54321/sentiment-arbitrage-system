export const NOT_ATH_RELAXED_SHADOW_COHORTS = {
  two_snapshot_strict: {
    order: 10,
    description: 'Current strict promotion gate: two consecutive snapshot_pass samples.',
  },
  two_quote_clean_snapshots: {
    order: 20,
    description: 'Two consecutive quote-clean samples, ignoring reclaim sub-gates.',
  },
  snapshot_pass_1_snapshot: {
    order: 30,
    description: 'At least one strict snapshot_pass sample.',
  },
  quote_clean_1_snapshot: {
    order: 40,
    description: 'At least one quote-clean executable sample.',
  },
  quote_clean_no_double_confirm: {
    order: 50,
    description: 'Quote-clean at least once, but not enough for the strict two-snapshot gate.',
  },
  quote_clean_activity_only: {
    order: 60,
    description: 'Quote-clean plus activity reclaim, ignoring volume and momentum reclaim.',
  },
  relaxed_liquidity_floor: {
    order: 70,
    description: 'Executable quote with gap/spread ok under a lower liquidity floor, but not strict quote-clean.',
  },
  relaxed_age_window: {
    order: 80,
    description: 'Late observation after the strict 30m confirm window, still shadow-only.',
  },
};

function hasTable(db, name) {
  return Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name));
}

function cohortMeta(cohort) {
  return NOT_ATH_RELAXED_SHADOW_COHORTS[cohort] || {
    order: 999,
    description: 'Unclassified relaxed shadow cohort.',
  };
}

function normalizedRate(numerator, denominator) {
  const n = Number(numerator || 0);
  const d = Number(denominator || 0);
  return d > 0 ? n / d : 0;
}

function addCohortDerivedFields(row) {
  const candidates = Number(row.candidates || 0);
  const cleanTradable = Number(row.clean_tradable_n || 0);
  const gold = Number(row.gold_n || 0);
  const silver = Number(row.silver_n || 0);
  const bronze = Number(row.bronze_n || 0);
  const meta = cohortMeta(row.cohort);
  return {
    ...row,
    description: meta.description,
    order: meta.order,
    clean_tradable_rate: normalizedRate(cleanTradable, candidates),
    gold_rate: normalizedRate(gold, candidates),
    medal_rate: normalizedRate(gold + silver + bronze, candidates),
  };
}

function relaxedShadowCohortSql({ sinceTs }) {
  const snapshotWhereSql = sinceTs ? 'AND snapshot_ts >= @since' : '';
  const missedWhereSql = sinceTs ? 'AND created_event_ts >= @since' : '';
  return `
    WITH snapshot_base AS (
      SELECT
        token_ca,
        COALESCE(signal_ts, 0) AS signal_key,
        signal_ts,
        symbol,
        snapshot_ts,
        first_seen_ts,
        horizon_sec,
        mark_price,
        quote_price,
        quote_gap_pct,
        spread_pct,
        liquidity_usd,
        COALESCE(quote_clean, 0) AS quote_clean,
        COALESCE(activity_reclaim, 0) AS activity_reclaim,
        COALESCE(volume_reclaim, 0) AS volume_reclaim,
        COALESCE(momentum_reclaim, 0) AS momentum_reclaim,
        COALESCE(snapshot_pass, 0) AS snapshot_pass
      FROM lotto_not_ath_watch_shadow_snapshots
      WHERE parent_blocker = 'not_ath_v17'
        ${snapshotWhereSql}
    ),
    snapshot_pairs AS (
      SELECT
        s1.token_ca,
        s1.signal_key,
        MAX(CASE WHEN s1.snapshot_pass = 1 AND s2.snapshot_pass = 1 THEN 1 ELSE 0 END) AS has_two_snapshot_pass,
        MAX(CASE WHEN s1.quote_clean = 1 AND s2.quote_clean = 1 THEN 1 ELSE 0 END) AS has_two_quote_clean
      FROM snapshot_base s1
      JOIN snapshot_base s2
        ON s2.token_ca = s1.token_ca
       AND s2.signal_key = s1.signal_key
       AND s2.horizon_sec = s1.horizon_sec + @snapshotIntervalSec
      GROUP BY s1.token_ca, s1.signal_key
    ),
    snapshot_features AS (
      SELECT
        s.token_ca,
        s.signal_key,
        MAX(s.signal_ts) AS signal_ts,
        COALESCE(MAX(s.symbol), substr(s.token_ca, 1, 8), '?') AS symbol,
        COUNT(*) AS snapshots,
        MIN(s.snapshot_ts) AS first_snapshot_ts,
        MAX(s.snapshot_ts) AS last_snapshot_ts,
        MIN(s.horizon_sec) AS first_horizon_sec,
        MAX(s.horizon_sec) AS max_horizon_sec,
        MAX(CASE WHEN s.quote_clean = 1 THEN 1 ELSE 0 END) AS has_quote_clean_1,
        MAX(CASE WHEN s.snapshot_pass = 1 THEN 1 ELSE 0 END) AS has_snapshot_pass_1,
        MAX(CASE WHEN s.quote_clean = 1 AND s.activity_reclaim = 1 THEN 1 ELSE 0 END) AS has_quote_clean_activity,
        MAX(CASE WHEN s.quote_clean = 1 AND (s.activity_reclaim = 1 OR s.volume_reclaim = 1 OR s.momentum_reclaim = 1) THEN 1 ELSE 0 END) AS has_quote_clean_any_reclaim,
        MAX(
          CASE
            WHEN s.quote_price IS NOT NULL
             AND s.mark_price IS NOT NULL
             AND s.quote_gap_pct IS NOT NULL
             AND ABS(s.quote_gap_pct) <= @maxQuoteGapPct
             AND (s.spread_pct IS NULL OR s.spread_pct <= @maxSpreadPct)
             AND COALESCE(s.liquidity_usd, 0) >= @relaxedLiquidityUsd
            THEN 1 ELSE 0
          END
        ) AS has_relaxed_quote_clean,
        MAX(
          CASE
            WHEN s.horizon_sec > @strictConfirmBySec
             AND (
               s.quote_clean = 1
               OR s.snapshot_pass = 1
               OR (
                 s.quote_price IS NOT NULL
                 AND s.mark_price IS NOT NULL
                 AND s.quote_gap_pct IS NOT NULL
                 AND ABS(s.quote_gap_pct) <= @maxQuoteGapPct
                 AND (s.spread_pct IS NULL OR s.spread_pct <= @maxSpreadPct)
                 AND COALESCE(s.liquidity_usd, 0) >= @relaxedLiquidityUsd
               )
             )
            THEN 1 ELSE 0
          END
        ) AS has_relaxed_age_signal,
        COALESCE(MAX(p.has_two_snapshot_pass), 0) AS has_two_snapshot_pass,
        COALESCE(MAX(p.has_two_quote_clean), 0) AS has_two_quote_clean
      FROM snapshot_base s
      LEFT JOIN snapshot_pairs p
        ON p.token_ca = s.token_ca
       AND p.signal_key = s.signal_key
      GROUP BY s.token_ca, s.signal_key
    ),
    missed_ranked AS (
      SELECT
        m.token_ca,
        COALESCE(m.signal_ts, 0) AS signal_key,
        COALESCE(m.symbol, substr(m.token_ca, 1, 8), '?') AS missed_symbol,
        m.created_event_ts,
        m.pnl_5m,
        m.pnl_15m,
        m.pnl_60m,
        m.pnl_24h,
        COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_24h, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) AS max_pnl,
        COALESCE(m.tradable_missed, 0) AS tradable_missed,
        COALESCE(m.would_stop_before_peak, 0) AS would_stop_before_peak,
        m.tradability_status,
        ROW_NUMBER() OVER (
          PARTITION BY m.token_ca, COALESCE(m.signal_ts, 0)
          ORDER BY COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_24h, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                   m.created_event_ts DESC,
                   m.id DESC
        ) AS rn
      FROM paper_missed_signal_attribution m
      WHERE m.route = 'LOTTO'
        AND m.component IN ('upstream_gate', 'lotto_entry_gate')
        AND m.reject_reason = 'not_ath_v17'
        AND m.baseline_price IS NOT NULL
        ${missedWhereSql}
    ),
    missed_one AS (
      SELECT * FROM missed_ranked WHERE rn = 1
    ),
    feature_outcomes AS (
      SELECT
        f.*,
        COALESCE(m.missed_symbol, f.symbol, substr(f.token_ca, 1, 8), '?') AS outcome_symbol,
        m.created_event_ts,
        m.pnl_5m,
        m.pnl_15m,
        m.pnl_60m,
        m.pnl_24h,
        COALESCE(m.max_pnl, 0) AS max_pnl,
        COALESCE(m.tradable_missed, 0) AS tradable_missed,
        COALESCE(m.would_stop_before_peak, 0) AS would_stop_before_peak,
        m.tradability_status
      FROM snapshot_features f
      LEFT JOIN missed_one m
        ON m.token_ca = f.token_ca
       AND m.signal_key = f.signal_key
    ),
    cohort_members AS (
      SELECT 'two_snapshot_strict' AS cohort, * FROM feature_outcomes WHERE has_two_snapshot_pass = 1
      UNION ALL
      SELECT 'two_quote_clean_snapshots' AS cohort, * FROM feature_outcomes WHERE has_two_quote_clean = 1
      UNION ALL
      SELECT 'snapshot_pass_1_snapshot' AS cohort, * FROM feature_outcomes WHERE has_snapshot_pass_1 = 1
      UNION ALL
      SELECT 'quote_clean_1_snapshot' AS cohort, * FROM feature_outcomes WHERE has_quote_clean_1 = 1
      UNION ALL
      SELECT 'quote_clean_no_double_confirm' AS cohort, * FROM feature_outcomes WHERE has_quote_clean_1 = 1 AND has_two_snapshot_pass = 0
      UNION ALL
      SELECT 'quote_clean_activity_only' AS cohort, * FROM feature_outcomes WHERE has_quote_clean_activity = 1 AND has_two_snapshot_pass = 0
      UNION ALL
      SELECT 'relaxed_liquidity_floor' AS cohort, * FROM feature_outcomes WHERE has_relaxed_quote_clean = 1 AND has_quote_clean_1 = 0
      UNION ALL
      SELECT 'relaxed_age_window' AS cohort, * FROM feature_outcomes WHERE has_relaxed_age_signal = 1 AND has_two_snapshot_pass = 0
    )`;
}

export function buildNotAthRelaxedShadowCohorts(db, options = {}) {
  const sinceTs = Number(options.sinceTs || 0) || 0;
  const limit = Math.max(1, Math.min(100, Number(options.limit || 25) || 25));
  const params = {
    since: sinceTs,
    limit,
    snapshotIntervalSec: Number(options.snapshotIntervalSec || 300) || 300,
    strictConfirmBySec: Number(options.strictConfirmBySec || 1800) || 1800,
    relaxedLiquidityUsd: Number(options.relaxedLiquidityUsd || 2500) || 2500,
    maxQuoteGapPct: Number(options.maxQuoteGapPct || 8) || 8,
    maxSpreadPct: Number(options.maxSpreadPct || 5) || 5,
  };
  if (
    !hasTable(db, 'lotto_not_ath_watch_shadow_snapshots')
    || !hasTable(db, 'paper_missed_signal_attribution')
  ) {
    return {
      available: false,
      params,
      cohorts: [],
      top_hits: [],
      note: 'Requires lotto_not_ath_watch_shadow_snapshots and paper_missed_signal_attribution tables.',
    };
  }

  const cte = relaxedShadowCohortSql({ sinceTs });
  const cohorts = db.prepare(`
    ${cte}
    SELECT
      cohort,
      COUNT(*) AS candidates,
      COUNT(DISTINCT token_ca) AS unique_tokens,
      COALESCE(SUM(CASE WHEN tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END), 0) AS clean_tradable_n,
      COALESCE(SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END), 0) AS gold_n,
      COALESCE(SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END), 0) AS silver_n,
      COALESCE(SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END), 0) AS bronze_n,
      AVG(max_pnl) AS avg_max_pnl,
      MAX(max_pnl) AS max_pnl,
      AVG(snapshots) AS avg_snapshots,
      MAX(max_horizon_sec) AS max_horizon_sec
    FROM cohort_members
    GROUP BY cohort
  `).all(params)
    .map(addCohortDerivedFields)
    .sort((a, b) => a.order - b.order || String(a.cohort).localeCompare(String(b.cohort)));

  const topHits = db.prepare(`
    ${cte}
    SELECT
      cohort,
      outcome_symbol AS symbol,
      token_ca,
      NULLIF(signal_key, 0) AS signal_ts,
      created_event_ts,
      pnl_5m,
      pnl_15m,
      pnl_60m,
      pnl_24h,
      max_pnl,
      tradable_missed,
      would_stop_before_peak,
      tradability_status,
      snapshots,
      first_horizon_sec,
      max_horizon_sec,
      has_quote_clean_1,
      has_snapshot_pass_1,
      has_two_quote_clean,
      has_two_snapshot_pass,
      has_relaxed_quote_clean,
      has_relaxed_age_signal
    FROM cohort_members
    WHERE cohort != 'two_snapshot_strict'
    ORDER BY max_pnl DESC, created_event_ts DESC
    LIMIT @limit
  `).all(params);

  return {
    available: true,
    params,
    cohorts,
    top_hits: topHits,
    note: 'Observation only. These cohorts measure recall and outcome proxy; they do not create paper/live entries.',
  };
}
