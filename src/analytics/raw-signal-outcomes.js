const GOLD_PCT = 100;
const SILVER_PCT = 50;
const BRONZE_PCT = 25;

function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveNumeric(value) {
  const n = numeric(value);
  return n != null && n > 0 ? n : null;
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function normalizeTimestampSec(value) {
  const n = numeric(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function isoFromSec(sec) {
  const n = numeric(sec);
  return n == null ? null : new Date(n * 1000).toISOString();
}

function tierForPct(pct) {
  const n = numeric(pct);
  if (n == null) return 'unknown';
  if (n >= GOLD_PCT) return 'gold';
  if (n >= SILVER_PCT) return 'silver';
  if (n >= BRONZE_PCT) return 'bronze';
  return 'sub25';
}

function baselineConfidence(lagSec) {
  const lag = numeric(lagSec);
  if (lag == null || lag < 0) return 'not_evaluable';
  if (lag <= 10) return 'high';
  if (lag <= 30) return 'medium';
  if (lag <= 300) return 'low';
  return 'not_evaluable';
}

function confidenceEligible(confidence) {
  return confidence === 'high' || confidence === 'medium';
}

function median(values) {
  const xs = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!xs.length) return null;
  const mid = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
}

function countBy(rows, keyFn) {
  const out = {};
  for (const row of rows || []) {
    const key = String(keyFn(row) || 'unknown');
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function groupByToken(rows) {
  const out = new Map();
  for (const row of rows || []) {
    const tokenCa = String(row.token_ca || '').trim();
    if (!tokenCa) continue;
    if (!out.has(tokenCa)) out.set(tokenCa, []);
    out.get(tokenCa).push(row);
  }
  return out;
}

function uniqueTokenCount(rows) {
  return new Set((rows || []).map((row) => String(row.token_ca || '').trim()).filter(Boolean)).size;
}

function uniqueByTokenBest(rows, scoreFn = (row) => Number(row.max_sustained_peak_pct || 0)) {
  const byToken = new Map();
  for (const row of rows || []) {
    const token = String(row.token_ca || '').trim();
    if (!token) continue;
    const current = byToken.get(token);
    const currentScore = current ? Number(scoreFn(current) || 0) : -Infinity;
    const nextScore = Number(scoreFn(row) || 0);
    if (!current || nextScore > currentScore) byToken.set(token, row);
  }
  return [...byToken.values()];
}

function normalizeSignal(row) {
  const signalTs = normalizeTimestampSec(row.signal_ts ?? row.timestamp_sec ?? row.timestamp ?? row.receive_ts ?? row.created_ts);
  return {
    signal_id: row.signal_id ?? row.id ?? null,
    token_ca: row.token_ca || null,
    symbol: row.symbol || null,
    signal_ts: signalTs,
    lifecycle_id: row.lifecycle_id ?? row.downstream_lifecycle_id ?? null,
    signal_type: row.signal_type || row.route || null,
    route: row.route || row.signal_type || null,
    hard_gate_status: row.hard_gate_status || null,
    source: row.source || row.data_source || 'premium_signals',
  };
}

function normalizeKline(row) {
  const ts = normalizeTimestampSec(row.timestamp_sec ?? row.timestamp ?? row.ts ?? row.sample_ts);
  const provider = row.provider || row.source || 'kline_1m';
  const poolAddress = row.pool_address || row.pool || null;
  const sourceKind = row.source_kind
    || (String(poolAddress || '').toLowerCase().startsWith('bonding_curve:') ? 'bonding_curve' : null)
    || (String(provider || '').toLowerCase().includes('bonding') ? 'bonding_curve' : null)
    || (String(provider || '').toLowerCase().includes('helius') ? 'amm_pool' : 'indexed_ohlcv');
  return {
    token_ca: row.token_ca || null,
    pool_address: poolAddress,
    timestamp: ts,
    open: positiveNumeric(row.open),
    high: positiveNumeric(row.high),
    low: positiveNumeric(row.low),
    close: positiveNumeric(row.close),
    volume: numeric(row.volume),
    provider,
    source_kind: sourceKind,
    source_family: row.source_family || (sourceKind === 'indexed_ohlcv' ? 'third_party_kline' : 'onchain_swap'),
    price_unit: row.price_unit || 'native',
  };
}

function normalizeTrade(row) {
  return {
    id: row.id ?? row.trade_id ?? null,
    token_ca: row.token_ca || null,
    entry_ts: normalizeTimestampSec(row.entry_ts),
    exit_ts: normalizeTimestampSec(row.exit_ts),
    exit_reason: row.exit_reason || null,
    pnl_pct: numeric(row.pnl_pct),
    peak_pnl: numeric(row.peak_pnl ?? row.peak_quote_pnl_pct),
  };
}

function sourceCompatible(baseline, pathRows) {
  const pool = baseline?.pool_address || null;
  const provider = baseline?.provider || null;
  const sourceKind = baseline?.source_kind || null;
  const unit = baseline?.price_unit || null;
  for (const row of pathRows || []) {
    if (pool && row.pool_address && row.pool_address !== pool) {
      return { same_source_path: false, reason: 'cross_source_path' };
    }
    if (provider && row.provider && row.provider !== provider) {
      return { same_source_path: false, reason: 'cross_source_path' };
    }
    if (sourceKind && row.source_kind && row.source_kind !== sourceKind) {
      return { same_source_path: false, reason: 'cross_source_path' };
    }
    if (unit && row.price_unit && row.price_unit !== unit) {
      return { same_source_path: false, reason: 'price_unit_mismatch' };
    }
  }
  return { same_source_path: true, reason: 'covered' };
}

function pathPriority(row = {}) {
  const kind = String(row.source_kind || '').toLowerCase();
  const provider = String(row.provider || '').toLowerCase();
  if (kind === 'bonding_curve') return 0;
  if (kind === 'amm_pool') return 1;
  if (provider.includes('helius')) return 2;
  if (provider.includes('gmgn')) return 3;
  if (provider.includes('gecko')) return 4;
  if (provider.includes('dex')) return 5;
  return 10;
}

function streamKey(row = {}) {
  return [
    row.provider || '',
    row.source_kind || '',
    row.pool_address || '',
    row.price_unit || '',
  ].join('|');
}

function chooseAnchorCompatibleRows(rows = [], signalTs, {
  baselineMaxLagSec = 300,
  horizonSec = 7200,
} = {}) {
  if (signalTs == null || !rows.length) return rows;
  const normalized = rows
    .map(normalizeKline)
    .filter((row) => row.timestamp != null && row.high != null && row.low != null && row.close != null)
    .sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
  if (!normalized.length) return [];

  const byStream = new Map();
  for (const row of normalized) {
    const key = streamKey(row);
    if (!byStream.has(key)) byStream.set(key, []);
    byStream.get(key).push(row);
  }

  const candidates = [...byStream.values()].map((streamRows) => {
    const firstRow = streamRows[0] || {};
    const baseline = streamRows.find((row) => (
      row.timestamp >= signalTs
      && row.timestamp <= signalTs + baselineMaxLagSec
      && row.close != null
      && row.close > 0
    )) || null;
    const firstAfter = streamRows.find((row) => row.timestamp >= signalTs) || null;
    const horizonRows = streamRows.filter((row) => row.timestamp >= signalTs && row.timestamp <= signalTs + horizonSec);
    const earlyRows = streamRows.filter((row) => row.timestamp >= signalTs && row.timestamp < signalTs + 900);
    return {
      rows: streamRows,
      baseline,
      firstAfter,
      horizonRows,
      earlyRows,
      priority: pathPriority(firstRow),
    };
  });

  const withBaseline = candidates.filter((candidate) => candidate.baseline);
  if (withBaseline.length) {
    return withBaseline.sort((a, b) => (
      pathPriority(a.baseline) - pathPriority(b.baseline)
      || Number(a.baseline.timestamp) - Number(b.baseline.timestamp)
      || b.earlyRows.length - a.earlyRows.length
      || b.horizonRows.length - a.horizonRows.length
    ))[0].rows;
  }

  const withFuturePath = candidates.filter((candidate) => candidate.firstAfter);
  if (withFuturePath.length) {
    return withFuturePath.sort((a, b) => (
      Number(a.firstAfter.timestamp) - Number(b.firstAfter.timestamp)
      || a.priority - b.priority
      || b.horizonRows.length - a.horizonRows.length
    ))[0].rows;
  }

  return candidates.sort((a, b) => a.priority - b.priority || b.rows.length - a.rows.length)[0]?.rows || [];
}

function earlyWindowMetrics(pathRows, signalTs, earlyWindowSec = 900) {
  if (signalTs == null) {
    return {
      early_15m_bar_count: 0,
      early_15m_expected_minutes: Math.ceil(earlyWindowSec / 60),
      early_15m_bar_coverage_pct: null,
      early_15m_complete: false,
      first_bar_ts: null,
      first_bar_lag_sec: null,
    };
  }
  const expected = Math.ceil(earlyWindowSec / 60);
  const rows = (pathRows || []).filter((row) => (
    row.timestamp != null
    && row.timestamp >= signalTs
    && row.timestamp < signalTs + earlyWindowSec
  ));
  const minuteSet = new Set(rows.map((row) => Math.floor(Number(row.timestamp) / 60)));
  const firstAfter = (pathRows || [])
    .filter((row) => row.timestamp != null && row.timestamp >= signalTs)
    .sort((a, b) => Number(a.timestamp) - Number(b.timestamp))[0] || null;
  const coveragePct = expected ? (minuteSet.size / expected) * 100.0 : null;
  return {
    early_15m_bar_count: minuteSet.size,
    early_15m_expected_minutes: expected,
    early_15m_bar_coverage_pct: coveragePct == null ? null : roundNumber(coveragePct, 2),
    early_15m_complete: coveragePct != null && coveragePct >= 80,
    first_bar_ts: firstAfter?.timestamp ?? null,
    first_bar_lag_sec: firstAfter?.timestamp != null ? Math.max(0, firstAfter.timestamp - signalTs) : null,
  };
}

function windowPeakPct(pathRows, baselinePrice, signalTs, horizonSec) {
  if (!baselinePrice || baselinePrice <= 0 || signalTs == null) return null;
  const rows = pathRows.filter((row) => row.timestamp >= signalTs && row.timestamp <= signalTs + horizonSec);
  const high = rows.reduce((best, row) => (
    row.high != null && (best == null || row.high > best) ? row.high : best
  ), null);
  return high == null ? null : roundNumber(((high / baselinePrice) - 1) * 100.0, 2);
}

function evaluateSustainedPeak({ pathRows, baselinePrice, peakTs, peakPct }) {
  if (peakPct == null || peakPct < SILVER_PCT) {
    return {
      evaluable: true,
      tier: tierForPct(peakPct),
      pct: peakPct == null ? null : roundNumber(peakPct, 2),
      reason: 'below_silver_threshold',
    };
  }
  const afterPeak = pathRows.filter((row) => row.timestamp != null && peakTs != null && row.timestamp >= peakTs);
  if (afterPeak.length < 2) {
    return {
      evaluable: false,
      tier: 'unknown',
      pct: null,
      reason: 'insufficient_bars_after_peak',
    };
  }
  const pctForClose = (row) => row.close == null ? null : ((row.close / baselinePrice) - 1) * 100.0;
  const pctForHigh = (row) => row.high == null ? null : ((row.high / baselinePrice) - 1) * 100.0;
  const closeRetentionPct = peakPct >= GOLD_PCT ? 70 : 35;
  const consecutiveRetained = afterPeak.slice(0, 3).filter((row) => {
    const closePct = pctForClose(row);
    return closePct != null && closePct >= closeRetentionPct;
  }).length >= 2;
  const threeMinuteRows = afterPeak.filter((row) => row.timestamp <= peakTs + 180);
  const enoughThreeMinuteRows = threeMinuteRows.length >= 2;
  const immediateDump = enoughThreeMinuteRows && threeMinuteRows.some((row) => {
    const closePct = pctForClose(row);
    return closePct != null && closePct < 10;
  });
  const volumes = pathRows
    .filter((row) => row.timestamp != null && peakTs != null && row.timestamp <= peakTs)
    .map((row) => row.volume)
    .filter((value) => value != null && value > 0);
  const medianVolume = median(volumes);
  const peakRow = pathRows.find((row) => row.timestamp === peakTs);
  const peakVolume = peakRow?.volume != null && peakRow.volume > 0 ? peakRow.volume : null;
  const volumeConfirmed = medianVolume != null && medianVolume > 0 && peakVolume != null && peakVolume >= medianVolume * 3;
  const sustained = consecutiveRetained || volumeConfirmed || (enoughThreeMinuteRows && !immediateDump);
  if (!sustained) {
    return {
      evaluable: enoughThreeMinuteRows || volumeConfirmed || consecutiveRetained,
      tier: 'sub25',
      pct: null,
      reason: 'wick_only_no_sustained_confirmation',
      close_retention_pct: closeRetentionPct,
      median_volume: medianVolume,
      peak_volume: peakVolume,
    };
  }
  const sustainedPct = Math.max(
    ...afterPeak.map((row) => pctForHigh(row)).filter((value) => value != null && Number.isFinite(value))
  );
  return {
    evaluable: true,
    tier: tierForPct(sustainedPct),
    pct: roundNumber(sustainedPct, 2),
    reason: consecutiveRetained ? 'close_retention_confirmed'
      : volumeConfirmed ? 'volume_confirmed'
        : 'no_immediate_dump_confirmed',
    close_retention_pct: closeRetentionPct,
    median_volume: medianVolume,
    peak_volume: peakVolume,
  };
}

function computeOutcomeForSignal(signalRow, {
  klineRows = [],
  trades = [],
  nowTs = Math.floor(Date.now() / 1000),
  horizonSec = 7200,
  baselineMaxLagSec = 300,
} = {}) {
  const signal = normalizeSignal(signalRow);
  const base = {
    schema_version: 'raw_signal_outcome.v1',
    ...signal,
    signal_iso: isoFromSec(signal.signal_ts),
    horizon_sec: horizonSec,
    observation_status: 'not_evaluable',
    right_censored: false,
    matured_at_ts: signal.signal_ts == null ? null : signal.signal_ts + horizonSec,
    matured_at_iso: signal.signal_ts == null ? null : isoFromSec(signal.signal_ts + horizonSec),
    kline_covered: false,
    coverage_reason: 'unknown',
    pool_found: false,
    baseline_confidence: 'not_evaluable',
    same_source_path: false,
    source_kind: null,
    source_family: null,
    early_15m_bar_count: 0,
    early_15m_expected_minutes: 15,
    early_15m_bar_coverage_pct: null,
    early_15m_complete: false,
    first_bar_ts: null,
    first_bar_lag_sec: null,
    raw_wick_tier: 'unknown',
    raw_sustained_tier: 'unknown',
    raw_primary_tier: 'not_evaluable',
    sustained_evaluable: false,
    outlier_flag: false,
    outlier_reason: null,
    did_enter: false,
    raw_dog_entered: false,
    raw_dog_realized: false,
    entered_before_peak: false,
    held_to_silver: false,
    held_to_gold: false,
    sold_before_silver: false,
    sold_before_gold: false,
    exit_reason: null,
  };
  if (!signal.token_ca || signal.signal_ts == null) {
    return { ...base, coverage_reason: 'signal_anchor_missing' };
  }
  if (nowTs < signal.signal_ts + horizonSec) {
    return {
      ...base,
      observation_status: 'pending',
      right_censored: true,
      coverage_reason: 'right_censored_open',
    };
  }
  const rows = (klineRows || [])
    .map(normalizeKline)
    .filter((row) => row.timestamp != null && row.high != null && row.low != null && row.close != null)
    .sort((a, b) => a.timestamp - b.timestamp);
  if (!rows.length) {
    return { ...base, observation_status: 'matured', coverage_reason: 'no_kline_for_token' };
  }
  const baseline = rows.find((row) => (
    row.timestamp >= signal.signal_ts
    && row.timestamp <= signal.signal_ts + baselineMaxLagSec
    && row.close != null
    && row.close > 0
  ));
  if (!baseline) {
    const firstAfter = rows.find((row) => row.timestamp >= signal.signal_ts);
    return {
      ...base,
      observation_status: 'matured',
      coverage_reason: firstAfter ? 'baseline_after_max_lag' : 'no_kline_after_anchor',
      baseline_lag_sec: firstAfter?.timestamp != null ? Math.max(0, firstAfter.timestamp - signal.signal_ts) : null,
      baseline_ts: firstAfter?.timestamp ?? null,
      baseline_iso: isoFromSec(firstAfter?.timestamp),
    };
  }
  const pathRows = rows.filter((row) => row.timestamp >= baseline.timestamp && row.timestamp <= signal.signal_ts + horizonSec);
  const early = earlyWindowMetrics(pathRows, signal.signal_ts, 900);
  const lagSec = Math.max(0, baseline.timestamp - signal.signal_ts);
  const confidence = baselineConfidence(lagSec);
  const compatibility = sourceCompatible(baseline, pathRows);
  if (!pathRows.length) {
    return {
      ...base,
      observation_status: 'matured',
      coverage_reason: 'no_kline_in_horizon',
      baseline_ts: baseline.timestamp,
      baseline_iso: isoFromSec(baseline.timestamp),
      baseline_lag_sec: lagSec,
      baseline_confidence: confidence,
      baseline_price: baseline.close,
      ...early,
    };
  }

  let peakHigh = null;
  let peakTs = null;
  for (const row of pathRows) {
    if (row.high != null && (peakHigh == null || row.high > peakHigh)) {
      peakHigh = row.high;
      peakTs = row.timestamp;
    }
  }
  const wickPeakPct = peakHigh == null ? null : ((peakHigh / baseline.close) - 1) * 100.0;
  const beforePeak = peakTs == null ? pathRows : pathRows.filter((row) => row.timestamp <= peakTs);
  const minLow = beforePeak.reduce((best, row) => (
    row.low != null && (best == null || row.low < best) ? row.low : best
  ), null);
  const sustained = evaluateSustainedPeak({ pathRows, baselinePrice: baseline.close, peakTs, peakPct: wickPeakPct });
  const outlier = wickPeakPct != null && (wickPeakPct > 5000 || wickPeakPct < -99.9);
  const primaryTier = !compatibility.same_source_path || !confidenceEligible(confidence) || outlier
    ? 'not_evaluable'
    : sustained.evaluable
      ? sustained.tier
      : 'not_evaluable';
  const normalizedTrades = [...(trades || [])]
    .map(normalizeTrade)
    .sort((a, b) => Number(a.entry_ts || 0) - Number(b.entry_ts || 0));
  const enteredTrade = normalizedTrades.find((trade) => trade.entry_ts != null && trade.entry_ts <= (peakTs ?? signal.signal_ts + horizonSec));
  const bestTradePeak = normalizedTrades.reduce((best, trade) => {
    const peak = numeric(trade.peak_pnl);
    return peak != null && (best == null || peak > best) ? peak : best;
  }, null);
  const heldToSilver = bestTradePeak != null && bestTradePeak >= 0.5;
  const heldToGold = bestTradePeak != null && bestTradePeak >= 1.0;
  const rawDog = primaryTier === 'gold' || primaryTier === 'silver';
  const realized = primaryTier === 'gold' ? heldToGold : primaryTier === 'silver' ? heldToSilver : false;
  return {
    ...base,
    observation_status: 'matured',
    kline_covered: compatibility.same_source_path && confidenceEligible(confidence) && !outlier,
    coverage_reason: outlier ? 'outlier_price' : compatibility.reason,
    pool_found: Boolean(baseline.pool_address),
    provider: baseline.provider,
    source_kind: baseline.source_kind,
    source_family: baseline.source_family,
    baseline_ts: baseline.timestamp,
    baseline_iso: isoFromSec(baseline.timestamp),
    baseline_lag_sec: lagSec,
    baseline_price: baseline.close,
    baseline_source: baseline.provider,
    baseline_provider: baseline.provider,
    baseline_pool_address: baseline.pool_address,
    baseline_price_unit: baseline.price_unit,
    baseline_confidence: confidence,
    path_provider: baseline.provider,
    path_pool_address: baseline.pool_address,
    path_price_unit: baseline.price_unit,
    path_source_kind: baseline.source_kind,
    path_source_family: baseline.source_family,
    same_source_path: compatibility.same_source_path,
    ...early,
    max_wick_peak_pct: wickPeakPct == null ? null : roundNumber(wickPeakPct, 2),
    max_sustained_peak_pct: sustained.pct,
    time_to_wick_peak_sec: peakTs == null ? null : Math.max(0, peakTs - signal.signal_ts),
    time_to_sustained_peak_sec: sustained.pct == null ? null : Math.max(0, peakTs - signal.signal_ts),
    raw_wick_tier: tierForPct(wickPeakPct),
    raw_sustained_tier: sustained.evaluable ? sustained.tier : 'unknown',
    raw_primary_tier: primaryTier,
    sustained_evaluable: sustained.evaluable,
    sustained_reason: sustained.reason,
    peak_5m_pct: windowPeakPct(pathRows, baseline.close, signal.signal_ts, 300),
    peak_15m_pct: windowPeakPct(pathRows, baseline.close, signal.signal_ts, 900),
    peak_60m_pct: windowPeakPct(pathRows, baseline.close, signal.signal_ts, 3600),
    peak_120m_pct: windowPeakPct(pathRows, baseline.close, signal.signal_ts, 7200),
    mae_before_peak_pct: minLow == null ? null : roundNumber(((minLow / baseline.close) - 1) * 100.0, 2),
    outlier_flag: outlier,
    outlier_reason: outlier ? 'raw_wick_peak_outlier' : null,
    did_enter: Boolean(enteredTrade),
    paper_trade_id: enteredTrade?.id ?? null,
    entered_before_peak: Boolean(enteredTrade),
    held_to_silver: heldToSilver,
    held_to_gold: heldToGold,
    raw_dog_entered: rawDog && Boolean(enteredTrade),
    raw_dog_realized: rawDog && realized,
    sold_before_silver: rawDog && Boolean(enteredTrade) && !heldToSilver,
    sold_before_gold: primaryTier === 'gold' && Boolean(enteredTrade) && !heldToGold,
    exit_reason: enteredTrade?.exit_reason || normalizedTrades[0]?.exit_reason || null,
  };
}

function buildRawSignalOutcomeReport({
  signals = [],
  klineRows = [],
  paperTrades = [],
  nowTs = Math.floor(Date.now() / 1000),
  horizonSec = 7200,
  baselineMaxLagSec = 300,
  coverageTargetPct = 80,
} = {}) {
  const klineByToken = groupByToken(klineRows);
  const tradesByToken = groupByToken(paperTrades);
  const outcomes = signals.map((signal) => {
    const tokenCa = String(signal.token_ca || '').trim();
    const signalTs = normalizeTimestampSec(signal.timestamp_sec ?? signal.signal_ts ?? signal.timestamp ?? signal.created_ts);
    const anchorRows = chooseAnchorCompatibleRows(klineByToken.get(tokenCa) || [], signalTs, {
      baselineMaxLagSec,
      horizonSec,
    });
    return computeOutcomeForSignal(signal, {
      klineRows: anchorRows,
      trades: tradesByToken.get(tokenCa) || [],
      nowTs,
      horizonSec,
      baselineMaxLagSec,
    });
  });
  const matured = outcomes.filter((row) => row.observation_status === 'matured');
  const pending = outcomes.filter((row) => row.right_censored);
  const eligible = matured.filter((row) => (
    row.kline_covered
    && confidenceEligible(row.baseline_confidence)
    && row.same_source_path
    && !row.outlier_flag
    && row.sustained_evaluable
  ));
  const rawDogs = eligible.filter((row) => row.raw_primary_tier === 'gold' || row.raw_primary_tier === 'silver');
  const rawDogUniqueRows = uniqueByTokenBest(rawDogs);
  const rawGoldUniqueRows = uniqueByTokenBest(rawDogs.filter((row) => row.raw_primary_tier === 'gold'));
  const rawSilverUniqueRows = uniqueByTokenBest(rawDogs.filter((row) => row.raw_primary_tier === 'silver'));
  const entered = uniqueByTokenBest(rawDogs.filter((row) => row.raw_dog_entered));
  const realized = uniqueByTokenBest(rawDogs.filter((row) => row.raw_dog_realized));
  const wickGoldSilver = matured.filter((row) => row.raw_wick_tier === 'gold' || row.raw_wick_tier === 'silver');
  const wickOnlyGoldSilver = matured.filter((row) => (
    (row.raw_wick_tier === 'gold' || row.raw_wick_tier === 'silver')
    && !(row.raw_primary_tier === 'gold' || row.raw_primary_tier === 'silver')
  ));
  const coveragePct = matured.length ? (eligible.length / matured.length) * 100.0 : null;
  const earlyMatured = matured.filter((row) => row.early_15m_bar_coverage_pct != null);
  const earlyComplete = matured.filter((row) => row.early_15m_complete);
  const earlyAvgPct = earlyMatured.length
    ? earlyMatured.reduce((sum, row) => sum + Number(row.early_15m_bar_coverage_pct || 0), 0) / earlyMatured.length
    : null;
  const rawEnteredRate = rawDogUniqueRows.length ? entered.length / rawDogUniqueRows.length : null;
  const rawRealizedRate = rawDogUniqueRows.length ? realized.length / rawDogUniqueRows.length : null;
  let denominatorStatus = 'undefined';
  if (coveragePct != null && coveragePct < coverageTargetPct) denominatorStatus = 'evidence_unavailable';
  else if (rawDogUniqueRows.length > 0) denominatorStatus = 'evaluable';
  return {
    schema_version: 'raw_signal_discovery_report.v1',
    generated_at: new Date(nowTs * 1000).toISOString(),
    summary: {
      total_signals: outcomes.length,
      matured_signals: matured.length,
      pending_signals: pending.length,
      right_censored_open: pending.length,
      raw_denominator_matured_only: uniqueTokenCount(eligible),
      raw_denominator_event_rows: eligible.length,
      raw_kline_coverage_pct: coveragePct == null ? null : roundNumber(coveragePct, 2),
      early_15m_complete_event_rows: earlyComplete.length,
      early_15m_complete_pct: matured.length ? roundNumber((earlyComplete.length / matured.length) * 100.0, 2) : null,
      early_15m_bar_coverage_avg_pct: earlyAvgPct == null ? null : roundNumber(earlyAvgPct, 2),
      raw_sustained_gold_unique: rawGoldUniqueRows.length,
      raw_sustained_silver_unique: rawSilverUniqueRows.length,
      raw_sustained_gold_silver_unique: rawDogUniqueRows.length,
      raw_sustained_gold_silver_event_rows: rawDogs.length,
      raw_wick_gold_silver_unique: uniqueTokenCount(wickGoldSilver),
      raw_wick_gold_silver_event_rows: wickGoldSilver.length,
      raw_wick_only_gold_silver_unique: uniqueTokenCount(wickOnlyGoldSilver),
      raw_wick_only_gold_silver_event_rows: wickOnlyGoldSilver.length,
      raw_gold_silver_entered: entered.length,
      raw_gold_silver_realized: realized.length,
      raw_dog_entered_rate: rawEnteredRate == null ? null : roundNumber(rawEnteredRate, 4),
      raw_dog_realized_rate: rawRealizedRate == null ? null : roundNumber(rawRealizedRate, 4),
      denominator_status: denominatorStatus,
    },
    coverage: {
      by_reason: countBy(outcomes, (row) => row.coverage_reason),
      baseline_confidence_breakdown: countBy(outcomes, (row) => row.baseline_confidence),
      sustained_evaluable_breakdown: countBy(outcomes, (row) => row.sustained_evaluable ? 'evaluable' : 'not_evaluable'),
      by_signal_type: countBy(outcomes, (row) => row.signal_type),
      by_hard_gate_status: countBy(outcomes, (row) => row.hard_gate_status),
      by_path_source_kind: countBy(outcomes, (row) => row.path_source_kind || row.source_kind || 'none'),
    },
    top_raw_dogs: rawDogUniqueRows
      .sort((a, b) => Number(b.max_sustained_peak_pct || 0) - Number(a.max_sustained_peak_pct || 0))
      .slice(0, 50),
    missed_raw_dogs: rawDogUniqueRows
      .filter((row) => !row.raw_dog_realized)
      .sort((a, b) => Number(b.max_sustained_peak_pct || 0) - Number(a.max_sustained_peak_pct || 0))
      .slice(0, 50),
    coverage_gap_tokens: matured
      .filter((row) => !row.kline_covered)
      .sort((a, b) => String(a.coverage_reason).localeCompare(String(b.coverage_reason)))
      .slice(0, 50),
    pending_outcomes: pending.slice(0, 50),
    outcomes,
  };
}

export {
  baselineConfidence,
  buildRawSignalOutcomeReport,
  computeOutcomeForSignal,
  earlyWindowMetrics,
  tierForPct,
};
