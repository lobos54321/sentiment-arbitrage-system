const DEFAULT_EARLY_WINDOW_SEC = 900;
const DEFAULT_HORIZON_SEC = 7200;

function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveNumeric(value) {
  const n = numeric(value);
  return n != null && n > 0 ? n : null;
}

function normalizeTimestampSec(value) {
  const n = numeric(value);
  if (n == null) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function normalizeToken(value) {
  return value == null ? '' : String(value).trim();
}

function looksLikePumpFunMint(tokenCa) {
  return String(tokenCa || '').trim().toLowerCase().endsWith('pump');
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function signalKey(signal = {}) {
  const tokenCa = normalizeToken(signal.token_ca);
  const signalTs = normalizeTimestampSec(signal.signal_ts ?? signal.timestamp_sec ?? signal.timestamp ?? signal.created_ts);
  const signalId = signal.signal_id ?? signal.id ?? null;
  return {
    signal_id: signalId == null ? (tokenCa && signalTs != null ? `${tokenCa}:${signalTs}` : null) : String(signalId),
    token_ca: tokenCa || null,
    symbol: signal.symbol || null,
    signal_ts: signalTs,
  };
}

function normalizeSourceKind(value, provider = null, poolAddress = null) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw) return raw;
  const providerText = String(provider || '').toLowerCase();
  const poolText = String(poolAddress || '').toLowerCase();
  if (poolText.startsWith('bonding_curve:') || providerText.includes('bonding')) return 'bonding_curve';
  if (providerText === 'helius' || providerText.includes('swap')) return 'amm_pool';
  return 'indexed_ohlcv';
}

function normalizeProvider(value, sourceKind = null) {
  const provider = String(value || '').trim();
  const providerLower = provider.toLowerCase();
  if (sourceKind === 'bonding_curve' && (!provider || providerLower === 'helius')) return 'helius_bonding_curve';
  if (sourceKind === 'amm_pool' && (!provider || providerLower === 'helius')) return 'helius_amm_pool';
  if (provider) return provider;
  if (sourceKind === 'bonding_curve') return 'helius_bonding_curve';
  if (sourceKind === 'amm_pool') return 'helius_amm_pool';
  return 'kline_1m';
}

function normalizePoolAddress(value, tokenCa = null, sourceKind = null) {
  const pool = String(value || '').trim();
  if (pool) return pool;
  if (sourceKind === 'bonding_curve' && tokenCa) return `bonding_curve:${tokenCa}`;
  return null;
}

function normalizeRawPathBar(row = {}, defaults = {}) {
  const tokenCa = normalizeToken(row.token_ca ?? defaults.token_ca);
  const rawSourceKind = row.source_kind ?? defaults.source_kind
    ?? (!row.pool_address && !defaults.pool_address && looksLikePumpFunMint(tokenCa) ? 'bonding_curve' : null);
  const sourceKind = normalizeSourceKind(rawSourceKind, row.provider ?? defaults.provider, row.pool_address ?? defaults.pool_address);
  const provider = normalizeProvider(row.provider ?? row.source ?? defaults.provider, sourceKind);
  const poolAddress = normalizePoolAddress(row.pool_address ?? row.pool ?? defaults.pool_address, tokenCa, sourceKind);
  const timestamp = normalizeTimestampSec(row.timestamp ?? row.timestamp_sec ?? row.ts ?? row.sample_ts ?? defaults.timestamp);
  return {
    token_ca: tokenCa || null,
    pool_address: poolAddress,
    timestamp,
    open: positiveNumeric(row.open),
    high: positiveNumeric(row.high),
    low: positiveNumeric(row.low),
    close: positiveNumeric(row.close),
    volume: numeric(row.volume) ?? 0,
    provider,
    source_kind: sourceKind,
    source_family: row.source_family || defaults.source_family || (sourceKind === 'indexed_ohlcv' ? 'third_party_kline' : 'onchain_swap'),
    price_unit: row.price_unit || defaults.price_unit || 'native',
    trade_count: numeric(row.trade_count) ?? null,
    first_trade_ts: normalizeTimestampSec(row.first_trade_ts),
    last_trade_ts: normalizeTimestampSec(row.last_trade_ts),
    fetched_at: normalizeTimestampSec(row.fetched_at) ?? normalizeTimestampSec(defaults.fetched_at) ?? null,
    payload_json: row.payload_json || null,
  };
}

function normalizeTradeLike(row = {}, defaults = {}) {
  const tokenCa = normalizeToken(row.token_ca ?? row.tokenCa ?? defaults.token_ca);
  const rawSourceKind = row.source_kind ?? defaults.source_kind
    ?? (!row.pool_address && !row.poolAddress && !defaults.pool_address && looksLikePumpFunMint(tokenCa) ? 'bonding_curve' : null);
  const sourceKind = normalizeSourceKind(rawSourceKind, row.source ?? row.provider ?? defaults.provider, row.pool_address ?? row.poolAddress ?? defaults.pool_address);
  const provider = normalizeProvider(row.provider ?? row.source ?? defaults.provider, sourceKind);
  const poolAddress = normalizePoolAddress(row.pool_address ?? row.poolAddress ?? defaults.pool_address, tokenCa, sourceKind);
  const blockTime = normalizeTimestampSec(row.block_time ?? row.blockTime ?? row.timestamp ?? row.ts);
  return {
    signature: row.signature || row.id || null,
    slot: numeric(row.slot) ?? null,
    blockTime,
    tokenCa,
    token_ca: tokenCa || null,
    poolAddress,
    pool_address: poolAddress,
    price: positiveNumeric(row.price),
    volume: Math.abs(numeric(row.volume ?? row.quote_amount ?? row.quoteAmount) ?? 0),
    baseAmount: numeric(row.base_amount ?? row.baseAmount) ?? null,
    quoteAmount: numeric(row.quote_amount ?? row.quoteAmount) ?? null,
    side: row.side || null,
    provider,
    source_kind: sourceKind,
    source_family: sourceKind === 'indexed_ohlcv' ? 'third_party_kline' : 'onchain_swap',
  };
}

function pickTransferAmount(transfer) {
  return Math.abs(
    numeric(
      transfer?.tokenAmount
      ?? transfer?.rawTokenAmount?.uiAmount
      ?? transfer?.rawTokenAmount?.tokenAmount
      ?? transfer?.amount
    ) ?? 0
  );
}

function normalizeBondingCurveTransaction(tx = {}, { tokenCa } = {}) {
  const token = normalizeToken(tokenCa);
  const signature = tx.signature || tx.transactionError?.signature || null;
  const blockTime = normalizeTimestampSec(tx.timestamp ?? tx.blockTime);
  if (!token || !signature || !blockTime || tx.transactionError || tx.meta?.err) return null;

  const tokenTransfers = Array.isArray(tx.tokenTransfers) ? tx.tokenTransfers : [];
  const nativeTransfers = Array.isArray(tx.nativeTransfers) ? tx.nativeTransfers : [];
  const baseTransfer = tokenTransfers
    .filter((transfer) => transfer?.mint === token)
    .map((transfer) => ({ transfer, amount: pickTransferAmount(transfer) }))
    .sort((a, b) => b.amount - a.amount)[0] || null;
  if (!baseTransfer?.amount) return null;

  const nativeLamports = nativeTransfers
    .map((transfer) => Math.abs(numeric(transfer?.amount) ?? 0))
    .filter((amount) => amount > 0)
    .sort((a, b) => b - a)[0] || 0;
  const quoteAmount = nativeLamports / 1e9;
  const price = quoteAmount > 0 ? quoteAmount / baseTransfer.amount : 0;
  if (!Number.isFinite(price) || price <= 0) return null;

  return {
    signature,
    slot: numeric(tx.slot) ?? null,
    blockTime,
    tokenCa: token,
    token_ca: token,
    poolAddress: `bonding_curve:${token}`,
    pool_address: `bonding_curve:${token}`,
    price,
    baseAmount: baseTransfer.amount,
    quoteAmount,
    volume: quoteAmount,
    side: null,
    provider: 'helius_bonding_curve',
    source: 'helius_bonding_curve',
    source_kind: 'bonding_curve',
    source_family: 'onchain_swap',
  };
}

function normalizeBondingCurveTransactions(transactions = [], { tokenCa } = {}) {
  return transactions
    .map((tx) => normalizeBondingCurveTransaction(tx, { tokenCa }))
    .filter(Boolean);
}

function aggregateSwapsToRawPriceBars(trades = [], defaults = {}) {
  const buckets = new Map();
  for (const trade of trades.map((row) => normalizeTradeLike(row, defaults))) {
    if (!trade.token_ca || trade.blockTime == null || trade.price == null || trade.price <= 0) continue;
    const minuteTs = Math.floor(trade.blockTime / 60) * 60;
    const key = [
      trade.token_ca,
      trade.pool_address || '',
      minuteTs,
      trade.provider || '',
      trade.source_kind || '',
      defaults.price_unit || 'native',
    ].join('|');
    const existing = buckets.get(key);
    if (!existing) {
      buckets.set(key, {
        token_ca: trade.token_ca,
        pool_address: trade.pool_address,
        timestamp: minuteTs,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        close: trade.price,
        volume: Math.abs(trade.volume || 0),
        provider: trade.provider,
        source_kind: trade.source_kind,
        source_family: trade.source_family,
        price_unit: defaults.price_unit || 'native',
        trade_count: 1,
        first_trade_ts: trade.blockTime,
        last_trade_ts: trade.blockTime,
        fetched_at: normalizeTimestampSec(defaults.fetched_at) ?? Math.floor(Date.now() / 1000),
        payload_json: null,
      });
      continue;
    }
    existing.high = Math.max(existing.high, trade.price);
    existing.low = Math.min(existing.low, trade.price);
    existing.close = trade.price;
    existing.volume += Math.abs(trade.volume || 0);
    existing.trade_count += 1;
    existing.first_trade_ts = Math.min(existing.first_trade_ts, trade.blockTime);
    existing.last_trade_ts = Math.max(existing.last_trade_ts, trade.blockTime);
  }
  return [...buckets.values()].sort((a, b) => (
    String(a.token_ca).localeCompare(String(b.token_ca))
    || Number(a.timestamp) - Number(b.timestamp)
    || String(a.provider).localeCompare(String(b.provider))
  ));
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

function groupRowsByToken(rows = []) {
  const out = new Map();
  for (const row of rows) {
    const normalized = normalizeRawPathBar(row);
    if (!normalized.token_ca || normalized.timestamp == null) continue;
    if (!out.has(normalized.token_ca)) out.set(normalized.token_ca, []);
    out.get(normalized.token_ca).push(normalized);
  }
  return out;
}

function choosePreferredRowsForToken(rawRows = [], klineRows = []) {
  const candidates = rawRows.length ? rawRows : klineRows;
  if (!candidates.length) return [];
  const byStream = new Map();
  for (const row of candidates) {
    const streamKey = [
      row.provider || '',
      row.source_kind || '',
      row.pool_address || '',
      row.price_unit || '',
    ].join('|');
    if (!byStream.has(streamKey)) byStream.set(streamKey, []);
    byStream.get(streamKey).push(row);
  }
  return [...byStream.values()]
    .sort((a, b) => {
      const a0 = a[0] || {};
      const b0 = b[0] || {};
      return pathPriority(a0) - pathPriority(b0)
        || b.length - a.length
        || String(a0.provider || '').localeCompare(String(b0.provider || ''));
    })[0]
    .sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
}

function mergePreferredPathRows({ signals = [], rawPathRows = [], klineRows = [] } = {}) {
  const rawByToken = groupRowsByToken(rawPathRows);
  const klineByToken = groupRowsByToken(klineRows);
  const tokens = new Set(signals.map((signal) => normalizeToken(signal.token_ca)).filter(Boolean));
  if (!tokens.size) {
    for (const token of rawByToken.keys()) tokens.add(token);
    for (const token of klineByToken.keys()) tokens.add(token);
  }
  const rows = [];
  const decisions = {};
  for (const token of tokens) {
    const rawRows = rawByToken.get(token) || [];
    const legacyRows = klineByToken.get(token) || [];
    const chosen = choosePreferredRowsForToken(rawRows, legacyRows);
    rows.push(...chosen);
    decisions[token] = {
      token_ca: token,
      source: rawRows.length ? 'raw_price_bars_1m' : (legacyRows.length ? 'legacy_kline_1m' : 'none'),
      rows: chosen.length,
      provider: chosen[0]?.provider || null,
      source_kind: chosen[0]?.source_kind || null,
      pool_address: chosen[0]?.pool_address || null,
    };
  }
  return { rows, decisions };
}

function earlyWindowMetrics(pathRows = [], signalTs, earlyWindowSec = DEFAULT_EARLY_WINDOW_SEC) {
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
  const rows = (pathRows || [])
    .filter((row) => row.timestamp != null && row.timestamp >= signalTs && row.timestamp < signalTs + earlyWindowSec)
    .sort((a, b) => a.timestamp - b.timestamp);
  const minuteSet = new Set(rows.map((row) => Math.floor(Number(row.timestamp) / 60)));
  const firstAfter = (pathRows || [])
    .filter((row) => row.timestamp != null && row.timestamp >= signalTs)
    .sort((a, b) => a.timestamp - b.timestamp)[0] || null;
  const pct = expected > 0 ? (minuteSet.size / expected) * 100.0 : null;
  return {
    early_15m_bar_count: minuteSet.size,
    early_15m_expected_minutes: expected,
    early_15m_bar_coverage_pct: pct == null ? null : roundNumber(pct, 2),
    early_15m_complete: pct != null && pct >= 80,
    first_bar_ts: firstAfter?.timestamp ?? null,
    first_bar_lag_sec: firstAfter?.timestamp != null ? Math.max(0, Number(firstAfter.timestamp) - Number(signalTs)) : null,
  };
}

function buildRawSignalObservations({
  signals = [],
  pathRows = [],
  nowTs = Math.floor(Date.now() / 1000),
  horizonSec = DEFAULT_HORIZON_SEC,
  earlyWindowSec = DEFAULT_EARLY_WINDOW_SEC,
} = {}) {
  const rowsByToken = groupRowsByToken(pathRows);
  return (signals || []).map((signalRow) => {
    const signal = signalKey(signalRow);
    const rows = (rowsByToken.get(signal.token_ca) || [])
      .filter((row) => signal.signal_ts == null || row.timestamp >= signal.signal_ts)
      .sort((a, b) => a.timestamp - b.timestamp);
    const maturedAt = signal.signal_ts == null ? null : signal.signal_ts + horizonSec;
    const rightCensored = signal.signal_ts != null && nowTs < maturedAt;
    const early = earlyWindowMetrics(rows, signal.signal_ts, earlyWindowSec);
    let coverageReason = 'covered';
    if (!signal.token_ca || signal.signal_ts == null) coverageReason = 'signal_anchor_missing';
    else if (rightCensored) coverageReason = 'right_censored_open';
    else if (!rows.length) coverageReason = 'no_raw_path_for_token';
    else if (early.first_bar_ts == null) coverageReason = 'no_raw_path_after_anchor';
    else if (early.first_bar_lag_sec > earlyWindowSec) coverageReason = 'raw_path_after_early_window';
    return {
      schema_version: 'raw_signal_observation.v1',
      ...signal,
      horizon_sec: horizonSec,
      status: rightCensored ? 'pending' : 'matured',
      right_censored: rightCensored,
      matured_at_ts: maturedAt,
      source_kind: rows[0]?.source_kind || null,
      provider: rows[0]?.provider || null,
      pool_address: rows[0]?.pool_address || null,
      path_row_count: rows.length,
      first_bar_ts: early.first_bar_ts,
      first_bar_lag_sec: early.first_bar_lag_sec,
      early_15m_bar_count: early.early_15m_bar_count,
      early_15m_expected_minutes: early.early_15m_expected_minutes,
      early_15m_bar_coverage_pct: early.early_15m_bar_coverage_pct,
      early_15m_complete: early.early_15m_complete,
      coverage_reason: coverageReason,
      updated_at: nowTs,
    };
  });
}

function summarizeRawPathDiagnostics({ signals = [], rawPathRows = [], klineRows = [], preferredRows = [], observations = [], decisions = {} } = {}) {
  const countBy = (rows, keyFn) => {
    const out = {};
    for (const row of rows || []) {
      const key = String(keyFn(row) || 'unknown');
      out[key] = (out[key] || 0) + 1;
    }
    return out;
  };
  const matured = observations.filter((row) => row.status === 'matured');
  const complete = matured.filter((row) => row.early_15m_complete);
  const avgEarlyCoverage = matured.length
    ? matured.reduce((sum, row) => sum + Number(row.early_15m_bar_coverage_pct || 0), 0) / matured.length
    : null;
  return {
    schema_version: 'raw_path_diagnostics.v1',
    signal_rows: signals.length,
    raw_path_rows: rawPathRows.length,
    legacy_kline_rows: klineRows.length,
    preferred_path_rows: preferredRows.length,
    preferred_tokens: new Set(preferredRows.map((row) => row.token_ca).filter(Boolean)).size,
    raw_path_tokens: new Set(rawPathRows.map((row) => row.token_ca).filter(Boolean)).size,
    legacy_kline_tokens: new Set(klineRows.map((row) => row.token_ca).filter(Boolean)).size,
    preferred_by_source_kind: countBy(preferredRows, (row) => row.source_kind),
    preferred_by_provider: countBy(preferredRows, (row) => row.provider),
    token_decision_by_source: countBy(Object.values(decisions), (row) => row.source),
    early_15m: {
      matured_observations: matured.length,
      complete_observations: complete.length,
      complete_pct: matured.length ? roundNumber((complete.length / matured.length) * 100.0, 2) : null,
      avg_bar_coverage_pct: avgEarlyCoverage == null ? null : roundNumber(avgEarlyCoverage, 2),
      by_reason: countBy(observations, (row) => row.coverage_reason),
    },
  };
}

function ensureRawPathObserverSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS raw_price_bars_1m (
      token_ca TEXT NOT NULL,
      pool_address TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume REAL DEFAULT 0,
      provider TEXT NOT NULL,
      source_kind TEXT NOT NULL,
      source_family TEXT,
      price_unit TEXT DEFAULT 'native',
      trade_count INTEGER,
      first_trade_ts INTEGER,
      last_trade_ts INTEGER,
      fetched_at INTEGER,
      payload_json TEXT,
      created_at INTEGER DEFAULT (strftime('%s', 'now')),
      updated_at INTEGER DEFAULT (strftime('%s', 'now')),
      PRIMARY KEY (token_ca, pool_address, timestamp, provider, source_kind, price_unit)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_price_bars_token_time
      ON raw_price_bars_1m(token_ca, timestamp);
    CREATE INDEX IF NOT EXISTS idx_raw_price_bars_source
      ON raw_price_bars_1m(source_kind, provider, timestamp);

    CREATE TABLE IF NOT EXISTS raw_signal_observations (
      signal_id TEXT,
      token_ca TEXT NOT NULL,
      symbol TEXT,
      signal_ts INTEGER NOT NULL,
      horizon_sec INTEGER,
      status TEXT,
      right_censored INTEGER DEFAULT 0,
      matured_at_ts INTEGER,
      source_kind TEXT,
      provider TEXT,
      pool_address TEXT,
      path_row_count INTEGER DEFAULT 0,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER DEFAULT 0,
      early_15m_expected_minutes INTEGER DEFAULT 15,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER DEFAULT 0,
      coverage_reason TEXT,
      payload_json TEXT,
      created_at INTEGER DEFAULT (strftime('%s', 'now')),
      updated_at INTEGER DEFAULT (strftime('%s', 'now')),
      PRIMARY KEY (signal_id, token_ca, signal_ts)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_signal_observations_window
      ON raw_signal_observations(signal_ts, status, coverage_reason);
    CREATE INDEX IF NOT EXISTS idx_raw_signal_observations_token
      ON raw_signal_observations(token_ca, signal_ts);
  `);
}

export {
  DEFAULT_EARLY_WINDOW_SEC,
  aggregateSwapsToRawPriceBars,
  buildRawSignalObservations,
  earlyWindowMetrics,
  ensureRawPathObserverSchema,
  mergePreferredPathRows,
  normalizeBondingCurveTransaction,
  normalizeBondingCurveTransactions,
  normalizeRawPathBar,
  summarizeRawPathDiagnostics,
};
