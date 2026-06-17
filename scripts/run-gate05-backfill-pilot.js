#!/usr/bin/env node
'use strict';

/**
 * Gate-0.5 Step-1 historical backfill feasibility pilot.
 *
 * This is a feasibility tool, not an edge test. It prepares a burned pilot
 * sample from historical premium_signals and, once provider bars are supplied,
 * reuses src/analytics/raw-signal-outcomes.js to compute backfill labels and
 * reconcile them against observer labels in the 2026-06-06..06-07 overlap.
 *
 * It intentionally does not query Dune/Gecko itself in v1. Provider spend is
 * driven by the emitted windows file, then the returned 1m bars are supplied via
 * --bars-jsonl. This keeps the sampling/burned-key discipline auditable before
 * any paid fetch happens.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';

import { buildRawSignalOutcomeReport } from '../src/analytics/raw-signal-outcomes.js';

const REPO = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const DEFAULT_RUN_CARD = path.join(REPO, 'claudedocs/gate0.5-step1-pilot-run-card-2026-06-17.md');
const DEFAULT_LIMIT = 200;
const DEFAULT_OVERLAP_START = Date.parse('2026-06-06T00:00:00Z') / 1000;
const DEFAULT_OVERLAP_END = Date.parse('2026-06-08T00:00:00Z') / 1000;
const DEFAULT_HISTORY_START = Date.parse('2026-04-01T00:00:00Z') / 1000;
const DEFAULT_HISTORY_END = Date.parse('2026-06-06T00:00:00Z') / 1000;
const MAY_METADATA_START = Date.parse('2026-05-01T00:00:00Z') / 1000;
const MIN_PREMIUM_SIGNAL_ROWS = 30_000;
const HORIZON_SEC = 7200;
const RECONCILE_PASS = 0.90;
const RECONCILE_PARTIAL = 0.80;
const STAGE_PASS = 0.70;
const STAGE_PARTIAL = 0.50;
const DOG_TIERS = new Set(['gold', 'silver']);
const DUD_TIERS = new Set(['bronze', 'sub25']);
const FORBIDDEN_STRATIFICATION_FIELDS = ['signal_type', 'is_ath', 'narrative_score', 'raw_message'];
const ALLOWED_STRATIFICATION_FIELDS = ['month', 'date', 'token_age_proxy', 'preliminary_domain_proxy', 'provider_availability_proxy'];
const FORBIDDEN_REPORT_KEYS = new Set(['auc', 'precision', 'recall', 'lift', 'signal_type_dog_rate', 'is_ath_dog_rate', 'narrative_score_split']);
const REPRODUCIBLE_OBSERVER_PROVIDERS = new Set(['geckoterminal', 'gmgn']);
const PREMIUM_SIGNAL_TS_EXPR = "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE timestamp END";

function die(message) {
  console.error(`run-gate05-backfill-pilot: ${message}`);
  process.exit(2);
}

function sha256File(filePath) {
  return fs.existsSync(filePath)
    ? crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex')
    : null;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n');
}

function normalizeTs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function iso(sec) {
  const n = normalizeTs(sec);
  return n == null ? null : new Date(n * 1000).toISOString();
}

function keyOf(row) {
  return `${row.token_ca}|${Math.floor(Number(row.signal_ts))}`;
}

function monthOf(ts) {
  return new Date(Number(ts) * 1000).toISOString().slice(0, 7);
}

function dayOf(ts) {
  return new Date(Number(ts) * 1000).toISOString().slice(0, 10);
}

function tierClass(tier) {
  const t = String(tier || '').toLowerCase();
  if (DOG_TIERS.has(t)) return 'dog';
  if (DUD_TIERS.has(t)) return 'dud';
  return 'unknown';
}

function pct(n, d, digits = 4) {
  if (!d) return null;
  const factor = 10 ** digits;
  return Math.round((Number(n) / Number(d)) * factor) / factor;
}

function countBy(rows, fn) {
  const out = {};
  for (const row of rows || []) {
    const key = String(fn(row) ?? 'unknown');
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function hasForbiddenKey(obj) {
  if (!obj || typeof obj !== 'object') return null;
  for (const key of Object.keys(obj)) {
    if (FORBIDDEN_REPORT_KEYS.has(String(key))) return String(key);
  }
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = hasForbiddenKey(item);
      if (found) return found;
    }
    return null;
  }
  for (const value of Object.values(obj)) {
    const found = hasForbiddenKey(value);
    if (found) return found;
  }
  return null;
}

function tableColumns(db, tableName) {
  return db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name);
}

function requireColumns(columns, required, tableName) {
  const missing = required.filter((name) => !columns.includes(name));
  if (missing.length) die(`${tableName} missing required columns: ${missing.join(', ')}`);
}

function optionalSelect(columns, name, alias = name) {
  return columns.includes(name) ? `${name} AS ${alias}` : `NULL AS ${alias}`;
}

function openDb(dbPath) {
  if (!dbPath) die('database path required');
  if (!fs.existsSync(dbPath)) die(`database not found: ${dbPath}`);
  return new Database(dbPath, { readonly: true, fileMustExist: true });
}

function loadPremiumSignals(premiumDbPath, {
  historyStart = DEFAULT_HISTORY_START,
  historyEnd = DEFAULT_HISTORY_END,
  overlapStart = DEFAULT_OVERLAP_START,
  overlapEnd = DEFAULT_OVERLAP_END,
  allowSmallPremiumDbForSmoke = false,
} = {}) {
  const db = openDb(premiumDbPath);
  const cols = tableColumns(db, 'premium_signals');
  const required = ['token_ca', 'timestamp', 'signal_type', 'is_ath', 'raw_message'];
  const includesMayOrLater = Math.max(historyEnd, overlapEnd) > MAY_METADATA_START;
  if (includesMayOrLater) required.push('narrative_score');
  requireColumns(cols, required, 'premium_signals');
  const totalRows = db.prepare('SELECT COUNT(*) AS n FROM premium_signals').get().n;
  if (!allowSmallPremiumDbForSmoke && totalRows < MIN_PREMIUM_SIGNAL_ROWS) {
    db.close();
    die(`premium_signals DB identity guard failed: row_count=${totalRows} < ${MIN_PREMIUM_SIGNAL_ROWS}. `
      + 'Pass the real sas_sentiment_current.db, not server_sentiment_arb.db. '
      + 'Use --allow-small-premium-db-for-smoke only for synthetic tests.');
  }
  const signalTypeRowsExpr = cols.includes('signal_type')
    ? "SUM(CASE WHEN signal_type IS NOT NULL AND signal_type <> '' THEN 1 ELSE 0 END)"
    : '0';
  const isAthRowsExpr = cols.includes('is_ath')
    ? 'SUM(CASE WHEN is_ath IS NOT NULL THEN 1 ELSE 0 END)'
    : '0';
  const narrativeRowsExpr = cols.includes('narrative_score')
    ? 'SUM(CASE WHEN narrative_score IS NOT NULL THEN 1 ELSE 0 END)'
    : '0';
  const rawMessageRowsExpr = cols.includes('raw_message')
    ? "SUM(CASE WHEN raw_message IS NOT NULL AND raw_message <> '' THEN 1 ELSE 0 END)"
    : '0';
  const monthInventory = db.prepare(`
    SELECT
      substr(datetime(${PREMIUM_SIGNAL_TS_EXPR}, 'unixepoch'), 1, 7) AS month,
      COUNT(*) AS rows,
      COUNT(DISTINCT token_ca) AS tokens,
      ${signalTypeRowsExpr} AS signal_type_rows,
      ${isAthRowsExpr} AS is_ath_rows,
      ${narrativeRowsExpr} AS narrative_score_rows,
      ${rawMessageRowsExpr} AS raw_message_rows
    FROM premium_signals
    GROUP BY month
    ORDER BY month
  `).all();
  const rows = db.prepare(`
    SELECT
      ${optionalSelect(cols, 'id', 'premium_id')},
      token_ca,
      ${optionalSelect(cols, 'symbol')},
      ${PREMIUM_SIGNAL_TS_EXPR} AS signal_ts,
      timestamp AS raw_timestamp,
      ${optionalSelect(cols, 'created_at')},
      ${optionalSelect(cols, 'signal_type')},
      ${optionalSelect(cols, 'is_ath')},
      ${optionalSelect(cols, 'narrative_score')},
      ${optionalSelect(cols, 'ai_narrative_tier')},
      ${optionalSelect(cols, 'raw_message')},
      ${optionalSelect(cols, 'source_message_ts')},
      ${optionalSelect(cols, 'receive_ts')},
      ${optionalSelect(cols, 'signal_source')},
      ${optionalSelect(cols, 'source_event_id')},
      ${optionalSelect(cols, 'market_cap')},
      ${optionalSelect(cols, 'volume_24h')},
      ${optionalSelect(cols, 'age')}
    FROM premium_signals
    WHERE token_ca IS NOT NULL
      AND timestamp IS NOT NULL
      AND ${PREMIUM_SIGNAL_TS_EXPR} >= ?
      AND ${PREMIUM_SIGNAL_TS_EXPR} < ?
    ORDER BY signal_ts ASC, token_ca ASC
  `).all(Math.min(historyStart, overlapStart), Math.max(historyEnd, overlapEnd));
  db.close();

  const dedup = new Map();
  let duplicateRows = 0;
  for (const row of rows) {
    const signalTs = normalizeTs(row.signal_ts);
    const token = String(row.token_ca || '').trim();
    if (!token || signalTs == null) continue;
    const key = `${token}|${signalTs}`;
    if (dedup.has(key)) {
      duplicateRows += 1;
      continue;
    }
    dedup.set(key, {
      premium_id: row.premium_id ?? null,
      token_ca: token,
      symbol: row.symbol || null,
      signal_ts: signalTs,
      signal_iso: iso(signalTs),
      month: monthOf(signalTs),
      day: dayOf(signalTs),
      signal_type: row.signal_type || null,
      is_ath: row.is_ath == null ? null : Number(row.is_ath),
      narrative_score: row.narrative_score == null ? null : Number(row.narrative_score),
      ai_narrative_tier: row.ai_narrative_tier || null,
      raw_message_present: Boolean(row.raw_message),
      source_message_ts: normalizeTs(row.source_message_ts),
      receive_ts: normalizeTs(row.receive_ts),
      signal_source: row.signal_source || null,
      source_event_id: row.source_event_id || null,
      market_cap: row.market_cap == null ? null : Number(row.market_cap),
      volume_24h: row.volume_24h == null ? null : Number(row.volume_24h),
      age: row.age || null,
    });
  }
  return {
    rows: [...dedup.values()],
    duplicateRows,
    identity: {
      row_count: totalRows,
      required_columns: required,
      month_inventory: monthInventory,
      timestamp_normalization: 'timestamp_ms_if_gt_1e12_else_unix_seconds',
      allow_small_db_for_smoke: Boolean(allowSmallPremiumDbForSmoke),
    },
  };
}

function loadObserverRows(observerDbPath, {
  overlapStart = DEFAULT_OVERLAP_START,
  overlapEnd = DEFAULT_OVERLAP_END,
} = {}) {
  const db = openDb(observerDbPath);
  const cols = tableColumns(db, 'raw_signal_outcomes');
  requireColumns(cols, ['token_ca', 'signal_ts', 'raw_primary_tier'], 'raw_signal_outcomes');
  const rows = db.prepare(`
    SELECT
      token_ca,
      signal_ts,
      raw_primary_tier,
      raw_sustained_tier,
      max_sustained_peak_pct,
      baseline_price,
      baseline_ts,
      baseline_lag_sec,
      baseline_price_unit,
      baseline_confidence,
      peak_120m_pct,
      time_to_sustained_peak_sec,
      path_source_kind,
      path_source_family,
      path_provider,
      sustained_reason,
      coverage_reason,
      kline_covered,
      same_source_path,
      sustained_evaluable,
      observation_status,
      outlier_flag
    FROM raw_signal_outcomes
    WHERE signal_ts >= ?
      AND signal_ts < ?
      AND raw_primary_tier IN ('gold','silver','bronze','sub25')
    ORDER BY signal_ts ASC, token_ca ASC
  `).all(overlapStart, overlapEnd);
  db.close();
  const byKey = new Map();
  for (const row of rows) {
    byKey.set(keyOf(row), row);
  }
  return { rows, byKey };
}

function labelabilityBucket(row) {
  if (row.market_cap != null && row.market_cap > 0) {
    if (row.market_cap < 50_000) return 'mc_lt_50k';
    if (row.market_cap < 250_000) return 'mc_50k_250k';
    return 'mc_gte_250k';
  }
  return 'mc_unknown';
}

function selectPilotSample(premiumRows, observerByKey, {
  limit = DEFAULT_LIMIT,
  historyStart = DEFAULT_HISTORY_START,
  historyEnd = DEFAULT_HISTORY_END,
  overlapStart = DEFAULT_OVERLAP_START,
  overlapEnd = DEFAULT_OVERLAP_END,
} = {}) {
  const hardLimit = Math.min(DEFAULT_LIMIT, Math.max(1, Number(limit || DEFAULT_LIMIT)));
  const overlap = premiumRows
    .filter((row) => row.signal_ts >= overlapStart && row.signal_ts < overlapEnd && observerByKey.has(keyOf(row)))
    .sort((a, b) => a.signal_ts - b.signal_ts || a.token_ca.localeCompare(b.token_ca));
  const selected = [];
  const seen = new Set();
  const take = (row, reason) => {
    if (selected.length >= hardLimit) return;
    const key = keyOf(row);
    if (seen.has(key)) return;
    seen.add(key);
    selected.push({ ...row, pilot_source: reason, has_observer_label: observerByKey.has(key) });
  };
  for (const row of overlap) take(row, 'overlap_reconciliation');

  if (selected.length < hardLimit) {
    const filler = premiumRows
      .filter((row) => row.signal_ts >= historyStart && row.signal_ts < historyEnd)
      .filter((row) => row.signal_type != null || row.raw_message_present || row.narrative_score != null)
      .map((row) => ({ ...row, labelability_bucket: labelabilityBucket(row) }))
      .sort((a, b) => a.month.localeCompare(b.month)
        || a.day.localeCompare(b.day)
        || a.labelability_bucket.localeCompare(b.labelability_bucket)
        || a.token_ca.localeCompare(b.token_ca)
        || a.signal_ts - b.signal_ts);
    for (const row of filler) take(row, 'historical_labelability_filler');
  }

  return {
    rows: selected,
    stats: {
      limit: hardLimit,
      selected: selected.length,
      overlap_selected: selected.filter((row) => row.pilot_source === 'overlap_reconciliation').length,
      filler_selected: selected.filter((row) => row.pilot_source === 'historical_labelability_filler').length,
      selected_by_month: countBy(selected, (row) => row.month),
      selected_by_source: countBy(selected, (row) => row.pilot_source),
      stratification_fields: ALLOWED_STRATIFICATION_FIELDS,
      forbidden_stratification_fields: FORBIDDEN_STRATIFICATION_FIELDS,
    },
  };
}

function normalizeBar(row) {
  return {
    token_ca: String(row.token_ca || row.mint || row.token_mint || '').trim(),
    timestamp: normalizeTs(row.timestamp ?? row.timestamp_sec ?? row.minute_ts ?? row.block_minute_ts ?? row.ts),
    open: row.open == null ? null : Number(row.open),
    high: row.high == null ? null : Number(row.high),
    low: row.low == null ? null : Number(row.low),
    close: row.close == null ? null : Number(row.close),
    volume: row.volume == null ? null : Number(row.volume),
    provider: row.provider || row.source || 'historical_backfill',
    source_kind: row.source_kind || 'indexed_ohlcv',
    source_family: row.source_family || (row.source_kind === 'bonding_curve' ? 'onchain_swap' : 'third_party_kline'),
    pool_address: row.pool_address || row.pool || `${row.provider || 'historical_backfill'}:${row.token_ca || row.mint || ''}`,
    price_unit: row.price_unit || 'native',
  };
}

function normalizeSourceValue(value) {
  const s = String(value || '').trim().toLowerCase();
  if (!s) return null;
  if (s === 'gecko' || s === 'geckoterminal') return 'geckoterminal';
  if (s === 'cache') return 'local_cache';
  return s;
}

function barsByToken(rows) {
  const out = new Map();
  for (const row of rows) {
    if (!out.has(row.token_ca)) out.set(row.token_ca, []);
    out.get(row.token_ca).push(row);
  }
  return out;
}

function summarizeReconciliationSourceMatch(pilotSignals, bars, observerByKey) {
  const byToken = barsByToken(bars);
  const mismatches = [];
  const checked = [];
  const excluded = [];
  const comparableKeys = new Set();
  let noBars = 0;
  for (const pilot of pilotSignals) {
    const observer = observerByKey.get(keyOf(pilot));
    if (!observer) continue;
    const expectedProvider = normalizeSourceValue(observer.path_provider);
    const expectedKind = normalizeSourceValue(observer.path_source_kind);
    if (!REPRODUCIBLE_OBSERVER_PROVIDERS.has(expectedProvider)) {
      excluded.push({
        token_ca: pilot.token_ca,
        signal_ts: pilot.signal_ts,
        reason: 'source_not_reproducible',
        expected_provider: expectedProvider,
        expected_source_kind: expectedKind,
      });
      continue;
    }
    const windowBars = (byToken.get(pilot.token_ca) || [])
      .filter((bar) => bar.timestamp >= pilot.signal_ts && bar.timestamp <= pilot.signal_ts + HORIZON_SEC);
    if (!windowBars.length) {
      noBars += 1;
      mismatches.push({
        token_ca: pilot.token_ca,
        signal_ts: pilot.signal_ts,
        reason: 'no_reconciliation_bars_for_observer_labeled_signal',
        expected_provider: expectedProvider,
        expected_source_kind: expectedKind,
      });
      continue;
    }
    const providers = [...new Set(windowBars.map((bar) => normalizeSourceValue(bar.provider)).filter(Boolean))].sort();
    const kinds = [...new Set(windowBars.map((bar) => normalizeSourceValue(bar.source_kind)).filter(Boolean))].sort();
    const providerOk = Boolean(expectedProvider) && providers.length > 0 && providers.every((provider) => provider === expectedProvider);
    const kindOk = !expectedKind || (kinds.length > 0 && kinds.every((kind) => kind === expectedKind));
    const record = {
      token_ca: pilot.token_ca,
      signal_ts: pilot.signal_ts,
      expected_provider: expectedProvider,
      observed_providers: providers,
      expected_source_kind: expectedKind,
      observed_source_kinds: kinds,
      bar_count: windowBars.length,
      provider_match: providerOk,
      source_kind_match: kindOk,
    };
    checked.push(record);
    if (providerOk && kindOk) comparableKeys.add(keyOf(pilot));
    if (!providerOk || !kindOk) mismatches.push(record);
  }
  return {
    checked_n: checked.length + noBars,
    comparable_n: comparableKeys.size,
    excluded_source_not_reproducible_n: excluded.length,
    matched_n: checked.filter((row) => row.provider_match && row.source_kind_match).length,
    no_bars_n: noBars,
    mismatch_n: mismatches.length,
    ok: mismatches.length === 0,
    comparable_keys: comparableKeys,
    provider_distribution: countBy(checked.flatMap((row) => row.observed_providers), (value) => value),
    observer_provider_distribution: countBy(checked, (row) => row.expected_provider || 'unknown'),
    source_kind_distribution: countBy(checked.flatMap((row) => row.observed_source_kinds), (value) => value),
    observer_source_kind_distribution: countBy(checked, (row) => row.expected_source_kind || 'unknown'),
    excluded_source_not_reproducible_examples: excluded.slice(0, 20),
    mismatches: mismatches.slice(0, 20),
  };
}

function readJsonl(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  return raw.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function readStageTags(filePath) {
  if (!filePath) return null;
  const rows = readJsonl(filePath);
  const byKey = new Map();
  for (const raw of rows) {
    const token = String(raw.token_ca || '').trim();
    const signalTs = normalizeTs(raw.signal_ts);
    if (!token || signalTs == null) continue;
    const curveTotal = Number(raw.curve_trade_count_total ?? raw.curve_trade_count ?? 0);
    const preCurve = Number(raw.pre_signal_curve_trade_count ?? 0);
    const postCurve = Number(raw.post_signal_curve_trade_count ?? 0);
    const outOfWindow = Number(raw.out_of_window_trade_count ?? 0);
    byKey.set(`${token}|${signalTs}`, {
      token_ca: token,
      signal_ts: signalTs,
      stage_tag: raw.stage_tag || (curveTotal > 0 ? 'curve_activity_observed' : 'no_curve_trade_observed'),
      curve_trade_count_total: Number.isFinite(curveTotal) ? curveTotal : 0,
      pre_signal_curve_trade_count: Number.isFinite(preCurve) ? preCurve : 0,
      post_signal_curve_trade_count: Number.isFinite(postCurve) ? postCurve : 0,
      out_of_window_trade_count: Number.isFinite(outOfWindow) ? outOfWindow : 0,
      stage_source: raw.stage_source || raw.provider || 'dune_stage_tag',
    });
  }
  return { rows, byKey };
}

function classifyStageFromOutcome(row) {
  const kind = String(row.path_source_kind || row.source_kind || '').toLowerCase();
  const provider = String(row.path_provider || row.provider || '').toLowerCase();
  const unit = String(row.baseline_price_unit || '').toLowerCase();
  if (kind === 'bonding_curve') return 'curve_active';
  if (kind === 'amm_pool') return 'graduated_amm';
  if (unit && unit !== 'native') return 'graduated_amm';
  if (provider.includes('gmgn')) return 'graduated_amm';
  if (kind === 'indexed_ohlcv') return 'indexed_unknown';
  return 'unknown';
}

function stageResolved(stage) {
  return stage === 'curve_active' || stage === 'graduated_amm' || stage === 'mature_recovery';
}

function classifyStageFromTag(tag) {
  if (!tag) return 'unknown';
  if (Number(tag.curve_trade_count_total || 0) > 0) return 'curve_active';
  return 'indexed_unknown';
}

function summarizeStage(outcomes, stageTags = null) {
  const rows = outcomes.map((row) => {
    const tag = stageTags?.byKey?.get(keyOf(row));
    return {
      ...row,
      stage: tag ? classifyStageFromTag(tag) : classifyStageFromOutcome(row),
      stage_tag: tag?.stage_tag || null,
      curve_trade_count_total: tag?.curve_trade_count_total ?? null,
      pre_signal_curve_trade_count: tag?.pre_signal_curve_trade_count ?? null,
      post_signal_curve_trade_count: tag?.post_signal_curve_trade_count ?? null,
      out_of_window_trade_count: tag?.out_of_window_trade_count ?? null,
    };
  });
  const resolved = rows.filter((row) => stageResolved(row.stage));
  const progressAvailable = rows.filter((row) => row.baseline_price != null && row.baseline_price_unit === 'native');
  return {
    stage_source: stageTags ? 'separate_stage_tags' : 'outcome_bar_source_proxy',
    total: rows.length,
    stage_distribution: countBy(rows, (row) => row.stage),
    stage_tag_rows: stageTags?.rows?.length ?? null,
    stage_tag_matched_n: stageTags ? rows.filter((row) => stageTags.byKey.has(keyOf(row))).length : null,
    curve_trade_observed_n: stageTags ? rows.filter((row) => Number(row.curve_trade_count_total || 0) > 0).length : null,
    stage_tag_out_of_window_trade_count: stageTags
      ? rows.reduce((sum, row) => sum + Number(row.out_of_window_trade_count || 0), 0)
      : null,
    stage_resolved_n: resolved.length,
    stage_resolved_rate: pct(resolved.length, rows.length),
    progress_available_proxy_n: progressAvailable.length,
    progress_available_proxy_rate: pct(progressAvailable.length, rows.length),
    baseline_at_signal_available_n: rows.filter((row) => row.baseline_price != null).length,
    graduation_or_amm_status_available_n: rows.filter((row) => classifyStageFromOutcome(row) === 'graduated_amm').length,
    unknown_stage_reasons: countBy(rows.filter((row) => !stageResolved(row.stage)), (row) => row.coverage_reason || 'unknown'),
  };
}

function stageVerdict(rate) {
  if (rate == null) return 'NOT_FEASIBLE';
  if (rate >= STAGE_PASS) return 'PASS';
  if (rate >= STAGE_PARTIAL) return 'PARTIAL';
  return 'NOT_FEASIBLE';
}

function reconciliationVerdict(rate) {
  if (rate == null) return 'NOT_FEASIBLE';
  if (rate >= RECONCILE_PASS) return 'PASS';
  if (rate >= RECONCILE_PARTIAL) return 'PARTIAL';
  return 'NOT_FEASIBLE';
}

function wilsonInterval(successes, total, z = 1.96) {
  if (!total) return { lo: null, hi: null };
  const p = successes / total;
  const denom = 1 + (z * z) / total;
  const center = (p + (z * z) / (2 * total)) / denom;
  const margin = (z * Math.sqrt((p * (1 - p) + (z * z) / (4 * total)) / total)) / denom;
  return {
    lo: Math.max(0, Math.round((center - margin) * 10_000) / 10_000),
    hi: Math.min(1, Math.round((center + margin) * 10_000) / 10_000),
  };
}

function compareOutcomes(backfill, observer) {
  const backfillClass = tierClass(backfill.raw_primary_tier);
  const observerClass = tierClass(observer.raw_primary_tier);
  const tierMatch = String(backfill.raw_primary_tier || '') === String(observer.raw_primary_tier || '');
  const classMatch = backfillClass !== 'unknown' && backfillClass === observerClass;
  let difference = null;
  if (!classMatch || !tierMatch) {
    if (backfill.coverage_reason !== observer.coverage_reason) difference = 'coverage';
    else if (Number(backfill.baseline_price || 0) && Number(observer.baseline_price || 0)
      && Math.abs(Number(backfill.baseline_price) / Number(observer.baseline_price) - 1) > 0.05) difference = 'baseline';
    else if (String(backfill.baseline_price_unit || '') !== String(observer.baseline_price_unit || '')) difference = 'unit';
    else if (String(backfill.sustained_reason || '') !== String(observer.sustained_reason || '')) difference = 'sustained_definition';
    else difference = 'tier_or_threshold';
  }
  return {
    token_ca: backfill.token_ca,
    signal_ts: backfill.signal_ts,
    class_match: classMatch,
    tier_match: tierMatch,
    backfill_class: backfillClass,
    observer_class: observerClass,
    backfill_tier: backfill.raw_primary_tier || null,
    observer_tier: observer.raw_primary_tier || null,
    backfill_sustained_peak_pct: backfill.max_sustained_peak_pct ?? null,
    observer_sustained_peak_pct: observer.max_sustained_peak_pct ?? null,
    backfill_baseline_price: backfill.baseline_price ?? null,
    observer_baseline_price: observer.baseline_price ?? null,
    backfill_peak_lag_sec: backfill.time_to_sustained_peak_sec ?? null,
    observer_peak_lag_sec: observer.time_to_sustained_peak_sec ?? null,
    backfill_peak_domain: classifyStageFromOutcome(backfill),
    observer_peak_domain: classifyStageFromOutcome(observer),
    difference_reason: difference,
  };
}

function computeFinalVerdict({ reconciliation, stage, costOk }) {
  if (reconciliation.verdict === 'NOT_FEASIBLE' || stage.verdict === 'NOT_FEASIBLE') {
    return 'HISTORICAL_BACKFILL_NOT_FEASIBLE';
  }
  if (!costOk || reconciliation.verdict === 'PARTIAL' || stage.verdict === 'PARTIAL') {
    return 'HISTORICAL_BACKFILL_PARTIAL';
  }
  return 'HISTORICAL_BACKFILL_FEASIBLE';
}

function parseArgs(argv) {
  const a = {
    mode: 'prepare',
    limit: DEFAULT_LIMIT,
    overlapStart: DEFAULT_OVERLAP_START,
    overlapEnd: DEFAULT_OVERLAP_END,
    historyStart: DEFAULT_HISTORY_START,
    historyEnd: DEFAULT_HISTORY_END,
    runCard: DEFAULT_RUN_CARD,
    costCredits: null,
    runtimeMs: null,
    allowSmallPremiumDbForSmoke: false,
  };
  const take = (argvRef, i) => {
    if (i + 1 >= argvRef.length || String(argvRef[i + 1] || '').startsWith('--')) die(`${argvRef[i]} requires a value`);
    return argvRef[i + 1];
  };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i];
    switch (k) {
      case '--help':
      case '-h':
        a.help = true;
        break;
      case '--mode': a.mode = take(argv, i); i += 1; break;
      case '--premium-db': a.premiumDb = take(argv, i); i += 1; break;
      case '--observer-db': a.observerDb = take(argv, i); i += 1; break;
      case '--out-dir': a.outDir = take(argv, i); i += 1; break;
      case '--pilot-signals': a.pilotSignals = take(argv, i); i += 1; break;
      case '--bars-jsonl': a.barsJsonl = take(argv, i); i += 1; break;
      case '--stage-tags-jsonl': a.stageTagsJsonl = take(argv, i); i += 1; break;
      case '--dune-manifest': a.duneManifest = take(argv, i); i += 1; break;
      case '--cost-credits': a.costCredits = Number(take(argv, i)); i += 1; break;
      case '--runtime-ms': a.runtimeMs = Number(take(argv, i)); i += 1; break;
      case '--limit': a.limit = Number(take(argv, i)); i += 1; break;
      case '--overlap-start': {
        const v = take(argv, i); a.overlapStart = normalizeTs(Number.isNaN(Number(v)) ? Date.parse(v) / 1000 : v); i += 1; break;
      }
      case '--overlap-end': {
        const v = take(argv, i); a.overlapEnd = normalizeTs(Number.isNaN(Number(v)) ? Date.parse(v) / 1000 : v); i += 1; break;
      }
      case '--history-start': {
        const v = take(argv, i); a.historyStart = normalizeTs(Number.isNaN(Number(v)) ? Date.parse(v) / 1000 : v); i += 1; break;
      }
      case '--history-end': {
        const v = take(argv, i); a.historyEnd = normalizeTs(Number.isNaN(Number(v)) ? Date.parse(v) / 1000 : v); i += 1; break;
      }
      case '--run-card': a.runCard = take(argv, i); i += 1; break;
      case '--allow-small-premium-db-for-smoke': a.allowSmallPremiumDbForSmoke = true; break;
      default:
        die(`unknown arg: ${k}`);
    }
  }
  return a;
}

function usage() {
  return [
    'usage:',
    '  node scripts/run-gate05-backfill-pilot.js --mode prepare --premium-db <db> --observer-db <raw_outcomes.db> --out-dir <dir>',
    '  node scripts/run-gate05-backfill-pilot.js --mode evaluate --observer-db <raw_outcomes.db> --pilot-signals <json> --bars-jsonl <bars.jsonl> --out-dir <dir>',
    '',
    'notes:',
    '  prepare emits pilot-signals.json, burned_keys.txt, signal_windows.csv, and provider-request.json.',
    '  evaluate consumes provider 1m bars and reuses src/analytics/raw-signal-outcomes.js.',
    '  prepare expects the real premium_signals metadata DB; use --allow-small-premium-db-for-smoke only for synthetic tests.',
    '  reconciliation bars must match the observer price source; use --stage-tags-jsonl for separate Dune curve-presence tags.',
  ].join('\n');
}

function runPrepare(args) {
  if (!args.premiumDb) die('--premium-db required in prepare mode');
  if (!args.observerDb) die('--observer-db required in prepare mode');
  if (!args.outDir) die('--out-dir required');
  fs.mkdirSync(args.outDir, { recursive: true });
  const premium = loadPremiumSignals(args.premiumDb, args);
  const observer = loadObserverRows(args.observerDb, args);
  const selected = selectPilotSample(premium.rows, observer.byKey, args);
  const windows = selected.rows.map((row, i) => ({
    window_id: `p${String(i + 1).padStart(5, '0')}`,
    token_ca: row.token_ca,
    signal_ts: row.signal_ts,
    window_start_ts: row.signal_ts,
    window_end_ts: row.signal_ts + HORIZON_SEC,
    has_observer_label: row.has_observer_label,
    pilot_source: row.pilot_source,
  }));
  const burned = selected.rows.map((row) => keyOf(row)).sort();
  writeJson(path.join(args.outDir, 'pilot-signals.json'), selected.rows);
  fs.writeFileSync(path.join(args.outDir, 'burned_keys.txt'), burned.join('\n') + '\n');
  fs.writeFileSync(path.join(args.outDir, 'signal_windows.csv'), [
    'window_id,token_ca,signal_ts,window_start_ts,window_end_ts,pilot_source,has_observer_label',
    ...windows.map((w) => `${w.window_id},${w.token_ca},${w.signal_ts},${w.window_start_ts},${w.window_end_ts},${w.pilot_source},${w.has_observer_label ? 1 : 0}`),
  ].join('\n') + '\n');
  writeJson(path.join(args.outDir, 'provider-request.json'), {
    schema_version: 'gate05_backfill_provider_request.v1',
    reconciliation_bars_must_match_observer_source: true,
    stage_tag_dune_template: 'scripts/gate05-backfill-pilot-stage-tags-dune.template.sql',
    provider_cost_ceiling_credits: 30,
    fetch_must_abort_if_estimated_or_final_cost_exceeds_ceiling: true,
    required_bar_schema: {
      token_ca: 'string',
      timestamp: 'unix seconds minute timestamp',
      open: 'number',
      high: 'number',
      low: 'number',
      close: 'number',
      volume: 'number|null',
      provider: 'string',
      source_kind: 'bonding_curve|amm_pool|indexed_ohlcv',
      source_family: 'onchain_swap|third_party_kline',
      pool_address: 'string',
      price_unit: 'native|usd_per_token',
    },
    windows,
  });
  const manifest = {
    schema_version: 'gate05_backfill_pilot_prepare.v1',
    generated_at: new Date().toISOString(),
    run_card: { path: path.resolve(args.runCard), sha256: sha256File(args.runCard) },
    inputs: {
      premium_db: { path: path.resolve(args.premiumDb), sha256: sha256File(args.premiumDb), identity: premium.identity },
      observer_db: { path: path.resolve(args.observerDb), sha256: sha256File(args.observerDb) },
    },
    params: {
      limit: Math.min(DEFAULT_LIMIT, Number(args.limit || DEFAULT_LIMIT)),
      overlap_start_ts: args.overlapStart,
      overlap_end_ts: args.overlapEnd,
      history_start_ts: args.historyStart,
      history_end_ts: args.historyEnd,
      horizon_sec: HORIZON_SEC,
      unit: '(token_ca, signal_ts)',
      burned: true,
    },
    premium_inventory: {
      full_db_row_count: premium.identity.row_count,
      month_inventory: premium.identity.month_inventory,
      input_rows_in_range: premium.rows.length,
      duplicate_rows_removed: premium.duplicateRows,
      by_month: countBy(premium.rows, (row) => row.month),
    },
    observer_overlap: {
      labeled_rows: observer.rows.length,
      by_tier: countBy(observer.rows, (row) => row.raw_primary_tier),
    },
    sample: selected.stats,
    forbidden: {
      no_candidate_feature_stratification: true,
      forbidden_stratification_fields: FORBIDDEN_STRATIFICATION_FIELDS,
      no_edge_metrics: true,
    },
    outputs: {
      pilot_signals: 'pilot-signals.json',
      burned_keys: 'burned_keys.txt',
      signal_windows: 'signal_windows.csv',
      provider_request: 'provider-request.json',
    },
  };
  writeJson(path.join(args.outDir, 'prepare-manifest.json'), manifest);
  const leak = hasForbiddenKey(manifest);
  if (leak) die(`forbidden report key leaked in prepare manifest: ${leak}`);
  console.log(JSON.stringify({ ok: true, phase: 'prepare', selected: selected.rows.length, overlap_selected: selected.stats.overlap_selected, out_dir: args.outDir }, null, 2));
}

function runEvaluate(args) {
  if (!args.pilotSignals) die('--pilot-signals required in evaluate mode');
  if (!args.observerDb) die('--observer-db required in evaluate mode');
  if (!args.barsJsonl) die('--bars-jsonl required in evaluate mode');
  if (!args.outDir) die('--out-dir required');
  fs.mkdirSync(args.outDir, { recursive: true });
  const pilotSignals = readJson(args.pilotSignals);
  const bars = readJsonl(args.barsJsonl).map(normalizeBar).filter((row) => row.token_ca && row.timestamp != null && row.high != null && row.low != null && row.close != null);
  const stageTags = args.stageTagsJsonl ? readStageTags(args.stageTagsJsonl) : null;
  const observer = loadObserverRows(args.observerDb, args);
  const reconciliationSourceMatch = summarizeReconciliationSourceMatch(pilotSignals, bars, observer.byKey);
  if (!reconciliationSourceMatch.ok) {
    die(`reconciliation bars do not match observer source: ${JSON.stringify(reconciliationSourceMatch.mismatches)}`);
  }
  const nowTs = Math.max(...pilotSignals.map((row) => Number(row.signal_ts || 0))) + HORIZON_SEC + 1;
  const report = buildRawSignalOutcomeReport({
    signals: pilotSignals.map((row) => ({ token_ca: row.token_ca, symbol: row.symbol, timestamp_sec: row.signal_ts, signal_type: row.signal_type })),
    klineRows: bars,
    nowTs,
    horizonSec: HORIZON_SEC,
  });
  const outcomesByKey = new Map(report.outcomes.map((row) => [keyOf(row), row]));
  const reconciliationRows = [];
  for (const pilot of pilotSignals) {
    const key = keyOf(pilot);
    const observerRow = observer.byKey.get(key);
    const backfillRow = outcomesByKey.get(key);
    if (!reconciliationSourceMatch.comparable_keys.has(key)) continue;
    if (!observerRow || !backfillRow) continue;
    reconciliationRows.push(compareOutcomes(backfillRow, observerRow));
  }
  const classMatches = reconciliationRows.filter((row) => row.class_match).length;
  const tierMatches = reconciliationRows.filter((row) => row.tier_match).length;
  const reconRate = pct(classMatches, reconciliationRows.length);
  const reconciliation = {
    overlap_compared_n: reconciliationRows.length,
    dog_dud_agreement_n: classMatches,
    dog_dud_agreement_rate: reconRate,
    dog_dud_agreement_ci95: wilsonInterval(classMatches, reconciliationRows.length),
    tier_agreement_n: tierMatches,
    tier_agreement_rate: pct(tierMatches, reconciliationRows.length),
    difference_taxonomy: countBy(reconciliationRows.filter((row) => !row.class_match || !row.tier_match), (row) => row.difference_reason),
    verdict: reconciliationVerdict(reconRate),
  };
  const stage = summarizeStage(report.outcomes, stageTags);
  stage.verdict = stageVerdict(stage.stage_resolved_rate);
  const labelable = report.outcomes.filter((row) => ['gold', 'silver', 'bronze', 'sub25'].includes(String(row.raw_primary_tier)));
  const outcomeLabelability = {
    total: report.outcomes.length,
    labelable_n: labelable.length,
    labelable_rate: pct(labelable.length, report.outcomes.length),
    complete_2h_window_proxy_n: report.outcomes.filter((row) => row.observation_status === 'matured' && row.coverage_reason === 'covered').length,
    sustained_peak_available_n: report.outcomes.filter((row) => row.max_sustained_peak_pct != null).length,
    baseline_available_n: report.outcomes.filter((row) => row.baseline_price != null).length,
    provider_failure_reasons: report.coverage?.by_reason || {},
  };
  const duneManifest = args.duneManifest && fs.existsSync(args.duneManifest) ? readJson(args.duneManifest) : null;
  const actualCredits = args.costCredits ?? duneManifest?.final_status?.credits_used ?? duneManifest?.credits_used ?? null;
  const cost = {
    actual_credits: actualCredits,
    runtime_ms: args.runtimeMs ?? duneManifest?.final_status?.execution_time_millis ?? null,
    bars_rows: bars.length,
    cost_per_100_signals: actualCredits == null ? null : Math.round((actualCredits / report.outcomes.length) * 100 * 1000) / 1000,
    estimated_credits_5k: actualCredits == null ? null : Math.round((actualCredits / report.outcomes.length) * 5000 * 1000) / 1000,
    estimated_credits_10k: actualCredits == null ? null : Math.round((actualCredits / report.outcomes.length) * 10000 * 1000) / 1000,
    estimated_credits_20k: actualCredits == null ? null : Math.round((actualCredits / report.outcomes.length) * 20000 * 1000) / 1000,
    cost_ceiling_credits: 30,
    cost_ok: actualCredits == null ? null : actualCredits <= 30,
  };
  const { comparable_keys: _internalComparableKeys, ...reconciliationSourceMatchSummary } = reconciliationSourceMatch;
  const finalVerdict = computeFinalVerdict({
    reconciliation,
    stage,
    costOk: cost.cost_ok !== false,
  });
  const summary = {
    schema_version: 'gate05_backfill_pilot_evaluation.v1',
    generated_at: new Date().toISOString(),
    run_card: { path: path.resolve(args.runCard), sha256: sha256File(args.runCard) },
    inputs: {
      pilot_signals: { path: path.resolve(args.pilotSignals), sha256: sha256File(args.pilotSignals), rows: pilotSignals.length },
      bars_jsonl: { path: path.resolve(args.barsJsonl), sha256: sha256File(args.barsJsonl), rows: bars.length },
      stage_tags_jsonl: args.stageTagsJsonl ? { path: path.resolve(args.stageTagsJsonl), sha256: sha256File(args.stageTagsJsonl), rows: stageTags?.rows?.length || 0 } : null,
      observer_db: { path: path.resolve(args.observerDb), sha256: sha256File(args.observerDb) },
      dune_manifest: args.duneManifest ? { path: path.resolve(args.duneManifest), sha256: sha256File(args.duneManifest) } : null,
    },
    locked_thresholds: {
      reconciliation_pass: RECONCILE_PASS,
      reconciliation_partial: RECONCILE_PARTIAL,
      stage_pass: STAGE_PASS,
      stage_partial: STAGE_PARTIAL,
      cost_ceiling_credits: 30,
    },
    outcome_labelability: outcomeLabelability,
    reconciliation_source_match: reconciliationSourceMatchSummary,
    stage_resolution: stage,
    reconciliation,
    cost,
    verdict: finalVerdict,
    next_step: finalVerdict === 'HISTORICAL_BACKFILL_FEASIBLE'
      ? 'write historical split + Gate-1/Gate-2 spec'
      : finalVerdict === 'HISTORICAL_BACKFILL_PARTIAL'
        ? 'name blocker and decide shrunk window/domain or instrument-forward'
        : 'fall back to instrument-forward or target downgrade',
    forbidden: {
      no_candidate_feature_effects_reported: true,
      no_auc_precision_recall_lift: true,
      no_live_strategy_change: true,
    },
  };
  writeJson(path.join(args.outDir, 'backfill-outcomes.json'), report.outcomes);
  writeJson(path.join(args.outDir, 'reconciliation-differences.json'), reconciliationRows.filter((row) => !row.class_match || !row.tier_match));
  writeJson(path.join(args.outDir, 'pilot-evaluation-summary.json'), summary);
  const leak = hasForbiddenKey(summary);
  if (leak) die(`forbidden report key leaked in evaluation summary: ${leak}`);
  console.log(JSON.stringify({ ok: true, phase: 'evaluate', verdict: finalVerdict, labelable_rate: outcomeLabelability.labelable_rate, stage_resolved_rate: stage.stage_resolved_rate, reconciliation_rate: reconciliation.dog_dud_agreement_rate, out_dir: args.outDir }, null, 2));
}

function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!fs.existsSync(args.runCard)) die(`run-card missing: ${args.runCard}`);
  if (args.mode === 'prepare') return runPrepare(args);
  if (args.mode === 'evaluate') return runEvaluate(args);
  die(`unknown --mode ${args.mode}`);
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export {
  classifyStageFromOutcome,
  compareOutcomes,
  computeFinalVerdict,
  loadObserverRows,
  loadPremiumSignals,
  reconciliationVerdict,
  selectPilotSample,
  stageVerdict,
};
