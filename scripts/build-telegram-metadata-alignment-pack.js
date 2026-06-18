#!/usr/bin/env node
'use strict';

/**
 * Builds a read-only Telegram metadata/raw outcome alignment pack.
 *
 * This is an instrumentation artifact, not an edge test: it joins point-in-time
 * premium Telegram metadata to raw-path observer outcomes by exact
 * (token_ca, signal_ts), then reports coverage/missingness/provenance only.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';

const DEFAULT_RAW_DB = '/Users/boliu/sas-data-room/oos-frozen-pack-20260617T001655Z/raw_signal_outcomes.snapshot.db';
const DEFAULT_PREMIUM_DB = '/Users/boliu/sas-data-room/gate05-remote-premium-20260618T003456Z/remote-premium-signals.snapshot.db';

const RAW_FIELDS = [
  'id',
  'signal_id',
  'token_ca',
  'symbol',
  'signal_ts',
  'signal_type',
  'route',
  'source',
  'observation_status',
  'matured_at_ts',
  'horizon_sec',
  'baseline_ts',
  'baseline_lag_sec',
  'baseline_price',
  'baseline_source',
  'baseline_provider',
  'baseline_price_unit',
  'baseline_confidence',
  'path_provider',
  'path_price_unit',
  'same_source_path',
  'kline_covered',
  'coverage_reason',
  'provider',
  'max_sustained_peak_pct',
  'time_to_sustained_peak_sec',
  'raw_sustained_tier',
  'raw_primary_tier',
  'sustained_evaluable',
  'sustained_reason',
  'outlier_flag',
  'outlier_reason',
  'created_at',
  'updated_at',
  'source_kind',
  'source_family',
  'path_source_kind',
  'path_source_family',
  'first_bar_ts',
  'first_bar_lag_sec',
  'early_15m_bar_coverage_pct',
  'early_15m_complete',
];

const PREMIUM_FIELDS = [
  'id',
  'remote_signal_id',
  'token_ca',
  'symbol',
  'timestamp',
  'source_message_ts',
  'receive_ts',
  'signal_type',
  'is_ath',
  'raw_message',
  'ai_narrative_tier',
  'source_event_id',
  'signal_source',
  'created_at',
  'narrative_score',
  'narrative_confidence',
  'narrative_tags',
];

const METADATA_FIELDS = [
  'remote_signal_id',
  'source_message_ts',
  'receive_ts',
  'signal_type',
  'is_ath',
  'narrative_score',
  'narrative_confidence',
  'ai_narrative_tier',
  'signal_source',
  'raw_message_present',
];

const RAW_QA_FIELDS = [
  'observation_status',
  'kline_covered',
  'coverage_reason',
  'baseline_confidence',
  'same_source_path',
  'outlier_flag',
  'sustained_evaluable',
  'baseline_price_unit',
  'path_provider',
  'path_source_kind',
  'path_source_family',
  'path_price_unit',
];

const METADATA_ROW_FIELD = {
  remote_signal_id: 'premium_remote_signal_id',
  signal_type: 'metadata_signal_type',
};

const FORBIDDEN_KEY_TERMS = [
  'auc',
  'precision',
  'recall',
  'lift',
  'dog_rate',
  'dud_rate',
  'p_dog',
  'p_dud',
  'separation',
  'effect_size',
  'mutual_info',
  'chi2',
];

function parseArgs(argv) {
  const args = {
    rawDb: DEFAULT_RAW_DB,
    premiumDb: DEFAULT_PREMIUM_DB,
  };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i];
    const v = argv[i + 1];
    if (k === '--raw-db') { args.rawDb = v; i += 1; }
    else if (k === '--premium-db') { args.premiumDb = v; i += 1; }
    else if (k === '--out-dir') { args.outDir = v; i += 1; }
    else if (k === '--help' || k === '-h') { args.help = true; }
    else throw new Error(`Unknown argument: ${k}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-telegram-metadata-alignment-pack.js --out-dir ~/sas-data-room/telegram-metadata-alignment-...',
    '',
    'Optional:',
    '  --raw-db raw_signal_outcomes.snapshot.db',
    '  --premium-db remote-premium-signals.snapshot.db',
  ].join('\n');
}

function sha256File(p) {
  return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}

function openRo(dbPath) {
  return new Database(dbPath, { readonly: true, fileMustExist: true });
}

function tableExists(db, table) {
  return Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table));
}

function tableColumns(db, table) {
  if (!tableExists(db, table)) return [];
  return db.prepare(`PRAGMA table_info(${table})`).all().map((r) => r.name);
}

function requireColumns(cols, required, table) {
  const missing = required.filter((c) => !cols.includes(c));
  if (missing.length) {
    throw new Error(`${table} missing required columns: ${missing.join(', ')}`);
  }
}

function selectList(cols, fields) {
  return fields.map((f) => (cols.includes(f) ? f : `NULL AS ${f}`)).join(',\n      ');
}

function normalizeSignalTs(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.floor(n > 100_000_000_000 ? n / 1000 : n);
}

function keyOf(tokenCa, signalTs) {
  return `${tokenCa}|${normalizeSignalTs(signalTs)}`;
}

function boolish(value) {
  if (value == null) return null;
  if (value === true || value === false) return value;
  if (value === 1 || value === 0) return Boolean(value);
  const s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes'].includes(s)) return true;
  if (['0', 'false', 'no'].includes(s)) return false;
  return null;
}

function countNonNull(row, fields) {
  return fields.reduce((n, f) => {
    const v = row[f];
    return n + (v == null || v === '' ? 0 : 1);
  }, 0);
}

function compareRawRows(a, b) {
  const aUpdated = Number(a.updated_at || 0);
  const bUpdated = Number(b.updated_at || 0);
  if (aUpdated !== bUpdated) return bUpdated - aUpdated;
  const aCreated = Number(a.created_at || 0);
  const bCreated = Number(b.created_at || 0);
  if (aCreated !== bCreated) return bCreated - aCreated;
  return Number(b.id || 0) - Number(a.id || 0);
}

function comparePremiumRows(a, b) {
  const aScore = countNonNull(a, METADATA_FIELDS.filter((f) => f !== 'raw_message_present'))
    + (a.raw_message ? 1 : 0)
    + (a.remote_signal_id != null ? 2 : 0);
  const bScore = countNonNull(b, METADATA_FIELDS.filter((f) => f !== 'raw_message_present'))
    + (b.raw_message ? 1 : 0)
    + (b.remote_signal_id != null ? 2 : 0);
  if (aScore !== bScore) return bScore - aScore;
  const aRemote = Number(a.remote_signal_id || 0);
  const bRemote = Number(b.remote_signal_id || 0);
  if (aRemote !== bRemote) return bRemote - aRemote;
  return Number(b.id || 0) - Number(a.id || 0);
}

function dedupeRows(rows, keyFn, compareFn) {
  const groups = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  const deduped = [];
  const duplicateKeys = [];
  let duplicateRowsRemoved = 0;
  for (const [key, group] of groups.entries()) {
    const sorted = [...group].sort(compareFn);
    deduped.push(sorted[0]);
    if (group.length > 1) {
      duplicateKeys.push({ key, rows: group.length, removed: group.length - 1 });
      duplicateRowsRemoved += group.length - 1;
    }
  }
  return {
    rows: deduped.sort((a, b) => normalizeSignalTs(a.signal_ts ?? a.timestamp) - normalizeSignalTs(b.signal_ts ?? b.timestamp) || String(a.token_ca).localeCompare(String(b.token_ca))),
    duplicate_key_count: duplicateKeys.length,
    duplicate_rows_removed: duplicateRowsRemoved,
    duplicate_keys_sample: duplicateKeys.slice(0, 20),
  };
}

function distribution(rows, field) {
  const counts = new Map();
  for (const row of rows) {
    const value = row[field] == null || row[field] === '' ? '<NULL>' : String(row[field]);
    counts.set(value, (counts.get(value) || 0) + 1);
  }
  return [...counts.entries()]
    .map(([value, count]) => ({ value, count }))
    .sort((a, b) => b.count - a.count || a.value.localeCompare(b.value));
}

function coverage(rows, field) {
  let present = 0;
  for (const row of rows) {
    const v = row[field];
    if (v != null && v !== '') present += 1;
  }
  return {
    present,
    missing: rows.length - present,
    pct_present: rows.length ? Number((present / rows.length).toFixed(6)) : null,
  };
}

function summarizeDateRange(rows, field) {
  const values = rows.map((r) => normalizeSignalTs(r[field])).filter((v) => v != null);
  if (!values.length) return { min_ts: null, max_ts: null, min_iso: null, max_iso: null };
  const min = Math.min(...values);
  const max = Math.max(...values);
  return {
    min_ts: min,
    max_ts: max,
    min_iso: new Date(min * 1000).toISOString(),
    max_iso: new Date(max * 1000).toISOString(),
  };
}

function classifyStage(row) {
  const kind = row.path_source_kind || row.source_kind || null;
  const family = row.path_source_family || row.source_family || null;
  const unit = row.path_price_unit || row.baseline_price_unit || null;
  const provider = row.path_provider || row.provider || null;
  if (kind === 'bonding_curve') {
    return { stage_at_signal_proxy: 'curve_active', stage_resolved: true, stage_reason: 'path_source_kind=bonding_curve' };
  }
  if (kind === 'amm_pool') {
    return { stage_at_signal_proxy: 'amm_pool_or_graduated', stage_resolved: true, stage_reason: 'path_source_kind=amm_pool' };
  }
  if (unit && unit !== 'native') {
    return { stage_at_signal_proxy: 'non_native_or_graduated', stage_resolved: true, stage_reason: 'path_price_unit_or_baseline_price_unit_non_native' };
  }
  if (family || kind || provider) {
    return { stage_at_signal_proxy: 'indexed_or_unknown_stage', stage_resolved: false, stage_reason: 'price_track_present_stage_not_resolved' };
  }
  return { stage_at_signal_proxy: 'missing_stage_source', stage_resolved: false, stage_reason: 'no_source_fields' };
}

function rawFormalEligible(row) {
  return row.observation_status === 'matured'
    && Number(row.kline_covered) === 1
    && ['high', 'medium'].includes(String(row.baseline_confidence || ''))
    && Number(row.same_source_path) === 1
    && Number(row.outlier_flag) === 0
    && Number(row.sustained_evaluable) === 1;
}

function loadRawRows(rawDbPath) {
  const db = openRo(rawDbPath);
  const cols = tableColumns(db, 'raw_signal_outcomes');
  requireColumns(cols, ['token_ca', 'signal_ts'], 'raw_signal_outcomes');
  const rows = db.prepare(`
    SELECT
      ${selectList(cols, RAW_FIELDS)}
    FROM raw_signal_outcomes
    WHERE token_ca IS NOT NULL AND signal_ts IS NOT NULL
  `).all();
  db.close();
  return rows.map((row) => ({ ...row, signal_ts: normalizeSignalTs(row.signal_ts) }));
}

function loadPremiumRows(premiumDbPath) {
  const db = openRo(premiumDbPath);
  const cols = tableColumns(db, 'premium_signals');
  requireColumns(cols, ['token_ca', 'timestamp'], 'premium_signals');
  const rows = db.prepare(`
    SELECT
      ${selectList(cols, PREMIUM_FIELDS)}
    FROM premium_signals
    WHERE token_ca IS NOT NULL AND timestamp IS NOT NULL
  `).all();
  db.close();
  return rows.map((row) => ({ ...row, signal_ts: normalizeSignalTs(row.timestamp) }));
}

function buildAlignedRows(rawRows, premiumByKey) {
  const aligned = [];
  const unmatched = [];
  for (const raw of rawRows) {
    const key = keyOf(raw.token_ca, raw.signal_ts);
    const premium = premiumByKey.get(key) || null;
    if (!premium) {
      unmatched.push({ token_ca: raw.token_ca, signal_ts: raw.signal_ts });
      continue;
    }
    const stage = classifyStage(raw);
    aligned.push({
      schema_version: 'telegram_metadata_raw_outcome_alignment_row.v1',
      join_key: key,
      token_ca: raw.token_ca,
      signal_ts: raw.signal_ts,
      raw_signal_id: raw.signal_id ?? null,
      raw_row_id: raw.id ?? null,
      premium_row_id: premium.id ?? null,
      premium_remote_signal_id: premium.remote_signal_id ?? null,
      premium_source_event_id: premium.source_event_id ?? null,
      raw_symbol: raw.symbol ?? null,
      premium_symbol: premium.symbol ?? null,
      source_message_ts: normalizeSignalTs(premium.source_message_ts),
      receive_ts: normalizeSignalTs(premium.receive_ts),
      metadata_signal_type: premium.signal_type ?? null,
      raw_signal_type: raw.signal_type ?? null,
      is_ath: boolish(premium.is_ath),
      narrative_score: premium.narrative_score ?? null,
      narrative_confidence: premium.narrative_confidence ?? null,
      narrative_tags: premium.narrative_tags ?? null,
      ai_narrative_tier: premium.ai_narrative_tier ?? null,
      signal_source: premium.signal_source ?? null,
      raw_message_present: Boolean(premium.raw_message && String(premium.raw_message).trim()),
      raw_primary_tier: raw.raw_primary_tier ?? null,
      raw_sustained_tier: raw.raw_sustained_tier ?? null,
      max_sustained_peak_pct: raw.max_sustained_peak_pct ?? null,
      time_to_sustained_peak_sec: raw.time_to_sustained_peak_sec ?? null,
      observation_status: raw.observation_status ?? null,
      kline_covered: raw.kline_covered ?? null,
      coverage_reason: raw.coverage_reason ?? null,
      baseline_confidence: raw.baseline_confidence ?? null,
      same_source_path: raw.same_source_path ?? null,
      outlier_flag: raw.outlier_flag ?? null,
      sustained_evaluable: raw.sustained_evaluable ?? null,
      sustained_reason: raw.sustained_reason ?? null,
      baseline_price_unit: raw.baseline_price_unit ?? null,
      path_provider: raw.path_provider ?? null,
      path_source_kind: raw.path_source_kind ?? null,
      path_source_family: raw.path_source_family ?? null,
      path_price_unit: raw.path_price_unit ?? null,
      first_bar_ts: raw.first_bar_ts ?? null,
      first_bar_lag_sec: raw.first_bar_lag_sec ?? null,
      early_15m_bar_coverage_pct: raw.early_15m_bar_coverage_pct ?? null,
      early_15m_complete: raw.early_15m_complete ?? null,
      stage_at_signal_proxy: stage.stage_at_signal_proxy,
      stage_resolved: stage.stage_resolved,
      stage_reason: stage.stage_reason,
      raw_formal_eligible: rawFormalEligible(raw),
    });
  }
  return { aligned, unmatched };
}

function scanKeys(obj, hits = []) {
  if (Array.isArray(obj)) {
    for (const v of obj) scanKeys(v, hits);
    return hits;
  }
  if (obj && typeof obj === 'object') {
    for (const [k, v] of Object.entries(obj)) {
      const lower = k.toLowerCase();
      if (FORBIDDEN_KEY_TERMS.some((term) => lower.includes(term))) hits.push(k);
      scanKeys(v, hits);
    }
  }
  return hits;
}

function assertNoForbiddenMetricKeys(...objects) {
  const hits = [...new Set(objects.flatMap((obj) => scanKeys(obj)))];
  if (hits.length) {
    throw new Error(`Forbidden metric keys present: ${hits.join(', ')}`);
  }
}

function buildPack({ rawDbPath, premiumDbPath }) {
  const rawShaBefore = sha256File(rawDbPath);
  const premiumShaBefore = sha256File(premiumDbPath);
  const rawLoaded = loadRawRows(rawDbPath);
  const premiumLoaded = loadPremiumRows(premiumDbPath);
  const rawDedup = dedupeRows(rawLoaded, (r) => keyOf(r.token_ca, r.signal_ts), compareRawRows);
  const premiumDedup = dedupeRows(premiumLoaded, (r) => keyOf(r.token_ca, r.signal_ts), comparePremiumRows);
  const premiumByKey = new Map(premiumDedup.rows.map((r) => [keyOf(r.token_ca, r.signal_ts), r]));
  const { aligned, unmatched } = buildAlignedRows(rawDedup.rows, premiumByKey);

  const rawKeys = new Set(rawDedup.rows.map((r) => keyOf(r.token_ca, r.signal_ts)));
  const premiumKeys = new Set(premiumDedup.rows.map((r) => keyOf(r.token_ca, r.signal_ts)));
  const premiumOnlyKeys = [...premiumKeys].filter((k) => !rawKeys.has(k));
  const rawShaAfter = sha256File(rawDbPath);
  const premiumShaAfter = sha256File(premiumDbPath);

  const metadataCoverage = {};
  for (const field of METADATA_FIELDS) metadataCoverage[field] = coverage(aligned, METADATA_ROW_FIELD[field] || field);
  const rawQaCoverage = {};
  for (const field of RAW_QA_FIELDS) rawQaCoverage[field] = coverage(aligned, field);

  const report = {
    schema_version: 'telegram_metadata_alignment_report.v1',
    generated_at: new Date().toISOString(),
    verdict: aligned.length > 0 ? 'TELEGRAM_METADATA_ALIGNMENT_READY_FOR_FORWARD_QA' : 'TELEGRAM_METADATA_ALIGNMENT_EMPTY',
    inputs: {
      raw_db: rawDbPath,
      raw_db_sha256_before: rawShaBefore,
      raw_db_sha256_after: rawShaAfter,
      raw_db_unchanged: rawShaBefore === rawShaAfter,
      premium_db: premiumDbPath,
      premium_db_sha256_before: premiumShaBefore,
      premium_db_sha256_after: premiumShaAfter,
      premium_db_unchanged: premiumShaBefore === premiumShaAfter,
    },
    join_key: ['token_ca', 'signal_ts'],
    raw_outcomes: {
      rows_loaded: rawLoaded.length,
      unique_signal_keys: rawDedup.rows.length,
      duplicate_key_count: rawDedup.duplicate_key_count,
      duplicate_rows_removed: rawDedup.duplicate_rows_removed,
      duplicate_keys_sample: rawDedup.duplicate_keys_sample,
      date_range: summarizeDateRange(rawDedup.rows, 'signal_ts'),
      signal_type_distribution: distribution(rawDedup.rows, 'signal_type'),
    },
    premium_signals: {
      rows_loaded: premiumLoaded.length,
      unique_signal_keys: premiumDedup.rows.length,
      duplicate_key_count: premiumDedup.duplicate_key_count,
      duplicate_rows_removed: premiumDedup.duplicate_rows_removed,
      duplicate_keys_sample: premiumDedup.duplicate_keys_sample,
      date_range: summarizeDateRange(premiumDedup.rows, 'timestamp'),
      signal_type_distribution: distribution(premiumDedup.rows, 'signal_type'),
    },
    alignment: {
      aligned_rows: aligned.length,
      raw_keys_without_premium_metadata: unmatched.length,
      premium_keys_without_raw_outcome: premiumOnlyKeys.length,
      exact_join_rate_raw_keys: rawDedup.rows.length ? Number((aligned.length / rawDedup.rows.length).toFixed(6)) : null,
      exact_join_rate_premium_keys: premiumDedup.rows.length ? Number((aligned.length / premiumDedup.rows.length).toFixed(6)) : null,
      unmatched_raw_sample: unmatched.slice(0, 20),
      premium_only_keys_sample: premiumOnlyKeys.slice(0, 20),
    },
    metadata_coverage: metadataCoverage,
    raw_outcome_coverage: rawQaCoverage,
    outcome_readiness: {
      formal_eligible_rows: aligned.filter((r) => r.raw_formal_eligible).length,
      formal_eligible_rate: aligned.length ? Number((aligned.filter((r) => r.raw_formal_eligible).length / aligned.length).toFixed(6)) : null,
      matured_rows: aligned.filter((r) => r.observation_status === 'matured').length,
      kline_covered_rows: aligned.filter((r) => Number(r.kline_covered) === 1).length,
      sustained_evaluable_rows: aligned.filter((r) => Number(r.sustained_evaluable) === 1).length,
    },
    stage_diagnostics: {
      resolved_rows: aligned.filter((r) => r.stage_resolved).length,
      resolved_rate: aligned.length ? Number((aligned.filter((r) => r.stage_resolved).length / aligned.length).toFixed(6)) : null,
      stage_distribution: distribution(aligned, 'stage_at_signal_proxy'),
      path_provider_distribution: distribution(aligned, 'path_provider'),
      path_source_kind_distribution: distribution(aligned, 'path_source_kind'),
      path_source_family_distribution: distribution(aligned, 'path_source_family'),
      baseline_price_unit_distribution: distribution(aligned, 'baseline_price_unit'),
    },
    metric_leak_check: {
      no_forbidden_metric_keys: true,
      guard: 'recursive_output_key_scan',
      note: 'This report contains join/missingness/provenance counts only; no feature-vs-outcome metric is computed.',
    },
  };
  assertNoForbiddenMetricKeys(report, aligned);
  return { report, rows: aligned };
}

function writeJsonl(filePath, rows) {
  fs.writeFileSync(filePath, rows.map((r) => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : ''));
}

function writePack({ rawDbPath, premiumDbPath, outDir }) {
  fs.mkdirSync(outDir, { recursive: true });
  const { report, rows } = buildPack({ rawDbPath, premiumDbPath });
  const rowsPath = path.join(outDir, 'aligned-telegram-metadata-outcomes.jsonl');
  const reportPath = path.join(outDir, 'alignment-qa-report.json');
  writeJsonl(rowsPath, rows);
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  const manifest = {
    schema_version: 'telegram_metadata_alignment_manifest.v1',
    generated_at: new Date().toISOString(),
    out_dir: outDir,
    rows_file: rowsPath,
    rows_file_sha256: sha256File(rowsPath),
    qa_report: reportPath,
    qa_report_sha256: sha256File(reportPath),
    verdict: report.verdict,
    aligned_rows: report.alignment.aligned_rows,
    exact_join_rate_raw_keys: report.alignment.exact_join_rate_raw_keys,
    metric_leak_check: report.metric_leak_check,
  };
  const manifestPath = path.join(outDir, 'manifest.json');
  assertNoForbiddenMetricKeys(manifest);
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  return { report, manifest, paths: { rowsPath, reportPath, manifestPath } };
}

function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.outDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 2);
  }
  const { report, paths } = writePack({
    rawDbPath: args.rawDb,
    premiumDbPath: args.premiumDb,
    outDir: args.outDir,
  });
  console.log(JSON.stringify({
    ok: true,
    verdict: report.verdict,
    out_dir: args.outDir,
    aligned_rows: report.alignment.aligned_rows,
    exact_join_rate_raw_keys: report.alignment.exact_join_rate_raw_keys,
    report: paths.reportPath,
    manifest: paths.manifestPath,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try { main(); } catch (err) { console.error(`FAIL_CLOSED: ${err.message}`); process.exit(1); }
}

export {
  buildPack,
  writePack,
  normalizeSignalTs,
  keyOf,
  classifyStage,
  assertNoForbiddenMetricKeys,
  distribution,
};
