#!/usr/bin/env node
// Fullnet row v2 + market-data / feature provenance (plan §15.17, Goal 2).
// Research-only: turns the coarse FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP owner into
// row-level auditable provenance, read ONLY from artifacts already on disk:
//   - raw_signal_outcomes.snapshot.db  -> per-signal kline_covered / coverage_reason / provider /
//        pool_found / early_15m_bar_count (signal-time kline + provider provenance)
//   - source-to-raw-rows.json          -> market_cap / volume_24h / holders / top10_pct (features)
//   - v1 row quote_source/route        -> quote provider provenance
// It NEVER reads the live data dir, NEVER touches gate/entry/exit/size, NEVER tunes thresholds, and
// keeps actual EV fail-closed. v2 (Goal 1) is imported unchanged; this is a pure additive layer.
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';
import { createRequire } from 'module';
import { buildV2, MODULE_CLOSURE_MAP, REPAIR_LADDER, VALID_BUCKETS } from './build-live-fullnet-row-v2.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const EARLY_WINDOW_SEC = 900; // early_15m_bar_count is measured over the first 15 minutes (15x 1m bars)
const EXPECTED_FEATURES = ['market_cap', 'volume_24h', 'holders', 'top10_pct', 'narrative_score'];
const MARKET_DATA_OWNER = 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP';

function num(v) { if (v == null || v === '') return null; const n = Number(v); return Number.isFinite(n) ? n : null; }
function rnd(v) { const n = num(v); return n == null ? null : Math.round(n); }
function signalKey(tokenCa, signalTs) { return `${tokenCa}|${rnd(signalTs) ?? 0}`; }
function rate(n, d) { return d ? Math.round((n / d) * 1e6) / 1e6 : null; }
function countBy(rows, fn) { const o = {}; for (const r of rows) { const k = fn(r) ?? 'unknown'; o[k] = (o[k] || 0) + 1; } return o; }
function topReasons(rows, fn) { return Object.entries(countBy(rows, fn)).map(([key, n]) => ({ key, n })).sort((a, b) => b.n - a.n || a.key.localeCompare(b.key)); }

// coverage_reason (when kline not covered) -> precise, stable kline_missing_reason
const KLINE_REASON_MAP = {
  no_kline_for_token: 'kline_not_available_for_token',
  baseline_after_max_lag: 'kline_baseline_after_max_lag',
  no_kline_after_anchor: 'kline_no_bars_after_anchor',
  outlier_price: 'kline_outlier_price_excluded',
  covered: 'kline_baseline_only_no_early_bars',
};

// Pure: per-signal kline + raw-provider provenance from a raw_signal_outcomes row (or null).
function klineProvenance(rso) {
  if (!rso) {
    return {
      kline_seen: false, kline_source: null, kline_bars_n: null, kline_window_sec: null,
      kline_coverage_pct: null, kline_cache_hit: null, kline_pool_found: null, kline_first_bar_lag_sec: null,
      kline_missing_reason: 'raw_signal_outcomes_row_absent_for_signal',
      raw_provider_seen: false, raw_provider_source: null,
      raw_provider_missing_reason: 'raw_signal_outcomes_row_absent_for_signal',
    };
  }
  const covered = Number(rso.kline_covered) === 1;
  const bars = rso.early_15m_bar_count == null ? null : Number(rso.early_15m_bar_count);
  const provider = rso.provider || rso.baseline_provider || null;
  const poolFound = rso.pool_found == null ? null : Number(rso.pool_found) === 1;
  let klineMissing = null;
  if (!covered) {
    klineMissing = KLINE_REASON_MAP[String(rso.coverage_reason || '')]
      || (rso.coverage_reason ? `kline_${rso.coverage_reason}` : 'kline_not_covered_reason_unrecorded');
    if (poolFound === false && (bars == null || bars === 0)) klineMissing = 'kline_pool_unresolved';
  }
  return {
    kline_seen: covered,
    kline_source: provider,
    kline_bars_n: bars,
    kline_window_sec: bars != null ? EARLY_WINDOW_SEC : null,
    kline_coverage_pct: rso.early_15m_bar_coverage_pct == null ? null : Number(rso.early_15m_bar_coverage_pct),
    kline_cache_hit: provider == null ? null : provider === 'local_cache',
    kline_pool_found: poolFound,
    kline_first_bar_lag_sec: rso.first_bar_lag_sec == null ? null : Number(rso.first_bar_lag_sec),
    kline_missing_reason: klineMissing,
    raw_provider_seen: provider != null,
    raw_provider_source: provider,
    raw_provider_missing_reason: provider != null ? null : 'raw_provider_not_recorded',
  };
}

// Pure: feature-vector availability from a source-to-raw row (or null).
function featureAvailability(srcRow) {
  if (!srcRow) {
    return {
      feature_vector_seen: false, feature_vector_fields_present: [], feature_vector_fields_missing: EXPECTED_FEATURES.slice(),
      feature_vector_missing_reason: 'source_to_raw_row_absent_for_signal',
    };
  }
  const present = []; const missing = [];
  for (const f of EXPECTED_FEATURES) { const v = srcRow[f]; (v != null && v !== '' ? present : missing).push(f); }
  return {
    feature_vector_seen: present.length > 0,
    feature_vector_fields_present: present,
    feature_vector_fields_missing: missing,
    feature_vector_missing_reason: missing.length ? `feature_vector_missing_field:${missing.join('|')}` : null,
  };
}

// Pure: quote provider provenance from the v1 row.
function quoteProvenance(row) {
  const src = row.quote_source ?? row.route_source ?? null;
  return {
    quote_provider_seen: src != null,
    quote_provider_source: src,
    quote_provider_missing_reason: src != null ? null
      : (row.route_missing_reason ?? row.quote_missing_reason ?? (row.has_decision ? 'quote_provider_not_recorded_in_decision' : 'no_decision_event_for_signal')),
  };
}

// Pure: market-data provenance confidence + warnings.
function marketDataConfidence(m) {
  const warnings = [];
  if (!m.kline_seen) warnings.push(m.kline_missing_reason || 'kline_not_seen');
  if (!m.raw_provider_seen) warnings.push(m.raw_provider_missing_reason || 'raw_provider_not_seen');
  if (m.feature_vector_fields_missing && m.feature_vector_fields_missing.length) warnings.push(`feature_missing:${m.feature_vector_fields_missing.join('|')}`);
  if (!m.quote_provider_seen) warnings.push(m.quote_provider_missing_reason || 'quote_provider_not_seen');
  if (m.kline_pool_found === false) warnings.push('kline_pool_unresolved');
  let confidence;
  if (m.kline_seen && m.raw_provider_seen && m.feature_vector_seen && m.quote_provider_seen) confidence = 'HIGH';
  else if (m.kline_seen && (m.raw_provider_seen || m.feature_vector_seen)) confidence = 'MEDIUM';
  else confidence = 'LOW';
  return { market_data_provenance_confidence: confidence, market_data_provenance_warnings: warnings };
}

// Precise sub-reason for rows whose final repair owner is the market-data gap (de-blackboxing).
function marketDataRepairDetail(row, m) {
  if (row.final_repair_owner_v2 !== MARKET_DATA_OWNER) return null;
  if (!m.kline_seen) return m.kline_missing_reason || 'kline_not_seen';
  if (m.kline_pool_found === false) return 'kline_pool_unresolved';
  if (m.feature_vector_fields_missing.length) return `feature_missing:${m.feature_vector_fields_missing.join('|')}`;
  if (!m.raw_provider_seen) return m.raw_provider_missing_reason;
  return 'market_data_provenance_low_confidence';
}

// Pure: augment one v2 row with market-data provenance (rso/srcRow may be null).
function augmentMarketDataRow(row, rso, srcRow) {
  const k = klineProvenance(rso);
  const f = featureAvailability(srcRow);
  const q = quoteProvenance(row);
  const conf = marketDataConfidence({ ...k, ...f, ...q });
  const merged = { ...row, ...k, ...f, ...q, ...conf };
  merged.market_data_repair_detail = marketDataRepairDetail(merged, merged);
  return merged;
}

// ---- module-closure reclassification (evidence-verified, no silent flips) ---------------------
const COVERED_OVERRIDES = {
  kline_cache: {
    row_fields: ['kline_seen', 'kline_source', 'kline_bars_n', 'kline_window_sec', 'kline_cache_hit', 'kline_coverage_pct', 'kline_missing_reason'],
    proof: (rows) => rows.some((r) => r.kline_seen === true) && rows.some((r) => r.kline_missing_reason != null || r.kline_seen === true),
    note: 'signal-time kline provenance from raw_signal_outcomes.snapshot.db; live kline_cache.db cross-check intentionally not run (research-only, no live-dir read)',
  },
  feature_availability: {
    row_fields: ['feature_vector_seen', 'feature_vector_fields_present', 'feature_vector_fields_missing', 'feature_vector_missing_reason'],
    proof: (rows) => rows.some((r) => r.feature_vector_seen === true),
    note: 'market_cap/volume_24h from source-to-raw; holders/top10_pct NOT exported in this pack (field-level missing, see report)',
  },
  raw_provider_evidence: {
    row_fields: ['raw_provider_seen', 'raw_provider_source', 'raw_provider_missing_reason', 'kline_pool_found'],
    proof: (rows) => rows.some((r) => r.raw_provider_seen === true),
    note: 'provider/baseline_provider/pool_found from raw_signal_outcomes.snapshot.db',
  },
};
const REFINED_BLOCKED_REASONS = {
  curve_pumpfun: 'curve_pumpfun_microstructure_not_in_raw_signal_outcomes_or_source_to_raw',
  source_channel_dbs: 'channel_registry_priors_not_exported__source_kind_family_only_partial',
  quote_intent_binding: 'quote_provider_source_now_covered_but_intent_to_fill_binding_not_exported__requires_zeabur_api_export',
  kline_cache: 'live_kline_cache_db_crosscheck_requires_live_dir_read__signal_time_provenance_covered_instead',
};

function buildClosureReport(rows) {
  const reclassified = [];
  const map = MODULE_CLOSURE_MAP.map((m) => {
    const ov = COVERED_OVERRIDES[m.group];
    if (ov && m.coverage_status === 'blocked') {
      if (ov.proof(rows)) {
        reclassified.push({ module_group: m.group, from: 'blocked', to: 'covered', evidence: ov.note, row_fields: ov.row_fields });
        return { ...m, coverage_status: 'covered', row_fields: ov.row_fields, note: ov.note };
      }
    }
    if (m.coverage_status === 'blocked' && REFINED_BLOCKED_REASONS[m.group]) {
      return { ...m, note: REFINED_BLOCKED_REASONS[m.group] };
    }
    return m;
  });
  const statusCounts = countBy(map, (m) => m.coverage_status);
  return {
    schema_version: 'live_fullnet_module_closure_coverage_report.v2_marketdata',
    note: 'Goal 2 (§15.17): market-data/feature provenance reclassified from blocked->covered ONLY where rows carry the evidence (proof-gated, no silent reclassify). Still-blocked market-data modules carry a precise reason.',
    every_module_mapped_to_A_G: map.every((m) => VALID_BUCKETS.has(m.bucket)),
    bucket_G_all_intentionally_excluded: map.filter((m) => m.bucket === 'G').every((m) => m.coverage_status === 'intentionally_excluded'),
    module_count_total: map.length,
    coverage_status_counts: statusCounts,
    reclassified_from_blocked: reclassified,
    modules: map.map((m) => ({
      module_group: m.group, bucket: m.bucket, coverage_status: m.coverage_status,
      row_level_fields_added: m.row_fields,
      reason_for_exclusion: m.bucket === 'G' ? (m.note || 'non_row_exclusion') : (m.coverage_status === 'blocked' ? (m.note || 'not_in_current_exports') : null),
      repair_owner_added: m.repair_owner,
      coverage_status_final: m.coverage_status,
    })),
  };
}

// ---- reports ----------------------------------------------------------------------------------
function marketDataProvenanceReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  const duds = rows.filter((r) => r.class === 'dud');
  const dist = (subset, fn) => countBy(subset, fn);
  return {
    schema_version: 'live_fullnet_market_data_provenance_report.v1',
    note: 'Per-signal kline + raw/quote provider provenance (signal-time). kline_seen uses authoritative kline_covered; bars_n=early_15m_bar_count. ex-post only; no strategy use.',
    row_count: rows.length,
    dog_count: dogs.length,
    dud_count: duds.length,
    pending_count: rows.filter((r) => r.class === 'pending').length,
    kline_seen_n: rows.filter((r) => r.kline_seen).length,
    kline_seen_dog_n: dogs.filter((r) => r.kline_seen).length,
    kline_pool_unresolved_n: rows.filter((r) => r.kline_pool_found === false).length,
    kline_cache_hit_n: rows.filter((r) => r.kline_cache_hit === true).length,
    raw_provider_seen_n: rows.filter((r) => r.raw_provider_seen).length,
    quote_provider_seen_n: rows.filter((r) => r.quote_provider_seen).length,
    kline_source_distribution: dist(rows, (r) => r.kline_source ?? '(none)'),
    kline_missing_reasons_top: topReasons(rows.filter((r) => !r.kline_seen), (r) => r.kline_missing_reason ?? 'unknown'),
    confidence_distribution_all: dist(rows, (r) => r.market_data_provenance_confidence),
    confidence_distribution_dog: dist(dogs, (r) => r.market_data_provenance_confidence),
    dog_vs_dud_kline_seen_rate: { dog: rate(dogs.filter((r) => r.kline_seen).length, dogs.length), dud: rate(duds.filter((r) => r.kline_seen).length, duds.length) },
    invalid_for_ev_count: rows.filter((r) => r.row_confidence === 'INVALID_FOR_EV').length,
    invalid_for_promotion_count: rows.filter((r) => r.row_confidence === 'INVALID_FOR_PROMOTION').length,
    recommended_repair_owner_if_material: MARKET_DATA_OWNER,
  };
}

function featureAvailabilityReport(rows) {
  const dogs = rows.filter((r) => r.class === 'dog');
  const fieldPresent = {};
  for (const f of EXPECTED_FEATURES) fieldPresent[f] = rows.filter((r) => (r.feature_vector_fields_present || []).includes(f)).length;
  return {
    schema_version: 'live_fullnet_feature_availability_report.v1',
    note: 'Feature-vector availability per signal from source-to-raw. Field-level present/missing; holders/top10_pct are not exported in this pack.',
    row_count: rows.length,
    dog_count: dogs.length,
    expected_fields: EXPECTED_FEATURES,
    field_present_counts: fieldPresent,
    feature_vector_seen_n: rows.filter((r) => r.feature_vector_seen).length,
    feature_vector_fields_missing_top: topReasons(rows, (r) => (r.feature_vector_fields_missing || []).join('|') || '(none)'),
    dog_vs_dud_feature_seen_rate: {
      dog: rate(dogs.filter((r) => r.feature_vector_seen).length, dogs.length),
      dud: rate(rows.filter((r) => r.class === 'dud' && r.feature_vector_seen).length, rows.filter((r) => r.class === 'dud').length),
    },
  };
}

// repair-owner report refined with market-data detail (ladder + distribution unchanged; itemized)
function repairOwnerReportMd(rows) {
  const ladderIndex = (o) => { const i = REPAIR_LADDER.indexOf(o); return i < 0 ? 99 : i; };
  const dogs = rows.filter((r) => r.class === 'dog');
  const owners = topReasons(rows, (r) => r.final_repair_owner_v2).map((e) => ({
    ...e, ladder_priority: ladderIndex(e.key),
    dog_n: dogs.filter((r) => r.final_repair_owner_v2 === e.key).length,
  })).sort((a, b) => a.ladder_priority - b.ladder_priority || b.n - a.n);
  const mdRows = rows.filter((r) => r.final_repair_owner_v2 === MARKET_DATA_OWNER);
  return {
    schema_version: 'live_fullnet_repair_owner_report.v2_marketdata',
    note: '§15.15.20 ladder unchanged (same predicates => same distribution as v2). The largest owner (market-data) is now itemized by precise sub-reason instead of a coarse black box.',
    ladder: REPAIR_LADDER,
    row_count: rows.length,
    owners,
    market_data_owner_n: mdRows.length,
    market_data_owner_dog_n: mdRows.filter((r) => r.class === 'dog').length,
    market_data_repair_detail_breakdown: topReasons(mdRows, (r) => r.market_data_repair_detail ?? 'unknown'),
  };
}

function rowConfidenceReportMd(rows) {
  const byClass = (cls) => rows.filter((r) => r.class === cls);
  const dist = (s) => countBy(s, (r) => r.row_confidence);
  return {
    schema_version: 'live_fullnet_row_confidence_report.v2_marketdata',
    row_count: rows.length, dog_count: byClass('dog').length, dud_count: byClass('dud').length, pending_count: byClass('pending').length,
    ev_eligible_count: rows.filter((r) => r.ev_eligible).length,
    invalid_for_ev_count: rows.filter((r) => r.row_confidence === 'INVALID_FOR_EV').length,
    distribution_all: dist(rows), distribution_dog: dist(byClass('dog')),
    market_data_confidence_distribution_all: countBy(rows, (r) => r.market_data_provenance_confidence),
  };
}

function evGateMd(rows) {
  const eligible = rows.filter((r) => r.ev_eligible);
  const nets = eligible.map((r) => num(r.net_pnl_pct)).filter((v) => v != null);
  return {
    ev_eligible_n: eligible.length,
    actual_net_ev_pct: nets.length ? Math.round((nets.reduce((a, b) => a + b, 0) / nets.length) * 1e4) / 1e4 : null,
    actual_net_ev_missing_reason: nets.length ? null : 'no_ev_eligible_rows_no_valid_entered_fill_exit_ledger',
    gate: nets.length ? 'ACTUAL_NET_EV_AVAILABLE' : 'BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER',
  };
}

function loadRsoMap(dbPath, tokens) {
  const db = new Database(path.resolve(dbPath), { readonly: true, fileMustExist: true });
  try {
    db.pragma('query_only = ON');
    const cols = 'token_ca, signal_ts, kline_covered, coverage_reason, provider, baseline_provider, pool_found, early_15m_bar_count, early_15m_bar_coverage_pct, first_bar_lag_sec';
    const map = new Map();
    const list = [...new Set(tokens.filter(Boolean))];
    for (let i = 0; i < list.length; i += 300) {
      const chunk = list.slice(i, i + 300);
      const ph = chunk.map((_, j) => `@t${j}`).join(',');
      const params = Object.fromEntries(chunk.map((t, j) => [`t${j}`, t]));
      for (const r of db.prepare(`SELECT ${cols} FROM raw_signal_outcomes WHERE token_ca IN (${ph})`).all(params)) {
        const key = signalKey(r.token_ca, r.signal_ts);
        if (!map.has(key)) map.set(key, r); // first row per (token,signal_ts)
      }
    }
    return map;
  } finally { db.close(); }
}

function loadSourceMap(sourceToRawPath) {
  const j = JSON.parse(fs.readFileSync(path.resolve(sourceToRawPath), 'utf8'));
  const rows = Array.isArray(j) ? j : (j.rows || []);
  const map = new Map();
  for (const r of rows) { const k = signalKey(r.token_ca, r.signal_ts); if (!map.has(k)) map.set(k, r); }
  return map;
}

function buildMarketData(args) {
  const v2 = buildV2(args);
  if (!args.rawSignalOutcomesDb) throw new Error('--raw-signal-outcomes-db is required');
  if (!args.sourceToRaw) throw new Error('--source-to-raw is required (for feature availability)');
  const tokens = v2.rows.map((r) => r.token_ca);
  const rsoMap = loadRsoMap(args.rawSignalOutcomesDb, tokens);
  const srcMap = loadSourceMap(args.sourceToRaw);
  const rows = v2.rows.map((r) => {
    const key = signalKey(r.token_ca, r.signal_ts);
    return augmentMarketDataRow(r, rsoMap.get(key) || null, srcMap.get(key) || null);
  });
  const closure = buildClosureReport(rows);
  const summary = {
    schema_version: 'live_fullnet_row_report_v2_marketdata.v1',
    generated_at: v2.summary.generated_at,
    do_not_change_strategy: true,
    guardrails: { ...v2.summary.guardrails, market_data_provenance_research_only: true, no_live_dir_read: true },
    inputs: { ...v2.summary.inputs, raw_signal_outcomes_db: path.resolve(args.rawSignalOutcomesDb) },
    total_signals: rows.length,
    by_class: v2.summary.by_class,
    projection_complete: v2.summary.projection_complete,
    projection_completeness: v2.summary.projection_completeness,
    ev_gate: evGateMd(rows),
    rso_join_n: rows.filter((r) => r.raw_provider_missing_reason !== 'raw_signal_outcomes_row_absent_for_signal' || r.kline_missing_reason !== 'raw_signal_outcomes_row_absent_for_signal').length,
    market_data_provenance_report: marketDataProvenanceReport(rows),
    feature_availability_report: featureAvailabilityReport(rows),
    row_confidence_report: rowConfidenceReportMd(rows),
    repair_owner_report: repairOwnerReportMd(rows),
    module_closure_coverage: closure,
    phase5_verdict: v2.summary.phase5_verdict,
  };
  return { summary, rows };
}

function writeMarketData(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const w = (name, obj) => { const p = path.join(outDir, name); fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`); return p; };
  const rowPath = path.join(outDir, 'row.jsonl');
  fs.writeFileSync(rowPath, `${report.rows.map((r) => JSON.stringify(r)).join('\n')}\n`);
  return {
    rowPath,
    summaryPath: w('summary.json', report.summary),
    repairOwnerPath: w('repair-owner-report.json', report.summary.repair_owner_report),
    rowConfidencePath: w('row-confidence-report.json', report.summary.row_confidence_report),
    moduleClosurePath: w('final-module-closure-coverage-report.json', report.summary.module_closure_coverage),
    marketDataPath: w('market-data-provenance-report.json', report.summary.market_data_provenance_report),
    featureAvailabilityPath: w('feature-availability-report.json', report.summary.feature_availability_report),
  };
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { timeoutMs: 60_000 };
  const map = {
    '--source-to-raw': 'sourceToRaw', '--source-24h': 'source24h', '--raw-discovery': 'rawDiscovery',
    '--a-class-events': 'aClassEvents', '--ledger-export': 'ledgerExport', '--lifecycle-db': 'lifecycleDb',
    '--raw-signal-outcomes-db': 'rawSignalOutcomesDb', '--out-dir': 'outDir',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--help' || a === '-h') { args.help = true; continue; }
    if (map[a]) { args[map[a]] = argv[i + 1]; i += 1; continue; }
    throw new Error(`Unknown argument: ${a}`);
  }
  return args;
}

function runCli(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log([
      'Usage:',
      '  node scripts/build-live-fullnet-row-v2-marketdata.js \\',
      '    --source-to-raw source-to-raw-rows.json --source-24h source-rows.json \\',
      '    --raw-discovery raw-dog-discovery-24h.json --a-class-events a-class-events-24h-complete.json \\',
      '    --ledger-export canonical-ledger-window.json --lifecycle-db lifecycle_tracks.snapshot.db \\',
      '    --raw-signal-outcomes-db raw_signal_outcomes.snapshot.db --out-dir <dir>',
      '',
      'Reads ONLY artifacts on disk (incl. the raw_signal_outcomes snapshot in the pack). Never reads the',
      'live data dir. Needs better-sqlite3: NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules.',
    ].join('\n'));
    return { help: true };
  }
  if (!args.outDir) throw new Error('--out-dir is required');
  const report = buildMarketData(args);
  const written = writeMarketData(report, path.resolve(args.outDir));
  const md = report.summary.market_data_provenance_report;
  console.log(JSON.stringify({
    ok: true,
    schema_version: report.summary.schema_version,
    total_signals: report.summary.total_signals,
    projection_complete: report.summary.projection_complete,
    ev_gate: report.summary.ev_gate.gate,
    kline_seen_n: md.kline_seen_n,
    reclassified_blocked_to_covered: report.summary.module_closure_coverage.reclassified_from_blocked.map((x) => x.module_group),
    coverage_status_counts: report.summary.module_closure_coverage.coverage_status_counts,
    market_data_owner_detail: report.summary.repair_owner_report.market_data_repair_detail_breakdown,
    paths: written,
  }, null, 2));
  return { report, written };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { runCli(); } catch (e) { console.error(e.stack || e.message); process.exit(1); }
}

export {
  klineProvenance,
  featureAvailability,
  quoteProvenance,
  marketDataConfidence,
  marketDataRepairDetail,
  augmentMarketDataRow,
  buildClosureReport,
  marketDataProvenanceReport,
  featureAvailabilityReport,
  evGateMd,
  buildMarketData,
  EXPECTED_FEATURES,
  KLINE_REASON_MAP,
};
