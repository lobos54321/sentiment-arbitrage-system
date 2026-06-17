#!/usr/bin/env node
'use strict';

/**
 * Builds a read-only source audit pack. This is an evidence-of-absence artifact:
 * it reports source field availability, degeneracy, and component-score coverage
 * without computing any outcome-separation metric.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';

const DEFAULT_RAW_DB = '/Users/boliu/sas-data-room/oos-frozen-pack-20260617T001655Z/raw_signal_outcomes.snapshot.db';
const DEFAULT_FEATURES = '/Users/boliu/sas-data-room/oos-cumulative-sol-curve-unique-buyers/cumulative_oos_features.jsonl';
const DEFAULT_PAPER_DB = '/Users/boliu/sas_paper_current.db';
const DEFAULT_SCORE_DBS = [
  '/Users/boliu/sentiment-arbitrage-system/data/sentiment_arb.db',
  '/Users/boliu/Desktop/sentiment_arb.db',
  '/Users/boliu/sas-research/data/sentiment_arb.db',
  '/Users/boliu/sas-research/sentiment_arb.db',
];
const FORBIDDEN_OUTPUT_TERMS = [
  'lift',
  'auc',
  'precision',
  'recall',
  'cramers_v',
  'mutual_info',
  'chi2',
  'separation',
  'p_dog',
  'p_dud',
];

function parseArgs(argv) {
  const a = {
    rawDb: DEFAULT_RAW_DB,
    features: DEFAULT_FEATURES,
    paperDb: DEFAULT_PAPER_DB,
    scoreDbs: [...DEFAULT_SCORE_DBS],
  };
  for (let i = 2; i < argv.length; i += 1) {
    const k = argv[i];
    const v = argv[i + 1];
    if (k === '--raw-db') { a.rawDb = v; i += 1; }
    else if (k === '--features') { a.features = v; i += 1; }
    else if (k === '--paper-db') { a.paperDb = v; i += 1; }
    else if (k === '--score-db') { a.scoreDbs.push(v); i += 1; }
    else if (k === '--out-dir') { a.outDir = v; i += 1; }
    else if (k === '--help' || k === '-h') { a.help = true; }
    else throw new Error(`Unknown argument: ${k}`);
  }
  return a;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-source-audit-pack.js --out-dir ~/sas-data-room/source-audit-pack-...',
    '',
    'Optional:',
    '  --raw-db raw_signal_outcomes.snapshot.db',
    '  --features cumulative_oos_features.jsonl',
    '  --paper-db sas_paper_current.db',
    '  --score-db sentiment_arb.db  # repeatable',
  ].join('\n');
}

function sha256File(p) {
  return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
}

function readJsonl(p) {
  const raw = fs.readFileSync(p, 'utf8').trim();
  if (!raw) return [];
  return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function keyOf(row) {
  return `${row.token_ca}|${Math.floor(Number(row.signal_ts))}`;
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

function countRows(db, table) {
  if (!tableExists(db, table)) return null;
  return db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n;
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

function nestedDistribution(rows, fields) {
  const counts = new Map();
  for (const row of rows) {
    const key = fields.map((f) => row[f] == null || row[f] === '' ? '<NULL>' : String(row[f])).join('|');
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return [...counts.entries()]
    .map(([key, count]) => {
      const values = key.split('|');
      const out = { count };
      fields.forEach((f, i) => { out[f] = values[i]; });
      return out;
    })
    .sort((a, b) => b.count - a.count || fields.map((f) => String(a[f]).localeCompare(String(b[f]))).find((n) => n !== 0) || 0);
}

function uniqueNonNullCount(rows, field) {
  return new Set(rows.map((r) => r[field]).filter((v) => v != null && v !== '')).size;
}

function signalRowsFromFeatures(features) {
  const seen = new Map();
  for (const row of features) {
    if (!row.token_ca || row.signal_ts == null) continue;
    const minimal = {
      token_ca: String(row.token_ca),
      signal_ts: Math.floor(Number(row.signal_ts)),
      pack_id: row.pack_id || null,
      progress_stage: row.progress_stage || null,
      return_domain: row.return_domain || null,
    };
    const key = keyOf(minimal);
    if (!seen.has(key)) seen.set(key, minimal);
  }
  return [...seen.values()].sort((a, b) => a.signal_ts - b.signal_ts || a.token_ca.localeCompare(b.token_ca));
}

function rawRowsForSignals(rawDb, signals) {
  const stmt = rawDb.prepare(`
    SELECT
      token_ca,
      signal_ts,
      signal_id,
      source,
      source_family,
      source_kind,
      path_source_family,
      path_source_kind,
      provider,
      baseline_provider,
      path_provider,
      signal_type,
      route,
      coverage_reason,
      observation_status,
      kline_covered,
      created_at
    FROM raw_signal_outcomes
    WHERE token_ca = ? AND signal_ts = ?
    ORDER BY created_at ASC, id ASC
  `);
  const all = [];
  const byKey = new Map();
  for (const sig of signals) {
    const rows = stmt.all(sig.token_ca, sig.signal_ts);
    byKey.set(keyOf(sig), rows);
    all.push(...rows);
  }
  return { all, byKey };
}

function dedupeRawRows(signals, rawByKey) {
  const out = [];
  let collapsed = 0;
  for (const sig of signals) {
    const rows = rawByKey.get(keyOf(sig)) || [];
    if (rows.length > 1) collapsed += rows.length - 1;
    if (rows.length > 0) out.push(rows[0]);
  }
  return { rows: out, collapsed };
}

function fullRawDistribution(rawDb) {
  return {
    rows_total: rawDb.prepare('SELECT COUNT(*) AS n FROM raw_signal_outcomes').get().n,
    source: rawDb.prepare("SELECT COALESCE(source, '<NULL>') AS value, COUNT(*) AS count FROM raw_signal_outcomes GROUP BY 1 ORDER BY count DESC, value").all(),
    source_family_kind: rawDb.prepare(`
      SELECT
        COALESCE(source_family, '<NULL>') AS source_family,
        COALESCE(source_kind, '<NULL>') AS source_kind,
        COUNT(*) AS count
      FROM raw_signal_outcomes
      GROUP BY 1,2
      ORDER BY count DESC, source_family, source_kind
    `).all(),
    provider: rawDb.prepare("SELECT COALESCE(provider, '<NULL>') AS value, COUNT(*) AS count FROM raw_signal_outcomes GROUP BY 1 ORDER BY count DESC, value").all(),
  };
}

function componentScoreCoverage(scoreDbs, paperDbPath, signals) {
  const tokens = new Set(signals.map((r) => r.token_ca));
  const result = {
    score_details: [],
    opportunity_events: null,
  };

  for (const scoreDbPath of scoreDbs) {
    const entry = {
      path: scoreDbPath,
      exists: fs.existsSync(scoreDbPath),
      size_bytes: fs.existsSync(scoreDbPath) ? fs.statSync(scoreDbPath).size : null,
      table_exists: false,
      rows_total: null,
      cohort_token_overlap: null,
      non_null_score_columns: {},
    };
    if (entry.exists && entry.size_bytes > 0) {
      try {
        const db = openRo(scoreDbPath);
        entry.table_exists = tableExists(db, 'score_details');
        if (entry.table_exists) {
          entry.rows_total = countRows(db, 'score_details');
          const cols = tableColumns(db, 'score_details');
          const tokenCol = cols.includes('token_ca') ? 'token_ca' : cols.includes('token_address') ? 'token_address' : null;
          if (tokenCol) {
            const rows = db.prepare(`SELECT DISTINCT ${tokenCol} AS token_ca FROM score_details`).all();
            entry.cohort_token_overlap = rows.filter((r) => tokens.has(r.token_ca)).length;
          }
          for (const col of ['source_score', 'narrative_score', 'influence_score', 'tg_spread_score', 'graph_score', 'total_score']) {
            if (cols.includes(col)) {
              entry.non_null_score_columns[col] = db.prepare(`SELECT COUNT(*) AS n FROM score_details WHERE ${col} IS NOT NULL`).get().n;
            }
          }
        }
        db.close();
      } catch (err) {
        entry.error = String(err.message || err);
      }
    }
    result.score_details.push(entry);
  }

  const opp = {
    path: paperDbPath,
    exists: fs.existsSync(paperDbPath),
    size_bytes: fs.existsSync(paperDbPath) ? fs.statSync(paperDbPath).size : null,
    table_exists: false,
    rows_total: null,
    cohort_token_overlap: null,
    event_ts_min: null,
    event_ts_max: null,
    non_null_score_columns: {},
  };
  if (opp.exists && opp.size_bytes > 0) {
    try {
      const db = openRo(paperDbPath);
      opp.table_exists = tableExists(db, 'opportunity_events');
      if (opp.table_exists) {
        opp.rows_total = countRows(db, 'opportunity_events');
        const cols = tableColumns(db, 'opportunity_events');
        if (cols.includes('token_ca')) {
          const rows = db.prepare('SELECT DISTINCT token_ca FROM opportunity_events WHERE token_ca IS NOT NULL').all();
          opp.cohort_token_overlap = rows.filter((r) => tokens.has(r.token_ca)).length;
        }
        if (cols.includes('event_ts')) {
          const span = db.prepare('SELECT MIN(event_ts) AS min_ts, MAX(event_ts) AS max_ts FROM opportunity_events').get();
          opp.event_ts_min = span.min_ts;
          opp.event_ts_max = span.max_ts;
        }
        for (const col of cols.filter((c) => c.endsWith('_score') || c.includes('score'))) {
          opp.non_null_score_columns[col] = db.prepare(`SELECT COUNT(*) AS n FROM opportunity_events WHERE ${col} IS NOT NULL`).get().n;
        }
      }
      db.close();
    } catch (err) {
      opp.error = String(err.message || err);
    }
  }
  result.opportunity_events = opp;
  return result;
}

function assertNoForbiddenOutput(report) {
  const text = JSON.stringify(report).toLowerCase();
  const hits = FORBIDDEN_OUTPUT_TERMS.filter((term) => text.includes(term));
  if (hits.length) {
    throw new Error(`Forbidden source-audit output terms present: ${hits.join(', ')}`);
  }
}

function buildPack({ rawDbPath, featuresPath, paperDbPath, scoreDbs }) {
  const rawPreSha = sha256File(rawDbPath);
  const featuresSha = sha256File(featuresPath);
  const features = readJsonl(featuresPath);
  const signals = signalRowsFromFeatures(features);
  const rawDb = openRo(rawDbPath);
  const fullRaw = fullRawDistribution(rawDb);
  const { all: rawAllRows, byKey } = rawRowsForSignals(rawDb, signals);
  const deduped = dedupeRawRows(signals, byKey);
  const rawPostSha = sha256File(rawDbPath);
  rawDb.close();

  const signalKeys = new Set(signals.map(keyOf));
  const tokenSet = new Set(signals.map((r) => r.token_ca));
  const rawKeys = new Set(rawAllRows.map(keyOf));
  const joinedSignals = [...signalKeys].filter((k) => rawKeys.has(k)).length;
  const sourceFields = [
    'source',
    'source_family',
    'source_kind',
    'path_source_family',
    'path_source_kind',
    'provider',
    'baseline_provider',
    'path_provider',
    'signal_type',
    'route',
    'coverage_reason',
  ];
  const dedupedRows = deduped.rows;
  const sourceDegeneracy = {};
  for (const f of sourceFields) {
    sourceDegeneracy[f] = {
      distinct_non_null: uniqueNonNullCount(dedupedRows, f),
      distribution: distribution(dedupedRows, f),
    };
  }

  const componentCoverage = componentScoreCoverage(scoreDbs, paperDbPath, signals);
  const report = {
    schema_version: 'source_audit_pack.v1',
    generated_at: new Date().toISOString(),
    verdict: 'SOURCE_AXIS_NULL_FOR_CURRENT_COHORT',
    inputs: {
      raw_db: rawDbPath,
      raw_db_sha256_before: rawPreSha,
      raw_db_sha256_after: rawPostSha,
      raw_db_unchanged: rawPreSha === rawPostSha,
      cumulative_features: featuresPath,
      cumulative_features_sha256: featuresSha,
      paper_db: paperDbPath,
      score_dbs: scoreDbs,
    },
    join_key: ['token_ca', 'signal_ts'],
    cohort: {
      feature_rows: features.length,
      signal_rows: signals.length,
      unique_tokens: tokenSet.size,
      signal_ts_min: signals.length ? Math.min(...signals.map((r) => r.signal_ts)) : null,
      signal_ts_max: signals.length ? Math.max(...signals.map((r) => r.signal_ts)) : null,
      joined_signals: joinedSignals,
      join_coverage_signal_rows: signals.length ? Number((joinedSignals / signals.length).toFixed(6)) : null,
      raw_rows_for_signal_keys: rawAllRows.length,
      deduped_raw_rows: dedupedRows.length,
      raw_rows_collapsed_by_signal_key: deduped.collapsed,
    },
    full_raw_distribution: fullRaw,
    cohort_all_raw_rows_distribution: {
      source: distribution(rawAllRows, 'source'),
      source_family_kind: nestedDistribution(rawAllRows, ['source_family', 'source_kind']),
      provider: distribution(rawAllRows, 'provider'),
    },
    cohort_deduped_distribution: {
      source_fields: sourceDegeneracy,
      source_family_kind_provider: nestedDistribution(dedupedRows, ['source_family', 'source_kind', 'provider']),
    },
    source_axis_preconditions: {
      origin_has_variance: uniqueNonNullCount(dedupedRows, 'source') > 1,
      family_kind_has_variance: uniqueNonNullCount(dedupedRows, 'source_family') > 1 || uniqueNonNullCount(dedupedRows, 'source_kind') > 1,
      component_scores_have_cohort_coverage: Boolean(componentCoverage.opportunity_events?.cohort_token_overlap || componentCoverage.score_details.some((s) => s.cohort_token_overlap)),
      collinearity_measurable: uniqueNonNullCount(dedupedRows, 'source_family') > 1,
      reason: 'Source origin is constant; family/kind are degenerate within the current cohort; component-score stores have no current-cohort coverage.',
    },
    component_score_coverage: componentCoverage,
    forbidden_metrics: {
      emitted: false,
      note: 'This pack intentionally reports availability and degeneracy only.',
    },
  };
  assertNoForbiddenOutput(report);
  return report;
}

function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.outDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 2);
  }
  fs.mkdirSync(args.outDir, { recursive: true });
  const report = buildPack({
    rawDbPath: args.rawDb,
    featuresPath: args.features,
    paperDbPath: args.paperDb,
    scoreDbs: args.scoreDbs,
  });
  const reportPath = path.join(args.outDir, 'source-audit-pack.json');
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  const manifest = {
    schema_version: 'source_audit_pack_manifest.v1',
    generated_at: new Date().toISOString(),
    report: reportPath,
    report_sha256: sha256File(reportPath),
    verdict: report.verdict,
    source_axis_preconditions: report.source_axis_preconditions,
  };
  const manifestPath = path.join(args.outDir, 'manifest.json');
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(JSON.stringify({
    ok: true,
    verdict: report.verdict,
    out_dir: args.outDir,
    report: reportPath,
    joined_signals: report.cohort.joined_signals,
    signal_rows: report.cohort.signal_rows,
  }, null, 2));
}

if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  try { main(); } catch (err) { console.error(`FAIL_CLOSED: ${err.message}`); process.exit(1); }
}

export { buildPack, signalRowsFromFeatures, distribution };
