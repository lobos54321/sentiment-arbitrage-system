#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { execFileSync } from 'child_process';
import { pathToFileURL } from 'url';

import {
  buildRawDogDecisionFunnel,
} from '../src/analytics/raw-dog-decision-funnel.js';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    paperDb: '',
    dogs: '',
    duds: '',
    outDir: '',
    decisionWindowSec: 900,
    preSignalGraceSec: 60,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--paper-db') { args.paperDb = next; i += 1; continue; }
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--decision-window-sec') { args.decisionWindowSec = Number(next); i += 1; continue; }
    if (key === '--pre-signal-grace-sec') { args.preSignalGraceSec = Number(next); i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-v10-decision-funnel-audit.js --paper-db paper_decision_subset.db --dogs rebuilt-clean-dogs.json --duds rebuilt-clean-duds.json --out-dir ./audit-out',
    '',
    'Consumes the v10 clean cohort JSON and a paper decision subset DB.',
    'Outputs signal-level raw→decision→quote/executable→would_enter funnel counts.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (Number.isFinite(n)) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : n;
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return Math.floor(parsed / 1000);
  }
  return null;
}

function readRows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.rows)) return parsed.rows;
  if (Array.isArray(parsed.results)) return parsed.results;
  throw new Error(`Expected array-like JSON rows in ${filePath}`);
}

function normalizeCohortRow(row = {}, cohortKind) {
  const correctedPeakPct = numeric(row.corrected_peak_pct);
  const rawTier = row.effective_tier || row.raw_primary_tier || row.tier || (cohortKind === 'dog' ? 'gold_or_silver' : 'dud');
  return {
    ...row,
    signal_id: row.signal_id || row.adjudication_key || `${row.token_ca || ''}|${row.signal_ts || ''}`,
    token_ca: row.token_ca,
    symbol: row.symbol || null,
    signal_ts: numeric(row.signal_ts),
    lifecycle_id: row.lifecycle_id || null,
    raw_primary_tier: rawTier,
    max_sustained_peak_pct: correctedPeakPct,
    max_wick_peak_pct: correctedPeakPct,
    time_to_sustained_peak_sec: numeric(row.time_to_sustained_peak_sec),
    raw_dog_entered: false,
    raw_dog_realized: false,
    held_to_silver: false,
    held_to_gold: false,
    cohort_kind: cohortKind,
    return_domain: row.return_domain || null,
    return_calculation_rule: row.return_calculation_rule || null,
  };
}

function runSqlJson(dbPath, sql) {
  const out = execFileSync('sqlite3', ['-json', dbPath, sql], {
    encoding: 'utf8',
    maxBuffer: 200 * 1024 * 1024,
  }).trim();
  return out ? JSON.parse(out) : [];
}

function tableExists(dbPath, tableName) {
  const rows = runSqlJson(dbPath, `SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='${tableName}' LIMIT 1;`);
  return rows.length > 0;
}

function readDecisionRecords(dbPath) {
  const out = [];
  if (tableExists(dbPath, 'a_class_decision_events')) {
    out.push(...runSqlJson(dbPath, `
      SELECT
        id,
        'a_class_decision_events' AS source_kind,
        event_ts,
        token_ca,
        symbol,
        lifecycle_id,
        NULL AS source_component,
        NULL AS source_reason,
        action,
        would_action,
        reason,
        hard_blockers_json,
        expected_rr,
        score,
        grade,
        block_cause,
        recoverability,
        classification_reason,
        quote_available,
        quote_executable,
        quote_clean,
        route_available,
        evidence_status,
        provider_hydrate_outcome,
        provider_reason,
        quote_failure_reason,
        route_failure_reason,
        NULL AS hydrate_outcome,
        NULL AS would_enter_a_class,
        NULL AS did_enter
      FROM a_class_decision_events
      ORDER BY event_ts ASC, id ASC;
    `));
  }
  if (tableExists(dbPath, 'opportunity_events')) {
    out.push(...runSqlJson(dbPath, `
      SELECT
        id,
        'opportunity_events' AS source_kind,
        event_ts,
        token_ca,
        symbol,
        lifecycle_id,
        source_component,
        source_reason,
        CASE
          WHEN COALESCE(did_enter, 0) = 1 THEN 'ENTER'
          WHEN COALESCE(would_enter_a_class, 0) = 1 THEN 'WOULD_ENTER'
          ELSE 'BLOCK'
        END AS action,
        NULL AS would_action,
        quote_failure_reason AS reason,
        hard_blockers_json,
        expected_rr,
        matrix_score AS score,
        NULL AS grade,
        block_cause,
        recoverability,
        classification_reason,
        quote_available,
        quote_executable,
        quote_clean,
        route_available,
        evidence_status,
        hydrate_outcome AS provider_hydrate_outcome,
        provider_reason,
        quote_failure_reason,
        NULL AS route_failure_reason,
        hydrate_outcome,
        would_enter_a_class,
        did_enter
      FROM opportunity_events
      ORDER BY event_ts ASC, id ASC;
    `));
  }
  return out.sort((a, b) => Number(a.event_ts || 0) - Number(b.event_ts || 0));
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows) {
    const value = typeof field === 'function' ? field(row) : row[field];
    const key = String(value ?? 'unknown');
    out[key] = Number(out[key] || 0) + 1;
  }
  return out;
}

function summarizeFunnel(funnel) {
  const rows = funnel.dogs || [];
  return {
    summary: funnel.summary,
    terminal_buckets: countBy(rows, 'terminal_bucket'),
    return_domain: countBy(rows, 'return_domain'),
    matched_by: countBy(rows, 'matched_by'),
    best_source_kind: countBy(rows, (row) => row.best_decision_record?.source_kind || 'none'),
    hydrate_outcome: countBy(rows, (row) => row.best_decision_record?.provider_hydrate_outcome || 'not_recorded'),
  };
}

function writeReport(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const jsonPath = path.join(outDir, 'v10-decision-funnel-audit.json');
  const mdPath = path.join(outDir, 'v10-decision-funnel-audit.md');
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  const lines = [
    '# V10 Decision Funnel Audit',
    '',
    `Generated: ${report.generated_at}`,
    '',
    '## Inputs',
    '',
    '```json',
    JSON.stringify(report.inputs, null, 2),
    '```',
    '',
    '## Dogs Funnel',
    '',
    '```json',
    JSON.stringify(report.dogs.summary, null, 2),
    '```',
    '',
    '## Duds Funnel',
    '',
    '```json',
    JSON.stringify(report.duds.summary, null, 2),
    '```',
    '',
    '## Guardrail',
    '',
    report.guardrail,
    '',
  ];
  fs.writeFileSync(mdPath, `${lines.join('\n')}\n`);
  return { jsonPath, mdPath };
}

function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.paperDb || !fs.existsSync(args.paperDb)) throw new Error('Provide --paper-db');
  if (!args.dogs || !fs.existsSync(args.dogs)) throw new Error('Provide --dogs');
  if (!args.duds || !fs.existsSync(args.duds)) throw new Error('Provide --duds');
  if (!args.outDir) throw new Error('Provide --out-dir');

  const dogRows = readRows(args.dogs).map((row) => normalizeCohortRow(row, 'dog'));
  const dudRows = readRows(args.duds).map((row) => normalizeCohortRow(row, 'dud'));
  const decisionRecords = readDecisionRecords(args.paperDb);

  const funnelOptions = {
    decisionRecords,
    decisionWindowSec: args.decisionWindowSec,
    preSignalGraceSec: args.preSignalGraceSec,
  };
  const dogsFunnel = buildRawDogDecisionFunnel({ rawDogs: dogRows, ...funnelOptions });
  const dudsFunnel = buildRawDogDecisionFunnel({ rawDogs: dudRows, ...funnelOptions });
  const report = {
    schema_version: 'v10_decision_funnel_audit.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      paper_db: args.paperDb,
      dogs_json: args.dogs,
      duds_json: args.duds,
      dogs_n: dogRows.length,
      duds_n: dudRows.length,
      decision_records_n: decisionRecords.length,
      decision_window_sec: args.decisionWindowSec,
      pre_signal_grace_sec: args.preSignalGraceSec,
    },
    dogs: summarizeFunnel(dogsFunnel),
    duds: summarizeFunnel(dudsFunnel),
    guardrail: 'This report is valid only if the paper decision subset covers the full v10 signal window. Partial smoke packs validate wiring only and must not be used for strategy conclusions.',
  };
  const paths = writeReport(report, args.outDir);
  console.log(JSON.stringify({
    ok: true,
    paths,
    dogs: report.dogs.summary,
    duds: report.duds.summary,
  }, null, 2));
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error) {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  }
}
