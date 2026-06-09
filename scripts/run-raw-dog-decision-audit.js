#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';

import {
  buildRawDogDecisionAudit,
  markdownForAudit,
} from '../src/analytics/raw-dog-decision-audit.js';

const projectRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {
    hours: 24,
    maxDuds: 200,
    rawDb: process.env.RAW_SIGNAL_OUTCOMES_DB || path.join(projectRoot, 'data', 'raw_signal_outcomes.db'),
    paperDb: process.env.PAPER_DB || path.join(projectRoot, 'data', 'paper_trades.db'),
    outDir: path.join(projectRoot, 'data', 'audits', 'raw-dog-decision'),
    timeoutMs: 60_000,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === '--hours') { out.hours = Number(next); i += 1; }
    else if (arg === '--max-duds') { out.maxDuds = Number(next); i += 1; }
    else if (arg === '--raw-db') { out.rawDb = next; i += 1; }
    else if (arg === '--paper-db') { out.paperDb = next; i += 1; }
    else if (arg === '--out-dir') { out.outDir = next; i += 1; }
    else if (arg === '--timeout-ms') { out.timeoutMs = Number(next); i += 1; }
    else if (arg === '--help' || arg === '-h') {
      out.help = true;
    }
  }
  out.hours = Math.max(1, Math.min(72, Number.isFinite(out.hours) ? out.hours : 24));
  out.maxDuds = Math.max(0, Math.min(1000, Number.isFinite(out.maxDuds) ? out.maxDuds : 200));
  out.timeoutMs = Math.max(5_000, Math.min(300_000, Number.isFinite(out.timeoutMs) ? out.timeoutMs : 60_000));
  return out;
}

function usage() {
  return [
    'Usage: node scripts/run-raw-dog-decision-audit.js [options]',
    '',
    'Options:',
    '  --hours 24',
    '  --max-duds 200',
    '  --raw-db ./data/raw_signal_outcomes.db',
    '  --paper-db ./data/paper_trades.db',
    '  --out-dir ./data/audits/raw-dog-decision',
    '  --timeout-ms 60000',
  ].join('\n');
}

function openReadonlySqlite(filePath, timeoutMs = 1500) {
  const db = new Database(filePath, { readonly: true, fileMustExist: true, timeout: timeoutMs });
  db.pragma('mmap_size = 0');
  db.pragma(`busy_timeout = ${Math.max(0, Number(timeoutMs) || 1500)}`);
  db.pragma('query_only = ON');
  return db;
}

function tableNames(db) {
  return new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
}

function tableColumns(db, tableName) {
  return new Set(db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name));
}

function optional(cols, name, fallback = 'NULL') {
  return cols.has(name) ? name : `${fallback} AS ${name}`;
}

function expr(cols, name, fallback = 'NULL') {
  return cols.has(name) ? name : fallback;
}

function tokenChunks(tokens, chunkSize = 200) {
  const out = [];
  for (let i = 0; i < tokens.length; i += chunkSize) out.push(tokens.slice(i, i + chunkSize));
  return out;
}

function rawEligibleSql() {
  return `
    observation_status = 'matured'
    AND COALESCE(kline_covered, 0) = 1
    AND baseline_confidence IN ('high', 'medium')
    AND COALESCE(same_source_path, 0) = 1
    AND COALESCE(outlier_flag, 0) = 0
    AND COALESCE(sustained_evaluable, 0) = 1
  `;
}

function rawLifecycleSql(cols) {
  if (cols.has('lifecycle_id') && cols.has('downstream_lifecycle_id')) {
    return 'COALESCE(lifecycle_id, downstream_lifecycle_id) AS lifecycle_id';
  }
  if (cols.has('lifecycle_id')) return 'lifecycle_id';
  if (cols.has('downstream_lifecycle_id')) return 'downstream_lifecycle_id AS lifecycle_id';
  return 'NULL AS lifecycle_id';
}

function attachEntryVolumeFeatures(rawDb, rows = []) {
  if (!rows.length || !tableNames(rawDb).has('raw_price_bars_1m')) return rows;
  const firstBar = rawDb.prepare(`
    SELECT timestamp, volume
    FROM raw_price_bars_1m
    WHERE token_ca = ?
      AND timestamp >= ?
      AND timestamp <= ?
    ORDER BY timestamp ASC
    LIMIT 1
  `);
  const earlyVolume = rawDb.prepare(`
    SELECT
      SUM(CASE WHEN timestamp < ? + 300 THEN COALESCE(volume, 0) ELSE 0 END) AS early_5m_volume,
      SUM(COALESCE(volume, 0)) AS early_15m_volume,
      COUNT(*) AS early_15m_volume_bar_count
    FROM raw_price_bars_1m
    WHERE token_ca = ?
      AND timestamp >= ?
      AND timestamp < ? + 900
  `);
  return rows.map((row) => {
    const token = String(row.token_ca || '').trim();
    const signalTs = Number(row.signal_ts);
    if (!token || !Number.isFinite(signalTs)) return row;
    const first = firstBar.get(token, signalTs, signalTs + 900) || {};
    const early = earlyVolume.get(signalTs, token, signalTs, signalTs) || {};
    return {
      ...row,
      entry_bar_ts: first.timestamp ?? null,
      entry_bar_volume: first.volume ?? null,
      early_5m_volume: early.early_5m_volume ?? null,
      early_15m_volume: early.early_15m_volume ?? null,
      early_15m_volume_bar_count: early.early_15m_volume_bar_count ?? null,
    };
  });
}

function readRawRows(rawDb, { sinceTs, maxDuds }) {
  const eligible = rawEligibleSql();
  const rawCols = tableColumns(rawDb, 'raw_signal_outcomes');
  const innerSelect = `
    signal_id, symbol, token_ca, ${rawLifecycleSql(rawCols)}, signal_ts, raw_primary_tier,
    max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
    baseline_confidence, coverage_reason, did_enter, held_to_silver,
    held_to_gold, raw_dog_entered, raw_dog_realized, exit_reason
  `;
  const outerSelect = `
    signal_id, symbol, token_ca, lifecycle_id, signal_ts, raw_primary_tier,
    max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
    baseline_confidence, coverage_reason, did_enter, held_to_silver,
    held_to_gold, raw_dog_entered, raw_dog_realized, exit_reason
  `;
  const rawDogs = rawDb.prepare(`
    WITH ranked AS (
      SELECT
        ${innerSelect},
        ROW_NUMBER() OVER (
          PARTITION BY token_ca
          ORDER BY COALESCE(max_sustained_peak_pct, 0) DESC, signal_ts DESC, signal_id DESC
        ) AS rn
      FROM raw_signal_outcomes
      WHERE signal_ts >= @since
        AND ${eligible}
        AND raw_primary_tier IN ('gold', 'silver')
        AND token_ca IS NOT NULL
        AND token_ca != ''
    )
    SELECT ${outerSelect}
    FROM ranked
    WHERE rn = 1
    ORDER BY COALESCE(max_sustained_peak_pct, 0) DESC
  `).all({ since: sinceTs });
  const duds = rawDb.prepare(`
    WITH ranked AS (
      SELECT
        ${innerSelect},
        ROW_NUMBER() OVER (
          PARTITION BY token_ca
          ORDER BY signal_ts DESC, COALESCE(max_sustained_peak_pct, 0) DESC, signal_id DESC
        ) AS rn
      FROM raw_signal_outcomes
      WHERE signal_ts >= @since
        AND ${eligible}
        AND COALESCE(raw_primary_tier, '') NOT IN ('gold', 'silver')
        AND token_ca IS NOT NULL
        AND token_ca != ''
    )
    SELECT ${outerSelect}
    FROM ranked
    WHERE rn = 1
    ORDER BY signal_ts DESC
    LIMIT @limit
  `).all({ since: sinceTs, limit: maxDuds });
  return {
    rawDogs: attachEntryVolumeFeatures(rawDb, rawDogs),
    dudCandidates: attachEntryVolumeFeatures(rawDb, duds),
  };
}

function decisionQueryWindow(rows = [], fallbackSinceTs, fallbackUntilTs) {
  const windows = rows.map((row) => {
    const signalTs = Number(row.signal_ts);
    if (!Number.isFinite(signalTs)) return null;
    const peakSec = Number(row.time_to_sustained_peak_sec);
    const endOffset = Number.isFinite(peakSec) && peakSec > 0 ? Math.max(60, Math.min(900, peakSec)) : 900;
    return { start: signalTs - 60, end: signalTs + endOffset };
  }).filter(Boolean);
  return {
    since: windows.length ? Math.min(...windows.map((row) => row.start)) : fallbackSinceTs,
    until: windows.length ? Math.max(...windows.map((row) => row.end)) : fallbackUntilTs,
  };
}

function readDecisionRecords(paperDb, rows = [], { sinceTs, untilTs }) {
  const tokens = [...new Set(rows.map((row) => String(row.token_ca || '').trim()).filter(Boolean))];
  if (!tokens.length) return [];
  const tables = tableNames(paperDb);
  const queryWindow = decisionQueryWindow(rows, sinceTs, untilTs);
  const out = [];
  const runChunks = (sqlForPlaceholders) => {
    for (const chunk of tokenChunks(tokens)) {
      const placeholders = chunk.map(() => '?').join(',');
      out.push(...sqlForPlaceholders(placeholders).all(queryWindow.since, queryWindow.until, ...chunk));
    }
  };
  if (tables.has('a_class_decision_events')) {
    const cols = tableColumns(paperDb, 'a_class_decision_events');
    runChunks((placeholders) => paperDb.prepare(`
      SELECT
        id,
        'a_class_decision_events' AS source_kind,
        event_ts,
        token_ca,
        ${optional(cols, 'symbol')},
        ${optional(cols, 'lifecycle_id')},
        ${optional(cols, 'source_component')},
        ${optional(cols, 'source_reason')},
        ${optional(cols, 'action', "'BLOCK'")},
        ${optional(cols, 'would_action')},
        ${optional(cols, 'reason')},
        ${optional(cols, 'hard_blockers_json', "'[]'")},
        ${optional(cols, 'expected_rr')},
        ${optional(cols, 'score')},
        ${optional(cols, 'grade')},
        ${optional(cols, 'block_cause')},
        ${optional(cols, 'recoverability')},
        ${optional(cols, 'quote_available')},
        ${optional(cols, 'quote_executable')},
        ${optional(cols, 'quote_clean')},
        ${optional(cols, 'route_available')},
        ${optional(cols, 'evidence_status')},
        ${optional(cols, 'provider_reason')},
        ${optional(cols, 'quote_failure_reason')},
        ${optional(cols, 'route_failure_reason')},
        NULL AS would_enter_a_class,
        NULL AS did_enter
      FROM a_class_decision_events
      WHERE event_ts >= ?
        AND event_ts <= ?
        AND token_ca IN (${placeholders})
      ORDER BY event_ts ASC, id ASC
    `));
  }
  if (tables.has('opportunity_events')) {
    const cols = tableColumns(paperDb, 'opportunity_events');
    runChunks((placeholders) => paperDb.prepare(`
      SELECT
        id,
        'opportunity_events' AS source_kind,
        event_ts,
        token_ca,
        ${optional(cols, 'symbol')},
        ${optional(cols, 'lifecycle_id')},
        ${optional(cols, 'source_component')},
        ${optional(cols, 'source_reason')},
        CASE
          WHEN COALESCE(${expr(cols, 'did_enter', '0')}, 0) = 1 THEN 'ENTER'
          WHEN COALESCE(${expr(cols, 'would_enter_a_class', '0')}, 0) = 1 THEN 'WOULD_ENTER'
          ELSE 'BLOCK'
        END AS action,
        NULL AS would_action,
        ${expr(cols, 'quote_failure_reason', 'NULL')} AS reason,
        ${optional(cols, 'hard_blockers_json', "'[]'")},
        ${optional(cols, 'expected_rr')},
        ${expr(cols, 'matrix_score', 'NULL')} AS score,
        NULL AS grade,
        ${optional(cols, 'block_cause')},
        ${optional(cols, 'recoverability')},
        ${optional(cols, 'quote_available')},
        ${optional(cols, 'quote_executable')},
        ${optional(cols, 'quote_clean')},
        ${optional(cols, 'route_available')},
        ${optional(cols, 'evidence_status')},
        ${optional(cols, 'provider_reason')},
        ${optional(cols, 'quote_failure_reason')},
        ${optional(cols, 'route_failure_reason')},
        ${optional(cols, 'would_enter_a_class', '0')},
        ${optional(cols, 'did_enter', '0')}
      FROM opportunity_events
      WHERE event_ts >= ?
        AND event_ts <= ?
        AND token_ca IN (${placeholders})
      ORDER BY event_ts ASC, id ASC
    `));
  }
  return out.sort((a, b) => Number(a.event_ts || 0) - Number(b.event_ts || 0) || Number(a.id || 0) - Number(b.id || 0));
}

function writeReports(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('T', '_').replace('Z', 'Z');
  const jsonPath = path.join(outDir, `raw_dog_decision_audit_${ts}.json`);
  const mdPath = path.join(outDir, `raw_dog_decision_audit_${ts}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, markdownForAudit(report));
  fs.copyFileSync(jsonPath, path.join(outDir, 'latest.json'));
  fs.copyFileSync(mdPath, path.join(outDir, 'latest.md'));
  return { jsonPath, mdPath, latestJsonPath: path.join(outDir, 'latest.json'), latestMdPath: path.join(outDir, 'latest.md') };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  const started = Date.now();
  const untilTs = Math.floor(started / 1000);
  const sinceTs = untilTs - args.hours * 3600;
  let rawDb;
  let paperDb;
  try {
    rawDb = openReadonlySqlite(args.rawDb, 1500);
    paperDb = openReadonlySqlite(args.paperDb, 1500);
    if (!tableNames(rawDb).has('raw_signal_outcomes')) {
      throw new Error('raw_signal_outcomes table missing');
    }
    const { rawDogs, dudCandidates } = readRawRows(rawDb, { sinceTs, maxDuds: args.maxDuds });
    const decisions = readDecisionRecords(paperDb, [...rawDogs, ...dudCandidates], { sinceTs, untilTs });
    const report = buildRawDogDecisionAudit({
      rawDogs,
      dudCandidates,
      decisionRecords: decisions,
      hours: args.hours,
      sinceTs,
      untilTs,
      rawDbPath: args.rawDb,
      paperDbPath: args.paperDb,
      maxDuds: args.maxDuds,
    });
    const paths = writeReports(report, args.outDir);
    console.log(JSON.stringify({
      ok: true,
      status: report.status,
      generated_at: report.generated_at,
      raw_dogs_n: report.inputs.raw_dogs_n,
      dud_sample_n: report.inputs.dud_sample_n,
      quote_clean_no_would_enter_raw_dogs_n: report.quote_clean_no_would_enter_audit.raw_dogs_n,
      comparison_duds_n: report.quote_clean_no_would_enter_audit.comparison_duds_n,
      next_main_contradiction: report.interpretation.next_main_contradiction,
      paths,
    }, null, 2));
  } catch (error) {
    const failed = {
      schema_version: 'raw_dog_decision_audit.v1',
      generated_at: new Date().toISOString(),
      status: 'failed',
      error: error?.message || String(error),
      inputs: {
        raw_db_path: args.rawDb,
        paper_db_path: args.paperDb,
      },
      interpretation: {
        dominant_observation: 'audit failed before producing evidence',
        next_main_contradiction: 'evidence_unavailable',
        do_not_change_strategy: true,
      },
    };
    const paths = writeReports(failed, args.outDir);
    console.error(JSON.stringify({ ok: false, error: failed.error, paths }, null, 2));
    process.exitCode = 1;
  } finally {
    try { rawDb?.close(); } catch {}
    try { paperDb?.close(); } catch {}
  }
}

await main();
