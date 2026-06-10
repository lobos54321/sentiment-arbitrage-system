#!/usr/bin/env node
import fs from 'fs';
import { dirname, join, resolve } from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import Database from 'better-sqlite3';

const execFileAsync = promisify(execFile);

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const hit = process.argv.find((arg) => arg === name || arg.startsWith(prefix));
  if (!hit) return fallback;
  if (hit === name) return true;
  return hit.slice(prefix.length);
}

function intArg(name, fallback, min, max) {
  const n = Number.parseInt(String(argValue(name, fallback)), 10);
  const value = Number.isFinite(n) ? n : fallback;
  return Math.max(min, Math.min(max, value));
}

function boolArg(name, fallback = false) {
  const raw = argValue(name, null);
  if (raw == null) return fallback;
  if (raw === true) return true;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function qmarks(list) {
  return list.map(() => '?').join(',');
}

function round(value, digits = 4) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function countWhere(rows, predicate) {
  return rows.filter(predicate).length;
}

function writeJsonAtomic(filePath, data) {
  fs.mkdirSync(dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
  fs.renameSync(tmp, filePath);
}

function eligibleSql() {
  return `
    observation_status = 'matured'
    AND COALESCE(kline_covered, 0) = 1
    AND baseline_confidence IN ('high', 'medium')
    AND COALESCE(same_source_path, 0) = 1
    AND COALESCE(outlier_flag, 0) = 0
    AND COALESCE(sustained_evaluable, 0) = 1
  `;
}

function getTables(db) {
  return new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
}

function rowsForDog(rawDb, dog) {
  return rawDb.prepare(`
    SELECT provider, source_kind, source_family, COUNT(*) AS bars,
      SUM(CASE WHEN COALESCE(volume, 0) > 0 THEN 1 ELSE 0 END) AS nonzero_volume_bars,
      MAX(volume) AS max_volume
    FROM raw_price_bars_1m
    WHERE token_ca = @token
    GROUP BY provider, source_kind, source_family
    ORDER BY bars DESC
  `).all({ token: dog.token_ca });
}

function rowsForDogWindow(rawDb, dog, beforeSec, afterSec) {
  return rawDb.prepare(`
    SELECT provider, source_kind, source_family, COUNT(*) AS bars,
      SUM(CASE WHEN COALESCE(volume, 0) > 0 THEN 1 ELSE 0 END) AS nonzero_volume_bars,
      MAX(volume) AS max_volume
    FROM raw_price_bars_1m
    WHERE token_ca = @token
      AND timestamp BETWEEN @start AND @end
    GROUP BY provider, source_kind, source_family
    ORDER BY bars DESC
  `).all({
    token: dog.token_ca,
    start: dog.min_signal_ts - beforeSec,
    end: dog.max_signal_ts + afterSec,
  });
}

function hasTrack(rows, pattern) {
  const text = JSON.stringify(rows || []).toLowerCase();
  return pattern.test(text);
}

async function touchGmgn(dogs, {
  maxTouch = 0,
  beforeSec = 300,
  afterSec = 1800,
  bars = 60,
  timeoutMs = 20000,
} = {}) {
  const enabled = maxTouch > 0;
  const hasKey = Boolean(process.env.GMGN_API_KEY);
  const out = {
    enabled,
    skipped_reason: enabled ? null : 'touch_disabled',
    key_present: hasKey,
    requested_n: enabled ? Math.min(maxTouch, dogs.length) : 0,
    touched_n: 0,
    ok_n: 0,
    nonzero_volume_n: 0,
    rate_limited_n: 0,
    errors: {},
    rows: [],
  };
  if (!enabled) return out;
  if (!hasKey) {
    out.skipped_reason = 'GMGN_API_KEY_missing';
    return out;
  }
  for (const dog of dogs.slice(0, maxTouch)) {
    const startTs = dog.min_signal_ts - beforeSec;
    const endTs = dog.max_signal_ts + afterSec;
    const row = {
      token_ca: dog.token_ca,
      tier: dog.tier,
      peak: dog.peak,
      start_ts: startTs,
      end_ts: endTs,
      ok: false,
      bars: 0,
      nonzero_volume_bars: 0,
      max_volume: null,
      error: null,
    };
    out.touched_n += 1;
    try {
      const { stdout } = await execFileAsync('gmgn-cli', [
        'market',
        'kline',
        '--chain',
        'sol',
        '--address',
        dog.token_ca,
        '--resolution',
        '1m',
        '--from',
        String(startTs),
        '--to',
        String(endTs),
        '--raw',
      ], {
        env: process.env,
        timeout: timeoutMs,
        maxBuffer: 2 * 1024 * 1024,
      });
      const raw = JSON.parse(String(stdout || '{}'));
      const list = Array.isArray(raw?.list) ? raw.list : (Array.isArray(raw) ? raw : []);
      const normalized = list.map((item) => ({
        ts: Math.floor(Number(item?.time || 0) / 1000),
        volume: Number(item?.volume || 0),
      })).filter((item) => Number.isFinite(item.ts) && item.ts >= startTs && item.ts <= endTs).slice(-bars);
      row.ok = normalized.length > 0;
      row.bars = normalized.length;
      row.nonzero_volume_bars = normalized.filter((item) => item.volume > 0).length;
      row.max_volume = normalized.length ? Math.max(...normalized.map((item) => item.volume || 0)) : null;
      if (row.ok) out.ok_n += 1;
      if (row.nonzero_volume_bars > 0) out.nonzero_volume_n += 1;
    } catch (error) {
      const message = String(error?.message || error || 'gmgn_touch_failed').slice(0, 180);
      row.error = /429|rate.?limit/i.test(message) ? 'gmgn_rate_limited' : message;
      if (row.error === 'gmgn_rate_limited') out.rate_limited_n += 1;
      out.errors[row.error] = Number(out.errors[row.error] || 0) + 1;
    }
    out.rows.push(row);
  }
  return out;
}

async function main() {
  const rawDbPath = resolve(String(argValue('--raw-db', '/private/tmp/sas-audit-download/rawdb/raw_signal_outcomes.db')));
  const paperDbPath = resolve(String(argValue('--paper-db', '/private/tmp/sas-audit-download/recovery-paper/paper_trades.db')));
  const outDir = resolve(String(argValue('--out-dir', '/private/tmp/sas-audit-download/track-probe')));
  const beforeSec = intArg('--before-sec', 1800, 0, 24 * 3600);
  const afterSec = intArg('--after-sec', 1800, 60, 24 * 3600);
  const touchMax = intArg('--touch-gmgn-max', 0, 0, 500);
  const touchBeforeSec = intArg('--touch-before-sec', 300, 0, 24 * 3600);
  const touchAfterSec = intArg('--touch-after-sec', 1800, 60, 24 * 3600);
  const touchBars = intArg('--touch-bars', 60, 1, 500);

  const rawDb = new Database(rawDbPath, { readonly: true, fileMustExist: true });
  const paperDb = new Database(paperDbPath, { readonly: true, fileMustExist: true });
  try {
    const paperTables = getTables(paperDb);
    const eligible = eligibleSql();
    const dogs = rawDb.prepare(`
      SELECT
        token_ca,
        MIN(symbol) AS symbol,
        MIN(signal_ts) AS min_signal_ts,
        MAX(signal_ts) AS max_signal_ts,
        MAX(max_sustained_peak_pct) AS peak,
        CASE WHEN MAX(CASE WHEN raw_primary_tier = 'gold' THEN 1 ELSE 0 END) = 1 THEN 'gold' ELSE 'silver' END AS tier,
        COUNT(*) AS event_rows
      FROM raw_signal_outcomes
      WHERE ${eligible}
        AND raw_primary_tier IN ('gold', 'silver')
        AND token_ca IS NOT NULL
        AND token_ca != ''
      GROUP BY token_ca
      ORDER BY peak DESC
    `).all();

    const dogRows = dogs.map((dog) => {
      const allBars = rowsForDog(rawDb, dog);
      const windowBars = rowsForDogWindow(rawDb, dog, beforeSec, afterSec);
      const externalAll = paperTables.has('external_alpha_snapshots')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM external_alpha_snapshots WHERE token_ca = ?').get(dog.token_ca).n
        : null;
      const externalWindow = paperTables.has('external_alpha_snapshots')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM external_alpha_snapshots WHERE token_ca = ? AND captured_at BETWEEN ? AND ?').get(dog.token_ca, dog.min_signal_ts - beforeSec, dog.max_signal_ts + afterSec).n
        : null;
      const externalState = paperTables.has('external_alpha_state')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM external_alpha_state WHERE token_ca = ?').get(dog.token_ca).n
        : null;
      const resonanceAll = paperTables.has('source_resonance_candidates')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM source_resonance_candidates WHERE token_ca = ?').get(dog.token_ca).n
        : null;
      const resonanceWindow = paperTables.has('source_resonance_candidates')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM source_resonance_candidates WHERE token_ca = ? AND signal_ts BETWEEN ? AND ?').get(dog.token_ca, dog.min_signal_ts - beforeSec, dog.max_signal_ts + afterSec).n
        : null;
      const gmgnPreSeen = paperTables.has('source_resonance_candidates')
        ? paperDb.prepare('SELECT COUNT(*) AS n FROM source_resonance_candidates WHERE token_ca = ? AND COALESCE(gmgn_pre_seen, 0) = 1').get(dog.token_ca).n
        : null;
      return {
        ...dog,
        peak: round(dog.peak, 2),
        raw_bars: {
          all: allBars,
          signal_window: windowBars,
          has_gmgn_or_amm: hasTrack(allBars, /gmgn|amm_pool/),
          has_gmgn_or_amm_near_signal: hasTrack(windowBars, /gmgn|amm_pool/),
        },
        external_alpha: {
          snapshots_all: externalAll,
          snapshots_signal_window: externalWindow,
          state_rows: externalState,
        },
        source_resonance: {
          candidates_all: resonanceAll,
          candidates_signal_window: resonanceWindow,
          gmgn_pre_seen_rows: gmgnPreSeen,
        },
      };
    });

    const touch = await touchGmgn(dogRows, {
      maxTouch: touchMax,
      beforeSec: touchBeforeSec,
      afterSec: touchAfterSec,
      bars: touchBars,
    });

    const summary = {
      total_raw_sustained_gold_silver_dogs: dogRows.length,
      gold_tokens: countWhere(dogRows, (row) => row.tier === 'gold'),
      silver_tokens: countWhere(dogRows, (row) => row.tier === 'silver'),
      raw_bars: {
        dog_tokens_with_gmgn_or_amm_bars: countWhere(dogRows, (row) => row.raw_bars.has_gmgn_or_amm),
        dog_tokens_with_gmgn_or_amm_bars_near_signal: countWhere(dogRows, (row) => row.raw_bars.has_gmgn_or_amm_near_signal),
        dog_tokens_with_any_nonzero_volume_bar: countWhere(dogRows, (row) => (row.raw_bars.all || []).some((bar) => Number(bar.nonzero_volume_bars || 0) > 0)),
      },
      external_alpha: {
        dog_tokens_seen_all_time: countWhere(dogRows, (row) => Number(row.external_alpha.snapshots_all || 0) > 0),
        dog_tokens_seen_signal_window: countWhere(dogRows, (row) => Number(row.external_alpha.snapshots_signal_window || 0) > 0),
        dog_tokens_in_state: countWhere(dogRows, (row) => Number(row.external_alpha.state_rows || 0) > 0),
      },
      source_resonance: {
        dog_tokens_seen_all_time: countWhere(dogRows, (row) => Number(row.source_resonance.candidates_all || 0) > 0),
        dog_tokens_seen_signal_window: countWhere(dogRows, (row) => Number(row.source_resonance.candidates_signal_window || 0) > 0),
        dog_tokens_with_gmgn_pre_seen: countWhere(dogRows, (row) => Number(row.source_resonance.gmgn_pre_seen_rows || 0) > 0),
      },
      gmgn_touch: touch,
    };

    const result = {
      schema_version: 'raw_dog_track_probe.v1',
      generated_at: new Date().toISOString(),
      inputs: {
        raw_db_path: rawDbPath,
        paper_db_path: paperDbPath,
        before_sec: beforeSec,
        after_sec: afterSec,
        touch_gmgn_max: touchMax,
      },
      summary,
      dogs: dogRows,
    };

    fs.mkdirSync(outDir, { recursive: true });
    const jsonPath = join(outDir, 'latest.json');
    const mdPath = join(outDir, 'latest.md');
    writeJsonAtomic(jsonPath, result);
    const md = [
      '# Raw Dog Track Probe',
      '',
      `Generated: ${result.generated_at}`,
      '',
      '## Summary',
      '',
      '```json',
      JSON.stringify(summary, null, 2),
      '```',
      '',
      '## Top Dogs',
      '',
      '| tier | peak | token | gmgn/amm bars | ext window | resonance window | gmgn pre-seen |',
      '|---|---:|---|---:|---:|---:|---:|',
      ...dogRows.slice(0, 50).map((row) => `| ${row.tier} | ${row.peak} | ${row.token_ca} | ${row.raw_bars.has_gmgn_or_amm ? 1 : 0} | ${row.external_alpha.snapshots_signal_window ?? ''} | ${row.source_resonance.candidates_signal_window ?? ''} | ${row.source_resonance.gmgn_pre_seen_rows ?? ''} |`),
      '',
    ].join('\n');
    fs.writeFileSync(mdPath, md);
    process.stdout.write(`${JSON.stringify({
      status: 'ok',
      out_dir: outDir,
      summary,
    }, null, 2)}\n`);
  } finally {
    rawDb.close();
    paperDb.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error?.stack || error?.message || String(error)}\n`);
  process.exit(1);
});
