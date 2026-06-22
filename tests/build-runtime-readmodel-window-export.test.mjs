import assert from 'node:assert/strict';
import { createRequire } from 'module';

import {
  windowValidity,
  extractParserSession,
  extractWorkerHealth,
  extractTokenLevel,
  ALL_MODULES,
  NO_SOURCE_REASON,
} from '../scripts/build-runtime-readmodel-window-export.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const START = 1781942715;
const END = 1782029115;

// ---- windowValidity: skew boundary + null --------------------------------------------------
{
  assert.equal(windowValidity(START, START, END).same_window_valid, true);
  assert.equal(windowValidity(END, START, END).same_window_valid, true);
  assert.equal(windowValidity(END + 300, START, END).same_window_valid, true);   // within skew
  assert.equal(windowValidity(END + 301, START, END).same_window_valid, false);  // beyond skew
  assert.equal(windowValidity(START - 301, START, END).same_window_valid, false);
  assert.equal(windowValidity(null, START, END).same_window_valid, false);
  assert.equal(ALL_MODULES.length, 13);
  assert.ok(!ALL_MODULES.includes('smart_money') && !ALL_MODULES.includes('curve_pumpfun')); // those are Goal 7
}

// ---- extractParserSession: exact (token,signal_ts) join, cross-window -> unjoined ----------
{
  const db = new Database(':memory:');
  db.exec('CREATE TABLE raw_signal_observations (signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER, status TEXT, source_kind TEXT, provider TEXT, first_bar_lag_sec INTEGER, matured_at_ts INTEGER, created_at INTEGER, updated_at INTEGER)');
  const ins = db.prepare('INSERT INTO raw_signal_observations VALUES (@signal_id,@token_ca,@symbol,@signal_ts,@status,@source_kind,@provider,@first_bar_lag_sec,@matured_at_ts,@created_at,@updated_at)');
  ins.run({ signal_id: 's1', token_ca: 'A', symbol: 'AA', signal_ts: 1781950000, status: 'matured', source_kind: 'indexed_ohlcv', provider: 'gmgn', first_bar_lag_sec: 7, matured_at_ts: 1781950100, created_at: 1781950001, updated_at: 1781950100 }); // in cohort + in window
  ins.run({ signal_id: 's2', token_ca: 'Z', symbol: 'ZZ', signal_ts: 1781950500, status: 'matured', source_kind: 'x', provider: 'gecko', first_bar_lag_sec: 3, matured_at_ts: null, created_at: 1781950501, updated_at: 1781950600 }); // NOT in cohort
  ins.run({ signal_id: 's3', token_ca: 'B', symbol: 'BB', signal_ts: 1700000000, status: 'matured', source_kind: 'x', provider: 'gmgn', first_bar_lag_sec: 1, matured_at_ts: null, created_at: 1700000001, updated_at: 1700000100 }); // pre-window
  const cohortKeys = new Set(['A|1781950000', 'B|1700000000']);
  const out = extractParserSession({ rawDb: db, rawSha: 'sha', cohortKeys, startTs: START, endTs: END });
  assert.equal(out.joined.length, 1);                 // only A (in cohort + in window)
  assert.equal(out.joined[0].token_ca, 'A');
  assert.equal(out.joined[0].join_confidence, 'HIGH');
  assert.equal(out.joined[0].same_window_valid, true);
  assert.equal(out.health.status, 'clean');
  assert.equal(out.unjoined.length, 2);               // Z (not cohort) + B (pre-window)
  db.close();
}

// extractParserSession: missing table -> module-level missing_reason, no throw
{
  const db = new Database(':memory:');
  const out = extractParserSession({ rawDb: db, cohortKeys: new Set(), startTs: START, endTs: END });
  assert.equal(out.joined.length, 0);
  assert.equal(out.health.status, 'missing');
  assert.ok(out.health.missing_reason.includes('raw_signal_observations_absent'));
  db.close();
}

// ---- extractWorkerHealth: post-window state -> stale/unjoined (not silently joined) ---------
{
  const db = new Database(':memory:');
  db.exec('CREATE TABLE raw_path_observer_provider_state (provider TEXT, cooldown_until INTEGER, last_error TEXT, updated_at INTEGER)');
  db.prepare('INSERT INTO raw_path_observer_provider_state VALUES (?,?,?,?)').run('helius', END + 9999, 'HTTP 429', END + 9999); // post-window
  const out = extractWorkerHealth({ rawDb: db, rawSha: 'sha', startTs: START, endTs: END });
  assert.equal(out.joined.length, 0);
  assert.equal(out.unjoined.length, 1);
  assert.equal(out.health.status, 'stale');
  assert.ok(out.health.missing_reason.includes('post_window'));
  db.close();
}

// extractWorkerHealth: in-window state -> partial covered
{
  const db = new Database(':memory:');
  db.exec('CREATE TABLE raw_path_observer_provider_state (provider TEXT, cooldown_until INTEGER, last_error TEXT, updated_at INTEGER)');
  db.prepare('INSERT INTO raw_path_observer_provider_state VALUES (?,?,?,?)').run('helius', null, null, START + 1000);
  const out = extractWorkerHealth({ rawDb: db, rawSha: 'sha', startTs: START, endTs: END });
  assert.equal(out.joined.length, 1);
  assert.equal(out.health.status, 'partial');
  db.close();
}

// ---- extractTokenLevel: 0 cohort join + null decision_timestamp -> empty + precise reason ----
{
  const db = new Database(':memory:');
  db.exec('CREATE TABLE tokens (token_ca TEXT, symbol TEXT, rating TEXT, action TEXT, position_tier TEXT, auto_buy_enabled INTEGER, decision_reasons TEXT, decision_timestamp INTEGER)');
  db.prepare('INSERT INTO tokens VALUES (?,?,?,?,?,?,?,?)').run('OTHER', 'O', 'A', 'buy', 'primary', 1, 'reasons', null);
  const out = extractTokenLevel({ sentDb: db, sentSha: 'sha', cohortTokens: new Set(['A', 'B']), startTs: START, endTs: END, moduleGroup: 'token_memory' });
  assert.equal(out.joined.length, 0);
  assert.equal(out.health.status, 'empty');
  assert.ok(out.health.missing_reason.includes('0_cohort_join'));
  assert.equal(out.unjoined.length, 1);
  db.close();
}

// every no-source module has a precise required-export reason
{
  for (const m of Object.keys(NO_SOURCE_REASON)) {
    assert.ok(/requires_|export/.test(NO_SOURCE_REASON[m]), `precise reason for ${m}`);
  }
}

console.log('build-runtime-readmodel-window-export tests passed');
