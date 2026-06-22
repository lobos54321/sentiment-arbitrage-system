import assert from 'node:assert/strict';
import { createRequire } from 'module';

import {
  materializeTokenMemory,
  materializeEvidenceConflictAging,
  materializeWatchlist,
  inWindow,
  TARGET_MODULES,
  MATERIALIZER_BLOCKED,
} from '../scripts/build-runtime-readmodel-materialized-export.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');
const START = 1781942715;
const END = 1782029115;

// inWindow + module set
{
  assert.equal(inWindow(START, START, END), true);
  assert.equal(inWindow(END + 301, START, END), false);
  assert.equal(inWindow(null, START, END), false);
  assert.equal(TARGET_MODULES.length, 12);
  assert.ok(!TARGET_MODULES.includes('parser_session')); // already covered in Goal 6
  assert.equal(Object.keys(MATERIALIZER_BLOCKED).length, 9); // 12 target - 3 materializable
}

// token_memory: prior in-window same-token signals -> structured count (not a label)
{
  const cohort = [
    { token_ca: 'A', signal_ts: 1781950000 },
    { token_ca: 'A', signal_ts: 1781960000 }, // 1 prior (A@1781950000)
    { token_ca: 'B', signal_ts: 1781955000 }, // no prior
  ];
  const out = materializeTokenMemory({ cohortSignals: cohort, startTs: START, endTs: END });
  assert.equal(out.status, 'MATERIALIZED_AND_JOINED');
  assert.equal(out.joined.length, 3);
  const a2 = out.joined.find((r) => r.token_ca === 'A' && r.signal_ts === 1781960000);
  const pl = JSON.parse(a2.payload_json);
  assert.equal(pl.prior_in_window_signal_n, 1);
  assert.equal(pl.last_prior_signal_ts, 1781950000);
  assert.equal(pl.memory_state, 'prior_in_window_signals');
  assert.equal(a2.join_confidence, 'MEDIUM');
  const b = out.joined.find((r) => r.token_ca === 'B');
  assert.equal(JSON.parse(b.payload_json).prior_in_window_signal_n, 0);
  assert.equal(b.join_confidence, 'HIGH');
}

// evidence_conflict_aging: structured age + real conflict from quote_clean mismatch (not a label)
{
  const cohort = [
    { token_ca: 'A', signal_ts: END - 100, quote_clean_mismatch_event_n: 2 },          // conflict
    { token_ca: 'B', signal_ts: END - 5000, quote_clean_source_mismatch: false, raw_missing_reason: 'no_raw' }, // stale
  ];
  const out = materializeEvidenceConflictAging({ cohortSignals: cohort, startTs: START, endTs: END });
  assert.equal(out.joined.length, 2);
  const a = JSON.parse(out.joined.find((r) => r.token_ca === 'A').payload_json);
  assert.equal(a.evidence_age_sec, 100);
  assert.equal(a.conflict_state, 'quote_clean_top_vs_risk_mismatch');
  const b = JSON.parse(out.joined.find((r) => r.token_ca === 'B').payload_json);
  assert.equal(b.conflict_state, 'no_conflict_detected');
  assert.equal(b.stale_reason, 'raw_or_kline_provenance_incomplete');
}

// watchlist: lifecycle tracks state machine, exact (token,signal_ts) join (in-memory db)
{
  const db = new Database(':memory:');
  db.exec('CREATE TABLE tracks (token_ca TEXT, signal_ts INTEGER, status TEXT, complete_reason TEXT, created_at INTEGER, complete_ts INTEGER, entry_ts INTEGER)');
  const ins = db.prepare('INSERT INTO tracks VALUES (?,?,?,?,?,?,?)');
  ins.run('A', 1781950000, 'active', null, 1781949000, null, null);     // in cohort + in window
  ins.run('Z', 1781951000, 'dead', 'expired', 1781950500, 1781952000, null); // NOT in cohort
  const cohortKeys = new Set(['A|1781950000']);
  const out = materializeWatchlist({ lifecycleDb: db, cohortKeys, startTs: START, endTs: END });
  assert.equal(out.joined.length, 1);
  assert.equal(out.joined[0].join_confidence, 'HIGH');
  assert.equal(JSON.parse(out.joined[0].payload_json).watchlist_state, 'active');
  assert.equal(out.unjoined.length, 1);
  db.close();

  // missing tracks table -> MATERIALIZER_MISSING_OR_INVALID, no throw
  const db2 = new Database(':memory:');
  const out2 = materializeWatchlist({ lifecycleDb: db2, cohortKeys: new Set(), startTs: START, endTs: END });
  assert.equal(out2.status, 'MATERIALIZER_MISSING_OR_INVALID');
  db2.close();
}

// every blocked module has a materializer-specific reason (NOT a generic "not_in_current_exports")
{
  for (const m of Object.keys(MATERIALIZER_BLOCKED)) {
    assert.ok(MATERIALIZER_BLOCKED[m].startsWith('materializer_ran:'), `materializer-specific reason for ${m}`);
    assert.ok(/requires_|emit|absent|not /.test(MATERIALIZER_BLOCKED[m]), `exact gap for ${m}`);
  }
}

console.log('build-runtime-readmodel-materialized-export tests passed');
