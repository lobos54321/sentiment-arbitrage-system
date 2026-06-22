import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  FINAL_MODULES,
  FINAL_REASON,
  buildFinalEmitterExport,
} from '../scripts/export-runtime-final-emitters-window.js';

const START = 1000;
const END = 2000;

function writeJsonl(file, rows) {
  fs.writeFileSync(file, rows.map((row) => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''));
}

function fullRow(over = {}) {
  return { token_ca: 'T', signal_ts: 1500, premium_signal_id: 'sig-1', ...over };
}

function validGmgn(over = {}) {
  return {
    token_ca: 'T',
    signal_ts: 1500,
    premium_signal_id: 'sig-1',
    module_group: 'gmgn_policy',
    evidence_ts: 1501,
    window_start_ts: START,
    window_end_ts: END,
    join_confidence: 'HIGH',
    payload_hash: 'hash-1',
    source: 'runtime-final-emitter-fixture',
    gmgn_policy_decision: 'ALLOW',
    gmgn_policy_reason: 'fixture',
    gmgn_policy_source: 'gmgn',
    gmgn_policy_version: 'v1',
    ...over,
  };
}

test('no runtime export => all 6 modules stay FINAL with exact reasons', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'g10-final-'));
  const rowPath = path.join(dir, 'row.jsonl');
  writeJsonl(rowPath, [fullRow()]);
  const out = buildFinalEmitterExport({ windowStartTs: START, windowEndTs: END, fullnetRow: rowPath });
  assert.equal(out.joined.length, 0);
  assert.equal(out.unjoined.length, 0);
  assert.deepEqual(out.summary.still_blocked_modules, FINAL_MODULES);
  for (const module of FINAL_MODULES) {
    const h = out.health.modules.find((m) => m.module_group === module);
    assert.equal(h.status, 'FINAL_EMITTER_MISSING_OR_INVALID_WITH_EXACT_REASON');
    assert.equal(h.missing_reason, FINAL_REASON[module]);
  }
  fs.rmSync(dir, { recursive: true, force: true });
});

test('same-window HIGH evidence with required fields covers module', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'g10-final-'));
  const rowPath = path.join(dir, 'row.jsonl');
  const exportPath = path.join(dir, 'runtime.jsonl');
  writeJsonl(rowPath, [fullRow()]);
  writeJsonl(exportPath, [validGmgn()]);
  const out = buildFinalEmitterExport({ windowStartTs: START, windowEndTs: END, fullnetRow: rowPath, runtimeFinalExport: exportPath });
  assert.equal(out.joined.length, 1);
  assert.equal(out.joined[0].module_group, 'gmgn_policy');
  assert.deepEqual(out.summary.covered_modules, ['gmgn_policy']);
  assert.equal(out.health.modules.find((m) => m.module_group === 'gmgn_policy').status, 'COVERED_WITH_SAME_WINDOW_PROOF');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('cross-window, token-only LOW, and missing fields cannot cover', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'g10-final-'));
  const rowPath = path.join(dir, 'row.jsonl');
  const exportPath = path.join(dir, 'runtime.jsonl');
  writeJsonl(rowPath, [fullRow()]);
  writeJsonl(exportPath, [
    validGmgn({ evidence_ts: 900 }),
    validGmgn({ join_confidence: 'LOW' }),
    validGmgn({ gmgn_policy_version: '' }),
  ]);
  const out = buildFinalEmitterExport({ windowStartTs: START, windowEndTs: END, fullnetRow: rowPath, runtimeFinalExport: exportPath });
  assert.equal(out.joined.length, 0);
  assert.equal(out.unjoined.length, 3);
  assert.ok(out.unjoined.some((row) => row.reject_reason === 'cross_window_or_missing_evidence_ts'));
  assert.ok(out.unjoined.some((row) => row.reject_reason === 'join_confidence_not_coverable:LOW'));
  assert.ok(out.unjoined.some((row) => row.reject_reason.includes('missing_required_fields:gmgn_policy_version')));
  fs.rmSync(dir, { recursive: true, force: true });
});
