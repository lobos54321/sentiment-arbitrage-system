import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';

import {
  buildV27PaperModeSafetyRuntimeEvidence,
  defaultV27PaperModeSafetyPath,
  writeV27PaperModeSafetyRuntimeEvidence,
} from '../src/runtime/v27-paper-mode-safety.js';

test('v27 paper mode safety runtime evidence defaults to paper-only safe', () => {
  const evidence = buildV27PaperModeSafetyRuntimeEvidence({
    config: { PAPER_ONLY_MODE: true, PREMIUM_LIVE_EXECUTION_ENABLED: false },
    env: {},
    now: new Date('2026-05-22T00:00:00Z'),
    pid: 123,
  });

  assert.equal(evidence.runtime_evidence_schema_version, 'v2.7.0.paper_mode_safety_runtime.v1');
  assert.equal(evidence.paper_live_boundary_ok, true);
  assert.equal(evidence.paper_mode_required, true);
  assert.equal(evidence.paper_only_mode, true);
  assert.equal(evidence.live_private_key_present, false);
  assert.deepEqual(evidence.violations, []);
});

test('v27 paper mode safety runtime evidence detects live capability without exposing secret value', () => {
  const evidence = buildV27PaperModeSafetyRuntimeEvidence({
    config: { PAPER_ONLY_MODE: true, PREMIUM_LIVE_EXECUTION_ENABLED: false },
    env: {
      PREMIUM_LIVE_EXECUTION_ENABLED: 'true',
      TRADE_WALLET_PRIVATE_KEY: 'secret-value',
      NETWORK_TRANSACTION_SIGNING_ENABLED: 'true',
    },
    liveComponents: {
      jupiterExecutor: {},
    },
  });

  assert.equal(evidence.paper_live_boundary_ok, false);
  assert.deepEqual(evidence.present_live_secret_names, ['TRADE_WALLET_PRIVATE_KEY']);
  assert.notEqual(evidence.live_secret_presence_hash, 'secret-value');
  assert.equal(JSON.stringify(evidence).includes('secret-value'), false);
  assert.ok(evidence.violations.includes('premium_live_execution_enabled'));
  assert.ok(evidence.violations.includes('live_private_key_present'));
  assert.ok(evidence.violations.includes('network_transaction_signing_enabled'));
  assert.ok(evidence.violations.includes('jupiter_executor_initialized'));
});

test('v27 paper mode safety runtime evidence writes to configured read model directory', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'v27-paper-mode-safety-'));
  const outputPath = defaultV27PaperModeSafetyPath({
    env: { V27_READ_MODEL_DIR: dir },
    projectRoot: '/unused',
  });

  assert.equal(outputPath, join(dir, 'paper_mode_safety.json'));

  const result = writeV27PaperModeSafetyRuntimeEvidence({
    path: outputPath,
    config: { PAPER_ONLY_MODE: true },
    env: {},
    now: new Date('2026-05-22T00:00:00Z'),
  });
  const payload = JSON.parse(fs.readFileSync(outputPath, 'utf8'));

  assert.equal(result.path, outputPath);
  assert.equal(payload.paper_live_boundary_ok, true);
  assert.equal(payload.generated_at, '2026-05-22T00:00:00.000Z');
});
