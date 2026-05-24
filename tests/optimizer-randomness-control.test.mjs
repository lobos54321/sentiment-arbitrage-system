import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';
import { AutoresearchLoop } from '../src/optimizer/autoresearch-loop.js';
import { ChallengerGenerator } from '../src/optimizer/challenger-generator.js';
import { StrategyMutator } from '../src/optimizer/strategy-mutator.js';

function baselineCandidate() {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-registry-'));
  return new PaperStrategyRegistry(join(dir, 'registry.json')).getBaseline();
}

function parseNotes(candidate) {
  return JSON.parse(candidate.notes);
}

test('strategy mutator records randomness control material in candidate notes', () => {
  const baseline = baselineCandidate();
  const candidate = new StrategyMutator().mutate(baseline, {
    createdAt: '2026-05-24T00:00:00.000Z',
    entropy: 'unit-mutator-entropy',
  });
  const notes = parseNotes(candidate);

  assert.equal(candidate.parentId, baseline.id);
  assert.equal(candidate.mutationSet.length, 1);
  assert.equal(candidate.id, notes.assignment_id);
  assert.match(notes.rng_seed, /^sha256:[0-9a-f]{64}$/);
  assert.equal(notes.rng_version, 'v2.7.0.strategy_experiment_rng.v1');
  assert.equal(notes.randomization_unit, 'strategy_experiment_candidate');
  assert.equal(notes.randomization_enabled, true);
  assert.equal(notes.deterministic_assignment, false);
  assert.equal(notes.assignment_algorithm, 'seeded_sha256_prng_v1');
  assert.match(notes.assignment_hash, /^[0-9a-f]{64}$/);
});

test('challenger generator records three seeded mutations with randomness evidence', () => {
  const baseline = baselineCandidate();
  const candidate = new ChallengerGenerator().generate(baseline, {
    createdAt: '2026-05-24T00:00:00.000Z',
    entropy: 'unit-generator-entropy',
  });
  const notes = parseNotes(candidate);

  assert.equal(candidate.parentId, baseline.id);
  assert.equal(candidate.createdBy, 'challenger-generator');
  assert.equal(candidate.mutationSet.length, 3);
  assert.equal(new Set(candidate.mutationSet.map((mutation) => mutation.path)).size, 3);
  assert.equal(candidate.id, notes.assignment_id);
  assert.match(notes.rng_seed, /^sha256:[0-9a-f]{64}$/);
  assert.equal(notes.rng_version, 'v2.7.0.strategy_experiment_rng.v1');
  assert.equal(notes.source, 'challenger-generator');
  assert.equal(notes.scoreMode, 'seeded-differentiated-fallback');
});

test('autoresearch loop preserves generator randomness notes when annotating run context', async () => {
  const baseline = baselineCandidate();
  const generatorCandidate = new ChallengerGenerator().generate(baseline, {
    createdAt: '2026-05-24T00:00:00.000Z',
    entropy: 'autoresearch-generator-entropy',
  });
  const upserted = [];
  const registered = [];
  const loop = Object.create(AutoresearchLoop.prototype);
  Object.assign(loop, {
    config: {
      datasets: {
        signalExport: 'signals',
        localRecorder: 'snapshots',
        paperTrades: 'paper',
      },
      promotion: {
        promotableMinScore: 1,
        promotableMinExpectancy: 1,
        promotableMinWinRate: 1,
      },
    },
    registry: {
      getBaseline: () => baseline,
      getChallenger: () => null,
      registerCandidate: async (candidate) => {
        registered.push(candidate);
      },
    },
    experimentStore: {
      upsert: (candidate) => {
        upserted.push(JSON.parse(JSON.stringify(candidate)));
      },
    },
    challengerGenerator: {
      generate: () => generatorCandidate,
    },
    mutator: {
      mutate: () => new StrategyMutator().mutate(baseline, {
        createdAt: '2026-05-24T00:00:00.000Z',
        entropy: 'autoresearch-mutator-entropy',
      }),
    },
    evaluator: {
      evaluateCandidate: async () => ({
        baselineMetrics: { expectancy: 0 },
        candidateMetrics: { expectancy: 0, winRate: 0, sampleSize: 0 },
      }),
    },
    comparator: {
      compare: () => ({ better: false, score: 0 }),
    },
    guardrails: {
      evaluate: () => ({ passed: false, results: { sampleSize: false } }),
    },
  });

  const result = await loop.runOnce({ trigger: 'unit' });
  const notes = JSON.parse(result.candidate.notes);

  assert.equal(result.kept, false);
  assert.equal(upserted.length, 2);
  assert.equal(registered.length, 1);
  assert.equal(notes.assignment_id, generatorCandidate.id);
  assert.match(notes.rng_seed, /^sha256:[0-9a-f]{64}$/);
  assert.equal(notes.rng_version, 'v2.7.0.strategy_experiment_rng.v1');
  assert.equal(notes.trigger, 'unit');
  assert.equal(notes.generatorUsed, true);
});
