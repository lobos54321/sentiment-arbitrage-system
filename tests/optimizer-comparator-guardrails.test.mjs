import assert from 'node:assert/strict';
import test from 'node:test';

import { ChampionChallengerComparator } from '../src/optimizer/champion-challenger.js';
import { PromotionGuardrails } from '../src/optimizer/promotion-guardrails.js';

const config = {
  guardrails: {
    minSampleSize: 20,
    minWinRate: 0.35,
    minExpectancy: 0.01,
    maxDrawdown: 35,
    maxTailLoss95: 45,
    maxFalsePositiveRate: 0.65
  }
};

const passingMetrics = {
  sampleSize: 25,
  winRate: 0.44,
  expectancy: 0.03,
  maxDrawdown: 20,
  tailLoss95: 30,
  falsePositiveRate: 0.4
};

test('champion challenger comparator gives a better candidate a positive score', () => {
  const comparator = new ChampionChallengerComparator();

  const result = comparator.compare(
    { expectancy: 0.01, winRate: 0.36, avgPnl: 1 },
    { expectancy: 0.035, winRate: 0.45, avgPnl: 3.5 }
  );

  assert.equal(result.better, true);
  assert.equal(result.score, 0.025);
});

test('champion challenger comparator does not mark a worse candidate better', () => {
  const comparator = new ChampionChallengerComparator();

  const result = comparator.compare(
    { expectancy: 0.03, winRate: 0.45 },
    { expectancy: 0.01, winRate: 0.5 }
  );

  assert.equal(result.better, false);
  assert.equal(result.score, -0.02);
});

test('promotion guardrails pass metrics at configured thresholds', () => {
  const guardrails = new PromotionGuardrails(config);

  const result = guardrails.evaluate({
    sampleSize: 20,
    winRate: 0.35,
    expectancy: 0.01,
    maxDrawdown: 35,
    tailLoss95: 45,
    falsePositiveRate: 0.65
  });

  assert.equal(result.passed, true);
  assert.equal(result.results.sampleSize, true);
  assert.equal(result.results.winRate, true);
  assert.equal(result.results.expectancy, true);
  assert.equal(result.results.drawdown, true);
  assert.equal(result.results.tailLoss95, true);
  assert.equal(result.results.falsePositiveRate, true);
});

test('promotion guardrails fail below sample size threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, sampleSize: 19 });

  assert.equal(result.passed, false);
  assert.equal(result.results.sampleSize, false);
});

test('promotion guardrails fail below win rate threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, winRate: 0.34 });

  assert.equal(result.passed, false);
  assert.equal(result.results.winRate, false);
});

test('promotion guardrails fail below expectancy threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, expectancy: 0.009 });

  assert.equal(result.passed, false);
  assert.equal(result.results.expectancy, false);
});

test('promotion guardrails fail above drawdown threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, maxDrawdown: 35.1 });

  assert.equal(result.passed, false);
  assert.equal(result.results.drawdown, false);
  assert.equal(result.results.maxDrawdown, false);
});

test('promotion guardrails fail above tail loss threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, tailLoss95: 45.1 });

  assert.equal(result.passed, false);
  assert.equal(result.results.tailLoss95, false);
});

test('promotion guardrails fail above false positive threshold', () => {
  const guardrails = new PromotionGuardrails(config);
  const result = guardrails.evaluate({ ...passingMetrics, falsePositiveRate: 0.651 });

  assert.equal(result.passed, false);
  assert.equal(result.results.falsePositiveRate, false);
});
