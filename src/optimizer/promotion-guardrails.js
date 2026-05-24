import autonomyConfig from '../config/autonomy-config.js';

function toFiniteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function buildGuardrails(config = {}) {
  return {
    ...autonomyConfig.guardrails,
    ...(config?.guardrails || {})
  };
}

export class PromotionGuardrails {
  constructor(config = autonomyConfig) {
    this.guardrails = buildGuardrails(config);
  }

  evaluate(metrics = {}) {
    const sampleSize = toFiniteNumber(metrics.sampleSize);
    const winRate = toFiniteNumber(metrics.winRate);
    const expectancy = toFiniteNumber(metrics.expectancy);
    const maxDrawdown = Math.abs(toFiniteNumber(metrics.maxDrawdown));
    const tailLoss95 = Math.abs(toFiniteNumber(metrics.tailLoss95));
    const falsePositiveRate = toFiniteNumber(metrics.falsePositiveRate);

    const checks = {
      sampleSize: sampleSize >= this.guardrails.minSampleSize,
      winRate: winRate >= this.guardrails.minWinRate,
      expectancy: expectancy >= this.guardrails.minExpectancy,
      drawdown: maxDrawdown <= this.guardrails.maxDrawdown,
      tailLoss95: tailLoss95 <= this.guardrails.maxTailLoss95,
      falsePositiveRate: falsePositiveRate <= this.guardrails.maxFalsePositiveRate
    };

    const passed = Object.values(checks).every(Boolean);

    return {
      passed,
      results: {
        ...checks,
        maxDrawdown: checks.drawdown,
        sampleSizeValue: sampleSize,
        minSampleSize: this.guardrails.minSampleSize,
        winRateValue: winRate,
        minWinRate: this.guardrails.minWinRate,
        expectancyValue: expectancy,
        minExpectancy: this.guardrails.minExpectancy,
        maxDrawdownValue: maxDrawdown,
        maxDrawdownLimit: this.guardrails.maxDrawdown,
        tailLoss95Value: tailLoss95,
        maxTailLoss95: this.guardrails.maxTailLoss95,
        falsePositiveRateValue: falsePositiveRate,
        maxFalsePositiveRate: this.guardrails.maxFalsePositiveRate
      }
    };
  }
}

export default PromotionGuardrails;
