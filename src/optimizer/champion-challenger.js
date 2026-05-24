function toFiniteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function roundScore(value) {
  return Number(value.toFixed(6));
}

export class ChampionChallengerComparator {
  compare(baselineMetrics = {}, candidateMetrics = {}) {
    const baselineExpectancy = toFiniteNumber(baselineMetrics.expectancy);
    const candidateExpectancy = toFiniteNumber(candidateMetrics.expectancy);
    const score = roundScore(candidateExpectancy - baselineExpectancy);

    return {
      better: score > 0,
      score,
      deltas: {
        expectancy: score,
        winRate: roundScore(
          toFiniteNumber(candidateMetrics.winRate) - toFiniteNumber(baselineMetrics.winRate)
        ),
        avgPnl: roundScore(
          toFiniteNumber(candidateMetrics.avgPnl) - toFiniteNumber(baselineMetrics.avgPnl)
        ),
        maxDrawdown: roundScore(
          toFiniteNumber(candidateMetrics.maxDrawdown) - toFiniteNumber(baselineMetrics.maxDrawdown)
        ),
        tailLoss95: roundScore(
          toFiniteNumber(candidateMetrics.tailLoss95) - toFiniteNumber(baselineMetrics.tailLoss95)
        ),
        falsePositiveRate: roundScore(
          toFiniteNumber(candidateMetrics.falsePositiveRate) - toFiniteNumber(baselineMetrics.falsePositiveRate)
        )
      }
    };
  }
}

export default ChampionChallengerComparator;
