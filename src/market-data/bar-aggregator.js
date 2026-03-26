export class BarAggregator {
  static aggregateToMinuteBars(trades = []) {
    const buckets = new Map();

    for (const trade of [...trades].sort((a, b) => (a.blockTime || 0) - (b.blockTime || 0))) {
      const ts = Number(trade.blockTime || 0);
      const price = Number(trade.price || 0);
      const volume = Number(trade.volume || 0);
      if (!Number.isFinite(ts) || ts <= 0 || !Number.isFinite(price) || price <= 0) {
        continue;
      }

      const minuteTs = Math.floor(ts / 60) * 60;
      const current = buckets.get(minuteTs);
      if (!current) {
        buckets.set(minuteTs, {
          timestamp: minuteTs,
          open: price,
          high: price,
          low: price,
          close: price,
          volume: Math.abs(volume)
        });
        continue;
      }

      current.high = Math.max(current.high, price);
      current.low = Math.min(current.low, price);
      current.close = price;
      current.volume += Math.abs(volume);
    }

    return [...buckets.values()].sort((a, b) => a.timestamp - b.timestamp);
  }
}

export default BarAggregator;
