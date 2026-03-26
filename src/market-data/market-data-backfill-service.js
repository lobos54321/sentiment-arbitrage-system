import autonomyConfig from '../config/autonomy-config.js';
import { BarAggregator } from './bar-aggregator.js';
import { HeliusHistoryClient } from './helius-history-client.js';
import { KlineRepository } from './kline-repository.js';
import { PoolResolver } from './pool-resolver.js';
import { TradeNormalizer } from './trade-normalizer.js';

export class MarketDataBackfillService {
  constructor(config = autonomyConfig, options = {}) {
    this.config = config;
    this.repository = options.repository || new KlineRepository(config.evaluator.klineCacheDbPath);
    this.poolResolver = options.poolResolver || new PoolResolver({ repository: this.repository });
    this.heliusClient = options.heliusClient || new HeliusHistoryClient(config.helius || {});
    this.tradeNormalizer = options.tradeNormalizer || new TradeNormalizer();
    this.ownsRepository = !options.repository;
  }

  getBars(tokenCa, startTs, endTs) {
    return this.repository.getBars(tokenCa, startTs, endTs);
  }

  getBarsBefore(tokenCa, signalTsSec, limit) {
    return this.repository.getBarsBefore(tokenCa, signalTsSec, limit);
  }

  listRecentCursors(limit = 20) {
    return this.repository.listRecentCursors(limit);
  }

  async backfillWindow({ tokenCa, signalTsSec, startTs, endTs, minBars = 1, poolAddress = null }) {
    const beforeBars = this.repository.getBars(tokenCa, startTs, endTs);
    if (beforeBars.length >= minBars) {
      return {
        provider: 'cache',
        poolAddress: beforeBars[0]?.pool_address || poolAddress || null,
        bars: beforeBars,
        tradesInserted: 0,
        barsWritten: 0,
        signaturesFetched: 0,
        transactionsFetched: 0,
        cacheHit: true,
        error: null
      };
    }

    if (!this.heliusClient.isEnabled()) {
      return {
        provider: null,
        poolAddress,
        bars: beforeBars,
        tradesInserted: 0,
        barsWritten: 0,
        signaturesFetched: 0,
        transactionsFetched: 0,
        cacheHit: false,
        error: 'helius_disabled'
      };
    }

    const resolverResult = poolAddress
      ? { poolAddress, provider: 'input', error: null }
      : await this.poolResolver.resolvePool(tokenCa);
    const resolvedPool = resolverResult.poolAddress;
    if (!resolvedPool) {
      return {
        provider: null,
        poolAddress: null,
        poolProvider: resolverResult.provider || null,
        bars: beforeBars,
        tradesInserted: 0,
        barsWritten: 0,
        signaturesFetched: 0,
        transactionsFetched: 0,
        cacheHit: false,
        error: 'no_pool'
      };
    }

    let signaturesFetched = 0;
    let transactionsFetched = 0;
    let normalizedTrades = [];
    let before = null;
    let oldestSignatureSeen = null;
    let newestSignatureSeen = null;
    let oldestBlockTime = null;
    let newestBlockTime = null;
    const maxPages = Number(this.config.helius?.maxPagesPerBackfill || 5);
    const targetEndTs = endTs || signalTsSec + (this.config.evaluator.maxHistoricalBars * 60);
    const targetStartTs = startTs || signalTsSec;

    try {
      for (let page = 0; page < maxPages; page += 1) {
        const pageResult = await this.heliusClient.fetchHistoryPage(resolvedPool, {
          before,
          limit: Number(this.config.helius?.pageSize || 100)
        });
        const signatures = pageResult.signatures || [];
        const transactions = pageResult.transactions || [];
        if (!signatures.length) {
          break;
        }

        signaturesFetched += signatures.length;
        transactionsFetched += transactions.length;
        before = signatures[signatures.length - 1]?.signature || null;
        newestSignatureSeen = newestSignatureSeen || signatures[0]?.signature || null;
        oldestSignatureSeen = signatures[signatures.length - 1]?.signature || oldestSignatureSeen;

        for (const sig of signatures) {
          if (sig.blockTime) {
            newestBlockTime = newestBlockTime == null ? sig.blockTime : Math.max(newestBlockTime, sig.blockTime);
            oldestBlockTime = oldestBlockTime == null ? sig.blockTime : Math.min(oldestBlockTime, sig.blockTime);
          }
        }

        const pageTrades = this.tradeNormalizer.normalizeTransactions(transactions, {
          tokenCa,
          poolAddress: resolvedPool
        });
        normalizedTrades.push(...pageTrades);

        if (oldestBlockTime != null && oldestBlockTime <= targetStartTs) {
          break;
        }
      }
    } catch (error) {
      this.repository.updateCursor({
        poolAddress: resolvedPool,
        tokenCa,
        oldestSignatureSeen,
        newestSignatureSeen,
        oldestBlockTime,
        newestBlockTime,
        status: 'error',
        error: error.message
      });
      return {
        provider: null,
        poolAddress: resolvedPool,
        poolProvider: resolverResult.provider || null,
        bars: beforeBars,
        tradesInserted: 0,
        barsWritten: 0,
        signaturesFetched,
        transactionsFetched,
        cacheHit: false,
        error: error.message
      };
    }

    const insertedTrades = this.repository.upsertTrades(normalizedTrades);
    const allStoredTrades = this.repository.getTrades(tokenCa, oldestBlockTime || 0, newestBlockTime || targetEndTs, resolvedPool).map((trade) => ({
      ...trade,
      blockTime: Number(trade.block_time),
      price: Number(trade.price),
      volume: Number(trade.volume)
    }));
    const relevantTrades = allStoredTrades.filter((trade) => trade.blockTime >= targetStartTs && trade.blockTime <= targetEndTs);
    const bars = BarAggregator.aggregateToMinuteBars(relevantTrades)
      .filter((bar) => bar.timestamp >= targetStartTs && bar.timestamp <= targetEndTs);
    const barsWritten = this.repository.upsertBars(tokenCa, resolvedPool, bars, 'helius');
    const inWindowNormalizedTrades = normalizedTrades.filter((trade) => trade.blockTime >= targetStartTs && trade.blockTime <= targetEndTs);
    const allBarsInFetchedRange = BarAggregator.aggregateToMinuteBars(allStoredTrades);
    const heliusBarError = bars.length
      ? null
      : normalizedTrades.length === 0
        ? 'no_helius_trades'
        : inWindowNormalizedTrades.length === 0
          ? 'helius_trades_outside_window'
          : allBarsInFetchedRange.length === 0
            ? 'helius_trades_unusable'
            : 'no_helius_bars';
    this.repository.upsertPoolMapping(tokenCa, resolvedPool, 'helius');
    this.repository.updateCursor({
      poolAddress: resolvedPool,
      tokenCa,
      oldestSignatureSeen,
      newestSignatureSeen,
      oldestBlockTime,
      newestBlockTime,
      status: 'ok',
      error: heliusBarError
    });

    return {
      provider: bars.length ? 'helius' : null,
      poolAddress: resolvedPool,
      poolProvider: resolverResult.provider || null,
      bars: this.repository.getBars(tokenCa, targetStartTs, targetEndTs),
      tradesInserted: insertedTrades,
      barsWritten,
      signaturesFetched,
      transactionsFetched,
      cacheHit: false,
      error: heliusBarError
    };
  }

  close() {
    if (this.ownsRepository) {
      this.repository.close();
    }
  }
}

export default MarketDataBackfillService;
