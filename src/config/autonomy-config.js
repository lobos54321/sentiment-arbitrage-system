import path from 'path';
import { fileURLToPath } from 'url';

const moduleDir = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(moduleDir, '../..');

export const autonomyConfig = {
  enabled: process.env.AUTONOMY_SIDECAR_ENABLED === 'true',
  observeOnly: process.env.AUTONOMY_OBSERVE_ONLY !== 'false',
  paperOnly: true,
  orchestrationIntervalMs: parseInt(process.env.AUTONOMY_INTERVAL_MS || `${30 * 60 * 1000}`, 10),
  loopIntervalMs: parseInt(process.env.AUTORESEARCH_LOOP_INTERVAL_MS || `${60 * 60 * 1000}`, 10),
  maxCandidatesPerCycle: parseInt(process.env.AUTONOMY_MAX_CANDIDATES || '1', 10),
  projectRoot,
  dataDir: path.join(projectRoot, 'data'),
  dbPath: process.env.DB_PATH || path.join(projectRoot, 'data', 'sentiment_arb.db'),
  exports: {
    zeaburBaseUrl: process.env.ZEABUR_URL || 'https://sentiment-arbitrage.zeabur.app',
    dashboardTokenEnv: 'DASHBOARD_TOKEN',
    filePath: path.join(projectRoot, 'data', 'zeabur-export.json')
  },
  datasets: {
    signalExport: 'zeabur-export',
    localRecorder: 'signal-snapshots',
    paperTrades: 'paper-shadow-book'
  },
  flags: {
    enablePremiumPaperRegistry: process.env.AUTONOMY_PREMIUM_REGISTRY !== 'false',
    enablePaperShadowComparison: process.env.AUTONOMY_PAPER_SHADOW === 'true',
    enableDashboardPanel: process.env.AUTONOMY_DASHBOARD !== 'false',
    enableOctopusResearch: process.env.AUTONOMY_OCTOPUS_RESEARCH === 'true',
    enableAutoresearchLoop: process.env.AUTONOMY_AUTORESEARCH !== 'false'
  },
  mutationSurface: {
    allowedTopLevelFields: [
      'sourceWeights',
      'scoreThresholds',
      'narrativeThresholds',
      'entryTimingFilters',
      'cooldownWindows',
      'paperExitRules',
      'sourceToggles',
      'paperRiskCaps'
    ],
    forbiddenPaths: [
      'execution',
      'wallet',
      'collectors',
      'hardGateStructure',
      'live'
    ]
  },
  guardrails: {
    minSampleSize: parseInt(process.env.AUTONOMY_MIN_SAMPLE_SIZE || '20', 10),
    minWinRate: parseFloat(process.env.AUTONOMY_MIN_WIN_RATE || '0.35'),
    minExpectancy: parseFloat(process.env.AUTONOMY_MIN_EXPECTANCY || '0'),
    maxDrawdown: parseFloat(process.env.AUTONOMY_MAX_DRAWDOWN || '35'),
    maxTailLoss95: parseFloat(process.env.AUTONOMY_MAX_TAIL_LOSS95 || '45'),
    maxFalsePositiveRate: parseFloat(process.env.AUTONOMY_MAX_FALSE_POSITIVE_RATE || '0.65')
  },
  octopus: {
    timeoutMs: parseInt(process.env.OCTOPUS_TIMEOUT_MS || '120000', 10),
    maxRetries: parseInt(process.env.OCTOPUS_MAX_RETRIES || '1', 10),
    budgetUsd: parseFloat(process.env.OCTOPUS_BUDGET_USD || '5'),
    scriptPath: process.env.OCTOPUS_ORCHESTRATE_PATH || '/Users/boliu/.claude/plugins/cache/nyldn-plugins/octo/9.7.6/orchestrate.sh'
  },
  evaluator: {
    klineProviderPriority: ['cache', 'helius', 'geckoterminal', 'dexscreener'],
    klineResolution: '1m',
    maxHistoricalBars: parseInt(process.env.AUTONOMY_MAX_HISTORICAL_BARS || '180', 10),
    klineCacheDbPath: path.join(projectRoot, 'data', 'kline_cache.db'),
    klineCacheCandidates: [
      path.join(projectRoot, 'data', 'kline_cache_backup_20260319.db'),
      path.join(projectRoot, 'data', 'kline_cache_backup_20260318.db'),
      path.join(projectRoot, 'data', 'kline_cache.db')
    ]
  },
  helius: {
    apiKey: process.env.HELIUS_API_KEY || '',
    rpcUrl: process.env.HELIUS_RPC_URL || '',
    enhancedUrl: process.env.HELIUS_ENHANCED_URL || 'https://api.helius.xyz/v0',
    signatureRps: parseFloat(process.env.HELIUS_SIGNATURE_RPS || '2'),
    transactionRps: parseFloat(process.env.HELIUS_TRANSACTION_RPS || '1'),
    pageSize: parseInt(process.env.HELIUS_HISTORY_PAGE_SIZE || '100', 10),
    batchSize: parseInt(process.env.HELIUS_TRANSACTION_BATCH_SIZE || '25', 10),
    maxPagesPerBackfill: parseInt(process.env.HELIUS_MAX_PAGES_PER_BACKFILL || '5', 10),
    incrementalMaxPoolsPerRun: parseInt(process.env.HELIUS_INCREMENTAL_MAX_POOLS_PER_RUN || '25', 10),
    incrementalWindowMinutes: parseInt(process.env.HELIUS_INCREMENTAL_WINDOW_MINUTES || '360', 10),
    incrementalOverlapMinutes: parseInt(process.env.HELIUS_INCREMENTAL_OVERLAP_MINUTES || '15', 10),
    trackedSignalLookbackHours: parseInt(process.env.HELIUS_TRACKED_SIGNAL_LOOKBACK_HOURS || '72', 10),
    trackedSignalLimit: parseInt(process.env.HELIUS_TRACKED_SIGNAL_LIMIT || '40', 10),
    openTradeLimit: parseInt(process.env.HELIUS_OPEN_TRADE_LIMIT || '20', 10)
  }
};

export function getAutonomyConfig(overrides = {}) {
  return {
    ...autonomyConfig,
    ...overrides,
    flags: { ...autonomyConfig.flags, ...(overrides.flags || {}) },
    guardrails: { ...autonomyConfig.guardrails, ...(overrides.guardrails || {}) },
    octopus: { ...autonomyConfig.octopus, ...(overrides.octopus || {}) },
    evaluator: { ...autonomyConfig.evaluator, ...(overrides.evaluator || {}) },
    helius: { ...autonomyConfig.helius, ...(overrides.helius || {}) }
  };
}

export default autonomyConfig;
