/**
 * Sentiment Arbitrage System - Main Entry Point
 * MVP 2.0 - Production-Ready On-Chain Sentiment Arbitrage
 *
 * Architecture:
 * 1. Telegram Signal Listener → Captures market signals
 * 2. Chain Snapshot → Real-time on-chain data (SOL/BSC)
 * 3. Hard Gates → Binary quality filters (liquidity, security, slippage)
 * 4. Soft Alpha Score → Multi-factor scoring (TG spread, holder quality, momentum)
 * 5. Decision Matrix → Buy/Greylist/Reject based on scores
 * 6. Position Sizer → Kelly-optimized position sizing
 * 7. GMGN Executor → Telegram Bot-based execution
 * 8. Position Monitor → Three-tier exit strategy
 */

import dotenv from 'dotenv';
import Database from 'better-sqlite3';
import fs from 'fs';
import { spawn, spawnSync } from 'child_process';
import { dirname, join } from 'path';
import { TelegramUserListener } from './inputs/telegram-user-listener.js';
import { SolanaSnapshotService } from './inputs/chain-snapshot-sol.js';
import { BSCSnapshotService } from './inputs/chain-snapshot-bsc.js';
import { HardGateFilter } from './gates/hard-gates.js';
import { SoftAlphaScorer } from './scoring/soft-alpha-score.js';
import { DecisionMatrix } from './decision/decision-matrix.js';
import { PositionSizer } from './decision/position-sizer.js';
import { GMGNTelegramExecutor } from './execution/gmgn-telegram-executor.js';
import { PositionMonitor } from './execution/position-monitor.js';
import GrokTwitterClient from './social/grok-twitter-client.js';
import { PermanentBlacklistService } from './database/permanent-blacklist.js';
import { PremiumChannelListener } from './inputs/premium-channel-listener.js';
import { PremiumSignalEngine } from './engines/premium-signal-engine.js';
import { JupiterUltraExecutor } from './execution/jupiter-ultra-executor.js';
import { ParityExecutor } from './execution/parity-executor.js';
import { LivePriceMonitorV2 } from './tracking/live-price-monitor-v2.js';
import { SharedQuoteClient } from './market-data/shared-quote-client.js';
import { applyMarketDataProcessOverride, isMarketDataProcessEnabled } from './market-data/shared-market-runtime.js';
import { KlineCollector } from './tracking/kline-collector.js';
import { startDashboardServer } from './web/dashboard-server.js';
import { LivePositionMonitor } from './execution/live-position-monitor.js';
import {
  quarantineLiveSecretsForPaperMode,
  writeV27PaperModeSafetyRuntimeEvidence,
} from './runtime/v27-paper-mode-safety.js';

if (process.env.V27_DOTENV_ALREADY_LOADED !== '1') {
  dotenv.config();
}
quarantineLiveSecretsForPaperMode({
  env: process.env,
  reason: process.env.V27_DOTENV_ALREADY_LOADED === '1'
    ? 'index_start_after_preload'
    : 'index_start_after_dotenv',
});

// 全局兜底：防止 async EventEmitter 回调的未捕获 rejection 导致进程崩溃
process.on('unhandledRejection', (reason, promise) => {
  console.error('🔴 [GLOBAL] Unhandled Promise Rejection:', reason);
});

process.on('uncaughtException', (error) => {
  console.error('🔴 [GLOBAL] Uncaught Exception:', error);
  // 给一点时间让日志写完，然后退出（uncaughtException 后状态不可信）
  setTimeout(() => process.exit(1), 3000);
});

function envFlag(name, defaultValue = true) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(raw).trim().toLowerCase());
}

function runVolumePreflightOnce() {
  if (!envFlag('NODE_STARTUP_PREFLIGHT_ENABLED', true)) return;
  const dataDir = process.env.ZEABUR_DATA_DIR || process.env.DATA_DIR || '/app/data';
  const script = join(process.cwd(), 'scripts', 'zeabur_preflight_cleanup.py');
  if (!fs.existsSync(script)) return;
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    const logPath = join(dataDir, 'preflight.log');
    fs.appendFileSync(logPath, `[node-preflight] ${new Date().toISOString()} starting ${script}\n`);
    const result = spawnSync('python3', [script], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ZEABUR_DATA_DIR: dataDir,
        PYTHONUNBUFFERED: '1',
      },
      encoding: 'utf8',
      timeout: Number(process.env.NODE_STARTUP_PREFLIGHT_TIMEOUT_MS || 45000),
      maxBuffer: 1024 * 1024,
    });
    if (result.stdout) fs.appendFileSync(logPath, result.stdout);
    if (result.stderr) fs.appendFileSync(logPath, result.stderr);
    fs.appendFileSync(logPath, `[node-preflight] ${new Date().toISOString()} exit status=${result.status} signal=${result.signal || ''} error=${result.error?.message || ''}\n`);
  } catch (error) {
    console.error('[node-preflight] failed:', error?.message || error);
  }
}

function runV27EventLogRecoveryPreflightOnce() {
  if (!envFlag('V27_EVENT_LOG_RECOVERY_PREFLIGHT_ENABLED', true)) return;
  const script = join(process.cwd(), 'scripts', 'v27_event_log_recover.py');
  if (!fs.existsSync(script)) return;
  const dataDir = process.env.ZEABUR_DATA_DIR || process.env.DATA_DIR || '/app/data';
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const recoveryDir = process.env.V27_EVENT_LOG_RECOVERY_DIR || join(dataDir, 'recovery', 'v27_event_log');
  const logPath = process.env.V27_EVENT_LOG_RECOVERY_LOG || join(dataDir, 'v27-event-log-recovery.log');
  try {
    fs.mkdirSync(dirname(logPath), { recursive: true });
    fs.appendFileSync(logPath, `[node-v27-recovery] ${new Date().toISOString()} starting ${script}\n`);
    const result = spawnSync('python3', [
      script,
      '--event-log-dir', eventLogDir,
      '--recovery-dir', recoveryDir,
      '--quarantine-invalid',
    ], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
        V27_EVENT_LOG_DIR: eventLogDir,
      },
      encoding: 'utf8',
      timeout: Number(process.env.V27_EVENT_LOG_RECOVERY_TIMEOUT_MS || 30000),
      maxBuffer: 1024 * 1024,
    });
    if (result.stdout) fs.appendFileSync(logPath, result.stdout);
    if (result.stderr) fs.appendFileSync(logPath, result.stderr);
    fs.appendFileSync(logPath, `[node-v27-recovery] ${new Date().toISOString()} exit status=${result.status} signal=${result.signal || ''} error=${result.error?.message || ''}\n`);
  } catch (error) {
    console.error('[node-v27-recovery] failed:', error?.message || error);
  }
}

function premiumLiveExecutionEnabled(config = {}) {
  return process.env.SHADOW_MODE === 'false'
    && Boolean(config.PREMIUM_LIVE_EXECUTION_ENABLED);
}

function startDashboardOnce() {
  if (global.__dashboardStarted) return;
  global.__dashboardStarted = true;
  startDashboardServer();
}

function startPythonSidecar({ name, args, env = {}, logPath }) {
  let child = null;
  let stopped = false;
  fs.mkdirSync(dirname(logPath), { recursive: true });
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });

  const launch = () => {
    if (stopped) return;
    logStream.write(`[node-supervisor] ${new Date().toISOString()} starting ${name}: python3 ${args.join(' ')}\n`);
    child = spawn('python3', args, {
      cwd: process.cwd(),
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
        ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.pipe(logStream, { end: false });
    child.stderr.pipe(logStream, { end: false });
    child.on('exit', (code, signal) => {
      logStream.write(`[node-supervisor] ${new Date().toISOString()} ${name} exited code=${code} signal=${signal}\n`);
      if (!stopped) setTimeout(launch, 15000);
    });
    child.on('error', (error) => {
      logStream.write(`[node-supervisor] ${new Date().toISOString()} ${name} spawn error: ${error.message}\n`);
      if (!stopped) setTimeout(launch, 15000);
    });
  };

  launch();
  return {
    name,
    stop() {
      stopped = true;
      try { if (child && !child.killed) child.kill('SIGTERM'); } catch {}
      try { logStream.end(`[node-supervisor] ${new Date().toISOString()} stopping ${name}\n`); } catch {}
    },
  };
}

function startShadowDataSidecars(config) {
  if (!envFlag('SOURCE_SHADOW_WORKERS_ENABLED', true)) {
    console.log('[ShadowWorkers] disabled by SOURCE_SHADOW_WORKERS_ENABLED=false');
    return [];
  }
  const paperDb = process.env.PAPER_DB || './data/paper_trades.db';
  const signalDb = process.env.SENTIMENT_DB || process.env.DB_PATH || config.DB_PATH || './data/sentiment_arb.db';
  const gmgnLog = process.env.GMGN_SCOUT_LOG || './data/gmgn-scout.log';
  const resonanceLog = process.env.SOURCE_RESONANCE_LOG || './data/source-resonance.log';
  const fastLaneLog = process.env.PAPER_FAST_LANE_LOG || './data/paper-fast-lane.log';
  const reviewSnapshotLog = process.env.PAPER_REVIEW_SNAPSHOT_LOG || './data/paper-review-snapshot.log';
  const v27TelegramMirrorLog = process.env.V27_TELEGRAM_SIGNAL_MIRROR_LOG || './data/v27-telegram-signal-mirror.log';
  const v27SourceLabelMirrorLog = process.env.V27_SOURCE_LABEL_MIRROR_LOG || './data/v27-source-label-mirror.log';
  const v27PaperTradeSourceLabelMirrorLog = process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_LOG || './data/v27-paper-trade-source-label-mirror.log';
  const v27TradeOutcomeMirrorLog = process.env.V27_TRADE_OUTCOME_MIRROR_LOG || './data/v27-trade-outcome-mirror.log';
  const v27StandardizedStopMirrorLog = process.env.V27_STANDARDIZED_STOP_MIRROR_LOG || './data/v27-standardized-stop-mirror.log';
  const v27ExAnteFeasibilityMirrorLog = process.env.V27_EX_ANTE_FEASIBILITY_MIRROR_LOG || './data/v27-ex-ante-feasibility-mirror.log';
  const v27EarliestActionableMirrorLog = process.env.V27_EARLIEST_ACTIONABLE_MIRROR_LOG || './data/v27-earliest-actionable-mirror.log';
  const v27RealtimeCleanMirrorLog = process.env.V27_REALTIME_CLEAN_MIRROR_LOG || './data/v27-realtime-clean-mirror.log';
  const v27QuoteIntentBindingMirrorLog = process.env.V27_QUOTE_INTENT_BINDING_MIRROR_LOG || './data/v27-quote-intent-binding-mirror.log';
  const v27IdempotencyContractMirrorLog = process.env.V27_IDEMPOTENCY_CONTRACT_MIRROR_LOG || './data/v27-idempotency-contract-mirror.log';
  const v27ExecutionControlMirrorLog = process.env.V27_EXECUTION_CONTROL_MIRROR_LOG || './data/v27-execution-control-mirror.log';
  const v27PaperLedgerMirrorLog = process.env.V27_PAPER_LEDGER_MIRROR_LOG || './data/v27-paper-ledger-mirror.log';
  const v27PaperDecisionMirrorLog = process.env.V27_PAPER_DECISION_MIRROR_LOG || './data/v27-paper-decision-mirror.log';
  const v27LifecycleMirrorLog = process.env.V27_LIFECYCLE_MIRROR_LOG || './data/v27-lifecycle-mirror.log';
  const v27ReadModelLog = process.env.V27_READ_MODEL_REFRESH_LOG || './data/v27-read-model-refresh.log';
  const lifecycleDb = process.env.LIFECYCLE_DB || './data/lifecycle_tracks.db';

  const workers = [
    startPythonSidecar({
      name: 'gmgn-candidate-scout',
      logPath: gmgnLog,
      args: [
        'scripts/gmgn_candidate_scout.py',
        '--loop',
        '--interval', process.env.GMGN_SCOUT_INTERVAL_SEC || '30',
        '--limit', process.env.GMGN_SCOUT_LIMIT || '50',
        '--state-db', paperDb,
        '--out', process.env.GMGN_CANDIDATES_OUT || './data/gmgn_candidates.jsonl',
        '--lock-file', process.env.GMGN_SCOUT_LOCK_FILE || '/tmp/gmgn_candidate_scout.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        EXTERNAL_ALPHA_DB: paperDb,
      },
    }),
    startPythonSidecar({
      name: 'source-resonance-shadow',
      logPath: resonanceLog,
      args: [
        'scripts/source_resonance_shadow.py',
        '--loop',
        '--interval', process.env.SOURCE_RESONANCE_INTERVAL_SEC || '20',
        '--lookback-hours', process.env.SOURCE_RESONANCE_LOOKBACK_HOURS || '24',
        '--limit', process.env.SOURCE_RESONANCE_LIMIT || '500',
        '--initial-delay', process.env.SOURCE_RESONANCE_INITIAL_DELAY_SEC || '20',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--lock-file', process.env.SOURCE_RESONANCE_LOCK_FILE || '/tmp/source_resonance_shadow.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        SENTIMENT_DB: signalDb,
        DB_PATH: signalDb,
      },
    }),
  ];
  if (envFlag('PAPER_FAST_LANE_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'paper-fast-lane',
      logPath: fastLaneLog,
      args: [
        'scripts/paper_fast_lane.py',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--concurrency', process.env.FAST_ENTRY_WORKER_CONCURRENCY || '2',
        '--lock-file', process.env.PAPER_FAST_LANE_LOCK_FILE || '/tmp/paper_fast_lane.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        SENTIMENT_DB: signalDb,
        DB_PATH: signalDb,
        PAPER_FAST_ENTRY_ENABLED: process.env.PAPER_FAST_ENTRY_ENABLED || 'true',
      },
    }));
  }
  if (envFlag('PAPER_REVIEW_SNAPSHOT_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'paper-review-snapshot',
      logPath: reviewSnapshotLog,
      args: [
        'scripts/paper_review_snapshot_worker.py',
        '--loop',
        '--paper-db', paperDb,
        '--out-dir', process.env.PAPER_REVIEW_LIVE_DIR || './data/review-artifacts/live',
        '--windows', process.env.PAPER_REVIEW_WINDOWS || '2,8,12,24',
        '--interval', process.env.PAPER_REVIEW_SNAPSHOT_INTERVAL_SEC || '300',
        '--limit', process.env.PAPER_REVIEW_SNAPSHOT_LIMIT || '40',
        '--lock-file', process.env.PAPER_REVIEW_SNAPSHOT_LOCK_FILE || '/tmp/paper_review_snapshot.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        PAPER_REVIEW_LIVE_DIR: process.env.PAPER_REVIEW_LIVE_DIR || './data/review-artifacts/live',
      },
    }));
  }
  if (envFlag('V27_TELEGRAM_SIGNAL_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-telegram-signal-mirror',
      logPath: v27TelegramMirrorLog,
      args: [
        'scripts/v27_mirror_telegram_signals.py',
        '--loop',
        '--new-only',
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_TELEGRAM_SIGNAL_MIRROR_INTERVAL_SEC || '10',
        '--limit', process.env.V27_TELEGRAM_SIGNAL_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_TELEGRAM_SIGNAL_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_TELEGRAM_SIGNAL_MIRROR_LOCK_FILE || '/tmp/v27_telegram_signal_mirror.lock',
      ],
      env: {
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_SOURCE_LABEL_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-source-label-mirror',
      logPath: v27SourceLabelMirrorLog,
      args: [
        'scripts/v27_mirror_source_labels.py',
        '--loop',
        '--new-only',
        '--db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_SOURCE_LABEL_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_SOURCE_LABEL_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_SOURCE_LABEL_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_SOURCE_LABEL_MIRROR_LOCK_FILE || '/tmp/v27_source_label_mirror.lock',
      ],
      env: {
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-paper-trade-source-label-mirror',
      logPath: v27PaperTradeSourceLabelMirrorLog,
      args: [
        'scripts/v27_mirror_paper_trade_source_labels.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_LIMIT || '500',
        '--min-peak-pnl', process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIN_PEAK_PNL || '0.5',
        '--initial-delay', process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_LOCK_FILE || '/tmp/v27_paper_trade_source_label_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_TRADE_OUTCOME_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-trade-outcome-mirror',
      logPath: v27TradeOutcomeMirrorLog,
      args: [
        'scripts/v27_mirror_trade_outcomes.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_TRADE_OUTCOME_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_TRADE_OUTCOME_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_TRADE_OUTCOME_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_TRADE_OUTCOME_MIRROR_LOCK_FILE || '/tmp/v27_trade_outcome_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_STANDARDIZED_STOP_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-standardized-stop-mirror',
      logPath: v27StandardizedStopMirrorLog,
      args: [
        'scripts/v27_mirror_standardized_stops.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_STANDARDIZED_STOP_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_STANDARDIZED_STOP_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_STANDARDIZED_STOP_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_STANDARDIZED_STOP_MIRROR_LOCK_FILE || '/tmp/v27_standardized_stop_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_STANDARDIZED_STOP_VERSION: process.env.V27_STANDARDIZED_STOP_VERSION || 'legacy_standardized_stop_v0.1',
        V27_STANDARDIZED_STOP_THRESHOLD_PCT: process.env.V27_STANDARDIZED_STOP_THRESHOLD_PCT || '-30',
        V27_STANDARDIZED_STOP_WINDOW: process.env.V27_STANDARDIZED_STOP_WINDOW || '60m',
        V27_STANDARDIZED_STOP_PRICE_TYPE: process.env.V27_STANDARDIZED_STOP_PRICE_TYPE || 'delayed_executable_exit_quote_proxy',
        V27_STANDARDIZED_STOP_EXECUTABLE_REQUIRED: process.env.V27_STANDARDIZED_STOP_EXECUTABLE_REQUIRED || 'true',
        V27_STANDARDIZED_STOP_FRICTION_MODEL_VERSION: process.env.V27_STANDARDIZED_STOP_FRICTION_MODEL_VERSION || 'legacy_round_trip_friction_v0.1',
      },
    }));
  }
  if (envFlag('V27_EX_ANTE_FEASIBILITY_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-ex-ante-feasibility-mirror',
      logPath: v27ExAnteFeasibilityMirrorLog,
      args: [
        'scripts/v27_mirror_ex_ante_feasibility.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_EX_ANTE_FEASIBILITY_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_EX_ANTE_FEASIBILITY_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_EX_ANTE_FEASIBILITY_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_EX_ANTE_FEASIBILITY_MIRROR_LOCK_FILE || '/tmp/v27_ex_ante_feasibility_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_EX_ANTE_FEASIBILITY_POLICY_VERSION: process.env.V27_EX_ANTE_FEASIBILITY_POLICY_VERSION || 'legacy_actual_paper_entry_feasibility_v0.1',
      },
    }));
  }
  if (envFlag('V27_EARLIEST_ACTIONABLE_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-earliest-actionable-mirror',
      logPath: v27EarliestActionableMirrorLog,
      args: [
        'scripts/v27_mirror_earliest_actionable_times.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_EARLIEST_ACTIONABLE_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_EARLIEST_ACTIONABLE_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_EARLIEST_ACTIONABLE_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_EARLIEST_ACTIONABLE_MIRROR_LOCK_FILE || '/tmp/v27_earliest_actionable_time_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_EARLIEST_ACTIONABLE_POLICY_VERSION: process.env.V27_EARLIEST_ACTIONABLE_POLICY_VERSION || 'legacy_actual_paper_entry_actionable_time_v0.1',
      },
    }));
  }
  if (envFlag('V27_REALTIME_CLEAN_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-realtime-clean-mirror',
      logPath: v27RealtimeCleanMirrorLog,
      args: [
        'scripts/v27_mirror_realtime_clean.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_REALTIME_CLEAN_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_REALTIME_CLEAN_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_REALTIME_CLEAN_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_REALTIME_CLEAN_MIRROR_LOCK_FILE || '/tmp/v27_realtime_clean_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_REALTIME_CLEAN_STANDARD_VERSION: process.env.V27_REALTIME_CLEAN_STANDARD_VERSION || 'legacy_round_trip_quote_clean_v0.1',
        V27_REALTIME_CLEAN_QUOTE_SOURCE: process.env.V27_REALTIME_CLEAN_QUOTE_SOURCE || 'paper_trade_round_trip_quote',
      },
    }));
  }
  if (envFlag('V27_QUOTE_INTENT_BINDING_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-quote-intent-binding-mirror',
      logPath: v27QuoteIntentBindingMirrorLog,
      args: [
        'scripts/v27_mirror_quote_intent_bindings.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_QUOTE_INTENT_BINDING_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_QUOTE_INTENT_BINDING_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_QUOTE_INTENT_BINDING_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_QUOTE_INTENT_BINDING_MIRROR_LOCK_FILE || '/tmp/v27_quote_intent_binding_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_QUOTE_INTENT_BINDING_VERSION: process.env.V27_QUOTE_INTENT_BINDING_VERSION || 'legacy_paper_trade_quote_intent_binding_v0.1',
        V27_QUOTE_INTENT_BINDING_QUOTE_SOURCE: process.env.V27_QUOTE_INTENT_BINDING_QUOTE_SOURCE || 'paper_trade_entry_quote_or_legacy_proxy',
        V27_QUOTE_INTENT_LEGACY_SIZE_SOL: process.env.V27_QUOTE_INTENT_LEGACY_SIZE_SOL || '0.003',
        V27_QUOTE_INTENT_LEGACY_SLIPPAGE_BPS: process.env.V27_QUOTE_INTENT_LEGACY_SLIPPAGE_BPS || '500',
      },
    }));
  }
  if (envFlag('V27_IDEMPOTENCY_CONTRACT_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-idempotency-contract-mirror',
      logPath: v27IdempotencyContractMirrorLog,
      args: [
        'scripts/v27_mirror_idempotency_contracts.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_IDEMPOTENCY_CONTRACT_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_IDEMPOTENCY_CONTRACT_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_IDEMPOTENCY_CONTRACT_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_IDEMPOTENCY_CONTRACT_MIRROR_LOCK_FILE || '/tmp/v27_idempotency_contract_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_ENVIRONMENT_ID: process.env.V27_ENVIRONMENT_ID || process.env.NODE_ENV || 'production',
        V27_IDEMPOTENCY_CONTRACT_VERSION: process.env.V27_IDEMPOTENCY_CONTRACT_VERSION || 'legacy_paper_entry_idempotency_v0.1',
        V27_IDEMPOTENCY_NAMESPACE: process.env.V27_IDEMPOTENCY_NAMESPACE || 'paper_entry_execution',
        V27_IDEMPOTENCY_COLLISION_POLICY: process.env.V27_IDEMPOTENCY_COLLISION_POLICY || 'reject_same_namespace_key_with_different_intent_hash',
        V27_IDEMPOTENCY_HASH_ALGORITHM: process.env.V27_IDEMPOTENCY_HASH_ALGORITHM || 'sha256(canonical_json)',
      },
    }));
  }
  if (envFlag('V27_EXECUTION_CONTROL_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-execution-control-mirror',
      logPath: v27ExecutionControlMirrorLog,
      args: [
        'scripts/v27_mirror_execution_control.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_EXECUTION_CONTROL_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_EXECUTION_CONTROL_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_EXECUTION_CONTROL_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_EXECUTION_CONTROL_MIRROR_LOCK_FILE || '/tmp/v27_execution_control_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_ENVIRONMENT_ID: process.env.V27_ENVIRONMENT_ID || process.env.NODE_ENV || 'production',
        V27_EXECUTION_CONTROL_VERSION: process.env.V27_EXECUTION_CONTROL_VERSION || 'legacy_paper_entry_execution_control_v0.1',
        V27_EXECUTION_LEASE_TTL_SEC: process.env.V27_EXECUTION_LEASE_TTL_SEC || '20',
      },
    }));
  }
  if (envFlag('V27_PAPER_LEDGER_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-paper-ledger-mirror',
      logPath: v27PaperLedgerMirrorLog,
      args: [
        'scripts/v27_mirror_paper_ledgers.py',
        '--loop',
        '--new-only',
        '--paper-db', paperDb,
        '--signal-db', signalDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_PAPER_LEDGER_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_PAPER_LEDGER_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_PAPER_LEDGER_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_PAPER_LEDGER_MIRROR_LOCK_FILE || '/tmp/v27_paper_ledger_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        DB_PATH: signalDb,
        SENTIMENT_DB: signalDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_ENVIRONMENT_ID: process.env.V27_ENVIRONMENT_ID || process.env.NODE_ENV || 'production',
        V27_PAPER_LEDGER_VERSION: process.env.V27_PAPER_LEDGER_VERSION || 'legacy_paper_position_capital_ledger_v0.1',
        V27_PAPER_LEDGER_CAPITAL_BASIS_SOL: process.env.V27_PAPER_LEDGER_CAPITAL_BASIS_SOL || '100',
        V27_PAPER_LEDGER_DEFAULT_POSITION_SIZE_SOL: process.env.V27_PAPER_LEDGER_DEFAULT_POSITION_SIZE_SOL || '0.06',
        V27_PAPER_LEDGER_RESERVATION_TTL_SEC: process.env.V27_PAPER_LEDGER_RESERVATION_TTL_SEC || '20',
      },
    }));
  }
  if (envFlag('V27_PAPER_DECISION_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-paper-decision-mirror',
      logPath: v27PaperDecisionMirrorLog,
      args: [
        'scripts/v27_mirror_paper_decisions.py',
        '--loop',
        '--new-only',
        '--include-missed',
        '--db', paperDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_PAPER_DECISION_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_PAPER_DECISION_MIRROR_LIMIT || '500',
        '--missed-limit', process.env.V27_PAPER_MISSED_MIRROR_LIMIT || process.env.V27_PAPER_DECISION_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_PAPER_DECISION_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_PAPER_DECISION_MIRROR_LOCK_FILE || '/tmp/v27_paper_decision_mirror.lock',
      ],
      env: {
        PAPER_DB: paperDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_LIFECYCLE_MIRROR_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-lifecycle-mirror',
      logPath: v27LifecycleMirrorLog,
      args: [
        'scripts/v27_mirror_lifecycle_tracks.py',
        '--loop',
        '--new-only',
        '--lifecycle-db', lifecycleDb,
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--interval', process.env.V27_LIFECYCLE_MIRROR_INTERVAL_SEC || '30',
        '--limit', process.env.V27_LIFECYCLE_MIRROR_LIMIT || '500',
        '--initial-delay', process.env.V27_LIFECYCLE_MIRROR_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_LIFECYCLE_MIRROR_LOCK_FILE || '/tmp/v27_lifecycle_mirror.lock',
      ],
      env: {
        LIFECYCLE_DB: lifecycleDb,
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
      },
    }));
  }
  if (envFlag('V27_READ_MODEL_REFRESH_WORKER_ENABLED', true)) {
    workers.push(startPythonSidecar({
      name: 'v27-read-model-refresh',
      logPath: v27ReadModelLog,
      args: [
        'scripts/v27_read_model_refresh.py',
        '--loop',
        '--event-log-dir', process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        '--output-dir', process.env.V27_READ_MODEL_DIR || './data/v27_read_models',
        '--interval', process.env.V27_READ_MODEL_REFRESH_INTERVAL_SEC || '60',
        '--initial-delay', process.env.V27_READ_MODEL_REFRESH_INITIAL_DELAY_SEC || '0',
        '--lock-file', process.env.V27_READ_MODEL_REFRESH_LOCK_FILE || '/tmp/v27_read_model_refresh.lock',
      ],
      env: {
        V27_EVENT_LOG_DIR: process.env.V27_EVENT_LOG_DIR || './data/v27_event_log',
        V27_READ_MODEL_DIR: process.env.V27_READ_MODEL_DIR || './data/v27_read_models',
      },
    }));
  }
  console.log(`[ShadowWorkers] started ${workers.map((w) => w.name).join(', ')}`);
  return workers;
}

class SentimentArbitrageSystem {
  constructor() {
    this.config = this.loadConfig();
    this.db = new Database(this.config.DB_PATH);

    // Initialize services
    this.telegramService = new TelegramUserListener(this.config, this.db);
    this.solService = new SolanaSnapshotService(this.config);
    this.bscService = new BSCSnapshotService(this.config);
    this.hardGateService = new HardGateFilter(this.config);
    this.softScorer = new SoftAlphaScorer(this.config, this.db);
    this.decisionEngine = new DecisionMatrix(this.config, this.db);
    this.positionSizer = new PositionSizer(this.config, this.db);
    this.executor = new GMGNTelegramExecutor(this.config, this.db);
    this.positionMonitor = new PositionMonitor(this.config, this.db);
    this.grokClient = new GrokTwitterClient();
    this.blacklistService = new PermanentBlacklistService(this.db);

    // System state
    this.isRunning = false;
    this.processedSignals = new Map();
    this.stats = {
      signals_received: 0,
      hard_gate_passed: 0,
      soft_score_computed: 0,
      buy_decisions: 0,
      greylist_decisions: 0,
      reject_decisions: 0,
      executions_success: 0,
      executions_failed: 0
    };

    console.log('\n' + '═'.repeat(80));
    console.log('🤖 SENTIMENT ARBITRAGE SYSTEM v2.0');
    console.log('═'.repeat(80));
    console.log(`Mode: ${this.config.SHADOW_MODE ? '🎭 SHADOW' : '💰 LIVE'}`);
    console.log(`Auto Buy: ${this.config.AUTO_BUY_ENABLED ? '✅ Enabled' : '❌ Disabled'}`);
    console.log(`Database: ${this.config.DB_PATH}`);
    console.log('═'.repeat(80) + '\n');
  }

  /**
   * Load configuration from environment
   */
  loadConfig() {
    return {
      // Database
      DB_PATH: process.env.DB_PATH || './data/sentiment_arb.db',

      // System mode
      NODE_ENV: process.env.NODE_ENV || 'development',
      SHADOW_MODE: process.env.SHADOW_MODE === 'true',
      AUTO_BUY_ENABLED: process.env.AUTO_BUY_ENABLED === 'true',
      LOG_LEVEL: process.env.LOG_LEVEL || 'info',

      // Safety limits
      MAX_CONCURRENT_POSITIONS: parseInt(process.env.MAX_CONCURRENT_POSITIONS || '10'),
      MAX_DAILY_TRADES: parseInt(process.env.MAX_DAILY_TRADES || '50'),
      TOTAL_CAPITAL_SOL: parseFloat(process.env.TOTAL_CAPITAL_SOL || '10.0'),
      TOTAL_CAPITAL_BNB: parseFloat(process.env.TOTAL_CAPITAL_BNB || '1.0'),

      // Position monitor
      POSITION_MONITOR_INTERVAL_MS: 120000, // 2 minutes

      // Signal processing
      SIGNAL_POLL_INTERVAL_MS: 30000, // 30 seconds
      MIN_SIGNAL_INTERVAL_MS: 60000, // Don't reprocess same token within 1 minute

      // Soft score weights (total = 1.0)
      soft_score_weights: {
        Narrative: 0.25,
        Influence: 0.25,
        TG_Spread: 0.30,
        Graph: 0.10,
        Source: 0.10
      },

      // Soft score thresholds
      soft_score_thresholds: {
        tg_spread: {
          excellent_channels: 8,
          good_channels: 5,
          min_channels: 3,
          max_cluster_penalty: 20
        },
        holder_quality: {
          max_top10_concentration: 30,
          min_unique_holders: 100,
          risk_wallet_threshold: 50
        },
        momentum: {
          price_change_24h_min: 10,
          volume_increase_min: 2.0
        },
        security: {
          min_security_score: 60
        },
        x_validation: {
          min_unique_authors: 2,
          multiplier_below_threshold: 0.8
        }
      },

      // Hard gate thresholds
      hard_gate_thresholds: {
        SOL: {
          min_liquidity_usd: 10000,
          min_holders: 50,
          max_top10_percent: 50,
          max_slippage_bps: 200,
          max_tax_percent: 5
        },
        BSC: {
          min_liquidity_usd: 20000,
          min_holders: 100,
          max_top10_percent: 60,
          max_slippage_bps: 300,
          max_tax_percent: 5,
          owner_safe_types: ['Renounced', 'MultiSig', 'TimeLock', 'Burned']
        }
      },

      // Decision matrix configuration
      decision_matrix: {
        rules: [
          { score_min: 80, score_max: 100, rating: 'S', action: 'AUTO_BUY', position_tier: 'large' },
          { score_min: 65, score_max: 79, rating: 'A', action: 'AUTO_BUY', position_tier: 'medium' },
          { score_min: 50, score_max: 64, rating: 'B', action: 'AUTO_BUY', position_tier: 'small' },
          { score_min: 35, score_max: 49, rating: 'C', action: 'WATCH_ONLY', position_tier: null },
          { score_min: 0, score_max: 34, rating: 'F', action: 'REJECT', position_tier: null }
        ]
      },

      // Position size templates
      position_templates: {
        SOL: {
          large: { sol: 2.0, usd_approx: 200 },
          medium: { sol: 1.0, usd_approx: 100 },
          small: { sol: 0.5, usd_approx: 50 }
        },
        BSC: {
          large: { bnb: 0.5, usd_approx: 200 },
          medium: { bnb: 0.25, usd_approx: 100 },
          small: { bnb: 0.125, usd_approx: 50 }
        }
      },

      // Cooldown periods
      cooldowns: {
        same_token_minutes: 60,
        same_narrative_minutes: 30,
        failed_trade_minutes: 15
      },

      // Position limits
      position_limits: {
        max_concurrent: 10,
        max_daily_trades: 50,
        max_per_narrative: 3
      },

      // Capital allocation
      total_capital_sol: process.env.TOTAL_CAPITAL_SOL || '10.0',
      total_capital_bnb: process.env.TOTAL_CAPITAL_BNB || '1.0'
    };
  }

  /**
   * Start the system
   */
  async start() {
    try {
      console.log('▶️  Starting Sentiment Arbitrage System...\n');

      // 1. Start Telegram listener
      console.log('📱 Starting Telegram signal listener...');
      await this.telegramService.start();
      // Expose telegram service globally for API access
      global.__telegramService = this.telegramService;
      console.log('   ✅ Telegram listener active\n');

      // 2. Start position monitor
      console.log('📊 Starting position monitor...');
      await this.positionMonitor.start();
      console.log('   ✅ Position monitor active\n');

      // 3. Start signal processing loop
      this.isRunning = true;
      this.startSignalProcessingLoop();

      console.log('✅ System fully operational!\n');
      console.log('━'.repeat(80));
      console.log('Waiting for signals...\n');

    } catch (error) {
      console.error('❌ System startup failed:', error);
      throw error;
    }
  }

  /**
   * Signal processing loop
   */
  startSignalProcessingLoop() {
    this.signalInterval = setInterval(async () => {
      try {
        await this.processNewSignals();
      } catch (error) {
        console.error('❌ Signal processing error:', error.message);
      }
    }, this.config.SIGNAL_POLL_INTERVAL_MS);
  }

  /**
   * Process new signals from Telegram
   */
  async processNewSignals() {
    try {
      // Get unprocessed signals
      const signals = this.db.prepare(`
        SELECT * FROM telegram_signals
        WHERE processed = 0
        ORDER BY timestamp ASC
        LIMIT 10
      `).all();

      for (const signal of signals) {
        await this.processSignal(signal);
      }

    } catch (error) {
      console.error('❌ Process new signals error:', error.message);
    }
  }

  /**
   * Process individual signal through complete pipeline
   */
  async processSignal(signal) {
    const { id, token_ca, chain, channel_name } = signal;
    const symbol = token_ca.substring(0, 8);

    try {
      // Check if recently processed
      const cacheKey = `${chain}:${token_ca}`;
      if (this.processedSignals.has(cacheKey)) {
        const lastProcessed = this.processedSignals.get(cacheKey);
        if (Date.now() - lastProcessed < this.config.MIN_SIGNAL_INTERVAL_MS) {
          this.markSignalProcessed(id);
          return;
        }
      }

      console.log('\n' + '─'.repeat(80));
      console.log(`🔔 NEW SIGNAL: ${symbol} (${chain}) from ${channel_name}`);
      console.log('─'.repeat(80));

      this.stats.signals_received++;

      // ==========================================
      // STEP 0: PERMANENT BLACKLIST CHECK
      // ==========================================
      const blacklistRecord = this.blacklistService.isBlacklisted(token_ca, chain);
      if (blacklistRecord) {
        console.log(`\n🚫 [0/7] PERMANENT BLACKLIST HIT`);
        console.log(`   Token: ${chain}/${token_ca}`);
        console.log(`   Reason: ${blacklistRecord.blacklist_reason}`);
        console.log(`   Blacklisted: ${new Date(blacklistRecord.blacklist_timestamp).toISOString()}`);
        console.log(`   ❌ REJECTED - Permanent blacklist (不再处理)`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      // ==========================================
      // STEP 1: CHAIN SNAPSHOT + TOKEN METADATA
      // ==========================================
      console.log('\n📊 [1/7] Fetching chain snapshot...');
      const snapshot = await this.getChainSnapshot(chain, token_ca);

      if (!snapshot) {
        console.log('   ❌ Failed to get snapshot - REJECT');
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      console.log(`   ✅ Snapshot: Price=$${snapshot.current_price?.toFixed(10)}, Liquidity=$${(snapshot.liquidity_usd || 0).toFixed(0)}`);

      // Get Token Metadata (name, symbol, description) for Narrative detection
      let tokenMetadata = {
        token_ca,
        chain,
        name: null,
        symbol: symbol || null,  // Use signal symbol as fallback
        description: null
      };

      try {
        const service = chain === 'SOL' ? this.solService : this.bscService;

        // Only fetch metadata if service has getTokenMetadata method
        if (typeof service.getTokenMetadata === 'function') {
          const metadata = await service.getTokenMetadata(token_ca);
          tokenMetadata = {
            token_ca,
            chain,
            name: metadata.name || null,
            symbol: metadata.symbol || symbol || null,  // Fallback to signal symbol
            description: metadata.description || null
          };
        }
      } catch (error) {
        console.log(`   ⚠️  Token metadata fetch failed: ${error.message}`);
        // Continue with null metadata - Narrative score will be 0
      }

      // ==========================================
      // STEP 2: HARD GATES
      // ==========================================
      console.log('\n🚧 [2/7] Running hard gates...');
      const gateResult = await this.hardGateService.evaluate(snapshot, chain);

      // Handle REJECT status
      if (gateResult.status === 'REJECT') {
        const reasonText = (gateResult.reasons || []).join(', ') || 'Unknown reason';
        console.log(`   ❌ Hard gate REJECT: ${reasonText}`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      // Handle GREYLIST status
      if (gateResult.status === 'GREYLIST') {
        const reasonText = (gateResult.reasons || []).join(', ') || 'Unknown data';
        console.log(`   ⚠️  Hard gate GREYLIST: ${reasonText}`);
        // Continue processing but log as greylist
        this.stats.greylist_decisions++;
      } else {
        console.log(`   ✅ All hard gates passed (PASS)`);
        this.stats.hard_gate_passed++;
      }

      // ==========================================
      // STEP 3: SOFT ALPHA SCORE
      // ==========================================
      console.log('\n📈 [3/7] Computing soft alpha score...');

      // Collect Twitter data using Grok API
      let twitterData = null;
      try {
        console.log('   🐦 Searching Twitter via Grok API...');
        twitterData = await this.grokClient.searchToken(
          snapshot.symbol || token_ca.substring(0, 8),
          token_ca,
          15  // 15-minute window
        );
        console.log(`   ✅ Twitter: ${twitterData.mention_count} mentions, ${twitterData.engagement} engagement`);
      } catch (error) {
        console.log(`   ⚠️  Twitter search failed: ${error.message}`);
        // Continue without Twitter data
        twitterData = {
          mention_count: 0,
          unique_authors: 0,
          engagement: 0,
          sentiment: 'neutral',
          kol_count: 0
        };
      }

      // Prepare data structures for soft scorer
      const socialData = {
        // Telegram data
        total_mentions: 1,
        unique_channels: 1,
        channels: [signal.channel_name],
        message_timestamp: signal.timestamp,

        // Twitter data (from Grok API)
        twitter_mentions: twitterData.mention_count,
        twitter_unique_authors: twitterData.unique_authors,
        twitter_kol_count: twitterData.kol_count,
        twitter_engagement: twitterData.engagement,
        twitter_sentiment: twitterData.sentiment
      };

      // Use tokenMetadata (from Step 1) for Narrative detection
      // If metadata fetch failed, tokenMetadata will have null values
      const scoreResult = await this.softScorer.calculate(socialData, tokenMetadata);

      console.log(`   📊 Score: ${scoreResult.score}/100`);
      console.log(`   Components:`);
      console.log(`      - Narrative: ${scoreResult.breakdown.narrative.score.toFixed(1)}`);
      console.log(`      - Influence: ${scoreResult.breakdown.influence.score.toFixed(1)}`);
      console.log(`      - TG Spread: ${scoreResult.breakdown.tg_spread.score.toFixed(1)}`);
      console.log(`      - Graph: ${scoreResult.breakdown.graph.score.toFixed(1)}`);
      console.log(`      - Source: ${scoreResult.breakdown.source.score.toFixed(1)}`);

      this.stats.soft_score_computed++;

      // ==========================================
      // STEP 4: DECISION MATRIX
      // ==========================================
      console.log('\n🎯 [4/7] Making decision...');

      // Build evaluation object for decision engine
      const evaluation = {
        token_ca: token_ca,
        chain: chain,
        hard_gate: gateResult,
        exit_gate: { status: 'PASS', reasons: [] }, // Exit gate not yet implemented
        soft_score: scoreResult
      };

      const decision = this.decisionEngine.decide(evaluation);

      console.log(`   Decision: ${decision.action} (Rating: ${decision.rating})`);
      const reasonText = Array.isArray(decision.reasons) ? decision.reasons[0] : 'Unknown';
      console.log(`   Reason: ${reasonText}`);

      if (decision.action === 'REJECT') {
        console.log(`   ❌ Rejected`);
        this.markSignalProcessed(id);
        this.stats.reject_decisions++;
        return;
      }

      if (decision.action === 'WATCH_ONLY' || decision.action === 'WATCH') {
        console.log(`   ⚠️  Watch only - manual verification required`);
        this.markSignalProcessed(id);
        this.stats.greylist_decisions++;
        return;
      }

      // AUTO_BUY or BUY_WITH_CONFIRM
      if (decision.action === 'AUTO_BUY' || decision.action === 'BUY_WITH_CONFIRM') {
        console.log(`   ✅ BUY signal - proceeding to position sizing`);
        this.stats.buy_decisions++;
      } else {
        // Unexpected action - log warning
        console.log(`   ⚠️  Unexpected action: ${decision.action}`);
        this.markSignalProcessed(id);
        return;
      }

      // ==========================================
      // STEP 5: POSITION SIZING
      // ==========================================
      console.log('\n💰 [5/7] Calculating position size...');

      // 使用 snapshot + tokenMetadata 作为仓位检查的 token 数据
      const positionCheck = await this.positionSizer.canOpenPosition(decision, { ...snapshot, ...tokenMetadata });

      if (!positionCheck.allowed) {
        console.log(`   ❌ Cannot trade: ${positionCheck.reason}`);
        this.markSignalProcessed(id);
        return;
      }

      console.log(`   ✅ Position approved`);
      if (positionCheck.adjusted_size) {
        console.log(`      Size: ${positionCheck.adjusted_size.amount} ${chain}`);
        console.log(`      (~$${positionCheck.adjusted_size.usd_value} USD)`);
      }

      // ==========================================
      // STEP 6: EXECUTION
      // ==========================================
      console.log('\n⚡ [6/7] Executing trade...');

      if (!this.config.AUTO_BUY_ENABLED) {
        console.log(`   ⏸️  Auto-buy disabled - Skipping execution`);
        this.markSignalProcessed(id);
        return;
      }

      const tradeParams = {
        chain,
        token_ca,
        position_size: positionCheck.adjusted_size || decision.position_size,
        max_slippage_bps: 500, // 5%
        symbol: snapshot.symbol || 'Unknown'
      };

      const executionResult = await this.executor.executeBuy(tradeParams);

      if (executionResult.success) {
        console.log(`   ✅ Execution successful!`);
        console.log(`      Trade ID: ${executionResult.trade_id}`);
        console.log(`      Method: ${executionResult.method}`);
        if (executionResult.tx_hash) {
          console.log(`      TX: ${executionResult.tx_hash}`);
        }
        this.stats.executions_success++;

        // Record position
        this.recordPosition(signal, snapshot, scoreResult, positionCheck.adjusted_size || decision.position_size, executionResult);

      } else {
        console.log(`   ❌ Execution failed: ${executionResult.error}`);
        this.stats.executions_failed++;
      }

      // ==========================================
      // STEP 7: MARK PROCESSED
      // ==========================================
      this.markSignalProcessed(id);
      this.processedSignals.set(cacheKey, Date.now());

      console.log('\n✅ Signal processing complete');
      console.log('─'.repeat(80) + '\n');

    } catch (error) {
      console.error(`❌ Process signal error [${symbol}]:`, error.message);
      this.markSignalProcessed(id);
    }
  }

  /**
   * Get chain snapshot
   */
  async getChainSnapshot(chain, tokenCA) {
    try {
      const service = chain === 'SOL' ? this.solService : this.bscService;
      return await service.getSnapshot(tokenCA);
    } catch (error) {
      console.error('❌ Get snapshot error:', error.message);
      return null;
    }
  }

  /**
   * Record position in database
   */
  recordPosition(signal, snapshot, scoreResult, positionSize, executionResult) {
    try {
      this.db.prepare(`
        INSERT INTO positions (
          chain, token_ca, symbol, signal_id,
          entry_time, entry_price, position_size_native, position_size_usd,
          alpha_score, confidence, kelly_fraction,
          entry_liquidity_usd, entry_top10_holders, entry_slippage_bps,
          entry_tg_accel, entry_risk_wallets,
          trade_id, entry_tx_hash, status
        ) VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
      `).run(
        signal.chain,
        signal.token_ca,
        snapshot.symbol || signal.token_ca.substring(0, 8),
        signal.id,
        snapshot.current_price,
        positionSize.position_size_native,
        positionSize.position_size_usd,
        scoreResult.final_score,
        positionSize.confidence,
        positionSize.kelly_fraction,
        snapshot.liquidity_usd,
        snapshot.top10_holders,
        snapshot.slippage_bps_1sol,
        scoreResult.breakdown.tg_accel || 0,
        JSON.stringify(snapshot.risk_wallets || []),
        executionResult.trade_id,
        executionResult.tx_hash || null
      );

      console.log('   ✅ Position recorded in database');

    } catch (error) {
      console.error('❌ Record position error:', error.message);
    }
  }

  /**
   * Mark signal as processed
   */
  markSignalProcessed(signalId) {
    try {
      this.db.prepare(`
        UPDATE telegram_signals
        SET processed = 1
        WHERE id = ?
      `).run(signalId);
    } catch (error) {
      console.error('❌ Mark processed error:', error.message);
    }
  }

  /**
   * Stop the system
   */
  async stop() {
    console.log('\n⏹️  Stopping Sentiment Arbitrage System...\n');

    this.isRunning = false;

    if (this.signalInterval) {
      clearInterval(this.signalInterval);
    }

    this.telegramService.stop();
    this.positionMonitor.stop();

    console.log('✅ System stopped\n');
    this.printStats();
  }

  /**
   * Print system statistics
   */
  printStats() {
    console.log('━'.repeat(80));
    console.log('📊 SESSION STATISTICS');
    console.log('━'.repeat(80));
    console.log(`Signals Received:      ${this.stats.signals_received}`);
    console.log(`Hard Gate Passed:      ${this.stats.hard_gate_passed}`);
    console.log(`Scores Computed:       ${this.stats.soft_score_computed}`);
    console.log(`Buy Decisions:         ${this.stats.buy_decisions}`);
    console.log(`Greylist Decisions:    ${this.stats.greylist_decisions}`);
    console.log(`Reject Decisions:      ${this.stats.reject_decisions}`);
    console.log(`Executions Success:    ${this.stats.executions_success}`);
    console.log(`Executions Failed:     ${this.stats.executions_failed}`);
    console.log('━'.repeat(80) + '\n');
  }

  /**
   * Get system status
   */
  getStatus() {
    return {
      is_running: this.isRunning,
      mode: this.config.SHADOW_MODE ? 'shadow' : 'live',
      auto_buy_enabled: this.config.AUTO_BUY_ENABLED,
      stats: this.stats,
      telegram_status: this.telegramService.getStatus(),
      monitor_status: this.positionMonitor.getStatus()
    };
  }
}

// ==========================================
// PREMIUM CHANNEL MODE
// ==========================================

class PremiumChannelSystem {
  constructor() {
    this.config = this.loadConfig();
    this.db = new Database(this.config.DB_PATH);
    this.listener = new PremiumChannelListener(this.config);
    this.engine = new PremiumSignalEngine(this.config, this.db);
    this.autonomySidecar = null;
    this.shadowDataSidecars = [];

    // 实盘组件（必须显式 PREMIUM_LIVE_EXECUTION_ENABLED=true 才启用）
    this.jupiterExecutor = null;
    this.liveExecutionExecutor = null;
    this.livePriceMonitor = null;
    this.livePositionMonitor = null;
    this.quoteClient = null;

    const isLive = premiumLiveExecutionEnabled(this.config);

    console.log('\n' + '═'.repeat(80));
    console.log('💎 PREMIUM CHANNEL MODE');
    console.log('═'.repeat(80));
    console.log(`Mode: ${isLive ? '💰 LIVE' : '📋 PAPER_ONLY'}`);
    if (isLive) {
      console.log(`执行器: Jupiter Swap`);
      console.log(`仓位: ${process.env.PREMIUM_POSITION_SOL || '0.12'} SOL`);
      console.log(`RPC: ${process.env.SOLANA_RPC_URL || 'mainnet-beta (default)'}`);
    }
    console.log(`Channel ID: ${process.env.PREMIUM_CHANNEL_ID || '3636518327'}`);
    console.log('═'.repeat(80) + '\n');
  }

  writePaperModeSafetyRuntimeEvidence(stage) {
    try {
      const { path, evidence } = writeV27PaperModeSafetyRuntimeEvidence({
        config: this.config,
        processRole: 'premium-channel-system',
        stage,
        liveComponents: {
          jupiterExecutor: this.jupiterExecutor,
          liveExecutionExecutor: this.liveExecutionExecutor,
          livePositionMonitor: this.livePositionMonitor,
          livePriceMonitor: this.livePriceMonitor,
          quoteClient: this.quoteClient,
        },
      });
      if (!evidence.paper_live_boundary_ok) {
        console.error(`[v27-paper-mode-safety] boundary violation stage=${stage} violations=${evidence.violations.join(',')} path=${path}`);
      } else {
        console.log(`[v27-paper-mode-safety] ok stage=${stage} path=${path}`);
      }
    } catch (error) {
      console.error('[v27-paper-mode-safety] failed to write runtime evidence:', error?.message || error);
    }
  }

  loadConfig() {
    return {
      DB_PATH: process.env.DB_PATH || './data/sentiment_arb.db',
      SHADOW_MODE: process.env.SHADOW_MODE !== 'false',
      PREMIUM_LIVE_EXECUTION_ENABLED: envFlag('PREMIUM_LIVE_EXECUTION_ENABLED', false),
      PAPER_ONLY_MODE: !envFlag('PREMIUM_LIVE_EXECUTION_ENABLED', false),
      AUTO_BUY_ENABLED: process.env.AUTO_BUY_ENABLED === 'true',
      total_capital_sol: process.env.TOTAL_CAPITAL_SOL || '10.0',
      hard_gate_thresholds: {
        SOL: {
          freeze_authority: 'DISABLED',
          mint_authority: 'DISABLED',
          lp_required: 'BURNED_OR_LOCKED',
          lp_lock_min_days: 30
        }
      },
      exit_gate_thresholds: {
        SOL: {
          min_liquidity_sol: 50,
          max_top10_percent: 30,
          max_wash_with_risk: 'MEDIUM'
        }
      },
      exit_gate_slippage: {
        test_sell_percentage: 20,
        sol_reject_threshold_pct: 5,
        sol_pass_threshold_pct: 2
      },
      cooldowns: {
        same_token_cooldown_minutes: 30,
        same_narrative_max_concurrent: 3
      },
      position_limits: {
        max_concurrent: parseInt(process.env.PREMIUM_MAX_POSITIONS || '5'),
        max_daily_trades: 50
      }
    };
  }

    async start() {
        // V3.4: Start dashboard FIRST so Zeabur health check passes immediately.
        // Previously this was at the end of start(), but Telegram listener init
        // could take 30-60s, causing Zeabur to kill the container before port 3000 responded.
        startDashboardOnce();
        this.writePaperModeSafetyRuntimeEvidence('before_sidecars');
        this.shadowDataSidecars = startShadowDataSidecars(this.config);

        const isLive = premiumLiveExecutionEnabled(this.config);
        const premiumMarketDataEnabled = applyMarketDataProcessOverride('MARKET_DATA_UNIFIED_PREMIUM');

    try {
      if (isLive) {
        this.jupiterExecutor = new JupiterUltraExecutor();
        this.jupiterExecutor.initialize();
        this.liveExecutionExecutor = new ParityExecutor({ mode: 'live', executor: this.jupiterExecutor });
      }

      console.log(`📡 [价格监控] Premium 统一使用 LivePriceMonitorV2 (${isLive ? 'LIVE' : 'PAPER_ONLY'}) | unified=${premiumMarketDataEnabled}`);
      this.quoteClient = new SharedQuoteClient(undefined, {
        jupiterApiKey: process.env.JUPITER_API_KEY || '',
        sharedQuotesEnabled: premiumMarketDataEnabled && isMarketDataProcessEnabled('MARKET_DATA_UNIFIED_PREMIUM')
      });
      this.livePriceMonitor = new LivePriceMonitorV2(this.jupiterExecutor, { quoteClient: this.quoteClient });
      this.livePriceMonitor.start();
      this.engine.setLivePriceMonitor(this.livePriceMonitor);
      this.writePaperModeSafetyRuntimeEvidence('after_market_data_init');

      // K 线收集器：持续记录价格到 SQLite 供回测使用
      this.klineCollector = new KlineCollector();
      this.klineCollector.attach(this.livePriceMonitor);

      // 实盘模式：额外初始化 LivePositionMonitor + Jupiter 执行器
      if (isLive) {
        this.livePositionMonitor = new LivePositionMonitor(this.livePriceMonitor, this.liveExecutionExecutor, this.engine.riskManager);
        await this.livePositionMonitor.start();

        // 注入到 engine
        this.engine.setLiveComponents(this.liveExecutionExecutor, this.livePositionMonitor);

        // 🔧 注册退出回调：退出后触发信号引擎冷却
        this.livePositionMonitor.onExit((symbol, tokenCA, pnl) => {
          if (this.engine && this.engine.markExitCooldown) {
            this.engine.markExitCooldown(symbol);
          }
        });

        const solBalance = await this.liveExecutionExecutor.getSolBalance();
        console.log(`💰 [实盘] 钱包余额: ${solBalance.toFixed(4)} SOL`);
      }
    } catch (error) {
      console.error(`❌ [Premium] 价格/执行组件初始化失败: ${error.message}`);
      if (isLive) {
        console.log('⚠️  降级为 SHADOW 模式价格监控');
      }
      this.jupiterExecutor = null;
      this.liveExecutionExecutor = null;
      this.livePositionMonitor = null;
      if (!this.livePriceMonitor) {
        this.quoteClient = this.quoteClient || new SharedQuoteClient(undefined, {
          jupiterApiKey: process.env.JUPITER_API_KEY || '',
          sharedQuotesEnabled: premiumMarketDataEnabled && isMarketDataProcessEnabled('MARKET_DATA_UNIFIED_PREMIUM')
        });
        this.livePriceMonitor = new LivePriceMonitorV2(null, { quoteClient: this.quoteClient });
        this.livePriceMonitor.start();
        this.engine.setLivePriceMonitor(this.livePriceMonitor);
        this.klineCollector = new KlineCollector();
        this.klineCollector.attach(this.livePriceMonitor);
      }
    }

    // 初始化引擎
    await this.engine.initialize();

    // 注册信号回调
    this.listener.onSignal(async (signal) => {
      try {
        await this.engine.processSignal(signal);
      } catch (error) {
        console.error('❌ [Premium] 信号处理异常:', error.message);
      }
    });

    // 启动监听
    const listenerStarted = await this.listener.start();

    // Expose listener globally for API access (channel history)
    global.__telegramService = this.listener;

    // 把 Telegram client 传给 engine 用于 Buzz 搜索
    if (this.listener.client) {
      this.engine.setTelegramClient(this.listener.client);
    }

    console.log('\n✅ Premium Channel System 运行中...');
    if (listenerStarted) {
      console.log('   等待频道信号...\n');
    } else {
      console.log('   ⚠️ Premium Telegram listener 未连接，premium_signals 不会有新数据');
      console.log('   请先配置 TELEGRAM_API_ID / TELEGRAM_API_HASH / TELEGRAM_SESSION');
      console.log('   然后运行: node scripts/authenticate-telegram.js\n');
    }

    // 暴露给 dashboard-server 用于手动暂停/恢复交易
    global.__riskManager = this.engine.riskManager;
    global.__premiumEngine = this.engine;
    global.__autonomySidecar = this.autonomySidecar;
    if (this.liveExecutionExecutor) global.__executor = this.liveExecutionExecutor;

    if (this.autonomySidecar) {
      this.autonomySidecar.start();
    }

        // Dashboard Server already started at the top of start() for Zeabur health check
    }

  async stop() {
    await this.listener.stop();
    await this.engine.stop();
    if (this.autonomySidecar) {
      this.autonomySidecar.stop();
    }
    for (const worker of this.shadowDataSidecars || []) {
      worker.stop();
    }
    if (this.livePositionMonitor) {
      this.livePositionMonitor.stop();
    }
    if (this.livePriceMonitor) {
      this.livePriceMonitor.stop();
    }
    if (this.klineCollector) {
      this.klineCollector.stop();
    }
    this.db.close();
    console.log('⏹️  Premium Channel System 已停止');
  }
}

// ==========================================
// MAIN EXECUTION
// ==========================================

async function main() {
  runVolumePreflightOnce();
  runV27EventLogRecoveryPreflightOnce();

  const mode = process.argv.includes('--premium') || process.env.PREMIUM_MODE_ENABLED === 'true'
    ? 'premium'
    : 'default';

  // In Zeabur, keep the dashboard/health endpoint alive even if the trading
  // runtime cannot start because a SQLite file or the persistent volume is in a
  // bad state. Paper workers are supervised separately by the startup script.
  if (mode === 'premium') {
    startDashboardOnce();
  }

  let system;
  try {
    system = mode === 'premium'
      ? new PremiumChannelSystem()
      : new SentimentArbitrageSystem();
  } catch (error) {
    global.__startupError = {
      message: error?.message || String(error),
      stack: error?.stack || null,
      at: new Date().toISOString(),
      mode,
    };
    console.error('❌ Runtime construction failed:', error);
    if (mode === 'premium') {
      console.error('⚠️  Dashboard-only degraded mode is active; check /health and /api/logs.');
      return;
    }
    throw error;
  }

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n\n🛑 Received SIGINT, shutting down gracefully...');
    if (system) await system.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\n\n🛑 Received SIGTERM, shutting down gracefully...');
    if (system) await system.stop();
    process.exit(0);
  });

  // Start system
  try {
    await system.start();
  } catch (error) {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  }
}

// Run
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Unhandled error:', error);
    process.exit(1);
  });
}

export { SentimentArbitrageSystem, PremiumChannelSystem };
