#!/usr/bin/env node
import fs from 'fs';
import os from 'os';
import path from 'path';
import { spawn } from 'child_process';
import autonomyConfig from '../src/config/autonomy-config.js';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';

const ROOT = autonomyConfig.projectRoot;
const DATA_DIR = autonomyConfig.dataDir;
const LOG_DIR = path.join(ROOT, 'logs');
const STATUS_PATH = process.env.AUTONOMY_DAEMON_STATUS_PATH || path.join(DATA_DIR, 'autonomy-daemon-status.json');
const LOCK_PATH = process.env.AUTONOMY_DAEMON_LOCK_PATH || path.join(DATA_DIR, 'autonomy-daemon.lock');
const CONTEXT_PATH = process.env.AUTONOMY_MEMORY_CONTEXT_PATH || path.join(DATA_DIR, 'strategy-memory-context.json');
const LOG_PATH = process.env.AUTONOMY_DAEMON_LOG_PATH || path.join(LOG_DIR, 'autonomy-daemon.log');
const BASE_INTERVAL_MS = parseInt(process.env.AUTONOMY_DAEMON_INTERVAL_MS || `${15 * 60 * 1000}`, 10);
const MAX_BACKOFF_MS = parseInt(process.env.AUTONOMY_DAEMON_MAX_BACKOFF_MS || `${2 * 60 * 60 * 1000}`, 10);
const AUTORESEARCH_EVERY = parseInt(process.env.AUTONOMY_AUTORESEARCH_EVERY || '4', 10);
const STEP_TIMEOUT_MS = parseInt(process.env.AUTONOMY_STEP_TIMEOUT_MS || `${20 * 60 * 1000}`, 10);
const DASHBOARD_TOKEN_REQUIRED = process.env.AUTONOMY_REQUIRE_DASHBOARD_TOKEN !== 'false';
const DEFAULT_DEEP_BACKFILL_LIMIT = '80';
const DEFAULT_DEEP_DATASET_LIMIT = '1000';
const LIGHT_BACKFILL_LIMIT = parseInt(process.env.AUTONOMY_LIGHT_BACKFILL_LIMIT || '15', 10);
const DEEP_BACKFILL_LIMIT = parseInt(process.env.AUTONOMY_DEEP_BACKFILL_LIMIT || process.env.AUTONOMY_BACKFILL_LIMIT || DEFAULT_DEEP_BACKFILL_LIMIT, 10);
const LIGHT_DATASET_LIMIT = parseInt(process.env.AUTONOMY_LIGHT_EVAL_DATASET_LIMIT || '250', 10);
const DEEP_DATASET_LIMIT = parseInt(process.env.AUTONOMY_DEEP_EVAL_DATASET_LIMIT || process.env.AUTONOMY_EVAL_DATASET_LIMIT || DEFAULT_DEEP_DATASET_LIMIT, 10);
const LIGHT_CACHE_ONLY_EVAL = process.env.AUTONOMY_LIGHT_CACHE_ONLY_EVAL === 'true';
const DEEP_CACHE_ONLY_EVAL = process.env.AUTONOMY_DEEP_CACHE_ONLY_EVAL !== 'true' ? false : true;
const LIGHT_COVERED_ONLY_EVAL = process.env.AUTONOMY_LIGHT_COVERED_ONLY_EVAL !== 'false';
const DEEP_COVERED_ONLY_EVAL = process.env.AUTONOMY_DEEP_COVERED_ONLY_EVAL === 'true';
const DEEP_CYCLE_EVERY = parseInt(process.env.AUTONOMY_DEEP_CYCLE_EVERY || '1', 10);
const FEATURE_RESEARCH_EVERY = parseInt(process.env.AUTONOMY_FEATURE_RESEARCH_EVERY || '1', 10);
const STRATEGY_DRAFT_EVERY = parseInt(process.env.AUTONOMY_STRATEGY_DRAFT_EVERY || '2', 10);
const RESEARCH_GAP_EVERY = parseInt(process.env.AUTONOMY_RESEARCH_GAP_EVERY || '3', 10);
const LOW_RISK_EXPANSION_EVERY = parseInt(process.env.AUTONOMY_LOW_RISK_EXPANSION_EVERY || '4', 10);
const CHALLENGER_GENERATION_EVERY = parseInt(process.env.AUTONOMY_CHALLENGER_GENERATION_EVERY || '2', 10);
const HELIUS_INCREMENTAL_SYNC_EVERY = parseInt(process.env.AUTONOMY_HELIUS_INCREMENTAL_SYNC_EVERY || '1', 10);

const remoteLoopScript = path.join(ROOT, 'scripts', 'run-remote-log-paper-loop.js');
const autoresearchScript = path.join(ROOT, 'scripts', 'run-autoresearch-loop.js');
const featureResearchScript = path.join(ROOT, 'scripts', 'run-feature-research.js');
const strategyDraftScript = path.join(ROOT, 'scripts', 'generate-strategy-draft.js');
const researchGapScript = path.join(ROOT, 'scripts', 'analyze-research-gaps.js');
const lowRiskExpansionScript = path.join(ROOT, 'scripts', 'expand-low-risk-data-collectors.js');
const challengerGenerationScript = path.join(ROOT, 'scripts', 'generate-differentiated-challenger.js');
const heliusPoolSyncScript = path.join(ROOT, 'scripts', 'run-helius-pool-sync.js');

const args = process.argv.slice(2);
const runOnce = args.includes('--once');
const skipAutoresearch = args.includes('--skip-autoresearch');
const skipRemoteLoop = args.includes('--skip-remote-loop');

let shuttingDown = false;
let cycleNumber = 0;
let consecutiveFailures = 0;
let lastSuccessAt = null;
let currentChild = null;

function ensureDirs() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

function appendLog(message) {
  ensureDirs();
  fs.appendFileSync(LOG_PATH, `[${new Date().toISOString()}] ${message}\n`);
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, payload) {
  ensureDirs();
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2));
}

function pidAlive(pid) {
  if (!pid || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function acquireLock() {
  ensureDirs();
  if (fs.existsSync(LOCK_PATH)) {
    try {
      const existing = readJson(LOCK_PATH);
      if (pidAlive(existing.pid)) {
        throw new Error(`Autonomy daemon already running with pid ${existing.pid}`);
      }
      appendLog(`Removing stale lock for pid ${existing.pid}`);
      fs.unlinkSync(LOCK_PATH);
    } catch (error) {
      if (error.message.includes('already running')) throw error;
      try { fs.unlinkSync(LOCK_PATH); } catch {}
    }
  }

  if (fs.existsSync(STATUS_PATH)) {
    try {
      const previousStatus = readJson(STATUS_PATH);
      if (!pidAlive(previousStatus.pid)) {
        writeJson(STATUS_PATH, {
          ...previousStatus,
          pid: process.pid,
          state: 'starting',
          currentStep: null,
          lastError: null,
          startedAt: new Date().toISOString(),
          lastHeartbeatAt: new Date().toISOString(),
          recoveredFromStaleStatus: true,
          previousPid: previousStatus.pid || null
        });
      }
    } catch {}
  }

  writeJson(LOCK_PATH, {
    pid: process.pid,
    hostname: os.hostname(),
    startedAt: new Date().toISOString(),
    logPath: LOG_PATH,
    statusPath: STATUS_PATH,
    contextPath: CONTEXT_PATH
  });
}

function releaseLock() {
  try { if (fs.existsSync(LOCK_PATH)) fs.unlinkSync(LOCK_PATH); } catch {}
}

function updateStatus(patch = {}) {
  const previous = fs.existsSync(STATUS_PATH) ? readJson(STATUS_PATH) : {};
  const status = {
    ...previous,
    pid: process.pid,
    state: patch.state ?? previous.state ?? 'starting',
    cycleNumber: patch.cycleNumber ?? cycleNumber,
    consecutiveFailures: patch.consecutiveFailures ?? consecutiveFailures,
    lastSuccessAt: patch.lastSuccessAt ?? lastSuccessAt,
    currentStep: patch.currentStep ?? previous.currentStep ?? null,
    lastError: patch.lastError ?? previous.lastError ?? null,
    lastHeartbeatAt: new Date().toISOString(),
    startedAt: previous.startedAt || patch.startedAt || new Date().toISOString(),
    lockPath: LOCK_PATH,
    logPath: LOG_PATH,
    contextPath: CONTEXT_PATH,
    ...patch
  };
  writeJson(STATUS_PATH, status);
  return status;
}

function summarizeMemory(findings) {
  return findings.slice(0, 5).map((finding, index) => ({
    rank: index + 1,
    title: finding.title,
    summary: finding.summary,
    nextActions: finding.nextActions,
    tags: finding.tags,
    updatedAt: finding.updatedAt
  }));
}

function buildMemoryContext() {
  const store = new StrategyResearchMemoryStore(autonomyConfig.dbPath);
  const recent = store.listRecent(10);
  const active = store.getActiveFindings(10);
  const payload = {
    generatedAt: new Date().toISOString(),
    totalRecent: recent.length,
    totalActive: active.length,
    highlights: summarizeMemory(active.length ? active : recent),
    nextActionBacklog: Array.from(new Set((active.length ? active : recent)
      .flatMap((finding) => finding.nextActions || [])
      .filter(Boolean))).slice(0, 10)
  };
  writeJson(CONTEXT_PATH, payload);
  return payload;
}

function runStep(label, scriptPath, scriptArgs = [], extraEnv = {}) {
  return new Promise((resolve) => {
    updateStatus({ state: 'running', currentStep: label, lastError: null });
    appendLog(`START ${label}`);

    const child = spawn(process.execPath, [scriptPath, ...scriptArgs], {
      cwd: ROOT,
      env: {
        ...process.env,
        ...extraEnv,
        AUTONOMY_MEMORY_CONTEXT_PATH: CONTEXT_PATH
      },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    currentChild = child;

    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      appendLog(`TIMEOUT ${label} after ${STEP_TIMEOUT_MS}ms`);
      try { child.kill('SIGTERM'); } catch {}
      setTimeout(() => {
        try { child.kill('SIGKILL'); } catch {}
      }, 5000).unref();
    }, STEP_TIMEOUT_MS);

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      stdout += text;
      updateStatus({ state: 'running', currentStep: label });
      appendLog(`${label} stdout: ${text.trimEnd()}`);
    });
    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderr += text;
      updateStatus({ state: 'running', currentStep: label });
      appendLog(`${label} stderr: ${text.trimEnd()}`);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timer);
      currentChild = null;
      const ok = code === 0 && !timedOut;
      appendLog(`END ${label} ok=${ok} code=${code} signal=${signal || 'none'}`);
      resolve({ ok, code, signal, stdout, stderr, timedOut });
    });

    child.on('error', (error) => {
      clearTimeout(timer);
      currentChild = null;
      appendLog(`ERROR ${label}: ${error.message}`);
      resolve({ ok: false, code: null, signal: null, stdout, stderr: `${stderr}\n${error.message}`.trim(), timedOut });
    });
  });
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function runCycle() {
  cycleNumber += 1;
  const cycleStartedAt = new Date().toISOString();
  updateStatus({
    state: 'running',
    currentStep: 'build-memory-context',
    cycleNumber,
    cycleStartedAt,
    lastError: null
  });

  const memoryContext = buildMemoryContext();
  appendLog(`Memory context built with ${memoryContext.highlights.length} highlights`);

  if (DASHBOARD_TOKEN_REQUIRED && !process.env.DASHBOARD_TOKEN) {
    throw new Error('DASHBOARD_TOKEN is required for autonomy daemon');
  }

  const deepCycle = DEEP_CYCLE_EVERY <= 1 || (DEEP_CYCLE_EVERY > 0 && cycleNumber % DEEP_CYCLE_EVERY === 0);
  const backfillLimit = deepCycle ? DEEP_BACKFILL_LIMIT : LIGHT_BACKFILL_LIMIT;
  const datasetLimit = deepCycle ? DEEP_DATASET_LIMIT : LIGHT_DATASET_LIMIT;
  const cacheOnlyEval = deepCycle ? DEEP_CACHE_ONLY_EVAL : LIGHT_CACHE_ONLY_EVAL;
  const coveredOnlyEval = deepCycle ? DEEP_COVERED_ONLY_EVAL : LIGHT_COVERED_ONLY_EVAL;

  if (process.env.HELIUS_API_KEY && HELIUS_INCREMENTAL_SYNC_EVERY > 0 && cycleNumber % HELIUS_INCREMENTAL_SYNC_EVERY === 0) {
    const heliusSyncResult = await runStep('helius-incremental-sync', heliusPoolSyncScript);
    if (!heliusSyncResult.ok) {
      throw new Error(`helius-incremental-sync failed${heliusSyncResult.timedOut ? ' (timeout)' : ''}: ${heliusSyncResult.stderr || heliusSyncResult.stdout || heliusSyncResult.code}`);
    }
  }

  if (!skipRemoteLoop) {
    const remoteResult = await runStep('remote-log-paper-loop', remoteLoopScript, [], {
      AUTONOMY_BACKFILL_LIMIT: `${backfillLimit}`,
      AUTONOMY_EVAL_DATASET_LIMIT: `${datasetLimit}`,
      AUTONOMY_CACHE_ONLY_EVAL: cacheOnlyEval ? 'true' : 'false',
      AUTONOMY_COVERED_ONLY_EVAL: coveredOnlyEval ? 'true' : 'false',
      AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE: deepCycle ? 'false' : 'true',
      AUTONOMY_SKIP_BACKFILL: backfillLimit <= 0 ? 'true' : 'false'
    });
    if (!remoteResult.ok) {
      throw new Error(`remote-log-paper-loop failed${remoteResult.timedOut ? ' (timeout)' : ''}: ${remoteResult.stderr || remoteResult.stdout || remoteResult.code}`);
    }
  }

  if (FEATURE_RESEARCH_EVERY > 0 && cycleNumber % FEATURE_RESEARCH_EVERY === 0) {
    const featureResearchResult = await runStep('feature-research', featureResearchScript, [], {
      AUTONOMY_FEATURE_RESEARCH_LIMIT: `${datasetLimit}`
    });
    if (!featureResearchResult.ok) {
      throw new Error(`feature-research failed${featureResearchResult.timedOut ? ' (timeout)' : ''}: ${featureResearchResult.stderr || featureResearchResult.stdout || featureResearchResult.code}`);
    }
  }

  if (STRATEGY_DRAFT_EVERY > 0 && cycleNumber % STRATEGY_DRAFT_EVERY === 0) {
    const strategyDraftResult = await runStep('strategy-draft', strategyDraftScript);
    if (!strategyDraftResult.ok) {
      throw new Error(`strategy-draft failed${strategyDraftResult.timedOut ? ' (timeout)' : ''}: ${strategyDraftResult.stderr || strategyDraftResult.stdout || strategyDraftResult.code}`);
    }
  }

  if (RESEARCH_GAP_EVERY > 0 && cycleNumber % RESEARCH_GAP_EVERY === 0) {
    const researchGapResult = await runStep('research-gap-analysis', researchGapScript);
    if (!researchGapResult.ok) {
      throw new Error(`research-gap-analysis failed${researchGapResult.timedOut ? ' (timeout)' : ''}: ${researchGapResult.stderr || researchGapResult.stdout || researchGapResult.code}`);
    }
  }

  if (LOW_RISK_EXPANSION_EVERY > 0 && cycleNumber % LOW_RISK_EXPANSION_EVERY === 0) {
    const lowRiskExpansionResult = await runStep('low-risk-expansion', lowRiskExpansionScript);
    if (!lowRiskExpansionResult.ok) {
      throw new Error(`low-risk-expansion failed${lowRiskExpansionResult.timedOut ? ' (timeout)' : ''}: ${lowRiskExpansionResult.stderr || lowRiskExpansionResult.stdout || lowRiskExpansionResult.code}`);
    }
  }

  if (CHALLENGER_GENERATION_EVERY > 0 && cycleNumber % CHALLENGER_GENERATION_EVERY === 0) {
    const challengerGenerationResult = await runStep('challenger-generation', challengerGenerationScript);
    if (!challengerGenerationResult.ok) {
      throw new Error(`challenger-generation failed${challengerGenerationResult.timedOut ? ' (timeout)' : ''}: ${challengerGenerationResult.stderr || challengerGenerationResult.stdout || challengerGenerationResult.code}`);
    }
  }

  if (!skipAutoresearch && AUTORESEARCH_EVERY > 0 && cycleNumber % AUTORESEARCH_EVERY === 0) {
    const autoresearchResult = await runStep('autoresearch-loop', autoresearchScript, ['daemon']);
    if (!autoresearchResult.ok) {
      throw new Error(`autoresearch-loop failed${autoresearchResult.timedOut ? ' (timeout)' : ''}: ${autoresearchResult.stderr || autoresearchResult.stdout || autoresearchResult.code}`);
    }
  }

  lastSuccessAt = new Date().toISOString();
  consecutiveFailures = 0;
  updateStatus({
    state: runOnce ? 'completed' : 'idle',
    currentStep: null,
    lastSuccessAt,
    lastCycleSummary: {
      cycleNumber,
      cycleStartedAt,
      completedAt: lastSuccessAt,
      memoryHighlights: memoryContext.highlights.length,
      nextActionBacklog: memoryContext.nextActionBacklog
    }
  });
}

function computeNextDelay() {
  if (consecutiveFailures <= 0) return BASE_INTERVAL_MS;
  return Math.min(BASE_INTERVAL_MS * (2 ** Math.min(consecutiveFailures, 5)), MAX_BACKOFF_MS);
}

function installSignalHandlers() {
  const handler = (signal) => {
    shuttingDown = true;
    appendLog(`Received ${signal}, shutting down`);
    updateStatus({ state: 'stopping', currentStep: null });
    try { currentChild?.kill('SIGTERM'); } catch {}
  };
  process.on('SIGINT', handler);
  process.on('SIGTERM', handler);
  process.on('exit', () => {
    releaseLock();
    try {
      updateStatus({ state: 'stopped', currentStep: null });
    } catch {}
  });
}

async function main() {
  ensureDirs();
  acquireLock();
  installSignalHandlers();
  appendLog('Autonomy daemon started');
  updateStatus({ state: 'starting', currentStep: null, cycleNumber, consecutiveFailures, lastSuccessAt: null });

  do {
    try {
      await runCycle();
    } catch (error) {
      consecutiveFailures += 1;
      const nextDelayMs = computeNextDelay();
      appendLog(`Cycle failed: ${error.stack || error.message}`);
      updateStatus({
        state: runOnce ? 'failed' : 'backoff',
        currentStep: null,
        lastError: error.message,
        consecutiveFailures,
        nextRetryAt: new Date(Date.now() + nextDelayMs).toISOString()
      });
      if (runOnce) {
        throw error;
      }
      await sleep(nextDelayMs);
      continue;
    }

    if (runOnce || shuttingDown) break;
    const delayMs = computeNextDelay();
    updateStatus({ state: 'sleeping', currentStep: null, nextRunAt: new Date(Date.now() + delayMs).toISOString() });
    await sleep(delayMs);
  } while (!shuttingDown);

  appendLog('Autonomy daemon exited');
  updateStatus({ state: 'stopped', currentStep: null });
  releaseLock();
}

main().catch((error) => {
  appendLog(`Daemon fatal error: ${error.stack || error.message}`);
  updateStatus({ state: 'failed', currentStep: null, lastError: error.message, consecutiveFailures });
  releaseLock();
  process.exit(1);
});
