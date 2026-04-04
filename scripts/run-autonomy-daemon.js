#!/usr/bin/env node
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { spawn } from 'child_process';
import autonomyConfig from '../src/config/autonomy-config.js';
import StrategyResearchMemoryStore from '../src/database/strategy-research-memory-store.js';
import AutonomyEventStore from '../src/database/autonomy-event-store.js';
import AutonomyRunStore from '../src/database/autonomy-run-store.js';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';
import { applyMarketDataProcessOverride } from '../src/market-data/shared-market-runtime.js';

const ROOT = autonomyConfig.projectRoot;
const DATA_DIR = autonomyConfig.dataDir;
const LOG_DIR = path.join(ROOT, 'logs');
const STATUS_PATH = process.env.AUTONOMY_DAEMON_STATUS_PATH || path.join(DATA_DIR, 'autonomy-daemon-status.json');
const LOCK_PATH = process.env.AUTONOMY_DAEMON_LOCK_PATH || path.join(DATA_DIR, 'autonomy-daemon.lock');
const CONTEXT_PATH = process.env.AUTONOMY_MEMORY_CONTEXT_PATH || path.join(DATA_DIR, 'strategy-memory-context.json');
const LOG_PATH = process.env.AUTONOMY_DAEMON_LOG_PATH || path.join(LOG_DIR, 'autonomy-daemon.log');
const STEP_TIMEOUT_MS = parseInt(process.env.AUTONOMY_STEP_TIMEOUT_MS || `${20 * 60 * 1000}`, 10);
const DASHBOARD_TOKEN_REQUIRED = process.env.AUTONOMY_REQUIRE_DASHBOARD_TOKEN !== 'false';
const MAINTENANCE_SWEEP_MS = autonomyConfig.eventQueue.maintenanceSweepMs;
const IDLE_SLEEP_MS = autonomyConfig.eventQueue.idleSleepMs;
const LEASE_MS = autonomyConfig.eventQueue.leaseMs;
const RETRY_BASE_MS = autonomyConfig.eventQueue.retryBaseMs;
const RETRY_MAX_MS = autonomyConfig.eventQueue.retryMaxMs;
const MAX_ATTEMPTS = autonomyConfig.eventQueue.maxAttempts;

const remoteLoopScript = path.join(ROOT, 'scripts', 'run-remote-log-paper-loop.js');
const heliusPoolSyncScript = path.join(ROOT, 'scripts', 'run-helius-pool-sync.js');
const pipelineScript = path.join(ROOT, 'scripts', 'run-paper-eval-pipeline.js');
const featureResearchScript = path.join(ROOT, 'scripts', 'run-feature-research.js');
const strategyDraftScript = path.join(ROOT, 'scripts', 'generate-strategy-draft.js');
const researchGapScript = path.join(ROOT, 'scripts', 'analyze-research-gaps.js');
const challengerGenerationScript = path.join(ROOT, 'scripts', 'generate-differentiated-challenger.js');
const autoresearchScript = path.join(ROOT, 'scripts', 'run-autoresearch-loop.js');

const args = process.argv.slice(2);
const runOnce = args.includes('--once');
const autonomyMarketDataEnabled = applyMarketDataProcessOverride('MARKET_DATA_UNIFIED_AUTONOMY');

let shuttingDown = false;
let currentChild = null;
let consecutiveFailures = 0;
let lastSuccessAt = null;
let lastMaintenanceAt = 0;
let processedEvents = 0;
let lastAdvancementAction = null;
let lastRejectionReason = null;
let pauseState = null;

const eventStore = new AutonomyEventStore(autonomyConfig.dbPath);
const runStore = new AutonomyRunStore(autonomyConfig.dbPath);
const memoryStore = new StrategyResearchMemoryStore(autonomyConfig.dbPath);
const registry = new PaperStrategyRegistry();

function ensureDirs() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

function appendLog(message) {
  ensureDirs();
  fs.appendFileSync(LOG_PATH, `[${new Date().toISOString()}] ${message}\n`);
}

appendLog(`market-data unified autonomy=${autonomyMarketDataEnabled}`);

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
  const recent = memoryStore.listRecent(10);
  const active = memoryStore.getActiveFindings(10);
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

function updateStatus(patch = {}) {
  const previous = fs.existsSync(STATUS_PATH) ? readJson(STATUS_PATH) : {};
  const baseline = registry.getBaseline();
  const challenger = registry.getChallenger();
  const recentRun = runStore.getLatest(1)[0] || null;
  const recentEvents = eventStore.listRecent(8);
  const status = {
    ...previous,
    pid: process.pid,
    state: patch.state ?? previous.state ?? 'starting',
    currentStep: patch.currentStep ?? previous.currentStep ?? null,
    currentEvent: patch.currentEvent ?? previous.currentEvent ?? null,
    lastError: patch.lastError ?? previous.lastError ?? null,
    lastHeartbeatAt: new Date().toISOString(),
    startedAt: previous.startedAt || patch.startedAt || new Date().toISOString(),
    lockPath: LOCK_PATH,
    logPath: LOG_PATH,
    contextPath: CONTEXT_PATH,
    processedEvents,
    consecutiveFailures,
    lastSuccessAt,
    queueDepth: eventStore.countPending(),
    baseline: baseline ? { id: baseline.id, status: baseline.status } : null,
    challenger: challenger ? { id: challenger.id, status: challenger.status } : null,
    lastAdvancementAction,
    lastRejectionReason,
    pauseState,
    latestRun: recentRun,
    recentEvents,
    ...patch
  };
  writeJson(STATUS_PATH, status);
  return status;
}

function parseJsonFromOutput(stdout) {
  const trimmed = String(stdout || '').trim();
  if (!trimmed) return null;
  try {
    return JSON.parse(trimmed);
  } catch {
    const start = trimmed.indexOf('{');
    const end = trimmed.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(trimmed.slice(start, end + 1));
      } catch {}
    }
    return null;
  }
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
      appendLog(`${label} stdout: ${text.trimEnd()}`);
    });
    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      stderr += text;
      appendLog(`${label} stderr: ${text.trimEnd()}`);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timer);
      currentChild = null;
      const ok = code === 0 && !timedOut;
      appendLog(`END ${label} ok=${ok} code=${code} signal=${signal || 'none'}`);
      resolve({ ok, code, signal, stdout, stderr, timedOut, parsed: parseJsonFromOutput(stdout) });
    });

    child.on('error', (error) => {
      clearTimeout(timer);
      currentChild = null;
      appendLog(`ERROR ${label}: ${error.message}`);
      resolve({ ok: false, code: null, signal: null, stdout, stderr: `${stderr}\n${error.message}`.trim(), timedOut, parsed: null });
    });
  });
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function nextRetryDelay(attempts) {
  return Math.min(RETRY_BASE_MS * (2 ** Math.min(attempts, 5)), RETRY_MAX_MS);
}

function emitEvent(eventType, payload = {}, { parentEventId = null, dedupeKey = null, availableAt = null } = {}) {
  const event = eventStore.enqueue({
    eventId: `${eventType}-${crypto.randomUUID()}`,
    eventType,
    payload,
    dedupeKey,
    availableAt,
    parentEventId,
    maxAttempts: MAX_ATTEMPTS
  });
  appendLog(`EMIT ${event.eventType} dedupe=${event.dedupeKey || 'none'} parent=${parentEventId || 'none'}`);
  return event;
}

function initializeSeedEvents() {
  if (eventStore.countPending() > 0 || eventStore.listRecent(1).length > 0) return;
  emitEvent('remote_logs_synced', { trigger: 'seed' }, { dedupeKey: 'remote_logs_seed' });
}

function shouldPauseOnTarget(metrics = {}, guardrailResults = {}) {
  const target = autonomyConfig.pauseTargets;
  const passedGuardrails = Object.values(guardrailResults || {}).every((value) => value === true || typeof value === 'number');
  return (metrics.expectancy || 0) >= target.minExpectancy
    && (metrics.winRate || 0) >= target.minWinRate
    && (metrics.falsePositiveRate || 1) <= target.maxFalsePositiveRate
    && (metrics.sampleSize || 0) >= target.minSampleSize
    && passedGuardrails;
}

function getPauseHitCount() {
  const memories = memoryStore.listRecent(20, 'autonomy-pause-check');
  return memories.filter((item) => item.status === 'active').length;
}

function recordPauseHit(candidateId, metrics, guardrailResults) {
  memoryStore.recordFinding({
    memoryId: `pause-hit-${candidateId}-${Date.now()}`,
    memoryType: 'autonomy-pause-check',
    title: `Pause target hit for ${candidateId}`,
    summary: `expectancy=${metrics.expectancy}, winRate=${metrics.winRate}, falsePositiveRate=${metrics.falsePositiveRate}`,
    scope: 'autonomy-daemon',
    strategyId: candidateId,
    candidateId,
    evidence: { metrics, guardrailResults },
    metrics,
    status: 'active',
    tags: ['pause-target'],
    nextActions: ['等待达到重复命中阈值后进入 paused_target_reached']
  });
}

function clearPauseHits() {
  const hits = memoryStore.listRecent(20, 'autonomy-pause-check');
  for (const hit of hits) {
    memoryStore.recordFinding({ ...hit, status: 'archived' });
  }
}

async function handlePromotionReview(payload, event) {
  const candidateId = payload.candidateId;
  if (!candidateId) {
    lastRejectionReason = 'promotion_review_missing_candidate';
    return { nextEvents: [] };
  }

  const candidate = registry.getCandidate(candidateId);
  if (!candidate) {
    lastRejectionReason = `unknown_candidate_${candidateId}`;
    return { nextEvents: [] };
  }

  const metrics = candidate.metrics || {};
  const guardrailResults = candidate.guardrailResults || {};

  if (candidate.status === 'qualified') {
    await registry.setChallenger(candidateId, 'event_driven_activation');
    lastAdvancementAction = { action: 'set_challenger', candidateId, at: new Date().toISOString() };
    emitEvent('challenger_eval_completed', { candidateId }, { parentEventId: event.eventId, dedupeKey: `challenger-eval-${candidateId}` });
    return { nextEvents: ['challenger_eval_completed'] };
  }

  if (candidate.status === 'promotable') {
    const hit = shouldPauseOnTarget(metrics, guardrailResults);
    if (hit) {
      recordPauseHit(candidateId, metrics, guardrailResults);
      if (getPauseHitCount() >= autonomyConfig.pauseTargets.consecutiveHitsRequired) {
        candidate.status = 'paused_target_reached';
        candidate.pausedAt = new Date().toISOString();
        await registry.registerCandidate(candidate);
        pauseState = {
          state: 'paused_target_reached',
          candidateId,
          metrics,
          guardrailResults,
          at: new Date().toISOString()
        };
        emitEvent('autonomy_paused_target_reached', { candidateId, metrics }, { parentEventId: event.eventId, dedupeKey: `paused-target-${candidateId}` });
        return { nextEvents: ['autonomy_paused_target_reached'] };
      }
    } else {
      clearPauseHits();
    }

    await registry.promote(candidateId, 'event_driven_promotion_review');
    lastAdvancementAction = { action: 'promote', candidateId, at: new Date().toISOString() };
    emitEvent('promotion_applied', { candidateId, metrics }, { parentEventId: event.eventId, dedupeKey: `promotion-applied-${candidateId}` });
    return { nextEvents: ['promotion_applied'] };
  }

  lastRejectionReason = `promotion_review_candidate_status_${candidate.status}`;
  return { nextEvents: [] };
}

async function processEvent(event) {
  const runId = `daemon-${event.eventType}-${crypto.randomUUID()}`;
  runStore.startRun({
    runId,
    startedAt: new Date().toISOString(),
    trigger: 'autonomy-daemon',
    triggerEventId: event.eventId,
    state: 'started',
    stageName: event.eventType,
    tasks: [event.eventType],
    candidateIds: event.payload?.candidateId ? [event.payload.candidateId] : []
  });

  updateStatus({ state: 'running', currentEvent: event, currentStep: event.eventType, lastError: null });
  let outcome = { nextEvents: [], machineSummary: null };

  try {
    switch (event.eventType) {
      case 'remote_logs_synced': {
        const result = await runStep('remote-log-paper-loop', remoteLoopScript, [], {
          AUTONOMY_SKIP_BACKFILL: process.env.AUTONOMY_SKIP_BACKFILL || 'true',
          AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE: process.env.AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE || 'true',
          AUTONOMY_CACHE_ONLY_EVAL: process.env.AUTONOMY_CACHE_ONLY_EVAL || 'true',
          AUTONOMY_COVERED_ONLY_EVAL: process.env.AUTONOMY_COVERED_ONLY_EVAL || 'false',
          AUTONOMY_EVAL_DATASET_LIMIT: process.env.AUTONOMY_EVAL_DATASET_LIMIT || '1000'
        });
        if (!result.ok) throw new Error(result.stderr || result.stdout || 'remote-log-paper-loop failed');
        const machine = result.parsed?.machine || {};
        outcome.machineSummary = machine;
        if (machine.nextEventType === 'paper_eval_requested') {
          emitEvent('paper_eval_requested', { sourceEventId: event.eventId }, { parentEventId: event.eventId, dedupeKey: 'paper-eval-requested' });
          outcome.nextEvents.push('paper_eval_requested');
        } else {
          emitEvent('remote_logs_unchanged', { sourceEventId: event.eventId }, { parentEventId: event.eventId, dedupeKey: 'remote-logs-unchanged' });
          outcome.nextEvents.push('remote_logs_unchanged');
        }
        break;
      }
      case 'paper_eval_requested': {
        const result = await runStep('paper-eval-pipeline', pipelineScript, [], {
          AUTONOMY_SKIP_EXPORT_SYNC: process.env.AUTONOMY_SKIP_EXPORT_SYNC || 'true',
          AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE: process.env.AUTONOMY_SKIP_INSPECT_KLINE_COVERAGE || 'true',
          AUTONOMY_SKIP_BACKFILL: process.env.AUTONOMY_SKIP_BACKFILL || 'true',
          AUTONOMY_COVERED_ONLY_EVAL: process.env.AUTONOMY_COVERED_ONLY_EVAL || 'false',
          AUTONOMY_CACHE_ONLY_EVAL: process.env.AUTONOMY_CACHE_ONLY_EVAL || 'true',
          AUTONOMY_EVAL_DATASET_LIMIT: process.env.AUTONOMY_EVAL_DATASET_LIMIT || '1000'
        });
        if (!result.ok) throw new Error(result.stderr || result.stdout || 'paper-eval-pipeline failed');
        const machine = result.parsed?.machine || {};
        outcome.machineSummary = machine;
        emitEvent('paper_eval_completed', { machine, evaluation: result.parsed?.evaluation || null }, { parentEventId: event.eventId, dedupeKey: `paper-eval-completed-${machine.datasetFingerprint || Date.now()}` });
        outcome.nextEvents.push('paper_eval_completed');
        if (machine.reasonCode === 'coverage_gap_detected') {
          emitEvent('market_sync_requested', { sourceEventId: event.eventId }, { parentEventId: event.eventId, dedupeKey: 'market-sync-requested' });
          outcome.nextEvents.push('market_sync_requested');
        }
        if (machine.reasonCode === 'research_gap_detected') {
          emitEvent('research_gap_detected', { sourceEventId: event.eventId }, { parentEventId: event.eventId, dedupeKey: 'research-gap-detected' });
          outcome.nextEvents.push('research_gap_detected');
        }
        if (machine.reasonCode === 'promotion_review_ready') {
          emitEvent('promotion_review_ready', { candidateId: machine.candidateId }, { parentEventId: event.eventId, dedupeKey: `promotion-review-ready-${machine.candidateId}` });
          outcome.nextEvents.push('promotion_review_ready');
        }
        break;
      }
      case 'market_sync_requested': {
        const result = await runStep('helius-incremental-sync', heliusPoolSyncScript);
        if (!result.ok) throw new Error(result.stderr || result.stdout || 'helius-incremental-sync failed');
        emitEvent('market_sync_completed', { readiness: result.parsed?.readiness || null }, { parentEventId: event.eventId, dedupeKey: 'market-sync-completed' });
        emitEvent('paper_eval_requested', { sourceEventId: event.eventId }, { parentEventId: event.eventId, dedupeKey: 'paper-eval-requested' });
        outcome.nextEvents.push('market_sync_completed', 'paper_eval_requested');
        outcome.machineSummary = result.parsed?.readiness || null;
        break;
      }
      case 'research_gap_detected': {
        const featureResult = await runStep('feature-research', featureResearchScript, [], {
          AUTONOMY_FEATURE_RESEARCH_LIMIT: process.env.AUTONOMY_EVAL_DATASET_LIMIT || '1000'
        });
        if (!featureResult.ok) throw new Error(featureResult.stderr || featureResult.stdout || 'feature-research failed');
        emitEvent('feature_research_completed', { findings: featureResult.parsed?.findings || [] }, { parentEventId: event.eventId, dedupeKey: 'feature-research-completed' });

        const draftResult = await runStep('strategy-draft', strategyDraftScript);
        if (!draftResult.ok) throw new Error(draftResult.stderr || draftResult.stdout || 'strategy-draft failed');
        emitEvent('strategy_draft_completed', { draft: draftResult.parsed?.draft || null }, { parentEventId: event.eventId, dedupeKey: 'strategy-draft-completed' });

        const gapResult = await runStep('research-gap-analysis', researchGapScript);
        if (!gapResult.ok) throw new Error(gapResult.stderr || gapResult.stdout || 'research-gap-analysis failed');

        const challengerResult = await runStep('challenger-generation', challengerGenerationScript);
        if (!challengerResult.ok) throw new Error(challengerResult.stderr || challengerResult.stdout || 'challenger-generation failed');
        const candidateId = challengerResult.parsed?.candidateId || null;
        emitEvent('challenger_generated', { candidateId }, { parentEventId: event.eventId, dedupeKey: candidateId ? `challenger-generated-${candidateId}` : `challenger-generated-${Date.now()}` });
        emitEvent('challenger_eval_completed', { candidateId }, { parentEventId: event.eventId, dedupeKey: candidateId ? `challenger-eval-${candidateId}` : `challenger-eval-${Date.now()}` });
        outcome.nextEvents.push('feature_research_completed', 'strategy_draft_completed', 'challenger_generated', 'challenger_eval_completed');
        outcome.machineSummary = { candidateId };
        break;
      }
      case 'challenger_eval_completed': {
        const candidateId = event.payload?.candidateId || registry.getChallenger()?.id || null;
        const result = await runStep('autoresearch-loop', autoresearchScript, ['daemon']);
        if (!result.ok) throw new Error(result.stderr || result.stdout || 'autoresearch-loop failed');
        const parsed = result.parsed || {};
        outcome.machineSummary = parsed;
        if (parsed.recommendation === 'promotion_review_ready' && parsed.candidate?.id) {
          emitEvent('promotion_review_ready', { candidateId: parsed.candidate.id }, { parentEventId: event.eventId, dedupeKey: `promotion-review-ready-${parsed.candidate.id}` });
          outcome.nextEvents.push('promotion_review_ready');
        } else if (parsed.recommendation === 'activate_challenger_review' && parsed.candidate?.id) {
          emitEvent('promotion_review_ready', { candidateId: parsed.candidate.id }, { parentEventId: event.eventId, dedupeKey: `promotion-review-ready-${parsed.candidate.id}` });
          outcome.nextEvents.push('promotion_review_ready');
        } else {
          lastRejectionReason = parsed.reason || 'challenger_not_advanced';
        }
        break;
      }
      case 'promotion_review_ready': {
        outcome = await handlePromotionReview(event.payload || {}, event);
        break;
      }
      case 'autonomy_paused_target_reached': {
        pauseState = {
          state: 'paused_target_reached',
          at: new Date().toISOString(),
          candidateId: event.payload?.candidateId || null,
          metrics: event.payload?.metrics || null
        };
        outcome.machineSummary = pauseState;
        break;
      }
      case 'remote_logs_unchanged':
      case 'market_sync_completed':
      case 'paper_eval_completed':
      case 'feature_research_completed':
      case 'strategy_draft_completed':
      case 'challenger_generated':
      case 'promotion_applied':
        outcome.machineSummary = event.payload || null;
        break;
      default:
        throw new Error(`Unsupported event type: ${event.eventType}`);
    }

    eventStore.complete(event.eventId, {
      state: 'completed',
      payload: { ...(event.payload || {}), outcome },
      runId
    });
    runStore.finishRun(runId, {
      state: 'completed',
      stageName: event.eventType,
      tasks: [event.eventType],
      candidateIds: event.payload?.candidateId ? [event.payload.candidateId] : [],
      researchSummary: event.eventType,
      machineSummary: outcome.machineSummary,
      promotionDecision: {
        nextEvents: outcome.nextEvents || [],
        lastAdvancementAction,
        lastRejectionReason
      },
      errors: []
    });

    processedEvents += 1;
    consecutiveFailures = 0;
    lastSuccessAt = new Date().toISOString();
    return outcome;
  } catch (error) {
    const failedEvent = eventStore.fail(event.eventId, {
      error: error.message,
      retryAt: new Date(Date.now() + nextRetryDelay(event.attempts || 0)).toISOString(),
      maxAttempts: MAX_ATTEMPTS
    });
    runStore.finishRun(runId, {
      state: failedEvent?.state === 'dead_letter' ? 'dead_letter' : 'failed',
      stageName: event.eventType,
      tasks: [event.eventType],
      candidateIds: event.payload?.candidateId ? [event.payload.candidateId] : [],
      researchSummary: null,
      machineSummary: null,
      promotionDecision: null,
      errors: [error.message]
    });
    throw error;
  }
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

async function maintenanceSweep() {
  const now = Date.now();
  if (now - lastMaintenanceAt < MAINTENANCE_SWEEP_MS) return;
  lastMaintenanceAt = now;
  const recovered = eventStore.recoverExpiredLeases();
  if (recovered > 0) {
    appendLog(`Recovered ${recovered} expired event leases`);
  }
  buildMemoryContext();
}

async function main() {
  ensureDirs();
  acquireLock();
  installSignalHandlers();
  appendLog('Autonomy daemon started (event-driven mode)');
  updateStatus({ state: 'starting', currentStep: null, processedEvents, consecutiveFailures, lastSuccessAt: null, pauseState: null });
  buildMemoryContext();

  if (DASHBOARD_TOKEN_REQUIRED && !process.env.DASHBOARD_TOKEN) {
    throw new Error('DASHBOARD_TOKEN is required for autonomy daemon');
  }

  initializeSeedEvents();

  do {
    try {
      await maintenanceSweep();

      if (pauseState?.state === 'paused_target_reached') {
        updateStatus({ state: 'paused_target_reached', currentStep: null, pauseState });
        if (runOnce) break;
        await sleep(IDLE_SLEEP_MS);
        continue;
      }

      const event = eventStore.leaseNext({ leaseOwner: `${os.hostname()}:${process.pid}`, leaseMs: LEASE_MS });
      if (!event) {
        updateStatus({ state: 'idle', currentStep: null, currentEvent: null });
        if (runOnce) break;
        await sleep(IDLE_SLEEP_MS);
        continue;
      }

      appendLog(`LEASE ${event.eventType} id=${event.eventId}`);
      await processEvent(event);
      updateStatus({ state: runOnce ? 'completed' : 'idle', currentStep: null, currentEvent: null, lastError: null });
    } catch (error) {
      consecutiveFailures += 1;
      appendLog(`Event loop failure: ${error.stack || error.message}`);
      updateStatus({
        state: runOnce ? 'failed' : 'backoff',
        currentStep: null,
        currentEvent: null,
        lastError: error.message,
        consecutiveFailures,
        nextRetryAt: new Date(Date.now() + nextRetryDelay(consecutiveFailures)).toISOString()
      });
      if (runOnce) throw error;
      await sleep(nextRetryDelay(consecutiveFailures));
    }
  } while (!shuttingDown);

  appendLog('Autonomy daemon exited');
  updateStatus({ state: pauseState?.state || 'stopped', currentStep: null, currentEvent: null });
  releaseLock();
}

main().catch((error) => {
  appendLog(`Daemon fatal error: ${error.stack || error.message}`);
  updateStatus({ state: 'failed', currentStep: null, currentEvent: null, lastError: error.message, consecutiveFailures });
  releaseLock();
  process.exit(1);
});
