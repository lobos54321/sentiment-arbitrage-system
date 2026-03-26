#!/usr/bin/env node
import fs from 'fs';
import autonomyConfig from '../src/config/autonomy-config.js';
import { FixedEvaluator } from '../src/optimizer/fixed-evaluator.js';
import { PaperStrategyRegistry } from '../src/config/paper-strategy-registry.js';

const evaluator = new FixedEvaluator(autonomyConfig);
const registry = new PaperStrategyRegistry();
const baseline = registry.getBaseline();
const exportPath = autonomyConfig.exports.filePath;

function metadataScore(signal) {
  return ((signal.ai_confidence || 0) > 0 ? 1 : 0) + (signal.ai_action ? 1 : 0) + (signal.ai_narrative_tier ? 1 : 0);
}

function actionPriority(action) {
  if (action === 'BUY') return 200;
  if (action === 'WATCH') return 100;
  return 0;
}

function gatePriority(status) {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'PASS') return 30;
  if (normalized.startsWith('GREYLIST_SIGNAL')) return 20;
  if (normalized.includes('REMOTE_LOG_IMPORT')) return 10;
  return 0;
}

function extractIndices(description = '') {
  const pick = (name) => {
    const re = new RegExp(`${name}[^0-9]*([0-9]+(?:\\.[0-9]+)?)`, 'i');
    const match = description.match(re);
    return match ? Number(match[1]) : 0;
  };

  return {
    super_index: { current: pick('super') },
    trade_index: { current: pick('trade'), signal: 0 },
    address_index: { current: pick('addr') },
    security_index: { current: pick('sec') }
  };
}

let signals = [];
if (fs.existsSync(exportPath)) {
  const parsed = JSON.parse(fs.readFileSync(exportPath, 'utf8'));
  signals = parsed.tables?.premium_signals?.rows || parsed.premium_signals || [];
} else {
  signals = evaluator.loadDataset();
}

const prioritizeRemote = process.env.AUTONOMY_PRIORITIZE_REMOTE_IMPORTS !== 'false';
const limit = parseInt(process.argv[2] || `${signals.length}`, 10);
const rankedSignals = [];
for (const signal of signals) {
  const normalizedSignal = {
    ...signal,
    token_ca: signal.token_ca || signal.tokenCa,
    is_ath: String(signal.description || '').includes('ATH') || signal.is_ath === 1 || signal.is_ath === true,
    ai_confidence: signal.ai_confidence || 0,
    indices: signal.indices || extractIndices(signal.description || '')
  };
  const cachedBars = evaluator.getCachedKlines(normalizedSignal.token_ca, Math.floor((normalizedSignal.timestamp || Date.now()) / 1000));
  const decision = registry.evaluateSignal(normalizedSignal, baseline);
  const signalContext = registry.evaluateSignal(normalizedSignal, baseline);
  const hasCoverage = cachedBars.length >= 2;
  const remoteBoost = prioritizeRemote && String(normalizedSignal.hard_gate_status || '').includes('REMOTE_LOG_IMPORT') ? 10 : 0;
  const notAthBoost = normalizedSignal.is_ath ? 0 : 12;
  const uncoveredBoost = hasCoverage ? 0 : 40;
  const metadataBoost = 3 - metadataScore(normalizedSignal);
  const gateBoost = gatePriority(normalizedSignal.hard_gate_status);
  const score = actionPriority(decision.action) + uncoveredBoost + notAthBoost + gateBoost + remoteBoost + metadataBoost + Math.round(decision.score || 0);
  rankedSignals.push({ signal: normalizedSignal, decision, hasCoverage, score, reason: signalContext.reason });
}
rankedSignals.sort((a, b) => b.score - a.score);
const selected = rankedSignals.slice(0, limit);

const results = [];
for (const item of selected) {
  const { signal, decision, hasCoverage, score, reason } = item;
  const data = await evaluator.backfillKlinesForSignal(signal);
  results.push({
    tokenCa: signal.token_ca,
    symbol: signal.symbol,
    action: decision.action,
    decisionScore: decision.score,
    priorityScore: score,
    hadCoverageBefore: hasCoverage,
    hardGateStatus: signal.hard_gate_status || null,
    reason,
    provider: data.provider,
    poolId: data.poolId,
    bars: data.bars.length,
    metrics: data.metrics || null,
    error: data.error || null
  });
  console.log(JSON.stringify(results[results.length - 1]));
}

console.log(JSON.stringify({ processed: selected.length, results }, null, 2));
