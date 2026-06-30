/**
 * Web Dashboard Server
 * 
 * 提供系统状态、信号源排名、虚拟仓位表现的 Web 界面
 */

import http from 'http';
import https from 'https';
import fs from 'fs';
import { createHash, randomUUID } from 'crypto';
import { URL, fileURLToPath } from 'url';
import { basename, dirname, join, isAbsolute, relative, resolve } from 'path';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import { execFile, spawn } from 'child_process';
import {
  applyFinalBlocker,
  chooseFinalBlocker,
  finalBlockerFromEvent,
  finalBlockerFromMissed,
  finalBlockerFromTrade,
} from './lifecycle-summary-utils.js';
import { summarizePremiumSignalGateHealth } from './data-source-health-utils.js';
import {
  buildTradeReplay,
  summarizeTradeReplays,
} from './trade-replay-utils.js';
import { buildModeEvReport } from './mode-ev-utils.js';
import {
  DEFAULT_ENTRY_MODE_REGISTRY_PATH,
  loadEntryModeRegistry,
  registryModesByTier,
  summarizeEntryModeRegistry,
} from './mode-registry-utils.js';
import {
  buildNotAthRelaxedShadowCohorts,
  NOT_ATH_WATCH_PARENT_BLOCKERS,
  NOT_ATH_WATCH_PARENT_BLOCKER_SQL,
  NOT_ATH_WATCH_MISSED_REJECT_MATCH_SQL,
} from './not-ath-watch-shadow-utils.js';
import {
  buildPremiumSignalOutcomeAudit,
  LOTTO_OBSERVE_UPSTREAM_STATUSES,
} from './premium-signal-outcome-audit-utils.js';
import {
  buildPaperReviewSnapshot,
  buildTradeReviewSummary,
  writePaperReviewSnapshotFiles,
} from './paper-review-snapshot-utils.js';
import {
  buildFastFailCounterfactualAudit,
  buildSampleGovernance,
  buildShadowTrailAudit,
} from './paper-learning-audit-utils.js';
import {
  buildRawDogDecisionFunnel,
} from '../analytics/raw-dog-decision-funnel.js';
import {
  buildRawSignalOutcomeReport,
} from '../analytics/raw-signal-outcomes.js';
import {
  aggregateSwapsToRawPriceBars,
  buildRawSignalObservations,
  ensureRawPathObserverSchema,
  mergePreferredPathRows,
  normalizeRawPathBar,
  summarizeRawPathDiagnostics,
} from '../analytics/raw-path-observer.js';

dotenv.config();

const PORT = process.env.PORT || 3000;
const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';
const DASHBOARD_TOKEN = process.env.DASHBOARD_TOKEN || '';
const getExperimentLeaderboard = () => [];
const listRecentExperiments = () => [];
export const V27_API_RESPONSE_ENVELOPE_VERSION = 'v2.7.0.api_response_envelope.v1';
const PAPER_REPORT_COOLDOWN_MS = Math.max(
  0,
  parseInt(process.env.PAPER_REPORT_COOLDOWN_MS || '5000', 10) || 5000
);
let paperReportBusy = false;
let paperReportCooldownUntil = 0;

export function apiJsonHeaders(cacheControl = 'no-store') {
  return {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': cacheControl,
  };
}

export function buildApiResponseErrorShape(payload = {}) {
  const hasError = Boolean(payload.error || payload.error_code || payload.accepted === false);
  return {
    has_error: hasError,
    accepted: payload.accepted === undefined ? null : Boolean(payload.accepted),
    error_field: payload.error ? 'error' : null,
    error_code: payload.error_code || null,
    status: payload.status || null,
  };
}

export function apiEnvelopePayloadForHash(payload = {}) {
  const { payload_hash, ...unsignedPayload } = payload || {};
  return unsignedPayload;
}

export function buildV27ManualEvidenceApiResponse(responseSchemaVersion, result = {}, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const payload = {
    generated_at: generatedAt,
    materialized: false,
    endpoint: options.endpoint || null,
    envelope_version: V27_API_RESPONSE_ENVELOPE_VERSION,
    response_schema_version: responseSchemaVersion,
    refresh_schema_version: responseSchemaVersion,
    ...result,
  };
  if (payload.accepted === false && !payload.error) {
    payload.error = payload.status || 'manual_evidence_request_rejected';
  }
  if (payload.accepted === false && !payload.error_code) {
    payload.error_code = payload.error || 'manual_evidence_request_rejected';
  }
  payload.error_shape = buildApiResponseErrorShape(payload);
  payload.payload_hash = auditSha256Hex(apiEnvelopePayloadForHash(payload));
  return payload;
}

function envFlagValue(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return Boolean(defaultValue);
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false;
  return Boolean(defaultValue);
}

/**
 * 验证敏感 API 的访问令牌
 * 需要设置环境变量 DASHBOARD_TOKEN，否则敏感端点被禁用
 */
function checkAuth(req, url, res) {
  if (!DASHBOARD_TOKEN) {
    res.writeHead(403, apiJsonHeaders());
    res.end(JSON.stringify({
      error: 'DASHBOARD_TOKEN not configured. Set DASHBOARD_TOKEN env var to enable this endpoint.',
      error_code: 'dashboard_token_not_configured',
    }));
    return false;
  }
  const token = url.searchParams.get('token') || req.headers['x-dashboard-token'] || '';
  if (token !== DASHBOARD_TOKEN) {
    res.writeHead(401, apiJsonHeaders());
    res.end(JSON.stringify({ error: 'Invalid or missing token', error_code: 'invalid_or_missing_token' }));
    return false;
  }
  return true;
}

function requirePost(req, res) {
  if (req.method !== 'POST') {
    res.writeHead(405, apiJsonHeaders());
    res.end(JSON.stringify({ error: 'Use POST', error_code: 'method_not_allowed_post_required' }));
    return false;
  }
  return true;
}

function parseUnixishTime(value) {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  if (/^\d+(\.\d+)?$/.test(trimmed)) {
    const numeric = Number(trimmed);
    if (!Number.isFinite(numeric)) return null;
    return numeric > 1_000_000_000_000 ? Math.floor(numeric / 1000) : Math.floor(numeric);
  }
  const parsedMs = Date.parse(trimmed);
  if (Number.isNaN(parsedMs)) return null;
  return Math.floor(parsedMs / 1000);
}

function windowedSinceTs(url, defaultHours = 6) {
  const explicit = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
  if (explicit) return explicit;
  const all = String(url.searchParams.get('all') || '').toLowerCase();
  if (['1', 'true', 'yes'].includes(all)) return null;
  const hours = Math.max(1, Math.min(parseInt(url.searchParams.get('hours') || String(defaultHours), 10) || defaultHours, 168));
  return Math.floor(Date.now() / 1000) - hours * 3600;
}

function reportSinceTs(url, defaultWindow = '72h') {
  const explicit = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
  if (explicit) return explicit;
  const all = String(url.searchParams.get('all') || '').toLowerCase();
  const windowParam = String(url.searchParams.get('window') || defaultWindow).trim().toLowerCase();
  if (['1', 'true', 'yes'].includes(all) || windowParam === 'all') return null;
  const match = windowParam.match(/^(\d+)(h|hr|hrs|hour|hours|d|day|days)$/);
  if (match) {
    const amount = Math.max(1, parseInt(match[1], 10) || 1);
    const unit = match[2];
    const hours = unit.startsWith('d') ? amount * 24 : amount;
    return Math.floor(Date.now() / 1000) - hours * 3600;
  }
  const hours = Math.max(1, Math.min(parseInt(url.searchParams.get('hours') || '72', 10) || 72, 24 * 120));
  return Math.floor(Date.now() / 1000) - hours * 3600;
}

export function boundedIntParam(url, name, defaultValue, minValue, maxValue) {
  const raw = parseInt(url.searchParams.get(name) || String(defaultValue), 10);
  const value = Number.isFinite(raw) ? raw : defaultValue;
  return Math.max(minValue, Math.min(value, maxValue));
}

export function boundedWindowedSinceTs(url, defaultHours = 1, maxHours = 2, options = {}) {
  const explicit = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
  if (explicit) return explicit;
  const all = String(url.searchParams.get('all') || '').toLowerCase();
  if (options.allowAll && ['1', 'true', 'yes'].includes(all)) return null;
  const nowSec = Number.isFinite(options.nowSec) ? options.nowSec : Math.floor(Date.now() / 1000);
  const hours = boundedIntParam(url, 'hours', defaultHours, 1, maxHours);
  return nowSec - hours * 3600;
}

function parseWindowHoursParam(value) {
  const raw = String(value || '').trim().toLowerCase();
  const match = raw.match(/^(\d+)\s*([hd])$/);
  if (!match) return null;
  const amount = Number.parseInt(match[1], 10);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return match[2] === 'd' ? amount * 24 : amount;
}

export function livePaperQueryGuard(url, endpoint, options = {}) {
  const nowSec = Number.isFinite(options.nowSec) ? options.nowSec : Math.floor(Date.now() / 1000);
  const defaultHours = Math.max(1, Number.parseInt(String(options.defaultHours ?? 2), 10) || 2);
  const maxHours = Math.max(1, Number.parseInt(String(options.maxHours ?? 2), 10) || 2);
  const defaultLimit = Math.max(1, Number.parseInt(String(options.defaultLimit ?? 500), 10) || 500);
  const maxLimit = Math.max(1, Number.parseInt(String(options.maxLimit ?? 1000), 10) || 1000);
  const defaultBootstrapIterations = Math.max(
    1,
    Number.parseInt(String(options.defaultBootstrapIterations ?? 1000), 10) || 1000
  );
  const maxBootstrapIterations = Math.max(
    1,
    Number.parseInt(String(options.maxBootstrapIterations ?? 3000), 10) || 3000
  );
  const explicitSince = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
  const requestedHoursFromSince = explicitSince ? Math.max(0, (nowSec - explicitSince) / 3600) : null;
  const requestedHours = requestedHoursFromSince
    ?? parseWindowHoursParam(url.searchParams.get('window'))
    ?? (Number.parseInt(url.searchParams.get('hours') || String(defaultHours), 10) || defaultHours);
  const requestedLimit = Number.parseInt(url.searchParams.get('limit') || String(defaultLimit), 10) || defaultLimit;
  const requestedBootstrapIterations = Number.parseInt(
    url.searchParams.get('bootstrap_iterations') || String(defaultBootstrapIterations),
    10
  ) || defaultBootstrapIterations;
  if (requestedHours > maxHours) {
    return {
      allowed: false,
      error: 'live_paper_query_window_too_wide',
      endpoint,
      requested_hours: Number(requestedHours.toFixed(3)),
      max_hours: maxHours,
      hint: 'Use the materialized endpoint for larger windows, or retry live with hours<=2.',
    };
  }
  if (requestedLimit > maxLimit) {
    return {
      allowed: false,
      error: 'live_paper_query_limit_too_large',
      endpoint,
      requested_limit: requestedLimit,
      max_limit: maxLimit,
      hint: `Retry with limit<=${maxLimit}.`,
    };
  }
  if (requestedBootstrapIterations > maxBootstrapIterations) {
    return {
      allowed: false,
      error: 'live_paper_query_bootstrap_too_large',
      endpoint,
      requested_bootstrap_iterations: requestedBootstrapIterations,
      max_bootstrap_iterations: maxBootstrapIterations,
      hint: `Retry with bootstrap_iterations<=${maxBootstrapIterations}.`,
    };
  }
  const boundedHours = Math.max(1, Math.min(requestedHours || defaultHours, maxHours));
  return {
    allowed: true,
    endpoint,
    window_hours: boundedHours,
    since_ts: explicitSince || Math.floor(nowSec - boundedHours * 3600),
    limit: Math.max(1, Math.min(requestedLimit, maxLimit)),
    bootstrap_iterations: Math.max(1, Math.min(requestedBootstrapIterations, maxBootstrapIterations)),
    max_hours: maxHours,
    max_limit: maxLimit,
    max_bootstrap_iterations: maxBootstrapIterations,
  };
}

export function resetPaperReportGateForTest() {
  paperReportBusy = false;
  paperReportCooldownUntil = 0;
}

export function tryBeginPaperReport(endpoint = 'paper_report', nowMs = Date.now()) {
  if (paperReportBusy) {
    return {
      allowed: false,
      reason: 'paper_report_busy',
      endpoint,
      retry_after_ms: PAPER_REPORT_COOLDOWN_MS,
    };
  }
  if (nowMs < paperReportCooldownUntil) {
    return {
      allowed: false,
      reason: 'paper_report_cooldown',
      endpoint,
      retry_after_ms: paperReportCooldownUntil - nowMs,
    };
  }
  paperReportBusy = true;
  let released = false;
  return {
    allowed: true,
    endpoint,
    release(releaseNowMs = Date.now()) {
      if (released) return;
      released = true;
      paperReportBusy = false;
      paperReportCooldownUntil = Math.max(
        paperReportCooldownUntil,
        releaseNowMs + PAPER_REPORT_COOLDOWN_MS
      );
    },
  };
}

export function shouldUseMaterializedMissedRecoverySummary(requestedHours, forceLive = false) {
  return Number.isFinite(requestedHours) && requestedHours >= 2 && !forceLive;
}

function beginLivePaperReport(res, endpoint) {
  const gate = tryBeginPaperReport(endpoint);
  if (gate.allowed) return gate.release;
  res.writeHead(429, {
    'Content-Type': 'application/json',
    'Retry-After': String(Math.max(1, Math.ceil((gate.retry_after_ms || PAPER_REPORT_COOLDOWN_MS) / 1000))),
  });
  res.end(JSON.stringify({
    error: gate.reason,
    endpoint,
    retry_after_ms: gate.retry_after_ms,
  }));
  return null;
}

function rejectLivePaperQuery(res, guard) {
  res.writeHead(202, apiJsonHeaders());
  res.end(JSON.stringify({
    available: false,
    materialized: true,
    live_query: false,
    ...guard,
  }, null, 2));
}

function missedAttributionTimeWhere(sinceTs, alias = '') {
  if (!sinceTs) return '';
  const prefix = alias ? `${alias}.` : '';
  return `WHERE (
    COALESCE(${prefix}signal_ts, 0) >= @since
    OR COALESCE(${prefix}created_event_ts, 0) >= @since
    OR COALESCE(${prefix}baseline_ts, 0) >= @since
  )`;
}

function maskedSecret(value) {
  const text = String(value || '');
  if (!text) return null;
  if (text.length <= 8) return `${text.slice(0, 2)}***`;
  return `${text.slice(0, 4)}***${text.slice(-4)}`;
}

function heliusConfigHealth() {
  const apiKey = process.env.HELIUS_API_KEY || '';
  const rpcUrl = process.env.HELIUS_RPC_URL || (apiKey ? `https://mainnet.helius-rpc.com/?api-key=${apiKey}` : '');
  const urlKey = (() => {
    try {
      return new URL(rpcUrl).searchParams.get('api-key') || '';
    } catch {
      return '';
    }
  })();
  return {
    api_key_present: Boolean(apiKey),
    api_key_masked: maskedSecret(apiKey),
    rpc_url_present: Boolean(rpcUrl),
    rpc_url_has_api_key: Boolean(urlKey),
    rpc_url_key_masked: maskedSecret(urlKey),
    effective_api_key_masked: maskedSecret(urlKey || apiKey),
    effective_api_key_source: urlKey ? 'HELIUS_RPC_URL' : (apiKey ? 'HELIUS_API_KEY' : null),
    rpc_url_key_matches_api_key: Boolean(apiKey && urlKey && apiKey === urlKey),
  };
}

async function probeHeliusRpcLive(timeoutMs = 5000) {
  const config = heliusConfigHealth();
  const apiKey = process.env.HELIUS_API_KEY || '';
  const rpcUrl = process.env.HELIUS_RPC_URL || (apiKey ? `https://mainnet.helius-rpc.com/?api-key=${apiKey}` : '');
  if (!rpcUrl) {
    return { ok: false, status: 'disabled', config };
  }
  const started = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 'health', method: 'getHealth' }),
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = null;
    try { payload = text ? JSON.parse(text) : null; } catch {}
    const rpcError = payload && payload.error ? payload.error : null;
    return {
      ok: response.ok && !rpcError,
      status: response.ok && !rpcError ? 'ok' : 'error',
      http_status: response.status,
      latency_ms: Date.now() - started,
      rpc_result: payload && payload.result !== undefined ? payload.result : null,
      rpc_error_code: rpcError ? rpcError.code : null,
      rpc_error_message: rpcError ? rpcError.message : null,
      body_excerpt: rpcError ? null : String(text || '').slice(0, 160),
      config,
    };
  } catch (e) {
    return {
      ok: false,
      status: e.name === 'AbortError' ? 'timeout' : 'error',
      latency_ms: Date.now() - started,
      error: e.message,
      config,
    };
  } finally {
    clearTimeout(timer);
  }
}

function tierCaseSql(expr, prefix = '') {
  return `
          SUM(CASE WHEN ${expr} >= 1.0 THEN 1 ELSE 0 END) AS ${prefix}gold_n,
          SUM(CASE WHEN ${expr} >= 0.5 AND ${expr} < 1.0 THEN 1 ELSE 0 END) AS ${prefix}silver_n,
          SUM(CASE WHEN ${expr} >= 0.25 AND ${expr} < 0.5 THEN 1 ELSE 0 END) AS ${prefix}bronze_n,
          SUM(CASE WHEN ${expr} < 0.25 THEN 1 ELSE 0 END) AS ${prefix}sub25_n`;
}

function sqlStringLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function sqlInList(values) {
  return values.map(sqlStringLiteral).join(', ');
}

function parseJsonObject(value) {
  if (!value || typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function parseJsonValue(value, fallback = null) {
  if (!value || typeof value !== 'string') return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function firstValue(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value).trim() !== '') return value;
  }
  return null;
}

function usableSymbol(value) {
  if (value === undefined || value === null) return null;
  const symbol = String(value).trim();
  if (!symbol || symbol.toUpperCase() === 'UNKNOWN') return null;
  return symbol;
}

function normalizeUnixishMs(value) {
  if (value === undefined || value === null || value === '') return null;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric > 1_000_000_000_000 ? Math.floor(numeric) : Math.floor(numeric * 1000);
}

function normalizeUnixishSec(value) {
  const ms = normalizeUnixishMs(value);
  return ms == null ? null : Math.floor(ms / 1000);
}

function inferEntryMode(row) {
  const monitorState = parseJsonObject(row.monitor_state_json);
  const lottoState = parseJsonObject(row.lotto_state_json);
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const entryDecision = lottoState.entryDecision || {};
  const monitorContract = monitorState.entryDecisionContract || {};
  const auditContract = entryAudit.entryDecisionContract || {};
  return String(firstValue(
    row.entry_mode,
    monitorState.entryMode,
    monitorState.entry_mode,
    monitorState.smartEntryReason,
    monitorState.passReason,
    monitorContract.entry_mode,
    auditContract.entry_mode,
    entryDecision.entry_mode,
    lottoState.entry_mode,
    row.signal_route ? `${String(row.signal_route).toLowerCase()}_unknown` : null,
    row.strategy_stage,
    'unknown'
  ));
}

function entryModeBucket(entryMode, positionSizeSol) {
  const mode = String(entryMode || '').toLowerCase();
  const size = Number(positionSizeSol || 0);
  if (mode.includes('gmgn') && mode.includes('tiny_scout')) return 'gmgn_tiny_scout';
  if (mode.includes('tiny_scout')) return 'tiny_scout';
  if (mode.includes('tiny_probe')) return 'tiny_scout';
  if (mode.includes('probe') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') && size > 0 && size <= 0.005) return 'tiny_scout';
  if (mode.includes('scout') || mode.includes('probe')) return 'scout';
  return 'primary';
}

function athRecoveryFamilyFor(entryMode, monitorState = {}) {
  if (monitorState.athRecoveryFamily) return String(monitorState.athRecoveryFamily);
  const mode = String(entryMode || '');
  if (mode === 'ath_reclaim_after_failure_tiny_probe') return 'recent_failure_reclaim';
  if (mode === 'ath_matrix_dissonance_tiny_probe') return 'matrix_dissonance';
  if (mode === 'ath_micro_reclaim_tiny_probe') return 'micro_reclaim';
  return null;
}

function lifecycleSummaryKey(row) {
  const normalizedSignalTs = normalizeUnixishSec(row.signal_ts);
  return row.lifecycle_id || `${row.token_ca || 'unknown'}:${normalizedSignalTs ?? ''}`;
}

function decisionStatus(row) {
  const decision = String(row.decision || '').toLowerCase();
  const eventType = String(row.event_type || '').toLowerCase();
  if (row.component === 'execution_api' && decision === 'filled_paper') return 'entered';
  if (eventType.includes('exit') || eventType.includes('close')) return 'closed';
  if (['reject', 'skip', 'abort', 'remove', 'expire', 'block', 'fail'].includes(decision)) return 'blocked';
  if (decision === 'wait') return 'waiting';
  if (['pending', 'pass', 'arm', 'registered', 'candidate', 'received', 'warn'].includes(decision)) return 'active';
  return decision || 'unknown';
}

function roundNumber(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const factor = 10 ** digits;
  return Math.round(n * factor) / factor;
}

function percentileNumber(values, pct) {
  const sorted = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((pct / 100) * sorted.length) - 1));
  return sorted[idx];
}

function ratioToPct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n * 100;
}

function nullableFiniteNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function roundNullableNumber(value, digits = 3) {
  const n = nullableFiniteNumber(value);
  return n == null ? null : roundNumber(n, digits);
}

export function buildLottoQuoteGapAuditSummary(rows = [], options = {}) {
  const recentLimit = Math.max(0, Math.min(Number(options.recentLimit || 25), 100));
  const bySize = new Map();
  const byReason = new Map();
  const tokenSet = new Set();
  let executableEvents = 0;
  let clean10Events = 0;
  let clean30Events = 0;
  let noMarkPriceEvents = 0;
  const eventBestGaps = [];
  const recentEvents = [];

  for (const row of rows || []) {
    const payload = parseJsonObject(row.payload_json);
    const curve = Array.isArray(payload.quote_curve) ? payload.quote_curve : [];
    const token = row.token_ca || payload.token_ca || null;
    if (token) tokenSet.add(token);
    const reason = String(row.reason || payload.gate_reason || 'unknown');
    if (!byReason.has(reason)) {
      byReason.set(reason, {
        reason,
        events: 0,
        unique_tokens: new Set(),
        executable_events: 0,
        clean10_events: 0,
        clean30_events: 0,
      });
    }
    const reasonRow = byReason.get(reason);
    reasonRow.events += 1;
    if (token) reasonRow.unique_tokens.add(token);

    const executableItems = curve.filter((item) => item && item.quote_executable);
    const gaps = executableItems
      .map((item) => nullableFiniteNumber(item.quote_gap_pct))
      .filter((value) => value != null);
    const absGaps = gaps.map(Math.abs);
    const bestAbsGap = absGaps.length ? Math.min(...absGaps) : null;
    const eventExecutable = executableItems.length > 0;
    if (eventExecutable) {
      executableEvents += 1;
      reasonRow.executable_events += 1;
    }
    if (bestAbsGap !== null) {
      eventBestGaps.push(bestAbsGap);
      if (bestAbsGap <= 10) {
        clean10Events += 1;
        reasonRow.clean10_events += 1;
      }
      if (bestAbsGap <= 30) {
        clean30Events += 1;
        reasonRow.clean30_events += 1;
      }
    }
    if (payload.mark_price == null) noMarkPriceEvents += 1;

    for (const item of curve) {
      if (!item) continue;
      const key = String(item.size_key || item.size_sol || 'unknown');
      if (!bySize.has(key)) {
        bySize.set(key, {
          size_key: key,
          size_sol: Number.isFinite(Number(item.size_sol)) ? Number(item.size_sol) : null,
          probes: 0,
          executable_n: 0,
          gap_values: [],
          abs_gap_values: [],
        });
      }
      const sizeRow = bySize.get(key);
      sizeRow.probes += 1;
      if (item.quote_executable) sizeRow.executable_n += 1;
      const gap = nullableFiniteNumber(item.quote_gap_pct);
      if (gap != null) {
        sizeRow.gap_values.push(gap);
        sizeRow.abs_gap_values.push(Math.abs(gap));
      }
    }

    if (recentEvents.length < recentLimit) {
      recentEvents.push({
        id: row.id,
        event_ts: row.event_ts,
        event_iso: row.event_ts ? new Date(Number(row.event_ts) * 1000).toISOString() : null,
        token_ca: token,
        symbol: row.symbol || null,
        signal_ts: row.signal_ts ?? null,
        reason,
        gate_decision: payload.gate_decision || row.decision || null,
        entry_mode_candidate: payload.entry_mode_candidate || null,
        intent_size_sol: payload.intent_size_sol ?? null,
        mark_price: payload.mark_price ?? null,
        no_mark_price: payload.mark_price == null,
        best_abs_quote_gap_pct: bestAbsGap == null ? null : roundNumber(bestAbsGap, 3),
        quote_executable_n: executableItems.length,
        quote_curve: curve.map((item) => ({
          size_key: item.size_key,
          size_sol: item.size_sol,
          quote_executable: Boolean(item.quote_executable),
          quote_reason: item.quote_reason || null,
          quote_gap_pct: roundNullableNumber(item.quote_gap_pct, 3),
          spread_pct: roundNullableNumber(item.spread_pct, 3),
          latency_ms: item.latency_ms ?? null,
        })),
      });
    }
  }

  const sizeRows = Array.from(bySize.values())
    .map((item) => ({
      size_key: item.size_key,
      size_sol: item.size_sol,
      probes: item.probes,
      executable_n: item.executable_n,
      executable_rate_pct: item.probes ? roundNumber((item.executable_n / item.probes) * 100, 1) : null,
      gap_n: item.gap_values.length,
      avg_quote_gap_pct: item.gap_values.length
        ? roundNumber(item.gap_values.reduce((sum, value) => sum + value, 0) / item.gap_values.length, 3)
        : null,
      median_abs_quote_gap_pct: roundNullableNumber(percentileNumber(item.abs_gap_values, 50), 3),
      p90_abs_quote_gap_pct: roundNullableNumber(percentileNumber(item.abs_gap_values, 90), 3),
      max_abs_quote_gap_pct: item.abs_gap_values.length ? roundNumber(Math.max(...item.abs_gap_values), 3) : null,
    }))
    .sort((a, b) => (a.size_sol ?? Number.MAX_SAFE_INTEGER) - (b.size_sol ?? Number.MAX_SAFE_INTEGER));

  const reasonRows = Array.from(byReason.values())
    .map((item) => ({
      reason: item.reason,
      events: item.events,
      unique_tokens: item.unique_tokens.size,
      executable_events: item.executable_events,
      clean10_events: item.clean10_events,
      clean30_events: item.clean30_events,
    }))
    .sort((a, b) => b.events - a.events || b.executable_events - a.executable_events);

  return {
    audit_schema_version: 'v2.7.0.lotto_quote_gap_audit_summary.v1',
    summary: {
      events: rows.length,
      unique_tokens: tokenSet.size,
      executable_events: executableEvents,
      executable_event_rate_pct: rows.length ? roundNumber((executableEvents / rows.length) * 100, 1) : null,
      clean10_events: clean10Events,
      clean10_event_rate_pct: rows.length ? roundNumber((clean10Events / rows.length) * 100, 1) : null,
      clean30_events: clean30Events,
      clean30_event_rate_pct: rows.length ? roundNumber((clean30Events / rows.length) * 100, 1) : null,
      no_mark_price_events: noMarkPriceEvents,
      best_gap_n: eventBestGaps.length,
      median_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(eventBestGaps, 50), 3),
      p90_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(eventBestGaps, 90), 3),
      max_best_abs_quote_gap_pct: eventBestGaps.length ? roundNumber(Math.max(...eventBestGaps), 3) : null,
      sample_warning: rows.length < 30 ? 'small_sample_quote_gap_audit' : null,
    },
    by_size: sizeRows,
    by_reason: reasonRows,
    recent_events: recentEvents,
  };
}

function lottoQuoteGapStatsFromPayload(payload) {
  const curve = Array.isArray(payload.quote_curve) ? payload.quote_curve : [];
  const executableItems = curve.filter((item) => item && item.quote_executable);
  const gaps = executableItems
    .map((item) => nullableFiniteNumber(item.quote_gap_pct))
    .filter((value) => value != null);
  const absGaps = gaps.map(Math.abs);
  const bestAbsGap = absGaps.length ? Math.min(...absGaps) : null;
  return {
    curve,
    executable_n: executableItems.length,
    executable: executableItems.length > 0,
    best_abs_quote_gap_pct: bestAbsGap,
    clean10: bestAbsGap != null && bestAbsGap <= 10,
    clean30: bestAbsGap != null && bestAbsGap <= 30,
  };
}

function missedTrustedPeakPnl(row) {
  for (const key of [
    'trusted_peak_pnl',
    'executable_peak_pnl',
    'quote_clean_peak_pnl',
    'tradable_peak_pnl',
    'max_pnl_recorded',
    'pnl_24h',
    'pnl_60m',
    'pnl_15m',
    'pnl_5m',
  ]) {
    const value = nullableFiniteNumber(row?.[key]);
    if (value != null) return value;
  }
  return 0;
}

function tierForPeakPnl(value) {
  const n = nullableFiniteNumber(value) ?? 0;
  if (n >= 1.0) return 'gold';
  if (n >= 0.5) return 'silver';
  if (n >= 0.25) return 'bronze';
  return 'sub25_or_unknown';
}

function positiveUnixishSec(value) {
  const normalized = normalizeUnixishSec(value);
  return normalized && normalized > 0 ? normalized : null;
}

function bestJoinCandidateForAudit(auditRow, missedCandidates, maxJoinDeltaSec) {
  const auditPayload = parseJsonObject(auditRow.payload_json);
  const auditSignalTs = positiveUnixishSec(auditRow.signal_ts ?? auditPayload.signal_ts);
  const auditEventTs = positiveUnixishSec(auditRow.event_ts ?? auditPayload.event_ts);
  const scored = [];
  for (const row of missedCandidates || []) {
    const missedSignalTs = positiveUnixishSec(row.signal_ts);
    const missedEventTs = positiveUnixishSec(row.created_event_ts ?? row.event_ts ?? row.baseline_ts);
    const signalDelta = auditSignalTs && missedSignalTs ? Math.abs(auditSignalTs - missedSignalTs) : null;
    const eventDelta = auditEventTs && missedEventTs ? Math.abs(auditEventTs - missedEventTs) : null;
    const deltas = [signalDelta, eventDelta].filter((value) => value != null);
    const bestDelta = deltas.length ? Math.min(...deltas) : null;
    if (bestDelta != null && bestDelta > maxJoinDeltaSec) continue;
    scored.push({
      row,
      signal_delta_sec: signalDelta,
      event_delta_sec: eventDelta,
      join_delta_sec: bestDelta,
      join_basis: signalDelta != null && signalDelta === bestDelta
        ? 'signal_ts'
        : (eventDelta != null && eventDelta === bestDelta ? 'event_ts' : 'token_only'),
      trusted_peak_pnl: missedTrustedPeakPnl(row),
      missed_event_ts: missedEventTs,
    });
  }
  scored.sort((a, b) => {
    const aHasDelta = a.join_delta_sec != null ? 0 : 1;
    const bHasDelta = b.join_delta_sec != null ? 0 : 1;
    return aHasDelta - bHasDelta
      || (a.join_delta_sec ?? Number.MAX_SAFE_INTEGER) - (b.join_delta_sec ?? Number.MAX_SAFE_INTEGER)
      || b.trusted_peak_pnl - a.trusted_peak_pnl
      || (b.missed_event_ts || 0) - (a.missed_event_ts || 0)
      || Number(b.row?.id || 0) - Number(a.row?.id || 0);
  });
  return scored[0] || null;
}

function emptyLottoWinnerGapGroup(keyFields) {
  return {
    ...keyFields,
    events: 0,
    unique_tokens: new Set(),
    clean_tradable_events: 0,
    clean_tradable_tokens: new Set(),
    medal_events: 0,
    medal_tokens: new Set(),
    clean_medal_events: 0,
    clean_medal_tokens: new Set(),
    gold_events: 0,
    gold_tokens: new Set(),
    silver_events: 0,
    silver_tokens: new Set(),
    bronze_events: 0,
    bronze_tokens: new Set(),
    executable_events: 0,
    clean10_events: 0,
    clean30_events: 0,
    gap_values: [],
    peak_values: [],
  };
}

function finalizeLottoWinnerGapGroup(group) {
  const medianTrustedPeak = percentileNumber(group.peak_values, 50);
  const hiddenKeys = new Set([
    'unique_tokens',
    'clean_tradable_tokens',
    'medal_tokens',
    'clean_medal_tokens',
    'gold_tokens',
    'silver_tokens',
    'bronze_tokens',
    'gap_values',
    'peak_values',
  ]);
  return {
    ...Object.fromEntries(Object.entries(group).filter(([key]) => !hiddenKeys.has(key))),
    unique_tokens: group.unique_tokens.size,
    clean_tradable_unique: group.clean_tradable_tokens.size,
    medal_unique: group.medal_tokens.size,
    clean_medal_unique: group.clean_medal_tokens.size,
    gold_unique: group.gold_tokens.size,
    silver_unique: group.silver_tokens.size,
    bronze_unique: group.bronze_tokens.size,
    executable_rate_pct: group.events ? roundNumber((group.executable_events / group.events) * 100, 1) : null,
    clean10_rate_pct: group.events ? roundNumber((group.clean10_events / group.events) * 100, 1) : null,
    clean30_rate_pct: group.events ? roundNumber((group.clean30_events / group.events) * 100, 1) : null,
    median_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(group.gap_values, 50), 3),
    p90_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(group.gap_values, 90), 3),
    max_best_abs_quote_gap_pct: group.gap_values.length ? roundNumber(Math.max(...group.gap_values), 3) : null,
    median_trusted_peak_pnl_pct: medianTrustedPeak == null ? null : roundNumber(medianTrustedPeak * 100, 2),
    max_trusted_peak_pnl_pct: group.peak_values.length ? roundNumber(Math.max(...group.peak_values) * 100, 2) : null,
  };
}

function updateLottoWinnerGapGroupFromJoinedRow(group, row) {
  const token = row?.token_ca ? String(row.token_ca) : null;
  const trustedPeak = nullableFiniteNumber(row?.trusted_peak_pnl);
  const gap = nullableFiniteNumber(row?.best_abs_quote_gap_pct);
  const cleanTradable = Boolean(row?.clean_tradable);
  const medal = trustedPeak != null && trustedPeak >= 0.25;
  const tier = row?.tier || 'sub25_or_unknown';

  group.events += 1;
  if (token) group.unique_tokens.add(token);
  if (cleanTradable) {
    group.clean_tradable_events += 1;
    if (token) group.clean_tradable_tokens.add(token);
  }
  if (medal) {
    group.medal_events += 1;
    if (token) group.medal_tokens.add(token);
  }
  if (medal && cleanTradable) {
    group.clean_medal_events += 1;
    if (token) group.clean_medal_tokens.add(token);
  }
  if (tier === 'gold') {
    group.gold_events += 1;
    if (token) group.gold_tokens.add(token);
  } else if (tier === 'silver') {
    group.silver_events += 1;
    if (token) group.silver_tokens.add(token);
  } else if (tier === 'bronze') {
    group.bronze_events += 1;
    if (token) group.bronze_tokens.add(token);
  }
  if (Boolean(row?.clean10)) group.clean10_events += 1;
  if (Boolean(row?.clean30)) group.clean30_events += 1;
  if (Boolean(row?.quote_executable_n > 0)) group.executable_events += 1;
  if (gap != null) group.gap_values.push(gap);
  if (trustedPeak != null) group.peak_values.push(trustedPeak);
}

const LOTTO_MISSED_RESCUE_ACTIVE_LOOKBACK_SEC = Math.max(
  60,
  Number(process.env.FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC || 8 * 60 * 60) || 8 * 60 * 60,
);
const LOTTO_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC = Math.max(
  LOTTO_MISSED_RESCUE_ACTIVE_LOOKBACK_SEC,
  Number(process.env.FAST_ENTRY_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC || 24 * 60 * 60) || 24 * 60 * 60,
);
const LOTTO_MISSED_RESCUE_ALLOWED_REASONS = new Set([
  'tracking_ttl_expired',
  'not_ath_v17',
  'lotto_mc_0',
  'not_ath_prebuy_kline_retry_expired',
  'not_ath_prebuy_kline_block',
  'entry_edge_spread_too_high',
  'missing_trigger_or_quote',
  'entry_edge_probe_missing_trigger_or_quote',
  'pre_pass_signal_too_stale',
  'momentum_fading',
  'weak_buying_pressure',
  'no_kline_low_volume',
  'negative_trend',
  'chasing_top',
  'trend_bearish_timeout',
  'scout_quality_buy_pressure_weak',
  'scout_quality_volume_low',
  'scout_quality_tx_low',
  'scout_quality_negative_trend',
  'matrices not yet aligned',
]);

function lottoMissedRescueReasonAllowed(reason) {
  const normalized = String(reason || '').trim().toLowerCase();
  if (!normalized) return false;
  return LOTTO_MISSED_RESCUE_ALLOWED_REASONS.has(normalized)
    || normalized.startsWith('lotto_stale_')
    || normalized.startsWith('timeout (')
    || normalized.startsWith('price_collapse');
}

function latestTimestampForMissedRescueScan(row) {
  const candidates = [
    row?.first_tradable_ts,
    row?.created_event_ts,
    row?.signal_ts,
    row?.baseline_ts,
    row?.updated_at,
    row?.event_ts,
    row?.missed_event_ts,
  ]
    .map(parseUnixishTime)
    .filter((value) => value != null && value > 0);
  return candidates.length ? Math.max(...candidates) : null;
}

function classifyMissedRescueScanCoverage(row, nowTs, { activeLookbackSec, backlogLookbackSec } = {}) {
  const activeSec = Number(activeLookbackSec || LOTTO_MISSED_RESCUE_ACTIVE_LOOKBACK_SEC);
  const backlogSec = Number(backlogLookbackSec || LOTTO_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC);
  const scanTs = latestTimestampForMissedRescueScan(row);
  const ageSec = scanTs == null ? null : Math.floor(Number(nowTs || Math.floor(Date.now() / 1000)) - scanTs);
  const reasonAllowed = lottoMissedRescueReasonAllowed(row?.reject_reason);
  const gapReasons = [];
  if (!row?.clean_tradable) gapReasons.push('not_clean_tradable');
  if (!reasonAllowed) gapReasons.push('reject_reason_not_in_missed_rescue_scan');
  if (scanTs == null) {
    gapReasons.push('missing_scanner_timestamp');
  } else {
    const active = ageSec <= activeSec;
    const backlog = !row?.fast_lane_rescue_seen && ageSec <= backlogSec;
    if (!active && !backlog) {
      gapReasons.push(row?.fast_lane_rescue_seen
        ? 'rescue_seen_outside_active_window'
        : 'unprocessed_outside_backlog_window');
    }
  }
  const window = scanTs == null
    ? null
    : (ageSec <= activeSec
      ? 'active'
      : (!row?.fast_lane_rescue_seen && ageSec <= backlogSec ? 'unprocessed_backlog' : 'outside_window'));
  return {
    eligible: gapReasons.length === 0,
    window,
    primary_gap_reason: gapReasons[0] || null,
    gap_reasons: gapReasons,
    scan_timestamp: scanTs,
    scan_time_age_sec: ageSec,
    reason_allowed: reasonAllowed,
  };
}

function buildMissedRescueScannerCoverage(joinedRows, options = {}) {
  const activeLookbackSec = Number(options.activeLookbackSec || LOTTO_MISSED_RESCUE_ACTIVE_LOOKBACK_SEC);
  const backlogLookbackSec = Number(options.backlogLookbackSec || LOTTO_MISSED_RESCUE_BACKLOG_LOOKBACK_SEC);
  const cleanMedalTokens = new Set();
  const eligibleTokens = new Set();
  const gapTokens = new Set();
  const byGap = new Map();
  let cleanMedalEvents = 0;
  let eligibleEvents = 0;
  let gapEvents = 0;
  let rescueSeenEvents = 0;
  let rescueSeenUnique = 0;
  const rescueSeenTokens = new Set();

  for (const row of joinedRows || []) {
    const medal = Number(row?.trusted_peak_pnl || 0) >= 0.25;
    if (!row?.clean_tradable || !medal) continue;
    cleanMedalEvents += 1;
    if (row.token_ca) cleanMedalTokens.add(String(row.token_ca));
    if (row.fast_lane_rescue_seen) {
      rescueSeenEvents += 1;
      if (row.token_ca) rescueSeenTokens.add(String(row.token_ca));
    }
    if (row.fast_lane_rescue_scan_eligible) {
      eligibleEvents += 1;
      if (row.token_ca) eligibleTokens.add(String(row.token_ca));
    } else {
      gapEvents += 1;
      if (row.token_ca) gapTokens.add(String(row.token_ca));
    }
    const primaryGap = row.fast_lane_rescue_scan_eligible
      ? 'scanner_eligible'
      : (row.fast_lane_rescue_scan_primary_gap || 'scanner_gap_unknown');
    const rescueBasis = row.fast_lane_rescue_seen
      ? (row.fast_lane_rescue_match_basis || 'rescue_match_unknown')
      : 'no_rescue_state';
    const key = [
      primaryGap,
      row.fast_lane_rescue_scan_window || '-',
      rescueBasis,
      row.fast_lane_rescue_scan_reason_allowed ? 'reason_allowed' : 'reason_not_allowed',
    ].join('|');
    if (!byGap.has(key)) {
      byGap.set(key, emptyLottoWinnerGapGroup({
        scan_gap_reason: primaryGap,
        scan_window: row.fast_lane_rescue_scan_window || null,
        rescue_match_basis: rescueBasis,
        reject_reason_allowed: Boolean(row.fast_lane_rescue_scan_reason_allowed),
      }));
    }
    updateLottoWinnerGapGroupFromJoinedRow(byGap.get(key), row);
  }
  rescueSeenUnique = rescueSeenTokens.size;
  return {
    policy: {
      active_lookback_sec: activeLookbackSec,
      backlog_lookback_sec: backlogLookbackSec,
      timestamp_fields: ['first_tradable_ts', 'created_event_ts', 'signal_ts', 'baseline_ts', 'updated_at'],
      reason_policy: 'same allowlist/prefixes as scripts/paper_fast_lane.py scan_missed_rescue_once',
    },
    summary: {
      clean_medal_joined_events: cleanMedalEvents,
      clean_medal_joined_unique: cleanMedalTokens.size,
      rescue_seen_events: rescueSeenEvents,
      rescue_seen_unique: rescueSeenUnique,
      scanner_eligible_events: eligibleEvents,
      scanner_eligible_unique: eligibleTokens.size,
      scanner_gap_events: gapEvents,
      scanner_gap_unique: gapTokens.size,
    },
    by_scan_gap: Array.from(byGap.values())
      .map(finalizeLottoWinnerGapGroup)
      .sort((a, b) => b.clean_medal_unique - a.clean_medal_unique
        || b.clean_medal_events - a.clean_medal_events
        || String(a.scan_gap_reason || '').localeCompare(String(b.scan_gap_reason || ''))),
  };
}

function fastLaneQueueStatusRank(status) {
  const normalized = String(status || '').trim().toLowerCase();
  if (['queued', 'claimed', 'retry_watch'].includes(normalized)) return 0;
  if (normalized === 'entered') return 1;
  if (['quote_failed', 'rate_limited'].includes(normalized)) return 2;
  if (['rejected', 'skipped'].includes(normalized)) return 3;
  return 4;
}

export function latestActionableFastLaneQueueByToken(queueRows = []) {
  const byToken = new Map();
  for (const row of queueRows || []) {
    if (!row?.token_ca) continue;
    const token = String(row.token_ca);
    const current = byToken.get(token);
    if (!current) {
      byToken.set(token, row);
      continue;
    }
    const rowRank = fastLaneQueueStatusRank(row.status);
    const currentRank = fastLaneQueueStatusRank(current.status);
    const rowTs = parseUnixishTime(row.updated_at || row.created_at) || 0;
    const currentTs = parseUnixishTime(current.updated_at || current.created_at) || 0;
    const rowId = Number(row.id || 0);
    const currentId = Number(current.id || 0);
    if (
      rowRank < currentRank
      || (rowRank === currentRank && rowTs > currentTs)
      || (rowRank === currentRank && rowTs === currentTs && rowId > currentId)
    ) {
      byToken.set(token, row);
    }
  }
  return byToken;
}

export function buildLottoQuoteGapWinnerJoinReport(auditRows = [], missedRows = [], options = {}) {
  const recentLimit = Math.max(0, Math.min(Number(options.recentLimit || 25), 100));
  const topLimit = Math.max(1, Math.min(Number(options.topLimit || recentLimit || 25), 100));
  const maxJoinDeltaSec = Math.max(60, Math.min(Number(options.maxJoinDeltaSec || 3600), 24 * 3600));
  const nowTs = Number(options.nowTs || Math.floor(Date.now() / 1000));
  const fastLaneRescueByMissedId = options.fastLaneRescueByMissedId instanceof Map
    ? options.fastLaneRescueByMissedId
    : new Map();
  const fastLaneRescueByToken = options.fastLaneRescueByToken instanceof Map
    ? options.fastLaneRescueByToken
    : new Map();
  const fastLaneQueueByToken = options.fastLaneQueueByToken instanceof Map
    ? options.fastLaneQueueByToken
    : new Map();
  const missedByToken = new Map();
  for (const row of missedRows || []) {
    const token = row?.token_ca ? String(row.token_ca) : null;
    if (!token) continue;
    if (!missedByToken.has(token)) missedByToken.set(token, []);
    missedByToken.get(token).push(row);
  }

  const auditTokenSet = new Set();
  const joinedTokenSet = new Set();
  const joinedMedalTokenSet = new Set();
  const cleanTradableTokenSet = new Set();
  const tierUniqueSets = {
    gold: new Set(),
    silver: new Set(),
    bronze: new Set(),
    sub25_or_unknown: new Set(),
  };
  const byTier = new Map();
  const byBlocker = new Map();
  const byRecoveryState = new Map();
  const joinedRows = [];
  const unjoinedRecentAudits = [];
  const joinedGapValues = [];
  let joinedEvents = 0;
  let joinedMedalEvents = 0;
  let cleanTradableEvents = 0;
  let cleanMedalEvents = 0;
  let executableEvents = 0;
  let clean10Events = 0;
  let clean30Events = 0;

  for (const auditRow of auditRows || []) {
    const payload = parseJsonObject(auditRow.payload_json);
    const token = auditRow.token_ca || payload.token_ca || null;
    if (token) auditTokenSet.add(String(token));
    const gapStats = lottoQuoteGapStatsFromPayload(payload);
    const join = token ? bestJoinCandidateForAudit(auditRow, missedByToken.get(String(token)), maxJoinDeltaSec) : null;
    if (!join) {
      if (unjoinedRecentAudits.length < recentLimit) {
        unjoinedRecentAudits.push({
          id: auditRow.id,
          event_ts: auditRow.event_ts,
          event_iso: auditRow.event_ts ? new Date(Number(auditRow.event_ts) * 1000).toISOString() : null,
          token_ca: token,
          symbol: auditRow.symbol || payload.symbol || null,
          reason: String(auditRow.reason || payload.gate_reason || 'unknown'),
          best_abs_quote_gap_pct: roundNullableNumber(gapStats.best_abs_quote_gap_pct, 3),
          quote_executable_n: gapStats.executable_n,
        });
      }
      continue;
    }

    const missed = join.row;
    const trustedPeak = join.trusted_peak_pnl;
    const tier = tierForPeakPnl(trustedPeak);
    const cleanTradable = Number(missed.tradable_missed || 0) === 1
      && Number(missed.would_stop_before_peak || 0) !== 1;
    const rescueByMissedId = missed.id != null ? fastLaneRescueByMissedId.get(Number(missed.id)) || null : null;
    const rescueByToken = token ? fastLaneRescueByToken.get(String(token)) || null : null;
    const rescue = rescueByMissedId || rescueByToken || null;
    const fastLaneRescueMatchBasis = rescueByMissedId
      ? 'missed_attribution_id'
      : (rescueByToken ? 'token_ca' : null);
    const queue = token ? fastLaneQueueByToken.get(String(token)) || null : null;
    const fastLaneRescueState = rescue?.state || null;
    const fastLaneRescueLastStatus = rescue?.last_status || null;
    const fastLaneRescueLastReason = rescue?.last_reason || null;
    const fastLaneLastStatus = fastLaneRescueLastStatus || queue?.status || null;
    const fastLaneLastReason = fastLaneRescueLastReason || queue?.first_error || queue?.last_error || null;
    const fastLaneEntryBranch = rescue?.entry_branch || queue?.entry_branch || null;
    const fastLaneEntryModeHint = rescue?.entry_mode_hint || queue?.entry_mode_hint || payload.entry_mode_candidate || null;
    const fastLaneBlocker = rescue?.blocker || null;
    const fastLaneQueueStatus = queue?.status || null;
    const fastLaneQueueReason = queue?.first_error || queue?.last_error || null;
    joinedEvents += 1;
    if (token) joinedTokenSet.add(String(token));
    if (trustedPeak >= 0.25) {
      joinedMedalEvents += 1;
      if (token) joinedMedalTokenSet.add(String(token));
    }
    if (trustedPeak >= 0.25 && cleanTradable) cleanMedalEvents += 1;
    if (token && tierUniqueSets[tier]) tierUniqueSets[tier].add(String(token));
    if (cleanTradable) {
      cleanTradableEvents += 1;
      if (token) cleanTradableTokenSet.add(String(token));
    }
    if (gapStats.executable) executableEvents += 1;
    if (gapStats.clean10) clean10Events += 1;
    if (gapStats.clean30) clean30Events += 1;
    if (gapStats.best_abs_quote_gap_pct != null) joinedGapValues.push(gapStats.best_abs_quote_gap_pct);

    if (!byTier.has(tier)) byTier.set(tier, emptyLottoWinnerGapGroup({ tier }));
    const tierGroup = byTier.get(tier);
    const blockerKey = [
      missed.route || '-',
      missed.component || missed.final_component || '-',
      missed.reject_reason || missed.final_reason || '-',
    ].join('|');
    if (!byBlocker.has(blockerKey)) {
      byBlocker.set(blockerKey, emptyLottoWinnerGapGroup({
        route: missed.route || '-',
        component: missed.component || missed.final_component || '-',
        reject_reason: missed.reject_reason || missed.final_reason || '-',
      }));
    }
    for (const group of [tierGroup, byBlocker.get(blockerKey)]) {
      group.events += 1;
      if (token) group.unique_tokens.add(String(token));
      if (cleanTradable) {
        group.clean_tradable_events += 1;
        if (token) group.clean_tradable_tokens.add(String(token));
      }
      if (trustedPeak >= 0.25) {
        group.medal_events += 1;
        if (token) group.medal_tokens.add(String(token));
      }
      if (trustedPeak >= 0.25 && cleanTradable) {
        group.clean_medal_events += 1;
        if (token) group.clean_medal_tokens.add(String(token));
      }
      if (tier === 'gold') {
        group.gold_events += 1;
        if (token) group.gold_tokens.add(String(token));
      }
      if (tier === 'silver') {
        group.silver_events += 1;
        if (token) group.silver_tokens.add(String(token));
      }
      if (tier === 'bronze') {
        group.bronze_events += 1;
        if (token) group.bronze_tokens.add(String(token));
      }
      if (gapStats.executable) group.executable_events += 1;
      if (gapStats.clean10) group.clean10_events += 1;
      if (gapStats.clean30) group.clean30_events += 1;
      if (gapStats.best_abs_quote_gap_pct != null) group.gap_values.push(gapStats.best_abs_quote_gap_pct);
      group.peak_values.push(trustedPeak);
    }

    joinedRows.push({
      audit_id: auditRow.id,
      missed_id: missed.id ?? null,
      token_ca: token,
      symbol: auditRow.symbol || missed.symbol || payload.symbol || null,
      audit_event_ts: auditRow.event_ts ?? null,
      audit_event_iso: auditRow.event_ts ? new Date(Number(auditRow.event_ts) * 1000).toISOString() : null,
      audit_signal_ts: auditRow.signal_ts ?? null,
      missed_event_ts: missed.created_event_ts ?? missed.event_ts ?? missed.baseline_ts ?? null,
      missed_signal_ts: missed.signal_ts ?? null,
      join_basis: join.join_basis,
      join_delta_sec: join.join_delta_sec,
      route: missed.route || null,
      component: missed.component || missed.final_component || null,
      reject_reason: missed.reject_reason || missed.final_reason || null,
      tradable_missed: missed.tradable_missed == null ? null : Number(missed.tradable_missed),
      would_stop_before_peak: missed.would_stop_before_peak == null ? null : Number(missed.would_stop_before_peak),
      clean_tradable: cleanTradable,
      trusted_peak_pnl: roundNullableNumber(trustedPeak, 4),
      trusted_peak_pnl_pct: roundNullableNumber(trustedPeak * 100, 2),
      tier,
      best_abs_quote_gap_pct: roundNullableNumber(gapStats.best_abs_quote_gap_pct, 3),
      quote_executable_n: gapStats.executable_n,
      clean10: gapStats.clean10,
      clean30: gapStats.clean30,
      audit_reason: String(auditRow.reason || payload.gate_reason || 'unknown'),
      entry_mode_candidate: payload.entry_mode_candidate || null,
      fast_lane_rescue_seen: Boolean(rescue),
      fast_lane_rescue_match_basis: fastLaneRescueMatchBasis,
      fast_lane_rescue_state: fastLaneRescueState,
      fast_lane_rescue_last_status: fastLaneRescueLastStatus,
      fast_lane_rescue_last_reason: fastLaneRescueLastReason,
      fast_lane_last_status: fastLaneLastStatus,
      fast_lane_last_reason: fastLaneLastReason,
      fast_lane_entry_branch: fastLaneEntryBranch,
      fast_lane_entry_mode_hint: fastLaneEntryModeHint,
      fast_lane_blocker: fastLaneBlocker,
      fast_lane_queue_status: fastLaneQueueStatus,
      fast_lane_queue_reason: fastLaneQueueReason,
      fast_lane_queue_updated_at: queue?.updated_at ?? null,
      fast_lane_queue_entry_branch: queue?.entry_branch ?? null,
      fast_lane_queue_source_type: queue?.source_type ?? null,
    });
    const scanCoverage = classifyMissedRescueScanCoverage(joinedRows[joinedRows.length - 1], nowTs);
    Object.assign(joinedRows[joinedRows.length - 1], {
      fast_lane_rescue_scan_eligible: scanCoverage.eligible,
      fast_lane_rescue_scan_window: scanCoverage.window,
      fast_lane_rescue_scan_primary_gap: scanCoverage.primary_gap_reason,
      fast_lane_rescue_scan_gap_reasons: scanCoverage.gap_reasons,
      fast_lane_rescue_scan_time_age_sec: scanCoverage.scan_time_age_sec,
      fast_lane_rescue_scan_timestamp: scanCoverage.scan_timestamp,
      fast_lane_rescue_scan_reason_allowed: scanCoverage.reason_allowed,
    });
  }

  const topJoinedWinners = joinedRows
    .slice()
    .sort((a, b) => (b.trusted_peak_pnl || 0) - (a.trusted_peak_pnl || 0)
      || (a.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER) - (b.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER))
    .slice(0, topLimit);
  const bestByToken = new Map();
  for (const row of joinedRows) {
    if (!row.token_ca) continue;
    const current = bestByToken.get(row.token_ca);
    const rowCleanScore = row.clean_tradable ? 1 : 0;
    const currentCleanScore = current?.clean_tradable ? 1 : 0;
    if (!current
      || (row.trusted_peak_pnl || 0) > (current.trusted_peak_pnl || 0)
      || ((row.trusted_peak_pnl || 0) === (current.trusted_peak_pnl || 0) && rowCleanScore > currentCleanScore)
      || ((row.trusted_peak_pnl || 0) === (current.trusted_peak_pnl || 0)
        && rowCleanScore === currentCleanScore
        && (row.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER) < (current.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER))) {
      bestByToken.set(row.token_ca, row);
    }
  }
  const topUniqueJoinedWinners = Array.from(bestByToken.values())
    .sort((a, b) => (b.trusted_peak_pnl || 0) - (a.trusted_peak_pnl || 0)
      || (a.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER) - (b.best_abs_quote_gap_pct ?? Number.MAX_SAFE_INTEGER))
    .slice(0, topLimit);
  for (const row of joinedRows) {
    const recoveryState = row.fast_lane_rescue_seen
      ? (row.fast_lane_rescue_state || 'rescue_state_missing')
      : 'unprocessed';
    const fastLaneStatus = row.fast_lane_rescue_seen
      ? (row.fast_lane_rescue_last_status || 'missing_status')
      : (row.fast_lane_last_status || row.fast_lane_queue_status || '-');
    const fastLaneReason = row.fast_lane_rescue_seen
      ? (row.fast_lane_rescue_last_reason || 'missing_reason')
      : (row.fast_lane_last_reason || row.fast_lane_queue_reason || '-');
    const entryBranch = row.fast_lane_entry_branch || '-';
    const entryModeHint = row.fast_lane_entry_mode_hint || '-';
    const recoveryKey = [recoveryState, fastLaneStatus, fastLaneReason, entryBranch, entryModeHint].join('|');
    if (!byRecoveryState.has(recoveryKey)) {
      byRecoveryState.set(recoveryKey, emptyLottoWinnerGapGroup({
        rescue_state: recoveryState,
        fast_lane_status: fastLaneStatus,
        fast_lane_reason: fastLaneReason,
        entry_branch: entryBranch === '-' ? null : entryBranch,
        entry_mode_hint: entryModeHint === '-' ? null : entryModeHint,
      }));
    }
    updateLottoWinnerGapGroupFromJoinedRow(byRecoveryState.get(recoveryKey), row);
  }

  return {
    audit_schema_version: 'v2.7.0.lotto_quote_gap_winner_join.v1',
    join_params: {
      max_join_delta_sec: maxJoinDeltaSec,
      recent_limit: recentLimit,
      top_limit: topLimit,
    },
    summary: {
      audit_events: auditRows.length,
      audit_unique_tokens: auditTokenSet.size,
      missed_rows: missedRows.length,
      missed_unique_tokens: missedByToken.size,
      joined_events: joinedEvents,
      joined_unique_tokens: joinedTokenSet.size,
      join_coverage_pct: auditRows.length ? roundNumber((joinedEvents / auditRows.length) * 100, 1) : null,
      clean_tradable_joined_events: cleanTradableEvents,
      clean_tradable_joined_unique: cleanTradableTokenSet.size,
      joined_medal_events: joinedMedalEvents,
      joined_medal_unique: joinedMedalTokenSet.size,
      clean_medal_joined_events: cleanMedalEvents,
      gold_events: Array.from(joinedRows).filter((row) => row.tier === 'gold').length,
      gold_unique: tierUniqueSets.gold.size,
      silver_events: Array.from(joinedRows).filter((row) => row.tier === 'silver').length,
      silver_unique: tierUniqueSets.silver.size,
      bronze_events: Array.from(joinedRows).filter((row) => row.tier === 'bronze').length,
      bronze_unique: tierUniqueSets.bronze.size,
      joined_executable_events: executableEvents,
      joined_executable_rate_pct: joinedEvents ? roundNumber((executableEvents / joinedEvents) * 100, 1) : null,
      joined_clean10_events: clean10Events,
      joined_clean10_rate_pct: joinedEvents ? roundNumber((clean10Events / joinedEvents) * 100, 1) : null,
      joined_clean30_events: clean30Events,
      joined_clean30_rate_pct: joinedEvents ? roundNumber((clean30Events / joinedEvents) * 100, 1) : null,
      best_gap_n: joinedGapValues.length,
      median_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(joinedGapValues, 50), 3),
      p90_best_abs_quote_gap_pct: roundNullableNumber(percentileNumber(joinedGapValues, 90), 3),
      max_best_abs_quote_gap_pct: joinedGapValues.length ? roundNumber(Math.max(...joinedGapValues), 3) : null,
      sample_warning: joinedEvents < 30 ? 'small_sample_quote_gap_winner_join' : null,
    },
    by_tier: Array.from(byTier.values())
      .map(finalizeLottoWinnerGapGroup)
      .sort((a, b) => ['gold', 'silver', 'bronze', 'sub25_or_unknown'].indexOf(a.tier)
        - ['gold', 'silver', 'bronze', 'sub25_or_unknown'].indexOf(b.tier)),
    by_blocker: Array.from(byBlocker.values())
      .map(finalizeLottoWinnerGapGroup)
      .sort((a, b) => b.clean_medal_unique - a.clean_medal_unique
        || b.clean_medal_events - a.clean_medal_events
        || b.medal_unique - a.medal_unique
        || b.medal_events - a.medal_events
        || b.clean10_events - a.clean10_events
        || b.events - a.events),
    by_recovery_state: Array.from(byRecoveryState.values())
      .map(finalizeLottoWinnerGapGroup)
      .sort((a, b) => b.clean_medal_unique - a.clean_medal_unique
        || b.medal_unique - a.medal_unique
        || b.clean10_events - a.clean10_events
        || b.events - a.events
        || String(a.rescue_state || '').localeCompare(String(b.rescue_state || ''))),
    missed_rescue_scanner_coverage: buildMissedRescueScannerCoverage(joinedRows),
    top_joined_winners: topJoinedWinners,
    top_unique_joined_winners: topUniqueJoinedWinners,
    unjoined_recent_audits: unjoinedRecentAudits,
    note: 'Read-only audit join: joins LOTTO quote-gap measurement events to missed attribution by token and nearby signal/event time; it does not change entry decisions.',
  };
}

function priceUnitAuditForTrade(row) {
  const entryAudit = parseJsonObject(row.entry_execution_audit_json);
  const exitAudit = parseJsonObject(row.exit_execution_audit_json);
  const monitorState = parseJsonObject(row.monitor_state_json);
  const warnings = [];

  const entryUnit = firstValue(entryAudit.effectivePriceUnit, entryAudit.entryPriceUnit, monitorState.entryPriceUnit);
  const triggerUnit = firstValue(monitorState.entryTriggerPriceUnit, entryAudit.entryTriggerPriceUnit);
  const quoteUnit = firstValue(monitorState.entryQuotePriceUnit, entryAudit.entryQuotePriceUnit);
  const exitUnit = firstValue(exitAudit.effectivePriceUnit, exitAudit.effectiveExitPriceUnit, monitorState.exitPriceUnit);
  const pnlUnit = firstValue(entryAudit.pnlUnit, exitAudit.pnlUnit, monitorState.pnlUnit);
  const accountingUnit = firstValue(entryAudit.accountingUnit, exitAudit.accountingUnit, monitorState.accountingUnit);

  if (row.entry_price != null && entryUnit !== 'SOL_PER_TOKEN') warnings.push('entry_price_unit_missing_or_not_sol_per_token');
  if (row.trigger_price != null && triggerUnit && triggerUnit !== 'SOL_PER_TOKEN') warnings.push('trigger_price_unit_not_sol_per_token');
  if (row.trigger_price != null && !triggerUnit) warnings.push('trigger_price_unit_missing');
  if (row.exit_price != null && Number(row.synthetic_close || 0) !== 1 && exitUnit && exitUnit !== 'SOL_PER_TOKEN') warnings.push('exit_price_unit_not_sol_per_token');
  if (row.exit_price != null && Number(row.synthetic_close || 0) !== 1 && !exitUnit) warnings.push('exit_price_unit_missing');
  if (row.pnl_pct != null && pnlUnit && pnlUnit !== 'RATIO_DECIMAL') warnings.push('pnl_unit_not_ratio_decimal');
  if (row.pnl_pct != null && !pnlUnit) warnings.push('pnl_unit_missing');
  if (accountingUnit && accountingUnit !== 'SOL') warnings.push('accounting_unit_not_sol');
  if (entryAudit.entryPriceUsd != null) warnings.push('legacy_entryPriceUsd_key_present');

  const entryEffective = Number(entryAudit.effectivePrice);
  if (Number.isFinite(entryEffective) && Number.isFinite(Number(row.entry_price))) {
    const gap = Math.abs(entryEffective - Number(row.entry_price));
    const denom = Math.max(Math.abs(entryEffective), Math.abs(Number(row.entry_price)), 1e-18);
    if (gap / denom > 0.000001) warnings.push('entry_price_differs_from_execution_effective_price');
  }

  return {
    trade_id: row.id,
    token_ca: row.token_ca,
    symbol: row.symbol,
    entry_unit: entryUnit || null,
    trigger_unit: triggerUnit || null,
    quote_unit: quoteUnit || null,
    exit_unit: exitUnit || null,
    pnl_unit: pnlUnit || null,
    accounting_unit: accountingUnit || null,
    accounting_source: firstValue(exitAudit.accountingSource, monitorState.accountingSource),
    synthetic_close: Number(row.synthetic_close || 0) === 1,
    warnings,
  };
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '../..');
const resolvedDbPath = isAbsolute(dbPath) ? dbPath : join(projectRoot, dbPath);

// 内存日志缓冲（保留最近 10000 条）
const MAX_LOG_LINES = 10000;
const logBuffer = [];
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;

// 日志文件路径
const logsDir = process.env.DASHBOARD_RUNTIME_LOG_DIR || join(projectRoot, 'logs');
const runtimeLogPath = join(logsDir, 'runtime.log');
const dashboardRequestMetricsPath = process.env.DASHBOARD_REQUEST_METRICS_PATH || join(logsDir, 'dashboard-request-metrics.json');
const dashboardRuntimeEventsPath = process.env.DASHBOARD_RUNTIME_EVENTS_PATH || join(logsDir, 'dashboard-runtime-events.jsonl');
const DASHBOARD_AUDIT_SCHEMA_VERSION = 'v2.7.0.audit_log_integrity.v1';
const DASHBOARD_AUDIT_GENESIS_HASH = 'GENESIS';
export const LOG_REDACTION_PATTERN_SET = 'v2.7.0.secret_pattern_set.dashboard_runtime.v1';

// 确保日志目录存在
try {
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
} catch (e) { /* ignore */ }

const DASHBOARD_REQUEST_METRICS_LIMIT = Math.max(
  20,
  Math.min(parseInt(process.env.DASHBOARD_REQUEST_METRICS_LIMIT || '120', 10) || 120, 500)
);
const DASHBOARD_SLOW_REQUEST_MS = Math.max(
  100,
  parseInt(process.env.DASHBOARD_SLOW_REQUEST_MS || '1000', 10) || 1000
);
const dashboardRequestMetrics = {
  schema_version: 'dashboard_request_metrics.v1',
  started_at: new Date().toISOString(),
  recent: [],
  slow: [],
  active: new Map(),
};

function memoryUsageMb() {
  const usage = process.memoryUsage();
  return {
    rss_mb: Math.round((usage.rss / 1024 / 1024) * 100) / 100,
    heap_used_mb: Math.round((usage.heapUsed / 1024 / 1024) * 100) / 100,
    heap_total_mb: Math.round((usage.heapTotal / 1024 / 1024) * 100) / 100,
    external_mb: Math.round((usage.external / 1024 / 1024) * 100) / 100,
  };
}

function sanitizeRequestParams(url) {
  const output = {};
  const secretKeys = new Set(['token', 'auth', 'authorization', 'key', 'api_key', 'dashboard_token']);
  for (const [key, value] of url.searchParams.entries()) {
    const normalized = String(key || '').toLowerCase();
    if (secretKeys.has(normalized) || normalized.includes('token') || normalized.includes('secret') || normalized.includes('key')) {
      output[key] = '[redacted]';
      continue;
    }
    if (String(value).length <= 80) output[key] = value;
  }
  return output;
}

function requestMetricsSnapshot() {
  return {
    schema_version: dashboardRequestMetrics.schema_version,
    generated_at: new Date().toISOString(),
    pid: process.pid,
    uptime_seconds: Math.floor(process.uptime()),
    memory: memoryUsageMb(),
    active: Array.from(dashboardRequestMetrics.active.values()),
    recent: dashboardRequestMetrics.recent.slice(-DASHBOARD_REQUEST_METRICS_LIMIT),
    slow: dashboardRequestMetrics.slow.slice(-DASHBOARD_REQUEST_METRICS_LIMIT),
    config: {
      slow_request_ms: DASHBOARD_SLOW_REQUEST_MS,
      limit: DASHBOARD_REQUEST_METRICS_LIMIT,
    },
  };
}

function readDashboardRuntimeEvents(limit = 80) {
  const boundedLimit = Math.max(1, Math.min(Number(limit) || 80, 500));
  try {
    if (!fs.existsSync(dashboardRuntimeEventsPath)) return [];
    const text = fs.readFileSync(dashboardRuntimeEventsPath, 'utf8');
    const lines = text.split(/\r?\n/).filter(Boolean).slice(-boundedLimit);
    return lines.map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return { raw: line, parse_error: true };
      }
    });
  } catch (error) {
    return [{ error: error?.message || String(error), read_failed: true }];
  }
}

function writeDashboardRequestMetricsSnapshot() {
  try {
    fs.writeFileSync(dashboardRequestMetricsPath, JSON.stringify(requestMetricsSnapshot(), null, 2));
  } catch {}
}

function appendDashboardRuntimeEvent(event) {
  try {
    fs.appendFileSync(dashboardRuntimeEventsPath, `${JSON.stringify({
      schema_version: 'dashboard_runtime_event.v1',
      ts: new Date().toISOString(),
      pid: process.pid,
      uptime_seconds: Math.floor(process.uptime()),
      memory: memoryUsageMb(),
      active_requests: Array.from(dashboardRequestMetrics.active.values()),
      ...event,
    })}\n`);
  } catch {}
}

function beginDashboardRequestMetric(req, url) {
  const metric = {
    id: randomUUID(),
    method: req.method || 'GET',
    path: url.pathname,
    params: sanitizeRequestParams(url),
    started_at: new Date().toISOString(),
    started_ms: Date.now(),
    memory_start: memoryUsageMb(),
  };
  dashboardRequestMetrics.active.set(metric.id, {
    id: metric.id,
    method: metric.method,
    path: metric.path,
    params: metric.params,
    started_at: metric.started_at,
    age_ms: 0,
    memory_start: metric.memory_start,
  });
  if (url.pathname.startsWith('/api/') && !url.pathname.startsWith('/api/logs')) {
    writeDashboardRequestMetricsSnapshot();
  }
  return metric;
}

function finishDashboardRequestMetric(metric, res, event = 'finish') {
  if (!metric || metric.finished) return;
  metric.finished = true;
  const endedMs = Date.now();
  const durationMs = endedMs - metric.started_ms;
  const memoryEnd = memoryUsageMb();
  const row = {
    id: metric.id,
    method: metric.method,
    path: metric.path,
    params: metric.params,
    status_code: res.statusCode,
    event,
    started_at: metric.started_at,
    ended_at: new Date(endedMs).toISOString(),
    duration_ms: durationMs,
    memory_start: metric.memory_start,
    memory_end: memoryEnd,
    rss_delta_mb: Math.round((memoryEnd.rss_mb - metric.memory_start.rss_mb) * 100) / 100,
  };
  dashboardRequestMetrics.active.delete(metric.id);
  dashboardRequestMetrics.recent.push(row);
  if (dashboardRequestMetrics.recent.length > DASHBOARD_REQUEST_METRICS_LIMIT) dashboardRequestMetrics.recent.shift();
  if (durationMs >= DASHBOARD_SLOW_REQUEST_MS || row.status_code >= 500 || event !== 'finish') {
    dashboardRequestMetrics.slow.push(row);
    if (dashboardRequestMetrics.slow.length > DASHBOARD_REQUEST_METRICS_LIMIT) dashboardRequestMetrics.slow.shift();
    appendDashboardRuntimeEvent({ event_type: 'dashboard_request_observation', request: row });
  }
  writeDashboardRequestMetricsSnapshot();
}

let dashboardShutdownStarted = false;
function handleDashboardShutdownSignal(signalName) {
  appendDashboardRuntimeEvent({ event_type: 'dashboard_process_signal', signal: signalName });
  writeDashboardRequestMetricsSnapshot();
  if (dashboardShutdownStarted) return;
  dashboardShutdownStarted = true;
  const forceExitMs = Math.max(1000, parseInt(process.env.DASHBOARD_SHUTDOWN_FORCE_EXIT_MS || '8000', 10) || 8000);
  const timer = setTimeout(() => {
    appendDashboardRuntimeEvent({ event_type: 'dashboard_process_force_exit', signal: signalName, after_ms: forceExitMs });
    process.exit(0);
  }, forceExitMs);
  try { timer.unref?.(); } catch {}
  try {
    server.close(() => {
      appendDashboardRuntimeEvent({ event_type: 'dashboard_process_graceful_exit', signal: signalName });
      writeDashboardRequestMetricsSnapshot();
      process.exit(0);
    });
  } catch {
    process.exit(0);
  }
}

for (const signalName of ['SIGTERM', 'SIGINT']) {
  process.on(signalName, () => handleDashboardShutdownSignal(signalName));
}

process.on('uncaughtExceptionMonitor', (error, origin) => {
  appendDashboardRuntimeEvent({
    event_type: 'dashboard_uncaught_exception_monitor',
    origin: origin || null,
    error: error?.message || String(error),
    stack: error?.stack || null,
  });
  writeDashboardRequestMetricsSnapshot();
});

process.on('unhandledRejection', (reason) => {
  appendDashboardRuntimeEvent({
    event_type: 'dashboard_unhandled_rejection',
    error: reason?.message || String(reason),
    stack: reason?.stack || null,
  });
  writeDashboardRequestMetricsSnapshot();
});

export function redactLogMessage(input) {
  let text = String(input ?? '');
  const secretNames = [
    'dashboard_token',
    'x-dashboard-token',
    'telegram_session',
    'telegram_bot_token',
    'telegram_api_hash',
    'gmgn_api_key',
    'api_key',
    'private_key',
    'wallet_private_key',
    'trade_wallet_private_key',
    'live_private_key',
    'solana_private_key',
    'bsc_private_key',
    'secret',
  ];
  const names = secretNames.join('|');
  text = text.replace(
    new RegExp(`(["']?(?:${names})["']?\\s*:\\s*["'])([^"']+)(["'])`, 'gi'),
    '$1[REDACTED]$3',
  );
  text = text.replace(
    /(authorization\s*[:=]\s*bearer\s+)([^\s"',;]+)/gi,
    '$1[REDACTED]',
  );
  text = text.replace(
    /([?&]token=)([^\s"',;&]+)/gi,
    '$1[REDACTED]',
  );
  text = text.replace(
    new RegExp(`((?:${names})\\s*[=:]\\s*)([^\\s"',;]+)`, 'gi'),
    '$1[REDACTED]',
  );
  return text;
}

export function canonicalAuditJson(value) {
  if (value === undefined) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalAuditJson(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalAuditJson(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

export function auditSha256Hex(value) {
  const text = typeof value === 'string' ? value : canonicalAuditJson(value);
  return createHash('sha256').update(text).digest('hex');
}

function auditPayloadForHash(event) {
  return {
    schema_version: event.schema_version,
    audit_event_id: event.audit_event_id,
    created_at: event.created_at,
    actor_id: event.actor_id,
    endpoint: event.endpoint,
    method: event.method,
    required_role: event.required_role,
    token_scope: event.token_scope,
    danger_level: event.danger_level,
    action: event.action,
    outcome: event.outcome,
    payload: event.payload || {},
  };
}

export function buildDashboardAuditEvent(input = {}) {
  const event = {
    schema_version: DASHBOARD_AUDIT_SCHEMA_VERSION,
    audit_event_id: input.audit_event_id || randomUUID(),
    created_at: input.created_at || new Date().toISOString(),
    actor_id: input.actor_id || 'dashboard_token',
    endpoint: input.endpoint,
    method: input.method || 'POST',
    required_role: input.required_role || 'dashboard_operator',
    token_scope: input.token_scope,
    danger_level: input.danger_level,
    action: input.action,
    outcome: input.outcome || 'attempt',
    payload: input.payload || {},
    prev_audit_hash: input.prev_audit_hash || DASHBOARD_AUDIT_GENESIS_HASH,
  };
  event.audit_payload_hash = auditSha256Hex(auditPayloadForHash(event));
  event.audit_chain_hash = auditSha256Hex({
    audit_event_id: event.audit_event_id,
    created_at: event.created_at,
    prev_audit_hash: event.prev_audit_hash,
    audit_payload_hash: event.audit_payload_hash,
  });
  return event;
}

export function verifyDashboardAuditChain(events = []) {
  let expectedPrev = DASHBOARD_AUDIT_GENESIS_HASH;
  const failures = [];
  for (const [index, event] of events.entries()) {
    const expectedPayloadHash = auditSha256Hex(auditPayloadForHash(event));
    const expectedChainHash = auditSha256Hex({
      audit_event_id: event.audit_event_id,
      created_at: event.created_at,
      prev_audit_hash: event.prev_audit_hash,
      audit_payload_hash: expectedPayloadHash,
    });
    if (event.prev_audit_hash !== expectedPrev) {
      failures.push({ index, reason: 'prev_audit_hash_mismatch' });
    }
    if (event.audit_payload_hash !== expectedPayloadHash) {
      failures.push({ index, reason: 'audit_payload_hash_mismatch' });
    }
    if (event.audit_chain_hash !== expectedChainHash) {
      failures.push({ index, reason: 'audit_chain_hash_mismatch' });
    }
    expectedPrev = event.audit_chain_hash;
  }
  return {
    ok: failures.length === 0,
    event_count: events.length,
    failures,
    last_audit_chain_hash: expectedPrev,
  };
}

function dashboardAuditLogPath(options = {}) {
  const raw = options.auditLogPath || process.env.V27_DASHBOARD_AUDIT_LOG_PATH || join(projectRoot, 'data', 'v27_dashboard_audit.jsonl');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function lastDashboardAuditHash(auditLogPath) {
  const text = readTinyText(auditLogPath, 1024 * 1024);
  if (!text) return DASHBOARD_AUDIT_GENESIS_HASH;
  const lines = text.split('\n').map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return DASHBOARD_AUDIT_GENESIS_HASH;
  const last = JSON.parse(lines[lines.length - 1]);
  if (!last.audit_chain_hash) {
    throw new Error('last audit event missing audit_chain_hash');
  }
  return last.audit_chain_hash;
}

export function appendDashboardAuditEvent(input = {}, options = {}) {
  const auditLogPath = dashboardAuditLogPath(options);
  fs.mkdirSync(dirname(auditLogPath), { recursive: true });
  const event = buildDashboardAuditEvent({
    ...input,
    prev_audit_hash: input.prev_audit_hash || lastDashboardAuditHash(auditLogPath),
  });
  fs.appendFileSync(auditLogPath, `${JSON.stringify(event)}\n`, { encoding: 'utf8', mode: 0o600 });
  return event;
}

function requireDashboardAuditEvent(req, res, url, input = {}) {
  try {
    return appendDashboardAuditEvent({
      endpoint: url.pathname,
      method: req.method,
      ...input,
    });
  } catch (error) {
    res.writeHead(500, apiJsonHeaders());
    res.end(JSON.stringify({ error: 'Audit log unavailable', error_code: 'audit_log_unavailable', detail: error.message }));
    return null;
  }
}

function formatLogArg(arg) {
  if (arg instanceof Error) {
    const detail = {
      name: arg.name,
      message: arg.message,
      stack: arg.stack,
    };
    if (arg.code) detail.code = arg.code;
    if (arg.cause) detail.cause = arg.cause instanceof Error
      ? { name: arg.cause.name, message: arg.cause.message, stack: arg.cause.stack }
      : arg.cause;
    return JSON.stringify(detail);
  }
  if (typeof arg === 'object' && arg !== null) {
    try {
      return JSON.stringify(arg);
    } catch (e) {
      return String(arg);
    }
  }
  return String(arg);
}

function captureLog(level, args) {
  const timestamp = new Date().toISOString();
  const message = redactLogMessage(args.map(formatLogArg).join(' '));
  const logLine = { timestamp, level, message };

  // 内存缓冲
  logBuffer.push(logLine);
  if (logBuffer.length > MAX_LOG_LINES) {
    logBuffer.shift();
  }

  // 写入文件（追加模式）
  try {
    fs.appendFileSync(runtimeLogPath, `[${timestamp}] [${level}] ${message}\n`);
  } catch (e) { /* ignore */ }
}

function writeRedactedLogStream(logStream, chunk) {
  logStream.write(redactLogMessage(chunk));
}

console.log = (...args) => {
  captureLog('INFO', args);
  originalConsoleLog.apply(console, args);
};

console.error = (...args) => {
  captureLog('ERROR', args);
  originalConsoleError.apply(console, args);
};

console.warn = (...args) => {
  captureLog('WARN', args);
  originalConsoleWarn.apply(console, args);
};

let db;
function openDashboardSqlite(dbPath, options) {
  const database = options === undefined ? new Database(dbPath) : new Database(dbPath, options);
  try {
    database.pragma('mmap_size = 0');
  } catch {
    // Best-effort SIGBUS mitigation. Some readonly or older SQLite builds can
    // reject the pragma; the caller should still be able to use the connection.
  }
  return database;
}

function getDb() {
  if (!db) {
    try {
      db = openDashboardSqlite(resolvedDbPath);
    } catch (e) {
      console.error('❌ Failed to open database:', e.message);
    }
  }
  return db;
}

function getPaperDbPath() {
  const paperDbPath = process.env.PAPER_DB || './data/paper_trades.db';
  return isAbsolute(paperDbPath) ? paperDbPath : join(projectRoot, paperDbPath);
}

function getRawSignalOutcomesDbPath() {
  const rawDbPath = process.env.RAW_SIGNAL_OUTCOMES_DB || './data/raw_signal_outcomes.db';
  return isAbsolute(rawDbPath) ? rawDbPath : join(projectRoot, rawDbPath);
}

function getAgentRunsRoot() {
  const raw = process.env.AGENT_RUNS_DIR || join(dirname(getPaperDbPath()), 'agent_runs');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function getAgentHandoffsDir() {
  const raw = process.env.AGENT_HANDOFFS_DIR || join(dirname(getPaperDbPath()), 'agent_handoffs');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function getHypothesisRegistryPath() {
  const raw = process.env.HYPOTHESIS_REGISTRY_PATH || join(dirname(getPaperDbPath()), 'hypothesis_registry.json');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function agentCaptureArtifactPaths() {
  const latestDir = join(getAgentRunsRoot(), 'latest');
  const agentLog = process.env.AGENT_CAPTURE_DISCOVERY_LOG || join(dirname(getPaperDbPath()), 'agent-capture-discovery.log');
  const runnerStatus = process.env.AGENT_CAPTURE_DISCOVERY_RUNNER_STATUS
    || join(dirname(getPaperDbPath()), 'agent-capture-discovery-runner-status.json');
  return {
    verdict: join(latestDir, 'reviewer_verdict.json'),
    summary: join(latestDir, 'run_summary.md'),
    handoff: join(getAgentHandoffsDir(), 'latest_codex_handoff.md'),
    registry: getHypothesisRegistryPath(),
    capture: join(latestDir, 'candidate_capture_discovery_24h.json'),
    capture_24h: join(latestDir, 'capture_discovery_24h.json'),
    capture_48h: join(latestDir, 'capture_discovery_48h.json'),
    capture_72h: join(latestDir, 'capture_discovery_72h.json'),
    raw_funnel: join(latestDir, 'raw_gold_silver_funnel_audit_24h.json'),
    shadow_decision_bridge: join(latestDir, 'shadow_decision_bridge_audit_24h.json'),
    downstream_readiness: join(latestDir, 'candidate_downstream_readiness_24h.json'),
    context_coverage: join(latestDir, 'context_coverage_audit_24h.json'),
    context_blocker_monitor: join(latestDir, 'context_blocker_monitor_24h.json'),
    a_class_fastlane: join(latestDir, 'a_class_fastlane_mode_audit_24h.json'),
    candidate_effectiveness: join(latestDir, 'candidate_effectiveness_24h.json'),
    candidate_improvement: join(latestDir, 'candidate_improvement_opportunities_24h.json'),
    capture_cross_validity: join(latestDir, 'capture_cross_validity_24h.json'),
    pnl: join(latestDir, 'pnl_cross_secondary_24h.json'),
    markov_runtime: join(latestDir, 'candidate_virtual_markov_runtime_24h.json'),
    markov_kline: join(latestDir, 'candidate_virtual_markov_kline_24h.json'),
    markov_candidate_lifecycle: join(latestDir, 'candidate_virtual_markov_candidate_lifecycle_24h.json'),
    markov_candidate_source: join(latestDir, 'candidate_virtual_markov_candidate_source_24h.json'),
    markov_candidate_signal_type: join(latestDir, 'candidate_virtual_markov_candidate_signal_type_24h.json'),
    markov_candidate_lifecycle_source: join(latestDir, 'candidate_virtual_markov_candidate_lifecycle_source_24h.json'),
    markov_effectiveness: join(latestDir, 'markov_effectiveness_24h.json'),
    volume_kline_coverage: join(latestDir, 'volume_kline_coverage_audit_24h.json'),
    matured_kline_recheck: join(latestDir, 'matured_kline_volume_recheck_audit_24h.json'),
    matured_volume_cross: join(latestDir, 'matured_volume_capture_cross_audit_24h.json'),
    hypothesis_validation: join(latestDir, 'hypothesis_validation_audit_24h.json'),
    oos_readiness_refresh: join(latestDir, 'oos_readiness_probe_refresh.json'),
    low_confidence_research: join(latestDir, 'low_confidence_research_capture_audit_24h.json'),
    quality_timing_research: join(latestDir, 'quality_timing_reject_research_audit_24h.json'),
    quality_timing_probe_validation: join(latestDir, 'quality_timing_candidate_probe_validation_24h.json'),
    runtime_health: join(latestDir, 'runtime_health_snapshot_24h.json'),
    runner_status: runnerStatus,
    tests: join(latestDir, 'tests.json'),
    log: agentLog,
  };
}

function agentArtifactContentType(filePath) {
  if (String(filePath || '').endsWith('.json')) return 'application/json; charset=utf-8';
  if (String(filePath || '').endsWith('.md')) return 'text/markdown; charset=utf-8';
  return 'text/plain; charset=utf-8';
}

function safeReadAgentJson(filePath) {
  try {
    if (!fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (error) {
    return { error_code: 'agent_artifact_json_parse_failed', error: error.message };
  }
}

function agentArtifactStat(filePath) {
  try {
    const stat = fs.statSync(filePath);
    return {
      available: stat.isFile(),
      path: filePath,
      size_bytes: stat.size,
      mtime: stat.mtime.toISOString(),
    };
  } catch (error) {
    return {
      available: false,
      path: filePath,
      error_code: error?.code === 'ENOENT' ? 'agent_artifact_not_found' : 'agent_artifact_stat_failed',
      error: error?.message,
    };
  }
}

function safeWriteAgentJson(filePath, payload) {
  fs.mkdirSync(dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tmp, filePath);
}

function processIsAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

let agentCaptureLoopRunner = {
  running: false,
  pid: null,
  started_at: null,
  run_id: null,
};

function readAgentCaptureLoopRunnerStatus() {
  const paths = agentCaptureArtifactPaths();
  const status = safeReadAgentJson(paths.runner_status);
  if (!status || status.error_code) {
    return {
      schema_version: 'agent_capture_loop_runner_status.v1',
      available: false,
      running: Boolean(agentCaptureLoopRunner.running && processIsAlive(agentCaptureLoopRunner.pid)),
      pid: agentCaptureLoopRunner.pid || null,
    };
  }
  const startedMs = Date.parse(status.started_at || '');
  const ageMs = Number.isFinite(startedMs) ? Date.now() - startedMs : null;
  const staleRunning = Boolean(status.running && ageMs != null && ageMs > 3 * 60 * 60 * 1000);
  const running = Boolean(status.running && !staleRunning && processIsAlive(status.pid));
  return {
    ...status,
    available: true,
    running,
    stale_running_status: staleRunning,
    age_minutes: ageMs == null ? null : +(ageMs / 60000).toFixed(2),
    pid_alive: processIsAlive(status.pid),
  };
}

function sanitizeCaptureHours(value, primaryHours) {
  const fallback = String(primaryHours || 24);
  const raw = String(value || fallback).split(',');
  const out = [];
  for (const item of raw) {
    const parsed = parseInt(item.trim(), 10);
    if (Number.isFinite(parsed) && parsed >= 1 && parsed <= 168 && !out.includes(parsed)) {
      out.push(parsed);
    }
  }
  return out.length ? out.join(',') : fallback;
}

function triggerAgentCaptureDiscoveryLoop(url) {
  const paths = agentCaptureArtifactPaths();
  const current = readAgentCaptureLoopRunnerStatus();
  if (current.running) {
    return {
      accepted: false,
      status: 'already_running',
      runner: current,
      promotion_allowed: false,
      strategy_change_allowed: false,
      automatic_runtime_change_allowed: false,
      paper_enablement_allowed: false,
    };
  }

  const hours = boundedIntParam(url, 'hours', 24, 1, 168);
  const captureHours = sanitizeCaptureHours(url.searchParams.get('capture_hours') || url.searchParams.get('capture-hours'), hours);
  const expectedCandidates = boundedIntParam(url, 'expected_candidates', 84, 1, 200);
  const reportTimeoutSec = boundedIntParam(url, 'report_timeout_sec', 600, 30, 3600);
  const testTimeoutSec = boundedIntParam(url, 'test_timeout_sec', 120, 30, 600);
  const maxScanRows = boundedIntParam(url, 'max_scan_rows', 2000000, 1000, 5000000);
  const quoteFixDeployTs = parseUnixishTime(url.searchParams.get('quote_fix_deploy_ts')) || 0;
  const oosProbeHours = String(url.searchParams.get('oos_probe_hours') || '0.25,0.5,1')
    .replace(/[^0-9.,]/g, '')
    .slice(0, 80) || '0.25,0.5,1';
  const timeoutMs = boundedIntParam(url, 'timeout_sec', 1800, 60, 7200) * 1000;
  const runId = `api_${new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z')}_${randomUUID().slice(0, 8)}`;
  const startedAt = new Date().toISOString();
  const args = [
    'scripts/agent_capture_discovery_loop.py',
    '--paper-db', getPaperDbPath(),
    '--raw-db', getRawSignalOutcomesDbPath(),
    '--kline-db', getKlineCacheDbPath(),
    '--data-dir', dirname(getPaperDbPath()),
    '--hours', String(hours),
    '--capture-hours', captureHours,
    '--expected-candidates', String(expectedCandidates),
    '--report-timeout-sec', String(reportTimeoutSec),
    '--test-timeout-sec', String(testTimeoutSec),
    '--max-scan-rows', String(maxScanRows),
    '--oos-probe-hours', oosProbeHours,
    '--max-runs', '1',
  ];
  if (quoteFixDeployTs > 0) {
    args.push('--quote-fix-deploy-ts', String(quoteFixDeployTs));
  }

  fs.mkdirSync(dirname(paths.log), { recursive: true });
  fs.mkdirSync(dirname(paths.runner_status), { recursive: true });
  const logStream = fs.createWriteStream(paths.log, { flags: 'a' });
  writeRedactedLogStream(logStream, `[agent-capture-loop] ${startedAt} start run_id=${runId} python3 ${args.join(' ')}\n`);

  const child = spawn('python3', args, {
    cwd: projectRoot,
    env: {
      ...process.env,
      PYTHONUNBUFFERED: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  agentCaptureLoopRunner = {
    running: true,
    pid: child.pid,
    started_at: startedAt,
    run_id: runId,
  };
  const baseStatus = {
    schema_version: 'agent_capture_loop_runner_status.v1',
    run_id: runId,
    running: true,
    pid: child.pid,
    started_at: startedAt,
    finished_at: null,
    command: ['python3', ...args],
    log_path: paths.log,
    latest_dir: join(getAgentRunsRoot(), 'latest'),
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
    notes: [
      'Read-only evaluator/report loop only.',
      'Does not modify strategy, gates, A_CLASS, executor, wallet, or risk settings.',
    ],
  };
  safeWriteAgentJson(paths.runner_status, baseStatus);

  child.stdout.on('data', (chunk) => writeRedactedLogStream(logStream, chunk));
  child.stderr.on('data', (chunk) => writeRedactedLogStream(logStream, chunk));

  let finished = false;
  const finish = (error, code, signal, timedOut = false) => {
    if (finished) return;
    finished = true;
    clearTimeout(timeoutHandle);
    const finishedAt = new Date().toISOString();
    const status = {
      ...baseStatus,
      running: false,
      finished_at: finishedAt,
      exit_code: code ?? null,
      signal: signal || null,
      timed_out: Boolean(timedOut),
      error: error ? error.message : null,
    };
    agentCaptureLoopRunner = {
      running: false,
      pid: child.pid,
      started_at: startedAt,
      finished_at: finishedAt,
      run_id: runId,
    };
    writeRedactedLogStream(logStream, `[agent-capture-loop] ${finishedAt} finish run_id=${runId} code=${code ?? ''} signal=${signal || ''} timed_out=${Boolean(timedOut)} error=${error?.message || ''}\n`);
    try { safeWriteAgentJson(paths.runner_status, status); } catch {}
    try { logStream.end(); } catch {}
  };
  const timeoutHandle = setTimeout(() => {
    try { child.kill('SIGTERM'); } catch {}
    finish(new Error(`timeout_after_${Math.floor(timeoutMs / 1000)}s`), null, 'SIGTERM', true);
  }, timeoutMs);
  child.on('error', (error) => finish(error, null, null, false));
  child.on('exit', (code, signal) => finish(code === 0 ? null : new Error(`exit_${code ?? signal}`), code, signal, false));

  return {
    accepted: true,
    status: 'started',
    runner: readAgentCaptureLoopRunnerStatus(),
    command: ['python3', ...args],
    log_path: paths.log,
    status_path: paths.runner_status,
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
  };
}

function buildAgentCaptureDiscoveryLatestSnapshot(options = {}) {
  const includeReports = Boolean(options.includeReports);
  const paths = agentCaptureArtifactPaths();
  const artifacts = {};
  for (const [name, filePath] of Object.entries(paths)) {
    artifacts[name] = agentArtifactStat(filePath);
  }
  const verdict = safeReadAgentJson(paths.verdict);
  const registry = safeReadAgentJson(paths.registry);
  const tests = safeReadAgentJson(paths.tests);
  const shadowDecisionBridge = safeReadAgentJson(paths.shadow_decision_bridge);
  const qualityTimingResearch = safeReadAgentJson(paths.quality_timing_research);
  const qualityTimingProbeValidation = safeReadAgentJson(paths.quality_timing_probe_validation);
  const runner = readAgentCaptureLoopRunnerStatus();
  const runtimeCommit = runtimeCommitFingerprint();
  const compactQualityTimingResearch = (report) => {
    if (!report) return null;
    return {
      available: !report.error_code,
      verdict: report.verdict || null,
      promotion_allowed: Boolean(report.promotion_allowed),
      strategy_change_allowed: Boolean(report.strategy_change_allowed),
      automatic_runtime_change_allowed: Boolean(report.automatic_runtime_change_allowed),
      paper_enablement_allowed: Boolean(report.paper_enablement_allowed),
      denominator: report.denominator || null,
      candidate_match_attribution: report.candidate_match_attribution ? {
        expected_candidates: report.candidate_match_attribution.expected_candidates,
        candidate_observation_rows: report.candidate_match_attribution.candidate_observation_rows,
        events_with_full_candidate_coverage:
          report.candidate_match_attribution.events_with_full_candidate_coverage,
        full_candidate_coverage_rate:
          report.candidate_match_attribution.full_candidate_coverage_rate,
        candidate_matched_any_events:
          report.candidate_match_attribution.candidate_matched_any_events,
        candidate_matched_any_rate:
          report.candidate_match_attribution.candidate_matched_any_rate,
        top_candidates: (report.candidate_match_attribution.top_candidates || []).slice(0, 10),
        top_families: (report.candidate_match_attribution.top_families || []).slice(0, 10),
      } : null,
      stage_attribution: report.stage_attribution ? {
        stage_counts: (report.stage_attribution.stage_counts || []).slice(0, 8),
        reason_counts: (report.stage_attribution.reason_counts || []).slice(0, 10),
      } : null,
      context_attribution: report.context_attribution ? {
        lifecycle_source_counts:
          (report.context_attribution.lifecycle_source_counts || []).slice(0, 10),
        markov_bucket_counts:
          (report.context_attribution.markov_bucket_counts || []).slice(0, 8),
        source_quote_clean_counts:
          (report.context_attribution.source_quote_clean_counts || []).slice(0, 8),
        source_quote_executable_counts:
          (report.context_attribution.source_quote_executable_counts || []).slice(0, 8),
      } : null,
      shadow_only_review: report.shadow_only_review ? {
        classification: report.shadow_only_review.classification || null,
        dominant_cluster: report.shadow_only_review.dominant_cluster || null,
        dominant_stage: report.shadow_only_review.dominant_stage || null,
        quality_timing_false_negative_upper_bound:
          report.shadow_only_review.quality_timing_false_negative_upper_bound || null,
        research_opportunity_count: report.shadow_only_review.research_opportunity_count,
        top_research_opportunities:
          (report.shadow_only_review.top_research_opportunities || []).slice(0, 8),
        promotion_allowed: Boolean(report.shadow_only_review.promotion_allowed),
        strategy_change_allowed: Boolean(report.shadow_only_review.strategy_change_allowed),
        automatic_runtime_change_allowed:
          Boolean(report.shadow_only_review.automatic_runtime_change_allowed),
        paper_enablement_allowed: Boolean(report.shadow_only_review.paper_enablement_allowed),
      } : null,
      shadow_only_next_actions: report.shadow_only_next_actions || [],
      blockers: report.blockers || [],
    };
  };
  const qualityTimingResearchSummary = compactQualityTimingResearch(
    verdict?.quality_timing_reject_research_audit || qualityTimingResearch,
  );
  const compactQualityTimingProbeValidation = (report) => {
    if (!report) return null;
    return {
      available: !report.error_code,
      classification: report.classification || null,
      next_action: report.next_action || null,
      promotion_allowed: Boolean(report.promotion_allowed),
      strategy_change_allowed: Boolean(report.strategy_change_allowed),
      automatic_runtime_change_allowed: Boolean(report.automatic_runtime_change_allowed),
      paper_enablement_allowed: Boolean(report.paper_enablement_allowed),
      denominator: report.denominator || {},
      status_counts: report.status_counts || {},
      top_repeated_probes: (report.top_repeated_probes || []).slice(0, 8),
    };
  };
  const qualityTimingProbeValidationSummary = compactQualityTimingProbeValidation(
    verdict?.quality_timing_candidate_probe_validation || qualityTimingProbeValidation,
  );
  const required = ['verdict', 'summary', 'handoff', 'registry'];
  const missingRequired = required.filter((name) => !artifacts[name]?.available);
  const payload = {
    schema_version: 'agent_capture_discovery_latest.v1',
    generated_at: new Date().toISOString(),
    phase: 'discovery_mesh',
    current_commit: runtimeCommit,
    deployment_commit: runtimeCommit,
    runtime_commit: runtimeCommit,
    artifact_current_commit: verdict?.current_commit || null,
    artifact_deployment_commit: verdict?.deployment_commit || null,
    latest_dir: join(getAgentRunsRoot(), 'latest'),
    handoff_dir: getAgentHandoffsDir(),
    required_artifacts_complete: missingRequired.length === 0,
    missing_required_artifacts: missingRequired,
    artifacts,
    verdict_summary: verdict ? {
      available: !verdict.error_code,
      classification: verdict.classification,
      blocked_subtype: verdict.blocked_subtype,
      next_action: verdict.next_action || null,
      parallel_next_action: verdict.parallel_next_action || null,
      parallel_next_action_reason: verdict.parallel_next_action_reason || null,
      top_blocker: verdict.top_blocker || verdict.next_highest_priority_blocker || null,
      actionable_blockers: verdict.actionable_blockers || [],
      context_clean_window_eta_iso: verdict.context_clean_window_eta_iso || null,
      current_commit: verdict.current_commit,
      deployment_commit: verdict.deployment_commit,
      promotion_allowed: Boolean(verdict.promotion_allowed),
      human_action_required: Boolean(verdict.human_action_required),
      non_quote_sensitive_capture_discovery_allowed: Boolean(verdict.non_quote_sensitive_capture_discovery_allowed),
      quote_sensitive_slices_blocked: Boolean(verdict.quote_sensitive_slices_blocked),
      blockers: verdict.blockers || [],
      candidate_count_expected: verdict.candidate_count_expected,
      candidate_count_observed: verdict.candidate_count_observed,
      observation_coverage_pct: verdict.observation_coverage_pct,
      raw_dog_rows_complete: Boolean(verdict.raw_dog_rows_complete),
      signal_id_join_rate: verdict.signal_id_join_rate,
      raw_all_signal_id_join_rate: verdict.raw_all_signal_id_join_rate,
      mesh_eligible_signal_id_join_rate: verdict.mesh_eligible_signal_id_join_rate,
      signal_identity_reconciliation: verdict.signal_identity_reconciliation ? {
        joined_exact_signal_id: verdict.signal_identity_reconciliation.joined_exact_signal_id,
        joined_by_signal_alias: verdict.signal_identity_reconciliation.joined_by_signal_alias,
        joined_by_lifecycle_id: verdict.signal_identity_reconciliation.joined_by_lifecycle_id,
        joined_by_token_time_high_confidence: verdict.signal_identity_reconciliation.joined_by_token_time_high_confidence,
        outside_candidate_observer_window: verdict.signal_identity_reconciliation.outside_candidate_observer_window,
        not_mesh_eligible: verdict.signal_identity_reconciliation.not_mesh_eligible,
        missing_candidate_observation: verdict.signal_identity_reconciliation.missing_candidate_observation,
        raw_event_duplicate: verdict.signal_identity_reconciliation.raw_event_duplicate,
        raw_event_derived_no_signal: verdict.signal_identity_reconciliation.raw_event_derived_no_signal,
        unknown_unjoined: verdict.signal_identity_reconciliation.unknown_unjoined,
      } : null,
      quote_context_coverage: verdict.quote_context_coverage ? {
        coverage_denominator_type: verdict.quote_context_coverage.coverage_denominator_type,
        coverage_denominator_rows: verdict.quote_context_coverage.coverage_denominator_rows,
        context_carrier_candidate_ids: verdict.quote_context_coverage.context_carrier_candidate_ids,
        source_quote_clean_present_rate: verdict.quote_context_coverage.source_quote_clean_present_rate,
        source_quote_executable_present_rate: verdict.quote_context_coverage.source_quote_executable_present_rate,
        source_quote_clean_true_rate: verdict.quote_context_coverage.source_quote_clean_true_rate,
        source_quote_clean_false_rate: verdict.quote_context_coverage.source_quote_clean_false_rate,
        source_quote_clean_missing_rate: verdict.quote_context_coverage.source_quote_clean_missing_rate,
        source_quote_clean_unknown_rate: verdict.quote_context_coverage.source_quote_clean_unknown_rate,
        source_quote_clean_not_applicable_rate: verdict.quote_context_coverage.source_quote_clean_not_applicable_rate,
        source_quote_executable_true_rate: verdict.quote_context_coverage.source_quote_executable_true_rate,
        source_quote_executable_false_rate: verdict.quote_context_coverage.source_quote_executable_false_rate,
        source_quote_executable_missing_rate: verdict.quote_context_coverage.source_quote_executable_missing_rate,
        source_quote_executable_unknown_rate: verdict.quote_context_coverage.source_quote_executable_unknown_rate,
        source_quote_executable_not_applicable_rate: verdict.quote_context_coverage.source_quote_executable_not_applicable_rate,
      } : null,
      quote_missing_root_cause: verdict.quote_missing_root_cause ? {
        quote_missing_rows_total: verdict.quote_missing_root_cause.quote_missing_rows_total,
        missing_by_context_schema_version: verdict.quote_missing_root_cause.missing_by_context_schema_version,
        missing_by_source_component: verdict.quote_missing_root_cause.missing_by_source_component,
        missing_by_signal_type: verdict.quote_missing_root_cause.missing_by_signal_type,
        missing_by_writer_path: verdict.quote_missing_root_cause.missing_by_writer_path,
        missing_by_lifecycle_profile: verdict.quote_missing_root_cause.missing_by_lifecycle_profile,
        missing_by_payload_key_presence: verdict.quote_missing_root_cause.missing_by_payload_key_presence,
        missing_due_to_legacy_schema_count: verdict.quote_missing_root_cause.missing_due_to_legacy_schema_count,
        missing_due_to_writer_path_count: verdict.quote_missing_root_cause.missing_due_to_writer_path_count,
        missing_should_be_not_applicable_count: verdict.quote_missing_root_cause.missing_should_be_not_applicable_count,
        missing_unknown_count: verdict.quote_missing_root_cause.missing_unknown_count,
        dominant_root_cause: verdict.quote_missing_root_cause.dominant_root_cause,
      } : null,
      quote_writer_fix_status: verdict.quote_writer_fix_status || null,
      quote_clean_window_status: verdict.quote_clean_window_status || null,
      quote_clean_window_eta_iso: verdict.quote_clean_window_eta_iso || null,
      quote_clean_window_seconds_remaining: verdict.quote_clean_window_seconds_remaining ?? null,
      context_field_writer_fix_status: verdict.context_field_writer_fix_status || null,
      context_clean_window_pending: Boolean(verdict.context_clean_window_pending),
      context_clean_window_eta_iso: verdict.context_clean_window_eta_iso || null,
      context_clean_window_seconds_remaining: verdict.context_clean_window_seconds_remaining ?? null,
      lifecycle_clean_window_pending: Boolean(verdict.lifecycle_clean_window_pending),
      source_component_clean_window_pending: Boolean(verdict.source_component_clean_window_pending),
      context_blocker_monitor_summary: verdict.context_blocker_monitor ? {
        available: Boolean(verdict.context_blocker_monitor.available),
        overall_verdict: verdict.context_blocker_monitor.overall_verdict || null,
        clean_window_monitor: verdict.context_blocker_monitor.clean_window_monitor ? {
          classification: verdict.context_blocker_monitor.clean_window_monitor.classification,
          pre_fix_rows_remaining: verdict.context_blocker_monitor.clean_window_monitor.pre_fix_rows_remaining,
          post_fix_rows: verdict.context_blocker_monitor.clean_window_monitor.post_fix_rows,
          rolling24_rows: verdict.context_blocker_monitor.clean_window_monitor.rolling24_rows,
          estimated_clean_at_iso: verdict.context_blocker_monitor.clean_window_monitor.estimated_clean_at_iso,
          seconds_until_natural_clean_window:
            verdict.context_blocker_monitor.clean_window_monitor.seconds_until_natural_clean_window,
          quote_coverage_rolling24:
            verdict.context_blocker_monitor.clean_window_monitor.quote_coverage_rolling24,
          quote_coverage_post_fix_rows_only:
            verdict.context_blocker_monitor.clean_window_monitor.quote_coverage_post_fix_rows_only,
        } : null,
        context_field_status:
          verdict.context_blocker_monitor.context_field_coverage_audit?.classification || null,
        context_field_blockers:
          verdict.context_blocker_monitor.context_field_coverage_audit?.blockers || [],
        context_field_progress: verdict.context_blocker_monitor.context_field_coverage_audit ? {
          lifecycle_profile: verdict.context_blocker_monitor.context_field_coverage_audit.lifecycle_profile ? {
            effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.lifecycle_profile.effective_present_rate,
            rows_needed_to_80pct:
              verdict.context_blocker_monitor.context_field_coverage_audit.lifecycle_profile.rows_needed_to_80pct,
            missing_rows:
              verdict.context_blocker_monitor.context_field_coverage_audit.lifecycle_profile.missing_rows,
            mature_effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.lifecycle_profile.mature_context?.effective_present_rate,
          } : null,
          source_component: verdict.context_blocker_monitor.context_field_coverage_audit.source_component ? {
            effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.source_component.effective_present_rate,
            rows_needed_to_80pct:
              verdict.context_blocker_monitor.context_field_coverage_audit.source_component.rows_needed_to_80pct,
            missing_rows:
              verdict.context_blocker_monitor.context_field_coverage_audit.source_component.missing_rows,
            mature_effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.source_component.mature_context?.effective_present_rate,
          } : null,
          volume_profile: verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile ? {
            effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile.effective_present_rate,
            rows_needed_to_80pct:
              verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile.rows_needed_to_80pct,
            missing_rows:
              verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile.missing_rows,
            unknown_rows:
              verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile.unknown_rows,
            mature_effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.volume_profile.mature_context?.effective_present_rate,
          } : null,
          markov_bucket: verdict.context_blocker_monitor.context_field_coverage_audit.markov_bucket ? {
            effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.markov_bucket.effective_present_rate,
            rows_needed_to_80pct:
              verdict.context_blocker_monitor.context_field_coverage_audit.markov_bucket.rows_needed_to_80pct,
            missing_rows:
              verdict.context_blocker_monitor.context_field_coverage_audit.markov_bucket.missing_rows,
            mature_effective_present_rate:
              verdict.context_blocker_monitor.context_field_coverage_audit.markov_bucket.mature_context?.effective_present_rate,
          } : null,
        } : null,
        reconciled_warnings: verdict.context_blocker_monitor.reconciled_warnings || [],
      } : null,
      volume_profile_coverage: verdict.volume_profile_coverage || null,
      kline_coverage: verdict.kline_coverage || null,
      runtime_health_status: verdict.runtime_health_status || null,
      runtime_health_blockers: verdict.runtime_health_blockers || [],
      runtime_health_warnings: verdict.runtime_health_warnings || [],
      A_CLASS_mode_status: verdict.A_CLASS_mode_status || null,
      current_capture_stage: verdict.current_capture_stage || null,
      detector_capture_rate: verdict.detector_capture_rate ?? null,
      decision_capture_rate: verdict.decision_capture_rate ?? null,
      pending_capture_rate: verdict.pending_capture_rate ?? null,
      final_eligibility_capture_rate: verdict.final_eligibility_capture_rate ?? null,
      paper_capture_rate: verdict.paper_capture_rate ?? null,
      realized_capture_rate: verdict.realized_capture_rate ?? null,
      mode_disabled_adjusted_final_eligibility_rate: (
        verdict.mode_disabled_adjusted_final_eligibility_rate
        ?? verdict.paper_entry_proposal_readiness?.mode_disabled_adjusted_final_eligibility_rate
        ?? null
      ),
      shadow_decision_bridge_summary: shadowDecisionBridge ? {
        available: !shadowDecisionBridge.error_code,
        status: shadowDecisionBridge.status || null,
        next_action: shadowDecisionBridge.next_action || null,
        root_cause: shadowDecisionBridge.root_cause || null,
        denominator: shadowDecisionBridge.denominator || null,
        bridge_expectation: shadowDecisionBridge.bridge_expectation || null,
        read_only_evidence_mirror: shadowDecisionBridge.read_only_evidence_mirror || null,
        mirror_event_count: Array.isArray(shadowDecisionBridge.mirror_events)
          ? shadowDecisionBridge.mirror_events.length
          : shadowDecisionBridge.denominator?.mirror_event_count ?? null,
        mirror_event_example_count: Array.isArray(shadowDecisionBridge.mirror_event_examples)
          ? shadowDecisionBridge.mirror_event_examples.length
          : null,
        mirror_event_coverage_vs_shadow_bridge_gap:
          shadowDecisionBridge.denominator?.mirror_event_coverage_vs_shadow_bridge_gap ?? null,
        mirror_event_truncated: shadowDecisionBridge.denominator?.mirror_event_truncated ?? null,
        promotion_allowed: Boolean(shadowDecisionBridge.promotion_allowed),
        automatic_bridge_to_entry_allowed: Boolean(shadowDecisionBridge.automatic_bridge_to_entry_allowed),
        paper_enablement_allowed: Boolean(shadowDecisionBridge.paper_enablement_allowed),
      } : null,
      paper_entry_proposal_readiness: verdict.paper_entry_proposal_readiness || null,
      stage2_entry_funnel_summary: verdict.stage2_entry_funnel_summary || null,
      entry_funnel_gap_summary: verdict.entry_funnel_gap_summary ? {
        pending_entry_signal_ids: verdict.entry_funnel_gap_summary.pending_entry_signal_ids,
        final_entry_contract_signal_ids: verdict.entry_funnel_gap_summary.final_entry_contract_signal_ids,
        pending_without_final_entry_contract:
          verdict.entry_funnel_gap_summary.pending_without_final_entry_contract,
        pending_to_final_entry_contract_rate:
          verdict.entry_funnel_gap_summary.pending_to_final_entry_contract_rate,
        pending_to_mode_adjusted_final_eligibility_rate:
          verdict.entry_funnel_gap_summary.pending_to_mode_adjusted_final_eligibility_rate,
        pending_without_final_entry_category_counts:
          verdict.entry_funnel_gap_summary.pending_without_final_entry_category_counts || null,
        readiness_gap_priority: verdict.entry_funnel_gap_summary.readiness_gap_priority || null,
        automatic_runtime_change_allowed:
          Boolean(verdict.entry_funnel_gap_summary.automatic_runtime_change_allowed),
        strategy_change_allowed:
          Boolean(verdict.entry_funnel_gap_summary.strategy_change_allowed),
        paper_enablement_allowed:
          Boolean(verdict.entry_funnel_gap_summary.paper_enablement_allowed),
      } : null,
      upstream_funnel_gap_summary: verdict.upstream_funnel_gap_summary || null,
      shadow_decision_bridge_audit_summary: verdict.shadow_decision_bridge_audit_summary || null,
      quality_timing_reject_research_audit: qualityTimingResearchSummary,
      quality_timing_candidate_probe_validation: qualityTimingProbeValidationSummary,
      final_entry_contract_blocker_breakdown: verdict.final_entry_contract_blocker_breakdown || null,
      per_candidate_effectiveness_summary: verdict.per_candidate_effectiveness_summary ? {
        candidate_count: verdict.per_candidate_effectiveness_summary.candidate_count,
        classification_counts: verdict.per_candidate_effectiveness_summary.classification_counts,
        top_candidates: (verdict.per_candidate_effectiveness_summary.top_candidates || []).slice(0, 10),
      } : null,
      Markov_effectiveness_summary: verdict.Markov_effectiveness_summary ? {
        status: verdict.Markov_effectiveness_summary.status,
        markov_used_for_promotion: Boolean(verdict.Markov_effectiveness_summary.markov_used_for_promotion),
        total_green_buckets: verdict.Markov_effectiveness_summary.total_green_buckets,
        total_yellow_buckets: verdict.Markov_effectiveness_summary.total_yellow_buckets,
        total_insufficient_buckets: verdict.Markov_effectiveness_summary.total_insufficient_buckets,
      } : null,
      two_d_cross_validity_summary: verdict.two_d_cross_validity_summary ? {
        valid_cross_count: verdict.two_d_cross_validity_summary.valid_cross_count,
        invalid_cross_count: verdict.two_d_cross_validity_summary.invalid_cross_count,
        invalid_reason_counts: verdict.two_d_cross_validity_summary.invalid_reason_counts,
      } : null,
      next_highest_priority_blocker: verdict.next_highest_priority_blocker,
      tests_passed: verdict.tests_passed,
      H1_status: verdict.H1_capture_metrics?.status,
      H2_status: verdict.H2_capture_metrics?.status,
      PnL_cross_secondary_status: verdict.PnL_cross_secondary_status?.status,
      virtual_Markov_discovery_status: verdict.virtual_Markov_discovery_status?.status,
    } : { available: false },
    registry_summary: registry ? {
      available: !registry.error_code,
      updated_at: registry.updated_at,
      promotion_allowed: Boolean(registry.promotion_allowed),
      hypothesis_keys: Object.keys(registry.hypotheses || {}),
      shadow_only_quality_timing_watch_count:
        Array.isArray(registry.shadow_only_quality_timing_watch)
          ? registry.shadow_only_quality_timing_watch.length
          : 0,
      shadow_only_quality_timing_candidate_probe_count:
        Array.isArray(registry.shadow_only_quality_timing_candidate_probes)
          ? registry.shadow_only_quality_timing_candidate_probes.length
          : 0,
      shadow_only_matured_volume_watch_count:
        Array.isArray(registry.shadow_only_matured_volume_watch)
          ? registry.shadow_only_matured_volume_watch.length
          : 0,
      recent_run_count: Array.isArray(registry.recent_runs) ? registry.recent_runs.length : 0,
    } : { available: false },
    tests_summary: tests ? {
      available: !tests.error_code,
      passed: Boolean(tests.passed),
      result_count: Array.isArray(tests.results) ? tests.results.length : 0,
    } : { available: false },
    runner_status: runner,
    notes: {
      read_only: true,
      discovery_only: 'This endpoint only exposes materialized discovery artifacts and read-only runner status. It never changes trading policy.',
    },
  };
  if (includeReports) {
    payload.reports = {
      verdict,
      registry,
      tests,
      capture: safeReadAgentJson(paths.capture),
      capture_24h: safeReadAgentJson(paths.capture_24h),
      capture_48h: safeReadAgentJson(paths.capture_48h),
      capture_72h: safeReadAgentJson(paths.capture_72h),
      raw_funnel: safeReadAgentJson(paths.raw_funnel),
      shadow_decision_bridge: safeReadAgentJson(paths.shadow_decision_bridge),
      downstream_readiness: safeReadAgentJson(paths.downstream_readiness),
      context_coverage: safeReadAgentJson(paths.context_coverage),
      context_blocker_monitor: safeReadAgentJson(paths.context_blocker_monitor),
      a_class_fastlane: safeReadAgentJson(paths.a_class_fastlane),
      candidate_effectiveness: safeReadAgentJson(paths.candidate_effectiveness),
      candidate_improvement: safeReadAgentJson(paths.candidate_improvement),
      capture_cross_validity: safeReadAgentJson(paths.capture_cross_validity),
      pnl: safeReadAgentJson(paths.pnl),
      markov_runtime: safeReadAgentJson(paths.markov_runtime),
      markov_kline: safeReadAgentJson(paths.markov_kline),
      markov_candidate_lifecycle: safeReadAgentJson(paths.markov_candidate_lifecycle),
      markov_candidate_source: safeReadAgentJson(paths.markov_candidate_source),
      markov_candidate_signal_type: safeReadAgentJson(paths.markov_candidate_signal_type),
      markov_candidate_lifecycle_source: safeReadAgentJson(paths.markov_candidate_lifecycle_source),
      markov_effectiveness: safeReadAgentJson(paths.markov_effectiveness),
      volume_kline_coverage: safeReadAgentJson(paths.volume_kline_coverage),
      matured_kline_recheck: safeReadAgentJson(paths.matured_kline_recheck),
      matured_volume_cross: safeReadAgentJson(paths.matured_volume_cross),
      hypothesis_validation: safeReadAgentJson(paths.hypothesis_validation),
      oos_readiness_refresh: safeReadAgentJson(paths.oos_readiness_refresh),
      low_confidence_research: safeReadAgentJson(paths.low_confidence_research),
      quality_timing_research: safeReadAgentJson(paths.quality_timing_research),
      quality_timing_probe_validation: safeReadAgentJson(paths.quality_timing_probe_validation),
      runtime_health: safeReadAgentJson(paths.runtime_health),
      runner_status: runner,
    };
  }
  return payload;
}

export function getRawDogDiscoveryApiSnapshotPath(options = {}) {
  const raw = options.snapshotPath
    || process.env.RAW_DOG_DISCOVERY_API_SNAPSHOT_PATH
    || join(dirname(getRawSignalOutcomesDbPath()), 'raw-dog-discovery-summary.json');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function getKlineCacheDbPath() {
  const raw = process.env.KLINE_CACHE_DB || process.env.KLINE_CACHE_DB_PATH || './data/kline_cache.db';
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function openRawSignalOutcomesDb({ readonly = false } = {}) {
  const rawDbPath = getRawSignalOutcomesDbPath();
  if (!readonly) {
    fs.mkdirSync(dirname(rawDbPath), { recursive: true });
  }
  const db = openDashboardSqlite(rawDbPath, readonly ? { readonly: true, fileMustExist: true } : undefined);
  if (!readonly) ensureRawSignalOutcomesSchema(db);
  return db;
}

function ensureRawSignalOutcomesSchema(db) {
  ensureRawPathObserverSchema(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS raw_signal_outcomes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      signal_id TEXT,
      token_ca TEXT NOT NULL,
      symbol TEXT,
      signal_ts INTEGER,
      signal_type TEXT,
      route TEXT,
      hard_gate_status TEXT,
      source TEXT,
      observation_status TEXT,
      right_censored INTEGER DEFAULT 0,
      matured_at_ts INTEGER,
      horizon_sec INTEGER,
      baseline_ts INTEGER,
      baseline_lag_sec REAL,
      baseline_price REAL,
      baseline_source TEXT,
      baseline_provider TEXT,
      baseline_pool_address TEXT,
      baseline_price_unit TEXT,
      baseline_confidence TEXT,
      source_kind TEXT,
      source_family TEXT,
      path_provider TEXT,
      path_pool_address TEXT,
      path_price_unit TEXT,
      path_source_kind TEXT,
      path_source_family TEXT,
      same_source_path INTEGER DEFAULT 0,
      kline_covered INTEGER DEFAULT 0,
      coverage_reason TEXT,
      pool_found INTEGER DEFAULT 0,
      provider TEXT,
      first_bar_ts INTEGER,
      first_bar_lag_sec INTEGER,
      early_15m_bar_count INTEGER DEFAULT 0,
      early_15m_expected_minutes INTEGER DEFAULT 15,
      early_15m_bar_coverage_pct REAL,
      early_15m_complete INTEGER DEFAULT 0,
      peak_5m_pct REAL,
      peak_15m_pct REAL,
      peak_60m_pct REAL,
      peak_120m_pct REAL,
      max_wick_peak_pct REAL,
      max_sustained_peak_pct REAL,
      time_to_wick_peak_sec INTEGER,
      time_to_sustained_peak_sec INTEGER,
      raw_wick_tier TEXT,
      raw_sustained_tier TEXT,
      raw_primary_tier TEXT,
      sustained_evaluable INTEGER DEFAULT 0,
      sustained_reason TEXT,
      outlier_flag INTEGER DEFAULT 0,
      outlier_reason TEXT,
      did_enter INTEGER DEFAULT 0,
      paper_trade_id TEXT,
      canonical_trade_id TEXT,
      entered_before_peak INTEGER DEFAULT 0,
      held_to_silver INTEGER DEFAULT 0,
      held_to_gold INTEGER DEFAULT 0,
      raw_dog_entered INTEGER DEFAULT 0,
      raw_dog_realized INTEGER DEFAULT 0,
      sold_before_silver INTEGER DEFAULT 0,
      sold_before_gold INTEGER DEFAULT 0,
      exit_reason TEXT,
      payload_json TEXT,
      created_at INTEGER DEFAULT (strftime('%s', 'now')),
      updated_at INTEGER DEFAULT (strftime('%s', 'now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_signal_outcomes_signal
      ON raw_signal_outcomes(signal_id, token_ca, signal_ts);
    CREATE INDEX IF NOT EXISTS idx_raw_signal_outcomes_window
      ON raw_signal_outcomes(signal_ts, observation_status, raw_primary_tier);
    CREATE INDEX IF NOT EXISTS idx_raw_signal_outcomes_token
      ON raw_signal_outcomes(token_ca, signal_ts);
  `);
  for (const stmt of [
    `ALTER TABLE raw_signal_outcomes ADD COLUMN source_kind TEXT`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN source_family TEXT`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN path_source_kind TEXT`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN path_source_family TEXT`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN first_bar_ts INTEGER`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN first_bar_lag_sec INTEGER`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN early_15m_bar_count INTEGER DEFAULT 0`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN early_15m_expected_minutes INTEGER DEFAULT 15`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN early_15m_bar_coverage_pct REAL`,
    `ALTER TABLE raw_signal_outcomes ADD COLUMN early_15m_complete INTEGER DEFAULT 0`,
  ]) {
    try { db.exec(stmt); } catch {}
  }
}

function upsertRawSignalOutcomes(db, outcomes) {
  if (!db || !Array.isArray(outcomes) || outcomes.length === 0) return 0;
  const stmt = db.prepare(`
    INSERT INTO raw_signal_outcomes (
      signal_id, token_ca, symbol, signal_ts, signal_type, route, hard_gate_status, source,
      observation_status, right_censored, matured_at_ts, horizon_sec,
      baseline_ts, baseline_lag_sec, baseline_price, baseline_source, baseline_provider,
      baseline_pool_address, baseline_price_unit, baseline_confidence,
      source_kind, source_family,
      path_provider, path_pool_address, path_price_unit, path_source_kind, path_source_family, same_source_path,
      kline_covered, coverage_reason, pool_found, provider,
      first_bar_ts, first_bar_lag_sec,
      early_15m_bar_count, early_15m_expected_minutes, early_15m_bar_coverage_pct, early_15m_complete,
      peak_5m_pct, peak_15m_pct, peak_60m_pct, peak_120m_pct,
      max_wick_peak_pct, max_sustained_peak_pct,
      time_to_wick_peak_sec, time_to_sustained_peak_sec,
      raw_wick_tier, raw_sustained_tier, raw_primary_tier,
      sustained_evaluable, sustained_reason,
      outlier_flag, outlier_reason, did_enter, paper_trade_id, canonical_trade_id,
      entered_before_peak, held_to_silver, held_to_gold, raw_dog_entered, raw_dog_realized,
      sold_before_silver, sold_before_gold, exit_reason, payload_json, updated_at
    ) VALUES (
      @signal_id, @token_ca, @symbol, @signal_ts, @signal_type, @route, @hard_gate_status, @source,
      @observation_status, @right_censored, @matured_at_ts, @horizon_sec,
      @baseline_ts, @baseline_lag_sec, @baseline_price, @baseline_source, @baseline_provider,
      @baseline_pool_address, @baseline_price_unit, @baseline_confidence,
      @source_kind, @source_family,
      @path_provider, @path_pool_address, @path_price_unit, @path_source_kind, @path_source_family, @same_source_path,
      @kline_covered, @coverage_reason, @pool_found, @provider,
      @first_bar_ts, @first_bar_lag_sec,
      @early_15m_bar_count, @early_15m_expected_minutes, @early_15m_bar_coverage_pct, @early_15m_complete,
      @peak_5m_pct, @peak_15m_pct, @peak_60m_pct, @peak_120m_pct,
      @max_wick_peak_pct, @max_sustained_peak_pct,
      @time_to_wick_peak_sec, @time_to_sustained_peak_sec,
      @raw_wick_tier, @raw_sustained_tier, @raw_primary_tier,
      @sustained_evaluable, @sustained_reason,
      @outlier_flag, @outlier_reason, @did_enter, @paper_trade_id, @canonical_trade_id,
      @entered_before_peak, @held_to_silver, @held_to_gold, @raw_dog_entered, @raw_dog_realized,
      @sold_before_silver, @sold_before_gold, @exit_reason, @payload_json, @updated_at
    )
    ON CONFLICT(signal_id, token_ca, signal_ts) DO UPDATE SET
      symbol = excluded.symbol,
      signal_type = excluded.signal_type,
      route = excluded.route,
      hard_gate_status = excluded.hard_gate_status,
      source = excluded.source,
      observation_status = excluded.observation_status,
      right_censored = excluded.right_censored,
      matured_at_ts = excluded.matured_at_ts,
      horizon_sec = excluded.horizon_sec,
      baseline_ts = excluded.baseline_ts,
      baseline_lag_sec = excluded.baseline_lag_sec,
      baseline_price = excluded.baseline_price,
      baseline_source = excluded.baseline_source,
      baseline_provider = excluded.baseline_provider,
      baseline_pool_address = excluded.baseline_pool_address,
      baseline_price_unit = excluded.baseline_price_unit,
      baseline_confidence = excluded.baseline_confidence,
      source_kind = excluded.source_kind,
      source_family = excluded.source_family,
      path_provider = excluded.path_provider,
      path_pool_address = excluded.path_pool_address,
      path_price_unit = excluded.path_price_unit,
      path_source_kind = excluded.path_source_kind,
      path_source_family = excluded.path_source_family,
      same_source_path = excluded.same_source_path,
      kline_covered = excluded.kline_covered,
      coverage_reason = excluded.coverage_reason,
      pool_found = excluded.pool_found,
      provider = excluded.provider,
      first_bar_ts = excluded.first_bar_ts,
      first_bar_lag_sec = excluded.first_bar_lag_sec,
      early_15m_bar_count = excluded.early_15m_bar_count,
      early_15m_expected_minutes = excluded.early_15m_expected_minutes,
      early_15m_bar_coverage_pct = excluded.early_15m_bar_coverage_pct,
      early_15m_complete = excluded.early_15m_complete,
      peak_5m_pct = excluded.peak_5m_pct,
      peak_15m_pct = excluded.peak_15m_pct,
      peak_60m_pct = excluded.peak_60m_pct,
      peak_120m_pct = excluded.peak_120m_pct,
      max_wick_peak_pct = excluded.max_wick_peak_pct,
      max_sustained_peak_pct = excluded.max_sustained_peak_pct,
      time_to_wick_peak_sec = excluded.time_to_wick_peak_sec,
      time_to_sustained_peak_sec = excluded.time_to_sustained_peak_sec,
      raw_wick_tier = excluded.raw_wick_tier,
      raw_sustained_tier = excluded.raw_sustained_tier,
      raw_primary_tier = excluded.raw_primary_tier,
      sustained_evaluable = excluded.sustained_evaluable,
      sustained_reason = excluded.sustained_reason,
      outlier_flag = excluded.outlier_flag,
      outlier_reason = excluded.outlier_reason,
      did_enter = excluded.did_enter,
      paper_trade_id = excluded.paper_trade_id,
      canonical_trade_id = excluded.canonical_trade_id,
      entered_before_peak = excluded.entered_before_peak,
      held_to_silver = excluded.held_to_silver,
      held_to_gold = excluded.held_to_gold,
      raw_dog_entered = excluded.raw_dog_entered,
      raw_dog_realized = excluded.raw_dog_realized,
      sold_before_silver = excluded.sold_before_silver,
      sold_before_gold = excluded.sold_before_gold,
      exit_reason = excluded.exit_reason,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at
  `);
  const tx = db.transaction((rows) => {
    for (const row of rows) {
      stmt.run(serializeRawSignalOutcomeForDb(row));
    }
  });
  tx(outcomes);
  return outcomes.length;
}

function serializeRawSignalOutcomeForDb(row) {
  const boolInt = (value) => value ? 1 : 0;
  const textOrNull = (value) => value == null ? null : String(value);
  const signalId = row.signal_id ?? (
    row.token_ca != null && row.signal_ts != null
      ? `${row.token_ca}:${row.signal_ts}`
      : null
  );
  return {
    signal_id: textOrNull(signalId),
    token_ca: textOrNull(row.token_ca),
    symbol: textOrNull(row.symbol),
    signal_ts: row.signal_ts ?? null,
    signal_type: textOrNull(row.signal_type),
    route: textOrNull(row.route),
    hard_gate_status: textOrNull(row.hard_gate_status),
    source: textOrNull(row.source),
    observation_status: textOrNull(row.observation_status),
    right_censored: boolInt(row.right_censored),
    matured_at_ts: row.matured_at_ts ?? null,
    horizon_sec: row.horizon_sec ?? null,
    baseline_ts: row.baseline_ts ?? null,
    baseline_lag_sec: row.baseline_lag_sec ?? null,
    baseline_price: row.baseline_price ?? null,
    baseline_source: textOrNull(row.baseline_source),
    baseline_provider: textOrNull(row.baseline_provider),
    baseline_pool_address: textOrNull(row.baseline_pool_address),
    baseline_price_unit: textOrNull(row.baseline_price_unit),
    baseline_confidence: textOrNull(row.baseline_confidence),
    source_kind: textOrNull(row.source_kind),
    source_family: textOrNull(row.source_family),
    path_provider: textOrNull(row.path_provider),
    path_pool_address: textOrNull(row.path_pool_address),
    path_price_unit: textOrNull(row.path_price_unit),
    path_source_kind: textOrNull(row.path_source_kind),
    path_source_family: textOrNull(row.path_source_family),
    same_source_path: boolInt(row.same_source_path),
    kline_covered: boolInt(row.kline_covered),
    coverage_reason: textOrNull(row.coverage_reason),
    pool_found: boolInt(row.pool_found),
    provider: textOrNull(row.provider),
    first_bar_ts: row.first_bar_ts ?? null,
    first_bar_lag_sec: row.first_bar_lag_sec ?? null,
    early_15m_bar_count: row.early_15m_bar_count ?? null,
    early_15m_expected_minutes: row.early_15m_expected_minutes ?? null,
    early_15m_bar_coverage_pct: row.early_15m_bar_coverage_pct ?? null,
    early_15m_complete: boolInt(row.early_15m_complete),
    peak_5m_pct: row.peak_5m_pct ?? null,
    peak_15m_pct: row.peak_15m_pct ?? null,
    peak_60m_pct: row.peak_60m_pct ?? null,
    peak_120m_pct: row.peak_120m_pct ?? null,
    max_wick_peak_pct: row.max_wick_peak_pct ?? null,
    max_sustained_peak_pct: row.max_sustained_peak_pct ?? null,
    time_to_wick_peak_sec: row.time_to_wick_peak_sec ?? null,
    time_to_sustained_peak_sec: row.time_to_sustained_peak_sec ?? null,
    raw_wick_tier: textOrNull(row.raw_wick_tier),
    raw_sustained_tier: textOrNull(row.raw_sustained_tier),
    raw_primary_tier: textOrNull(row.raw_primary_tier),
    sustained_evaluable: boolInt(row.sustained_evaluable),
    sustained_reason: textOrNull(row.sustained_reason),
    outlier_flag: boolInt(row.outlier_flag),
    outlier_reason: textOrNull(row.outlier_reason),
    did_enter: boolInt(row.did_enter),
    paper_trade_id: textOrNull(row.paper_trade_id),
    canonical_trade_id: textOrNull(row.canonical_trade_id),
    entered_before_peak: boolInt(row.entered_before_peak),
    held_to_silver: boolInt(row.held_to_silver),
    held_to_gold: boolInt(row.held_to_gold),
    raw_dog_entered: boolInt(row.raw_dog_entered),
    raw_dog_realized: boolInt(row.raw_dog_realized),
    sold_before_silver: boolInt(row.sold_before_silver),
    sold_before_gold: boolInt(row.sold_before_gold),
    exit_reason: textOrNull(row.exit_reason),
    payload_json: JSON.stringify(row),
    updated_at: Math.floor(Date.now() / 1000),
  };
}

function upsertRawPriceBars(db, bars) {
  if (!db || !Array.isArray(bars) || bars.length === 0) return 0;
  const stmt = db.prepare(`
    INSERT INTO raw_price_bars_1m (
      token_ca, pool_address, timestamp, open, high, low, close, volume,
      provider, source_kind, source_family, price_unit,
      trade_count, first_trade_ts, last_trade_ts, fetched_at, payload_json, updated_at
    ) VALUES (
      @token_ca, @pool_address, @timestamp, @open, @high, @low, @close, @volume,
      @provider, @source_kind, @source_family, @price_unit,
      @trade_count, @first_trade_ts, @last_trade_ts, @fetched_at, @payload_json, @updated_at
    )
    ON CONFLICT(token_ca, pool_address, timestamp, provider, source_kind, price_unit) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      source_family = excluded.source_family,
      trade_count = excluded.trade_count,
      first_trade_ts = excluded.first_trade_ts,
      last_trade_ts = excluded.last_trade_ts,
      fetched_at = excluded.fetched_at,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at
  `);
  const now = Math.floor(Date.now() / 1000);
  const tx = db.transaction((rows) => {
    for (const row of rows) {
      const normalized = normalizeRawPathBar(row);
      if (!normalized.token_ca || !normalized.pool_address || normalized.timestamp == null) continue;
      if (normalized.open == null || normalized.high == null || normalized.low == null || normalized.close == null) continue;
      stmt.run({
        ...normalized,
        volume: normalized.volume ?? 0,
        source_family: normalized.source_family || null,
        price_unit: normalized.price_unit || 'native',
        trade_count: normalized.trade_count ?? null,
        first_trade_ts: normalized.first_trade_ts ?? null,
        last_trade_ts: normalized.last_trade_ts ?? null,
        fetched_at: normalized.fetched_at ?? now,
        payload_json: normalized.payload_json || null,
        updated_at: now,
      });
    }
  });
  tx(bars);
  return bars.length;
}

function upsertRawSignalObservations(db, observations) {
  if (!db || !Array.isArray(observations) || observations.length === 0) return 0;
  const stmt = db.prepare(`
    INSERT INTO raw_signal_observations (
      signal_id, token_ca, symbol, signal_ts, horizon_sec, status, right_censored, matured_at_ts,
      source_kind, provider, pool_address, path_row_count, first_bar_ts, first_bar_lag_sec,
      early_15m_bar_count, early_15m_expected_minutes, early_15m_bar_coverage_pct, early_15m_complete,
      coverage_reason, payload_json, updated_at
    ) VALUES (
      @signal_id, @token_ca, @symbol, @signal_ts, @horizon_sec, @status, @right_censored, @matured_at_ts,
      @source_kind, @provider, @pool_address, @path_row_count, @first_bar_ts, @first_bar_lag_sec,
      @early_15m_bar_count, @early_15m_expected_minutes, @early_15m_bar_coverage_pct, @early_15m_complete,
      @coverage_reason, @payload_json, @updated_at
    )
    ON CONFLICT(signal_id, token_ca, signal_ts) DO UPDATE SET
      symbol = excluded.symbol,
      horizon_sec = excluded.horizon_sec,
      status = excluded.status,
      right_censored = excluded.right_censored,
      matured_at_ts = excluded.matured_at_ts,
      source_kind = excluded.source_kind,
      provider = excluded.provider,
      pool_address = excluded.pool_address,
      path_row_count = excluded.path_row_count,
      first_bar_ts = excluded.first_bar_ts,
      first_bar_lag_sec = excluded.first_bar_lag_sec,
      early_15m_bar_count = excluded.early_15m_bar_count,
      early_15m_expected_minutes = excluded.early_15m_expected_minutes,
      early_15m_bar_coverage_pct = excluded.early_15m_bar_coverage_pct,
      early_15m_complete = excluded.early_15m_complete,
      coverage_reason = excluded.coverage_reason,
      payload_json = excluded.payload_json,
      updated_at = excluded.updated_at
  `);
  const textOrNull = (value) => value == null ? null : String(value);
  const boolInt = (value) => value ? 1 : 0;
  const tx = db.transaction((rows) => {
    for (const row of rows) {
      if (!row.token_ca || row.signal_ts == null) continue;
      stmt.run({
        signal_id: textOrNull(row.signal_id || `${row.token_ca}:${row.signal_ts}`),
        token_ca: textOrNull(row.token_ca),
        symbol: textOrNull(row.symbol),
        signal_ts: row.signal_ts,
        horizon_sec: row.horizon_sec ?? null,
        status: textOrNull(row.status),
        right_censored: boolInt(row.right_censored),
        matured_at_ts: row.matured_at_ts ?? null,
        source_kind: textOrNull(row.source_kind),
        provider: textOrNull(row.provider),
        pool_address: textOrNull(row.pool_address),
        path_row_count: row.path_row_count ?? 0,
        first_bar_ts: row.first_bar_ts ?? null,
        first_bar_lag_sec: row.first_bar_lag_sec ?? null,
        early_15m_bar_count: row.early_15m_bar_count ?? 0,
        early_15m_expected_minutes: row.early_15m_expected_minutes ?? 15,
        early_15m_bar_coverage_pct: row.early_15m_bar_coverage_pct ?? null,
        early_15m_complete: boolInt(row.early_15m_complete),
        coverage_reason: textOrNull(row.coverage_reason),
        payload_json: JSON.stringify(row),
        updated_at: Math.floor(Date.now() / 1000),
      });
    }
  });
  tx(observations);
  return observations.length;
}

function tokenChunks(tokens, chunkSize = 250) {
  const out = [];
  for (let i = 0; i < tokens.length; i += chunkSize) {
    out.push(tokens.slice(i, i + chunkSize));
  }
  return out;
}

function optionalSqlColumn(cols, name, fallback = 'NULL') {
  return cols.has(name) ? name : `${fallback} AS ${name}`;
}

function optionalSqlExpr(cols, name, fallback = 'NULL') {
  return cols.has(name) ? name : fallback;
}

function attachSharedBlockCause(row = {}) {
  const classification = classifyAClassBlockCause(row);
  return {
    ...row,
    block_cause: row.block_cause || classification.category || 'UNKNOWN',
    recoverability: row.recoverability || classification.recoverability || null,
    classification_reason: row.classification_reason || classification.classification_reason || classification.reason || null,
    would_enter_a_class: row.would_enter_a_class ?? (classification.would_enter_a_class ? 1 : 0),
    did_enter: row.did_enter ?? (classification.did_enter ? 1 : 0),
  };
}

function rawDogDecisionQueryWindow(rawDogs = [], fallbackSinceTs = null, fallbackUntilTs = null) {
  const windows = (rawDogs || []).map((row) => {
    const signalTs = Number(row.signal_ts);
    if (!Number.isFinite(signalTs)) return null;
    const peakSec = Number(row.time_to_sustained_peak_sec);
    const endOffset = Number.isFinite(peakSec) && peakSec > 0 ? Math.max(60, Math.min(900, peakSec)) : 900;
    return {
      start_ts: Math.floor(signalTs - 60),
      end_ts: Math.floor(signalTs + endOffset),
    };
  }).filter(Boolean);
  const minStart = windows.length ? Math.min(...windows.map((row) => row.start_ts)) : null;
  const maxEnd = windows.length ? Math.max(...windows.map((row) => row.end_ts)) : null;
  return {
    since_ts: minStart ?? fallbackSinceTs,
    until_ts: maxEnd ?? fallbackUntilTs,
  };
}

function readRawDogDecisionRecordsFromPaperDb({
  paperDbPath = getPaperDbPath(),
  rawDogs = [],
  sinceTs = null,
  untilTs = null,
  timeoutMs = 1500,
} = {}) {
  const tokens = [...new Set((rawDogs || []).map((row) => String(row.token_ca || '').trim()).filter(Boolean))];
  const diagnostics = {
    available: false,
    path: paperDbPath,
    raw_dogs: Array.isArray(rawDogs) ? rawDogs.length : 0,
    tokens: tokens.length,
    records: 0,
    source_issues: [],
    since_ts: null,
    until_ts: null,
  };
  if (!tokens.length) {
    diagnostics.source_issues.push('no_raw_dogs');
    return { records: [], diagnostics };
  }
  if (!paperDbPath || !fs.existsSync(paperDbPath)) {
    diagnostics.source_issues.push('paper_db_missing');
    return { records: [], diagnostics };
  }
  const queryWindow = rawDogDecisionQueryWindow(rawDogs, sinceTs, untilTs);
  const startTs = Number.isFinite(Number(queryWindow.since_ts)) ? Number(queryWindow.since_ts) : null;
  const endTs = Number.isFinite(Number(queryWindow.until_ts)) ? Number(queryWindow.until_ts) : Math.floor(Date.now() / 1000);
  diagnostics.since_ts = startTs;
  diagnostics.until_ts = endTs;
  let paperDb;
  const rows = [];
  try {
    paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: timeoutMs });
    const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
    const runTokenQuery = (sqlForChunk) => {
      for (const chunk of tokenChunks(tokens)) {
        const placeholders = chunk.map(() => '?').join(',');
        rows.push(...sqlForChunk(placeholders).all(startTs, endTs, ...chunk));
      }
    };

    if (tableNames.has('a_class_decision_events')) {
      const cols = getTableColumns(paperDb, 'a_class_decision_events');
      runTokenQuery((placeholders) => paperDb.prepare(`
        SELECT
          id,
          'a_class_decision_events' AS source_kind,
          event_ts,
          token_ca,
          ${optionalSqlColumn(cols, 'symbol')},
          ${optionalSqlColumn(cols, 'lifecycle_id')},
          ${optionalSqlColumn(cols, 'route_bucket')},
          ${optionalSqlColumn(cols, 'source_table')},
          ${optionalSqlColumn(cols, 'source_component')},
          ${optionalSqlColumn(cols, 'source_reason')},
          ${optionalSqlColumn(cols, 'action', "'BLOCK'")},
          ${optionalSqlColumn(cols, 'would_action')},
          ${optionalSqlColumn(cols, 'reason')},
          ${optionalSqlColumn(cols, 'hard_blockers_json', "'[]'")},
          ${optionalSqlColumn(cols, 'risk_json')},
          ${optionalSqlColumn(cols, 'candidate_json')},
          ${optionalSqlColumn(cols, 'expected_rr')},
          ${optionalSqlColumn(cols, 'score')},
          ${optionalSqlColumn(cols, 'grade')},
          ${optionalSqlColumn(cols, 'block_cause')},
          ${optionalSqlColumn(cols, 'recoverability')},
          ${optionalSqlColumn(cols, 'classification_reason')},
          ${optionalSqlColumn(cols, 'blocker_classifications_json')},
          ${optionalSqlColumn(cols, 'quote_available')},
          ${optionalSqlColumn(cols, 'quote_executable')},
          ${optionalSqlColumn(cols, 'quote_clean')},
          ${optionalSqlColumn(cols, 'route_available')},
          ${optionalSqlColumn(cols, 'quote_source')},
          ${optionalSqlColumn(cols, 'data_confidence')},
          ${optionalSqlColumn(cols, 'provider_reason')},
          ${optionalSqlColumn(cols, 'evidence_status')},
          ${optionalSqlColumn(cols, 'quote_failure_reason')},
          ${optionalSqlColumn(cols, 'route_failure_reason')},
          ${optionalSqlColumn(cols, 'liquidity_usd')},
          ${optionalSqlColumn(cols, 'spread_pct')},
          NULL AS would_enter_a_class,
          NULL AS did_enter,
          ${optionalSqlExpr(cols, 'provider_hydrate_outcome', 'NULL')} AS provider_hydrate_outcome,
          ${optionalSqlExpr(cols, 'provider_hydrate_outcome', 'NULL')} AS hydrate_outcome
        FROM a_class_decision_events
        WHERE event_ts >= ?
          AND event_ts <= ?
          AND token_ca IN (${placeholders})
        ORDER BY event_ts ASC, id ASC
      `));
    } else {
      diagnostics.source_issues.push('a_class_decision_events_missing');
    }

    if (tableNames.has('opportunity_events')) {
      const cols = getTableColumns(paperDb, 'opportunity_events');
      runTokenQuery((placeholders) => paperDb.prepare(`
        SELECT
          id,
          'opportunity_events' AS source_kind,
          event_ts,
          token_ca,
          ${optionalSqlColumn(cols, 'symbol')},
          ${optionalSqlColumn(cols, 'lifecycle_id')},
          ${optionalSqlColumn(cols, 'route_bucket')},
          ${optionalSqlExpr(cols, 'source_type', 'NULL')} AS source_table,
          ${optionalSqlColumn(cols, 'source_component')},
          ${optionalSqlColumn(cols, 'source_reason')},
          CASE
            WHEN COALESCE(${optionalSqlExpr(cols, 'did_enter', '0')}, 0) = 1 THEN 'ENTER'
            WHEN COALESCE(${optionalSqlExpr(cols, 'would_enter_a_class', '0')}, 0) = 1 THEN 'WOULD_ENTER'
            ELSE 'BLOCK'
          END AS action,
          NULL AS would_action,
          ${optionalSqlExpr(cols, 'quote_failure_reason', 'NULL')} AS reason,
          ${optionalSqlColumn(cols, 'hard_blockers_json', "'[]'")},
          NULL AS risk_json,
          ${optionalSqlExpr(cols, 'raw_payload_json', 'NULL')} AS candidate_json,
          ${optionalSqlColumn(cols, 'expected_rr')},
          ${optionalSqlExpr(cols, 'matrix_score', 'NULL')} AS score,
          NULL AS grade,
          ${optionalSqlColumn(cols, 'block_cause')},
          ${optionalSqlColumn(cols, 'recoverability')},
          ${optionalSqlColumn(cols, 'classification_reason')},
          ${optionalSqlColumn(cols, 'blocker_classifications_json')},
          ${optionalSqlColumn(cols, 'quote_available')},
          ${optionalSqlColumn(cols, 'quote_executable')},
          ${optionalSqlColumn(cols, 'quote_clean')},
          ${optionalSqlColumn(cols, 'route_available')},
          ${optionalSqlColumn(cols, 'quote_source')},
          ${optionalSqlColumn(cols, 'data_confidence')},
          ${optionalSqlColumn(cols, 'provider_reason')},
          ${optionalSqlColumn(cols, 'evidence_status')},
          ${optionalSqlColumn(cols, 'quote_failure_reason')},
          ${optionalSqlColumn(cols, 'route_failure_reason')},
          ${optionalSqlColumn(cols, 'liquidity_usd')},
          ${optionalSqlColumn(cols, 'spread_pct')},
          ${optionalSqlColumn(cols, 'would_enter_a_class', '0')},
          ${optionalSqlColumn(cols, 'did_enter', '0')},
          ${optionalSqlExpr(cols, 'hydrate_outcome', 'NULL')} AS provider_hydrate_outcome,
          ${optionalSqlExpr(cols, 'hydrate_outcome', 'NULL')} AS hydrate_outcome
        FROM opportunity_events
        WHERE event_ts >= ?
          AND event_ts <= ?
          AND token_ca IN (${placeholders})
        ORDER BY event_ts ASC, id ASC
      `));
    } else {
      diagnostics.source_issues.push('opportunity_events_missing');
    }

    const normalizedRows = rows
      .map(attachSharedBlockCause)
      .sort((a, b) => Number(a.event_ts || 0) - Number(b.event_ts || 0) || Number(a.id || 0) - Number(b.id || 0));
    diagnostics.available = true;
    diagnostics.records = normalizedRows.length;
    return { records: normalizedRows, diagnostics };
  } catch (error) {
    diagnostics.source_issues.push(error?.message || String(error));
    return { records: [], diagnostics };
  } finally {
    try { if (paperDb) paperDb.close(); } catch {}
  }
}

function dedupePathRows(rows = []) {
  const byKey = new Map();
  for (const row of rows.map((item) => normalizeRawPathBar(item))) {
    if (!row.token_ca || row.timestamp == null || row.high == null || row.low == null || row.close == null) continue;
    const key = [
      row.token_ca,
      row.pool_address || '',
      row.timestamp,
      row.provider || '',
      row.source_kind || '',
      row.price_unit || '',
    ].join('|');
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, row);
      continue;
    }
    existing.open = existing.open ?? row.open;
    existing.high = Math.max(Number(existing.high || 0), Number(row.high || 0));
    existing.low = Math.min(Number(existing.low || row.low || 0), Number(row.low || existing.low || 0));
    existing.close = row.close ?? existing.close;
    existing.volume = Number(existing.volume || 0) + Number(row.volume || 0);
    existing.trade_count = Number(existing.trade_count || 0) + Number(row.trade_count || 0) || null;
    existing.first_trade_ts = existing.first_trade_ts == null ? row.first_trade_ts : Math.min(existing.first_trade_ts, row.first_trade_ts ?? existing.first_trade_ts);
    existing.last_trade_ts = existing.last_trade_ts == null ? row.last_trade_ts : Math.max(existing.last_trade_ts, row.last_trade_ts ?? existing.last_trade_ts);
  }
  return [...byKey.values()].sort((a, b) => (
    String(a.token_ca).localeCompare(String(b.token_ca))
    || Number(a.timestamp) - Number(b.timestamp)
    || String(a.provider).localeCompare(String(b.provider))
  ));
}

function readRawPriceBarsFromDb(db, tokens, startTs, endTs) {
  if (!db || !Array.isArray(tokens) || !tokens.length) return [];
  const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
  if (!tables.has('raw_price_bars_1m')) return [];
  const rows = [];
  for (const chunk of tokenChunks(tokens)) {
    const placeholders = chunk.map(() => '?').join(',');
    rows.push(...db.prepare(`
      SELECT
        token_ca, pool_address, timestamp, open, high, low, close, volume,
        provider, source_kind, source_family, price_unit,
        trade_count, first_trade_ts, last_trade_ts, fetched_at, payload_json
      FROM raw_price_bars_1m
      WHERE timestamp >= ?
        AND timestamp <= ?
        AND token_ca IN (${placeholders})
      ORDER BY token_ca ASC, timestamp ASC
    `).all(startTs, endTs, ...chunk));
  }
  return rows;
}

function normalizeKlineCacheProvider(provider) {
  const text = String(provider || '').toLowerCase();
  if (text.includes('helius')) return { provider: 'helius_amm_pool', source_kind: 'amm_pool', source_family: 'onchain_swap' };
  if (text.includes('gmgn')) return { provider: 'gmgn', source_kind: 'indexed_ohlcv', source_family: 'third_party_kline' };
  if (text.includes('dex')) return { provider: 'dexscreener', source_kind: 'indexed_ohlcv', source_family: 'third_party_kline' };
  if (text.includes('gecko')) return { provider: 'geckoterminal', source_kind: 'indexed_ohlcv', source_family: 'third_party_kline' };
  return { provider: provider || 'kline_cache', source_kind: 'indexed_ohlcv', source_family: 'third_party_kline' };
}

function readKlineCachePathRows(tokens, startTs, endTs) {
  const klineDbPath = getKlineCacheDbPath();
  const diagnostics = {
    available: false,
    path: klineDbPath,
    rows: 0,
    helius_trade_rows: 0,
    aggregated_raw_rows: 0,
    error: null,
  };
  if (!fs.existsSync(klineDbPath) || !tokens.length) {
    diagnostics.error = !tokens.length ? 'no_tokens' : 'kline_cache_db_missing';
    return { rows: [], aggregatedRawRows: [], diagnostics };
  }
  let klineDb;
  try {
    klineDb = openDashboardSqlite(klineDbPath, { readonly: true, fileMustExist: true });
    const tables = new Set(klineDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
    diagnostics.available = true;
    const rows = [];
    const tradeRows = [];
    if (tables.has('kline_1m')) {
      const cols = getTableColumns(klineDb, 'kline_1m');
      if (cols.has('token_ca') && cols.has('timestamp') && cols.has('high') && cols.has('low') && cols.has('close')) {
        for (const chunk of tokenChunks(tokens)) {
          const placeholders = chunk.map(() => '?').join(',');
          const fetched = klineDb.prepare(`
            SELECT
              token_ca,
              ${cols.has('pool_address') ? 'pool_address' : "'' AS pool_address"},
              timestamp,
              ${cols.has('open') ? 'open' : 'close AS open'},
              high,
              low,
              close,
              ${cols.has('volume') ? 'volume' : '0 AS volume'},
              ${cols.has('provider') ? 'provider' : "'kline_cache' AS provider"},
              ${cols.has('fetched_at') ? 'fetched_at' : 'NULL AS fetched_at'}
            FROM kline_1m
            WHERE timestamp >= ?
              AND timestamp <= ?
              AND token_ca IN (${placeholders})
            ORDER BY token_ca ASC, timestamp ASC
          `).all(startTs, endTs, ...chunk);
          rows.push(...fetched.map((row) => {
            const source = normalizeKlineCacheProvider(row.provider);
            return {
              ...row,
              provider: source.provider,
              source_kind: source.source_kind,
              source_family: source.source_family,
              price_unit: 'native',
            };
          }));
        }
      }
    }
    if (tables.has('helius_trades')) {
      const cols = getTableColumns(klineDb, 'helius_trades');
      if (cols.has('token_ca') && cols.has('block_time') && cols.has('price')) {
        for (const chunk of tokenChunks(tokens)) {
          const placeholders = chunk.map(() => '?').join(',');
          tradeRows.push(...klineDb.prepare(`
            SELECT
              ${cols.has('signature') ? 'signature' : 'NULL AS signature'},
              ${cols.has('slot') ? 'slot' : 'NULL AS slot'},
              block_time,
              token_ca,
              ${cols.has('pool_address') ? 'pool_address' : "'' AS pool_address"},
              price,
              ${cols.has('base_amount') ? 'base_amount' : 'NULL AS base_amount'},
              ${cols.has('quote_amount') ? 'quote_amount' : 'NULL AS quote_amount'},
              ${cols.has('volume') ? 'volume' : '0 AS volume'},
              ${cols.has('side') ? 'side' : 'NULL AS side'},
              ${cols.has('source') ? 'source' : "'helius' AS source"}
            FROM helius_trades
            WHERE block_time >= ?
              AND block_time <= ?
              AND token_ca IN (${placeholders})
            ORDER BY token_ca ASC, block_time ASC
          `).all(startTs, endTs, ...chunk));
        }
      }
    }
    const aggregatedRawRows = aggregateSwapsToRawPriceBars(tradeRows.map((row) => {
      const sourceText = String(row.source || '').toLowerCase();
      const poolText = String(row.pool_address || '').trim();
      const pumpFunNoPool = !poolText && String(row.token_ca || '').toLowerCase().endsWith('pump');
      const bondingCurve = sourceText.includes('bonding') || pumpFunNoPool;
      return {
        ...row,
        source_kind: bondingCurve ? 'bonding_curve' : 'amm_pool',
        provider: bondingCurve ? 'helius_bonding_curve' : 'helius_amm_pool',
        pool_address: poolText || (bondingCurve ? `bonding_curve:${row.token_ca}` : ''),
      };
    }), { price_unit: 'native' });
    diagnostics.rows = rows.length;
    diagnostics.helius_trade_rows = tradeRows.length;
    diagnostics.aggregated_raw_rows = aggregatedRawRows.length;
    return { rows, aggregatedRawRows, diagnostics };
  } catch (error) {
    diagnostics.error = error?.message || String(error);
    return { rows: [], aggregatedRawRows: [], diagnostics };
  } finally {
    try { if (klineDb) klineDb.close(); } catch {}
  }
}

export function buildRawDogDiscoverySnapshot({
  signalDb,
  paperDbPath = getPaperDbPath(),
  sinceTs = null,
  limit = 5000,
  nowTs = Math.floor(Date.now() / 1000),
  horizonSec = 7200,
  baselineMaxLagSec = 300,
  coverageTargetPct = 80,
  persist = true,
} = {}) {
  if (!signalDb) {
    return {
      available: false,
      error: 'sentiment database unavailable',
      report: null,
      persisted_rows: 0,
    };
  }
  const signalTables = new Set(
    signalDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
  );
  if (!signalTables.has('premium_signals')) {
    return {
      available: false,
      error: 'premium_signals table not found',
      report: null,
      persisted_rows: 0,
    };
  }
  const signalCols = getTableColumns(signalDb, 'premium_signals');
  const timestampExpr = signalCols.has('timestamp')
    ? "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
    : "0";
  const signalWhere = sinceTs && signalCols.has('timestamp')
    ? `WHERE ((timestamp > 1000000000000 AND timestamp >= @sinceMs) OR (timestamp <= 1000000000000 AND timestamp >= @since))`
    : (sinceTs ? `WHERE ${timestampExpr} >= @since` : '');
  const signalRows = signalDb.prepare(`
    SELECT
      ${signalCols.has('id') ? 'id' : 'NULL AS id'},
      ${signalCols.has('symbol') ? 'symbol' : 'NULL AS symbol'},
      token_ca,
      ${signalCols.has('timestamp') ? 'timestamp' : 'NULL AS timestamp'},
      ${timestampExpr} AS timestamp_sec,
      ${signalCols.has('created_at') ? 'created_at' : 'NULL AS created_at'},
      ${signalCols.has('lifecycle_id') ? 'lifecycle_id' : 'NULL AS lifecycle_id'},
      ${signalCols.has('downstream_lifecycle_id') ? 'downstream_lifecycle_id' : 'NULL AS downstream_lifecycle_id'},
      ${signalCols.has('signal_type') ? 'signal_type' : 'NULL AS signal_type'},
      ${signalCols.has('hard_gate_status') ? 'hard_gate_status' : 'NULL AS hard_gate_status'},
      ${signalCols.has('gate_result') ? 'gate_result' : 'NULL AS gate_result'},
      ${signalCols.has('ai_action') ? 'ai_action' : 'NULL AS ai_action'}
    FROM premium_signals
    ${signalWhere}
    ORDER BY ${timestampExpr} DESC, ${signalCols.has('id') ? 'id' : timestampExpr} DESC
    LIMIT @limit
  `).all(sinceTs ? { since: sinceTs, sinceMs: sinceTs * 1000, limit } : { limit });
  const uniqueSignalTokens = [...new Set(signalRows.map((row) => String(row.token_ca || '').trim()).filter(Boolean))];
  const pathSinceTs = sinceTs || Math.max(0, nowTs - horizonSec - 6 * 3600);
  const pathUntilTs = nowTs;

  let rawDbForPath = null;
  let durableRawPathRows = [];
  let rawPathDbError = null;
  try {
    const rawDbPath = getRawSignalOutcomesDbPath();
    if (persist || fs.existsSync(rawDbPath)) {
      rawDbForPath = openRawSignalOutcomesDb({ readonly: !persist && fs.existsSync(rawDbPath) });
      durableRawPathRows = readRawPriceBarsFromDb(rawDbForPath, uniqueSignalTokens, pathSinceTs, pathUntilTs);
    }
  } catch (error) {
    rawPathDbError = error?.message || String(error);
  }

  const klineCachePath = readKlineCachePathRows(uniqueSignalTokens, pathSinceTs, pathUntilTs);
  const cachePathRows = dedupePathRows([...(klineCachePath.rows || []), ...(klineCachePath.aggregatedRawRows || [])]);
  let rawBarsPersisted = 0;
  if (persist && rawDbForPath && klineCachePath.aggregatedRawRows?.length) {
    try {
      rawBarsPersisted = upsertRawPriceBars(rawDbForPath, klineCachePath.aggregatedRawRows);
    } catch (error) {
      rawPathDbError = error?.message || String(error);
    }
  }

  let klineRows = [];
  const klineDiagnostics = {
    available: false,
    table: 'kline_1m',
    rows: 0,
    unique_signal_tokens: 0,
    queried_tokens: 0,
    query_chunks: 0,
    coverage_window: {
      since_ts: sinceTs,
      until_ts: nowTs,
      horizon_sec: horizonSec,
      baseline_max_lag_sec: baselineMaxLagSec,
    },
  };
  if (signalTables.has('kline_1m')) {
    const klineCols = getTableColumns(signalDb, 'kline_1m');
    const required = ['token_ca', 'timestamp', 'high', 'low', 'close'];
    if (required.every((name) => klineCols.has(name))) {
      klineDiagnostics.available = true;
      klineDiagnostics.unique_signal_tokens = uniqueSignalTokens.length;
      const klineSinceTs = pathSinceTs;
      const klineUntilTs = pathUntilTs;
      klineDiagnostics.coverage_window.since_ts = klineSinceTs;
      klineDiagnostics.coverage_window.until_ts = klineUntilTs;
      const chunkSize = 250;
      for (let i = 0; i < uniqueSignalTokens.length; i += chunkSize) {
        const chunk = uniqueSignalTokens.slice(i, i + chunkSize);
        if (!chunk.length) continue;
        const placeholders = chunk.map(() => '?').join(',');
        const rows = signalDb.prepare(`
          SELECT
            token_ca,
            timestamp,
            ${klineCols.has('pool_address') ? 'pool_address' : 'NULL AS pool_address'},
            ${klineCols.has('open') ? 'open' : 'NULL AS open'},
            high,
            low,
            close,
            ${klineCols.has('volume') ? 'volume' : 'NULL AS volume'},
            ${klineCols.has('source') ? 'source' : 'NULL AS source'},
            ${klineCols.has('provider') ? 'provider' : 'NULL AS provider'},
            ${klineCols.has('price_unit') ? 'price_unit' : 'NULL AS price_unit'}
          FROM kline_1m
          WHERE timestamp >= ?
            AND timestamp <= ?
            AND token_ca IN (${placeholders})
          ORDER BY token_ca ASC, timestamp ASC
        `).all(klineSinceTs, klineUntilTs, ...chunk);
        klineRows.push(...rows);
        klineDiagnostics.query_chunks += 1;
      }
      klineDiagnostics.queried_tokens = uniqueSignalTokens.length;
      klineDiagnostics.rows = klineRows.length;
    } else {
      klineDiagnostics.note = 'kline_1m missing required token_ca/timestamp/high/low/close columns';
    }
  } else {
    klineDiagnostics.note = 'kline_1m table missing';
  }

  let paperTrades = [];
  const paperDiagnostics = {
    available: false,
    path: paperDbPath,
    rows: 0,
  };
  let paperDb;
  try {
    if (paperDbPath && fs.existsSync(paperDbPath)) {
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const paperTables = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (paperTables.has('paper_trades')) {
        const tradeCols = getTableColumns(paperDb, 'paper_trades');
        const trustedPeakExpr = trustedTradePeakSqlExpr(tradeCols);
        const tradeWhere = sinceTs
          ? 'WHERE COALESCE(entry_ts, exit_ts, 0) >= @since OR COALESCE(exit_ts, 0) >= @since'
          : '';
        paperTrades = paperDb.prepare(`
          SELECT
            ${tradeCols.has('id') ? 'id' : 'NULL AS id'},
            token_ca,
            ${tradeCols.has('symbol') ? 'symbol' : 'NULL AS symbol'},
            ${tradeCols.has('entry_ts') ? 'entry_ts' : 'NULL AS entry_ts'},
            ${tradeCols.has('exit_ts') ? 'exit_ts' : 'NULL AS exit_ts'},
            ${tradeCols.has('exit_reason') ? 'exit_reason' : 'NULL AS exit_reason'},
            ${tradeCols.has('pnl_pct') ? 'pnl_pct' : 'NULL AS pnl_pct'},
            ${trustedPeakExpr} AS peak_pnl,
            ${tradeCols.has('position_size_sol') ? 'position_size_sol' : 'NULL AS position_size_sol'},
            ${tradeCols.has('signal_route') ? 'signal_route' : 'NULL AS signal_route'},
            ${tradeCols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode'}
          FROM paper_trades
          ${tradeWhere}
          ORDER BY COALESCE(entry_ts, exit_ts, 0) DESC, id DESC
          LIMIT @limit
        `).all(sinceTs ? { since: sinceTs, limit } : { limit });
        paperDiagnostics.available = true;
        paperDiagnostics.rows = paperTrades.length;
      } else {
        paperDiagnostics.note = 'paper_trades table missing';
      }
    } else {
      paperDiagnostics.note = 'paper DB path missing';
    }
  } finally {
    try { if (paperDb) paperDb.close(); } catch {}
  }

  const rawPathRows = dedupePathRows([...durableRawPathRows, ...cachePathRows]);
  const preferredPath = mergePreferredPathRows({
    signals: signalRows,
    rawPathRows,
    klineRows,
  });
  const rawSignalObservations = buildRawSignalObservations({
    signals: signalRows,
    pathRows: preferredPath.rows,
    nowTs,
    horizonSec,
    earlyWindowSec: 900,
  });
  const rawPathDiagnostics = summarizeRawPathDiagnostics({
    signals: signalRows,
    rawPathRows,
    klineRows,
    preferredRows: preferredPath.rows,
    observations: rawSignalObservations,
    decisions: preferredPath.decisions,
  });

  const report = buildRawSignalOutcomeReport({
    signals: signalRows,
    klineRows: preferredPath.rows,
    paperTrades,
    nowTs,
    horizonSec,
    baselineMaxLagSec,
    coverageTargetPct,
  });
  const decisionEvidence = readRawDogDecisionRecordsFromPaperDb({
    paperDbPath,
    rawDogs: report.top_raw_dogs || [],
    sinceTs: pathSinceTs,
    untilTs: nowTs,
  });
  const decisionFunnel = buildRawDogDecisionFunnel({
    rawDogs: report.top_raw_dogs || [],
    decisionRecords: decisionEvidence.records,
  });
  report.decision_funnel = decisionFunnel;
  report.summary = {
    ...report.summary,
    decision_funnel: decisionFunnel.summary,
  };

  let persistedRows = 0;
  let persistedObservationRows = 0;
  let rawDbError = null;
  if (persist) {
    try {
      if (!rawDbForPath) rawDbForPath = openRawSignalOutcomesDb();
      persistedRows = upsertRawSignalOutcomes(rawDbForPath, report.outcomes || []);
      persistedObservationRows = upsertRawSignalObservations(rawDbForPath, rawSignalObservations);
    } catch (error) {
      rawDbError = error?.message || String(error);
    }
  }
  try { if (rawDbForPath) rawDbForPath.close(); } catch {}

  return {
    available: true,
    generated_at: report.generated_at,
    raw_db_path: getRawSignalOutcomesDbPath(),
    sentiment_db_path: resolvedDbPath,
    paper_db_path: paperDbPath,
    filters: {
      since_ts: sinceTs,
      since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
      limit,
      now_ts: nowTs,
      horizon_sec: horizonSec,
      baseline_max_lag_sec: baselineMaxLagSec,
      coverage_target_pct: coverageTargetPct,
    },
    diagnostics: {
      signals: {
        rows: signalRows.length,
      },
      kline: klineDiagnostics,
      raw_path: {
        ...rawPathDiagnostics,
        durable_raw_rows: durableRawPathRows.length,
        cache_rows: klineCachePath.rows?.length || 0,
        cache_aggregated_raw_rows: klineCachePath.aggregatedRawRows?.length || 0,
        raw_bars_persisted: rawBarsPersisted,
        raw_db_error: rawPathDbError,
        kline_cache: klineCachePath.diagnostics,
        path_window: {
          since_ts: pathSinceTs,
          until_ts: pathUntilTs,
          horizon_sec: horizonSec,
          early_window_sec: 900,
        },
      },
      paper: paperDiagnostics,
      decision_evidence: decisionEvidence.diagnostics,
      raw_db: {
        persisted_rows: persistedRows,
        persisted_observation_rows: persistedObservationRows,
        error: rawDbError,
      },
    },
    report,
  };
}

function rawOutcomeEligibleSql() {
  return `
    observation_status = 'matured'
    AND COALESCE(kline_covered, 0) = 1
    AND baseline_confidence IN ('high', 'medium')
    AND COALESCE(same_source_path, 0) = 1
    AND COALESCE(outlier_flag, 0) = 0
    AND COALESCE(sustained_evaluable, 0) = 1
  `;
}

export function readRawSignalOutcomeRollingSummary({
  hours = 24,
  limit = 50,
  coverageTargetPct = 80,
  includeRows = false,
  rowsLimit = 50000,
} = {}) {
  const rawDbPath = getRawSignalOutcomesDbPath();
  const sinceTs = Math.floor(Date.now() / 1000) - Math.max(1, Number(hours) || 24) * 3600;
  if (!fs.existsSync(rawDbPath)) {
    return {
      available: false,
      db_path: rawDbPath,
      since_ts: sinceTs,
      note: 'raw_signal_outcomes durable DB missing; call /api/paper/raw-dog-discovery to build it',
    };
  }
  let db;
  try {
    db = openRawSignalOutcomesDb({ readonly: true });
    const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
    if (!tables.has('raw_signal_outcomes')) {
      return {
        available: false,
        db_path: rawDbPath,
        since_ts: sinceTs,
        note: 'raw_signal_outcomes table missing',
      };
    }
    const eligibleSql = rawOutcomeEligibleSql();
    const summary = db.prepare(`
      WITH
      rows AS (
        SELECT *
        FROM raw_signal_outcomes
        WHERE signal_ts >= @since
      ),
      eligible AS (
        SELECT *
        FROM rows
        WHERE ${eligibleSql}
      ),
      dog_events AS (
        SELECT *
        FROM eligible
        WHERE raw_primary_tier IN ('gold', 'silver')
      ),
      dog_tokens AS (
        SELECT
          token_ca,
          MAX(CASE WHEN raw_primary_tier = 'gold' THEN 1 ELSE 0 END) AS is_gold,
          MAX(CASE WHEN raw_primary_tier = 'silver' THEN 1 ELSE 0 END) AS is_silver,
          MAX(COALESCE(raw_dog_entered, 0)) AS raw_dog_entered,
          MAX(COALESCE(raw_dog_realized, 0)) AS raw_dog_realized,
          MAX(COALESCE(sold_before_silver, 0)) AS sold_before_silver,
          MAX(COALESCE(sold_before_gold, 0)) AS sold_before_gold
        FROM dog_events
        WHERE token_ca IS NOT NULL AND token_ca != ''
        GROUP BY token_ca
      ),
      eligible_tokens AS (
        SELECT DISTINCT token_ca
        FROM eligible
        WHERE token_ca IS NOT NULL AND token_ca != ''
      ),
      wick_tokens AS (
        SELECT DISTINCT token_ca
        FROM rows
        WHERE token_ca IS NOT NULL AND token_ca != ''
          AND raw_wick_tier IN ('gold', 'silver')
      ),
      wick_only_tokens AS (
        SELECT DISTINCT token_ca
        FROM rows
        WHERE token_ca IS NOT NULL AND token_ca != ''
          AND raw_wick_tier IN ('gold', 'silver')
          AND raw_primary_tier NOT IN ('gold', 'silver')
      )
      SELECT
        (SELECT COUNT(*) FROM rows) AS total_signals,
        (SELECT SUM(CASE WHEN observation_status = 'matured' THEN 1 ELSE 0 END) FROM rows) AS matured_signals,
        (SELECT SUM(CASE WHEN COALESCE(right_censored, 0) = 1 THEN 1 ELSE 0 END) FROM rows) AS right_censored_open,
        (SELECT COUNT(*) FROM eligible_tokens) AS raw_denominator_matured_only,
        (SELECT COUNT(*) FROM eligible) AS raw_denominator_event_rows,
        (SELECT COUNT(*) FROM dog_tokens) AS raw_sustained_gold_silver_unique,
        (SELECT COUNT(*) FROM dog_events) AS raw_sustained_gold_silver_event_rows,
        (SELECT SUM(CASE WHEN is_gold = 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS raw_sustained_gold_unique,
        (SELECT SUM(CASE WHEN is_silver = 1 AND is_gold != 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS raw_sustained_silver_unique,
        (SELECT COUNT(*) FROM wick_tokens) AS raw_wick_gold_silver_unique,
        (SELECT COUNT(*) FROM rows WHERE raw_wick_tier IN ('gold', 'silver')) AS raw_wick_gold_silver_event_rows,
        (SELECT COUNT(*) FROM wick_only_tokens) AS raw_wick_only_gold_silver_unique,
        (SELECT COUNT(*) FROM rows WHERE raw_wick_tier IN ('gold', 'silver') AND raw_primary_tier NOT IN ('gold', 'silver')) AS raw_wick_only_gold_silver_event_rows,
        (SELECT SUM(CASE WHEN raw_dog_entered = 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS raw_gold_silver_entered,
        (SELECT SUM(CASE WHEN raw_dog_realized = 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS raw_gold_silver_realized,
        (SELECT SUM(CASE WHEN sold_before_silver = 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS sold_before_silver,
        (SELECT SUM(CASE WHEN sold_before_gold = 1 THEN 1 ELSE 0 END) FROM dog_tokens) AS sold_before_gold
    `).get({ since: sinceTs }) || {};
    const matured = Number(summary.matured_signals || 0);
    const denominator = Number(summary.raw_denominator_matured_only || 0);
    const denominatorEvents = Number(summary.raw_denominator_event_rows || 0);
    const rawDogs = Number(summary.raw_sustained_gold_silver_unique || 0);
    const entered = Number(summary.raw_gold_silver_entered || 0);
    const realized = Number(summary.raw_gold_silver_realized || 0);
    const coveragePct = matured > 0 ? roundNumber((denominatorEvents / matured) * 100.0, 2) : null;
    let denominatorStatus = 'undefined';
    if (coveragePct != null && coveragePct < coverageTargetPct) denominatorStatus = 'evidence_unavailable';
    else if (rawDogs > 0) denominatorStatus = 'evaluable';
    const byReason = db.prepare(`
      SELECT coverage_reason, COUNT(*) AS n
      FROM raw_signal_outcomes
      WHERE signal_ts >= @since
      GROUP BY coverage_reason
      ORDER BY n DESC
    `).all({ since: sinceTs });
    const topRawDogs = db.prepare(`
      WITH ranked AS (
        SELECT
          signal_id, symbol, token_ca, signal_ts, raw_primary_tier,
          max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
          baseline_confidence, coverage_reason, did_enter, held_to_silver,
          held_to_gold, raw_dog_entered, raw_dog_realized, exit_reason,
          ROW_NUMBER() OVER (
            PARTITION BY token_ca
            ORDER BY COALESCE(max_sustained_peak_pct, 0) DESC, signal_ts DESC, signal_id DESC
          ) AS rn
        FROM raw_signal_outcomes
        WHERE signal_ts >= @since
          AND ${eligibleSql}
          AND raw_primary_tier IN ('gold', 'silver')
          AND token_ca IS NOT NULL
          AND token_ca != ''
      )
      SELECT
        signal_id, symbol, token_ca, signal_ts, raw_primary_tier,
        max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
        baseline_confidence, coverage_reason, did_enter, held_to_silver,
        held_to_gold, raw_dog_entered, raw_dog_realized, exit_reason
      FROM ranked
      WHERE rn = 1
      ORDER BY COALESCE(max_sustained_peak_pct, 0) DESC
      LIMIT @limit
    `).all({ since: sinceTs, limit });
    const rawDogRowsLimit = Math.max(1, Math.min(Number(rowsLimit) || 50000, 50000));
    const rawDogRows = includeRows ? db.prepare(`
      SELECT
        signal_id, symbol, token_ca, signal_ts, raw_primary_tier, raw_sustained_tier,
        max_sustained_peak_pct, max_wick_peak_pct, time_to_sustained_peak_sec,
        baseline_confidence, coverage_reason, did_enter, held_to_silver,
        held_to_gold, raw_dog_entered, raw_dog_realized, sold_before_silver,
        sold_before_gold, exit_reason, first_bar_ts, first_bar_lag_sec,
        early_15m_bar_count, early_15m_expected_minutes, early_15m_bar_coverage_pct,
        early_15m_complete, source_kind, source_family, path_source_kind,
        path_source_family, path_provider, path_pool_address, baseline_source,
        baseline_provider, baseline_pool_address
      FROM raw_signal_outcomes
      WHERE signal_ts >= @since
        AND ${eligibleSql}
        AND raw_primary_tier IN ('gold', 'silver')
        AND token_ca IS NOT NULL
        AND token_ca != ''
      ORDER BY signal_ts ASC, token_ca ASC, signal_id ASC
      LIMIT @limit
    `).all({ since: sinceTs, limit: rawDogRowsLimit }) : [];
    const missedRawDogs = topRawDogs.filter((row) => !row.raw_dog_realized);
    const decisionEvidence = readRawDogDecisionRecordsFromPaperDb({
      paperDbPath: getPaperDbPath(),
      rawDogs: topRawDogs,
      sinceTs,
      untilTs: Math.floor(Date.now() / 1000),
    });
    const decisionFunnel = buildRawDogDecisionFunnel({
      rawDogs: topRawDogs,
      decisionRecords: decisionEvidence.records,
    });
    return {
      available: true,
      schema_version: 'raw_signal_outcomes_rolling_summary.v1',
      db_path: rawDbPath,
      since_ts: sinceTs,
      since_iso: new Date(sinceTs * 1000).toISOString(),
      coverage_target_pct: coverageTargetPct,
      summary: {
        total_signals: Number(summary.total_signals || 0),
        matured_signals: matured,
        right_censored_open: Number(summary.right_censored_open || 0),
        raw_denominator_matured_only: denominator,
        raw_denominator_event_rows: denominatorEvents,
        raw_kline_coverage_pct: coveragePct,
        raw_sustained_gold_unique: Number(summary.raw_sustained_gold_unique || 0),
        raw_sustained_silver_unique: Number(summary.raw_sustained_silver_unique || 0),
        raw_sustained_gold_silver_unique: rawDogs,
        raw_sustained_gold_silver_event_rows: Number(summary.raw_sustained_gold_silver_event_rows || 0),
        raw_wick_gold_silver_unique: Number(summary.raw_wick_gold_silver_unique || 0),
        raw_wick_gold_silver_event_rows: Number(summary.raw_wick_gold_silver_event_rows || 0),
        raw_wick_only_gold_silver_unique: Number(summary.raw_wick_only_gold_silver_unique || 0),
        raw_wick_only_gold_silver_event_rows: Number(summary.raw_wick_only_gold_silver_event_rows || 0),
        raw_gold_silver_entered: entered,
        raw_gold_silver_realized: realized,
        raw_dog_entered_rate: rawDogs ? roundNumber(entered / rawDogs, 4) : null,
        raw_dog_realized_rate: rawDogs ? roundNumber(realized / rawDogs, 4) : null,
        sold_before_silver: Number(summary.sold_before_silver || 0),
        sold_before_gold: Number(summary.sold_before_gold || 0),
        denominator_status: denominatorStatus,
        decision_funnel: decisionFunnel.summary,
      },
      coverage: {
        by_reason: byReason,
      },
      decision_funnel: decisionFunnel,
      decision_evidence: decisionEvidence.diagnostics,
      top_raw_dogs: topRawDogs,
      missed_raw_dogs: missedRawDogs.slice(0, limit),
      raw_dogs: rawDogRows,
      raw_dog_rows: {
        included: Boolean(includeRows),
        limit: includeRows ? rawDogRowsLimit : 0,
        loaded_event_rows: rawDogRows.length,
        rows_complete_against_summary: includeRows
          ? rawDogRows.length >= Number(summary.raw_sustained_gold_silver_event_rows || 0)
          : null,
      },
      notes: {
        capture_definition: 'raw_dog_entered is separate from raw_dog_realized; the goal capture metric must use raw_dog_realized.',
        source: 'durable raw_signal_outcomes DB, not paper_trades.db; this survives paper DB quarantine/reset.',
      },
    };
  } catch (error) {
    return {
      available: false,
      db_path: rawDbPath,
      since_ts: sinceTs,
      error: error?.message || String(error),
    };
  } finally {
    try { if (db) db.close(); } catch {}
  }
}

export function buildRawDogDiscoveryApiPayloadFromRollingSummary(snapshot = {}, options = {}) {
  const limit = Math.max(1, Math.min(Number(options.limit) || 50, 500));
  const includeRows = Boolean(options.includeRows);
  const rowsLimit = Math.max(1, Math.min(Number(options.rowsLimit) || 50000, 50000));
  const requestedHours = Math.max(1, Math.min(Number(options.hours) || 24, 168));
  const topRawDogs = Array.isArray(snapshot.top_raw_dogs) ? snapshot.top_raw_dogs.slice(0, limit) : [];
  const missedRawDogs = Array.isArray(snapshot.missed_raw_dogs) ? snapshot.missed_raw_dogs.slice(0, limit) : [];
  const rawDogs = includeRows && Array.isArray(snapshot.raw_dogs)
    ? snapshot.raw_dogs.slice(0, rowsLimit)
    : [];
  const expectedRawDogEvents = Number(snapshot.summary?.raw_sustained_gold_silver_event_rows || 0);
  return {
    schema_version: 'raw_dog_discovery_api.v1',
    generated_at: snapshot.generated_at || options.generatedAt || new Date().toISOString(),
    materialized: true,
    live_query: false,
    source: options.source || 'raw_dog_discovery_static_snapshot',
    requested_hours: requestedHours,
    coverage_target_pct: snapshot.coverage_target_pct ?? Number(options.coverageTargetPct ?? 80),
    snapshot_path: options.snapshotPath || null,
    snapshot_written_at: options.snapshotWrittenAt || null,
    available: Boolean(snapshot.available),
    db_path: snapshot.db_path || null,
    since_ts: snapshot.since_ts ?? null,
    since_iso: snapshot.since_iso || (snapshot.since_ts ? new Date(Number(snapshot.since_ts) * 1000).toISOString() : null),
    summary: snapshot.summary || null,
    decision_funnel: snapshot.decision_funnel || snapshot.summary?.decision_funnel || null,
    decision_evidence: snapshot.decision_evidence || null,
    coverage: snapshot.coverage || null,
    top_raw_dogs: topRawDogs,
    missed_raw_dogs: missedRawDogs,
    raw_dogs: rawDogs,
    raw_dog_rows: snapshot.raw_dog_rows
      ? {
          ...snapshot.raw_dog_rows,
          included: includeRows,
          returned_event_rows: rawDogs.length,
          rows_complete_against_summary: includeRows
            ? rawDogs.length >= expectedRawDogEvents
            : null,
        }
      : {
          included: includeRows,
          returned_event_rows: rawDogs.length,
          rows_complete_against_summary: includeRows
            ? rawDogs.length >= expectedRawDogEvents
            : null,
        },
    coverage_gap_tokens: [],
    pending_outcomes: [],
    error: snapshot.error || null,
    note: snapshot.note || null,
    notes: {
      ...(snapshot.notes || {}),
      live_query: 'Default is a static worker snapshot. Pass live=1 for a bounded live rebuild (max 2h / 1000 rows).',
      snapshot: 'Served from the isolated raw dog discovery worker output; no request-time raw/paper DB aggregation.',
    },
  };
}

export function writeRawDogDiscoveryApiSnapshot(payload = {}, options = {}) {
  const snapshotPath = getRawDogDiscoveryApiSnapshotPath(options);
  fs.mkdirSync(dirname(snapshotPath), { recursive: true });
  const tmpPath = `${snapshotPath}.${process.pid}.${Date.now()}.tmp`;
  const output = {
    ...payload,
    snapshot_path: snapshotPath,
    snapshot_written_at: payload.snapshot_written_at || new Date().toISOString(),
  };
  fs.writeFileSync(tmpPath, `${JSON.stringify(output, null, 2)}\n`);
  fs.renameSync(tmpPath, snapshotPath);
  return { path: snapshotPath, bytes: fs.statSync(snapshotPath).size, payload: output };
}

export function readRawDogDiscoveryApiSnapshot(options = {}) {
  const requestedHours = Math.max(1, Math.min(Number(options.hours) || 24, 168));
  const coverageTargetPct = Number(options.coverageTargetPct ?? 80);
  const limit = Math.max(1, Math.min(Number(options.limit) || 50, 500));
  const includeRows = Boolean(options.includeRows);
  const rowsLimit = Math.max(1, Math.min(Number(options.rowsLimit) || 50000, 50000));
  const snapshotPath = getRawDogDiscoveryApiSnapshotPath(options);
  const base = {
    schema_version: 'raw_dog_discovery_api.v1',
    materialized: true,
    live_query: false,
    source: 'raw_dog_discovery_static_snapshot',
    requested_hours: requestedHours,
    coverage_target_pct: coverageTargetPct,
    snapshot_path: snapshotPath,
    summary: null,
    decision_funnel: null,
    coverage: null,
    top_raw_dogs: [],
    missed_raw_dogs: [],
    raw_dogs: [],
    raw_dog_rows: {
      included: includeRows,
      returned_event_rows: 0,
    },
    coverage_gap_tokens: [],
    pending_outcomes: [],
  };
  try {
    if (!fs.existsSync(snapshotPath)) {
      return {
        ...base,
        available: false,
        error_code: 'raw_dog_discovery_snapshot_missing',
        note: 'raw dog discovery static snapshot missing; wait for the isolated worker or pass live=1 for a bounded live rebuild.',
      };
    }
    const stat = fs.statSync(snapshotPath);
    const payload = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
    const payloadHours = Number(payload.requested_hours || requestedHours);
    if (Number.isFinite(payloadHours) && payloadHours !== requestedHours) {
      return {
        ...base,
        available: false,
        error_code: 'raw_dog_discovery_snapshot_window_mismatch',
        snapshot_written_at: payload.snapshot_written_at || null,
        generated_at: payload.generated_at || null,
        note: `static snapshot window is ${payloadHours}h, requested ${requestedHours}h`,
      };
    }
    const ageSec = Math.max(0, Math.floor((Date.now() - stat.mtimeMs) / 1000));
    const topRawDogs = Array.isArray(payload.top_raw_dogs) ? payload.top_raw_dogs.slice(0, limit) : [];
    const missedRawDogs = Array.isArray(payload.missed_raw_dogs) ? payload.missed_raw_dogs.slice(0, limit) : [];
    const rawDogs = includeRows && Array.isArray(payload.raw_dogs) ? payload.raw_dogs.slice(0, rowsLimit) : [];
    const expectedRawDogEvents = Number(payload.summary?.raw_sustained_gold_silver_event_rows || 0);
    return {
      ...base,
      ...payload,
      source: payload.source || 'raw_dog_discovery_static_snapshot',
      requested_hours: requestedHours,
      coverage_target_pct: payload.coverage_target_pct ?? coverageTargetPct,
      snapshot_path: snapshotPath,
      snapshot_age_sec: ageSec,
      top_raw_dogs: topRawDogs,
      missed_raw_dogs: missedRawDogs,
      raw_dogs: rawDogs,
      raw_dog_rows: payload.raw_dog_rows
        ? {
            ...payload.raw_dog_rows,
            included: includeRows,
            returned_event_rows: rawDogs.length,
            rows_complete_against_summary: includeRows
              ? rawDogs.length >= expectedRawDogEvents
              : null,
          }
        : {
            included: includeRows,
            returned_event_rows: rawDogs.length,
            rows_complete_against_summary: includeRows
              ? rawDogs.length >= expectedRawDogEvents
              : null,
          },
      coverage_gap_tokens: [],
      pending_outcomes: [],
      notes: {
        ...(payload.notes || {}),
        live_query: 'Default is a static worker snapshot. Pass live=1 for a bounded live rebuild (max 2h / 1000 rows).',
      },
    };
  } catch (error) {
    return {
      ...base,
      available: false,
      error_code: 'raw_dog_discovery_snapshot_read_failed',
      error: error?.message || String(error),
    };
  }
}

function envBool(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw == null || raw === '') return Boolean(defaultValue);
  const value = String(raw).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(value)) return true;
  if (['0', 'false', 'no', 'off'].includes(value)) return false;
  return Boolean(defaultValue);
}

function envInt(name, defaultValue, minValue, maxValue) {
  const raw = parseInt(String(process.env[name] ?? defaultValue), 10);
  const value = Number.isFinite(raw) ? raw : defaultValue;
  return Math.max(minValue, Math.min(maxValue, value));
}

let rawDogDiscoveryObserverTimer = null;
let rawDogDiscoveryObserverBusy = false;
const rawDogDiscoveryObserverState = {
  schema_version: 'raw_dog_discovery_observer.v1',
  enabled: false,
  running: false,
  interval_sec: null,
  window_hours: null,
  limit: null,
  last_started_at: null,
  last_completed_at: null,
  last_duration_ms: null,
  last_persisted_rows: null,
  last_summary: null,
  last_diagnostics: null,
  error_count: 0,
  last_error: null,
};

function rawDogDiscoveryObserverStatus() {
  return {
    ...rawDogDiscoveryObserverState,
    running: Boolean(rawDogDiscoveryObserverTimer),
    busy: Boolean(rawDogDiscoveryObserverBusy),
  };
}

function startRawDogDiscoveryObserver() {
  if (rawDogDiscoveryObserverTimer) return rawDogDiscoveryObserverStatus();
  if (!envBool('RAW_DOG_DISCOVERY_OBSERVER_ENABLED', false)) {
    rawDogDiscoveryObserverState.enabled = false;
    rawDogDiscoveryObserverState.last_error = 'disabled_by_RAW_DOG_DISCOVERY_OBSERVER_ENABLED';
    return rawDogDiscoveryObserverStatus();
  }
  const intervalSec = envInt('RAW_DOG_DISCOVERY_OBSERVER_INTERVAL_SEC', 300, 60, 3600);
  const initialDelaySec = envInt('RAW_DOG_DISCOVERY_OBSERVER_INITIAL_DELAY_SEC', 20, 0, 300);
  const windowHours = envInt('RAW_DOG_DISCOVERY_OBSERVER_WINDOW_HOURS', 24, 1, 168);
  const limit = envInt('RAW_DOG_DISCOVERY_OBSERVER_LIMIT', 20000, 100, 50000);
  const horizonSec = envInt('RAW_DOG_DISCOVERY_OBSERVER_HORIZON_SEC', 7200, 300, 24 * 3600);
  const baselineMaxLagSec = envInt('RAW_DOG_DISCOVERY_OBSERVER_BASELINE_MAX_LAG_SEC', 300, 0, 3600);
  const coverageTargetPct = envInt('RAW_DOG_DISCOVERY_OBSERVER_COVERAGE_TARGET_PCT', 80, 0, 100);
  rawDogDiscoveryObserverState.enabled = true;
  rawDogDiscoveryObserverState.interval_sec = intervalSec;
  rawDogDiscoveryObserverState.window_hours = windowHours;
  rawDogDiscoveryObserverState.limit = limit;

  const runOnce = () => {
    if (rawDogDiscoveryObserverBusy) return;
    rawDogDiscoveryObserverBusy = true;
    const started = Date.now();
    const nowTs = Math.floor(started / 1000);
    rawDogDiscoveryObserverState.last_started_at = new Date(started).toISOString();
    rawDogDiscoveryObserverState.last_error = null;
    try {
      const signalDb = getDb();
      const snapshot = buildRawDogDiscoverySnapshot({
        signalDb,
        sinceTs: nowTs - windowHours * 3600,
        limit,
        nowTs,
        horizonSec,
        baselineMaxLagSec,
        coverageTargetPct,
        persist: true,
      });
      rawDogDiscoveryObserverState.last_completed_at = new Date().toISOString();
      rawDogDiscoveryObserverState.last_duration_ms = Date.now() - started;
      rawDogDiscoveryObserverState.last_persisted_rows = snapshot.diagnostics?.raw_db?.persisted_rows ?? null;
      rawDogDiscoveryObserverState.last_summary = snapshot.report?.summary || null;
      rawDogDiscoveryObserverState.last_diagnostics = snapshot.diagnostics || null;
      if (!snapshot.available || snapshot.diagnostics?.raw_db?.error) {
        rawDogDiscoveryObserverState.error_count += 1;
        rawDogDiscoveryObserverState.last_error = snapshot.error || snapshot.diagnostics?.raw_db?.error || 'raw_discovery_snapshot_unavailable';
      }
      console.log(`[RAW_DOG_DISCOVERY_OBSERVER] persisted=${rawDogDiscoveryObserverState.last_persisted_rows ?? 'n/a'} coverage=${rawDogDiscoveryObserverState.last_summary?.raw_kline_coverage_pct ?? 'n/a'} status=${rawDogDiscoveryObserverState.last_summary?.denominator_status || 'unknown'}`);
    } catch (error) {
      rawDogDiscoveryObserverState.error_count += 1;
      rawDogDiscoveryObserverState.last_error = error?.message || String(error);
      rawDogDiscoveryObserverState.last_completed_at = new Date().toISOString();
      rawDogDiscoveryObserverState.last_duration_ms = Date.now() - started;
      console.warn(`[RAW_DOG_DISCOVERY_OBSERVER] failed: ${rawDogDiscoveryObserverState.last_error}`);
    } finally {
      rawDogDiscoveryObserverBusy = false;
    }
  };

  const initial = setTimeout(runOnce, initialDelaySec * 1000);
  if (typeof initial.unref === 'function') initial.unref();
  rawDogDiscoveryObserverTimer = setInterval(runOnce, intervalSec * 1000);
  if (typeof rawDogDiscoveryObserverTimer.unref === 'function') rawDogDiscoveryObserverTimer.unref();
  return rawDogDiscoveryObserverStatus();
}

function fileInfo(label, filePath) {
  try {
    const stats = fs.statSync(filePath);
    return {
      label,
      path: filePath,
      exists: true,
      size_bytes: stats.size,
      size_mb: Math.round((stats.size / (1024 * 1024)) * 100) / 100,
      mtime: stats.mtime.toISOString(),
    };
  } catch (error) {
    return {
      label,
      path: filePath,
      exists: false,
      error: error?.code === 'ENOENT' ? null : error.message,
    };
  }
}

function readTinyText(filePath, maxBytes = 2000) {
  try {
    if (!fs.existsSync(filePath)) return null;
    const stats = fs.statSync(filePath);
    const fd = fs.openSync(filePath, 'r');
    try {
      const length = Math.min(maxBytes, stats.size);
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, Math.max(0, stats.size - length));
      return buffer.toString('utf8');
    } finally {
      fs.closeSync(fd);
    }
  } catch {
    return null;
  }
}

function sqliteDownloadUsesBackup(url) {
  return !['0', 'false', 'raw'].includes(String(url.searchParams.get('backup') || '1').toLowerCase());
}

function sqliteDbFamilySizeBytes(sourcePath) {
  let total = 0;
  for (const path of [sourcePath, `${sourcePath}-wal`, `${sourcePath}-shm`]) {
    try {
      if (fs.existsSync(path)) total += fs.statSync(path).size;
    } catch {}
  }
  return total;
}

function sqliteSnapshotSpaceRequirementBytes(sourcePath) {
  const familyBytes = sqliteDbFamilySizeBytes(sourcePath);
  const multiplier = Number(process.env.SQLITE_DOWNLOAD_SPACE_MULTIPLIER || 1.35);
  const minReserveMb = Number(process.env.SQLITE_DOWNLOAD_MIN_FREE_MB || 256);
  return Math.ceil(familyBytes * Math.max(1.0, multiplier) + minReserveMb * 1024 * 1024);
}

function assertSqliteDownloadSpace(sourcePath, snapshotDir, options = {}) {
  if (options.skipSpaceCheck) return null;
  if (typeof fs.statfsSync !== 'function') return null;
  let stats;
  try {
    stats = fs.statfsSync(snapshotDir);
  } catch (error) {
    const err = new Error(`snapshot space check failed: ${error.message}`);
    err.statusCode = 507;
    throw err;
  }
  const free = Number(stats.bavail || stats.bfree || 0) * Number(stats.bsize || 0);
  const required = sqliteSnapshotSpaceRequirementBytes(sourcePath);
  if (free > 0 && free < required) {
    const err = new Error(
      `insufficient snapshot space: free_mb=${Math.round((free / (1024 * 1024)) * 100) / 100} ` +
      `required_mb=${Math.round((required / (1024 * 1024)) * 100) / 100}`
    );
    err.statusCode = 507;
    err.detail = {
      snapshot_dir: snapshotDir,
      free_bytes: free,
      required_bytes: required,
      sqlite_family_bytes: sqliteDbFamilySizeBytes(sourcePath),
    };
    throw err;
  }
  return {
    snapshot_dir: snapshotDir,
    free_bytes: free,
    required_bytes: required,
    sqlite_family_bytes: sqliteDbFamilySizeBytes(sourcePath),
  };
}

function tempSqliteDownloadPath(prefix) {
  const safePrefix = String(prefix || 'sqlite_download').replace(/[^a-z0-9_-]/gi, '_').slice(0, 48);
  const tmpDir = process.env.SQLITE_DOWNLOAD_TMP_DIR || '/tmp';
  try { fs.mkdirSync(tmpDir, { recursive: true }); } catch {}
  return join(tmpDir, `${safePrefix}_${process.pid}_${Date.now()}_${randomUUID()}.db`);
}

async function createSqliteDownloadSnapshot(sourcePath, prefix, options = {}) {
  if (!fs.existsSync(sourcePath)) {
    const error = new Error(`${options.label || 'SQLite database'} not found`);
    error.statusCode = 404;
    throw error;
  }
  const timeout = Number(options.timeoutMs || 10000);
  let sourceDb;
  let snapshotPath = null;
  try {
    sourceDb = openDashboardSqlite(sourcePath, { readonly: true, fileMustExist: true, timeout });
    try { sourceDb.pragma(`busy_timeout = ${timeout}`); } catch {}
    try { sourceDb.pragma('wal_checkpoint(PASSIVE)'); } catch {}
    if (typeof sourceDb.backup !== 'function') {
      return {
        path: sourcePath,
        cleanupPath: null,
        mode: 'raw',
        note: 'better-sqlite3 backup() unavailable; streamed raw main DB file',
      };
    }
    snapshotPath = tempSqliteDownloadPath(prefix);
    const snapshotDir = dirname(snapshotPath);
    const space = assertSqliteDownloadSpace(sourcePath, snapshotDir, {
      skipSpaceCheck: options.skipSpaceCheck,
    });
    await sourceDb.backup(snapshotPath);
    return {
      path: snapshotPath,
      cleanupPath: snapshotPath,
      mode: 'sqlite_backup_snapshot',
      note: 'WAL-safe SQLite backup snapshot',
      space,
    };
  } catch (error) {
    try { if (snapshotPath && fs.existsSync(snapshotPath)) fs.unlinkSync(snapshotPath); } catch {}
    throw error;
  } finally {
    try { if (sourceDb) sourceDb.close(); } catch {}
  }
}

function streamDownloadFile(res, filePath, filename, cleanupPath = null, extraHeaders = {}) {
  const stats = fs.statSync(filePath);
  res.writeHead(200, {
    'Content-Type': 'application/octet-stream',
    'Content-Disposition': `attachment; filename="${filename}"`,
    'Content-Length': stats.size,
    ...extraHeaders,
  });
  const fileStream = fs.createReadStream(filePath);
  const cleanup = () => {
    try { if (cleanupPath && fs.existsSync(cleanupPath)) fs.unlinkSync(cleanupPath); } catch {}
  };
  fileStream.on('close', cleanup);
  fileStream.on('error', cleanup);
  res.on('close', cleanup);
  fileStream.pipe(res);
}

async function downloadSqliteDatabase(req, res, url, sourcePath, filename, label, prefix) {
  if (!checkAuth(req, url, res)) return false;
  if (!fs.existsSync(sourcePath)) {
    res.writeHead(404, apiJsonHeaders());
    res.end(JSON.stringify({ error: `${label} not found`, path: sourcePath }));
    return true;
  }
  try {
    let download = { path: sourcePath, cleanupPath: null, mode: 'raw', note: 'raw main DB file' };
    if (sqliteDownloadUsesBackup(url)) {
      download = await createSqliteDownloadSnapshot(sourcePath, prefix, {
        label,
        timeoutMs: boundedIntParam(url, 'backup_timeout_ms', 10000, 1000, 60000),
        skipSpaceCheck: ['1', 'true', 'yes'].includes(String(url.searchParams.get('skip_space_check') || '').toLowerCase()),
      });
    }
    streamDownloadFile(res, download.path, filename, download.cleanupPath, {
      'X-SQLite-Download-Mode': download.mode,
      'X-SQLite-Download-Note': download.note,
      'X-SQLite-Source-Path': sourcePath,
      'X-SQLite-Family-Bytes': String(download.space?.sqlite_family_bytes ?? sqliteDbFamilySizeBytes(sourcePath)),
      'X-SQLite-Snapshot-Required-Bytes': String(download.space?.required_bytes ?? ''),
    });
  } catch (error) {
    res.writeHead(error.statusCode || 500, apiJsonHeaders());
    res.end(JSON.stringify({
      error: `${label} backup failed: ${error.message}`,
      path: sourcePath,
      detail: error.detail || null,
      storage_health: buildStorageHealthSnapshot({ includeDisk: true, includeFileStats: true }),
    }));
  }
  return true;
}

export function incidentArtifactRoots(options = {}) {
  const root = options.projectRoot || projectRoot;
  const dataDir = options.dataDir || process.env.ZEABUR_DATA_DIR || join(root, 'data');
  return {
    backup: resolve(options.backupDir || process.env.ZEABUR_PAPER_DB_BACKUP_DIR || join(dataDir, 'backup', 'paper-db-family')),
    recovery: resolve(options.recoveryDir || process.env.ZEABUR_RECOVERY_DIR || join(dataDir, 'recovery')),
    evidence: resolve(options.evidenceDir || process.env.PAPER_EVIDENCE_LOG_DIR || join(dataDir, 'paper_evidence_log')),
  };
}

export function resolveIncidentArtifactPath(scope, requestedPath, options = {}) {
  const scopeKey = String(scope || '').trim().toLowerCase();
  const roots = incidentArtifactRoots(options);
  const root = roots[scopeKey];
  if (!root) {
    return {
      ok: false,
      statusCode: 400,
      error: 'Invalid incident artifact scope',
      error_code: 'invalid_incident_artifact_scope',
      allowed_scopes: Object.keys(roots),
    };
  }
  const rawPath = String(requestedPath || '').trim();
  if (!rawPath) {
    return {
      ok: false,
      statusCode: 400,
      error: 'Missing incident artifact path',
      error_code: 'missing_incident_artifact_path',
    };
  }
  if (rawPath.includes('\0')) {
    return {
      ok: false,
      statusCode: 400,
      error: 'Invalid incident artifact path',
      error_code: 'invalid_incident_artifact_path',
    };
  }
  const artifactPath = resolve(root, rawPath);
  const relativePath = relative(root, artifactPath);
  if (relativePath.startsWith('..') || isAbsolute(relativePath)) {
    return {
      ok: false,
      statusCode: 403,
      error: 'Incident artifact path escapes allowed root',
      error_code: 'incident_artifact_path_outside_root',
    };
  }
  return {
    ok: true,
    scope: scopeKey,
    root,
    path: artifactPath,
    relative_path: relativePath,
  };
}

function incidentArtifactContentType(filePath) {
  const lower = String(filePath || '').toLowerCase();
  if (lower.endsWith('.json')) return 'application/json; charset=utf-8';
  if (lower.endsWith('.jsonl') || lower.endsWith('.log') || lower.endsWith('.txt') || lower.endsWith('.integrity_error')) {
    return 'text/plain; charset=utf-8';
  }
  return 'application/octet-stream';
}

function artifactItem(scope, root, artifactPath, stats, options = {}) {
  const relativePath = relative(root, artifactPath);
  const item = {
    scope,
    relative_path: relativePath,
    path: artifactPath,
    type: stats.isDirectory() ? 'directory' : stats.isFile() ? 'file' : stats.isSymbolicLink() ? 'symlink' : 'other',
    size_bytes: stats.isFile() ? stats.size : null,
    size_mb: stats.isFile() ? Math.round((stats.size / (1024 * 1024)) * 100) / 100 : null,
    mtime: stats.mtime.toISOString(),
  };
  if (stats.isFile()) {
    item.download_path = `/api/paper/incident-artifact/download?scope=${encodeURIComponent(scope)}&path=${encodeURIComponent(relativePath)}`;
    const lower = artifactPath.toLowerCase();
    if (options.includePreviews && (
      lower.endsWith('.json')
      || lower.endsWith('.jsonl')
      || lower.endsWith('.log')
      || lower.endsWith('.txt')
      || lower.endsWith('.integrity_error')
    )) {
      item.text_preview = readTinyText(artifactPath, options.previewBytes || 2000);
    }
  }
  return item;
}

export function buildIncidentArtifactSnapshot(options = {}) {
  const roots = incidentArtifactRoots(options);
  const requestedScope = String(options.scope || 'all').trim().toLowerCase();
  const scopes = requestedScope === 'all'
    ? Object.keys(roots)
    : Object.keys(roots).filter((scope) => scope === requestedScope);
  const maxFiles = Math.max(1, Math.min(Number(options.maxFiles || 80) || 80, 500));
  const maxDepth = Math.max(0, Math.min(Number(options.maxDepth || 5) || 5, 8));
  const includePreviews = Boolean(options.includePreviews);
  const previewBytes = Math.max(1, Math.min(Number(options.previewBytes || 2000) || 2000, 8000));
  const result = {
    generated_at: new Date().toISOString(),
    artifact_roots: {},
    requested_scope: requestedScope,
    max_files: maxFiles,
    max_depth: maxDepth,
    items: [],
    truncated: false,
  };

  if (!scopes.length) {
    result.error = 'Invalid incident artifact scope';
    result.error_code = 'invalid_incident_artifact_scope';
    result.allowed_scopes = Object.keys(roots);
    return result;
  }

  for (const scope of scopes) {
    const root = roots[scope];
    result.artifact_roots[scope] = fileInfo(scope, root);
    if (!fs.existsSync(root)) continue;
    const stack = [{ dir: root, depth: 0 }];
    while (stack.length && result.items.length < maxFiles) {
      const current = stack.shift();
      let entries = [];
      try {
        entries = fs.readdirSync(current.dir, { withFileTypes: true })
          .sort((a, b) => a.name.localeCompare(b.name));
      } catch (error) {
        result.items.push({
          scope,
          relative_path: relative(root, current.dir),
          path: current.dir,
          type: 'directory',
          error: error.message,
        });
        continue;
      }
      for (const entry of entries) {
        if (result.items.length >= maxFiles) {
          result.truncated = true;
          break;
        }
        const artifactPath = join(current.dir, entry.name);
        let stats;
        try {
          stats = fs.lstatSync(artifactPath);
        } catch (error) {
          result.items.push({
            scope,
            relative_path: relative(root, artifactPath),
            path: artifactPath,
            type: 'unknown',
            error: error.message,
          });
          continue;
        }
        result.items.push(artifactItem(scope, root, artifactPath, stats, {
          includePreviews,
          previewBytes,
        }));
        if (stats.isDirectory() && current.depth < maxDepth) {
          stack.push({ dir: artifactPath, depth: current.depth + 1 });
        }
      }
    }
    if (result.items.length >= maxFiles) {
      result.truncated = true;
      break;
    }
  }

  return result;
}

function paperFastLaneHealthPath(env = process.env) {
  const raw = env.PAPER_FAST_LANE_HEALTH_PATH || './data/paper-fast-lane-health.json';
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function signalSourceFreshnessHealthPath(env = process.env) {
  const readModelDir = env.V27_READ_MODEL_DIR || './data/v27_read_models';
  const raw = env.SIGNAL_SOURCE_FRESHNESS_HEALTH_PATH || join(readModelDir, 'signal_source_freshness.json');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function runtimeFinalEvidencePath(env = process.env) {
  const raw = env.RUNTIME_FINAL_EVIDENCE_LOG || '/app/data/runtime_final_evidence.jsonl';
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

export function readRuntimeFinalEvidenceHealth(options = {}) {
  const env = options.env || process.env;
  const evidencePath = options.evidencePath || runtimeFinalEvidencePath(env);
  const configured = Boolean(env.RUNTIME_FINAL_EVIDENCE_LOG);
  const parent = dirname(evidencePath);
  const health = {
    available: false,
    configured,
    path: evidencePath,
    parent_exists: false,
    status: 'runtime_final_evidence_missing',
  };
  try {
    health.parent_exists = fs.existsSync(parent);
    if (!fs.existsSync(evidencePath)) return health;
    const stats = fs.statSync(evidencePath);
    health.available = true;
    health.status = 'ok';
    health.size_bytes = stats.size;
    health.mtime = stats.mtime.toISOString();
    return health;
  } catch (error) {
    return {
      ...health,
      status: 'runtime_final_evidence_stat_failed',
      error: error?.message || String(error),
    };
  }
}

export function readSignalSourceFreshnessHealth(options = {}) {
  const healthPath = options.healthPath || signalSourceFreshnessHealthPath(options.env || process.env);
  try {
    if (!fs.existsSync(healthPath)) {
      return {
        available: false,
        path: healthPath,
        status: 'signal_source_freshness_health_missing',
      };
    }
    const payload = JSON.parse(fs.readFileSync(healthPath, 'utf8'));
    return {
      available: true,
      path: healthPath,
      status: payload?.status || 'unknown',
      fail_closed: Boolean(payload?.fail_closed),
      schema_version: payload?.schema_version || null,
      generated_at: payload?.generated_at || null,
      generated_at_iso: payload?.generated_at_iso || null,
      source: payload?.source || null,
      source_note: payload?.source_note || null,
      sentiment_db_path: payload?.sentiment_db_path || null,
      latest_ts: payload?.latest_ts || null,
      latest_iso: payload?.latest_iso || null,
      age_minutes: payload?.age_minutes ?? null,
      total: Number(payload?.total || 0),
      warn_after_minutes: payload?.warn_after_minutes ?? null,
      fail_closed_after_minutes: payload?.fail_closed_after_minutes ?? null,
      entry_action: payload?.entry_action || null,
    };
  } catch (error) {
    return {
      available: false,
      path: healthPath,
      status: 'signal_source_freshness_health_parse_failed',
      error: error?.message || String(error),
    };
  }
}

export function readPaperFastLaneHealth(options = {}) {
  const env = options.env || process.env;
  const healthPath = options.healthPath || paperFastLaneHealthPath(env);
  const sourceShadowWorkersEnabled = envFlagValue(
    env.INDEX_RUNTIME_CHILD_SOURCE_SHADOW_WORKERS_ENABLED ?? env.SOURCE_SHADOW_WORKERS_ENABLED,
    false
  );
  const paperDbWriteSidecarsEnabled = envFlagValue(env.PAPER_DB_WRITE_SIDECARS_ENABLED, false);
  const paperFastLaneEnabled = paperDbWriteSidecarsEnabled && envFlagValue(env.PAPER_FAST_LANE_ENABLED, false);
  const required = options.required === undefined
    ? envFlagValue(env.PAPER_FAST_LANE_HEALTH_REQUIRED, sourceShadowWorkersEnabled && paperFastLaneEnabled)
    : Boolean(options.required);
  const maxAgeMinutes = Math.max(
    1,
    Math.min(
      1440,
      Number.parseInt(String(options.maxAgeMinutes ?? process.env.PAPER_FAST_LANE_HEALTH_MAX_AGE_MINUTES ?? '30'), 10) || 30
    )
  );
  const nowMs = Number.isFinite(Number(options.nowMs)) ? Number(options.nowMs) : Date.now();
  try {
    if (!fs.existsSync(healthPath)) {
      return {
        available: false,
        path: healthPath,
        status: 'paper_fast_lane_health_missing',
        required,
      };
    }
    const payload = JSON.parse(fs.readFileSync(healthPath, 'utf8'));
    const heartbeatAt = payload?.updated_at || payload?.missed_rescue?.last_scan_at || null;
    const ageMinutes = heartbeatAt ? snapshotAgeMinutes({ generated_at: heartbeatAt }, nowMs) : null;
    const stale = ageMinutes == null || ageMinutes > maxAgeMinutes;
    const hasScanError = Boolean(payload?.missed_rescue?.last_error);
    return {
      available: true,
      path: healthPath,
      status: hasScanError
        ? 'paper_fast_lane_scan_error'
        : (stale ? 'paper_fast_lane_health_stale_or_undated' : 'ok'),
      required,
      schema_version: payload?.schema_version || null,
      updated_at: payload?.updated_at || null,
      heartbeat_at: heartbeatAt,
      age_minutes: ageMinutes,
      max_age_minutes: maxAgeMinutes,
      fresh: !stale,
      paper_db_exists: payload?.paper_db_exists ?? null,
      worker_state: payload?.worker_state || null,
      missed_rescue: {
        last_scan_at: payload?.missed_rescue?.last_scan_at || null,
        scan_count: Number(payload?.missed_rescue?.scan_count || 0),
        error_count: Number(payload?.missed_rescue?.error_count || 0),
        last_result: payload?.missed_rescue?.last_result || {},
        last_error: payload?.missed_rescue?.last_error || null,
      },
    };
  } catch (error) {
    return {
      available: false,
      path: healthPath,
      status: 'paper_fast_lane_health_parse_failed',
      error: error?.message || String(error),
    };
  }
}

export function readPaperDbRuntimeHealth(options = {}) {
  const paperDbPath = options.paperDbPath || getPaperDbPath();
  const markerPath = options.integrityMarkerPath || `${paperDbPath}.integrity_error`;
  const health = {
    available: false,
    path: paperDbPath,
    status: 'paper_db_missing',
    integrity_marker: {
      exists: false,
      path: markerPath,
    },
  };
  try {
    if (fs.existsSync(paperDbPath)) {
      const stats = fs.statSync(paperDbPath);
      health.available = true;
      health.status = 'ok';
      health.size_bytes = stats.size;
      health.size_mb = Math.round((stats.size / (1024 * 1024)) * 100) / 100;
      health.mtime = stats.mtime.toISOString();
      if (stats.size === 0) {
        health.status = 'paper_db_empty';
        health.reason = 'paper_trades_db_zero_bytes';
      } else {
        try {
          const fd = fs.openSync(paperDbPath, 'r');
          try {
            const header = Buffer.alloc(16);
            fs.readSync(fd, header, 0, 16, 0);
            if (!header.equals(Buffer.from('SQLite format 3\0', 'binary'))) {
              health.status = 'paper_db_invalid_sqlite_header';
              health.reason = 'paper_trades_db_header_not_sqlite';
            }
          } finally {
            fs.closeSync(fd);
          }
        } catch (headerError) {
          health.status = 'paper_db_header_check_failed';
          health.reason = headerError?.message || String(headerError);
        }
      }
    }
    if (fs.existsSync(markerPath)) {
      const markerStats = fs.statSync(markerPath);
      health.status = 'paper_db_integrity_marker_present';
      health.integrity_marker = {
        exists: true,
        path: markerPath,
        size_bytes: markerStats.size,
        mtime: markerStats.mtime.toISOString(),
        text_preview: readTinyText(markerPath, 2000),
      };
    }
    return health;
  } catch (error) {
    return {
      ...health,
      status: 'paper_db_runtime_health_failed',
      error: error?.message || String(error),
    };
  }
}

function paperDbHealthIsUsable(health) {
  return Boolean(health && health.available && health.status === 'ok');
}

export function resolveDashboardLogPath(pathname, env = process.env) {
  const logPathByEndpoint = {
    '/api/logs/node': env.NODE_RUNTIME_LOG_PATH || '/app/data/node.log',
    '/api/logs/dashboard': env.DASHBOARD_LOG || '/app/data/dashboard.log',
    '/api/logs/lifecycle': env.LIFECYCLE_LOG || '/app/data/lifecycle.log',
    '/api/logs/maintenance': env.MAINTENANCE_LOG || '/app/data/maintenance.log',
    '/api/logs/social-service': env.SOCIAL_SERVICE_LOG || '/app/data/social-service.log',
    '/api/logs/candidate-shadow-observer': env.CANDIDATE_SHADOW_LOG || '/app/data/candidate-shadow-observer.log',
    '/api/logs/source-resonance': env.SOURCE_RESONANCE_LOG || '/app/data/source-resonance.log',
    '/api/logs/gmgn-scout': env.GMGN_SCOUT_LOG || '/app/data/gmgn-scout.log',
    '/api/logs/runtime-final-evidence': env.RUNTIME_FINAL_EVIDENCE_LOG || '/app/data/runtime_final_evidence.jsonl',
    '/api/logs/raw-path-observer': env.RAW_PATH_OBSERVER_LOG || '/app/data/raw-path-observer.log',
    '/api/logs/raw-dog-discovery-observer': env.RAW_DOG_DISCOVERY_OBSERVER_LOG || '/app/data/raw-dog-discovery-observer.log',
    '/api/logs/paper-fast-lane': env.PAPER_FAST_LANE_LOG || '/app/data/paper-fast-lane.log',
    '/api/logs/v27-telegram-signal-mirror': env.V27_TELEGRAM_SIGNAL_MIRROR_LOG || '/app/data/v27-telegram-signal-mirror.log',
    '/api/logs/v27-source-label-mirror': env.V27_SOURCE_LABEL_MIRROR_LOG || '/app/data/v27-source-label-mirror.log',
    '/api/logs/v27-paper-trade-source-label-mirror': env.V27_PAPER_TRADE_SOURCE_LABEL_MIRROR_LOG || '/app/data/v27-paper-trade-source-label-mirror.log',
    '/api/logs/v27-trade-outcome-mirror': env.V27_TRADE_OUTCOME_MIRROR_LOG || '/app/data/v27-trade-outcome-mirror.log',
    '/api/logs/v27-standardized-stop-mirror': env.V27_STANDARDIZED_STOP_MIRROR_LOG || '/app/data/v27-standardized-stop-mirror.log',
    '/api/logs/v27-ex-ante-feasibility-mirror': env.V27_EX_ANTE_FEASIBILITY_MIRROR_LOG || '/app/data/v27-ex-ante-feasibility-mirror.log',
    '/api/logs/v27-earliest-actionable-mirror': env.V27_EARLIEST_ACTIONABLE_MIRROR_LOG || '/app/data/v27-earliest-actionable-mirror.log',
    '/api/logs/v27-realtime-clean-mirror': env.V27_REALTIME_CLEAN_MIRROR_LOG || '/app/data/v27-realtime-clean-mirror.log',
    '/api/logs/v27-quote-intent-binding-mirror': env.V27_QUOTE_INTENT_BINDING_MIRROR_LOG || '/app/data/v27-quote-intent-binding-mirror.log',
    '/api/logs/v27-raw-provider-evidence-mirror': env.V27_RAW_PROVIDER_EVIDENCE_MIRROR_LOG || '/app/data/v27-raw-provider-evidence-mirror.log',
    '/api/logs/v27-raw-provider-probe-evidence': env.V27_RAW_PROVIDER_PROBE_EVIDENCE_LOG || '/app/data/v27-raw-provider-probe-evidence.log',
    '/api/logs/v27-randomness-control-mirror': env.V27_RANDOMNESS_CONTROL_MIRROR_LOG || '/app/data/v27-randomness-control-mirror.log',
    '/api/logs/v27-normal-tiny-ops-evidence': env.V27_NORMAL_TINY_OPS_EVIDENCE_LOG || '/app/data/v27-normal-tiny-ops-evidence.log',
    '/api/logs/v27-idempotency-contract-mirror': env.V27_IDEMPOTENCY_CONTRACT_MIRROR_LOG || '/app/data/v27-idempotency-contract-mirror.log',
    '/api/logs/v27-execution-control-mirror': env.V27_EXECUTION_CONTROL_MIRROR_LOG || '/app/data/v27-execution-control-mirror.log',
    '/api/logs/v27-paper-ledger-mirror': env.V27_PAPER_LEDGER_MIRROR_LOG || '/app/data/v27-paper-ledger-mirror.log',
    '/api/logs/v27-recovery-control-mirror': env.V27_RECOVERY_CONTROL_MIRROR_LOG || '/app/data/v27-recovery-control-mirror.log',
    '/api/logs/v27-paper-decision-mirror': env.V27_PAPER_DECISION_MIRROR_LOG || '/app/data/v27-paper-decision-mirror.log',
    '/api/logs/v27-lifecycle-mirror': env.V27_LIFECYCLE_MIRROR_LOG || '/app/data/v27-lifecycle-mirror.log',
    '/api/logs/v27-read-model-refresh': env.V27_READ_MODEL_REFRESH_LOG || '/app/data/v27-read-model-refresh.log',
    '/api/logs/v27-event-log-recovery': env.V27_EVENT_LOG_RECOVERY_LOG || '/app/data/v27-event-log-recovery.log',
  };
  return logPathByEndpoint[pathname] || null;
}

export function buildStorageHealthSnapshot(options = {}) {
  const root = options.projectRoot || projectRoot;
  const dataDir = options.dataDir || process.env.ZEABUR_DATA_DIR || join(root, 'data');
  const includeDisk = options.includeDisk === true || String(process.env.STORAGE_HEALTH_INCLUDE_DISK || '').toLowerCase() === 'true';
  const includeFileStats = options.includeFileStats === true || String(process.env.STORAGE_HEALTH_INCLUDE_FILE_STATS || '').toLowerCase() === 'true';
  const includePreflightTail = options.includePreflightTail === true || String(process.env.STORAGE_HEALTH_INCLUDE_PREFLIGHT_TAIL || '').toLowerCase() === 'true';
  const paperDbPath = options.paperDbPath || getPaperDbPath();
  const signalDbPath = options.signalDbPath || resolvedDbPath;
  const klineDbPath = options.klineDbPath || (isAbsolute(process.env.KLINE_DB || '')
    ? process.env.KLINE_DB
    : join(root, process.env.KLINE_DB || 'data/kline_cache.db'));
  const lifecycleDbPath = options.lifecycleDbPath || (isAbsolute(process.env.LIFECYCLE_DB || '')
    ? process.env.LIFECYCLE_DB
    : join(root, process.env.LIFECYCLE_DB || 'data/lifecycle_tracks.db'));
  let disk = { available: false };
  try {
    if (includeDisk && typeof fs.statfsSync === 'function') {
      const stats = fs.statfsSync(dataDir);
      const total = Number(stats.blocks || 0) * Number(stats.bsize || 0);
      const free = Number(stats.bavail || stats.bfree || 0) * Number(stats.bsize || 0);
      const used = Math.max(0, total - free);
      disk = {
        available: true,
        path: dataDir,
        total_bytes: total,
        used_bytes: used,
        free_bytes: free,
        used_pct: total ? Math.round((used / total) * 1000) / 10 : null,
        free_mb: Math.round((free / (1024 * 1024)) * 100) / 100,
        low_free: free > 0 && free < Number(process.env.ZEABUR_DISK_WARN_FREE_MB || 256) * 1024 * 1024,
      };
    }
  } catch (error) {
    disk = { available: false, path: dataDir, error: error.message };
  }
  const describeFile = ([label, path]) => includeFileStats ? fileInfo(label, path) : {
    label,
    path,
    skipped: true,
    reason: 'file_stats_disabled',
  };
  const dbFiles = [
    ['paper_trades', paperDbPath],
    ['paper_trades_wal', `${paperDbPath}-wal`],
    ['paper_trades_shm', `${paperDbPath}-shm`],
    ['paper_trades_integrity_marker', `${paperDbPath}.integrity_error`],
    ['sentiment_arb', signalDbPath],
    ['sentiment_arb_wal', `${signalDbPath}-wal`],
    ['kline_cache', klineDbPath],
    ['kline_cache_wal', `${klineDbPath}-wal`],
    ['lifecycle_tracks', lifecycleDbPath],
    ['lifecycle_tracks_wal', `${lifecycleDbPath}-wal`],
  ].map(describeFile);
  const logFiles = [
    'dashboard.log',
    'node.log',
    'maintenance.log',
    'paper-trader.log',
    'paper-fast-lane.log',
    'paper-db-retention.log',
    'paper-review-snapshot.log',
    'source-resonance.log',
    'gmgn-scout.log',
    'v27-telegram-signal-mirror.log',
    'v27-source-label-mirror.log',
    'v27-paper-trade-source-label-mirror.log',
    'v27-trade-outcome-mirror.log',
    'v27-standardized-stop-mirror.log',
    'v27-ex-ante-feasibility-mirror.log',
    'v27-earliest-actionable-mirror.log',
    'v27-realtime-clean-mirror.log',
    'v27-quote-intent-binding-mirror.log',
    'v27-raw-provider-evidence-mirror.log',
    'v27-raw-provider-probe-evidence.log',
    'v27-randomness-control-mirror.log',
    'v27-normal-tiny-ops-evidence.log',
    'v27-idempotency-contract-mirror.log',
    'v27-execution-control-mirror.log',
    'v27-paper-ledger-mirror.log',
    'v27-recovery-control-mirror.log',
    'v27-paper-decision-mirror.log',
    'v27-lifecycle-mirror.log',
    'v27-read-model-refresh.log',
    'v27-event-log-recovery.log',
    'lifecycle.log',
    'preflight.log',
  ].map((name) => describeFile([name, join(dataDir, name)]));
  return {
    generated_at: new Date().toISOString(),
    data_dir: dataDir,
    disk,
    db_files: dbFiles,
    log_files: logFiles,
    integrity_error: includeFileStats ? readTinyText(`${paperDbPath}.integrity_error`) : null,
    preflight_tail: includePreflightTail ? readTinyText(join(dataDir, 'preflight.log'), 4000) : null,
    retention_tail: includePreflightTail ? readTinyText(join(dataDir, 'paper-db-retention.log'), 4000) : null,
  };
}

function getTableColumns(database, tableName) {
  return new Set(database.prepare(`PRAGMA table_info(${tableName})`).all().map(row => row.name));
}

function aClassEventRow(row) {
  const freshness = parseJsonValue(row.freshness_json, {});
  const expectedRrDetail = parseJsonValue(row.expected_rr_detail_json, {});
  const matrix = parseJsonValue(row.matrix_json, {});
  const aiReview = parseJsonValue(row.ai_review_json, {});
  const controllerAction = parseJsonValue(row.controller_action_json, {});
  const discoveryExit = parseJsonValue(row.discovery_exit_json, null);
  return {
    id: row.id,
    event_ts: row.event_ts,
    event_iso: row.event_ts ? new Date(Number(row.event_ts) * 1000).toISOString() : null,
    token_ca: row.token_ca,
    symbol: row.symbol,
    lifecycle_id: row.lifecycle_id,
    route_bucket: row.route_bucket,
    normalized_mode: row.normalized_mode,
    source_table: row.source_table,
    source_id: row.source_id,
    source_component: row.source_component,
    source_reason: row.source_reason,
    action: row.action,
    grade: row.grade,
    size_sol: row.size_sol,
    score: row.score,
    reason: row.reason,
    would_action: row.would_action ?? null,
    expected_rr: row.expected_rr ?? null,
    expected_upside_pct: row.expected_upside_pct ?? null,
    defined_risk_pct: row.defined_risk_pct ?? null,
    bottom_ticket_size_sol: row.bottom_ticket_size_sol ?? null,
    expected_rr_detail: expectedRrDetail && typeof expectedRrDetail === 'object' ? expectedRrDetail : {},
    matrix: matrix && typeof matrix === 'object' ? matrix : {},
    ai_review: aiReview && typeof aiReview === 'object' ? aiReview : {},
    controller_action: controllerAction && typeof controllerAction === 'object' ? controllerAction : {},
    denominator_key: row.denominator_key ?? null,
    discovery_exit: discoveryExit && typeof discoveryExit === 'object' ? discoveryExit : null,
    hard_blockers: parseJsonValue(row.hard_blockers_json, []),
    soft_notes: parseJsonValue(row.soft_notes_json, []),
    freshness,
    budget: parseJsonValue(row.budget_json, {}),
    risk: parseJsonValue(row.risk_json, {}),
    opportunity_age_sec: freshness?.opportunity_age_sec ?? null,
    raw_signal_age_sec: freshness?.raw_signal_age_sec ?? null,
    freshness_sources: freshness?.freshness_sources || [],
  };
}

function normalizedBlockerText(value) {
  return String(value ?? '').trim().toLowerCase();
}

function compactContextText(values = []) {
  return values
    .filter((value) => value !== undefined && value !== null && String(value).trim() !== '')
    .map((value) => String(value).toLowerCase())
    .join(' ');
}

function parseBlockerList(value) {
  const parsed = Array.isArray(value) ? value : parseJsonValue(value, []);
  if (Array.isArray(parsed)) return parsed.map((item) => String(item || '').trim()).filter(Boolean);
  if (parsed == null || parsed === '') return [];
  return [String(parsed).trim()].filter(Boolean);
}

function boolishValue(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    return ['1', 'true', 'yes', 'on', 'ok', 'clean', 'available', 'executable', 'pass'].includes(value.trim().toLowerCase());
  }
  return Boolean(value);
}

function inferAClassBlockersFromEvent(row = {}) {
  const blockers = parseBlockerList(row.hard_blockers_json ?? row.hard_blockers);
  const hasExplicit = new Set(blockers.map(normalizedBlockerText));
  const push = (blocker) => {
    if (!hasExplicit.has(blocker)) {
      blockers.push(blocker);
      hasExplicit.add(blocker);
    }
  };
  const action = String(row.action || '').trim().toUpperCase();
  const blockedish = action === 'BLOCK' || action === 'SHADOW' || action.includes('BLOCK');
  if ('quote_available' in row && row.quote_available != null && !boolishValue(row.quote_available)) push('quote_not_available');
  if ('quote_executable' in row && row.quote_executable != null && !boolishValue(row.quote_executable)) push('quote_not_executable');
  if ('route_available' in row && row.route_available != null && !boolishValue(row.route_available)) push('route_unavailable');
  if (blockedish && 'liquidity_usd' in row && row.liquidity_usd == null) push('liquidity_unknown');
  if (blockedish && 'spread_pct' in row && row.spread_pct == null) push('spread_unknown');
  return blockers;
}

export function classifyAClassBlocker(blocker, context = {}) {
  const b = normalizedBlockerText(blocker);
  const risk = context.risk && typeof context.risk === 'object' ? context.risk : parseJsonValue(context.risk_json, {});
  const candidate = context.candidate && typeof context.candidate === 'object' ? context.candidate : parseJsonValue(context.candidate_json, {});
  const dataConfidence = normalizedBlockerText(
    context.data_confidence
    ?? risk?.data_confidence
    ?? candidate?.data_confidence
    ?? '',
  );
  const quoteSource = normalizedBlockerText(
    context.quote_source
    ?? risk?.quote_source
    ?? candidate?.quote_source
    ?? '',
  );
  const evidence = compactContextText([
    context.route_failure_reason,
    context.quote_failure_reason,
    context.provider_reason,
    context.evidence_status,
    context.reason,
    context.source_reason,
    risk?.route_failure_reason,
    risk?.quote_failure_reason,
    risk?.provider_reason,
    risk?.evidence_status,
    candidate?.route_failure_reason,
    candidate?.quote_failure_reason,
    candidate?.provider_reason,
    candidate?.evidence_status,
  ]);

  const marketRouteFailure = /\b(no[_ -]?route|trapped|token[_ -]?not[_ -]?tradable|not tradable|route[_ -]?failure[_ -]?red|honeypot|rug)\b/.test(evidence);
  const infraContext = !evidence || dataConfidence === 'unknown' || dataConfidence === 'partial' || dataConfidence === 'quote_only' || !quoteSource
    || /\b(rate[_ -]?limited|429|timeout|provider|missing|unknown|stale|quote[_ -]?failed|unavailable)\b/.test(evidence);

  if (!b) {
    return {
      blocker: blocker ?? null,
      category: 'UNKNOWN',
      recoverability: 'unknown',
      reason: 'empty_blocker',
    };
  }

  if (/(creator[_ -]?(close|dump)|rug|security|honeypot|bundler|rat[_ -]?trader|entrapment|top10|mint[_ -]?authority|freeze[_ -]?authority)/.test(b)) {
    return { blocker, category: 'MARKET', recoverability: 'exclude_from_clean_denominator', reason: 'hard_security_or_structure_red_flag' };
  }
  if (/(liquidity[_ -]?(below|min|too[_ -]?low)|spread[_ -]?(extreme|too[_ -]?high)|route[_ -]?failure[_ -]?red|trapped|token[_ -]?not[_ -]?tradable|no[_ -]?route)/.test(b)) {
    return { blocker, category: 'MARKET', recoverability: 'exclude_from_clean_denominator', reason: 'market_execution_or_liquidity_red_flag' };
  }
  if (b.includes('route_unavailable')) {
    if (marketRouteFailure) {
      return { blocker, category: 'MARKET', recoverability: 'exclude_from_clean_denominator', reason: 'route_unavailable_confirmed_by_no_route_or_trapped_reason' };
    }
    return {
      blocker,
      category: infraContext ? 'INFRA' : 'MARKET',
      recoverability: infraContext ? 'provider_or_evidence_recoverable' : 'exclude_from_clean_denominator',
      reason: infraContext ? 'route_unavailable_without_market_failure_evidence' : 'route_unavailable_with_market_context',
    };
  }
  if (b.includes('quote_not_executable')) {
    if (marketRouteFailure) {
      return { blocker, category: 'MARKET', recoverability: 'exclude_from_clean_denominator', reason: 'quote_not_executable_confirmed_by_market_route_failure' };
    }
    return { blocker, category: 'INFRA', recoverability: 'provider_or_evidence_recoverable', reason: 'quote_not_executable_without_market_failure_evidence' };
  }
  if (/\b(quote[_ -]?(not[_ -]?available|source[_ -]?missing|age[_ -]?unknown|stale|missing|unknown|failed)|liquidity[_ -]?unknown|spread[_ -]?unknown|unknown[_ -]?data|data[_ -]?unknown|provider[_ -]?(missing|failed|rate[_ -]?limited)|rate[_ -]?limited|429)\b/.test(b)) {
    return { blocker, category: 'INFRA', recoverability: 'provider_or_evidence_recoverable', reason: 'provider_or_evidence_missing' };
  }
  if (/(expected[_ -]?rr|defined[_ -]?loss|loss[_ -]?risk|cooldown|budget|circuit|mode[_ -]?(disabled|shadow|down)|max[_ -]?concurrent|duplicate|prior[_ -]?(exposure|fastlane)|already[_ -]?fastlane|counterfactual[_ -]?only|watch[_ -]?only|shadow[_ -]?only|entry[_ -]?mode[_ -]?quality|matrix|matrices|scout[_ -]?quality|buy[_ -]?pressure|volume[_ -]?low|negative[_ -]?trend|momentum)/.test(b)) {
    return { blocker, category: 'POLICY', recoverability: 'policy_or_strategy_review', reason: 'strategy_or_budget_guardrail' };
  }
  return { blocker, category: 'UNKNOWN', recoverability: 'needs_review', reason: 'unmapped_blocker' };
}

export function classifyAClassBlockCause(row = {}) {
  const persistedCategory = String(row.block_cause || '').trim().toUpperCase();
  if (['INFRA', 'MARKET', 'POLICY', 'UNKNOWN'].includes(persistedCategory)) {
    const persistedClassifications = parseJsonValue(row.blocker_classifications_json, []);
    const blockers = inferAClassBlockersFromEvent(row);
    const blocker_classifications = Array.isArray(persistedClassifications) && persistedClassifications.length
      ? persistedClassifications
      : blockers.map((blocker) => classifyAClassBlocker(blocker, row));
    const action = String(row.action || '').toUpperCase();
    const wouldAction = String(row.would_action || '').toUpperCase();
    const wouldEnter = action === 'WOULD_ENTER'
      || wouldAction === 'WOULD_ENTER'
      || boolishValue(row.would_enter_a_class);
    const didEnter = action === 'ENTER' || boolishValue(row.did_enter);
    const blocked = blocker_classifications.length > 0 || ['BLOCK', 'SHADOW'].includes(action) || action.includes('BLOCK');
    return {
      category: persistedCategory,
      recoverability: row.recoverability || null,
      classification_reason: row.classification_reason || null,
      blocked,
      would_enter_a_class: wouldEnter,
      did_enter: didEnter,
      blockers,
      blocker_classifications,
      infra_recoverable: persistedCategory === 'INFRA',
      market_unexecutable: persistedCategory === 'MARKET',
      policy_guardrail: persistedCategory === 'POLICY',
    };
  }

  const risk = row.risk && typeof row.risk === 'object' ? row.risk : parseJsonValue(row.risk_json, {});
  const candidate = row.candidate && typeof row.candidate === 'object' ? row.candidate : parseJsonValue(row.candidate_json, {});
  const blockers = inferAClassBlockersFromEvent(row);
  const blocker_classifications = blockers.map((blocker) => classifyAClassBlocker(blocker, {
    ...row,
    risk,
    candidate,
    data_confidence: row.data_confidence ?? risk?.data_confidence ?? candidate?.data_confidence,
    quote_source: row.quote_source ?? risk?.quote_source ?? candidate?.quote_source,
  }));
  const categories = new Set(blocker_classifications.map((item) => item.category));
  let category = 'UNKNOWN';
  if (categories.has('MARKET')) category = 'MARKET';
  else if (categories.has('POLICY')) category = 'POLICY';
  else if (categories.has('INFRA')) category = 'INFRA';

  const action = String(row.action || '').toUpperCase();
  const wouldAction = String(row.would_action || '').toUpperCase();
  const wouldEnter = action === 'WOULD_ENTER'
    || wouldAction === 'WOULD_ENTER'
    || boolishValue(row.would_enter_a_class);
  const didEnter = action === 'ENTER' || boolishValue(row.did_enter);
  const blocked = blocker_classifications.length > 0 || ['BLOCK', 'SHADOW'].includes(action) || action.includes('BLOCK');
  return {
    category,
    blocked,
    would_enter_a_class: wouldEnter,
    did_enter: didEnter,
    blockers,
    blocker_classifications,
    infra_recoverable: category === 'INFRA',
    market_unexecutable: category === 'MARKET',
    policy_guardrail: category === 'POLICY',
  };
}

function incrementBreakdownGroup(map, key, row, classification, extra = {}) {
  const group = map.get(key) || {
    ...extra,
    n: 0,
    blocked_n: 0,
    unique_tokens: 0,
    would_enter_n: 0,
    did_enter_n: 0,
    latest_event_ts: null,
    token_set: new Set(),
  };
  group.n += 1;
  if (classification.blocked) group.blocked_n += 1;
  if (classification.would_enter_a_class) group.would_enter_n += 1;
  if (classification.did_enter) group.did_enter_n += 1;
  if (row.token_ca) group.token_set.add(row.token_ca);
  if (row.event_ts != null) group.latest_event_ts = Math.max(Number(group.latest_event_ts || 0), Number(row.event_ts || 0));
  map.set(key, group);
  return group;
}

function finalizeBreakdownGroups(map, sortKey = 'n') {
  return Array.from(map.values()).map((group) => {
    const { token_set, ...rest } = group;
    return {
      ...rest,
      unique_tokens: token_set instanceof Set ? token_set.size : Number(rest.unique_tokens || 0),
      latest_event_ts: rest.latest_event_ts || null,
      latest_event_iso: rest.latest_event_ts ? new Date(Number(rest.latest_event_ts) * 1000).toISOString() : null,
    };
  }).sort((a, b) => Number(b[sortKey] || 0) - Number(a[sortKey] || 0) || Number(b.n || 0) - Number(a.n || 0));
}

function incrementHydrationGroup(map, key, row, extra = {}) {
  const group = map.get(key) || {
    ...extra,
    n: 0,
    unique_tokens: 0,
    quote_clean_n: 0,
    would_enter_n: 0,
    did_enter_n: 0,
    latest_event_ts: null,
    token_set: new Set(),
  };
  group.n += 1;
  if (row.token_ca) group.token_set.add(row.token_ca);
  if (boolishValue(row.quote_clean)) group.quote_clean_n += 1;
  const action = String(row.action || '').toUpperCase();
  if (action === 'WOULD_ENTER' || String(row.would_action || '').toUpperCase() === 'WOULD_ENTER' || boolishValue(row.would_enter_a_class)) {
    group.would_enter_n += 1;
  }
  if (action === 'ENTER' || boolishValue(row.did_enter)) group.did_enter_n += 1;
  if (row.event_ts != null) group.latest_event_ts = Math.max(Number(group.latest_event_ts || 0), Number(row.event_ts || 0));
  map.set(key, group);
  return group;
}

export function buildAClassBlockCauseBreakdown(rows = [], options = {}) {
  const categoryMap = new Map();
  const blockerMap = new Map();
  const sourceMap = new Map();
  const hydrationMap = new Map();
  const hydrationSourceMap = new Map();
  const recent = [];
  const uniqueTokens = new Set();
  const limit = Math.max(0, Math.min(Number(options.limit || 50), 500));
  let total = 0;
  let blocked = 0;
  let wouldEnter = 0;
  let didEnter = 0;
  let latestEventTs = null;

  for (const raw of Array.isArray(rows) ? rows : []) {
    const row = raw || {};
    const classification = classifyAClassBlockCause(row);
    total += 1;
    if (classification.blocked) blocked += 1;
    if (classification.would_enter_a_class) wouldEnter += 1;
    if (classification.did_enter) didEnter += 1;
    if (row.token_ca) uniqueTokens.add(row.token_ca);
    if (row.event_ts != null) latestEventTs = Math.max(Number(latestEventTs || 0), Number(row.event_ts || 0));
    const hydrateOutcome = String(row.hydrate_outcome ?? row.provider_hydrate_outcome ?? 'not_recorded') || 'not_recorded';
    const dataConfidence = String(row.data_confidence ?? 'unknown') || 'unknown';
    incrementHydrationGroup(hydrationMap, hydrateOutcome, row, {
      provider_hydrate_outcome: hydrateOutcome,
    });
    const hydrationSourceKey = [
      row.source_kind || row.source_table || row.source_type || 'unknown',
      row.source_component || 'unknown',
      hydrateOutcome,
      dataConfidence,
    ].join('|');
    incrementHydrationGroup(hydrationSourceMap, hydrationSourceKey, row, {
      source_kind: row.source_kind || row.source_table || row.source_type || 'unknown',
      source_component: row.source_component || 'unknown',
      provider_hydrate_outcome: hydrateOutcome,
      data_confidence: dataConfidence,
    });
    incrementBreakdownGroup(categoryMap, classification.category, row, classification, {
      category: classification.category,
      recoverability: classification.category === 'INFRA'
        ? 'provider_or_evidence_recoverable'
        : (classification.category === 'MARKET' ? 'exclude_from_clean_denominator' : (classification.category === 'POLICY' ? 'policy_or_strategy_review' : 'needs_review')),
    });
    const sourceKey = [
      row.source_kind || row.source_table || row.source_type || 'unknown',
      row.source_component || 'unknown',
      classification.category,
    ].join('|');
    incrementBreakdownGroup(sourceMap, sourceKey, row, classification, {
      source_kind: row.source_kind || row.source_table || row.source_type || 'unknown',
      source_component: row.source_component || 'unknown',
      category: classification.category,
    });
    for (const item of classification.blocker_classifications) {
      const blockerKey = `${item.category}|${item.blocker}`;
      incrementBreakdownGroup(blockerMap, blockerKey, row, classification, {
        blocker: item.blocker,
        category: item.category,
        recoverability: item.recoverability,
        classification_reason: item.reason,
      });
    }
    if (recent.length < limit) {
      recent.push({
        id: row.id ?? null,
        source_kind: row.source_kind || row.source_table || row.source_type || 'unknown',
        event_ts: row.event_ts ?? null,
        event_iso: row.event_ts ? new Date(Number(row.event_ts) * 1000).toISOString() : null,
        token_ca: row.token_ca ?? null,
        symbol: row.symbol ?? null,
        route_bucket: row.route_bucket ?? row.route ?? null,
        source_component: row.source_component ?? null,
        source_reason: row.source_reason ?? row.reason ?? null,
        action: row.action ?? null,
        would_action: row.would_action ?? null,
        category: classification.category,
        blockers: classification.blockers,
        blocker_classifications: classification.blocker_classifications,
        data_confidence: row.data_confidence ?? null,
        quote_source: row.quote_source ?? null,
        quote_failure_reason: row.quote_failure_reason ?? row.route_failure_reason ?? null,
        evidence_status: row.evidence_status ?? null,
        hydrate_outcome: hydrateOutcome,
        hydrate_success: boolishValue(row.hydrate_success),
      });
    }
  }
  const categorySummary = finalizeBreakdownGroups(categoryMap);
  const categoryByKey = Object.fromEntries(categorySummary.map((row) => [row.category, row]));
  return {
    schema_version: 'v1.a_class_block_cause_breakdown',
    generated_at: new Date().toISOString(),
    total_events: total,
    blocked_events: blocked,
    unique_tokens: uniqueTokens.size,
    would_enter_n: wouldEnter,
    did_enter_n: didEnter,
    latest_event_ts: latestEventTs,
    latest_event_iso: latestEventTs ? new Date(Number(latestEventTs) * 1000).toISOString() : null,
    infra_recoverable: {
      events: categoryByKey.INFRA?.n || 0,
      blocked_events: categoryByKey.INFRA?.blocked_n || 0,
      unique_tokens: categoryByKey.INFRA?.unique_tokens || 0,
      would_enter_n: categoryByKey.INFRA?.would_enter_n || 0,
      did_enter_n: categoryByKey.INFRA?.did_enter_n || 0,
    },
    market_unexecutable: {
      events: categoryByKey.MARKET?.n || 0,
      blocked_events: categoryByKey.MARKET?.blocked_n || 0,
      unique_tokens: categoryByKey.MARKET?.unique_tokens || 0,
      would_enter_n: categoryByKey.MARKET?.would_enter_n || 0,
      did_enter_n: categoryByKey.MARKET?.did_enter_n || 0,
    },
    policy_guardrail: {
      events: categoryByKey.POLICY?.n || 0,
      blocked_events: categoryByKey.POLICY?.blocked_n || 0,
      unique_tokens: categoryByKey.POLICY?.unique_tokens || 0,
      would_enter_n: categoryByKey.POLICY?.would_enter_n || 0,
      did_enter_n: categoryByKey.POLICY?.did_enter_n || 0,
    },
    category_summary: categorySummary,
    blocker_summary: finalizeBreakdownGroups(blockerMap).slice(0, 100),
    source_component_summary: finalizeBreakdownGroups(sourceMap).slice(0, 100),
    hydrate_summary: finalizeBreakdownGroups(hydrationMap).slice(0, 100),
    hydrate_source_summary: finalizeBreakdownGroups(hydrationSourceMap).slice(0, 100),
    recent_events: recent,
    interpretation: {
      infra_recoverable: 'Provider/evidence gaps that should be excluded from clean denominator until quote/route evidence is fixed.',
      market_unexecutable: 'True market/security/route/liquidity failures that should remain excluded from clean executable denominator.',
      policy_guardrail: 'Strategy/budget/shadow gates; review with denominator and EV, not provider fixes.',
    },
  };
}

function sanitizeAClassP0Discovery(raw) {
  const section = raw && typeof raw === 'object' ? raw : null;
  if (!section) {
    return {
      available: false,
      status: 'shadow_pending',
      reason: 'a_class_p0_discovery_materialized_section_missing',
      quote_clean_gold_silver_seen_count: 0,
      quote_clean_gold_silver_would_enter_count: 0,
      would_enter_no_route_rate: null,
      would_enter_trapped_rate: null,
      unknown_data_rate: null,
      outlier_trimmed_would_rr: null,
      source_breakdown: {},
      source_component_breakdown: {},
      hydrate_outcome_breakdown: {},
      observed_hydrate_outcome_breakdown: {},
      denominator_exclusion_breakdown: {},
      hydrate_outcome_exclusion_breakdown: {},
      unknown_reason_breakdown: {},
      missed_blockers: [],
      discovery_exit: null,
    };
  }
  const blockerRows = Array.isArray(section.missed_blockers) ? section.missed_blockers : [];
  const missedBlockers = blockerRows.map((row) => ({
    route: row.route ?? null,
    component: row.component ?? null,
    reject_reason: row.reject_reason ?? null,
    unique_tokens: Number(row.unique_tokens || 0),
    gold_n: Number(row.gold_n || 0),
    silver_n: Number(row.silver_n || 0),
    max_adjusted_peak: roundNullableNumber(row.max_adjusted_peak, 6),
  }));
  const available = section.available !== false && section.status !== 'evidence_unavailable';
  return {
    available,
    status: available ? (section.status || 'shadow_ready') : (section.status || 'shadow_pending'),
    reason: section.reason || null,
    denominator_key: section.denominator_key || null,
    quote_clean_gold_silver_seen_count: Number(section.quote_clean_gold_silver_seen_count || 0),
    quote_clean_gold_silver_gold_count: Number(section.quote_clean_gold_silver_gold_count || 0),
    quote_clean_gold_silver_silver_count: Number(section.quote_clean_gold_silver_silver_count || 0),
    quote_clean_gold_silver_would_enter_count: Number(section.quote_clean_gold_silver_would_enter_count || 0),
    would_enter_no_route_rate: roundNullableNumber(section.would_enter_no_route_rate, 6),
    would_enter_trapped_rate: roundNullableNumber(section.would_enter_trapped_rate, 6),
    unknown_data_rate: roundNullableNumber(section.unknown_data_rate, 6),
    outlier_trimmed_would_rr: roundNullableNumber(section.outlier_trimmed_would_rr, 6),
    defined_risk_pct: roundNullableNumber(section.defined_risk_pct, 6),
    source_breakdown: section.source_breakdown && typeof section.source_breakdown === 'object'
      ? { ...section.source_breakdown }
      : {},
    source_component_breakdown: section.source_component_breakdown && typeof section.source_component_breakdown === 'object'
      ? { ...section.source_component_breakdown }
      : {},
    hydrate_outcome_breakdown: section.hydrate_outcome_breakdown && typeof section.hydrate_outcome_breakdown === 'object'
      ? { ...section.hydrate_outcome_breakdown }
      : {},
    observed_hydrate_outcome_breakdown: section.observed_hydrate_outcome_breakdown && typeof section.observed_hydrate_outcome_breakdown === 'object'
      ? { ...section.observed_hydrate_outcome_breakdown }
      : {},
    denominator_exclusion_breakdown: section.denominator_exclusion_breakdown && typeof section.denominator_exclusion_breakdown === 'object'
      ? { ...section.denominator_exclusion_breakdown }
      : {},
    hydrate_outcome_exclusion_breakdown: section.hydrate_outcome_exclusion_breakdown && typeof section.hydrate_outcome_exclusion_breakdown === 'object'
      ? { ...section.hydrate_outcome_exclusion_breakdown }
      : {},
    unknown_reason_breakdown: section.unknown_reason_breakdown && typeof section.unknown_reason_breakdown === 'object'
      ? { ...section.unknown_reason_breakdown }
      : {},
    source_issues: Array.isArray(section.source_issues) ? section.source_issues.map(String) : [],
    missed_blockers: missedBlockers,
    discovery_exit: section.discovery_exit && typeof section.discovery_exit === 'object'
      ? { ...section.discovery_exit }
      : null,
    expected_rr_detail: section.expected_rr_detail && typeof section.expected_rr_detail === 'object'
      ? { ...section.expected_rr_detail }
      : {},
  };
}

function aClassP0DiscoveryFromSnapshot(liveSnapshot) {
  return sanitizeAClassP0Discovery(liveSnapshot?.a_class_p0_discovery || liveSnapshot?.a_class?.p0_discovery || null);
}

export function summarizeAClassMatrixEvents(events = []) {
  const dimensionNames = ['source_strength', 'execution_quality', 'market_flow', 'security_cleanliness', 'freshness_lifecycle', 'historical_ev'];
  const stateCounts = {};
  const gradeCounts = {};
  let withMatrix = 0;
  for (const event of Array.isArray(events) ? events : []) {
    const matrix = event?.matrix && typeof event.matrix === 'object' ? event.matrix : {};
    if (!matrix.matrix_version) continue;
    withMatrix += 1;
    const grade = matrix.matrix_grade || 'UNKNOWN';
    gradeCounts[grade] = (gradeCounts[grade] || 0) + 1;
    for (const name of dimensionNames) {
      const state = matrix[name] || matrix.dimensions?.[name]?.state || 'UNKNOWN';
      const key = `${name}:${state}`;
      stateCounts[key] = (stateCounts[key] || 0) + 1;
    }
  }
  return {
    available: withMatrix > 0,
    schema_version: 'v1.a_class_matrix_summary',
    total_events: Array.isArray(events) ? events.length : 0,
    matrix_events: withMatrix,
    grade_counts: gradeCounts,
    state_counts: Object.entries(stateCounts)
      .map(([key, n]) => {
        const [dimension, state] = key.split(':');
        return { dimension, state, n };
      })
      .sort((a, b) => b.n - a.n),
  };
}

export function buildMissedDogAiReviewFromP0(p0Discovery) {
  const p0 = p0Discovery || {};
  const hardTokens = ['rug', 'security', 'honeypot', 'blacklist', 'creator', 'bundler', 'rat', 'entrapment', 'no_route', 'trapped', 'quote_not_executable', 'liquidity_unknown'];
  const recommendations = (Array.isArray(p0.missed_blockers) ? p0.missed_blockers : []).map((row) => {
    const reason = String(row.reject_reason || row.blocker || 'unknown');
    const lower = reason.toLowerCase();
    const hardSecurity = hardTokens.some((token) => lower.includes(token));
    const goldN = Number(row.gold_n || 0);
    const silverN = Number(row.silver_n || 0);
    const uniqueTokens = Number(row.unique_tokens || 0);
    const maxPeak = Number(row.max_adjusted_peak || 0);
    let recommendation = 'no_action';
    if (hardSecurity) recommendation = 'keep_hard_block';
    else if (goldN >= 1 || silverN >= 3) recommendation = 'allow_a_class_only';
    else if (uniqueTokens >= 5 && maxPeak >= 0.50) recommendation = 'investigate_data_quality';
    return {
      route: row.route ?? null,
      component: row.component ?? null,
      reject_reason: reason,
      unique_tokens: uniqueTokens,
      gold_n: goldN,
      silver_n: silverN,
      max_adjusted_peak: row.max_adjusted_peak ?? null,
      hard_security_blocker: hardSecurity,
      recommendation,
    };
  }).sort((a, b) => {
    const ap = a.recommendation === 'allow_a_class_only' ? 1 : 0;
    const bp = b.recommendation === 'allow_a_class_only' ? 1 : 0;
    return bp - ap || b.gold_n - a.gold_n || b.silver_n - a.silver_n || b.unique_tokens - a.unique_tokens;
  });
  return {
    schema_version: 'v1.ai_strategy_advisory.shadow_only',
    reviewer: 'AI_MISSED_DOG_REVIEWER_LOCAL_SHADOW_JS',
    advisory_only: true,
    can_trigger_trade: false,
    can_override_hard_gate: false,
    ai_grade: recommendations.some((row) => row.recommendation === 'allow_a_class_only') ? 'actionable' : 'observe',
    reason: 'Ranks missed dog blockers without downgrading hard security gates.',
    recommendations: recommendations.slice(0, 50),
    allow_a_class_only_count: recommendations.filter((row) => row.recommendation === 'allow_a_class_only').length,
    keep_hard_block_count: recommendations.filter((row) => row.recommendation === 'keep_hard_block').length,
  };
}

export function buildCounterfactualAiAuditFromP0(p0Discovery) {
  const p0 = p0Discovery || {};
  const blockers = [];
  const seen = Number(p0.quote_clean_gold_silver_seen_count || 0);
  const wouldEnter = Number(p0.quote_clean_gold_silver_would_enter_count || 0);
  const rr = finiteNumber(p0.outlier_trimmed_would_rr, null);
  const noRouteRate = Number(p0.would_enter_no_route_rate || 0);
  const trappedRate = Number(p0.would_enter_trapped_rate || 0);
  const unknownRate = Number(p0.unknown_data_rate || 0);
  if (seen < 8) blockers.push('quote_clean_gold_silver_seen_below_min');
  if (wouldEnter < 5) blockers.push('quote_clean_gold_silver_would_enter_below_min');
  if (rr == null || rr < 2.0) blockers.push('outlier_trimmed_would_rr_below_2');
  if (noRouteRate > 0.10) blockers.push('would_enter_no_route_rate_above_10pct');
  if (trappedRate > 0.10) blockers.push('would_enter_trapped_rate_above_10pct');
  if (unknownRate > 0.05) blockers.push('unknown_data_rate_above_5pct');
  return {
    schema_version: 'v1.ai_strategy_advisory.shadow_only',
    reviewer: 'AI_COUNTERFACTUAL_AUDITOR_LOCAL_SHADOW_JS',
    advisory_only: true,
    can_trigger_trade: false,
    can_override_hard_gate: false,
    pass: blockers.length === 0,
    ai_grade: blockers.length === 0 ? 'promotion_evidence_ok' : 'shadow_continue',
    blockers,
    candidate_count: seen,
    would_enter_count: wouldEnter,
    outlier_trimmed_would_rr: rr,
    would_enter_no_route_rate: noRouteRate,
    would_enter_trapped_rate: trappedRate,
    unknown_data_rate: unknownRate,
  };
}

export function buildGoalControllerActions({ rollingGoalStatus = null, p0Discovery = null, counterfactualAudit = null, missedDogReview = null } = {}) {
  const goal = rollingGoalStatus || {};
  const p0 = p0Discovery || {};
  const audit = counterfactualAudit || {};
  const missed = missedDogReview || {};
  const actions = [];
  const blockers = [];
  if (['insufficient_sample', 'evidence_unavailable'].includes(goal.status)) {
    blockers.push('rolling_goal_sample_or_evidence_insufficient');
  }
  if (goal.max_single_trade_loss_ok === false) {
    actions.push({ mode: 'ALL_LIVE_RISK', action: 'DISABLE', reason: 'single_trade_loss_limit_breached' });
  }
  const discoveryExit = p0.discovery_exit || {};
  if ((discoveryExit.advisory || discoveryExit.advisory_action) === 'PROMOTE_TINY_CANARY' && audit.pass) {
    actions.push({
      mode: 'A_CLASS_FASTLANE',
      action: 'TINY_CANARY',
      size_sol: discoveryExit.canary_size_sol || 0.001,
      reason: 'counterfactual_denominator_and_rr_passed',
      requires_human_approval: true,
    });
  } else {
    actions.push({
      mode: 'A_CLASS_FASTLANE',
      action: 'SHADOW',
      reason: 'p0_discovery_or_counterfactual_audit_not_green',
    });
  }
  if (Number(p0.would_enter_no_route_rate || 0) > 0.10 || Number(p0.would_enter_trapped_rate || 0) > 0.10) {
    actions.push({ mode: 'A_CLASS_FASTLANE', action: 'DISABLE', reason: 'route_health_risk_above_threshold' });
  }
  const allowCount = Number(missed.allow_a_class_only_count || 0);
  if (allowCount) {
    actions.push({
      mode: 'MISSED_DOG_BLOCKERS',
      action: 'ALLOW_A_CLASS_ONLY',
      reason: 'missed_dog_reviewer_found_soft_blocker_candidates',
      candidate_blocker_count: allowCount,
    });
  }
  let nextSafeAction = 'keep_a_class_shadow';
  if (actions.some((row) => row.action === 'TINY_CANARY')) nextSafeAction = 'prepare_0_001_tiny_paper_after_observability_green';
  if (actions.some((row) => row.action === 'DISABLE')) nextSafeAction = 'disable_or_shadow_risky_modes';
  return {
    schema_version: 'v1.strategy_goal_controller.advisory',
    advisory_only: true,
    can_trigger_trade: false,
    actions,
    blockers,
    next_safe_action: nextSafeAction,
  };
}

function canonicalLedgerTradeRow(row) {
  return {
    id: row.id,
    trade_id: row.trade_id,
    token_ca: row.token_ca,
    symbol: row.symbol,
    lifecycle_id: row.lifecycle_id,
    route_bucket: row.route_bucket,
    entry_mode: row.entry_mode,
    normalized_mode: row.normalized_mode,
    strategy_family: row.strategy_family,
    entry_ts: row.entry_ts,
    entry_iso: row.entry_ts ? new Date(Number(row.entry_ts) * 1000).toISOString() : null,
    exit_ts: row.exit_ts,
    exit_iso: row.exit_ts ? new Date(Number(row.exit_ts) * 1000).toISOString() : null,
    entry_size_sol: row.entry_size_sol,
    realized_exit_sol: row.realized_exit_sol,
    realized_pnl_sol: row.realized_pnl_sol,
    realized_pnl_pct: row.realized_pnl_pct == null ? null : roundNumber(Number(row.realized_pnl_pct) * 100, 3),
    peak_quote_pnl_pct: row.peak_quote_pnl_pct == null ? null : roundNumber(Number(row.peak_quote_pnl_pct) * 100, 3),
    max_drawdown_pct: row.max_drawdown_pct == null ? null : roundNumber(Number(row.max_drawdown_pct) * 100, 3),
    exit_reason: row.exit_reason,
    accounting_source: row.accounting_source,
    trapped_flag: Boolean(row.trapped_flag),
    no_route_flag: Boolean(row.no_route_flag),
    stale_flag: Boolean(row.stale_flag),
    outlier_flag: Boolean(row.outlier_flag),
    outlier_reason: row.outlier_reason,
    is_a_class_fastlane: Boolean(row.is_a_class_fastlane),
    a_class_grade: row.a_class_grade,
    a_class_score: row.a_class_score,
    a_class_size_rule: row.a_class_size_rule,
  };
}

function sqlCol(name, qualifier = '') {
  return qualifier ? `${qualifier}.${name}` : name;
}

function trustedTradePeakSqlExpr(cols, qualifier = '') {
  const trusted = sqlCol('trusted_peak_pnl', qualifier);
  const quote = sqlCol('quote_peak_pnl', qualifier);
  if (cols.has('trusted_peak_pnl') && cols.has('quote_peak_pnl')) {
    return `COALESCE(NULLIF(${trusted}, 0), NULLIF(${quote}, 0), 0)`;
  }
  if (cols.has('trusted_peak_pnl')) return `COALESCE(NULLIF(${trusted}, 0), 0)`;
  if (cols.has('quote_peak_pnl')) return `COALESCE(NULLIF(${quote}, 0), 0)`;
  return cols.has('peak_pnl') ? `COALESCE(${sqlCol('peak_pnl', qualifier)}, 0)` : '0';
}

function markTradePeakSqlExpr(cols, qualifier = '') {
  if (cols.has('mark_peak_pnl')) return `COALESCE(${sqlCol('mark_peak_pnl', qualifier)}, 0)`;
  return cols.has('peak_pnl') ? `COALESCE(${sqlCol('peak_pnl', qualifier)}, 0)` : '0';
}

function optionalSqlCol(cols, name, fallback = 'NULL', alias = name) {
  return cols.has(name) ? name : `${fallback} AS ${alias}`;
}

function funnelEntityKey(row) {
  return row?.token_ca ? String(row.token_ca) : (row?.lifecycle_id ? `lifecycle:${row.lifecycle_id}` : null);
}

function addFunnelStage(stage, row) {
  stage.events += 1;
  const key = funnelEntityKey(row);
  if (key) stage.entities.add(key);
  if (row?.token_ca) stage.tokens.add(String(row.token_ca));
}

function makeFunnelStage(stage, label) {
  return {
    stage,
    label,
    events: 0,
    entities: new Set(),
    tokens: new Set(),
  };
}

function serializeFunnelStage(stage) {
  return {
    stage: stage.stage,
    label: stage.label,
    events: stage.events,
    unique_entities: stage.entities.size,
    unique_tokens: stage.tokens.size,
  };
}

function incrementCounter(map, key, amount = 1) {
  const label = String(key || 'unknown');
  map.set(label, (map.get(label) || 0) + amount);
}

function counterToRows(map, limit = 50) {
  return Array.from(map.entries())
    .map(([key, n]) => ({ key, n }))
    .sort((a, b) => Number(b.n || 0) - Number(a.n || 0) || String(a.key).localeCompare(String(b.key)))
    .slice(0, limit);
}

function extractMarkovGate(payload = {}) {
  const candidates = [
    payload.gate,
    payload.markov_reclaim_gate,
    payload.markovReclaimGate,
    payload.markov_reclaim_forecast?.gate,
    payload.markovReclaimForecast?.gate,
    payload.lotto_markov_reclaim_forecast?.gate,
    payload.revival_canary?.markov_reclaim_gate,
    payload.revival_canary?.markovReclaimGate,
    payload.revival_canary?.markov_reclaim_forecast?.gate,
    payload.revival_canary?.markovReclaimForecast?.gate,
  ];
  return candidates.find((candidate) => candidate && typeof candidate === 'object' && !Array.isArray(candidate)) || {};
}

function extractFunnelEventMode(row, payload = {}) {
  const gate = extractMarkovGate(payload);
  const value = firstValue(
    payload.entry_mode,
    payload.entryMode,
    payload.scout_mode,
    payload.scoutMode,
    payload.entryModeHint,
    payload.entry_mode_hint,
    gate.entry_mode,
    gate.entryMode,
    payload.revival_canary?.entry_mode,
    payload.revival_canary?.entryMode,
    payload.revival_canary?.entry_mode_hint,
    payload.revival_canary?.entryModeHint,
    row.entry_mode_hint,
    ''
  );
  return value == null ? '' : String(value);
}

function extractFunnelEventBranch(row, payload = {}) {
  const gate = extractMarkovGate(payload);
  const value = firstValue(
    payload.entry_branch,
    payload.entryBranch,
    payload.queue_entry_branch,
    payload.queueEntryBranch,
    payload.revival_canary?.entry_branch,
    payload.revival_canary?.entryBranch,
    payload.learning_bypass?.entry_branch,
    payload.learning_bypass?.entryBranch,
    gate.entry_branch,
    gate.entryBranch,
    row.entry_branch,
    ''
  );
  return value == null ? '' : String(value);
}

function extractMarkovBucket(payload = {}) {
  const gate = extractMarkovGate(payload);
  const value = firstValue(
    gate.markov_bucket,
    gate.markovBucket,
    payload.markov_bucket,
    payload.markovBucket,
    payload.revival_canary?.markov_bucket,
    payload.revival_canary?.markovBucket,
    payload.learning_bypass?.markov_bucket,
    payload.learning_bypass?.markovBucket,
    ''
  );
  return value == null ? '' : String(value).toLowerCase();
}

function isTargetNotAthReclaim({ mode, branch, sourceType, reason }, target) {
  const modeText = String(mode || '');
  const branchText = String(branch || '');
  const sourceText = String(sourceType || '');
  const reasonText = String(reason || '');
  return (
    modeText === target.entryMode
    || branchText === target.entryBranch
    || sourceText === 'not_ath_reclaim_fast'
    || reasonText === target.entryBranch
  );
}

export function buildNotAthReclaimFunnelReport(database, tableNames, sinceTs, options = {}) {
  const target = {
    entryMode: options.entryMode || 'lotto_not_ath_reclaim_tiny_probe',
    entryBranch: options.entryBranch || 'not_ath_reclaim_quote_clean_tiny_probe',
  };
  const limit = Math.max(1, Math.min(Number.parseInt(String(options.limit ?? 5000), 10) || 5000, 20000));
  const nowTs = Number.isFinite(options.nowTs) ? Number(options.nowTs) : Math.floor(Date.now() / 1000);
  const since = Number.isFinite(Number(sinceTs)) ? Number(sinceTs) : nowTs - 6 * 3600;
  const stages = {
    markov_green: makeFunnelStage('markov_green', 'Markov green forecast/gate'),
    canary_allow: makeFunnelStage('canary_allow', 'Revival canary allowed or previewed'),
    branch_block: makeFunnelStage('branch_block', 'Branch circuit blocked'),
    branch_bypass: makeFunnelStage('branch_bypass', 'Branch circuit learning bypass'),
    queued: makeFunnelStage('queued', 'Fast-lane queue seen'),
    quote_drift_reject: makeFunnelStage('quote_drift_reject', 'Rejected by quote drift guard'),
    entered: makeFunnelStage('entered', 'Paper entry filled'),
    closed: makeFunnelStage('closed', 'Closed paper trade'),
    peak30: makeFunnelStage('peak30', 'Reached +30% trusted peak'),
    peak50: makeFunnelStage('peak50', 'Reached +50% trusted peak'),
    peak100: makeFunnelStage('peak100', 'Reached +100% trusted peak'),
  };
  const byMarkovBucket = new Map();
  const canaryReasons = new Map();
  const branchReasons = new Map();
  const queueStatus = new Map();
  const queueReasons = new Map();
  const recentEvents = [];
  const recentQueue = [];
  const recentTrades = [];

  if (tableNames.has('paper_decision_events')) {
    const rows = database.prepare(`
      SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
             signal_ts, strategy_stage, route, component, event_type, decision,
             reason, data_source, payload_json
      FROM paper_decision_events
      ORDER BY id DESC
      LIMIT @limit
    `).all({ limit });
    for (const row of rows) {
      if (Number(row.event_ts || 0) < since) continue;
      if (!['markov_reclaim', 'revival_canary', 'paper_fast_lane', 'entry_mode_quality'].includes(String(row.component || ''))) continue;
      const payload = parseJsonObject(row.payload_json);
      const mode = extractFunnelEventMode(row, payload);
      const branch = extractFunnelEventBranch(row, payload);
      const sourceType = firstValue(payload.queue_source_type, payload.source_type, payload.signal_type, row.data_source);
      if (!isTargetNotAthReclaim({ mode, branch, sourceType, reason: row.reason }, target)) continue;

      const bucket = extractMarkovBucket(payload);
      const component = String(row.component || '');
      const eventType = String(row.event_type || '');
      const decision = String(row.decision || '');
      if (bucket) incrementCounter(byMarkovBucket, bucket);
      if (component === 'markov_reclaim') {
        if (bucket === 'green' || decision === 'allow') addFunnelStage(stages.markov_green, row);
      } else if (component === 'revival_canary') {
        incrementCounter(canaryReasons, row.reason);
        if (decision === 'allow' || eventType === 'entry_allow' || eventType === 'entry_preview') {
          addFunnelStage(stages.canary_allow, row);
        }
      } else if (component === 'paper_fast_lane' && eventType === 'branch_circuit') {
        incrementCounter(branchReasons, row.reason);
        addFunnelStage(stages.branch_block, row);
      } else if (component === 'paper_fast_lane' && eventType === 'branch_circuit_learning_bypass') {
        incrementCounter(branchReasons, row.reason);
        addFunnelStage(stages.branch_bypass, row);
      }
      if (recentEvents.length < 40) {
        recentEvents.push({
          id: row.id,
          event_ts: row.event_ts,
          token_ca: row.token_ca,
          symbol: row.symbol,
          component: row.component,
          event_type: row.event_type,
          decision: row.decision,
          reason: row.reason,
          entry_mode: mode || null,
          entry_branch: branch || null,
          markov_bucket: bucket || null,
        });
      }
    }
  }

  if (tableNames.has('paper_fast_entry_queue')) {
    const queueCols = getTableColumns(database, 'paper_fast_entry_queue');
    const updatedExpr = queueCols.has('updated_at') ? 'updated_at' : 'created_at';
    const queueRows = database.prepare(`
      SELECT id, created_at, ${optionalSqlCol(queueCols, 'updated_at', 'created_at')},
             token_ca, symbol, source_type, entry_mode_hint, entry_branch,
             status, ${optionalSqlCol(queueCols, 'last_error')},
             ${optionalSqlCol(queueCols, 'first_error')},
             ${optionalSqlCol(queueCols, 'payload_json')},
             ${optionalSqlCol(queueCols, 'market_session', "'unknown'")}
      FROM paper_fast_entry_queue
      ORDER BY id DESC
      LIMIT @limit
    `).all({ limit });
    for (const row of queueRows) {
      const rowCreated = Number(row.created_at || 0);
      const rowUpdated = Number(firstValue(row.updated_at, row.created_at, 0));
      if (rowCreated < since && rowUpdated < since) continue;
      if (!isTargetNotAthReclaim({
        mode: row.entry_mode_hint,
        branch: row.entry_branch,
        sourceType: row.source_type,
        reason: row.first_error || row.last_error,
      }, target)) continue;
      addFunnelStage(stages.queued, row);
      incrementCounter(queueStatus, row.status);
      const reason = firstValue(row.first_error, row.last_error, 'none');
      incrementCounter(queueReasons, `${row.status || 'unknown'}:${reason}`);
      if (reason === 'fast_lane_quote_drift_hard_reject') addFunnelStage(stages.quote_drift_reject, row);
      if (row.status === 'entered') addFunnelStage(stages.entered, row);
      if (recentQueue.length < 40) {
        recentQueue.push({
          id: row.id,
          created_at: row.created_at,
          updated_at: row.updated_at,
          token_ca: row.token_ca,
          symbol: row.symbol,
          source_type: row.source_type,
          entry_mode_hint: row.entry_mode_hint,
          entry_branch: row.entry_branch,
          status: row.status,
          reason,
          market_session: row.market_session,
        });
      }
    }
  }

  const tradeSummary = {
    total: 0,
    open: 0,
    closed: 0,
    wins: 0,
    losses: 0,
    peak30_n: 0,
    peak50_n: 0,
    peak100_n: 0,
    total_pnl_pct: 0,
    total_peak_pct: 0,
    pnl_n: 0,
    peak_n: 0,
    est_pnl_sol: 0,
    entry_quote_success_n: 0,
    entry_quote_failure_n: 0,
  };
  if (tableNames.has('paper_trades')) {
    const tradeCols = getTableColumns(database, 'paper_trades');
    const branchFilter = tradeCols.has('entry_branch') ? 'OR entry_branch = @entryBranch' : '';
    const modeFilter = tradeCols.has('entry_mode') ? 'OR entry_mode = @entryMode' : '';
    const routeFilter = tradeCols.has('signal_route') ? "OR signal_route = 'not_ath_reclaim_fast'" : '';
    const tradeRows = database.prepare(`
      SELECT id, symbol, token_ca, lifecycle_id, entry_ts, exit_ts, exit_reason,
             ${tradeCols.has('pnl_pct') ? 'pnl_pct' : 'NULL AS pnl_pct'},
             ${trustedTradePeakSqlExpr(tradeCols)} AS peak_pnl,
             ${markTradePeakSqlExpr(tradeCols)} AS mark_peak_pnl,
             ${optionalSqlCol(tradeCols, 'peak_trust_status', "'legacy_peak'")},
             ${optionalSqlCol(tradeCols, 'position_size_sol')},
             ${optionalSqlCol(tradeCols, 'signal_route')},
             ${optionalSqlCol(tradeCols, 'entry_mode')},
             ${optionalSqlCol(tradeCols, 'entry_branch')},
             ${optionalSqlCol(tradeCols, 'entry_execution_audit_json')}
      FROM paper_trades
      WHERE entry_ts >= @since
        AND (
          0
          ${branchFilter}
          ${modeFilter}
          ${routeFilter}
        )
      ORDER BY entry_ts DESC, id DESC
      LIMIT @limit
    `).all({ since, entryBranch: target.entryBranch, entryMode: target.entryMode, limit });
    for (const row of tradeRows) {
      addFunnelStage(stages.entered, row);
      tradeSummary.total += 1;
      const closed = row.exit_ts != null || row.exit_reason != null;
      if (closed) {
        tradeSummary.closed += 1;
        addFunnelStage(stages.closed, row);
      } else {
        tradeSummary.open += 1;
      }
      const pnl = Number(row.pnl_pct);
      if (Number.isFinite(pnl)) {
        tradeSummary.pnl_n += 1;
        tradeSummary.total_pnl_pct += ratioToPct(pnl);
        if (closed && pnl > 0) tradeSummary.wins += 1;
        if (closed && pnl <= 0) tradeSummary.losses += 1;
        if (row.position_size_sol != null) tradeSummary.est_pnl_sol += pnl * Number(row.position_size_sol || 0);
      }
      const peak = Number(row.peak_pnl);
      if (Number.isFinite(peak)) {
        tradeSummary.peak_n += 1;
        tradeSummary.total_peak_pct += ratioToPct(peak);
        if (peak >= 0.30) {
          tradeSummary.peak30_n += 1;
          addFunnelStage(stages.peak30, row);
        }
        if (peak >= 0.50) {
          tradeSummary.peak50_n += 1;
          addFunnelStage(stages.peak50, row);
        }
        if (peak >= 1.00) {
          tradeSummary.peak100_n += 1;
          addFunnelStage(stages.peak100, row);
        }
      }
      const audit = parseJsonObject(row.entry_execution_audit_json);
      if (audit.success === true || audit.routeAvailable === true) tradeSummary.entry_quote_success_n += 1;
      if (audit.success === false || audit.routeAvailable === false || audit.failureReason) tradeSummary.entry_quote_failure_n += 1;
      if (recentTrades.length < 40) {
        recentTrades.push({
          id: row.id,
          token_ca: row.token_ca,
          symbol: row.symbol,
          entry_ts: row.entry_ts,
          exit_ts: row.exit_ts,
          exit_reason: row.exit_reason,
          signal_route: row.signal_route,
          entry_mode: row.entry_mode,
          entry_branch: row.entry_branch,
          position_size_sol: row.position_size_sol,
          pnl_pct: Number.isFinite(pnl) ? roundNumber(ratioToPct(pnl), 2) : null,
          peak_pnl_pct: Number.isFinite(peak) ? roundNumber(ratioToPct(peak), 2) : null,
          mark_peak_pnl_pct: row.mark_peak_pnl == null ? null : roundNumber(ratioToPct(row.mark_peak_pnl), 2),
          peak_trust_status: row.peak_trust_status || null,
        });
      }
    }
  }

  const serializedStages = Object.values(stages).map(serializeFunnelStage);
  const stageByName = Object.fromEntries(serializedStages.map((stage) => [stage.stage, stage]));
  const quoteAttempts = stageByName.quote_drift_reject.unique_tokens + stageByName.entered.unique_tokens;
  const markovGreen = stageByName.markov_green.unique_tokens;
  const canaryAllow = stageByName.canary_allow.unique_tokens;
  const entered = stageByName.entered.unique_tokens;
  const closed = stageByName.closed.unique_tokens;
  return {
    schema_version: 'v2.7.0.not_ath_reclaim_funnel.v1',
    target,
    filters: {
      since_ts: since,
      since_iso: new Date(since * 1000).toISOString(),
      limit,
    },
    available: {
      paper_decision_events: tableNames.has('paper_decision_events'),
      paper_fast_entry_queue: tableNames.has('paper_fast_entry_queue'),
      paper_trades: tableNames.has('paper_trades'),
    },
    summary: {
      markov_green_unique: markovGreen,
      canary_allow_unique: canaryAllow,
      branch_block_unique: stageByName.branch_block.unique_tokens,
      branch_bypass_unique: stageByName.branch_bypass.unique_tokens,
      queued_unique: stageByName.queued.unique_tokens,
      quote_drift_reject_unique: stageByName.quote_drift_reject.unique_tokens,
      entered_unique: entered,
      closed_unique: closed,
      peak30_unique: stageByName.peak30.unique_tokens,
      peak50_unique: stageByName.peak50.unique_tokens,
      peak100_unique: stageByName.peak100.unique_tokens,
      markov_green_to_entered_pct: markovGreen ? roundNumber((entered / markovGreen) * 100, 1) : null,
      canary_allow_to_entered_pct: canaryAllow ? roundNumber((entered / canaryAllow) * 100, 1) : null,
      quote_attempt_to_entered_pct: quoteAttempts ? roundNumber((entered / quoteAttempts) * 100, 1) : null,
    },
    stages: serializedStages,
    by_markov_bucket: counterToRows(byMarkovBucket),
    canary_reason_summary: counterToRows(canaryReasons),
    branch_reason_summary: counterToRows(branchReasons),
    queue_status_summary: counterToRows(queueStatus),
    queue_reason_summary: counterToRows(queueReasons),
    trade_summary: {
      total: tradeSummary.total,
      open: tradeSummary.open,
      closed: tradeSummary.closed,
      wins: tradeSummary.wins,
      losses: tradeSummary.losses,
      win_rate_pct: tradeSummary.closed ? roundNumber((tradeSummary.wins / tradeSummary.closed) * 100, 1) : null,
      avg_pnl_pct: tradeSummary.pnl_n ? roundNumber(tradeSummary.total_pnl_pct / tradeSummary.pnl_n, 2) : null,
      avg_peak_pnl_pct: tradeSummary.peak_n ? roundNumber(tradeSummary.total_peak_pct / tradeSummary.peak_n, 2) : null,
      peak30_n: tradeSummary.peak30_n,
      peak50_n: tradeSummary.peak50_n,
      peak100_n: tradeSummary.peak100_n,
      est_pnl_sol: roundNumber(tradeSummary.est_pnl_sol, 6),
      entry_quote_success_n: tradeSummary.entry_quote_success_n,
      entry_quote_failure_n: tradeSummary.entry_quote_failure_n,
      entry_quote_success_rate_pct: (tradeSummary.entry_quote_success_n + tradeSummary.entry_quote_failure_n)
        ? roundNumber((tradeSummary.entry_quote_success_n / (tradeSummary.entry_quote_success_n + tradeSummary.entry_quote_failure_n)) * 100, 1)
        : null,
    },
    recent_events: recentEvents,
    recent_queue: recentQueue,
    recent_trades: recentTrades,
    notes: {
      unique_counting: 'unique_tokens counts token_ca only; unique_entities also falls back to lifecycle_id where token_ca is missing.',
      funnel_interpretation: 'Stages are not a strict one-row pipeline; decision events, queue rows, and trade rows are joined by target mode/branch over the same time window.',
      action_hint: 'If canary_allow is high but entered is low, inspect queue_reason_summary for quote drift or branch circuit blockers.',
      performance_guardrail: 'Large tables are scanned from the recent id tail and then filtered in memory to keep the live dashboard responsive.',
    },
  };
}

function trustedMissedPeakSqlExpr(cols, qualifier = 'm') {
  let names = ['executable_peak_pnl', 'quote_clean_peak_pnl', 'tradable_peak_pnl']
    .filter((name) => cols.has(name))
    .map((name) => sqlCol(name, qualifier));
  if (names.length === 0) {
    names = ['max_pnl_recorded', 'pnl_24h', 'pnl_60m', 'pnl_15m', 'pnl_5m']
      .filter((name) => cols.has(name))
      .map((name) => sqlCol(name, qualifier));
  }
  return `COALESCE(${[...names, '0'].join(', ')})`;
}

function markMissedPeakSqlExpr(cols, qualifier = 'm') {
  const names = ['theoretical_peak_pnl', 'max_pnl_recorded', 'pnl_24h', 'pnl_60m', 'pnl_15m', 'pnl_5m']
    .filter((name) => cols.has(name))
    .map((name) => sqlCol(name, qualifier));
  return `COALESCE(${[...names, '0'].join(', ')})`;
}

export function buildDogCatchGoalProgress(paperDb, tableNames, sinceTs, options = {}) {
  const targetCatchRate = Number(options.targetCatchRate ?? 0.60);
  const targetWinRate = Number(options.targetWinRate ?? 0.55);
  const targetRoi = Number(options.targetRoi ?? 2.0);
  const dogPeakRatio = Number(options.dogPeakRatio ?? 0.50);
  const winPeakRatio = Number(options.winPeakRatio ?? 0.30);
  const result = {
    available: Boolean(paperDb),
    since_ts: sinceTs,
    targets: {
      clean_gold_silver_capture_rate: targetCatchRate,
      peak_win_rate: targetWinRate,
      realized_roi: targetRoi,
      dog_peak_threshold: dogPeakRatio,
      win_peak_threshold: winPeakRatio,
    },
    trades: {
      fills: 0,
      closed: 0,
      peak_wins: 0,
      peak_win_rate: null,
      captured_gold_silver_unique: 0,
      realized_pnl_sol: 0,
      deployed_sol: 0,
      realized_roi: null,
    },
    missed: {
      clean_gold_silver_unique: 0,
      clean_gold_unique: 0,
      clean_silver_unique: 0,
    },
    goal: {
      eligible_gold_silver_unique: 0,
      captured_gold_silver_unique: 0,
      clean_gold_silver_capture_rate: null,
      pass: false,
      blockers: [],
    },
  };
  if (!paperDb) return result;

  const capturedTokens = new Set();
  if (tableNames.has('paper_trades')) {
    const tradeCols = getTableColumns(paperDb, 'paper_trades');
    const peakExpr = trustedTradePeakSqlExpr(tradeCols, 'pt');
    const pnlExpr = tradeCols.has('pnl_pct') ? 'pt.pnl_pct' : '0';
    const sizeExpr = tradeCols.has('position_size_sol') ? 'pt.position_size_sol' : '0';
    const entryTsExpr = tradeCols.has('entry_ts') ? 'COALESCE(pt.entry_ts, 0)' : '0';
    const exitTsExpr = tradeCols.has('exit_ts') ? 'COALESCE(pt.exit_ts, 0)' : '0';
    const tokenExpr = tradeCols.has('token_ca') ? 'pt.token_ca' : 'NULL';
    const closedExpr = tradeCols.has('exit_ts') || tradeCols.has('exit_reason') || tradeCols.has('pnl_pct')
      ? `CASE WHEN ${[
        tradeCols.has('exit_ts') ? 'pt.exit_ts IS NOT NULL' : null,
        tradeCols.has('exit_reason') ? 'pt.exit_reason IS NOT NULL' : null,
        tradeCols.has('pnl_pct') ? 'pt.pnl_pct IS NOT NULL' : null,
      ].filter(Boolean).join(' OR ')} THEN 1 ELSE 0 END`
      : '0';
    const rows = paperDb.prepare(`
      SELECT ${tokenExpr} AS token_ca,
             ${peakExpr} AS peak_pnl,
             ${pnlExpr} AS pnl_pct,
             ${sizeExpr} AS position_size_sol,
             ${closedExpr} AS closed
      FROM paper_trades pt
      WHERE (${entryTsExpr} >= @since OR ${exitTsExpr} >= @since)
    `).all({ since: sinceTs });
    result.trades.fills = rows.length;
    for (const row of rows) {
      const peak = Number(row.peak_pnl || 0);
      const pnl = Number(row.pnl_pct || 0);
      const size = Number(row.position_size_sol || 0);
      if (Number(row.closed || 0) === 1) result.trades.closed += 1;
      if (peak >= winPeakRatio) result.trades.peak_wins += 1;
      if (peak >= dogPeakRatio && row.token_ca) capturedTokens.add(String(row.token_ca));
      result.trades.deployed_sol += Number.isFinite(size) ? size : 0;
      result.trades.realized_pnl_sol += Number.isFinite(pnl) && Number.isFinite(size) ? pnl * size : 0;
    }
    result.trades.captured_gold_silver_unique = capturedTokens.size;
    result.trades.peak_win_rate = result.trades.fills ? result.trades.peak_wins / result.trades.fills : null;
    result.trades.realized_roi = result.trades.deployed_sol > 0
      ? result.trades.realized_pnl_sol / result.trades.deployed_sol
      : null;
  }

  if (tableNames.has('paper_missed_signal_attribution')) {
    const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
    const tradeColsForMissed = tableNames.has('paper_trades') ? getTableColumns(paperDb, 'paper_trades') : new Set();
    const missedTradeWindowPredicates = [
      tradeColsForMissed.has('entry_ts') ? 'COALESCE(pt.entry_ts, 0) >= @since' : null,
      tradeColsForMissed.has('exit_ts') ? 'COALESCE(pt.exit_ts, 0) >= @since' : null,
    ].filter(Boolean);
    const maxPnlExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
    const eventTsExpr = `COALESCE(${[
      missedCols.has('created_event_ts') ? 'm.created_event_ts' : null,
      missedCols.has('signal_ts') ? 'm.signal_ts' : null,
      missedCols.has('baseline_ts') ? 'm.baseline_ts' : null,
      '0',
    ].filter(Boolean).join(', ')})`;
    const quoteCleanExpr = missedCols.has('tradable_missed')
      ? `COALESCE(m.tradable_missed, 0) = 1 AND COALESCE(${missedCols.has('would_stop_before_peak') ? 'm.would_stop_before_peak' : '0'}, 0) != 1`
      : '0';
    const caughtFilter = tableNames.has('paper_trades') && tradeColsForMissed.has('token_ca') && missedTradeWindowPredicates.length > 0
      ? `AND NOT EXISTS (
          SELECT 1 FROM paper_trades pt
          WHERE pt.token_ca = m.token_ca
            AND (${missedTradeWindowPredicates.join(' OR ')})
        )`
      : '';
    const row = paperDb.prepare(`
      WITH ranked AS (
        SELECT m.token_ca,
               MAX(${maxPnlExpr}) AS max_pnl
        FROM paper_missed_signal_attribution m
        WHERE ${eventTsExpr} >= @since
          AND (${quoteCleanExpr})
          ${caughtFilter}
        GROUP BY m.token_ca
      )
      SELECT
        COALESCE(SUM(CASE WHEN max_pnl >= @dogPeak THEN 1 ELSE 0 END), 0) AS clean_gold_silver,
        COALESCE(SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END), 0) AS clean_gold,
        COALESCE(SUM(CASE WHEN max_pnl >= @dogPeak AND max_pnl < 1.0 THEN 1 ELSE 0 END), 0) AS clean_silver
      FROM ranked
    `).get({ since: sinceTs, dogPeak: dogPeakRatio });
    result.missed.clean_gold_silver_unique = Number(row?.clean_gold_silver || 0);
    result.missed.clean_gold_unique = Number(row?.clean_gold || 0);
    result.missed.clean_silver_unique = Number(row?.clean_silver || 0);
  }

  result.goal.captured_gold_silver_unique = result.trades.captured_gold_silver_unique;
  result.goal.eligible_gold_silver_unique = result.trades.captured_gold_silver_unique + result.missed.clean_gold_silver_unique;
  result.goal.clean_gold_silver_capture_rate = result.goal.eligible_gold_silver_unique
    ? result.goal.captured_gold_silver_unique / result.goal.eligible_gold_silver_unique
    : null;
  if (result.goal.clean_gold_silver_capture_rate == null || result.goal.clean_gold_silver_capture_rate < targetCatchRate) {
    result.goal.blockers.push('clean_gold_silver_capture_rate_below_target');
  }
  if (result.trades.peak_win_rate == null || result.trades.peak_win_rate < targetWinRate) {
    result.goal.blockers.push('peak_win_rate_below_target');
  }
  if (result.trades.realized_roi == null || result.trades.realized_roi < targetRoi) {
    result.goal.blockers.push('realized_roi_below_target');
  }
  result.goal.pass = result.goal.blockers.length === 0;
  return result;
}

function trustedPeakRatio(row = {}) {
  const trusted = Number(row.trusted_peak_pnl);
  if (Number.isFinite(trusted) && trusted > 0) return trusted;
  const quote = Number(row.quote_peak_pnl);
  if (Number.isFinite(quote) && quote > 0) return quote;
  if (row.trusted_peak_pnl === undefined && row.quote_peak_pnl === undefined) {
    const legacy = Number(row.peak_pnl);
    return Number.isFinite(legacy) ? legacy : null;
  }
  return null;
}

function marketSessionForTs(value) {
  const ts = Number(value);
  if (!Number.isFinite(ts) || ts <= 0) return 'unknown';
  const hour = new Date(ts * 1000).getUTCHours();
  if (hour >= 0 && hour < 8) return 'asia';
  if (hour >= 8 && hour < 14) return 'europe';
  if (hour >= 14 && hour < 22) return 'us';
  return 'quiet';
}

function percentileLinear(values, pct) {
  const sorted = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!sorted.length) return null;
  if (sorted.length === 1) return sorted[0];
  const pos = (sorted.length - 1) * pct;
  const lo = Math.floor(pos);
  const hi = Math.min(lo + 1, sorted.length - 1);
  const frac = pos - lo;
  return sorted[lo] * (1 - frac) + sorted[hi] * frac;
}

function getPaperReviewDir() {
  const raw = process.env.PAPER_REVIEW_DIR || join(dirname(getPaperDbPath()), 'review-artifacts');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function getLivePaperReviewDir() {
  const raw = process.env.PAPER_REVIEW_LIVE_DIR || join(getPaperReviewDir(), 'live');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function livePaperReviewPath(hours) {
  const safeHours = Math.max(1, Math.min(24, Number.parseInt(String(hours || 24), 10) || 24));
  return join(getLivePaperReviewDir(), `paper_review_${safeHours}h.json`);
}

function writeLivePaperReviewSnapshot(hours, snapshot) {
  const target = livePaperReviewPath(hours);
  fs.mkdirSync(dirname(target), { recursive: true });
  const tmp = `${target}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
  fs.renameSync(tmp, target);
  return target;
}

function paperReviewSnapshotRefreshStatusPath() {
  const raw = process.env.PAPER_REVIEW_SNAPSHOT_REFRESH_STATUS
    || join(dirname(getPaperDbPath()), 'paper-review-snapshot-refresh-status.json');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

function paperReviewSnapshotRefreshLogPath() {
  const raw = process.env.PAPER_REVIEW_SNAPSHOT_REFRESH_LOG
    || join(dirname(getPaperDbPath()), 'paper-review-snapshot-refresh.log');
  return isAbsolute(raw) ? raw : join(projectRoot, raw);
}

let paperReviewSnapshotRefreshRunner = {
  running: false,
  pid: null,
  started_at: null,
  run_id: null,
};

function readPaperReviewSnapshotRefreshStatus() {
  const statusPath = paperReviewSnapshotRefreshStatusPath();
  const status = safeReadAgentJson(statusPath);
  if (!status || status.error_code) {
    return {
      schema_version: 'paper_review_snapshot_refresh_status.v1',
      available: false,
      running: Boolean(paperReviewSnapshotRefreshRunner.running && processIsAlive(paperReviewSnapshotRefreshRunner.pid)),
      pid: paperReviewSnapshotRefreshRunner.pid || null,
      status_path: statusPath,
      log_path: paperReviewSnapshotRefreshLogPath(),
      live_snapshot_health: readPaperReviewSnapshotHealth(),
    };
  }
  const startedMs = Date.parse(status.started_at || '');
  const ageMs = Number.isFinite(startedMs) ? Date.now() - startedMs : null;
  const staleRunning = Boolean(status.running && ageMs != null && ageMs > 3 * 60 * 60 * 1000);
  const running = Boolean(status.running && !staleRunning && processIsAlive(status.pid));
  return {
    ...status,
    available: true,
    running,
    stale_running_status: staleRunning,
    age_minutes: ageMs == null ? null : +(ageMs / 60000).toFixed(2),
    pid_alive: processIsAlive(status.pid),
    live_snapshot_health: readPaperReviewSnapshotHealth(),
  };
}

function triggerPaperReviewSnapshotRefresh(url) {
  const current = readPaperReviewSnapshotRefreshStatus();
  if (current.running) {
    return {
      accepted: false,
      status: 'already_running',
      runner: current,
      promotion_allowed: false,
      strategy_change_allowed: false,
      automatic_runtime_change_allowed: false,
      paper_enablement_allowed: false,
    };
  }
  const windows = sanitizeCaptureHours(url.searchParams.get('windows') || url.searchParams.get('hours') || '24', 24);
  const limit = boundedIntParam(url, 'limit', 40, 1, 500);
  const timeoutMs = boundedIntParam(url, 'timeout_sec', 1800, 60, 7200) * 1000;
  const runId = `paper_review_${new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z')}_${randomUUID().slice(0, 8)}`;
  const startedAt = new Date().toISOString();
  const statusPath = paperReviewSnapshotRefreshStatusPath();
  const logPath = paperReviewSnapshotRefreshLogPath();
  const lockPath = process.env.PAPER_REVIEW_SNAPSHOT_API_LOCK_FILE || '/tmp/paper_review_snapshot_api_refresh.lock';
  const args = [
    'scripts/paper_review_snapshot_worker.py',
    '--paper-db', getPaperDbPath(),
    '--out-dir', getLivePaperReviewDir(),
    '--windows', windows,
    '--limit', String(limit),
    '--lock-file', lockPath,
  ];
  fs.mkdirSync(dirname(logPath), { recursive: true });
  fs.mkdirSync(dirname(statusPath), { recursive: true });
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[paper-review-refresh] ${startedAt} start run_id=${runId} python3 ${args.join(' ')}\n`);
  const child = spawn('python3', args, {
    cwd: projectRoot,
    env: {
      ...process.env,
      PYTHONUNBUFFERED: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  paperReviewSnapshotRefreshRunner = {
    running: true,
    pid: child.pid,
    started_at: startedAt,
    run_id: runId,
  };
  const baseStatus = {
    schema_version: 'paper_review_snapshot_refresh_status.v1',
    run_id: runId,
    running: true,
    pid: child.pid,
    started_at: startedAt,
    finished_at: null,
    command: ['python3', ...args],
    status_path: statusPath,
    log_path: logPath,
    live_dir: getLivePaperReviewDir(),
    windows,
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
    notes: [
      'Read-only paper review snapshot materialization only.',
      'Does not modify strategy, gates, A_CLASS, executor, wallet, or risk settings.',
    ],
  };
  safeWriteAgentJson(statusPath, baseStatus);
  child.stdout.on('data', (chunk) => writeRedactedLogStream(logStream, chunk));
  child.stderr.on('data', (chunk) => writeRedactedLogStream(logStream, chunk));

  let finished = false;
  const finish = (error, code, signal, timedOut = false) => {
    if (finished) return;
    finished = true;
    clearTimeout(timeoutHandle);
    const finishedAt = new Date().toISOString();
    const status = {
      ...baseStatus,
      running: false,
      finished_at: finishedAt,
      exit_code: code ?? null,
      signal: signal || null,
      timed_out: Boolean(timedOut),
      error: error ? error.message : null,
      live_snapshot_health: readPaperReviewSnapshotHealth(),
    };
    paperReviewSnapshotRefreshRunner = {
      running: false,
      pid: child.pid,
      started_at: startedAt,
      finished_at: finishedAt,
      run_id: runId,
    };
    writeRedactedLogStream(logStream, `[paper-review-refresh] ${finishedAt} finish run_id=${runId} code=${code ?? ''} signal=${signal || ''} timed_out=${Boolean(timedOut)} error=${error?.message || ''}\n`);
    try { safeWriteAgentJson(statusPath, status); } catch {}
    try { logStream.end(); } catch {}
  };
  const timeoutHandle = setTimeout(() => {
    try { child.kill('SIGTERM'); } catch {}
    finish(new Error(`timeout_after_${Math.floor(timeoutMs / 1000)}s`), null, 'SIGTERM', true);
  }, timeoutMs);
  child.on('error', (error) => finish(error, null, null, false));
  child.on('exit', (code, signal) => finish(code === 0 ? null : new Error(`exit_${code ?? signal}`), code, signal, false));

  return {
    accepted: true,
    status: 'started',
    runner: readPaperReviewSnapshotRefreshStatus(),
    command: ['python3', ...args],
    log_path: logPath,
    status_path: statusPath,
    promotion_allowed: false,
    strategy_change_allowed: false,
    automatic_runtime_change_allowed: false,
    paper_enablement_allowed: false,
  };
}

function nearestLivePaperReviewHours(requestedHours) {
  const requested = Math.max(1, Number.parseInt(String(requestedHours || 24), 10) || 24);
  const windows = String(process.env.PAPER_REVIEW_WINDOWS || '2,8,12,24')
    .split(',')
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b);
  return windows.find((hours) => hours >= requested) || windows[windows.length - 1] || Math.min(requested, 24);
}

function readLivePaperReview(hours) {
  const path = livePaperReviewPath(hours);
  if (!fs.existsSync(path)) return null;
  try {
    return JSON.parse(fs.readFileSync(path, 'utf8'));
  } catch (error) {
    return { error: error.message, path };
  }
}

function snapshotAgeMinutes(snapshot, nowMs = Date.now()) {
  const generatedAt = snapshot?.generated_at;
  if (!generatedAt) return null;
  const generatedMs = Date.parse(generatedAt);
  if (!Number.isFinite(generatedMs)) return null;
  return roundNumber(Math.max(0, nowMs - generatedMs) / 60000, 1);
}

export function readPaperReviewSnapshotHealth(options = {}) {
  const requestedHours = Math.max(1, Math.min(24, Number.parseInt(String(options.hours ?? options.requestedHours ?? 24), 10) || 24));
  const materializedHours = options.materializedHours == null
    ? nearestLivePaperReviewHours(requestedHours)
    : Math.max(1, Math.min(24, Number.parseInt(String(options.materializedHours), 10) || requestedHours));
  const maxAgeMinutes = Math.max(
    1,
    Math.min(
      1440,
      Number.parseInt(String(options.maxAgeMinutes ?? process.env.PAPER_REVIEW_SNAPSHOT_MAX_AGE_MINUTES ?? '30'), 10) || 30
    )
  );
  const nowMs = Number.isFinite(Number(options.nowMs)) ? Number(options.nowMs) : Date.now();
  const path = livePaperReviewPath(materializedHours);
  if (!fs.existsSync(path)) {
    return {
      available: false,
      status: 'paper_review_snapshot_missing',
      path,
      requested_hours: requestedHours,
      materialized_hours: materializedHours,
      max_age_minutes: maxAgeMinutes,
    };
  }
  const snapshot = readLivePaperReview(materializedHours);
  if (!snapshot || snapshot.error) {
    return {
      available: false,
      status: 'paper_review_snapshot_invalid',
      path,
      requested_hours: requestedHours,
      materialized_hours: materializedHours,
      max_age_minutes: maxAgeMinutes,
      error: snapshot?.error || 'paper_review_snapshot_unreadable',
    };
  }
  const ageMinutes = snapshotAgeMinutes(snapshot, nowMs);
  const fresh = Boolean(ageMinutes != null && ageMinutes <= maxAgeMinutes);
  return {
    available: true,
    status: fresh ? 'ok' : 'paper_review_snapshot_stale_or_undated',
    path,
    requested_hours: requestedHours,
    materialized_hours: materializedHours,
    generated_at: snapshot.generated_at || null,
    snapshot_id: snapshot.snapshot_id || null,
    age_minutes: ageMinutes,
    max_age_minutes: maxAgeMinutes,
    fresh,
  };
}

function v27ReadModelDir(options = {}) {
  const root = options.projectRoot || projectRoot;
  const raw = options.readModelDir || process.env.V27_READ_MODEL_DIR || join(root, 'data', 'v27_read_models');
  return isAbsolute(raw) ? raw : join(root, raw);
}

function v27DenominatorFreshnessPath(options = {}) {
  const root = options.projectRoot || projectRoot;
  const raw = options.healthPath || process.env.V27_DENOMINATOR_FRESHNESS_PATH || join(v27ReadModelDir(options), 'denominator_freshness.json');
  return isAbsolute(raw) ? raw : join(root, raw);
}

function v27ModeReadinessPath(options = {}) {
  const root = options.projectRoot || projectRoot;
  const raw = options.modeReadinessPath || process.env.V27_MODE_READINESS_PATH || join(v27ReadModelDir(options), 'mode_readiness.json');
  return isAbsolute(raw) ? raw : join(root, raw);
}

let v27ReadModelManualRefresh = {
  running: false,
  started_at: null,
  pid: null,
};

function triggerV27ReadModelRefresh(options = {}) {
  if (v27ReadModelManualRefresh.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27ReadModelManualRefresh,
    };
  }
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const outputDir = process.env.V27_READ_MODEL_DIR || './data/v27_read_models';
  const lockFile = process.env.V27_READ_MODEL_REFRESH_LOCK_FILE || '/tmp/v27_read_model_refresh.lock';
  const timeoutMs = Number(options.timeoutMs || process.env.V27_READ_MODEL_MANUAL_REFRESH_TIMEOUT_MS || 600000);
  const logPathRaw = process.env.V27_READ_MODEL_REFRESH_LOG || join(outputDir, '..', 'v27-read-model-refresh.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_read_model_refresh.py',
    '--event-log-dir', eventLogDir,
    '--output-dir', outputDir,
    '--lock-file', lockFile,
    '--progress',
  ];
  if (options.includeRecords) args.push('--include-records');
  if (options.strict) args.push('--strict');

  fs.mkdirSync(dirname(logPath), { recursive: true });
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-read-model-refresh-once: python3 ${args.join(' ')}\n`);

  const child = spawn('python3', args, {
    cwd: projectRoot,
    env: {
      ...process.env,
      PYTHONUNBUFFERED: '1',
      V27_EVENT_LOG_DIR: eventLogDir,
      V27_READ_MODEL_DIR: outputDir,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let timedOut = false;
  let finished = false;
  const timeoutHandle = setTimeout(() => {
    timedOut = true;
    try { child.kill('SIGTERM'); } catch {}
  }, timeoutMs);
  const finish = (error, code, signal) => {
    if (finished) return;
    finished = true;
    clearTimeout(timeoutHandle);
    const finishedAt = new Date().toISOString();
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-read-model-refresh-once failed code=${code || ''} signal=${signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-read-model-refresh-once completed\n`);
    }
    v27ReadModelManualRefresh = {
      running: false,
      started_at: startedAt,
      finished_at: finishedAt,
      pid: child.pid,
      error: error ? error.message : null,
    };
    try { logStream.end(); } catch {}
  };

  child.stdout.on('data', (chunk) => {
    writeRedactedLogStream(logStream, chunk);
  });
  child.stderr.on('data', (chunk) => {
    writeRedactedLogStream(logStream, chunk);
  });
  child.on('error', (error) => {
    finish(error, null, null);
  });
  child.on('close', (code, signal) => {
    if (code === 0 && !signal && !timedOut) {
      finish(null, code, signal);
      return;
    }
    const reason = timedOut ? `v27-read-model-refresh timed out after ${timeoutMs}ms` : `v27-read-model-refresh exited code=${code || ''} signal=${signal || ''}`;
    finish(new Error(reason), code, signal);
  });

  v27ReadModelManualRefresh = {
    running: true,
    started_at: startedAt,
    pid: child.pid,
    event_log_dir: eventLogDir,
    output_dir: outputDir,
    log_path: logPath,
    timeout_ms: timeoutMs,
  };
  return {
    accepted: true,
    status: 'started',
    ...v27ReadModelManualRefresh,
  };
}

let v27RecoveryControlManualMirror = {
  running: false,
  started_at: null,
  pid: null,
};

function triggerV27RecoveryControlMirror(options = {}) {
  if (v27RecoveryControlManualMirror.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27RecoveryControlManualMirror,
    };
  }
  const paperDbPath = getPaperDbPath();
  const signalDbPath = resolvedDbPath;
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const environmentId = options.environmentId || process.env.V27_ENVIRONMENT_ID || process.env.NODE_ENV || 'local';
  const recoveryVersion = options.recoveryVersion || process.env.V27_RECOVERY_CONTROL_VERSION || 'legacy_paper_recovery_control_v0.1';
  const timeoutMs = Math.max(30000, Math.min(options.timeoutMs || Number(process.env.V27_RECOVERY_CONTROL_MANUAL_TIMEOUT_MS || 600000) || 600000, 1800000));
  const logPathRaw = process.env.V27_RECOVERY_CONTROL_MIRROR_LOG || join(projectRoot, 'data', 'v27-recovery-control-mirror.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_mirror_recovery_controls.py',
    '--new-only',
    '--paper-db',
    paperDbPath,
    '--signal-db',
    signalDbPath,
    '--event-log-dir',
    eventLogDir,
    '--recovery-version',
    recoveryVersion,
    '--environment-id',
    environmentId,
  ];
  const env = {
    ...process.env,
    V27_EVENT_LOG_DIR: eventLogDir,
    V27_ENVIRONMENT_ID: environmentId,
    V27_RECOVERY_CONTROL_VERSION: recoveryVersion,
  };
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-recovery-control-mirror-once: python3 ${args.join(' ')}\n`);
  v27RecoveryControlManualMirror = {
    running: true,
    started_at: startedAt,
    pid: null,
  };
  const child = execFile('python3', args, {
    cwd: projectRoot,
    env,
    timeout: timeoutMs,
    killSignal: 'SIGTERM',
    maxBuffer: 50 * 1024 * 1024,
  }, (error, stdout, stderr) => {
    const finishedAt = new Date().toISOString();
    if (stdout) writeRedactedLogStream(logStream, String(stdout));
    if (stderr) writeRedactedLogStream(logStream, String(stderr));
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-recovery-control-mirror-once failed code=${error.code || ''} signal=${error.signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-recovery-control-mirror-once completed\n`);
    }
    v27RecoveryControlManualMirror = {
      running: false,
      started_at: startedAt,
      pid: child?.pid || null,
      completed_at: finishedAt,
      exit_code: error ? error.code ?? null : 0,
      exit_signal: error ? error.signal ?? null : null,
      error: error ? error.message : null,
      log_path: logPath,
    };
    logStream.end();
  });
  v27RecoveryControlManualMirror = {
    ...v27RecoveryControlManualMirror,
    pid: child.pid,
    log_path: logPath,
    timeout_ms: timeoutMs,
  };
  return {
    accepted: true,
    status: 'started',
    ...v27RecoveryControlManualMirror,
  };
}

let v27RawProviderEvidenceManualMirror = {
  running: false,
  started_at: null,
  pid: null,
};

let v27RawProviderProbeEvidenceManualRecord = {
  running: false,
  started_at: null,
  pid: null,
};

let v27RandomnessControlManualMirror = {
  running: false,
  started_at: null,
  pid: null,
};

function triggerV27RawProviderEvidenceMirror(options = {}) {
  if (v27RawProviderEvidenceManualMirror.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27RawProviderEvidenceManualMirror,
    };
  }
  const paperDbPath = getPaperDbPath();
  const signalDbPath = resolvedDbPath;
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const evidenceVersion = options.evidenceVersion || process.env.V27_RAW_PROVIDER_EVIDENCE_VERSION || 'legacy_paper_raw_provider_evidence_v0.1';
  const defaultProvider = options.defaultProvider || process.env.V27_RAW_PROVIDER_DEFAULT_PROVIDER || 'jupiter_ultra';
  const defaultEndpoint = options.defaultEndpoint || process.env.V27_RAW_PROVIDER_DEFAULT_ENDPOINT || '/ultra/v1/order';
  const cursorOverlapIdsRaw = options.cursorOverlapIds ?? process.env.V27_RAW_PROVIDER_CURSOR_OVERLAP_IDS ?? 100;
  const cursorOverlapIds = Math.max(0, Number(cursorOverlapIdsRaw) || 0);
  const limitRaw = options.limit ?? process.env.V27_RAW_PROVIDER_EVIDENCE_MIRROR_LIMIT ?? 500;
  const limit = limitRaw === null ? null : Math.max(1, Number(limitRaw) || 500);
  const timeoutMs = Math.max(30000, Math.min(options.timeoutMs || Number(process.env.V27_RAW_PROVIDER_EVIDENCE_MANUAL_TIMEOUT_MS || 600000) || 600000, 1800000));
  const logPathRaw = process.env.V27_RAW_PROVIDER_EVIDENCE_MIRROR_LOG || join(projectRoot, 'data', 'v27-raw-provider-evidence-mirror.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_mirror_raw_provider_evidence.py',
    '--new-only',
    '--paper-db',
    paperDbPath,
    '--signal-db',
    signalDbPath,
    '--event-log-dir',
    eventLogDir,
    '--evidence-version',
    evidenceVersion,
    '--default-provider',
    defaultProvider,
    '--default-endpoint',
    defaultEndpoint,
    '--cursor-overlap-ids',
    String(cursorOverlapIds),
  ];
  if (options.sinceId !== undefined && options.sinceId !== null) args.push('--since-id', String(options.sinceId));
  if (options.untilId !== undefined && options.untilId !== null) args.push('--until-id', String(options.untilId));
  if (limit !== null) args.push('--limit', String(limit));
  if (options.dryRun) args.push('--dry-run');
  if (options.strict) args.push('--strict');
  if (options.trustedOnly) args.push('--trusted-only');
  const env = {
    ...process.env,
    V27_EVENT_LOG_DIR: eventLogDir,
    V27_RAW_PROVIDER_EVIDENCE_VERSION: evidenceVersion,
    V27_RAW_PROVIDER_DEFAULT_PROVIDER: defaultProvider,
    V27_RAW_PROVIDER_DEFAULT_ENDPOINT: defaultEndpoint,
  };
  fs.mkdirSync(dirname(logPath), { recursive: true });
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-raw-provider-evidence-mirror-once: python3 ${args.join(' ')}\n`);
  v27RawProviderEvidenceManualMirror = {
    running: true,
    started_at: startedAt,
    pid: null,
  };
  const child = execFile('python3', args, {
    cwd: projectRoot,
    env,
    timeout: timeoutMs,
    killSignal: 'SIGTERM',
    maxBuffer: 50 * 1024 * 1024,
  }, (error, stdout, stderr) => {
    const finishedAt = new Date().toISOString();
    if (stdout) writeRedactedLogStream(logStream, String(stdout));
    if (stderr) writeRedactedLogStream(logStream, String(stderr));
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-raw-provider-evidence-mirror-once failed code=${error.code || ''} signal=${error.signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-raw-provider-evidence-mirror-once completed\n`);
    }
    v27RawProviderEvidenceManualMirror = {
      running: false,
      started_at: startedAt,
      pid: child?.pid || null,
      completed_at: finishedAt,
      exit_code: error ? error.code ?? null : 0,
      exit_signal: error ? error.signal ?? null : null,
      error: error ? error.message : null,
      log_path: logPath,
    };
    logStream.end();
  });
  v27RawProviderEvidenceManualMirror = {
    ...v27RawProviderEvidenceManualMirror,
    pid: child.pid,
    log_path: logPath,
    timeout_ms: timeoutMs,
    limit,
    dry_run: Boolean(options.dryRun),
    trusted_only: Boolean(options.trustedOnly),
  };
  return {
    accepted: true,
    status: 'started',
    ...v27RawProviderEvidenceManualMirror,
  };
}

function triggerV27RawProviderProbeEvidence(options = {}) {
  if (v27RawProviderProbeEvidenceManualRecord.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27RawProviderProbeEvidenceManualRecord,
    };
  }
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const evidenceVersion = options.evidenceVersion || process.env.V27_RAW_PROVIDER_EVIDENCE_VERSION || 'legacy_paper_raw_provider_evidence_v0.1';
  const provider = options.provider || process.env.V27_RAW_PROVIDER_DEFAULT_PROVIDER || 'jupiter_ultra';
  const endpoint = options.endpoint || process.env.V27_RAW_PROVIDER_DEFAULT_ENDPOINT || '/ultra/v1/order';
  const timeoutMs = Math.max(30000, Math.min(options.timeoutMs || Number(process.env.V27_RAW_PROVIDER_PROBE_EVIDENCE_TIMEOUT_MS || 600000) || 600000, 1800000));
  const logPathRaw = process.env.V27_RAW_PROVIDER_PROBE_EVIDENCE_LOG || join(projectRoot, 'data', 'v27-raw-provider-probe-evidence.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_record_raw_provider_probe_evidence.py',
    '--event-log-dir',
    eventLogDir,
    '--evidence-version',
    evidenceVersion,
    '--provider',
    provider,
    '--endpoint',
    endpoint,
  ];
  if (options.runId) args.push('--run-id', String(options.runId));
  if (options.endpointBase) args.push('--endpoint-base', String(options.endpointBase));
  if (options.inputMint) args.push('--input-mint', String(options.inputMint));
  if (options.outputMint) args.push('--output-mint', String(options.outputMint));
  if (options.outputSymbol) args.push('--output-symbol', String(options.outputSymbol));
  if (options.amountRaw) args.push('--amount-raw', String(options.amountRaw));
  if (options.slippageBps !== undefined && options.slippageBps !== null) args.push('--slippage-bps', String(options.slippageBps));
  if (options.timeoutSec !== undefined && options.timeoutSec !== null) args.push('--timeout-sec', String(options.timeoutSec));
  if (options.dryRun) args.push('--dry-run');
  if (options.strict) args.push('--strict');
  const env = {
    ...process.env,
    V27_EVENT_LOG_DIR: eventLogDir,
    V27_RAW_PROVIDER_EVIDENCE_VERSION: evidenceVersion,
    V27_RAW_PROVIDER_DEFAULT_PROVIDER: provider,
    V27_RAW_PROVIDER_DEFAULT_ENDPOINT: endpoint,
  };
  fs.mkdirSync(dirname(logPath), { recursive: true });
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-raw-provider-probe-evidence-once: python3 ${args.join(' ')}\n`);
  v27RawProviderProbeEvidenceManualRecord = {
    running: true,
    started_at: startedAt,
    pid: null,
  };
  const child = execFile('python3', args, {
    cwd: projectRoot,
    env,
    timeout: timeoutMs,
    killSignal: 'SIGTERM',
    maxBuffer: 50 * 1024 * 1024,
  }, (error, stdout, stderr) => {
    const finishedAt = new Date().toISOString();
    if (stdout) writeRedactedLogStream(logStream, String(stdout));
    if (stderr) writeRedactedLogStream(logStream, String(stderr));
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-raw-provider-probe-evidence-once failed code=${error.code || ''} signal=${error.signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-raw-provider-probe-evidence-once completed\n`);
    }
    v27RawProviderProbeEvidenceManualRecord = {
      running: false,
      started_at: startedAt,
      pid: child?.pid || null,
      completed_at: finishedAt,
      exit_code: error ? error.code ?? null : 0,
      exit_signal: error ? error.signal ?? null : null,
      error: error ? error.message : null,
      log_path: logPath,
    };
    logStream.end();
  });
  v27RawProviderProbeEvidenceManualRecord = {
    ...v27RawProviderProbeEvidenceManualRecord,
    pid: child.pid,
    log_path: logPath,
    timeout_ms: timeoutMs,
    dry_run: Boolean(options.dryRun),
    strict: Boolean(options.strict),
  };
  return {
    accepted: true,
    status: 'started',
    ...v27RawProviderProbeEvidenceManualRecord,
  };
}

function triggerV27RandomnessControlMirror(options = {}) {
  if (v27RandomnessControlManualMirror.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27RandomnessControlManualMirror,
    };
  }
  const signalDbPath = resolvedDbPath;
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const auditVersion = options.auditVersion || process.env.V27_RANDOMNESS_CONTROL_AUDIT_VERSION || 'legacy_strategy_experiment_randomness_control_v0.1';
  const defaultRandomizationUnit = options.defaultRandomizationUnit || process.env.V27_RANDOMNESS_CONTROL_DEFAULT_UNIT || 'strategy_experiment_candidate';
  const environmentId = options.environmentId || process.env.V27_ENVIRONMENT_ID || process.env.NODE_ENV || 'production';
  const limitRaw = options.limit ?? process.env.V27_RANDOMNESS_CONTROL_MIRROR_LIMIT ?? 500;
  const limit = limitRaw === null ? null : Math.max(1, Number(limitRaw) || 500);
  const timeoutMs = Math.max(30000, Math.min(options.timeoutMs || Number(process.env.V27_RANDOMNESS_CONTROL_MANUAL_TIMEOUT_MS || 600000) || 600000, 1800000));
  const logPathRaw = process.env.V27_RANDOMNESS_CONTROL_MIRROR_LOG || join(projectRoot, 'data', 'v27-randomness-control-mirror.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_mirror_randomness_controls.py',
    '--new-only',
    '--db',
    signalDbPath,
    '--event-log-dir',
    eventLogDir,
    '--audit-version',
    auditVersion,
    '--default-randomization-unit',
    defaultRandomizationUnit,
    '--environment-id',
    environmentId,
  ];
  if (options.sinceCreatedAt) args.push('--since-created-at', String(options.sinceCreatedAt));
  if (options.untilCreatedAt) args.push('--until-created-at', String(options.untilCreatedAt));
  if (Array.isArray(options.statuses)) {
    for (const status of options.statuses) {
      if (status) args.push('--status', String(status));
    }
  }
  if (limit !== null) args.push('--limit', String(limit));
  if (options.dryRun) args.push('--dry-run');
  if (options.strict) args.push('--strict');
  const env = {
    ...process.env,
    DB_PATH: signalDbPath,
    SENTIMENT_DB: signalDbPath,
    V27_EVENT_LOG_DIR: eventLogDir,
    V27_ENVIRONMENT_ID: environmentId,
    V27_RANDOMNESS_CONTROL_AUDIT_VERSION: auditVersion,
    V27_RANDOMNESS_CONTROL_DEFAULT_UNIT: defaultRandomizationUnit,
  };
  fs.mkdirSync(dirname(logPath), { recursive: true });
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-randomness-control-mirror-once: python3 ${args.join(' ')}\n`);
  v27RandomnessControlManualMirror = {
    running: true,
    started_at: startedAt,
    pid: null,
  };
  const child = execFile('python3', args, {
    cwd: projectRoot,
    env,
    timeout: timeoutMs,
    killSignal: 'SIGTERM',
    maxBuffer: 50 * 1024 * 1024,
  }, (error, stdout, stderr) => {
    const finishedAt = new Date().toISOString();
    if (stdout) writeRedactedLogStream(logStream, String(stdout));
    if (stderr) writeRedactedLogStream(logStream, String(stderr));
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-randomness-control-mirror-once failed code=${error.code || ''} signal=${error.signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-randomness-control-mirror-once completed\n`);
    }
    v27RandomnessControlManualMirror = {
      running: false,
      started_at: startedAt,
      pid: child?.pid || null,
      completed_at: finishedAt,
      exit_code: error ? error.code ?? null : 0,
      exit_signal: error ? error.signal ?? null : null,
      error: error ? error.message : null,
      log_path: logPath,
    };
    logStream.end();
  });
  v27RandomnessControlManualMirror = {
    ...v27RandomnessControlManualMirror,
    pid: child.pid,
    log_path: logPath,
    timeout_ms: timeoutMs,
    limit,
    dry_run: Boolean(options.dryRun),
    statuses: options.statuses || [],
  };
  return {
    accepted: true,
    status: 'started',
    ...v27RandomnessControlManualMirror,
  };
}

let v27NormalTinyOpsEvidenceManualRecord = {
  running: false,
  started_at: null,
  pid: null,
};

function triggerV27NormalTinyOpsEvidence(options = {}) {
  if (v27NormalTinyOpsEvidenceManualRecord.running) {
    return {
      accepted: false,
      status: 'already_running',
      ...v27NormalTinyOpsEvidenceManualRecord,
    };
  }
  const eventLogDir = process.env.V27_EVENT_LOG_DIR || './data/v27_event_log';
  const timeoutMs = Math.max(30000, Math.min(options.timeoutMs || Number(process.env.V27_NORMAL_TINY_OPS_EVIDENCE_TIMEOUT_MS || 600000) || 600000, 1800000));
  const logPathRaw = process.env.V27_NORMAL_TINY_OPS_EVIDENCE_LOG || join(projectRoot, 'data', 'v27-normal-tiny-ops-evidence.log');
  const logPath = isAbsolute(logPathRaw) ? logPathRaw : join(projectRoot, logPathRaw);
  const args = [
    'scripts/v27_record_normal_tiny_ops_evidence.py',
    '--event-log-dir',
    eventLogDir,
  ];
  if (options.runId) args.push('--run-id', String(options.runId));
  if (options.scratchDir) args.push('--scratch-dir', String(options.scratchDir));
  if (Array.isArray(options.workerRoles)) {
    for (const role of options.workerRoles) {
      if (role) args.push('--worker-role', String(role));
    }
  }
  if (options.dryRun) args.push('--dry-run');
  if (options.strict) args.push('--strict');
  const env = {
    ...process.env,
    V27_EVENT_LOG_DIR: eventLogDir,
  };
  fs.mkdirSync(dirname(logPath), { recursive: true });
  const startedAt = new Date().toISOString();
  const logStream = fs.createWriteStream(logPath, { flags: 'a' });
  writeRedactedLogStream(logStream, `[dashboard-trigger] ${startedAt} starting v27-normal-tiny-ops-evidence-once: python3 ${args.join(' ')}\n`);
  v27NormalTinyOpsEvidenceManualRecord = {
    running: true,
    started_at: startedAt,
    pid: null,
  };
  const child = execFile('python3', args, {
    cwd: projectRoot,
    env,
    timeout: timeoutMs,
    killSignal: 'SIGTERM',
    maxBuffer: 50 * 1024 * 1024,
  }, (error, stdout, stderr) => {
    const finishedAt = new Date().toISOString();
    if (stdout) writeRedactedLogStream(logStream, String(stdout));
    if (stderr) writeRedactedLogStream(logStream, String(stderr));
    if (error) {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-normal-tiny-ops-evidence-once failed code=${error.code || ''} signal=${error.signal || ''} error=${error.message}\n`);
    } else {
      writeRedactedLogStream(logStream, `[dashboard-trigger] ${finishedAt} v27-normal-tiny-ops-evidence-once completed\n`);
    }
    v27NormalTinyOpsEvidenceManualRecord = {
      running: false,
      started_at: startedAt,
      pid: child?.pid || null,
      completed_at: finishedAt,
      exit_code: error ? error.code ?? null : 0,
      exit_signal: error ? error.signal ?? null : null,
      error: error ? error.message : null,
      log_path: logPath,
    };
    logStream.end();
  });
  v27NormalTinyOpsEvidenceManualRecord = {
    ...v27NormalTinyOpsEvidenceManualRecord,
    pid: child.pid,
    log_path: logPath,
    timeout_ms: timeoutMs,
    dry_run: Boolean(options.dryRun),
    strict: Boolean(options.strict),
  };
  return {
    accepted: true,
    status: 'started',
    ...v27NormalTinyOpsEvidenceManualRecord,
  };
}

export function readV27ModeReadiness(options = {}) {
  const path = v27ModeReadinessPath(options);
  const generatedAt = new Date().toISOString();
  if (!fs.existsSync(path)) {
    return {
      generated_at: generatedAt,
      available: false,
      materialized: true,
      path,
      highest_allowed_mode: null,
      blocking_reasons: ['v27_mode_readiness_missing'],
      health: {
        observe_only_ready: false,
        shadow_ready: false,
        ultra_tiny_ready: false,
        normal_tiny_ready: false,
        status: 'v27_mode_readiness_missing',
      },
    };
  }
  try {
    const payload = normalizeV27ModeReadinessPayload(JSON.parse(fs.readFileSync(path, 'utf8')));
    return {
      generated_at: generatedAt,
      available: true,
      materialized: true,
      path,
      ...payload,
    };
  } catch (error) {
    return {
      generated_at: generatedAt,
      available: false,
      materialized: true,
      path,
      highest_allowed_mode: null,
      blocking_reasons: ['v27_mode_readiness_parse_failed'],
      error: error.message,
      health: {
        observe_only_ready: false,
        shadow_ready: false,
        ultra_tiny_ready: false,
        normal_tiny_ready: false,
        status: 'v27_mode_readiness_parse_failed',
      },
    };
  }
}

export function buildV27KpiProofStatus(options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const nowMs = Number.isFinite(Number(options.nowMs)) ? Number(options.nowMs) : Date.parse(generatedAt) || Date.now();
  const requestedHours = Math.max(1, Math.min(72, parseInt(String(options.requestedHours ?? 24), 10) || 24));
  const materializedSnapshotHours = Math.min(requestedHours, 24);
  const maxSnapshotAgeMinutes = Math.max(1, Math.min(1440, parseInt(String(options.maxSnapshotAgeMinutes ?? 30), 10) || 30));
  const targetCatchRate = Number.isFinite(Number(options.targetCatchRate)) ? Number(options.targetCatchRate) : 0.60;
  const targetWinRate = Number.isFinite(Number(options.targetWinRate)) ? Number(options.targetWinRate) : 0.55;
  const targetRoi = Number.isFinite(Number(options.targetRoi)) ? Number(options.targetRoi) : 2.0;
  const dogPeakRatio = Number.isFinite(Number(options.dogPeakRatio)) ? Number(options.dogPeakRatio) : 0.50;
  const winPeakRatio = Number.isFinite(Number(options.winPeakRatio)) ? Number(options.winPeakRatio) : 0.30;
  const paperDbPath = options.paperDbPath || getPaperDbPath();
  const paperDbExists = options.paperDbExists === undefined ? fs.existsSync(paperDbPath) : Boolean(options.paperDbExists);
  const dashboardTokenConfigured = options.dashboardTokenConfigured === undefined
    ? Boolean(DASHBOARD_TOKEN)
    : Boolean(options.dashboardTokenConfigured);
  const modeReadiness = options.modeReadiness === undefined ? readV27ModeReadiness() : options.modeReadiness;
  const denominatorHealth = options.denominatorHealth === undefined ? readV27DenominatorReadModelHealth() : options.denominatorHealth;
  const liveSnapshot = options.liveSnapshot === undefined ? readLivePaperReview(materializedSnapshotHours) : options.liveSnapshot;
  const snapshotAvailable = Boolean(liveSnapshot && !liveSnapshot.error);
  const snapshotAge = snapshotAvailable ? snapshotAgeMinutes(liveSnapshot, nowMs) : null;
  const snapshotFresh = Boolean(snapshotAvailable && snapshotAge != null && snapshotAge <= maxSnapshotAgeMinutes);
  const dogCatchGoal = snapshotAvailable ? dogCatchGoalFromLiveSnapshot(liveSnapshot, {
    dbPath: paperDbPath,
    requestedHours,
    options: {
      targetCatchRate,
      targetWinRate,
      targetRoi,
      dogPeakRatio,
      winPeakRatio,
    },
  }) : null;
  const failureAttribution = publicDogCatchFailureAttribution(dogCatchGoal, {
    targetCatchRate,
  });
  const dogCatchBlockers = Array.isArray(dogCatchGoal?.goal?.blockers) ? dogCatchGoal.goal.blockers : [];
  const metricValue = (value, digits = 4) => {
    const n = Number(value);
    return Number.isFinite(n) ? roundNumber(n, digits) : null;
  };
  const proofMetrics = {
    clean_gold_silver_capture_rate: metricValue(dogCatchGoal?.goal?.clean_gold_silver_capture_rate),
    peak_win_rate: metricValue(dogCatchGoal?.trades?.peak_win_rate),
    realized_roi: metricValue(dogCatchGoal?.trades?.realized_roi),
    eligible_gold_silver_unique: metricValue(dogCatchGoal?.goal?.eligible_gold_silver_unique, 0),
    captured_gold_silver_unique: metricValue(dogCatchGoal?.goal?.captured_gold_silver_unique ?? dogCatchGoal?.trades?.captured_gold_silver_unique, 0),
    missed_clean_gold_silver_unique: metricValue(dogCatchGoal?.missed?.clean_gold_silver_unique, 0),
    fills: metricValue(dogCatchGoal?.trades?.fills, 0),
    closed: metricValue(dogCatchGoal?.trades?.closed, 0),
  };
  const proofTargetGaps = {
    clean_gold_silver_capture_rate: proofMetrics.clean_gold_silver_capture_rate == null ? null : roundNumber(targetCatchRate - proofMetrics.clean_gold_silver_capture_rate, 4),
    peak_win_rate: proofMetrics.peak_win_rate == null ? null : roundNumber(targetWinRate - proofMetrics.peak_win_rate, 4),
    realized_roi: proofMetrics.realized_roi == null ? null : roundNumber(targetRoi - proofMetrics.realized_roi, 4),
  };
  const modeNormalTinyReady = Boolean(modeReadiness?.health?.normal_tiny_ready);
  const denominatorDashboardSafe = Boolean(denominatorHealth?.dashboard_safe);
  const blockers = [];
  if (!dashboardTokenConfigured) blockers.push('dashboard_token_missing_for_protected_kpi_evidence');
  if (!paperDbExists) blockers.push('paper_trades_db_missing');
  if (!snapshotAvailable) blockers.push(liveSnapshot?.error ? 'materialized_review_snapshot_invalid' : 'materialized_review_snapshot_missing');
  if (snapshotAvailable && !snapshotFresh) blockers.push('materialized_review_snapshot_stale_or_undated');
  if (!modeReadiness?.available) blockers.push('v27_mode_readiness_missing');
  if (!modeNormalTinyReady) blockers.push('normal_tiny_mode_readiness_not_green');
  if (!denominatorHealth?.available) blockers.push('v27_denominator_read_model_missing');
  if (!denominatorDashboardSafe) blockers.push('v27_denominator_read_model_not_dashboard_safe');
  for (const blocker of dogCatchBlockers) blockers.push(blocker);
  const uniqueBlockers = [...new Set(blockers)];
  const verified = Boolean(
    dashboardTokenConfigured
    && paperDbExists
    && snapshotFresh
    && modeNormalTinyReady
    && denominatorDashboardSafe
    && dogCatchGoal?.goal?.pass === true
  );
  let status = 'kpi_targets_not_met';
  if (verified) {
    status = 'kpi_verified';
  } else if (!dashboardTokenConfigured) {
    status = 'kpi_evidence_token_gated';
  } else if (!paperDbExists || !snapshotAvailable || !modeReadiness?.available || !denominatorHealth?.available) {
    status = 'kpi_evidence_incomplete';
  } else if (!snapshotFresh) {
    status = 'kpi_evidence_stale';
  }
  return {
    generated_at: generatedAt,
    schema_version: 'v2.7.0.kpi_proof_status.v1',
    public_safe: true,
    window_hours: requestedHours,
    materialized_snapshot_hours: materializedSnapshotHours,
    claim: {
      verified,
      status,
      target_summary: {
        clean_gold_silver_capture_rate: targetCatchRate,
        peak_win_rate: targetWinRate,
        realized_roi: targetRoi,
        dog_peak_threshold: dogPeakRatio,
        win_peak_threshold: winPeakRatio,
      },
      metrics: proofMetrics,
      target_gaps: proofTargetGaps,
      failure_attribution: failureAttribution,
    },
    evidence_sources: {
      protected_paper_endpoints: {
        require_dashboard_token: true,
        dashboard_token_configured: dashboardTokenConfigured,
        status: dashboardTokenConfigured ? 'available_with_dashboard_token' : 'token_not_configured',
        endpoints: [
          '/api/paper/review-snapshot',
          '/api/paper/missed-recovery-summary',
          '/api/paper/mode-ev',
          '/api/paper/entry-mode-performance',
          '/api/paper/dog-catch-goal',
        ],
      },
      paper_db: {
        exists: paperDbExists,
      },
      materialized_review_snapshot: {
        available: snapshotAvailable,
        fresh: snapshotFresh,
        generated_at: snapshotAvailable ? liveSnapshot.generated_at || null : null,
        snapshot_id: snapshotAvailable ? liveSnapshot.snapshot_id || null : null,
        age_minutes: snapshotAge,
        max_age_minutes: maxSnapshotAgeMinutes,
      },
      mode_readiness: {
        available: Boolean(modeReadiness?.available),
        highest_allowed_mode: modeReadiness?.highest_allowed_mode || null,
        normal_tiny_ready: modeNormalTinyReady,
        status: modeReadiness?.health?.status || null,
      },
      denominator_read_model: {
        available: Boolean(denominatorHealth?.available),
        dashboard_safe: denominatorDashboardSafe,
        normal_tiny_ready: Boolean(denominatorHealth?.health?.normal_tiny_ready),
        status: denominatorHealth?.health?.status || null,
      },
      dog_catch_goal: {
        available: Boolean(dogCatchGoal?.available),
        pass: Boolean(dogCatchGoal?.goal?.pass),
        blockers: dogCatchBlockers,
      },
    },
    blockers: uniqueBlockers,
    notes: {
      scope: 'public status only; raw trades, missed-dog rows, and PnL detail remain behind protected paper endpoints',
      claim_rule: '24h KPI is verified only when auth, fresh materialized evidence, mode readiness, denominator safety, and dog-catch goal are all green',
      failure_attribution: 'aggregate route/component/reason counts only; token addresses, trade rows, and raw PnL detail remain token-gated',
    },
  };
}

function publicDogCatchFailureAttribution(dogCatchGoal, { targetCatchRate = 0.60, limit = 10 } = {}) {
  const captured = Number(dogCatchGoal?.goal?.captured_gold_silver_unique ?? dogCatchGoal?.trades?.captured_gold_silver_unique ?? 0);
  const eligible = Number(dogCatchGoal?.goal?.eligible_gold_silver_unique ?? 0);
  const cleanMissed = Number(dogCatchGoal?.missed?.clean_gold_silver_unique ?? 0);
  const currentRateRaw = dogCatchGoal?.goal?.clean_gold_silver_capture_rate;
  const currentRate = Number.isFinite(Number(currentRateRaw)) ? roundNumber(Number(currentRateRaw), 4) : null;
  const target = Number.isFinite(Number(targetCatchRate)) ? Number(targetCatchRate) : 0.60;
  const requiredCaptured = eligible > 0 ? Math.ceil(target * eligible) : null;
  const additionalCapturesNeeded = requiredCaptured == null ? null : Math.max(0, requiredCaptured - captured);
  const rawRows = Array.isArray(dogCatchGoal?.missed?.by_blocker) ? dogCatchGoal.missed.by_blocker : [];
  const missedByBlocker = rawRows
    .map((row) => {
      const gold = Number(row.gold_n || 0);
      const silver = Number(row.silver_n || 0);
      const cleanGoldSilver = gold + silver;
      return {
        route: row.route ?? null,
        component: row.component ?? null,
        reject_reason: row.reject_reason ?? null,
        clean_gold_silver_unique: cleanGoldSilver,
        gold_n: gold,
        silver_n: silver,
        unique_tokens: Number(row.unique_tokens || cleanGoldSilver || 0),
        max_pnl: Number.isFinite(Number(row.max_pnl)) ? roundNumber(Number(row.max_pnl), 4) : null,
      };
    })
    .filter((row) => row.clean_gold_silver_unique > 0)
    .sort((a, b) => (
      b.clean_gold_silver_unique - a.clean_gold_silver_unique
      || b.gold_n - a.gold_n
      || (Number(b.max_pnl || 0) - Number(a.max_pnl || 0))
    ))
    .slice(0, limit);
  const rawPipelineRows = Array.isArray(dogCatchGoal?.missed?.reclaim_pipeline)
    ? dogCatchGoal.missed.reclaim_pipeline
    : [];
  const missedReclaimPipeline = rawPipelineRows
    .map((row) => {
      const gold = Number(row.gold_n || 0);
      const silver = Number(row.silver_n || 0);
      const cleanGoldSilver = gold + silver;
      return {
        route: row.route ?? null,
        component: row.component ?? null,
        reject_reason: row.reject_reason ?? null,
        rescue_state: row.rescue_state ?? null,
        fast_lane_status: row.fast_lane_status ?? null,
        fast_lane_reason: row.fast_lane_reason ?? null,
        entry_branch: row.entry_branch ?? null,
        entry_mode_hint: row.entry_mode_hint ?? null,
        clean_gold_silver_unique: cleanGoldSilver,
        gold_n: gold,
        silver_n: silver,
        unique_tokens: Number(row.unique_tokens || cleanGoldSilver || 0),
        max_pnl: Number.isFinite(Number(row.max_pnl)) ? roundNumber(Number(row.max_pnl), 4) : null,
      };
    })
    .filter((row) => row.clean_gold_silver_unique > 0)
    .sort((a, b) => (
      b.clean_gold_silver_unique - a.clean_gold_silver_unique
      || b.gold_n - a.gold_n
      || (Number(b.max_pnl || 0) - Number(a.max_pnl || 0))
    ))
    .slice(0, limit);
  return {
    available: Boolean(dogCatchGoal?.available),
    public_safe: true,
    target_capture_rate: target,
    current_capture_rate: currentRate,
    eligible_gold_silver_unique: Number.isFinite(eligible) ? eligible : 0,
    captured_gold_silver_unique: Number.isFinite(captured) ? captured : 0,
    missed_clean_gold_silver_unique: Number.isFinite(cleanMissed) ? cleanMissed : 0,
    required_captured_gold_silver_unique: requiredCaptured,
    additional_captures_needed_for_target: additionalCapturesNeeded,
    top_missed_blocker: missedByBlocker[0] || null,
    top_reclaim_pipeline_gap: missedReclaimPipeline[0] || null,
    missed_by_blocker: missedByBlocker,
    missed_reclaim_pipeline: missedReclaimPipeline,
    notes: {
      privacy: 'aggregate counts only; no token addresses, signal IDs, trade rows, or raw per-token PnL',
      action_hint: missedByBlocker.length
        ? 'Tune or instrument the top route/component/reason first; reclaim pipeline counts show whether the miss was unprocessed, stale, watch-only, queued, or entered.'
        : 'No clean gold/silver missed blocker bucket is available in the materialized snapshot.',
    },
  };
}

function normalizeV27ModeReadinessPayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  const normalTinyReady = Boolean(payload.health?.normal_tiny_ready);
  const output = JSON.parse(JSON.stringify(payload));
  const sections = [
    ['read_model', 'read_model_fresh', Boolean(output.read_model?.health?.dashboard_safe)],
    ['basic_readiness', 'basic_contracts_ready', Array.isArray(output.basic_readiness?.blocking_contracts) && output.basic_readiness.blocking_contracts.length === 0],
    ['projection_consumer', 'projection_consumer_ready', Boolean(output.projection_consumer?.health?.shadow_consumer_ready)],
  ];
  for (const [section, componentReadyKey, componentReady] of sections) {
    if (!output[section] || typeof output[section] !== 'object') continue;
    if (!output[section].health || typeof output[section].health !== 'object') continue;
    output[section].health[componentReadyKey] = componentReady;
    output[section].health.normal_tiny_ready = normalTinyReady;
    output[section].health.normal_tiny_ready_source = 'mode_readiness_matrix';
  }
  return output;
}

export function readV27DenominatorReadModelHealth(options = {}) {
  const path = v27DenominatorFreshnessPath(options);
  const generatedAt = new Date().toISOString();
  if (!fs.existsSync(path)) {
    return {
      generated_at: generatedAt,
      available: false,
      materialized: true,
      path,
      dashboard_safe: false,
      blocking_reasons: ['v27_read_model_health_missing'],
      health: {
        dashboard_safe: false,
        normal_tiny_ready: false,
        status: 'v27_read_model_health_missing',
      },
    };
  }
  try {
    const payload = JSON.parse(fs.readFileSync(path, 'utf8'));
    const projectionStatus = payload.projection_status || payload.verifier_report?.projection_status || payload.projection?.health?.status || null;
    const eventLogLatestSeq = payload.event_log_latest_seq ?? payload.verifier_report?.event_log_latest_seq ?? payload.read_model?.event_log_latest_seq ?? null;
    const unsafeProjectionStatuses = new Set(['event_log_invalid', 'not_built', 'seed_empty']);
    const derivedBlockingReasons = [];
    if (unsafeProjectionStatuses.has(projectionStatus)) {
      derivedBlockingReasons.push(`projection_status_${projectionStatus}`);
    }
    if (Number.isFinite(Number(eventLogLatestSeq)) && Number(eventLogLatestSeq) <= 0) {
      derivedBlockingReasons.push('event_log_empty');
    }
    const dashboardSafe = Boolean(payload.dashboard_safe || payload.health?.dashboard_safe) && derivedBlockingReasons.length === 0;
    const normalTinyReady = payload.health?.normal_tiny_ready ?? payload.mode_readiness?.normal_tiny_ready ?? false;
    const blockingReasons = Array.isArray(payload.blocking_reasons)
      ? payload.blocking_reasons
      : Array.isArray(payload.verifier_report?.blocking_reasons)
        ? payload.verifier_report.blocking_reasons
        : [];
    return {
      generated_at: generatedAt,
      available: true,
      materialized: true,
      path,
      ...payload,
      dashboard_safe: dashboardSafe,
      blocking_reasons: [...new Set([...blockingReasons, ...derivedBlockingReasons])],
      health: {
        ...(payload.health || {}),
        dashboard_safe: dashboardSafe,
        normal_tiny_ready: Boolean(normalTinyReady),
        status: payload.health?.status || (dashboardSafe ? 'read_model_refresh_ok' : 'read_model_refresh_not_ready'),
      },
    };
  } catch (error) {
    return {
      generated_at: generatedAt,
      available: false,
      materialized: true,
      path,
      dashboard_safe: false,
      blocking_reasons: ['v27_read_model_health_parse_failed'],
      error: error.message,
      health: {
        dashboard_safe: false,
        normal_tiny_ready: false,
        status: 'v27_read_model_health_parse_failed',
      },
    };
  }
}

function missedRecoveryRowFromLiveSnapshot(row = {}, section = 'overall') {
  const uniqueTokens = Number(row.unique_tokens || 0);
  const gold = Number(row.gold_unique || row.gold_n || 0);
  const silver = Number(row.silver_unique || row.silver_n || 0);
  const bronze = Number(row.bronze_unique || row.bronze_n || 0);
  return {
    section,
    route: row.route ?? null,
    component: row.component ?? null,
    reject_reason: row.reject_reason ?? null,
    unique_tokens: uniqueTokens,
    gold_n: gold,
    silver_n: silver,
    bronze_n: bronze,
    sub25_n: Math.max(0, uniqueTokens - gold - silver - bronze),
    mark_only_gold_n: Number(row.mark_only_gold_unique || 0),
    mark_only_silver_n: Number(row.mark_only_silver_unique || 0),
    mark_only_bronze_n: Number(row.mark_only_bronze_unique || 0),
    quote_executable_unique: Number(row.quote_executable_unique || 0),
    clean_tradable_unique: Number(row.clean_tradable_unique || row.quote_executable_unique || 0),
    tradable_unique: Number(row.tradable_unique || 0),
    stop_before_peak_unique: Number(row.stop_before_peak_unique || 0),
    max_pnl: row.max_pnl ?? null,
    avg_max_pnl: row.avg_max_pnl ?? null,
  };
}

export function missedRecoverySummaryFromLiveSnapshot(liveSnapshot, { dbPath, requestedHours, limit }) {
  const missed = liveSnapshot?.missed || {};
  const overall = missedRecoveryRowFromLiveSnapshot(missed.overall || {}, 'overall');
  const byGate = Array.isArray(missed.by_gate) ? missed.by_gate : [];
  const byBlockerAllUnique = byGate
    .map((row) => missedRecoveryRowFromLiveSnapshot(row, 'by_blocker_all_unique'))
    .slice(0, limit);
  const byBlockerCleanQuote = byBlockerAllUnique
    .filter((row) => Number(row.quote_executable_unique || 0) > 0)
    .map((row) => ({ ...row, section: 'by_blocker_clean_quote' }))
    .slice(0, limit);
  const topDogs = Array.isArray(missed.top_dogs) ? missed.top_dogs : [];
  const cleanTopDogs = topDogs
    .filter((row) => Number(row.quote_exec || row.quote_executable_unique || 0) > 0)
    .filter((row) => Number(row.would_stop_before_peak || 0) !== 1)
    .slice(0, limit);
  return {
    generated_at: new Date().toISOString(),
    db_path: dbPath,
    materialized: true,
    materialized_snapshot_id: liveSnapshot.snapshot_id,
    materialized_generated_at: liveSnapshot.generated_at,
    materialized_path: livePaperReviewPath(requestedHours),
    filters: {
      since_ts: liveSnapshot.window?.since_ts ?? null,
      since_iso: liveSnapshot.window?.since_iso ?? null,
      tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
      clean_quote_definition: 'tradable_missed=1 and would_stop_before_peak!=1',
      include_actions: false,
      live_query: false,
    },
    query_ms: 0,
    overall_unique: overall,
    by_route: [],
    by_blocker_clean_quote: byBlockerCleanQuote,
    by_blocker_all_unique: byBlockerAllUnique,
    top_clean_quote_dogs: cleanTopDogs,
    recovery_actionability: [],
    recovery_actions: [],
    notes: {
      endpoint_goal: 'materialized missed-dog recovery summary; pass live=1 only for short-window debugging',
      recovery_actions: 'omitted from materialized live-safe snapshot',
      source_snapshot: 'paper_review_snapshot_worker',
    },
  };
}

function entryModePerformanceFromLiveSnapshot(liveSnapshot, { dbPath, requestedHours, limit }) {
  const section = liveSnapshot?.entry_mode_performance || {};
  return {
    generated_at: new Date().toISOString(),
    db_path: dbPath,
    materialized: true,
    materialized_snapshot_id: liveSnapshot.snapshot_id,
    materialized_generated_at: liveSnapshot.generated_at,
    materialized_path: livePaperReviewPath(requestedHours),
    filters: {
      since_ts: liveSnapshot.window?.since_ts ?? null,
      since_iso: liveSnapshot.window?.since_iso ?? null,
      limit,
      live_query: false,
    },
    bucket_summary: section.bucket_summary || {},
    by_entry_mode: Array.isArray(section.by_entry_mode) ? section.by_entry_mode.slice(0, limit) : [],
    recent: Array.isArray(section.recent) ? section.recent.slice(0, Math.min(limit, 50)) : [],
    row_count: Number(section.row_count || 0),
    row_limit: section.row_limit ?? null,
  };
}

function routeHealthFromLiveSnapshot(liveSnapshot, { dbPath, requestedHours, limit }) {
  const section = liveSnapshot?.route_health || {};
  return {
    generated_at: new Date().toISOString(),
    db_path: dbPath,
    materialized: true,
    materialized_snapshot_id: liveSnapshot.snapshot_id,
    materialized_generated_at: liveSnapshot.generated_at,
    materialized_path: livePaperReviewPath(requestedHours),
    window_hours: requestedHours,
    filters: {
      since_ts: liveSnapshot.window?.since_ts ?? null,
      since_iso: liveSnapshot.window?.since_iso ?? null,
      limit,
      live_query: false,
    },
    available: section.available !== false,
    totals: section.totals || {},
    routes: Array.isArray(section.routes) ? section.routes.slice(0, limit) : [],
    notes: section.notes || {},
  };
}

function effectiveRuntimeModeState(row, nowTs = Math.floor(Date.now() / 1000)) {
  const cooldownUntil = finiteNumber(row?.cooldown_until_ts, null);
  const storedStatus = String(row?.status || 'LIVE').toUpperCase();
  const storedCircuit = Boolean(Number(row?.circuit_broken || 0));
  const inCooldown = cooldownUntil != null && cooldownUntil > nowTs;
  let status = storedStatus;
  let action = String(row?.action || storedStatus).toUpperCase();
  let circuitBroken = storedCircuit;
  let recoveryRequired = false;
  let reason = row?.reason || null;
  if (storedCircuit && inCooldown) {
    status = 'CIRCUIT_BROKEN';
    action = 'SHADOW';
    circuitBroken = true;
  } else if (storedCircuit) {
    status = 'SHADOW';
    action = 'SHADOW';
    circuitBroken = false;
    recoveryRequired = true;
    reason = 'cooldown_elapsed_requires_clean_windows';
  }
  return {
    mode_key: row?.mode_key || 'A_CLASS_FASTLANE',
    status,
    action,
    circuit_broken: circuitBroken,
    stored_status: storedStatus,
    stored_circuit_broken: storedCircuit,
    reason,
    source_trade_id: row?.source_trade_id || null,
    token_ca: row?.token_ca || null,
    symbol: row?.symbol || null,
    last_realized_pnl_pct: roundNullableNumber(row?.last_realized_pnl_pct, 6),
    last_realized_pnl_sol: roundNullableNumber(row?.last_realized_pnl_sol, 9),
    loss_cap_pct: roundNullableNumber(row?.loss_cap_pct, 6),
    breach_count: Number(row?.breach_count || 0),
    last_breach_ts: finiteNumber(row?.last_breach_ts, null),
    cooldown_until_ts: cooldownUntil,
    cooldown_remaining_sec: cooldownUntil == null ? 0 : Math.max(0, cooldownUntil - nowTs),
    recovery_required: recoveryRequired,
    clean_windows_required: Number(row?.clean_windows_required || 4),
    detail: parseJsonValue(row?.detail_json, {}),
  };
}

function aClassRuntimeSafetyFromDb(paperDb, tableNames, sinceTs = null) {
  const nowTs = Math.floor(Date.now() / 1000);
  let lossCapBreachN = 0;
  let recentBreaches = [];
  if (tableNames.has('canonical_trade_ledger')) {
    const cols = getTableColumns(paperDb, 'canonical_trade_ledger');
    if (cols.has('loss_cap_breach')) {
      const tsExpr = cols.has('exit_ts')
        ? 'COALESCE(exit_ts, updated_at, created_at, 0)'
        : (cols.has('updated_at') ? 'COALESCE(updated_at, created_at, 0)' : '0');
      const where = sinceTs == null
        ? 'WHERE COALESCE(loss_cap_breach, 0) = 1'
        : `WHERE COALESCE(loss_cap_breach, 0) = 1 AND ${tsExpr} >= @sinceTs`;
      const params = sinceTs == null ? {} : { sinceTs };
      lossCapBreachN = Number(paperDb.prepare(`SELECT COUNT(*) AS n FROM canonical_trade_ledger ${where}`).get(params)?.n || 0);
      recentBreaches = paperDb.prepare(`
        SELECT trade_id, token_ca, symbol, normalized_mode, entry_mode,
               exit_ts, realized_pnl_pct, realized_pnl_sol, loss_cap_pct,
               exit_reason, no_route_flag, trapped_flag
        FROM canonical_trade_ledger
        ${where}
        ORDER BY ${tsExpr} DESC
        LIMIT 20
      `).all(params);
    }
  }
  const modeStates = tableNames.has('a_class_mode_runtime_state')
    ? paperDb.prepare('SELECT * FROM a_class_mode_runtime_state ORDER BY updated_at DESC').all().map((row) => effectiveRuntimeModeState(row, nowTs))
    : [];
  const downgradedModes = modeStates.filter((state) => state.status !== 'LIVE' || state.recovery_required);
  const modeCircuitBroken = modeStates.some((state) => state.circuit_broken);
  return {
    available: true,
    schema_version: 'v1.a_class_runtime_safety',
    loss_cap_breach_n: lossCapBreachN,
    mode_circuit_broken: modeCircuitBroken,
    downgraded_modes: downgradedModes,
    mode_states: modeStates,
    recent_breaches: recentBreaches,
    next_safe_action: modeCircuitBroken
      ? 'keep_breached_modes_shadow_until_cooldown'
      : (downgradedModes.length ? 'keep_breached_modes_shadow_until_clean_windows' : 'continue_a_class_observation'),
  };
}

export function aClassStatusFromLiveSnapshot(liveSnapshot, { dbPath, requestedHours, materializedHours = requestedHours, limit = 30 }) {
  const section = liveSnapshot?.a_class || {};
  const rows = (value) => Array.isArray(value) ? value : [];
  const p0Discovery = aClassP0DiscoveryFromSnapshot(liveSnapshot);
  const rrSummary = {
    schema_version: 'v1.a_class_rr_summary',
    available: Boolean(p0Discovery.available),
    outlier_trimmed_would_rr: p0Discovery.outlier_trimmed_would_rr,
    defined_risk_pct: p0Discovery.defined_risk_pct,
    quote_clean_gold_silver_seen_count: p0Discovery.quote_clean_gold_silver_seen_count,
    quote_clean_gold_silver_would_enter_count: p0Discovery.quote_clean_gold_silver_would_enter_count,
    source_breakdown: p0Discovery.source_breakdown || {},
    source_component_breakdown: p0Discovery.source_component_breakdown || {},
    hydrate_outcome_breakdown: p0Discovery.hydrate_outcome_breakdown || {},
    observed_hydrate_outcome_breakdown: p0Discovery.observed_hydrate_outcome_breakdown || {},
    denominator_exclusion_breakdown: p0Discovery.denominator_exclusion_breakdown || {},
    hydrate_outcome_exclusion_breakdown: p0Discovery.hydrate_outcome_exclusion_breakdown || {},
    unknown_reason_breakdown: p0Discovery.unknown_reason_breakdown || {},
  };
  const runtimeSafety = (section.runtime_safety && typeof section.runtime_safety === 'object')
    ? section.runtime_safety
    : {
        available: false,
        loss_cap_breach_n: 0,
        mode_circuit_broken: false,
        downgraded_modes: [],
        next_safe_action: null,
      };
  const shadowPending = !p0Discovery.available;
  return {
    generated_at: new Date().toISOString(),
    db_path: dbPath,
    available: Boolean(section.available) && !shadowPending,
    status: shadowPending ? 'shadow_pending' : 'shadow_ready',
    materialized: true,
    live_query: false,
    requested_window_hours: requestedHours,
    materialized_window_hours: materializedHours,
    materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
    materialized_generated_at: liveSnapshot?.generated_at || null,
    materialized_path: livePaperReviewPath(materializedHours),
    since_ts: liveSnapshot?.window?.since_ts ?? null,
    enabled_env: String(process.env.A_CLASS_ENABLED || 'false').toLowerCase() === 'true',
    shadow_eval_enabled_env: String(process.env.A_CLASS_SHADOW_EVAL_ENABLED || 'true').toLowerCase() !== 'false',
    total: Number(section.total || 0),
    would_enter: Number(section.would_enter || 0),
    enter: Number(section.enter || 0),
    action_summary: rows(section.action_summary).map((row) => ({
      ...row,
      avg_score: roundNullableNumber(row.avg_score, 2),
      would_enter_size_sol: roundNullableNumber(row.would_enter_size_sol, 6),
    })),
    grade_summary: rows(section.grade_summary).map((row) => ({
      ...row,
      avg_score: roundNullableNumber(row.avg_score, 2),
    })),
    source_summary: rows(section.source_summary).slice(0, limit).map((row) => ({
      ...row,
      avg_score: roundNullableNumber(row.avg_score, 2),
      would_enter_size_sol: roundNullableNumber(row.would_enter_size_sol, 6),
    })),
    reason_summary: rows(section.reason_summary).slice(0, limit).map((row) => ({
      ...row,
      max_score: roundNullableNumber(row.max_score, 2),
    })),
    hard_blockers: rows(section.hard_blockers),
    recent_events: rows(section.recent_events).slice(0, limit),
    runtime_safety: runtimeSafety,
    rr_summary: rrSummary,
    loss_cap_breach_n: Number(runtimeSafety.loss_cap_breach_n || 0),
    mode_circuit_broken: Boolean(runtimeSafety.mode_circuit_broken),
    downgraded_modes: rows(runtimeSafety.downgraded_modes),
    next_safe_action: runtimeSafety.next_safe_action || null,
    p0_discovery: p0Discovery,
    shadow_pending: shadowPending,
    note: 'Default is materialized by paper_review_snapshot_worker; pass live=1 for an on-demand DB scan.',
  };
}

export function dogCatchGoalFromLiveSnapshot(liveSnapshot, { dbPath, requestedHours, options = {} }) {
  const section = liveSnapshot?.dog_catch_goal;
  if (section && typeof section === 'object' && section.available !== false) {
    return {
      generated_at: new Date().toISOString(),
      db_path: dbPath,
      window_hours: requestedHours,
      materialized: true,
      live_query: false,
      materialized_snapshot_id: liveSnapshot.snapshot_id || null,
      materialized_generated_at: liveSnapshot.generated_at || null,
      available: true,
      ...section,
    };
  }
  const missedSummary = missedRecoverySummaryFromLiveSnapshot(liveSnapshot, {
    dbPath,
    requestedHours,
    limit: 80,
  });
  const routeHealth = routeHealthFromLiveSnapshot(liveSnapshot, {
    dbPath,
    requestedHours,
    limit: 120,
  });
  const dogPeak = Number(options.dogPeakRatio ?? 0.50);
  const winPeak = Number(options.winPeakRatio ?? 0.30);
  const targetCatchRate = Number(options.targetCatchRate ?? 0.60);
  const targetWinRate = Number(options.targetWinRate ?? 0.55);
  const targetRoi = Number(options.targetRoi ?? 2.0);
  const captured = Number(routeHealth?.totals?.entered || 0);
  const cleanGold = Number(missedSummary?.overall_unique?.gold_n || 0);
  const cleanSilver = Number(missedSummary?.overall_unique?.silver_n || 0);
  const eligible = captured + cleanGold + cleanSilver;
  const captureRate = eligible > 0 ? captured / eligible : null;
  const blockers = [];
  if (captureRate == null || captureRate < targetCatchRate) blockers.push('clean_gold_silver_capture_rate_below_target');
  blockers.push('peak_win_rate_requires_trade_level_snapshot');
  blockers.push('realized_roi_requires_trade_level_snapshot');
  return {
    generated_at: new Date().toISOString(),
    db_path: dbPath,
    window_hours: requestedHours,
    materialized: true,
    live_query: false,
    materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
    materialized_generated_at: liveSnapshot?.generated_at || null,
    available: Boolean(liveSnapshot),
    since_ts: liveSnapshot?.window?.since_ts ?? null,
    targets: {
      clean_gold_silver_capture_rate: targetCatchRate,
      peak_win_rate: targetWinRate,
      realized_roi: targetRoi,
      dog_peak_threshold: dogPeak,
      win_peak_threshold: winPeak,
    },
    trades: {
      fills: captured,
      closed: null,
      peak_wins: null,
      peak_win_rate: null,
      captured_gold_silver_unique: captured,
      realized_pnl_sol: null,
      deployed_sol: null,
      realized_roi: null,
    },
    missed: {
      clean_gold_silver_unique: cleanGold + cleanSilver,
      clean_gold_unique: cleanGold,
      clean_silver_unique: cleanSilver,
      by_blocker: Array.isArray(missedSummary?.by_blocker_clean_quote)
        ? missedSummary.by_blocker_clean_quote.filter((row) => Number(row.gold_n || 0) + Number(row.silver_n || 0) > 0)
        : [],
    },
    goal: {
      eligible_gold_silver_unique: eligible,
      captured_gold_silver_unique: captured,
      clean_gold_silver_capture_rate: captureRate,
      pass: false,
      blockers,
    },
    notes: {
      fallback: 'dog_catch_goal section missing in this snapshot; using route-health entered count and missed summary until the snapshot worker refreshes.',
    },
  };
}

function finiteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function lowestEntryModeLossPct(liveSnapshot) {
  const rows = Array.isArray(liveSnapshot?.entry_mode_performance?.by_entry_mode)
    ? liveSnapshot.entry_mode_performance.by_entry_mode
    : [];
  const losses = rows
    .map((row) => finiteNumber(row.max_loss_pct, null))
    .filter((value) => value != null);
  return losses.length ? Math.min(...losses) : null;
}

function modeActionsFromSnapshot(liveSnapshot, metrics, targets) {
  const actions = [];
  const aClass = liveSnapshot?.a_class || {};
  const aClassWouldEnter = Number(aClass.would_enter || 0);
  const aClassEnter = Number(aClass.enter || 0);
  const aClassEnabled = String(process.env.A_CLASS_ENABLED || 'false').toLowerCase() === 'true';
  if (aClass.available) {
    actions.push({
      mode: 'A_CLASS_FASTLANE',
      status: aClassEnabled ? 'TINY_ONLY' : 'SHADOW',
      would_enter_24h: aClassWouldEnter,
      enter_24h: aClassEnter,
      recommended_action: !aClassEnabled && aClassWouldEnter > 0
        ? 'prepare_0_001_tiny_paper_after_observability_green'
        : 'observe',
      reason: !aClassEnabled && aClassWouldEnter > 0
        ? 'a_class_has_shadow_would_enter_samples_but_live_tiny_disabled'
        : 'a_class_not_ready_to_upgrade',
    });
  }
  const rows = Array.isArray(liveSnapshot?.entry_mode_performance?.by_entry_mode)
    ? liveSnapshot.entry_mode_performance.by_entry_mode
    : [];
  for (const row of rows.slice(0, 8)) {
    const closed = Number(row.closed || row.closed_n || 0);
    const winRatePct = finiteNumber(row.win_rate_pct, null);
    const avgPnlPct = finiteNumber(row.avg_pnl_pct, null);
    const maxLossPct = finiteNumber(row.max_loss_pct, null);
    let status = 'TINY_ONLY';
    let recommended = 'collect_more_samples';
    let reason = 'insufficient_closed_trades_for_mode_promotion';
    if (maxLossPct != null && maxLossPct <= -Math.abs(targets.max_single_trade_loss_pct)) {
      status = 'DISABLED';
      recommended = 'disable_or_shadow_until_loss_source_explained';
      reason = 'mode_max_loss_breached_goal_limit';
    } else if (closed >= targets.min_closed_trades_24h && avgPnlPct != null && avgPnlPct < 0) {
      status = 'SHADOW';
      recommended = 'downgrade_to_shadow';
      reason = 'mode_ev_negative_after_min_sample';
    } else if (closed >= targets.min_closed_trades_24h && winRatePct != null && winRatePct >= targets.target_realized_win_rate * 100 && avgPnlPct != null && avgPnlPct > 0) {
      status = 'LIVE';
      recommended = 'allow_or_observe';
      reason = 'mode_meets_win_rate_and_positive_ev';
    }
    actions.push({
      mode: row.entry_mode || row.bucket || 'unknown',
      bucket: row.bucket || null,
      status,
      closed_24h: closed,
      win_rate_pct: winRatePct,
      avg_pnl_pct: avgPnlPct,
      max_loss_pct: maxLossPct,
      recommended_action: recommended,
      reason,
    });
  }
  if (!actions.length) {
    actions.push({
      mode: 'ALL',
      status: 'SHADOW',
      recommended_action: 'build_more_evidence',
      reason: 'no_mode_performance_or_a_class_snapshot_available',
    });
  }
  return actions;
}

export function buildRolling24hGoalStatusFromLiveSnapshot(liveSnapshot, options = {}) {
  const generatedAt = options.generatedAt || new Date().toISOString();
  const nowMs = Number.isFinite(Number(options.nowMs)) ? Number(options.nowMs) : Date.parse(generatedAt) || Date.now();
  const requestedHours = Math.max(1, Math.min(24, parseInt(String(options.requestedHours ?? 24), 10) || 24));
  const materializedHours = Math.max(1, Math.min(24, parseInt(String(options.materializedHours ?? requestedHours), 10) || requestedHours));
  const maxSnapshotAgeMinutes = Math.max(1, Math.min(1440, parseInt(String(options.maxSnapshotAgeMinutes ?? 30), 10) || 30));
  const dbPath = options.dbPath || getPaperDbPath();
  const paperDbHealth = options.paperDbHealth || null;
  const livePaperDbUsable = paperDbHealth ? paperDbHealthIsUsable(paperDbHealth) : true;
  const targets = {
    target_realized_win_rate: finiteNumber(options.targetRealizedWinRate, finiteNumber(process.env.ROLLING_GOAL_WIN_RATE, 0.60)),
    target_gold_silver_capture_rate: finiteNumber(options.targetGoldSilverCaptureRate, finiteNumber(process.env.DOG_CATCH_GOAL_CAPTURE_RATE, 0.60)),
    target_strategy_bucket_roi: finiteNumber(options.targetStrategyBucketRoi, finiteNumber(process.env.DOG_CATCH_GOAL_ROI, 2.0)),
    max_single_trade_loss_pct: Math.abs(finiteNumber(options.maxSingleTradeLossPct, finiteNumber(process.env.ROLLING_GOAL_MAX_SINGLE_LOSS_PCT, 20))),
    min_closed_trades_24h: Math.max(0, parseInt(String(options.minClosedTrades ?? process.env.ROLLING_GOAL_MIN_CLOSED_TRADES_24H ?? 20), 10) || 20),
    min_gold_silver_candidates_24h: Math.max(0, parseInt(String(options.minGoldSilverCandidates ?? process.env.ROLLING_GOAL_MIN_GOLD_SILVER_CANDIDATES_24H ?? 5), 10) || 5),
  };
  const snapshotAvailable = Boolean(liveSnapshot && !liveSnapshot.error);
  const snapshotAge = snapshotAvailable ? snapshotAgeMinutes(liveSnapshot, nowMs) : null;
  const snapshotFresh = Boolean(snapshotAvailable && snapshotAge != null && snapshotAge <= maxSnapshotAgeMinutes);
  const aClassP0Discovery = snapshotAvailable ? aClassP0DiscoveryFromSnapshot(liveSnapshot) : sanitizeAClassP0Discovery(null);
  const runtimeSafety = (liveSnapshot?.a_class?.runtime_safety && typeof liveSnapshot.a_class.runtime_safety === 'object')
    ? liveSnapshot.a_class.runtime_safety
    : {
        available: false,
        loss_cap_breach_n: 0,
        mode_circuit_broken: false,
        downgraded_modes: [],
        next_safe_action: null,
      };
  const shadowPending = snapshotAvailable && !aClassP0Discovery.available;
  const dogCatch = snapshotAvailable ? dogCatchGoalFromLiveSnapshot(liveSnapshot, {
    dbPath,
    requestedHours,
    options: {
      targetCatchRate: targets.target_gold_silver_capture_rate,
      targetWinRate: targets.target_realized_win_rate,
      targetRoi: targets.target_strategy_bucket_roi,
      dogPeakRatio: finiteNumber(options.dogPeakRatio, 0.50),
      winPeakRatio: finiteNumber(options.winPeakRatio, 0.30),
    },
  }) : null;
  const tradeTotals = liveSnapshot?.trades?.totals || {};
  const closed = finiteNumber(tradeTotals.closed, finiteNumber(dogCatch?.trades?.closed, 0)) || 0;
  const wins = finiteNumber(tradeTotals.wins, null);
  const realizedWinRate = closed > 0 && wins != null ? wins / closed : null;
  const eligibleGoldSilver = finiteNumber(dogCatch?.goal?.eligible_gold_silver_unique, 0) || 0;
  const capturedGoldSilver = finiteNumber(dogCatch?.goal?.captured_gold_silver_unique ?? dogCatch?.trades?.captured_gold_silver_unique, 0) || 0;
  const captureRate = eligibleGoldSilver > 0 ? capturedGoldSilver / eligibleGoldSilver : null;
  const deployedSol = finiteNumber(tradeTotals.deployed_sol, finiteNumber(dogCatch?.trades?.deployed_sol, 0)) || 0;
  const realizedPnlSol = finiteNumber(tradeTotals.est_pnl_sol, finiteNumber(dogCatch?.trades?.realized_pnl_sol, null));
  const bucketRoi = deployedSol > 0 && realizedPnlSol != null
    ? realizedPnlSol / deployedSol
    : finiteNumber(dogCatch?.trades?.realized_roi, null);
  const minPnlRatio = finiteNumber(tradeTotals.min_pnl, null);
  const maxSingleLossPct = minPnlRatio == null ? lowestEntryModeLossPct(liveSnapshot) : minPnlRatio * 100.0;
  const blockers = [];
  const metricBlockers = [];
  const sampleBlockers = [];
  const evidenceBlockers = [];
  if (!livePaperDbUsable) evidenceBlockers.push(`live_${paperDbHealth?.status || 'paper_db_unavailable'}`);
  if (!snapshotAvailable) evidenceBlockers.push(liveSnapshot?.error ? 'materialized_review_snapshot_invalid' : 'materialized_review_snapshot_missing');
  if (snapshotAvailable && !snapshotFresh) evidenceBlockers.push('materialized_review_snapshot_stale_or_undated');
  if (shadowPending) evidenceBlockers.push('a_class_p0_shadow_discovery_pending');
  if (runtimeSafety.mode_circuit_broken) evidenceBlockers.push('a_class_mode_runtime_circuit_broken');
  if (closed < targets.min_closed_trades_24h) sampleBlockers.push('insufficient_closed_trades_24h');
  if (eligibleGoldSilver < targets.min_gold_silver_candidates_24h) sampleBlockers.push('insufficient_gold_silver_denominator_24h');
  if (realizedWinRate == null || realizedWinRate < targets.target_realized_win_rate) metricBlockers.push('realized_win_rate_below_target');
  if (captureRate == null || captureRate < targets.target_gold_silver_capture_rate) metricBlockers.push('gold_silver_capture_rate_below_target');
  if (bucketRoi == null || bucketRoi < targets.target_strategy_bucket_roi) metricBlockers.push('strategy_bucket_roi_below_target');
  if (maxSingleLossPct == null) {
    metricBlockers.push('max_single_trade_loss_unavailable');
  } else if (maxSingleLossPct <= -targets.max_single_trade_loss_pct) {
    metricBlockers.push('max_single_trade_loss_breached');
  }
  blockers.push(...evidenceBlockers, ...sampleBlockers, ...metricBlockers);
  const validSample = sampleBlockers.length === 0;
  const pass = snapshotFresh && validSample && metricBlockers.length === 0 && evidenceBlockers.length === 0;
  let status = 'under_target';
  if (shadowPending) {
    status = 'shadow_pending';
  } else if (pass) {
    status = 'pass';
  } else if (evidenceBlockers.length) {
    status = 'evidence_unavailable';
  } else if (!validSample) {
    status = 'insufficient_sample';
  }
  const metrics = {
    realized_win_rate: realizedWinRate == null ? null : roundNumber(realizedWinRate, 4),
    gold_silver_capture_rate: captureRate == null ? null : roundNumber(captureRate, 4),
    strategy_bucket_roi: bucketRoi == null ? null : roundNumber(bucketRoi, 4),
    max_single_trade_loss_pct: maxSingleLossPct == null ? null : roundNumber(maxSingleLossPct, 2),
    closed_trades_24h: closed,
    wins_24h: wins,
    eligible_gold_silver_24h: eligibleGoldSilver,
    captured_gold_silver_24h: capturedGoldSilver,
    quote_clean_gold_silver_seen_24h: aClassP0Discovery.quote_clean_gold_silver_seen_count,
    quote_clean_gold_silver_would_enter_24h: aClassP0Discovery.quote_clean_gold_silver_would_enter_count,
    would_enter_no_route_rate: aClassP0Discovery.would_enter_no_route_rate,
    would_enter_trapped_rate: aClassP0Discovery.would_enter_trapped_rate,
    unknown_data_rate: aClassP0Discovery.unknown_data_rate,
    outlier_trimmed_would_rr: aClassP0Discovery.outlier_trimmed_would_rr,
    loss_cap_breach_n: Number(runtimeSafety.loss_cap_breach_n || 0),
    mode_circuit_broken: Boolean(runtimeSafety.mode_circuit_broken),
    downgraded_modes: Array.isArray(runtimeSafety.downgraded_modes) ? runtimeSafety.downgraded_modes : [],
    deployed_sol_24h: roundNullableNumber(deployedSol, 6),
    realized_pnl_sol_24h: roundNullableNumber(realizedPnlSol, 6),
  };
  const aClassEvents = Array.isArray(liveSnapshot?.a_class?.recent_events) ? liveSnapshot.a_class.recent_events : [];
  const matrixSummary = summarizeAClassMatrixEvents(aClassEvents);
  const rrSummary = {
    schema_version: 'v1.a_class_rr_summary',
    available: Boolean(aClassP0Discovery.available),
    outlier_trimmed_would_rr: aClassP0Discovery.outlier_trimmed_would_rr,
    defined_risk_pct: aClassP0Discovery.defined_risk_pct,
    quote_clean_gold_silver_seen_count: aClassP0Discovery.quote_clean_gold_silver_seen_count,
    quote_clean_gold_silver_would_enter_count: aClassP0Discovery.quote_clean_gold_silver_would_enter_count,
    source_breakdown: aClassP0Discovery.source_breakdown || {},
    source_component_breakdown: aClassP0Discovery.source_component_breakdown || {},
    hydrate_outcome_breakdown: aClassP0Discovery.hydrate_outcome_breakdown || {},
    observed_hydrate_outcome_breakdown: aClassP0Discovery.observed_hydrate_outcome_breakdown || {},
    denominator_exclusion_breakdown: aClassP0Discovery.denominator_exclusion_breakdown || {},
    hydrate_outcome_exclusion_breakdown: aClassP0Discovery.hydrate_outcome_exclusion_breakdown || {},
    unknown_reason_breakdown: aClassP0Discovery.unknown_reason_breakdown || {},
    recent_event_rr: aClassEvents
      .filter((event) => event.expected_rr != null)
      .slice(0, 20)
      .map((event) => ({
        id: event.id,
        symbol: event.symbol,
        action: event.action,
        grade: event.grade,
        expected_rr: event.expected_rr,
        expected_upside_pct: event.expected_upside_pct,
        defined_risk_pct: event.defined_risk_pct,
        bottom_ticket_size_sol: event.bottom_ticket_size_sol,
      })),
  };
  const materializedAi = liveSnapshot?.ai_strategy_review || liveSnapshot?.a_class?.ai_strategy_review || null;
  const missedDogReview = materializedAi?.missed_dog_review || buildMissedDogAiReviewFromP0(aClassP0Discovery);
  const counterfactualAudit = materializedAi?.counterfactual_audit || buildCounterfactualAiAuditFromP0(aClassP0Discovery);
  const controllerActions = materializedAi?.controller_actions || liveSnapshot?.strategy_goal_controller || buildGoalControllerActions({
    rollingGoalStatus: { status },
    p0Discovery: aClassP0Discovery,
    counterfactualAudit,
    missedDogReview,
  });
  return {
    generated_at: generatedAt,
    schema_version: 'v1.rolling_24h_strategy_goal_status',
    goal: 'rolling_24h_convexity_capture',
    materialized: true,
    live_query: false,
    available: snapshotAvailable && snapshotFresh && !shadowPending && livePaperDbUsable,
    shadow_pending: shadowPending,
    pass,
    status,
    db_path: dbPath,
    config_path: join(projectRoot, 'config', 'strategy-goal.yaml'),
    requested_window_hours: requestedHours,
    materialized_window_hours: materializedHours,
    materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
    materialized_generated_at: liveSnapshot?.generated_at || null,
    materialized_path: livePaperReviewPath(materializedHours),
    materialized_snapshot_fresh: snapshotFresh,
    snapshot_age_minutes: snapshotAge,
    max_snapshot_age_minutes: maxSnapshotAgeMinutes,
    live_paper_db_health: paperDbHealth,
    targets,
    metrics,
    matrix_summary: matrixSummary,
    rr_summary: rrSummary,
    ai_advisory: {
      schema_version: 'v1.rolling_goal_ai_advisory_bundle',
      advisory_only: true,
      missed_dog_review: missedDogReview,
      counterfactual_audit: counterfactualAudit,
    },
    controller_actions: controllerActions.actions || [],
    controller: controllerActions,
    runtime_safety: runtimeSafety,
    next_safe_action: runtimeSafety.mode_circuit_broken
      ? (runtimeSafety.next_safe_action || 'keep_breached_modes_shadow_until_cooldown')
      : (controllerActions.next_safe_action || runtimeSafety.next_safe_action || 'keep_a_class_shadow'),
    a_class_p0_discovery: aClassP0Discovery,
    top_missed_blockers: aClassP0Discovery.missed_blockers,
    target_gaps: {
      realized_win_rate: realizedWinRate == null ? null : roundNumber(targets.target_realized_win_rate - realizedWinRate, 4),
      gold_silver_capture_rate: captureRate == null ? null : roundNumber(targets.target_gold_silver_capture_rate - captureRate, 4),
      strategy_bucket_roi: bucketRoi == null ? null : roundNumber(targets.target_strategy_bucket_roi - bucketRoi, 4),
      max_single_trade_loss_pct: maxSingleLossPct == null ? null : roundNumber(maxSingleLossPct + targets.max_single_trade_loss_pct, 2),
    },
    blockers: [...new Set(blockers)],
    evidence_blockers: evidenceBlockers,
    sample_blockers: sampleBlockers,
    metric_blockers: metricBlockers,
    mode_actions: snapshotAvailable ? modeActionsFromSnapshot(liveSnapshot, metrics, targets) : [],
    notes: {
      endpoint_goal: 'rolling 24h strategy controller status; default reads materialized snapshot only so dashboard health is not blocked by SQLite scans',
      success_rule: 'pass requires fresh materialized evidence, minimum sample denominators, realized win rate >=60%, quote-clean gold/silver catch rate >=60%, deployed-capital ROI >=200%, and no non-outlier trade loss worse than -20%',
      next_controller_step: 'use mode_actions to decide LIVE/TINY_ONLY/SHADOW/DISABLED; do not increase size without positive EV and clean denominator evidence',
    },
  };
}

function runtimeCommitFingerprint() {
  const envCommit = firstValue(
    process.env.GIT_COMMIT,
    process.env.COMMIT_SHA,
    process.env.SOURCE_VERSION,
    process.env.RAILWAY_GIT_COMMIT_SHA,
    process.env.ZEABUR_GIT_COMMIT_SHA,
    process.env.ZEABUR_GIT_COMMIT,
    process.env.ZEABUR_COMMIT_SHA,
    process.env.VERCEL_GIT_COMMIT_SHA,
    process.env.GITHUB_SHA,
    process.env.RENDER_GIT_COMMIT
  );
  if (envCommit) return String(envCommit);
  try {
    const head = fs.readFileSync(join(projectRoot, '.git', 'HEAD'), 'utf8').trim();
    if (head.startsWith('ref:')) {
      const refPath = head.replace(/^ref:\s*/, '');
      return fs.readFileSync(join(projectRoot, '.git', refPath), 'utf8').trim();
    }
    return head;
  } catch {
    return 'unknown';
  }
}

export function dashboardEntrypointInfo() {
  const argv1 = process.argv?.[1] || null;
  return {
    schema_version: 'dashboard_entrypoint.v1',
    argv0: process.argv?.[0] || null,
    argv1,
    entrypoint_file: argv1,
    entrypoint_basename: argv1 ? basename(argv1) : null,
    npm_lifecycle_event: process.env.npm_lifecycle_event || null,
    runtime_role_env: process.env.DASHBOARD_RUNTIME_ROLE || null,
    embedded_dashboard_enabled: process.env.EMBEDDED_DASHBOARD_ENABLED || null,
    process_name: process.env.name || null,
  };
}

function reviewPolicyFingerprint(registrySummary = null) {
  const envKeys = [
    'HARD_GATE_PASS_TINY_PROBE_ENABLED',
    'HARD_GATE_PASS_DIRECT_ENTRY_ENABLED',
    'FAST_ENTRY_HARD_GATE_DIRECT_ENABLED',
    'PRE_PASS_RESONANCE_TINY_PROBE_ENABLED',
    'SOURCE_RESONANCE_TINY_PROBE_ENABLED',
    'SOURCE_RESONANCE_DIRECT_PROBE_ENABLED',
    'SOURCE_RESONANCE_TINY_PROBE_BYPASS_SMART_ENTRY',
    'SOURCE_RESONANCE_TINY_PROBE_REQUIRE_QUOTE_CLEAN',
    'REVIVAL_CANARY_POLICY_VERSION',
    'ENTRY_MODE_POLICY_VERSION',
    'PREMIUM_LIVE_EXECUTION_ENABLED',
    'DOG_CATCHER_V1_ENABLED',
    'DOG_CATCHER_POLICY_VERSION',
    'SOURCE_RESONANCE_SOFT_OVERRIDE_ENABLED',
    'PRE_PASS_RELAXED_CANARY_ENABLED',
    'DOG_CATCHER_HARD_GATE_PEAK10_FACTOR',
  ];
  const env = {};
  for (const key of envKeys) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  return {
    registry_summary: registrySummary,
    env,
    live_execution_enabled: String(process.env.PREMIUM_LIVE_EXECUTION_ENABLED || '').toLowerCase() === 'true',
    review_rule: 'paper-only closed-loop snapshot; strategy changes should compare fixed-window snapshots by commit and policy fingerprint',
  };
}

function loadReviewTradeRows(paperDb, tableNames, sinceTs, limit, options = {}) {
  if (!tableNames.has('paper_trades')) return [];
  const fastRecent = options.fastRecent !== false;
  const cols = getTableColumns(paperDb, 'paper_trades');
  const col = (name, fallback = `NULL AS ${name}`) => cols.has(name) ? name : fallback;
  const selectCols = [
    'id',
    col('symbol'),
    col('token_ca'),
    col('lifecycle_id'),
    col('entry_ts'),
    col('exit_ts'),
    col('exit_reason'),
    col('pnl_pct'),
    col('peak_pnl'),
    col('mark_peak_pnl'),
    col('quote_peak_pnl'),
    col('trusted_peak_pnl'),
    col('peak_trust_status'),
    col('position_size_sol'),
    col('capital_tier'),
    col('position_size_class'),
    col('paper_only'),
    col('regime_tag'),
    col('signal_to_quote_latency_ms'),
    col('signal_to_quote_drift_pct'),
    col('quote_spread_pct'),
    col('signal_route'),
    col('strategy_stage'),
    col('entry_mode'),
    col('policy_version'),
    col('entry_branch'),
    col('intervention_flags_json'),
    col('monitor_state_json'),
    col('lotto_state_json'),
    col('entry_execution_audit_json'),
    col('exit_execution_audit_json'),
  ];
  const tsWhere = sinceTs
    ? 'WHERE COALESCE(entry_ts, exit_ts, 0) >= @since OR COALESCE(exit_ts, 0) >= @since'
    : '';
  const rows = paperDb.prepare(`
    SELECT ${selectCols.join(', ')}
    FROM paper_trades
    ${fastRecent ? '' : tsWhere}
    ORDER BY ${fastRecent ? 'id DESC' : 'COALESCE(entry_ts, exit_ts, 0) DESC, id DESC'}
    LIMIT @limit
  `).all(!fastRecent && sinceTs ? { since: sinceTs, limit } : { limit });
  if (!fastRecent || !sinceTs) return rows;
  return rows.filter((row) => Number(firstValue(row.entry_ts, row.exit_ts, 0) || 0) >= sinceTs);
}

function loadPathSamplesByTrade(paperDb, tableNames, tradeIds = [], limitPerTrade = 500) {
  const byTrade = new Map();
  if (!tableNames.has('paper_trade_path_samples') || !tradeIds.length) return byTrade;
  const ids = Array.from(new Set(tradeIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)));
  if (!ids.length) return byTrade;
  const idSql = sqlInList(ids);
  const rows = paperDb.prepare(`
    SELECT *
    FROM paper_trade_path_samples
    WHERE trade_id IN (${idSql})
    ORDER BY trade_id, sample_ts ASC, id ASC
  `).all();
  const counts = new Map();
  for (const row of rows) {
    const tradeId = Number(row.trade_id);
    const count = counts.get(tradeId) || 0;
    if (count >= limitPerTrade) continue;
    counts.set(tradeId, count + 1);
    if (!byTrade.has(tradeId)) byTrade.set(tradeId, []);
    byTrade.get(tradeId).push(row);
  }
  return byTrade;
}

function buildReviewLatencySummary(paperDb, tableNames, sinceTs) {
  const summaries = [];
  if (tableNames.has('paper_trades')) {
    const cols = getTableColumns(paperDb, 'paper_trades');
    if (cols.has('signal_to_quote_latency_ms')) {
      const whereSql = sinceTs ? 'WHERE COALESCE(entry_ts, exit_ts, 0) >= @since' : '';
      const tierExpr = cols.has('capital_tier') ? "COALESCE(capital_tier, 'unknown')" : "'unknown'";
      const modeExpr = cols.has('entry_mode') ? "COALESCE(entry_mode, 'unknown')" : "'unknown'";
      const driftExpr = cols.has('signal_to_quote_drift_pct') ? 'signal_to_quote_drift_pct' : 'NULL';
      const rows = paperDb.prepare(`
        SELECT
          ${tierExpr} || ':' || ${modeExpr} AS stage,
          COUNT(*) AS n,
          AVG(signal_to_quote_latency_ms) AS avg_lag_from_source_ms,
          MAX(signal_to_quote_latency_ms) AS max_lag_from_source_ms,
          AVG(${driftExpr}) AS avg_signal_to_quote_drift_pct,
          MAX(${driftExpr}) AS max_signal_to_quote_drift_pct
        FROM paper_trades
        ${whereSql}
        GROUP BY ${tierExpr}, ${modeExpr}
        HAVING n > 0
        ORDER BY n DESC
      `).all(sinceTs ? { since: sinceTs } : {});
      summaries.push(...rows.map((row) => ({
        ...row,
        source: 'paper_trades.entry_execution_audit',
      })));
    }
  }
  if (!tableNames.has('latency_audit_events')) return summaries;
  const whereSql = sinceTs ? 'WHERE COALESCE(event_ts, signal_ts, 0) >= @since' : '';
  summaries.push(...paperDb.prepare(`
    SELECT
      stage,
      COUNT(*) AS n,
      AVG(lag_from_source_ms) AS avg_lag_from_source_ms,
      MAX(lag_from_source_ms) AS max_lag_from_source_ms,
      AVG(lag_from_receive_ms) AS avg_lag_from_receive_ms,
      MAX(lag_from_receive_ms) AS max_lag_from_receive_ms
    FROM latency_audit_events
    ${whereSql}
    GROUP BY stage
    ORDER BY stage
  `).all(sinceTs ? { since: sinceTs } : {}).map((row) => ({
    ...row,
    source: 'latency_audit_events',
  })));
  return summaries;
}

function buildReviewTableCoverage(paperDb, tableNames, options = {}) {
  const includeStats = options.includeStats === true;
  const tables = [
    { table: 'paper_trades', ts: 'COALESCE(entry_ts, exit_ts, 0)' },
    { table: 'paper_decision_events', ts: 'event_ts' },
    { table: 'paper_missed_signal_attribution', ts: 'COALESCE(signal_ts, created_event_ts, baseline_ts, 0)' },
    { table: 'source_resonance_candidates', ts: 'signal_ts' },
    { table: 'latency_audit_events', ts: 'COALESCE(event_ts, signal_ts, 0)' },
    { table: 'external_alpha_health', ts: 'last_run_ts' },
  ];
  return tables.map((item) => {
    if (!tableNames.has(item.table)) {
      return { table: item.table, available: false, rows: 0, min_ts: null, max_ts: null };
    }
    if (!includeStats) {
      return { table: item.table, available: true, stats_skipped: true };
    }
    try {
      const row = paperDb.prepare(`
        SELECT COUNT(*) AS rows, MIN(${item.ts}) AS min_ts, MAX(${item.ts}) AS max_ts
        FROM ${item.table}
      `).get();
      return {
        table: item.table,
        available: true,
        rows: Number(row.rows || 0),
        min_ts: row.min_ts ?? null,
        min_iso: row.min_ts ? new Date(Number(row.min_ts) * 1000).toISOString() : null,
        max_ts: row.max_ts ?? null,
        max_iso: row.max_ts ? new Date(Number(row.max_ts) * 1000).toISOString() : null,
      };
    } catch (error) {
      return { table: item.table, available: true, error: error.message };
    }
  });
}

function buildReviewHealthRows(paperDb, tableNames, tableName) {
  if (!tableNames.has(tableName)) return [];
  return paperDb.prepare(`SELECT * FROM ${tableName} ORDER BY updated_at DESC`).all();
}

const CLOSED_LOOP_PROBE_MODES = [
  'hard_gate_pass_tiny_probe',
  'pre_pass_resonance_tiny_probe',
  'source_resonance_tiny_probe',
  'lotto_upstream_realtime_tiny_scout',
];

function emptyClosedLoopProbeSummary(mode) {
  return {
    entry_mode: mode,
    armed_events: 0,
    armed_unique: 0,
    reject_events: 0,
    reject_unique: 0,
    wait_events: 0,
    wait_unique: 0,
    deduped_unique: 0,
    quote_issue_unique: 0,
    fills: 0,
    fill_unique: 0,
    wins: 0,
    avg_pnl_pct: null,
    total_pnl_pct: null,
    max_peak_pnl_pct: null,
  };
}

function isSqliteBusyError(error) {
  const message = String(error?.message || error || '').toLowerCase();
  return message.includes('database is locked') || message.includes('sqlite_busy');
}

function skippedClosedLoopProbeSummary(reason, error = null) {
  return {
    skipped: true,
    skip_reason: reason,
    error: error ? String(error.message || error) : undefined,
    by_mode: Object.fromEntries(CLOSED_LOOP_PROBE_MODES.map((mode) => [mode, emptyClosedLoopProbeSummary(mode)])),
    paper_pnl_by_entry_mode: [],
  };
}

function skippedClosedLoopSourceSummary(reason, error = null) {
  return {
    available: false,
    skipped: true,
    skip_reason: reason,
    error: error ? String(error.message || error) : undefined,
    candidate_rows: null,
    unique_tokens: null,
    gmgn_pre_seen_unique: null,
    quote_clean_unique: null,
    telegram_gmgn_unique: null,
  };
}

function skippedClosedLoopMissedDogSummary(reason, error = null) {
  return {
    available: false,
    skipped: true,
    skip_reason: reason,
    error: error ? String(error.message || error) : undefined,
    unique_tokens: null,
    quote_clean_unique: null,
    quote_clean_dog_unique: null,
    gold_unique: null,
    silver_unique: null,
    bronze_unique: null,
    top_missed_dogs: [],
    by_final_blocker: [],
  };
}

function closedLoopTimestampExpr(cols, tableAlias = '') {
  const prefix = tableAlias ? `${tableAlias}.` : '';
  if (cols.has('timestamp')) {
    return `CASE WHEN ${prefix}timestamp > 1000000000000 THEN CAST(${prefix}timestamp / 1000 AS INTEGER) ELSE CAST(${prefix}timestamp AS INTEGER) END`;
  }
  if (cols.has('timestamp_sec')) return `CAST(${prefix}timestamp_sec AS INTEGER)`;
  if (cols.has('created_at')) return `CAST(strftime('%s', ${prefix}created_at) AS INTEGER)`;
  return '0';
}

function buildClosedLoopSignalSummary(signalDb, sinceTs) {
  const empty = {
    available: false,
    premium_signal_rows: 0,
    premium_unique_tokens: 0,
    hard_gate_pass_rows: 0,
    hard_gate_pass_unique: 0,
    legacy_observe_unique: 0,
  };
  if (!signalDb) return empty;
  const signalTables = new Set(
    signalDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
  );
  if (!signalTables.has('premium_signals')) return empty;
  const signalCols = getTableColumns(signalDb, 'premium_signals');
  if (!signalCols.has('token_ca')) return { ...empty, available: true, note: 'premium_signals.token_ca missing' };
  const tsExpr = closedLoopTimestampExpr(signalCols);
  const whereSql = sinceTs
    ? (signalCols.has('timestamp')
      ? `WHERE ((timestamp > 1000000000000 AND timestamp >= @sinceMs) OR (timestamp <= 1000000000000 AND timestamp >= @since))`
      : `WHERE ${tsExpr} >= @since`)
    : '';
  const params = sinceTs
    ? (signalCols.has('timestamp') ? { since: sinceTs, sinceMs: sinceTs * 1000 } : { since: sinceTs })
    : {};
  const hardGateExpr = signalCols.has('hard_gate_status') ? 'hard_gate_status' : "''";
  const legacyStatusSql = sqlInList(LOTTO_OBSERVE_UPSTREAM_STATUSES);
  return {
    ...empty,
    available: true,
    ...signalDb.prepare(`
      SELECT
        COUNT(*) AS premium_signal_rows,
        COUNT(DISTINCT token_ca) AS premium_unique_tokens,
        COALESCE(SUM(CASE WHEN ${hardGateExpr} = 'PASS' THEN 1 ELSE 0 END), 0) AS hard_gate_pass_rows,
        COUNT(DISTINCT CASE WHEN ${hardGateExpr} = 'PASS' THEN token_ca END) AS hard_gate_pass_unique,
        COUNT(DISTINCT CASE WHEN ${hardGateExpr} IN (${legacyStatusSql}) THEN token_ca END) AS legacy_observe_unique
      FROM premium_signals
      ${whereSql}
    `).get(params),
  };
}

export function buildClosedLoopProbeSummary(
  paperDb,
  tableNames,
  sinceTs,
  { includePaperPnlDetails = true, includeDecisionEventDetails = true } = {}
) {
  const byMode = Object.fromEntries(CLOSED_LOOP_PROBE_MODES.map((mode) => [mode, emptyClosedLoopProbeSummary(mode)]));
  const paperPnlByEntryMode = [];
  if (!paperDb) return { by_mode: byMode, paper_pnl_by_entry_mode: paperPnlByEntryMode };

  if (tableNames.has('paper_decision_events')) {
    const probeComponentFilterSql = `
          component IN (
            'hard_gate_pass_probe',
            'pre_pass_resonance_probe',
            'source_resonance_probe',
            'lotto_upstream_realtime_scout',
            'lotto_upstream_realtime_probe'
          )`;
    const rows = includeDecisionEventDetails
      ? paperDb.prepare(`
      WITH events AS (
        SELECT
          CASE
            WHEN component = 'hard_gate_pass_probe' THEN 'hard_gate_pass_tiny_probe'
            WHEN component = 'pre_pass_resonance_probe' THEN 'pre_pass_resonance_tiny_probe'
            WHEN component = 'source_resonance_probe' THEN 'source_resonance_tiny_probe'
            WHEN component IN ('lotto_upstream_realtime_scout', 'lotto_upstream_realtime_probe') THEN 'lotto_upstream_realtime_tiny_scout'
            ELSE NULL
          END AS mode,
          event_type,
          decision,
          reason,
          token_ca
        FROM paper_decision_events
        ${sinceTs ? `WHERE event_ts >= @since AND ${probeComponentFilterSql}` : `WHERE ${probeComponentFilterSql}`}
      )
      SELECT
        mode,
        SUM(CASE WHEN event_type = 'pending_entry' THEN 1 ELSE 0 END) AS armed_events,
        COUNT(DISTINCT CASE WHEN event_type = 'pending_entry' THEN token_ca END) AS armed_unique,
        SUM(CASE WHEN decision = 'reject' OR event_type LIKE '%reject%' THEN 1 ELSE 0 END) AS reject_events,
        COUNT(DISTINCT CASE WHEN decision = 'reject' OR event_type LIKE '%reject%' THEN token_ca END) AS reject_unique,
        SUM(CASE WHEN decision = 'wait' THEN 1 ELSE 0 END) AS wait_events,
        COUNT(DISTINCT CASE WHEN decision = 'wait' THEN token_ca END) AS wait_unique,
        COUNT(DISTINCT CASE WHEN reason = 'probe_deduped_existing_mode' THEN token_ca END) AS deduped_unique,
        COUNT(DISTINCT CASE WHEN reason LIKE 'quote_%' THEN token_ca END) AS quote_issue_unique
      FROM events
      WHERE mode IS NOT NULL
      GROUP BY mode
    `).all(sinceTs ? { since: sinceTs } : {})
      : paperDb.prepare(`
      WITH events AS (
        SELECT
          CASE
            WHEN component = 'hard_gate_pass_probe' THEN 'hard_gate_pass_tiny_probe'
            WHEN component = 'pre_pass_resonance_probe' THEN 'pre_pass_resonance_tiny_probe'
            WHEN component = 'source_resonance_probe' THEN 'source_resonance_tiny_probe'
            WHEN component IN ('lotto_upstream_realtime_scout', 'lotto_upstream_realtime_probe') THEN 'lotto_upstream_realtime_tiny_scout'
            ELSE NULL
          END AS mode,
          token_ca
        FROM paper_decision_events
        ${sinceTs ? 'WHERE event_ts >= @since AND' : 'WHERE'}
          event_type = 'pending_entry'
          AND ${probeComponentFilterSql}
      )
      SELECT
        mode,
        COUNT(*) AS armed_events,
        COUNT(DISTINCT token_ca) AS armed_unique,
        0 AS reject_events,
        0 AS reject_unique,
        0 AS wait_events,
        0 AS wait_unique,
        0 AS deduped_unique,
        0 AS quote_issue_unique
      FROM events
      WHERE mode IS NOT NULL
      GROUP BY mode
    `).all(sinceTs ? { since: sinceTs } : {});
    for (const row of rows) {
      if (!byMode[row.mode]) byMode[row.mode] = emptyClosedLoopProbeSummary(row.mode);
      Object.assign(byMode[row.mode], {
        armed_events: Number(row.armed_events || 0),
        armed_unique: Number(row.armed_unique || 0),
        reject_events: Number(row.reject_events || 0),
        reject_unique: Number(row.reject_unique || 0),
        wait_events: Number(row.wait_events || 0),
        wait_unique: Number(row.wait_unique || 0),
        deduped_unique: Number(row.deduped_unique || 0),
        quote_issue_unique: Number(row.quote_issue_unique || 0),
      });
    }
  }

  if (tableNames.has('paper_trades')) {
    const tradeCols = getTableColumns(paperDb, 'paper_trades');
    const trustedPeakExpr = trustedTradePeakSqlExpr(tradeCols);
    const modeFilterSql = includePaperPnlDetails
      ? ''
      : `AND COALESCE(entry_mode, 'unknown') IN (${sqlInList(CLOSED_LOOP_PROBE_MODES)})`;
    const tradeSourceSql = sinceTs
      ? `
      WITH recent_trades AS (
        SELECT entry_mode, token_ca, pnl_pct, ${trustedPeakExpr} AS trusted_peak_pnl
        FROM paper_trades
        WHERE entry_ts >= @since ${modeFilterSql}
        UNION ALL
        SELECT entry_mode, token_ca, pnl_pct, ${trustedPeakExpr} AS trusted_peak_pnl
        FROM paper_trades
        WHERE entry_ts IS NULL AND exit_ts >= @since ${modeFilterSql}
      )
      `
      : '';
    const tradeTableSql = sinceTs ? 'recent_trades' : 'paper_trades';
    const tradeWhereSql = !sinceTs && modeFilterSql ? `WHERE 1=1 ${modeFilterSql}` : '';
    const tradeRows = paperDb.prepare(`
      ${tradeSourceSql}
      SELECT
        COALESCE(entry_mode, 'unknown') AS entry_mode,
        COUNT(*) AS fills,
        COUNT(DISTINCT token_ca) AS fill_unique,
        SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
        AVG(pnl_pct) AS avg_pnl,
        SUM(pnl_pct) AS total_pnl,
        MAX(${sinceTs ? 'trusted_peak_pnl' : trustedPeakExpr}) AS max_peak_pnl
      FROM ${tradeTableSql}
      ${tradeWhereSql}
      GROUP BY COALESCE(entry_mode, 'unknown')
      ORDER BY fills DESC, total_pnl DESC
    `).all(sinceTs ? { since: sinceTs } : {});
    for (const row of tradeRows) {
      const entry = {
        entry_mode: row.entry_mode,
        fills: Number(row.fills || 0),
        fill_unique: Number(row.fill_unique || 0),
        wins: Number(row.wins || 0),
        win_rate: row.fills ? Number(row.wins || 0) / Number(row.fills) : null,
        avg_pnl_pct: row.avg_pnl == null ? null : roundNumber(Number(row.avg_pnl) * 100, 2),
        total_pnl_pct: row.total_pnl == null ? null : roundNumber(Number(row.total_pnl) * 100, 2),
        max_peak_pnl_pct: row.max_peak_pnl == null ? null : roundNumber(Number(row.max_peak_pnl) * 100, 2),
      };
      if (includePaperPnlDetails) paperPnlByEntryMode.push(entry);
      if (byMode[row.entry_mode]) {
        Object.assign(byMode[row.entry_mode], {
          fills: entry.fills,
          fill_unique: entry.fill_unique,
          wins: entry.wins,
          avg_pnl_pct: entry.avg_pnl_pct,
          total_pnl_pct: entry.total_pnl_pct,
          max_peak_pnl_pct: entry.max_peak_pnl_pct,
        });
      }
    }
  }
  return { by_mode: byMode, paper_pnl_by_entry_mode: paperPnlByEntryMode };
}

export function buildClosedLoopMissedDogSummary(paperDb, tableNames, sinceTs, limit, { includeDetails = true } = {}) {
  const empty = {
    available: false,
    unique_tokens: 0,
    quote_clean_unique: 0,
    quote_clean_dog_unique: 0,
    gold_unique: 0,
    silver_unique: 0,
    bronze_unique: 0,
    mark_only_gold_unique: 0,
    mark_only_silver_unique: 0,
    mark_only_bronze_unique: 0,
    top_missed_dogs: [],
    by_final_blocker: [],
  };
  if (!paperDb || !tableNames.has('paper_missed_signal_attribution')) return empty;
  const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
  const missedColumn = (name, fallback = 'NULL') => missedCols.has(name) ? `m.${name}` : fallback;
  const eventTsExpr = `COALESCE(${[
    missedCols.has('created_event_ts') ? 'm.created_event_ts' : null,
    missedCols.has('signal_ts') ? 'm.signal_ts' : null,
    missedCols.has('baseline_ts') ? 'm.baseline_ts' : null,
    '0',
  ].filter(Boolean).join(', ')})`;
  const missedWhereTsExpr = missedCols.has('created_event_ts')
    ? 'm.created_event_ts'
    : (missedCols.has('signal_ts') ? 'm.signal_ts' : eventTsExpr);
  const maxPnlExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
  const markPnlExpr = markMissedPeakSqlExpr(missedCols, 'm');
  const quoteCleanExpr = missedCols.has('tradable_missed')
    ? `CASE
        WHEN COALESCE(m.tradable_missed, 0) = 1
         AND COALESCE(${missedCols.has('would_stop_before_peak') ? 'm.would_stop_before_peak' : '0'}, 0) != 1
        THEN 1 ELSE 0
      END`
    : '0';
  const hasPaperTrades = tableNames.has('paper_trades');
  const tradeCols = hasPaperTrades ? getTableColumns(paperDb, 'paper_trades') : new Set();
  const tradeWindowPredicates = [
    tradeCols.has('entry_ts') ? 'COALESCE(pt.entry_ts, 0) >= @since' : null,
    tradeCols.has('exit_ts') ? 'COALESCE(pt.exit_ts, 0) >= @since' : null,
    tradeCols.has('accounting_outcome') ? "COALESCE(pt.accounting_outcome, '') = 'open'" : null,
    tradeCols.has('created_at') ? 'COALESCE(strftime(\'%s\', pt.created_at), 0) >= @since' : null,
  ].filter(Boolean);
  const tradeWindowFilter = sinceTs && tradeWindowPredicates.length > 0
    ? `AND (${tradeWindowPredicates.join(' OR ')})`
    : '';
  const caughtTokenFilter = hasPaperTrades && tradeCols.has('token_ca')
    ? `AND NOT EXISTS (
        SELECT 1
        FROM paper_trades pt
        WHERE pt.token_ca = m.token_ca
          ${tradeWindowFilter}
      )`
    : '';
  const missedBaseCte = `
    WITH base AS (
      SELECT
        m.token_ca,
        COALESCE(${missedColumn('symbol')}, substr(m.token_ca, 1, 8), '?') AS symbol,
        ${missedColumn('signal_id')} AS signal_id,
        ${missedColumn('signal_ts')} AS signal_ts,
        COALESCE(${missedColumn('route')}, '-') AS route,
        COALESCE(${missedColumn('component')}, '-') AS final_component,
        COALESCE(${missedColumn('reject_reason')}, '-') AS final_reason,
        ${missedColumn('rejection_hardness')} AS rejection_hardness,
        ${missedColumn('tradability_status')} AS tradability_status,
        ${missedColumn('tradability_reason')} AS tradability_reason,
        ${missedColumn('theoretical_peak_pnl')} AS theoretical_peak_pnl,
        ${missedColumn('quote_clean_peak_pnl')} AS quote_clean_peak_pnl,
        ${missedColumn('executable_peak_pnl')} AS executable_peak_pnl,
        ${missedColumn('executable_peak_source')} AS executable_peak_source,
        ${missedColumn('quote_sample_n', '0')} AS quote_sample_n,
        ${missedColumn('tradable_peak_pnl')} AS tradable_peak_pnl,
        ${quoteCleanExpr} AS quote_clean,
        ${maxPnlExpr} AS max_pnl,
        ${markPnlExpr} AS mark_pnl,
        CASE
          WHEN ${maxPnlExpr} >= 0.25 THEN 'trusted_peak'
          WHEN ${markPnlExpr} >= 0.25 THEN 'mark_only_peak_untrusted'
          ELSE 'sub25_or_unknown'
        END AS peak_trust_status,
        ${eventTsExpr} AS event_ts,
        CASE
          WHEN m.component = 'source_resonance_probe' THEN 'source_resonance_tiny_probe'
          WHEN m.component = 'hard_gate_pass_probe' THEN 'hard_gate_pass_tiny_probe'
          WHEN m.component = 'pre_pass_resonance_probe' THEN 'pre_pass_resonance_tiny_probe'
          WHEN m.component IN ('lotto_upstream_realtime_scout', 'lotto_upstream_realtime_probe') THEN 'lotto_upstream_realtime_tiny_scout'
          ELSE NULL
        END AS entry_mode_candidate
      FROM paper_missed_signal_attribution m
      WHERE 1 = 1
        ${sinceTs ? `AND ${missedWhereTsExpr} >= @since` : ''}
        ${caughtTokenFilter}
    ),
    ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY token_ca
          ORDER BY COALESCE(max_pnl, 0) DESC, COALESCE(mark_pnl, 0) DESC, COALESCE(event_ts, 0) DESC
        ) AS rn
      FROM base
      WHERE token_ca IS NOT NULL AND token_ca != ''
    )`;

  let summary;
  let topBase = [];
  let byFinalBlocker = [];

  if (includeDetails) {
    const one = paperDb.prepare(`
      ${missedBaseCte}
      SELECT
        token_ca,
        symbol,
        signal_id,
        signal_ts,
        route,
        final_component,
        final_reason,
        rejection_hardness,
        tradability_status,
        tradability_reason,
        theoretical_peak_pnl,
        quote_clean_peak_pnl,
        executable_peak_pnl,
        executable_peak_source,
        quote_sample_n,
        tradable_peak_pnl,
        quote_clean,
        max_pnl,
        mark_pnl,
        peak_trust_status,
        event_ts,
        entry_mode_candidate
      FROM ranked
      WHERE rn = 1
    `).all(sinceTs ? { since: sinceTs } : {}).map((row) => ({
      ...row,
      source_resonance_cohort: null,
      gmgn_pre_seen: null,
      gmgn_lead_time_sec: null,
      quote_clean: Number(row.quote_clean || 0) === 1,
      max_pnl: Number(row.max_pnl || 0),
      mark_pnl: Number(row.mark_pnl || 0),
      peak_trust_status: row.peak_trust_status || null,
      tradable_peak_pnl: row.tradable_peak_pnl == null ? null : Number(row.tradable_peak_pnl),
      rejection_hardness: row.rejection_hardness || null,
      theoretical_peak_pnl: row.theoretical_peak_pnl == null ? null : Number(row.theoretical_peak_pnl),
      quote_clean_peak_pnl: row.quote_clean_peak_pnl == null ? null : Number(row.quote_clean_peak_pnl),
      executable_peak_pnl: row.executable_peak_pnl == null ? null : Number(row.executable_peak_pnl),
      executable_peak_source: row.executable_peak_source || null,
      quote_sample_n: row.quote_sample_n == null ? null : Number(row.quote_sample_n),
      final_blocker_key: `${row.route}:${row.final_component}:${row.final_reason}`,
    }));

    summary = {
      unique_tokens: one.length,
      quote_clean_unique: 0,
      quote_clean_dog_unique: 0,
      gold_unique: 0,
      silver_unique: 0,
      bronze_unique: 0,
      mark_only_gold_unique: 0,
      mark_only_silver_unique: 0,
      mark_only_bronze_unique: 0,
    };
    const blockerMap = new Map();
    for (const row of one) {
      if (row.quote_clean) summary.quote_clean_unique += 1;
      if (row.quote_clean && row.max_pnl >= 0.25) summary.quote_clean_dog_unique += 1;
      if (row.max_pnl >= 1.0) summary.gold_unique += 1;
      else if (row.max_pnl >= 0.5) summary.silver_unique += 1;
      else if (row.max_pnl >= 0.25) summary.bronze_unique += 1;
      if (row.max_pnl < 1.0 && row.mark_pnl >= 1.0) summary.mark_only_gold_unique += 1;
      else if (row.max_pnl < 0.5 && row.mark_pnl >= 0.5) summary.mark_only_silver_unique += 1;
      else if (row.max_pnl < 0.25 && row.mark_pnl >= 0.25) summary.mark_only_bronze_unique += 1;
      const blocker = blockerMap.get(row.final_blocker_key) || {
        route: row.route,
        final_component: row.final_component,
        final_reason: row.final_reason,
        final_blocker_key: row.final_blocker_key,
        unique_tokens: 0,
        quote_clean_unique: 0,
        gold_unique: 0,
        silver_unique: 0,
        bronze_unique: 0,
        mark_only_gold_unique: 0,
        mark_only_silver_unique: 0,
        mark_only_bronze_unique: 0,
        max_pnl: 0,
        mark_pnl: 0,
      };
      blocker.unique_tokens += 1;
      if (row.quote_clean) blocker.quote_clean_unique += 1;
      if (row.max_pnl >= 1.0) blocker.gold_unique += 1;
      else if (row.max_pnl >= 0.5) blocker.silver_unique += 1;
      else if (row.max_pnl >= 0.25) blocker.bronze_unique += 1;
      if (row.max_pnl < 1.0 && row.mark_pnl >= 1.0) blocker.mark_only_gold_unique += 1;
      else if (row.max_pnl < 0.5 && row.mark_pnl >= 0.5) blocker.mark_only_silver_unique += 1;
      else if (row.max_pnl < 0.25 && row.mark_pnl >= 0.25) blocker.mark_only_bronze_unique += 1;
      blocker.max_pnl = Math.max(blocker.max_pnl, row.max_pnl);
      blocker.mark_pnl = Math.max(blocker.mark_pnl, row.mark_pnl || 0);
      blockerMap.set(row.final_blocker_key, blocker);
    }

    topBase = one
      .filter((row) => row.max_pnl >= 0.25 || row.mark_pnl >= 0.25)
      .sort((a, b) => (
        Number(b.max_pnl || 0) - Number(a.max_pnl || 0)
        || Number(b.mark_pnl || 0) - Number(a.mark_pnl || 0)
        || Number(b.event_ts || 0) - Number(a.event_ts || 0)
      ))
      .slice(0, limit);

    byFinalBlocker = Array.from(blockerMap.values())
      .sort((a, b) => (
        Number(b.gold_unique || 0) - Number(a.gold_unique || 0)
        || Number(b.silver_unique || 0) - Number(a.silver_unique || 0)
        || Number(b.bronze_unique || 0) - Number(a.bronze_unique || 0)
        || Number(b.mark_only_gold_unique || 0) - Number(a.mark_only_gold_unique || 0)
        || Number(b.mark_only_silver_unique || 0) - Number(a.mark_only_silver_unique || 0)
        || Number(b.mark_only_bronze_unique || 0) - Number(a.mark_only_bronze_unique || 0)
        || Number(b.unique_tokens || 0) - Number(a.unique_tokens || 0)
        || Number(b.max_pnl || 0) - Number(a.max_pnl || 0)
      ))
      .slice(0, limit)
      .map((row) => ({
        ...row,
        max_pnl_pct: roundNumber(Number(row.max_pnl || 0) * 100, 2),
        mark_pnl_pct: roundNumber(Number(row.mark_pnl || 0) * 100, 2),
      }));
  } else {
    const summaryOnlyCte = `
      WITH base AS (
	        SELECT
	          m.token_ca,
	          ${quoteCleanExpr} AS quote_clean,
	          ${maxPnlExpr} AS max_pnl,
	          ${markPnlExpr} AS mark_pnl
        FROM paper_missed_signal_attribution m
        WHERE 1 = 1
          ${sinceTs ? `AND ${missedWhereTsExpr} >= @since` : ''}
          ${caughtTokenFilter}
      ),
      per_token AS (
        SELECT
	          token_ca,
	          MAX(CASE WHEN COALESCE(quote_clean, 0) = 1 THEN 1 ELSE 0 END) AS quote_clean,
	          MAX(COALESCE(max_pnl, 0)) AS max_pnl,
	          MAX(COALESCE(mark_pnl, 0)) AS mark_pnl,
	          MAX(CASE
            WHEN COALESCE(quote_clean, 0) = 1 AND COALESCE(max_pnl, 0) >= 0.25
            THEN 1 ELSE 0
          END) AS quote_clean_dog
        FROM base b
        WHERE token_ca IS NOT NULL AND token_ca != ''
        GROUP BY token_ca
      )`;
    summary = paperDb.prepare(`
      ${summaryOnlyCte}
      SELECT
        COUNT(*) AS unique_tokens,
        COALESCE(SUM(CASE WHEN quote_clean = 1 THEN 1 ELSE 0 END), 0) AS quote_clean_unique,
        COALESCE(SUM(CASE WHEN quote_clean_dog = 1 THEN 1 ELSE 0 END), 0) AS quote_clean_dog_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) >= 1.0 THEN 1 ELSE 0 END), 0) AS gold_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) >= 0.5 AND COALESCE(max_pnl, 0) < 1.0 THEN 1 ELSE 0 END), 0) AS silver_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) >= 0.25 AND COALESCE(max_pnl, 0) < 0.5 THEN 1 ELSE 0 END), 0) AS bronze_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) < 1.0 AND COALESCE(mark_pnl, 0) >= 1.0 THEN 1 ELSE 0 END), 0) AS mark_only_gold_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) < 0.5 AND COALESCE(mark_pnl, 0) >= 0.5 AND COALESCE(mark_pnl, 0) < 1.0 THEN 1 ELSE 0 END), 0) AS mark_only_silver_unique,
	        COALESCE(SUM(CASE WHEN COALESCE(max_pnl, 0) < 0.25 AND COALESCE(mark_pnl, 0) >= 0.25 AND COALESCE(mark_pnl, 0) < 0.5 THEN 1 ELSE 0 END), 0) AS mark_only_bronze_unique
      FROM per_token
    `).get(sinceTs ? { since: sinceTs } : {});
  }

  if (includeDetails && tableNames.has('source_resonance_candidates') && topBase.length > 0) {
    const sourceCols = getTableColumns(paperDb, 'source_resonance_candidates');
    const sourceColumn = (name, fallback = 'NULL') => sourceCols.has(name) ? name : `${fallback} AS ${name}`;
    const sourceOrderTs = sourceCols.has('signal_ts') ? 'COALESCE(signal_ts, 0)' : '0';
    const sourceOrderUpdated = sourceCols.has('updated_at') ? 'COALESCE(updated_at, 0)' : '0';
    const tokens = topBase.map((row) => row.token_ca);
    const tokenSql = sqlInList(tokens);
    const sourceRows = paperDb.prepare(`
      SELECT
        token_ca,
        ${sourceColumn('cohort')},
        ${sourceColumn('gmgn_pre_seen', '0')},
        ${sourceColumn('gmgn_lead_time_sec')},
        ${sourceOrderTs} AS source_signal_ts,
        ${sourceOrderUpdated} AS source_updated_at
      FROM source_resonance_candidates
      WHERE token_ca IN (${tokenSql})
      ORDER BY token_ca, ${sourceOrderTs} DESC, ${sourceOrderUpdated} DESC
    `).all();
    const sourceByToken = new Map();
    for (const row of sourceRows) {
      if (!sourceByToken.has(row.token_ca)) sourceByToken.set(row.token_ca, row);
    }
    for (const row of topBase) {
      const source = sourceByToken.get(row.token_ca) || {};
      row.source_resonance_cohort = source.cohort || null;
      row.gmgn_pre_seen = source.gmgn_pre_seen == null ? null : Number(source.gmgn_pre_seen) === 1;
      row.gmgn_lead_time_sec = source.gmgn_lead_time_sec ?? null;
    }
  }

  const topMissedDogs = topBase
    .map((row) => ({
      ...row,
      max_pnl_pct: roundNumber(Number(row.max_pnl || 0) * 100, 2),
      mark_pnl_pct: row.mark_pnl == null ? null : roundNumber(Number(row.mark_pnl) * 100, 2),
      peak_trust_status: row.peak_trust_status || null,
      theoretical_peak_pnl_pct: row.theoretical_peak_pnl == null ? null : roundNumber(Number(row.theoretical_peak_pnl) * 100, 2),
      quote_clean_peak_pnl_pct: row.quote_clean_peak_pnl == null ? null : roundNumber(Number(row.quote_clean_peak_pnl) * 100, 2),
      executable_peak_pnl_pct: row.executable_peak_pnl == null ? null : roundNumber(Number(row.executable_peak_pnl) * 100, 2),
      tradable_peak_pnl_pct: row.tradable_peak_pnl == null ? null : roundNumber(Number(row.tradable_peak_pnl) * 100, 2),
    }));
  return {
    available: true,
    unique_tokens: Number(summary?.unique_tokens || 0),
    quote_clean_unique: Number(summary?.quote_clean_unique || 0),
    quote_clean_dog_unique: Number(summary?.quote_clean_dog_unique || 0),
    gold_unique: Number(summary?.gold_unique || 0),
    silver_unique: Number(summary?.silver_unique || 0),
    bronze_unique: Number(summary?.bronze_unique || 0),
    mark_only_gold_unique: Number(summary?.mark_only_gold_unique || 0),
    mark_only_silver_unique: Number(summary?.mark_only_silver_unique || 0),
    mark_only_bronze_unique: Number(summary?.mark_only_bronze_unique || 0),
    top_missed_dogs: topMissedDogs,
    by_final_blocker: byFinalBlocker,
  };
}

function buildClosedLoopSourceResonanceSummary(paperDb, tableNames, sinceTs, { includeSummary = true } = {}) {
  const empty = {
    available: false,
    candidate_rows: 0,
    unique_tokens: 0,
    gmgn_pre_seen_unique: 0,
    quote_clean_unique: 0,
    telegram_gmgn_unique: 0,
  };
  if (!paperDb || !tableNames.has('source_resonance_candidates')) return empty;
  if (!includeSummary) return { ...empty, available: true, skipped: true };
  const cols = getTableColumns(paperDb, 'source_resonance_candidates');
  const tsExpr = cols.has('signal_ts') ? 'COALESCE(signal_ts, 0)' : '0';
  const gmgnPreSeenExpr = cols.has('gmgn_pre_seen') ? 'COALESCE(gmgn_pre_seen, 0)' : '0';
  const quoteCleanExpr = cols.has('quote_clean_seen') ? 'COALESCE(quote_clean_seen, 0)' : '0';
  const cohortExpr = cols.has('cohort') ? 'cohort' : "''";
  const whereSql = sinceTs ? `WHERE ${tsExpr} >= @since` : '';
  return {
    ...empty,
    available: true,
    ...paperDb.prepare(`
      SELECT
        COUNT(*) AS candidate_rows,
        COUNT(DISTINCT token_ca) AS unique_tokens,
        COUNT(DISTINCT CASE WHEN ${gmgnPreSeenExpr} = 1 THEN token_ca END) AS gmgn_pre_seen_unique,
        COUNT(DISTINCT CASE WHEN ${quoteCleanExpr} = 1 THEN token_ca END) AS quote_clean_unique,
        COUNT(DISTINCT CASE WHEN ${cohortExpr} IN ('telegram_gmgn', 'telegram_gmgn_quote_clean') THEN token_ca END) AS telegram_gmgn_unique
      FROM source_resonance_candidates
      ${whereSql}
    `).get(sinceTs ? { since: sinceTs } : {}),
  };
}

function buildClosedLoopDecision(report72h) {
  const probesSkipped = Boolean(report72h?.probes?.skipped);
  const hard = report72h?.probes?.by_mode?.hard_gate_pass_tiny_probe || emptyClosedLoopProbeSummary('hard_gate_pass_tiny_probe');
  const prePass = report72h?.probes?.by_mode?.pre_pass_resonance_tiny_probe || emptyClosedLoopProbeSummary('pre_pass_resonance_tiny_probe');
  const source = report72h?.probes?.by_mode?.source_resonance_tiny_probe || emptyClosedLoopProbeSummary('source_resonance_tiny_probe');
  const passUnique = Number(report72h?.premium_signals?.hard_gate_pass_unique || 0);
  const cleanMissedDogsRaw = report72h?.missed_dogs?.quote_clean_dog_unique;
  const cleanMissedDogs = cleanMissedDogsRaw == null ? null : Number(cleanMissedDogsRaw || 0);
  const hardCoverage = !probesSkipped && passUnique > 0 ? hard.armed_unique / passUnique : null;
  const actions = [];
  if (cleanMissedDogs == null) actions.push('run_include_raw_72h_for_clean_quote_missed_dog_count');
  else if (cleanMissedDogs > 0) actions.push('inspect_top_clean_quote_missed_dog_blockers');
  if (probesSkipped) actions.push('run_include_72h_probes_for_probe_coverage');
  else if (passUnique > 0 && (hardCoverage == null || hardCoverage < 0.5)) actions.push('continue_hard_gate_pass_tiny_probe_until_pass_coverage_improves');
  if (!probesSkipped && (hard.fills || 0) < 50) actions.push('collect_more_hard_gate_baseline_samples_before_tightening');
  if (!probesSkipped && (prePass.fills || 0) < 50) actions.push('collect_more_pre_pass_resonance_samples_before_tightening');
  if (!probesSkipped && (source.fills || 0) < 50) actions.push('collect_more_source_resonance_samples_before_upgrade');
  if ((hard.fills || 0) >= 50 && hard.avg_pnl_pct != null && hard.avg_pnl_pct < 0) actions.push('tighten_or_lower_hard_gate_baseline_rate_limit');
  if (
    (source.fills || 0) >= 50
    && source.avg_pnl_pct != null
    && hard.avg_pnl_pct != null
    && source.avg_pnl_pct > hard.avg_pnl_pct
  ) {
    actions.push('consider_larger_paper_size_for_source_resonance_only');
  }
  return {
    horizon: '72h',
    status: actions.some((action) => action.startsWith('tighten')) ? 'tighten'
      : actions.some((action) => action.startsWith('consider')) ? 'upgrade_paper_only'
        : 'collect_more_samples',
    hard_gate_pass_probe_coverage: hardCoverage == null ? null : roundNumber(hardCoverage, 3),
    clean_quote_missed_dog_unique: cleanMissedDogs,
    actions,
    guardrails: {
      execution_scope: 'paper_only',
      blocked_entry_modes_remain_blocked: true,
      live_execution_requires_env: 'PREMIUM_LIVE_EXECUTION_ENABLED=true',
    },
  };
}

function buildClosedLoopWindowReport({
  signalDb,
  paperDb,
  sinceTs,
  limit,
  includeMissedSummary = true,
  includeMissedDetails = true,
  includeSourceSummary = true,
  includeProbeSummary = true,
  includePaperPnlDetails = true,
  includeDecisionEventDetails = true,
  timings = null,
  timingPrefix = 'window',
}) {
  const timed = (name, fn) => {
    const startedAt = Date.now();
    try {
      const value = fn();
      if (timings) timings[`${timingPrefix}.${name}`] = Date.now() - startedAt;
      return value;
    } catch (error) {
      if (!isSqliteBusyError(error)) throw error;
      if (timings) {
        timings[`${timingPrefix}.${name}`] = Date.now() - startedAt;
        timings[`${timingPrefix}.${name}.error`] = 'database_busy';
      }
      if (name === 'table_names') return new Set();
      if (name === 'probes') return skippedClosedLoopProbeSummary('database_busy', error);
      if (name === 'source_resonance') return skippedClosedLoopSourceSummary('database_busy', error);
      if (name === 'missed_dogs') return skippedClosedLoopMissedDogSummary('database_busy', error);
      throw error;
    }
  };
  const tableNames = timed('table_names', () => (
    paperDb
      ? new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name))
      : new Set()
  ));
  return {
    since_ts: sinceTs,
    since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
    premium_signals: timed('premium_signals', () => buildClosedLoopSignalSummary(signalDb, sinceTs)),
    probes: timed('probes', () => (
      includeProbeSummary
        ? buildClosedLoopProbeSummary(paperDb, tableNames, sinceTs, {
          includePaperPnlDetails,
          includeDecisionEventDetails,
        })
        : {
          ...skippedClosedLoopProbeSummary('omitted_from_default_72h_decision_path'),
        }
    )),
    source_resonance: timed('source_resonance', () => buildClosedLoopSourceResonanceSummary(paperDb, tableNames, sinceTs, {
      includeSummary: includeSourceSummary,
    })),
    missed_dogs: timed('missed_dogs', () => (
      includeMissedSummary
        ? buildClosedLoopMissedDogSummary(paperDb, tableNames, sinceTs, limit, {
          includeDetails: includeMissedDetails,
        })
        : skippedClosedLoopMissedDogSummary('omitted_from_default_72h_decision_path')
    )),
  };
}

function cleanupOpenPaperPositions({ reason = 'manual_cleanup', pnlPct = 0 } = {}) {
  const paperDbPath = getPaperDbPath();
  if (!fs.existsSync(paperDbPath)) {
    const error = new Error(`Paper trades database not found at ${paperDbPath}`);
    error.statusCode = 404;
    throw error;
  }

  const paperDb = openDashboardSqlite(paperDbPath);
  try {
    const tableExists = paperDb.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='paper_trades'`).get();
    if (!tableExists) {
      const error = new Error('paper_trades table not found');
      error.statusCode = 404;
      throw error;
    }

    const columns = getTableColumns(paperDb, 'paper_trades');
    const openRows = paperDb.prepare(
      `SELECT id, symbol, strategy_stage FROM paper_trades WHERE exit_reason IS NULL ORDER BY id ASC`
    ).all();

    if (!openRows.length) {
      return {
        dbPath: paperDbPath,
        updated: 0,
        reason,
        pnlPct,
        openBefore: 0,
        symbols: [],
      };
    }

    const assignments = [
      `exit_price = COALESCE(exit_price, 0)`,
      `exit_ts = ?`,
      `exit_reason = ?`,
      `pnl_pct = ?`,
    ];
    const includeStageOutcome = columns.has('stage_outcome');
    if (includeStageOutcome) assignments.push(`stage_outcome = ?`);
    if (columns.has('trailing_active')) assignments.push(`trailing_active = 0`);
    if (columns.has('exit_execution_json')) assignments.push(`exit_execution_json = NULL`);
    if (columns.has('exit_quote_failures')) assignments.push(`exit_quote_failures = 0`);
    if (columns.has('last_exit_quote_failure')) assignments.push(`last_exit_quote_failure = NULL`);

    const exitTs = Math.floor(Date.now() / 1000);
    const updateStmt = paperDb.prepare(`
      UPDATE paper_trades
      SET ${assignments.join(',\n          ')}
      WHERE id = ?
    `);

    const updateTxn = paperDb.transaction((rows) => {
      for (const row of rows) {
        const stage = row.strategy_stage || 'stage1';
        const params = [exitTs, reason, pnlPct];
        if (includeStageOutcome) {
          params.push(`${stage}_${reason}`);
        }
        params.push(row.id);
        updateStmt.run(...params);
      }
    });
    updateTxn(openRows);

    return {
      dbPath: paperDbPath,
      updated: openRows.length,
      reason,
      pnlPct,
      openBefore: openRows.length,
      symbols: openRows.slice(0, 20).map(row => row.symbol || `id:${row.id}`),
    };
  } finally {
    paperDb.close();
  }
}

/**
 * HTML 模板
 */
function renderDashboard(data) {
  return `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Sentiment Arbitrage Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { 
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
      color: #e4e4e4;
      min-height: 100vh;
      padding: 20px;
    }
    .container { max-width: 1400px; margin: 0 auto; }
    h1 { 
      text-align: center; 
      margin-bottom: 30px; 
      color: #00d9ff;
      font-size: 2.5em;
      text-shadow: 0 0 20px rgba(0, 217, 255, 0.3);
    }
    .grid { 
      display: grid; 
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
      gap: 20px; 
      margin-bottom: 30px;
    }
    .card {
      background: rgba(255, 255, 255, 0.05);
      border-radius: 15px;
      padding: 20px;
      border: 1px solid rgba(255, 255, 255, 0.1);
      backdrop-filter: blur(10px);
    }
    .card h2 {
      color: #00d9ff;
      margin-bottom: 15px;
      font-size: 1.2em;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .stat-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 15px;
    }
    .stat {
      background: rgba(0, 0, 0, 0.2);
      padding: 15px;
      border-radius: 10px;
      text-align: center;
    }
    .stat-value {
      font-size: 2em;
      font-weight: bold;
      color: #00ff88;
    }
    .stat-value.negative { color: #ff4757; }
    .stat-value.neutral { color: #ffa502; }
    .stat-label { color: #888; font-size: 0.9em; margin-top: 5px; }
    
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }
    th, td {
      padding: 12px 8px;
      text-align: left;
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    th { color: #00d9ff; font-weight: 600; }
    tr:hover { background: rgba(255, 255, 255, 0.05); }
    
    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 4px;
      font-size: 0.8em;
      font-weight: 600;
    }
    .badge-green { background: rgba(0, 255, 136, 0.2); color: #00ff88; }
    .badge-yellow { background: rgba(255, 165, 2, 0.2); color: #ffa502; }
    .badge-red { background: rgba(255, 71, 87, 0.2); color: #ff4757; }
    
    .exit-strategy {
      background: rgba(0, 217, 255, 0.1);
      border-radius: 10px;
      padding: 15px;
      margin-top: 10px;
    }
    .exit-strategy h3 { color: #00d9ff; margin-bottom: 10px; }
    .exit-rule {
      display: flex;
      justify-content: space-between;
      padding: 8px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .exit-rule:last-child { border-bottom: none; }
    
    .refresh-info {
      text-align: center;
      color: #666;
      margin-top: 20px;
      font-size: 0.9em;
    }
    
    .pnl-positive { color: #00ff88; }
    .pnl-negative { color: #ff4757; }
    
    .token-address {
      font-family: monospace;
      font-size: 0.85em;
      color: #888;
    }
  </style>
  <!-- 移除 meta refresh，改用 AJAX 轮询 -->
  <script>
    const _token=new URLSearchParams(window.location.search).get('token')||'';
    const _q=_token?'?token='+encodeURIComponent(_token):'';
    const _a=_token?'&token='+encodeURIComponent(_token):'';
    // SOL 余额轮询（每 30 秒，不刷页面）
    async function refreshSolBalance() {
      try {
        const r = await fetch('/api/wallet-balance'+_q);
        const d = await r.json();
        const el = document.getElementById('stat-sol-balance');
        if (el) {
          if (d.balance !== null && d.balance !== undefined) {
            el.textContent = d.balance.toFixed(4);
            el.style.color = d.balance < 0.5 ? '#ff4757' : d.balance < 1 ? '#ffa502' : '#00ff88';
          } else {
            el.textContent = '未知';
          }
        }
      } catch (e) { /* ignore */ }
    }

    // 自动刷新页面 (保持表格数据最新)
    document.addEventListener('DOMContentLoaded', () => {
      refreshSolBalance();
      setInterval(refreshSolBalance, 30000);
      // 每 60 秒刷新一次整页
      setInterval(() => {
        location.reload();
      }, 60000);
    });

    // 立即手动刷新
    function manualRefresh() {
      location.reload();
    }

    // 暂停/恢复交易
    async function toggleTrading(action) {
      if (action === 'pause' && !confirm('确认暂停交易？将暂停4小时。')) return;
      if (action === 'resume' && !confirm('确认恢复交易？连亏计数将重置。')) return;
      try {
        const endpoint = action === 'pause' ? '/api/pause-trading' : '/api/resume-trading';
        const r = await fetch(endpoint + _q, { method: 'POST' });
        const d = await r.json();
        if (d.success) {
          alert(d.message);
          location.reload();
        } else {
          alert('操作失败: ' + (d.error || '未知错误'));
        }
      } catch (e) {
        alert('请求失败: ' + e.message);
      }
    }

    async function resetDailyLoss() {
      if (!confirm('确认重置今日亏损统计起点？\n历史数据不会被删除，仅从当前时间重新开始计算。')) return;
      try {
        const r = await fetch('/api/reset-daily-loss' + _q, { method: 'POST' });
        const d = await r.json();
        if (d.success) {
          alert(d.message);
          location.reload();
        } else {
          alert('操作失败: ' + (d.error || '未知错误'));
        }
      } catch (e) {
        alert('请求失败: ' + e.message);
      }
    }
  </script>
</head>
<body>
  <div class="container">
    <h1>🤖 Sentiment Arbitrage Dashboard <button onclick="manualRefresh()" style="font-size:0.5em;padding:5px 15px;cursor:pointer;background:#00d9ff;border:none;border-radius:5px;color:#000;">🔄 更新</button> <button onclick="location.reload()" style="font-size:0.5em;padding:5px 15px;cursor:pointer;background:#666;border:none;border-radius:5px;color:#fff;margin-left:5px;">↻ 整页刷新</button></h1>
    
    <!-- 系统概览 -->
    <div class="grid">
      <div class="card">
        <h2>📊 系统状态</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value" id="stat-mode">${data.overview.mode}</div>
            <div class="stat-label">运行模式</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-channels">${data.overview.channels}</div>
            <div class="stat-label">监控频道</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-signals">${data.overview.signals_today}</div>
            <div class="stat-label">今日信号</div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-positions">${data.overview.positions_open}</div>
            <div class="stat-label">持仓数量</div>
          </div>
          <div class="stat">
            <div class="stat-value ${(data.risk.daily_pnl_sol || 0) >= 0 ? '' : ((data.risk.daily_pnl_sol || 0) <= -0.5 ? 'negative' : 'neutral')}" id="stat-pnl">
              ${(data.risk.daily_pnl_sol || 0) >= 0 ? '+' : ''}${(data.risk.daily_pnl_sol || 0).toFixed(2)}
            </div>
            <div class="stat-label">今日 SOL 盈亏</div>
          </div>
          <div class="stat">
            <div class="stat-value ${data.risk.is_paused ? 'negative' : ''}" id="stat-trading-status">
              ${data.risk.is_paused ? '已暂停' : '正常'}
            </div>
            <div class="stat-label">风控状态</div>
            <div style="margin-top:8px;display:flex;gap:6px;justify-content:center;flex-wrap:wrap">
              <button id="btn-pause" onclick="toggleTrading('pause')" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#ff4757;border:none;border-radius:4px;color:#fff;${data.risk.is_paused ? 'display:none' : ''}">⏸ 暂停</button>
              <button id="btn-resume" onclick="toggleTrading('resume')" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#2ed573;border:none;border-radius:4px;color:#fff;${data.risk.is_paused ? '' : 'display:none'}">▶ 恢复</button>
              <button onclick="resetDailyLoss()" style="font-size:0.75em;padding:4px 10px;cursor:pointer;background:#ffa502;border:none;border-radius:4px;color:#fff;" title="重置今日亏损统计起点，不删除历史数据">🔄 重置今日亏损</button>
            </div>
          </div>
          <div class="stat">
            <div class="stat-value" id="stat-sol-balance">--</div>
            <div class="stat-label">钱包余额 (SOL)</div>
          </div>
        </div>
      </div>
      
      <div class="card">
        <h2>💰 虚拟收益统计</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value ${(data.performance.total_pnl || 0) >= 0 ? '' : 'negative'}">${(data.performance.total_pnl || 0) >= 0 ? '+' : ''}${(data.performance.total_pnl || 0).toFixed(1)}%</div>

            <div class="stat-label">总收益率</div>
          </div>
          <div class="stat">
            <div class="stat-value ${data.performance.win_rate >= 50 ? '' : 'neutral'}">${data.performance.win_rate.toFixed(1)}%</div>
            <div class="stat-label">胜率</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.performance.total_trades}</div>
            <div class="stat-label">总交易数</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.performance.avg_pnl >= 0 ? '+' : ''}${data.performance.avg_pnl.toFixed(2)}%</div>
            <div class="stat-label">平均收益</div>
          </div>
        </div>
      </div>

      <!-- 观察池概览 -->
      <div class="card">
        <h2>🔭 三级观察池 (Waiting Room)</h2>
        <div class="stat-grid">
          <div class="stat">
            <div class="stat-value" style="color: #ffda44;">${data.observationPool.counts.gold}</div>
            <div class="stat-label">🥇 金池 (待毕业)</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="color: #c0c0c0;">${data.observationPool.counts.silver}</div>
            <div class="stat-label">🥈 银池 (观察中)</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="color: #cd7f32;">${data.observationPool.counts.bronze}</div>
            <div class="stat-label">🥉 铜池 (海选)</div>
          </div>
          <div class="stat">
            <div class="stat-value">${data.observationPool.counts.total}</div>
            <div class="stat-label">当前总数</div>
          </div>
        </div>
        <div style="margin-top: 15px; font-size: 0.85em; color: #888; text-align: center;">
          🥇金池：5min | 🥈银池：8min | 🥉铜池：10min
        </div>
      </div>
    </div>

    <!-- ==================== v7.4 新增模块状态卡片 ==================== -->
    <div class="grid" style="margin-bottom: 20px;">
      <!-- Hunter Performance -->
      <div class="card">
        <h2>🎯 猎人表现 (Hunter Performance)</h2>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>类型</th>
              <th>交易数</th>
              <th>胜率</th>
              <th>平均收益</th>
              <th>仓位倍数</th>
            </tr>
          </thead>
          <tbody>
            ${Object.entries(data.hunterPerformance).filter(([type]) => type !== 'UNKNOWN' || data.hunterPerformance[type].trades > 0).map(([type, stats]) => {
    const emoji = type === 'FOX' ? '🦊' : type === 'TURTLE' ? '🐢' : type === 'WOLF' ? '🐺' : '❓';
    const winRate = stats.trades > 0 ? (stats.wins / stats.trades * 100) : 0;
    return `
                <tr>
                  <td>${emoji} ${type}</td>
                  <td>${stats.trades}</td>
                  <td><span class="badge ${winRate >= 50 ? 'badge-green' : winRate >= 30 ? 'badge-yellow' : 'badge-red'}">${winRate.toFixed(1)}%</span></td>
                  <td class="${stats.avgPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">${stats.avgPnl >= 0 ? '+' : ''}${stats.avgPnl.toFixed(2)}%</td>
                  <td><strong>${stats.multiplier}x</strong></td>
                </tr>
              `;
  }).join('')}
            ${Object.values(data.hunterPerformance).every(s => s.trades === 0) ? '<tr><td colspan="5" style="text-align:center;color:#666;">等待猎人信号数据...</td></tr>' : ''}
          </tbody>
        </table>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          FOX=金狗猎人 | TURTLE=波段玩家 | WOLF=稳定盈利
        </div>
      </div>

      <!-- Signal Source Distribution -->
      <div class="card">
        <h2>📡 信号来源分布 (v7.4 Lineage)</h2>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>来源</th>
              <th>信号数</th>
              <th>胜率</th>
              <th>平均收益</th>
            </tr>
          </thead>
          <tbody>
            ${Object.entries(data.signalSources).filter(([_, stats]) => stats.count > 0).map(([source, stats]) => {
    const emoji = source === 'ultra_sniper_v2' ? '🎯' : source === 'shadow_v2' ? '🥷' : source === 'flash_scout' ? '⚡' : '🔭';
    const displayName = source === 'ultra_sniper_v2' ? 'Ultra Sniper V2' :
      source === 'shadow_v2' ? 'Shadow Protocol V2' :
        source === 'flash_scout' ? 'Flash Scout' : 'Tiered Observer';
    return `
                <tr>
                  <td>${emoji} ${displayName}</td>
                  <td>${stats.count}</td>
                  <td><span class="badge ${stats.winRate >= 50 ? 'badge-green' : stats.winRate >= 30 ? 'badge-yellow' : 'badge-red'}">${stats.winRate.toFixed(1)}%</span></td>
                  <td class="${stats.avgPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">${stats.avgPnl >= 0 ? '+' : ''}${stats.avgPnl.toFixed(2)}%</td>
                </tr>
              `;
  }).join('')}
            ${Object.values(data.signalSources).every(s => s.count === 0) ? '<tr><td colspan="4" style="text-align:center;color:#666;">等待信号来源数据...</td></tr>' : ''}
          </tbody>
        </table>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          v7.4 信号血统追踪 | 数据随交易积累
        </div>
      </div>

      <!-- API Gateway 健康状态 -->
      <div class="card">
        <h2>🛡️ API 网关健康 (v7.4.1)</h2>
        <div class="stat-grid" style="grid-template-columns: repeat(3, 1fr);">
          <div class="stat">
            <div class="stat-value" style="font-size:1.5em;">${data.apiHealth.gmgn.circuitBreaker ? '🔴 熔断中' : '🟢 正常'}</div>
            <div class="stat-label">GMGN Gateway</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="font-size:1.2em;">${data.apiHealth.gmgn.requestsToday || 0}</div>
            <div class="stat-label">今日请求数</div>
          </div>
          <div class="stat">
            <div class="stat-value" style="font-size:1.2em;">${data.apiHealth.gmgn.rateLimited || 0}</div>
            <div class="stat-label">限流次数</div>
          </div>
        </div>
        <div style="margin-top: 10px; font-size: 0.8em; color: #888;">
          令牌桶: 10/s | 熔断阈值: 5次失败 | 冷却: 60秒
        </div>
      </div>
    </div>

    <!-- 观察池详情列表 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🔬 实时观察队列 (实时动态更新)</h2>
      <table style="font-size: 0.9em;">
        <thead>
          <tr>
            <th>池级</th>
            <th>代币</th>
            <th>链</th>
            <th>分数</th>
            <th>观察时长</th>
            <th>聪明钱 (初→现)</th>
            <th>价格变化</th>
            <th>特征标签</th>
          </tr>
        </thead>
        <tbody>
          ${data.observationPool.tokens.map(t => `
            <tr>
              <td>
                <span class="badge ${t.tier === 'GOLD' ? 'badge-green' : t.tier === 'SILVER' ? 'badge-yellow' : ''}" 
                      style="${t.tier === 'BRONZE' ? 'background:rgba(205,127,50,0.2);color:#cd7f32;' : ''}">
                  ${t.tier === 'GOLD' ? '🥇 GOLD' : t.tier === 'SILVER' ? '🥈 SILVER' : '🥉 BRONZE'}
                </span>
              </td>
              <td><strong>${t.symbol}</strong></td>
              <td><span class="badge ${t.chain === 'SOL' ? 'badge-green' : 'badge-yellow'}">${t.chain}</span></td>
              <td><strong>${t.score}</strong></td>
              <td>${t.observeMinutes} min</td>
              <td>${t.smInitial} → ${t.smCurrent} (${(t.smCurrent - t.smInitial) >= 0 ? '+' : ''}${t.smCurrent - t.smInitial})</td>
              <td class="${parseFloat(t.priceChange) >= 0 ? 'pnl-positive' : 'pnl-negative'}">${parseFloat(t.priceChange) >= 0 ? '+' : ''}${t.priceChange}%</td>
              <td><span class="badge ${t.tag === 'GOLDEN' ? 'badge-green' : 'badge-yellow'}">${t.tag}</span></td>
            </tr>
          `).join('')}
          ${data.observationPool.tokens.length === 0 ? '<tr><td colspan="8" style="text-align:center;color:#666;">观察池当前为空，寻找信号中...</td></tr>' : ''}
        </tbody>
      </table>
    </div>
    
    <!-- 信号源排名 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🏆 信号源排名 (按胜率)</h2>
      <table>
        <thead>
          <tr>
            <th>排名</th>
            <th>信号源</th>
            <th>信号数</th>
            <th>胜率</th>
            <th>平均收益</th>
            <th>最佳</th>
            <th>最差</th>
          </tr>
        </thead>
        <tbody>
          ${data.sources.map((s, i) => `
            <tr>
              <td>${i + 1}</td>
              <td>${s.source_id || 'Unknown'}</td>
              <td>${s.total_signals}</td>
              <td><span class="badge ${(s.win_rate || 0) >= 50 ? 'badge-green' : (s.win_rate || 0) >= 30 ? 'badge-yellow' : 'badge-red'}">${(s.win_rate || 0).toFixed(1)}%</span></td>
              <td class="${(s.avg_pnl || 0) >= 0 ? 'pnl-positive' : 'pnl-negative'}">${(s.avg_pnl || 0) >= 0 ? '+' : ''}${(s.avg_pnl || 0).toFixed(2)}%</td>
              <td class="pnl-positive">+${(s.best_pnl || 0).toFixed(1)}%</td>
              <td class="pnl-negative">${(s.worst_pnl || 0).toFixed(1)}%</td>
            </tr>
          `).join('')}
          ${data.sources.length === 0 ? '<tr><td colspan="7" style="text-align:center;color:#666;">暂无数据，系统运行中...</td></tr>' : ''}
        </tbody>
      </table>
    </div>
    
    <!-- 虚拟仓位 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>📈 虚拟仓位表现</h2>
      <table>
        <thead>
          <tr>
            <th>代币</th>
            <th>链</th>
            <th>实时PnL</th>
            <th>止盈策略</th>
            <th>买入</th>
            <th>已卖出</th>
            <th>入场价</th>
            <th>收益率</th>
            <th>剩余</th>
            <th>状态</th>
            <th>入场时间</th>
            <th>退出时间</th>
            <th>持仓时长</th>
          </tr>
        </thead>

        <tbody>
          ${data.positions.map(p => {
    // v8.0 修复：对于 partial 状态，显示 last_partial_sell_price
    const isPartial = p.status === 'partial';
    const isBreakeven = isPartial && p.breakeven_done === 1;

    // 卖出价格：partial 用 last_partial_sell_price，closed 用 exit_price
    const sellPrice = isPartial ? p.last_partial_sell_price : (p.exit_price || null);
    const exitPrice = sellPrice || p.breakeven_price || p.price_15m || p.price_5m || null;

    // PnL 计算：使用卖出时的价格
    let displayPnl = p.pnl_percent || 0;
    if (isPartial && p.last_partial_sell_price && p.entry_price) {
      // partial 状态：显示卖出时的收益率
      displayPnl = ((p.last_partial_sell_price - p.entry_price) / p.entry_price * 100);
    } else if (isBreakeven && p.breakeven_price && p.entry_price) {
      displayPnl = ((p.breakeven_price - p.entry_price) / p.entry_price * 100);
    }

    const remainingPercent = p.remaining_percent != null ? p.remaining_percent : 100;
    // 买入金额
    const buyAmount = p.position_size_native ? p.position_size_native.toFixed(3) : '-';
    const buyUnit = p.chain === 'SOL' ? 'SOL' : 'BNB';
    const buyUsd = p.position_size_usd ? ('$' + p.position_size_usd.toFixed(0)) : '';

    // v8.0 修复：已卖出金额 = 入场成本 * 卖出% * (1 + 收益率)
    const soldPercent = 100 - remainingPercent;
    const soldMultiplier = displayPnl > 0 ? (1 + displayPnl / 100) : 1;
    const soldAmount = p.position_size_native && soldPercent > 0
      ? (p.position_size_native * soldPercent / 100 * soldMultiplier).toFixed(3)
      : '-';
    const soldUsd = p.position_size_usd && soldPercent > 0
      ? ('$' + (p.position_size_usd * soldPercent / 100 * soldMultiplier).toFixed(0))
      : '';
    // 数据库存的是 UTC 时间，需要加 'Z' 后缀才能正确转换为本地时间
    const parseUTC = (t) => {
      if (!t) return null;
      if (typeof t === 'number') return new Date(t * 1000);
      const str = String(t);
      return new Date(str.includes('Z') || str.includes('+') ? str : str + 'Z');
    };
    const entryTime = p.entry_time ? parseUTC(p.entry_time).toLocaleString('zh-CN') : '-';
    // v9.3: 退出时间和持仓时长
    const exitTime = p.exit_time ? parseUTC(p.exit_time).toLocaleString('zh-CN') : '-';
    const entryDate = p.entry_time ? parseUTC(p.entry_time) : null;
    const exitDate = p.exit_time ? parseUTC(p.exit_time) : new Date();
    let holdDuration = '-';
    if (entryDate) {
      const mins = Math.round((exitDate - entryDate) / 60000);
      if (mins >= 60) {
        holdDuration = Math.floor(mins / 60) + 'h' + (mins % 60) + 'm';
      } else {
        holdDuration = mins + 'min';
      }
    }
    // 状态显示
    let statusText = p.status || '-';
    let statusClass = 'badge-yellow';
    if (isBreakeven) {
      // 已完成翻倍出本
      statusText = '💰已出本';
      statusClass = 'badge-green';
    } else if (isPartial) {
      // partial 但未完成翻倍出本（中途止盈）
      statusText = 'partial';
      statusClass = displayPnl >= 0 ? 'badge-green' : 'badge-yellow';
    } else if (p.status === 'closed') {
      statusClass = displayPnl >= 0 ? 'badge-green' : 'badge-red';
    } else if (p.status === 'open') {
      statusText = 'open';
    }
    // v7.4 猎人类型
    const hunterType = p.signal_hunter_type || '-';
    const hunterEmoji = hunterType === 'FOX' ? '🦊' : hunterType === 'TURTLE' ? '🐢' : hunterType === 'WOLF' ? '🐺' : '';
    // v7.4 信号来源
    const signalSource = p.signal_source || p.entry_source || '-';
    const sourceShort = signalSource.replace('ultra_sniper_v2', 'Ultra').replace('shadow_v2', 'Shadow').replace('flash_scout', 'Flash').replace('tiered_observer', 'Observer');

    // v8.0 实时 PnL 和止盈策略
    const livePnl = p.current_pnl != null ? p.current_pnl : 0;
    const tierStrategy = p.tier_strategy || '';
    // 提取止盈策略的简短显示 (例如从 "等待止盈 (TIER_A: 当前 +10.7%, 目标 +150%, 持仓 26min)" 提取 "TIER_A +150%")
    const tierMatch = tierStrategy.match(/\((TIER_[SsAaBbCc]|默认|DEFAULT)[^)]*目标 \+?(\d+)%/i);
    const tierShort = tierMatch ? `${tierMatch[1].toUpperCase()} +${tierMatch[2]}%` : (p.status === 'open' ? '监控中' : '-');

    return `
            <tr>
              <td>
                <div><strong>${p.symbol || 'Unknown'}</strong></div>
                <div class="token-address" style="font-size:0.7em;word-break:break-all;">${(p.token_ca || 'N/A').slice(0, 12)}...</div>
              </td>
              <td><span class="badge ${p.chain === 'SOL' ? 'badge-green' : 'badge-yellow'}">${p.chain || '-'}</span></td>
              <td class="${livePnl >= 0 ? 'pnl-positive' : 'pnl-negative'}" style="font-weight:bold;">
                ${p.status === 'open' ? ((livePnl >= 0 ? '+' : '') + livePnl.toFixed(1) + '%') : '-'}
              </td>
              <td style="font-size:0.8em;">
                <span class="badge ${tierShort.includes('TIER_S') ? 'badge-green' : tierShort.includes('TIER_A') ? 'badge-green' : 'badge-yellow'}">${tierShort}</span>
              </td>
              <td style="white-space:nowrap;">
                <div>${buyAmount} ${buyUnit}</div>
                <div style="font-size:0.8em;color:#888;">${buyUsd}</div>
              </td>
              <td style="white-space:nowrap;">
                ${soldPercent > 0 ? `<div class="pnl-positive">${soldAmount} ${buyUnit}</div><div style="font-size:0.8em;color:#888;">${soldUsd}</div>` : '-'}
              </td>
              <td>$${p.entry_price ? p.entry_price.toFixed(10) : 'N/A'}</td>
              <td class="${displayPnl >= 0 ? 'pnl-positive' : 'pnl-negative'}">
                ${displayPnl !== 0 ? ((displayPnl >= 0 ? '+' : '') + displayPnl.toFixed(1) + '%') : '-'}
              </td>
              <td>${remainingPercent < 100 ? (remainingPercent.toFixed(0) + '%') : '100%'}</td>
              <td><span class="badge ${statusClass}">${statusText}</span></td>
              <td style="font-size:0.85em;">${entryTime}</td>
              <td style="font-size:0.85em;">${exitTime}</td>
              <td style="font-size:0.85em;">${holdDuration}</td>
            </tr>
          `}).join('')}
          ${data.positions.length === 0 ? '<tr><td colspan="13" style="text-align:center;color:#666;">暂无仓位数据，等待 DeBot 信号通过验证...</td></tr>' : ''}

        </tbody>
      </table>
    </div>
    
    <!-- v18 最近信号记录 -->
    <div class="card" style="margin-bottom: 20px;">
      <h2>🔥 最近信号 (v18 指标过滤)</h2>
      <table>
        <thead>
          <tr>
            <th>代币</th>
            <th>MC</th>
            <th>Super</th>
            <th>SupΔ</th>
            <th>Trade</th>
            <th>结果</th>
          </tr>
        </thead>
        <tbody>
          ${data.recent_scores.map(s => `
            <tr>
              <td>${s.symbol}</td>
              <td>${s.mc}</td>
              <td>${s.superCurrent}</td>
              <td>${s.superDelta}</td>
              <td>${s.tradeCurrent}</td>
              <td><span class="badge ${s.passed ? 'badge-green' : 'badge-red'}">${s.status}</span></td>
            </tr>
          `).join('')}
          ${data.recent_scores.length === 0 ? '<tr><td colspan="6" style="text-align:center;color:#666;">等待新信号...</td></tr>' : ''}
        </tbody>
      </table>
    </div>

    <!-- v18 出场策略 ASYMMETRIC -->
    <div class="card">
      <h2>⚙️ 策略 v18：非对称收割 (ASYMMETRIC)</h2>
      <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="exit-strategy">
          <h3>🛑 止损</h3>
          <div class="exit-rule"><span>止损线</span><span class="pnl-negative">${data.config.exitStrategy.stopLoss}</span></div>
          <div class="exit-rule"><span>死水超时</span><span class="pnl-negative">${data.config.exitStrategy.deadWater}</span></div>
          <div class="exit-rule"><span>最大持仓</span><span class="pnl-negative">${data.config.exitStrategy.maxHold}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>💰 分批止盈</h3>
          <div class="exit-rule"><span>TP1</span><span class="pnl-positive">${data.config.exitStrategy.tp1}</span></div>
          <div class="exit-rule"><span>TP2</span><span class="pnl-positive">${data.config.exitStrategy.tp2}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🚀 高倍止盈</h3>
          <div class="exit-rule"><span>TP3</span><span class="pnl-positive">${data.config.exitStrategy.tp3}</span></div>
          <div class="exit-rule"><span>TP4</span><span class="pnl-positive">${data.config.exitStrategy.tp4}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>📊 仓位</h3>
          <div class="exit-rule"><span>单笔仓位</span><span class="pnl-positive">${data.config.position.sizeSol} SOL</span></div>
          <div class="exit-rule"><span>在险上限</span><span>${data.config.position.maxAtRisk} 个</span></div>
          <div class="exit-rule"><span>Moonbag</span><span>不占槽位</span></div>
        </div>
      </div>
    </div>
    
    <!-- v18 入场过滤条件 -->
    <div class="card" style="margin-top: 20px;">
      <h2>🎯 v18 入场过滤条件</h2>
      <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
        <div class="exit-strategy">
          <h3>📊 市值 & ATH</h3>
          <div class="exit-rule"><span>Market Cap</span><span>${data.config.entryFilters.marketCap}</span></div>
          <div class="exit-rule"><span>ATH</span><span>${data.config.entryFilters.athOnly}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🔥 Super Index</h3>
          <div class="exit-rule"><span>Super_cur</span><span>${data.config.entryFilters.superIndex}</span></div>
          <div class="exit-rule"><span>SupΔ</span><span>${data.config.entryFilters.superDelta}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>📈 Trade & Address</h3>
          <div class="exit-rule"><span>Trade_cur</span><span>${data.config.entryFilters.tradeCurrent}</span></div>
          <div class="exit-rule"><span>TΔ</span><span>${data.config.entryFilters.tradeDelta}</span></div>
          <div class="exit-rule"><span>Addr_cur</span><span>${data.config.entryFilters.addressCurrent}</span></div>
        </div>
        <div class="exit-strategy">
          <h3>🛡️ 安全 & 防追高</h3>
          <div class="exit-rule"><span>Sec_cur</span><span>${data.config.entryFilters.securityCurrent}</span></div>
          <div class="exit-rule"><span>防追高</span><span>实时MC &gt; 信号MC×1.2 拒绝</span></div>
          <div class="exit-rule"><span>Freeze/Mint</span><span>必须 DISABLED</span></div>
        </div>
      </div>
    </div>
    
    <!-- AI 复盘与策略调整 -->
    <div class="card" style="margin-top: 20px;">
      <h2>🤖 AI 复盘与策略调整</h2>
      <div class="grid" style="grid-template-columns: 1fr 2fr;">
        <div class="stat" style="text-align: left; padding: 15px;">
          <h3 style="color: #00d9ff; margin-bottom: 10px;">⚡ 当前状态</h3>
          <div style="margin-bottom: 8px;">交易状态: <span class="badge ${data.risk.is_paused ? 'badge-red' : 'badge-green'}">${data.risk.is_paused ? '已暂停' : '运行中'}</span></div>
          <div style="margin-bottom: 8px;">今日SOL盈亏: <span class="${data.risk.daily_pnl_sol >= 0 ? 'pnl-positive' : 'pnl-negative'}">${data.risk.daily_pnl_sol >= 0 ? '+' : ''}${(data.risk.daily_pnl_sol || 0).toFixed(4)} SOL</span></div>
          <div>今日BNB盈亏: <span class="${data.risk.daily_pnl_bnb >= 0 ? 'pnl-positive' : 'pnl-negative'}">${data.risk.daily_pnl_bnb >= 0 ? '+' : ''}${(data.risk.daily_pnl_bnb || 0).toFixed(4)} BNB</span></div>
        </div>
        <div>
          <h3 style="color: #00d9ff; margin-bottom: 10px;">📊 动态阈值 (AI 自动调整)</h3>
          <table style="font-size: 0.9em;">
            <thead>
              <tr>
                <th>参数</th>
                <th>当前值</th>
                <th>更新时间</th>
                <th>更新者</th>
              </tr>
            </thead>
            <tbody>
              ${data.thresholds.map(t => `
                <tr>
                  <td><code>${t.key}</code></td>
                  <td><strong>${t.value}</strong></td>
                  <td style="font-size:0.85em;">${t.updated_at || '-'}</td>
                  <td><span class="badge ${t.updated_by === 'AI_AUTO_REVIEW' ? 'badge-green' : 'badge-yellow'}">${t.updated_by}</span></td>
                </tr>
              `).join('')}
              ${data.thresholds.length === 0 ? '<tr><td colspan="4" style="text-align:center;color:#666;">暂无阈值数据</td></tr>' : ''}
            </tbody>
          </table>
        </div>
      </div>
      
      <!-- AI 复盘历史记录 -->
      <div style="margin-top: 20px;">
        <h3 style="color: #00d9ff; margin-bottom: 10px;">📜 AI 复盘历史</h3>
        <table style="font-size: 0.9em;">
          <thead>
            <tr>
              <th>时间</th>
              <th>触发原因</th>
              <th>交易数</th>
              <th>胜率</th>
              <th>关键洞察</th>
              <th>优先行动</th>
            </tr>
          </thead>
          <tbody>
            ${data.reviewHistory.map(r => `
              <tr>
                <td style="font-size:0.85em;">${r.review_time || '-'}</td>
                <td><span class="badge ${r.trigger_reason === 'consecutive_losses' ? 'badge-red' : 'badge-yellow'}">${r.trigger_reason === 'consecutive_losses' ? '连续亏损' : r.trigger_reason}</span></td>
                <td>${r.trade_count || 0}</td>
                <td><span class="badge ${r.win_rate >= 50 ? 'badge-green' : 'badge-red'}">${(r.win_rate || 0).toFixed(1)}%</span></td>
                <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.ai_key_insight || ''}">${r.ai_key_insight || '-'}</td>
                <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${r.priority_action || ''}">${r.priority_action || '-'}</td>
              </tr>
            `).join('')}
            ${data.reviewHistory.length === 0 ? '<tr><td colspan="6" style="text-align:center;color:#666;">尚未进行过 AI 复盘</td></tr>' : ''}
          </tbody>
        </table>
      </div>
    </div>
    
    <div class="refresh-info">
      数据每30秒自动更新 (不刷新页面) | 最后更新: <span id="last-refresh-time">${new Date().toLocaleString('zh-CN')}</span>
    </div>
  </div>
</body>
</html>
`;
}

/**
 * v18 策略配置（与 premium-signal-engine.js 保持同步）
 * 不再使用 CrossValidator 评分，改用信号指标直接过滤
 */
const V18_STRATEGY_CONFIG = {
  // 入场条件（信号指标硬过滤）
  entryFilters: {
    marketCap: '30K - 300K',
    superIndex: '80 - 1000',
    superDelta: '≥ 5',
    tradeCurrent: '≥ 1',
    tradeDelta: '≥ 1',
    addressCurrent: '≥ 3',
    securityCurrent: '≥ 15',
    athOnly: 'ATH#1 直接入场'
  },
  // 仓位
  position: {
    sizeSol: parseFloat(process.env.PREMIUM_POSITION_SOL || '0.06'),
    maxAtRisk: 5,
    moonbagNotCounted: true
  },
  // 出场策略 ASYMMETRIC
  exitStrategy: {
    tp1: '+45-60%区间峰值 → 卖60% (SL移至0%)',
    tp2: '+100% → 卖50%剩余',
    tp3: '+200% → 卖50%剩余',
    tp4: '+500% → 卖80%剩余 → Moonbag',
    stopLoss: '-35%',
    deadWater: '15分钟无波动',
    maxHold: '30分钟'
  }
};

/**
 * 获取仪表盘数据
 */
function getDashboardData() {
  const data = {
    overview: {
      mode: 'SHADOW',
      channels: 0,
      signals_today: 0,
      positions_open: 0
    },
    performance: {
      total_pnl: 0,
      win_rate: 0,
      total_trades: 0,
      avg_pnl: 0
    },
    sources: [],
    positions: [],
    recent_scores: [],
    risk: {
      daily_pnl_sol: 0,
      daily_pnl_bnb: 0,
      is_paused: false,
      consecutive_losses: 0
    },
    thresholds: [],
    reviewHistory: [],
    observationPool: { tokens: [], counts: { total: 0, gold: 0, silver: 0, bronze: 0 } },
    config: V18_STRATEGY_CONFIG,
    // v7.4 新增模块状态
    hunterPerformance: {
      FOX: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.2 },
      TURTLE: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.5 },
      WOLF: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.0 },
      UNKNOWN: { trades: 0, wins: 0, avgPnl: 0, multiplier: 1.0 }
    },
    signalSources: {
      ultra_sniper_v2: { count: 0, winRate: 0, avgPnl: 0 },
      shadow_v2: { count: 0, winRate: 0, avgPnl: 0 },
      flash_scout: { count: 0, winRate: 0, avgPnl: 0 },
      tiered_observer: { count: 0, winRate: 0, avgPnl: 0 }
    },
    apiHealth: {
      gmgn: { status: 'unknown', circuitBreaker: false, requestsToday: 0 },
      debot: { status: 'unknown', lastSuccess: null }
    },
    autonomy: {
      enabled: !!global.__autonomySidecar,
      status: global.__autonomySidecar?.getStatus?.() || null,
      leaderboard: getExperimentLeaderboard(),
      premiumEngine: global.__premiumEngine?.getStats?.() || null
    }
  };

  if (!db) return data;

  try {
    // 系统概览
    // 检查 telegram_channels 表是否存在
    const tcExists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='telegram_channels'`).get();
    if (tcExists) {
      const channels = db.prepare(`SELECT COUNT(*) as c FROM telegram_channels WHERE active = 1`).get();
      data.overview.channels = channels?.c || 0;
    } else {
      data.overview.channels = 0;
    }

    // 今日信号 = 今日买入的交易数（来自 DeBot/CrossValidator）
    const signalsToday = db.prepare(`
      SELECT COUNT(*) as c FROM positions 
      WHERE DATE(entry_time) = DATE('now')
    `).get();
    data.overview.signals_today = signalsToday?.c || 0;

    // v7.5 只统计 open 状态，partial 不占仓位（与 RiskManager 逻辑一致）
    const openPositions = db.prepare(`SELECT COUNT(*) as c FROM positions WHERE status = 'open'`).get();
    data.overview.positions_open = openPositions?.c || 0;

    // 虚拟收益统计（只用 positions 表 - DeBot 验证通过的交易）
    const perfStats = db.prepare(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
        AVG(pnl_percent) as avg_pnl,
        SUM(pnl_percent) as total_pnl
      FROM positions 
      WHERE status = 'closed'
    `).get();

    if (perfStats && perfStats.total > 0) {
      data.performance.total_trades = perfStats.total;
      data.performance.win_rate = (perfStats.wins / perfStats.total) * 100;
      data.performance.avg_pnl = perfStats.avg_pnl || 0;
      data.performance.total_pnl = perfStats.total_pnl || 0;
    }

    // 风险管理状态 (修复时区问题 + pnl_native 可能为空)
    // 当 pnl_native 为空时，使用 pnl_percent * position_size_native / 100 估算
    const dailyPnL = db.prepare(`
      SELECT 
        chain,
        SUM(COALESCE(pnl_native, pnl_percent * position_size_native / 100)) as total_pnl
      FROM positions 
      WHERE status = 'closed'
      AND exit_time >= datetime('now', '-11 hours', 'start of day', '+11 hours')
      GROUP BY chain
    `).all();
    for (const row of dailyPnL) {
      if (row.chain === 'SOL') data.risk.daily_pnl_sol = row.total_pnl;
      if (row.chain === 'BSC') data.risk.daily_pnl_bnb = row.total_pnl;
    }

    const pauseState = db.prepare(`
      SELECT value, expires_at FROM system_state WHERE key = 'trading_paused'
    `).get();
    if (pauseState && pauseState.expires_at > Date.now() / 1000) {
      data.risk.is_paused = true;
    }

    // 获取动态阈值配置
    try {
      const thresholds = db.prepare(`
        SELECT key, value, updated_at, updated_by 
        FROM dynamic_thresholds 
        ORDER BY updated_at DESC
      `).all();
      data.thresholds = thresholds || [];
    } catch (e) {
      console.log('Dashboard thresholds query error:', e.message);
    }

    // 获取 AI 复盘历史
    try {
      const reviews = db.prepare(`
        SELECT review_time, trigger_reason, trade_count, win_rate, 
               ai_key_insight, priority_action
        FROM ai_review_history 
        ORDER BY review_time DESC
        LIMIT 10
      `).all();
      data.reviewHistory = reviews || [];
    } catch (e) {
      console.log('Dashboard review history query error:', e.message);
    }

    // 获取观察池状态 (从 JSON 文件)
    try {
      const poolPath = join(projectRoot, 'data', 'observation_pool.json');
      if (fs.existsSync(poolPath)) {
        const poolJson = fs.readFileSync(poolPath, 'utf8');
        data.observationPool = JSON.parse(poolJson);
      }
    } catch (e) {
      console.log('Dashboard pool query error:', e.message);
    }

    // 信号源排名（v7.4 使用 signal_source 字段）
    try {
      const sources = db.prepare(`
        SELECT
          COALESCE(signal_source, entry_source, 'unknown') as source_id,
          COUNT(*) as total_signals,
          ROUND(AVG(pnl_percent), 2) as avg_pnl,
          ROUND(MAX(pnl_percent), 2) as best_pnl,
          ROUND(MIN(CASE WHEN pnl_percent < 0 THEN pnl_percent END), 2) as worst_pnl,
          ROUND(SUM(CASE WHEN pnl_percent > 0 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) as win_rate
        FROM positions
        WHERE status = 'closed'
        GROUP BY source_id
        ORDER BY win_rate DESC, total_signals DESC
        LIMIT 20
      `).all();
      data.sources = sources || [];
    } catch (e) {
      console.log('Dashboard ranking query error:', e.message);
    }

    // 虚拟仓位：只显示 DeBot 验证通过的交易（positions 表）
    const positions = db.prepare(`
      SELECT 
        p.*,
        CASE 
          WHEN p.status = 'open' THEN 
            ROUND((julianday('now') - julianday(p.entry_time)) * 24 * 60) || ' min'
          ELSE 
            ROUND((julianday(p.exit_time) - julianday(p.entry_time)) * 24 * 60) || ' min'
        END as hold_time
      FROM positions p
      ORDER BY 
        CASE 
          WHEN p.status = 'open' THEN 0 
          WHEN p.status = 'partial' THEN 1 
          ELSE 2 
        END,
        p.entry_time DESC
      LIMIT 100
    `).all();
    data.positions = positions || [];

    // 最近信号记录 — 从 premium_signals 表获取真实数据
    try {
      const recentSignals = database.prepare(`
        SELECT symbol, market_cap, hard_gate_status, signal_type, is_ath, parse_status, executed, gate_result, timestamp
        FROM premium_signals ORDER BY id DESC LIMIT 15
      `).all();
      data.recent_scores = recentSignals.map(s => {
        const gateResult = (() => {
          try { return s.gate_result ? JSON.parse(s.gate_result) : null; } catch { return null; }
        })();
        const mc = s.market_cap ? `$${(s.market_cap / 1000).toFixed(1)}K` : '?';
        const signalLabel = (s.signal_type || (s.is_ath ? 'ATH' : 'NEW_TRENDING') || '').toUpperCase();
        const passed = (gateResult?.status || '').toUpperCase() === 'PASS' || s.hard_gate_status === 'PASS';
        return {
          symbol: s.symbol || '?',
          mc,
          superCurrent: signalLabel === 'ATH' ? 'ATH' : 'NT',
          superDelta: s.parse_status || '-',
          tradeCurrent: signalLabel,
          passed,
          status: passed
            ? (s.executed ? 'BUY' : 'PASS')
            : (gateResult?.status || s.hard_gate_status || 'SKIP').replace('NOT_ATH_PREBUY_KLINE_', '')
        };
      });
    } catch (e) {
      data.recent_scores = [];
    }

    // ==================== v7.4 新增查询 ====================

    // 1. Hunter Performance (按猎人类型统计)
    try {
      const hunterStats = db.prepare(`
        SELECT
          COALESCE(signal_hunter_type, 'UNKNOWN') as hunter_type,
          COUNT(*) as trades,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_hunter_type
      `).all();

      for (const row of hunterStats) {
        const type = row.hunter_type || 'UNKNOWN';
        if (data.hunterPerformance[type]) {
          data.hunterPerformance[type].trades = row.trades;
          data.hunterPerformance[type].wins = row.wins;
          data.hunterPerformance[type].avgPnl = row.avg_pnl || 0;
        }
      }
    } catch (e) {
      console.log('Dashboard hunter performance query error:', e.message);
    }

    // 2. Signal Source Distribution (按信号来源统计)
    try {
      const sourceStats = db.prepare(`
        SELECT
          COALESCE(signal_source, 'tiered_observer') as source,
          COUNT(*) as count,
          SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
          AVG(pnl_percent) as avg_pnl
        FROM positions
        WHERE status = 'closed'
        GROUP BY signal_source
      `).all();

      for (const row of sourceStats) {
        const source = row.source || 'tiered_observer';
        if (data.signalSources[source]) {
          data.signalSources[source].count = row.count;
          data.signalSources[source].winRate = row.count > 0 ? (row.wins / row.count * 100) : 0;
          data.signalSources[source].avgPnl = row.avg_pnl || 0;
        }
      }
    } catch (e) {
      console.log('Dashboard signal source query error:', e.message);
    }

    // 3. API Gateway 健康状态 (从 v7.4.2 持久化文件读取)
    try {
      const gatewayStatsPath = join(projectRoot, 'data', 'gmgn_gateway_stats.json');
      if (fs.existsSync(gatewayStatsPath)) {
        const gatewayStats = JSON.parse(fs.readFileSync(gatewayStatsPath, 'utf8'));
        data.apiHealth.gmgn = {
          status: gatewayStats.circuitBreaker ? 'circuit_open' : 'ok',
          circuitBreaker: gatewayStats.circuitBreaker || false,
          requestsToday: gatewayStats.requestsToday || 0,
          rateLimited: gatewayStats.rateLimited || 0,
          lastUpdate: gatewayStats.timestamp
        };
      }
    } catch (e) {
      console.log('Dashboard gateway stats read error:', e.message);
    }

  } catch (error) {
    console.error('❌ Get dashboard data error:', error.message);
  }

  return data;
}

// ==================== v7.3 API 数据函数 ====================

/**
 * v7.3 获取模块健康数据
 */
function getModuleHealthData(windowDays = 7) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    modules: [],
    summary: {}
  };

  try {
    // 从 module_performance 表获取数据
    let modulePerf = [];
    try {
      modulePerf = db.prepare(`
        SELECT * FROM module_performance
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM module_performance)
        AND window_days = ?
      `).all(windowDays);
    } catch (e) {
      // 表可能不存在，尝试从 positions 表直接计算
    }

    // 如果没有 module_performance 数据，从 positions 表计算
    if (modulePerf.length === 0) {
      modulePerf = db.prepare(`
        SELECT
          entry_source as module_name,
          COUNT(*) as total_trades,
          SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) as win_count,
          SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
          AVG(exit_pnl_percent) as avg_pnl,
          SUM(exit_pnl_percent) as total_pnl,
          MAX(exit_pnl_percent) as best_pnl,
          MIN(exit_pnl_percent) as worst_pnl
        FROM positions
        WHERE status = 'closed'
        AND exit_time >= ?
        AND entry_source IS NOT NULL
        GROUP BY entry_source
      `).all(cutoff);
    }

    result.modules = modulePerf.map(m => ({
      name: m.module_name,
      trades: m.total_trades,
      winRate: m.win_rate?.toFixed(1) || 0,
      avgPnl: m.avg_pnl?.toFixed(1) || 0,
      totalPnl: m.total_pnl?.toFixed(0) || 0,
      status: (m.win_rate || 0) < 30 ? 'CRITICAL' :
        (m.win_rate || 0) < 40 ? 'WARNING' :
          (m.win_rate || 0) >= 50 ? 'EXCELLENT' : 'HEALTHY'
    }));

    // 计算总体统计
    const totalTrades = modulePerf.reduce((sum, m) => sum + (m.total_trades || 0), 0);
    const totalPnl = modulePerf.reduce((sum, m) => sum + (m.total_pnl || 0), 0);

    result.summary = {
      totalModules: modulePerf.length,
      totalTrades,
      avgPnl: totalTrades > 0 ? (totalPnl / totalTrades).toFixed(1) : 0,
      healthyCount: result.modules.filter(m => m.status === 'HEALTHY' || m.status === 'EXCELLENT').length,
      warningCount: result.modules.filter(m => m.status === 'WARNING').length,
      criticalCount: result.modules.filter(m => m.status === 'CRITICAL').length
    };

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取 AI 叙事有效性数据
 */
function getNarrativeEffectivenessData() {
  const result = {
    timestamp: new Date().toISOString(),
    effective: null,
    tiers: [],
    correlation: null,
    recommendation: null
  };

  try {
    // 使用 intention_tier 字段（实际字段名）
    const data = db.prepare(`
      SELECT
        intention_tier as ai_narrative_tier,
        AVG(exit_pnl_percent) as avg_pnl,
        COUNT(*) as trades,
        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
      FROM positions
      WHERE status = 'closed'
      AND intention_tier IS NOT NULL
      AND exit_time >= datetime('now', '-7 days')
      GROUP BY intention_tier
      HAVING COUNT(*) >= 3
      ORDER BY
        CASE intention_tier
          WHEN 'TIER_S' THEN 1
          WHEN 'TIER_A' THEN 2
          WHEN 'TIER_B' THEN 3
          WHEN 'TIER_C' THEN 4
          ELSE 5
        END
    `).all();

    result.tiers = data.map(d => ({
      tier: d.ai_narrative_tier,
      trades: d.trades,
      avgPnl: d.avg_pnl?.toFixed(1) || 0,
      winRate: d.win_rate?.toFixed(1) || 0
    }));

    if (data.length >= 2) {
      // 检查单调性
      const tierOrder = ['TIER_S', 'TIER_A', 'TIER_B', 'TIER_C'];
      const orderedData = tierOrder
        .map(t => data.find(d => d.ai_narrative_tier === t))
        .filter(Boolean);

      const orderedPnl = orderedData.map(d => d.avg_pnl || 0);

      let monotonic = true;
      for (let i = 1; i < orderedPnl.length; i++) {
        if (orderedPnl[i] >= orderedPnl[i - 1]) {
          monotonic = false;
          break;
        }
      }

      // 简化相关性计算
      const tierScore = { 'TIER_S': 4, 'TIER_A': 3, 'TIER_B': 2, 'TIER_C': 1 };
      const points = orderedData.map(d => ({
        x: tierScore[d.ai_narrative_tier],
        y: d.avg_pnl || 0
      }));

      const n = points.length;
      if (n >= 2) {
        const sumX = points.reduce((a, p) => a + p.x, 0);
        const sumY = points.reduce((a, p) => a + p.y, 0);
        const sumXY = points.reduce((a, p) => a + p.x * p.y, 0);
        const sumX2 = points.reduce((a, p) => a + p.x * p.x, 0);
        const sumY2 = points.reduce((a, p) => a + p.y * p.y, 0);

        const numerator = n * sumXY - sumX * sumY;
        const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        result.correlation = denominator !== 0 ? (numerator / denominator).toFixed(2) : 0;
      }

      result.effective = monotonic && parseFloat(result.correlation) > 0.3;
      result.recommendation = result.effective ?
        '叙事评分有效，保持使用' :
        '叙事评分效果不明显，考虑调整';
    }

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取 A/B 测试数据
 */
function getABTestData(windowDays = 14) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    groups: [],
    difference: null,
    significant: null
  };

  try {
    const groups = db.prepare(`
      SELECT
        experiment_group,
        COUNT(*) as trades,
        AVG(exit_pnl_percent) as avg_pnl,
        SUM(CASE WHEN exit_pnl_percent >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate,
        SUM(exit_pnl_percent) as total_pnl
      FROM positions
      WHERE status = 'closed'
      AND exit_time >= ?
      AND experiment_group IS NOT NULL
      GROUP BY experiment_group
    `).all(cutoff);

    result.groups = groups.map(g => ({
      name: g.experiment_group,
      trades: g.trades,
      avgPnl: g.avg_pnl?.toFixed(1) || 0,
      winRate: g.win_rate?.toFixed(1) || 0,
      totalPnl: g.total_pnl?.toFixed(0) || 0
    }));

    const control = groups.find(g => g.experiment_group === 'control');
    const treatment = groups.find(g => g.experiment_group === 'treatment');

    if (control && treatment) {
      result.difference = {
        avgPnl: ((treatment.avg_pnl || 0) - (control.avg_pnl || 0)).toFixed(1),
        winRate: ((treatment.win_rate || 0) - (control.win_rate || 0)).toFixed(1)
      };

      // 简化显著性判断
      result.significant = control.trades >= 20 && treatment.trades >= 20 &&
        Math.abs(parseFloat(result.difference.avgPnl)) > 5;
    }

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * v7.3 获取拒绝信号数据
 */
function getRejectedSignalsData(windowDays = 7) {
  const cutoff = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  const result = {
    timestamp: new Date().toISOString(),
    windowDays,
    stages: [],
    summary: {}
  };

  try {
    // 检查表是否存在
    const tableExists = db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name='rejected_signals'
    `).get();

    if (!tableExists) {
      result.error = 'rejected_signals 表不存在';
      return result;
    }

    const stages = db.prepare(`
      SELECT
        rejection_stage,
        COUNT(*) as total,
        SUM(CASE WHEN tracking_completed = 1 THEN 1 ELSE 0 END) as tracked,
        AVG(CASE WHEN tracking_completed = 1 THEN would_have_profit ELSE NULL END) as avg_avoided_pnl,
        SUM(CASE WHEN would_have_profit < 0 THEN 1 ELSE 0 END) as correct_rejections,
        SUM(CASE WHEN would_have_profit < -20 THEN 1 ELSE 0 END) as dodged_big_loss,
        SUM(CASE WHEN would_have_profit > 50 THEN 1 ELSE 0 END) as missed_big_gain
      FROM rejected_signals
      WHERE created_at >= ?
      GROUP BY rejection_stage
    `).all(cutoff);

    result.stages = stages.map(s => ({
      stage: s.rejection_stage,
      total: s.total,
      tracked: s.tracked,
      avgAvoidedPnl: s.avg_avoided_pnl?.toFixed(1) || 'N/A',
      accuracy: s.tracked > 0 ? ((s.correct_rejections / s.tracked) * 100).toFixed(1) : 'N/A',
      dodgedBigLoss: s.dodged_big_loss || 0,
      missedBigGain: s.missed_big_gain || 0
    }));

    // 总体统计
    const totalRejected = stages.reduce((sum, s) => sum + s.total, 0);
    const totalTracked = stages.reduce((sum, s) => sum + (s.tracked || 0), 0);
    const totalCorrect = stages.reduce((sum, s) => sum + (s.correct_rejections || 0), 0);

    result.summary = {
      totalRejected,
      totalTracked,
      overallAccuracy: totalTracked > 0 ? ((totalCorrect / totalTracked) * 100).toFixed(1) : 'N/A'
    };

  } catch (e) {
    result.error = e.message;
  }

  return result;
}

/**
 * HTTP 服务器
 */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const requestMetric = beginDashboardRequestMetric(req, url);
  res.once('finish', () => finishDashboardRequestMetric(requestMetric, res, 'finish'));
  res.once('close', () => {
    if (!requestMetric.finished) finishDashboardRequestMetric(requestMetric, res, 'close');
  });

  if (url.pathname === '/' || url.pathname === '/health' || url.pathname === '/ping') {
    const shadowSidecars = Array.isArray(global.__shadowDataSidecars)
      ? global.__shadowDataSidecars.map((worker) => (
        typeof worker?.getStatus === 'function'
          ? worker.getStatus()
          : { name: worker?.name || 'unknown', running: null }
      ))
      : [];
    const paperFastLaneHealth = readPaperFastLaneHealth();
    const paperDbHealth = readPaperDbRuntimeHealth();
    const paperReviewSnapshotHealth = readPaperReviewSnapshotHealth();
    const signalSourceFreshnessHealth = readSignalSourceFreshnessHealth();
    const runtimeFinalEvidenceHealth = readRuntimeFinalEvidenceHealth();
    const degraded = Boolean(
      global.__startupError
      || !paperDbHealthIsUsable(paperDbHealth)
      || (paperFastLaneHealth.required && paperFastLaneHealth.status !== 'ok')
      || paperReviewSnapshotHealth.status !== 'ok'
      || signalSourceFreshnessHealth.fail_closed
    );
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: degraded ? 'degraded' : 'ok',
      message: 'Sentiment Arbitrage API Running',
      timestamp: Date.now(),
      commit: runtimeCommitFingerprint(),
      runtime_role: process.env.DASHBOARD_RUNTIME_ROLE || (process.env.EMBEDDED_DASHBOARD_ENABLED === 'false' ? 'worker' : 'standalone_or_embedded_dashboard'),
      entrypoint: dashboardEntrypointInfo(),
      pid: process.pid,
      port: PORT,
      uptime_seconds: Math.floor(process.uptime()),
      startup_error: global.__startupError || null,
      runtime_worker: global.__runtimeWorkerStatus || null,
      shadow_sidecars: {
        available: shadowSidecars.length > 0,
        running: shadowSidecars.filter((worker) => worker.running === true).length,
        total: shadowSidecars.length,
        workers: shadowSidecars,
      },
      paper_fast_lane_health: paperFastLaneHealth,
      paper_review_snapshot_health: paperReviewSnapshotHealth,
      paper_db_health: paperDbHealth,
      runtime_final_evidence: runtimeFinalEvidenceHealth,
      signal_source_freshness_health: signalSourceFreshnessHealth,
      raw_path_observer_worker: global.__rawPathObserverWorkerStatus || null,
      raw_dog_discovery_worker: global.__rawDogDiscoveryWorkerStatus || null,
      raw_dog_discovery_observer: rawDogDiscoveryObserverStatus(),
      dashboard_request_metrics: {
        active_count: dashboardRequestMetrics.active.size,
        recent_count: dashboardRequestMetrics.recent.length,
        slow_count: dashboardRequestMetrics.slow.length,
        last_slow: dashboardRequestMetrics.slow[dashboardRequestMetrics.slow.length - 1] || null,
        path: dashboardRequestMetricsPath,
        events_path: dashboardRuntimeEventsPath,
      },
    }));
    return;
  } else if (url.pathname === '/api/runtime/request-metrics') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, apiJsonHeaders());
    res.end(JSON.stringify(requestMetricsSnapshot(), null, 2));
    return;
  } else if (url.pathname === '/api/runtime/events') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, apiJsonHeaders());
    res.end(JSON.stringify({
      schema_version: 'dashboard_runtime_events_api.v1',
      generated_at: new Date().toISOString(),
      path: dashboardRuntimeEventsPath,
      events: readDashboardRuntimeEvents(boundedIntParam(url, 'limit', 80, 1, 500)),
      request_metrics: requestMetricsSnapshot(),
    }, null, 2));
    return;
  } else if (url.pathname === '/dashboard') {
    res.writeHead(302, { 'Location': '/premium' });
    res.end();
  } else if (url.pathname === '/api/status') {
    if (!checkAuth(req, url, res)) return;
    const data = getDashboardData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data, null, 2));
  } else if (url.pathname === '/api/autonomy-status') {
    if (!checkAuth(req, url, res)) return;
    const payload = {
      autonomy: global.__autonomySidecar?.getStatus?.() || { enabled: false },
      leaderboard: getExperimentLeaderboard(),
      recentExperiments: listRecentExperiments()
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(payload, null, 2));
  } else if (url.pathname === '/api/module-health') {
    // v7.3 模块健康状态 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 7;
    const moduleHealth = getModuleHealthData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(moduleHealth, null, 2));
  } else if (url.pathname === '/api/narrative-effectiveness') {
    // v7.3 AI 叙事有效性 API
    if (!checkAuth(req, url, res)) return;
    const narrativeData = getNarrativeEffectivenessData();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(narrativeData, null, 2));
  } else if (url.pathname === '/api/ab-test') {
    // v7.3 A/B 测试状态 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 14;
    const abTestData = getABTestData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(abTestData, null, 2));
  } else if (url.pathname === '/api/rejected-signals') {
    // v7.3 拒绝信号统计 API
    if (!checkAuth(req, url, res)) return;
    const windowDays = parseInt(url.searchParams.get('window')) || 7;
    const rejectedData = getRejectedSignalsData(windowDays);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(rejectedData, null, 2));
  } else if (url.pathname === '/api/channel-history') {
    // Fetch Telegram channel message history for backtest
    if (!checkAuth(req, url, res)) return;
    try {
      const tg = global.__telegramService;
      if (!tg || !tg.client) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Telegram service not available' }));
        return;
      }
      const limit = Math.min(parseInt(url.searchParams.get('limit')) || 200, 3000);
      const history = await tg.getChannelHistory(limit);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(history, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/shadow-pnl') {
    // Premium Channel Shadow PnL API
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      // 单独查询所有未关闭仓位（不受 LIMIT 限制）
      const open = d.prepare(`
        SELECT symbol, score, entry_mc, entry_time, exit_pnl, high_pnl, low_pnl, exit_reason, closed, closed_at
        FROM shadow_pnl WHERE closed = 0 ORDER BY entry_time DESC
      `).all();

      // 查询最近已关闭的交易
      const closed = d.prepare(`
        SELECT symbol, score, entry_mc, entry_time, exit_pnl, high_pnl, low_pnl, exit_reason, closed, closed_at
        FROM shadow_pnl WHERE closed = 1 ORDER BY entry_time DESC LIMIT 200
      `).all();

      // 统计所有已关闭交易（不受 LIMIT 限制）
      const allStats = d.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN exit_pnl <= 0 THEN 1 ELSE 0 END) as losses,
          AVG(exit_pnl) as avgPnl,
          SUM(exit_pnl) as totalPnl
        FROM shadow_pnl WHERE closed = 1
      `).get();

      const winRate = allStats.total > 0 ? (allStats.wins / allStats.total * 100) : 0;

      // 按 exit_reason 分组
      const byReason = {};
      for (const r of closed) {
        const reason = (r.exit_reason || 'UNKNOWN').replace(/\(.*\)/, '');
        if (!byReason[reason]) byReason[reason] = { count: 0, totalPnl: 0 };
        byReason[reason].count++;
        byReason[reason].totalPnl += r.exit_pnl || 0;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        summary: {
          total: allStats.total || 0,
          wins: allStats.wins || 0,
          losses: allStats.losses || 0,
          winRate: +winRate.toFixed(1),
          avgPnl: +(allStats.avgPnl || 0).toFixed(1),
          totalPnl: +(allStats.totalPnl || 0).toFixed(1)
        },
        open: open.map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        recent: closed.slice(0, 50).map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        byReason
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/premium') {
    // Premium Channel Dashboard 页面
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(renderPremiumDashboard());
  } else if (url.pathname === '/api/live-positions') {
    // 实盘交易记录 API
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      // 检查 live_positions 表是否存在
      const tableExists = d.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name='live_positions'`).get();
      if (!tableExists) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ summary: { total: 0, wins: 0, losses: 0, winRate: 0, totalPnl: 0, totalSolSpent: 0, totalSolReceived: 0 }, open: [], recent: [] }));
        return;
      }

      const open = d.prepare(`
        SELECT token_ca, symbol, entry_mc, entry_sol, token_amount, high_pnl, low_pnl, status, entry_time,
               sold_pct, total_sol_received, tp1_triggered, tp2_triggered, tp3_triggered, tp4_triggered, moonbag_active
        FROM live_positions WHERE status = 'open' ORDER BY entry_time DESC
      `).all();

      const closed = d.prepare(`
        SELECT token_ca, symbol, entry_mc, entry_sol, exit_pnl, high_pnl, low_pnl, exit_reason, status, entry_time, closed_at, total_sol_received,
               entry_price, token_amount, token_decimals
        FROM live_positions WHERE status = 'closed' ORDER BY entry_time DESC LIMIT 50
      `).all();

      const allStats = d.prepare(`
        SELECT
          COUNT(*) as total,
          SUM(CASE WHEN exit_pnl > 0 THEN 1 ELSE 0 END) as wins,
          SUM(CASE WHEN exit_pnl <= 0 THEN 1 ELSE 0 END) as losses,
          AVG(exit_pnl) as avgPnl,
          SUM(exit_pnl) as totalPnl,
          SUM(entry_sol) as totalSolSpent,
          SUM(CASE WHEN total_sol_received >= 0 THEN total_sol_received ELSE 0 END) as totalSolReceived
        FROM live_positions WHERE status = 'closed'
      `).get();

      const winRate = allStats.total > 0 ? (allStats.wins / allStats.total * 100) : 0;
      // 计算真实总 PnL（基于实际 SOL 进出）
      const realTotalPnl = allStats.totalSolSpent > 0
        ? ((allStats.totalSolReceived - allStats.totalSolSpent) / allStats.totalSolSpent) * 100
        : 0;

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        summary: {
          total: allStats.total || 0,
          wins: allStats.wins || 0,
          losses: allStats.losses || 0,
          winRate: +winRate.toFixed(1),
          avgPnl: +(allStats.avgPnl || 0).toFixed(1),
          totalPnl: +(allStats.totalPnl || 0).toFixed(1),
          realTotalPnl: +realTotalPnl.toFixed(1),
          totalSolSpent: +(allStats.totalSolSpent || 0).toFixed(4),
          totalSolReceived: +(allStats.totalSolReceived || 0).toFixed(4)
        },
        open: open.map(r => ({ ...r, entry_mc_k: +(r.entry_mc / 1000).toFixed(1) })),
        recent: closed.map(r => {
          // 计算实际 SOL 收益（total_sol_received < 0 表示不可追踪，用 exit_pnl）
          const solRecv = (r.total_sol_received != null && r.total_sol_received >= 0) ? r.total_sol_received : 0;
          const realPnl = (solRecv > 0 && r.entry_sol > 0)
            ? ((solRecv - r.entry_sol) / r.entry_sol * 100)
            : (r.exit_pnl || 0);
          // 计算峰值捕获率
          const captureRate = r.high_pnl > 0 ? (realPnl / r.high_pnl * 100) : 0;
          // 计算损失
          const loss = r.high_pnl - realPnl;

          return {
            ...r,
            entry_mc_k: +(r.entry_mc / 1000).toFixed(1),
            real_pnl: +realPnl.toFixed(1),
            capture_rate: +captureRate.toFixed(1),
            loss: +loss.toFixed(1)
          };
        })
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/close-position') {
    // 手动关闭持仓 API — 需要 POST + token 认证
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method not allowed. Use POST.' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      if (!d) throw new Error('Database not ready');

      const ca = url.searchParams.get('ca');
      const reason = url.searchParams.get('reason') || 'MANUAL_CLOSE';

      if (!ca) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Missing ca parameter' }));
        return;
      }

      // 检查是否存在
      const pos = d.prepare(`SELECT * FROM live_positions WHERE token_ca = ? AND status = 'open'`).get(ca);
      if (!pos) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Position not found or already closed' }));
        return;
      }
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:admin_mutation',
        danger_level: 'critical',
        action: 'close_position',
        payload: { ca, reason, symbol: pos.symbol || null },
      })) return;

      // 关闭持仓
      d.prepare(`
        UPDATE live_positions
        SET status = 'closed',
            exit_reason = ?,
            exit_pnl = -100,
            closed_at = ?,
            total_sol_received = 0
        WHERE token_ca = ? AND status = 'open'
      `).run(reason, Date.now(), ca);

      console.log(`🔧 [手动关闭] ${pos.symbol} (${ca.substring(0, 8)}...) - ${reason}`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        message: `Position ${pos.symbol} closed`,
        ca: ca,
        reason: reason
      }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/pause-trading') {
    // 手动暂停交易
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const hours = parseInt(url.searchParams.get('hours') || '4');
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:risk_mutation',
        danger_level: 'admin_mutation',
        action: 'pause_trading',
        payload: { hours },
      })) return;
      const rm = global.__riskManager;
      if (rm) {
        rm.manualPause(hours);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `交易已暂停 ${hours} 小时`, pausedUntil: rm.state.pausedUntil }));
      } else {
        // fallback: 直接写 DB
        const d = getDb();
        const pauseUntil = Math.floor(Date.now() / 1000) + hours * 3600;
        d.prepare(`INSERT OR REPLACE INTO system_state (key, value, expires_at) VALUES ('trading_paused', 'true', ?)`).run(pauseUntil);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `交易已暂停 ${hours} 小时 (DB only)`, pausedUntil: new Date(pauseUntil * 1000) }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/resume-trading') {
    // 手动恢复交易
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:risk_mutation',
        danger_level: 'admin_mutation',
        action: 'resume_trading',
        payload: {},
      })) return;
      const rm = global.__riskManager;
      if (rm) {
        rm.resumeTrading();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '交易已恢复，连亏计数已重置' }));
      } else {
        const d = getDb();
        d.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '交易已恢复 (DB only)' }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/trading-status') {
    // 获取交易状态
    if (!checkAuth(req, url, res)) return;
    try {
      const rm = global.__riskManager;
      if (rm) {
        const status = rm.getStatus();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ paused: !!status.pausedUntil, pausedUntil: status.pausedUntil, consecutiveLosses: status.consecutiveLosses, canTrade: status.canTrade, dailyNetPnl: status.dailyNetPnlSol, dailyLossLimit: status.dailyLossLimitSol }));
      } else {
        const d = getDb();
        const row = d.prepare(`SELECT value, expires_at FROM system_state WHERE key = 'trading_paused'`).get();
        const paused = row && row.expires_at > Date.now() / 1000;
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ paused, pausedUntil: paused ? new Date(row.expires_at * 1000) : null }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/reset-daily-loss') {
    // 重置今日亏损统计起点（用于"重新开始"，不删除历史数据）
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:risk_mutation',
        danger_level: 'admin_mutation',
        action: 'reset_daily_loss',
        payload: {},
      })) return;
      const rm = global.__riskManager;
      if (rm) {
        rm.resetDailyLoss();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: `今日亏损统计已重置，从 ${new Date().toLocaleString()} 起重新计算` }));
      } else {
        const d = getDb();
        d.prepare(`INSERT OR REPLACE INTO system_state (key, value) VALUES ('daily_loss_reset_ts', ?)`).run(Date.now().toString());
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, message: '今日亏损统计已重置 (DB only)' }));
      }
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }

  } else if (url.pathname === '/api/reset-live-data') {
    // 清空实盘交易数据，重新开始
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const d = getDb();
      const count = d.prepare(`SELECT COUNT(*) as c FROM live_positions`).get().c;
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:admin_mutation',
        danger_level: 'critical',
        action: 'reset_live_data',
        payload: { live_position_count: count },
      })) return;
      d.prepare(`DELETE FROM live_positions`).run();
      try { d.prepare(`DELETE FROM system_state WHERE key = 'trading_paused'`).run(); } catch(e) { /* table may not exist */ }
      const rm = global.__riskManager;
      if (rm) {
        rm.state.pausedUntil = null;
        rm.state.consecutiveLosses = 0;
        rm._circuitBreakerTriggered = false;
        rm._circuitBreakerLogged = false;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, message: `已清空 ${count} 条实盘记录，风控状态已重置` }));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
  } else if (url.pathname === '/api/paper-trades/cleanup') {
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }
    if (!checkAuth(req, url, res)) return;
    try {
      const reason = (url.searchParams.get('reason') || 'manual_cleanup').trim() || 'manual_cleanup';
      const pnlPctRaw = url.searchParams.get('pnl_pct') || '0';
      const pnlPct = Number(pnlPctRaw);
      if (!Number.isFinite(pnlPct)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'pnl_pct must be a valid number' }));
        return;
      }
      if (!requireDashboardAuditEvent(req, res, url, {
        required_role: 'dashboard_admin',
        token_scope: 'dashboard:paper_mutation',
        danger_level: 'admin_mutation',
        action: 'paper_trades_cleanup',
        payload: { reason, pnl_pct: pnlPct },
      })) return;
      const result = cleanupOpenPaperPositions({ reason, pnlPct });
      console.log(`🧹 Cleaned ${result.updated} open paper positions reason=${reason} pnlPct=${pnlPct} db=${result.dbPath}`);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: true, ...result }));
    } catch (e) {
      res.writeHead(e.statusCode || 500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/download/database') {
    // 数据库下载端点 — 默认走 SQLite backup，避免遗漏 WAL 中的线上新信号。
    await downloadSqliteDatabase(req, res, url, resolvedDbPath, 'sentiment_arb.db', 'Signal database', 'sentiment_arb_download');
    return;
  } else if (url.pathname === '/api/download/kline_cache') {
    // K线数据库下载 — 默认走 SQLite backup，避免只下载主文件导致回测缺口。
    const klineDbPath = join(projectRoot, 'data', 'kline_cache.db');
    await downloadSqliteDatabase(req, res, url, klineDbPath, 'kline_cache.db', 'Kline cache database', 'kline_cache_download');
    return;
  } else if (url.pathname === '/api/download/lifecycle_tracks') {
    const lifecycleDbPath = isAbsolute(process.env.LIFECYCLE_DB || '')
      ? process.env.LIFECYCLE_DB
      : join(projectRoot, process.env.LIFECYCLE_DB || 'data/lifecycle_tracks.db');
    await downloadSqliteDatabase(req, res, url, lifecycleDbPath, 'lifecycle_tracks.db', 'Lifecycle tracks database', 'lifecycle_tracks_download');
    return;
  } else if (url.pathname === '/api/data/download/sentiment-db') {
    await downloadSqliteDatabase(req, res, url, resolvedDbPath, 'sentiment_arb.db', 'Signal database', 'sentiment_arb_download');
    return;
  } else if (url.pathname === '/api/data/download/raw-signal-outcomes') {
    await downloadSqliteDatabase(req, res, url, getRawSignalOutcomesDbPath(), 'raw_signal_outcomes.db', 'Raw signal outcomes database', 'raw_signal_outcomes_download');
    return;
  } else if (url.pathname === '/api/data/download/paper-trades') {
    await downloadSqliteDatabase(req, res, url, getPaperDbPath(), 'paper_trades.db', 'Paper trades database', 'paper_trades_download');
    return;
  } else if (url.pathname === '/api/data/download/kline-cache') {
    await downloadSqliteDatabase(req, res, url, join(projectRoot, 'data', 'kline_cache.db'), 'kline_cache.db', 'Kline cache database', 'kline_cache_download');
    return;
  } else if (url.pathname === '/api/data/download/lifecycle-tracks') {
    const lifecycleDbPath = isAbsolute(process.env.LIFECYCLE_DB || '')
      ? process.env.LIFECYCLE_DB
      : join(projectRoot, process.env.LIFECYCLE_DB || 'data/lifecycle_tracks.db');
    await downloadSqliteDatabase(req, res, url, lifecycleDbPath, 'lifecycle_tracks.db', 'Lifecycle tracks database', 'lifecycle_tracks_download');
    return;
  } else if (url.pathname === '/api/agent/capture-discovery/latest') {
    if (!checkAuth(req, url, res)) return;
    const includeReports = ['1', 'true', 'yes', 'on'].includes(String(url.searchParams.get('include_reports') || '').toLowerCase());
    const snapshot = buildAgentCaptureDiscoveryLatestSnapshot({ includeReports });
    res.writeHead(snapshot.required_artifacts_complete ? 200 : 202, apiJsonHeaders());
    res.end(JSON.stringify(snapshot, null, 2));
    return;
  } else if (url.pathname === '/api/agent/capture-discovery/run-status') {
    if (!checkAuth(req, url, res)) return;
    const snapshot = buildAgentCaptureDiscoveryLatestSnapshot({ includeReports: false });
    res.writeHead(200, apiJsonHeaders());
    res.end(JSON.stringify({
      schema_version: 'agent_capture_loop_run_status_api.v1',
      generated_at: new Date().toISOString(),
      runner_status: snapshot.runner_status,
      verdict_summary: snapshot.verdict_summary,
      artifacts: {
        verdict: snapshot.artifacts.verdict,
        summary: snapshot.artifacts.summary,
        runtime_health: snapshot.artifacts.runtime_health,
        runner_status: snapshot.artifacts.runner_status,
        log: snapshot.artifacts.log,
      },
      promotion_allowed: false,
      strategy_change_allowed: false,
      automatic_runtime_change_allowed: false,
      paper_enablement_allowed: false,
    }, null, 2));
    return;
  } else if (url.pathname === '/api/agent/capture-discovery/run') {
    if (!checkAuth(req, url, res)) return;
    if (!requirePost(req, res)) return;
    try {
      const result = triggerAgentCaptureDiscoveryLoop(url);
      res.writeHead(result.accepted ? 202 : 409, apiJsonHeaders());
      res.end(JSON.stringify(result, null, 2));
    } catch (error) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({
        error: error.message,
        error_code: 'agent_capture_loop_run_failed',
        promotion_allowed: false,
        strategy_change_allowed: false,
        automatic_runtime_change_allowed: false,
        paper_enablement_allowed: false,
      }, null, 2));
    }
    return;
  } else if (url.pathname === '/api/data/download/agent-capture-discovery') {
    if (!checkAuth(req, url, res)) return;
    const aliases = {
      reviewer_verdict: 'verdict',
      run_summary: 'summary',
      latest_handoff: 'handoff',
      hypothesis_registry: 'registry',
      capture_report: 'capture',
      capture_report_24h: 'capture_24h',
      capture_report_48h: 'capture_48h',
      capture_report_72h: 'capture_72h',
      raw_gold_silver_funnel: 'raw_funnel',
      raw_funnel_audit: 'raw_funnel',
      shadow_decision_bridge_audit: 'shadow_decision_bridge',
      shadow_entry_decision_bridge: 'shadow_decision_bridge',
      candidate_downstream_readiness: 'downstream_readiness',
      context_coverage_audit: 'context_coverage',
      context_blocker_monitor: 'context_blocker_monitor',
      a_class_fastlane_mode_audit: 'a_class_fastlane',
      a_class_mode_audit: 'a_class_fastlane',
      candidate_effectiveness_report: 'candidate_effectiveness',
      candidate_improvement_opportunities: 'candidate_improvement',
      capture_cross_validity_report: 'capture_cross_validity',
      pnl_cross: 'pnl',
      markov_runtime: 'markov_runtime',
      markov_kline: 'markov_kline',
      markov_candidate_lifecycle: 'markov_candidate_lifecycle',
      markov_candidate_source: 'markov_candidate_source',
      markov_candidate_signal_type: 'markov_candidate_signal_type',
      markov_candidate_lifecycle_source: 'markov_candidate_lifecycle_source',
      markov_effectiveness_report: 'markov_effectiveness',
      volume_kline_coverage_audit: 'volume_kline_coverage',
      matured_kline_volume_recheck: 'matured_kline_recheck',
      matured_volume_capture_cross: 'matured_volume_cross',
      hypothesis_validation_audit: 'hypothesis_validation',
      oos_readiness_probe_refresh: 'oos_readiness_refresh',
      low_confidence_research_capture: 'low_confidence_research',
      quality_timing_reject_research: 'quality_timing_research',
      quality_timing_candidate_probe_validation: 'quality_timing_probe_validation',
      quality_timing_probe_validation: 'quality_timing_probe_validation',
      runtime_health: 'runtime_health',
      runner_status: 'runner_status',
      self_tests: 'tests',
      agent_log: 'log',
    };
    const requested = String(url.searchParams.get('artifact') || 'verdict').trim();
    const artifact = aliases[requested] || requested;
    const paths = agentCaptureArtifactPaths();
    const artifactPath = paths[artifact];
    if (!artifactPath) {
      res.writeHead(400, apiJsonHeaders());
      res.end(JSON.stringify({
        error: 'unsupported_agent_capture_discovery_artifact',
        supported_artifacts: Object.keys(paths).sort(),
        supported_aliases: Object.keys(aliases).sort(),
      }, null, 2));
      return;
    }
    if (!fs.existsSync(artifactPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({
        error: 'agent_capture_discovery_artifact_not_found',
        artifact,
        path: artifactPath,
      }, null, 2));
      return;
    }
    streamDownloadFile(
      res,
      artifactPath,
      `agent-capture-discovery-${artifact}${artifactPath.endsWith('.md') ? '.md' : artifactPath.endsWith('.log') ? '.log' : '.json'}`,
      null,
      {
        'Content-Type': agentArtifactContentType(artifactPath),
        'Cache-Control': 'no-store',
      },
    );
    return;
  } else if (url.pathname === '/api/data/download/audit-bundle') {
    if (!checkAuth(req, url, res)) return;
    const origin = `https://${req.headers.host || 'sentiment-arbitrage.zeabur.app'}`;
    const tokenHint = '<DASHBOARD_TOKEN>';
    const payload = {
      generated_at: new Date().toISOString(),
      bundle_type: 'audit_manifest',
      note: 'Download DB endpoints use SQLite backup snapshots by default; pass backup=raw only for debugging.',
      downloads: {
        signal_db: `${origin}/api/data/download/sentiment-db?token=${tokenHint}`,
        raw_signal_outcomes_db: `${origin}/api/data/download/raw-signal-outcomes?token=${tokenHint}`,
        paper_trades_db: `${origin}/api/data/download/paper-trades?token=${tokenHint}`,
        kline_cache_db: `${origin}/api/data/download/kline-cache?token=${tokenHint}`,
        lifecycle_tracks_db: `${origin}/api/data/download/lifecycle-tracks?token=${tokenHint}`,
        canonical_ledger_json: `${origin}/api/data/download/canonical-ledger?token=${tokenHint}`,
        agent_reviewer_verdict_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=verdict`,
        agent_run_summary_md: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=summary`,
        agent_codex_handoff_md: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=handoff`,
        agent_hypothesis_registry_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=registry`,
        agent_capture_report_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=capture`,
        agent_capture_report_24h_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=capture_24h`,
        agent_capture_report_48h_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=capture_48h`,
        agent_capture_report_72h_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=capture_72h`,
        agent_raw_gold_silver_funnel_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=raw_funnel`,
        agent_shadow_decision_bridge_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=shadow_decision_bridge`,
        agent_candidate_downstream_readiness_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=downstream_readiness`,
        agent_context_coverage_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=context_coverage`,
        agent_context_blocker_monitor_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=context_blocker_monitor`,
        agent_a_class_fastlane_mode_audit_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=a_class_fastlane`,
        agent_candidate_effectiveness_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=candidate_effectiveness`,
        agent_candidate_improvement_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=candidate_improvement`,
        agent_capture_cross_validity_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=capture_cross_validity`,
        agent_pnl_cross_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=pnl`,
        agent_markov_runtime_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_runtime`,
        agent_markov_kline_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_kline`,
        agent_markov_candidate_lifecycle_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_candidate_lifecycle`,
        agent_markov_candidate_source_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_candidate_source`,
        agent_markov_candidate_signal_type_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_candidate_signal_type`,
        agent_markov_candidate_lifecycle_source_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_candidate_lifecycle_source`,
        agent_markov_effectiveness_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=markov_effectiveness`,
        agent_volume_kline_coverage_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=volume_kline_coverage`,
        agent_matured_kline_recheck_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=matured_kline_recheck`,
        agent_matured_volume_cross_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=matured_volume_cross`,
        agent_hypothesis_validation_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=hypothesis_validation`,
        agent_oos_readiness_refresh_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=oos_readiness_refresh`,
        agent_low_confidence_research_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=low_confidence_research`,
        agent_quality_timing_research_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=quality_timing_research`,
        agent_quality_timing_probe_validation_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=quality_timing_probe_validation`,
        agent_runtime_health_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=runtime_health`,
        agent_runner_status_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=runner_status`,
        agent_self_tests_json: `${origin}/api/data/download/agent-capture-discovery?token=${tokenHint}&artifact=tests`,
      },
      review_apis: {
        agent_capture_discovery_latest: `${origin}/api/agent/capture-discovery/latest?token=${tokenHint}`,
        rolling_24h_goal: `${origin}/api/goal/rolling-24h?token=${tokenHint}`,
        a_class_status: `${origin}/api/a-class/status?token=${tokenHint}&hours=24`,
        a_class_events: `${origin}/api/a-class/events?token=${tokenHint}&hours=24&limit=500`,
        a_class_block_causes: `${origin}/api/a-class/block-causes?token=${tokenHint}&hours=24&limit=100`,
        a_class_matrix: `${origin}/api/a-class/matrix?token=${tokenHint}&hours=24&limit=500`,
        a_class_ai_reviews: `${origin}/api/a-class/ai-reviews?token=${tokenHint}&hours=24&limit=500`,
        a_class_scorecard: `${origin}/api/scorecard/a-class?token=${tokenHint}&hours=168`,
        controller_actions: `${origin}/api/goal/controller-actions?token=${tokenHint}&hours=24`,
        missed_dog_ai_review: `${origin}/api/missed-dog/ai-review?token=${tokenHint}&hours=24`,
        counterfactual_ai_audit: `${origin}/api/counterfactual/ai-audit?token=${tokenHint}&hours=24`,
        fast_lane: `${origin}/api/paper/fast-lane?token=${tokenHint}&live=1&hours=2`,
        source_resonance: `${origin}/api/paper/source-resonance?token=${tokenHint}&live=1&hours=2`,
        storage_health: `${origin}/api/paper/storage-health?token=${tokenHint}&files=1`,
      },
      storage_health: buildStorageHealthSnapshot({ includeFileStats: true }),
    };
    const text = JSON.stringify(payload, null, 2);
    res.writeHead(200, {
      'Content-Type': 'application/json; charset=utf-8',
      'Content-Disposition': 'attachment; filename="sentiment-arbitrage-audit-manifest.json"',
      'Content-Length': Buffer.byteLength(text),
    });
    res.end(text);
    return;
  } else if (url.pathname === '/api/data/download/canonical-ledger') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      // HARD CAP: large limits made this endpoint SELECT * x 5 tables (incl big *_json columns)
      // and serialize tens of thousands of rows, which overloaded/crashed the live dashboard
      // (limit=20000 -> 90s+ then all endpoints 502). Cap at 2000 so a single request stays bounded.
      // Complete a_class coverage comes from the paginated /api/a-class/events (before_id) instead.
      const limit = boundedIntParam(url, 'limit', 1000, 1, 2000);
      const sinceTs = boundedWindowedSinceTs(url, 24, 24 * 120, { allowAll: true });
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 5000, 1000, 30000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      const tables = {};
      for (const table of ['canonical_trade_ledger', 'paper_trades', 'paper_decision_events', 'a_class_decision_events', 'paper_missed_signal_attribution']) {
        if (!tableNames.has(table)) {
          tables[table] = { available: false, rows: [] };
          continue;
        }
        const cols = getTableColumns(paperDb, table);
        const firstExisting = (names, fallback = '0') => names.find((name) => cols.has(name)) || fallback;
        const orderCol = (table === 'canonical_trade_ledger' || table === 'paper_trades')
          ? `COALESCE(${[
              firstExisting(['entry_ts'], null),
              firstExisting(['exit_ts'], null),
              firstExisting(['created_at'], null),
              '0',
            ].filter(Boolean).join(', ')})`
          : ((table === 'a_class_decision_events' || table === 'paper_decision_events')
              ? firstExisting(['event_ts'], '0')
              : `COALESCE(${[
                  firstExisting(['created_event_ts'], null),
                  firstExisting(['signal_ts'], null),
                  firstExisting(['created_at'], null),
                  '0',
                ].filter(Boolean).join(', ')})`);
        const whereClause = sinceTs == null ? '' : `WHERE ${orderCol} >= @sinceTs`;
        const orderBy = cols.has('id') ? 'id DESC' : `${orderCol} DESC`;
        const rows = paperDb.prepare(`
          SELECT *
          FROM ${table}
          ${whereClause}
          ORDER BY ${orderBy}
          LIMIT @limit
        `).all({ limit, sinceTs: sinceTs ?? 0 });
        tables[table] = { available: true, count: rows.length, rows, since_ts: sinceTs };
      }
      const payload = {
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        export_type: 'canonical_ledger_and_a_class_evidence',
        limit,
        since_ts: sinceTs,
        window_hours: sinceTs == null ? null : Math.round((Math.floor(Date.now() / 1000) - sinceTs) / 3600),
        tables,
      };
      const text = JSON.stringify(payload, null, 2);
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Disposition': 'attachment; filename="canonical-ledger-export.json"',
        'Content-Length': Buffer.byteLength(text),
      });
      res.end(text);
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/data-source-policy') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      generated_at: new Date().toISOString(),
      policy_version: 'v1_explicit_fail_modes',
      principles: [
        'Execution price and PnL must use SOL/token quote truth, not GMGN/DexScreener USD marks.',
        'Entry quote failure is fail-closed: do not enter without a fresh Jupiter-compatible quote.',
        'GMGN unavailable is fail-soft: do not boost or rescue, but do not reject the base signal only because GMGN is down.',
        'DexScreener trend unavailable is route-dependent: LOTTO defensive gates should wait/expire; optional watchlist guards may warn and fail-open.',
        'Exit quote failure does not synthesize profit; it records quote failure and keeps monitoring unless a trapped/no-route fail-safe triggers.',
      ],
      boundaries: [
        { boundary: 'entry_execution_quote', source: 'Jupiter/shared quote', unavailable: 'fail_closed', action: 'retry within quote window, then drop pending entry' },
        { boundary: 'entry_timing_price', source: 'Redis/shared quote/DexScreener/GeckoTerminal', unavailable: 'fail_closed', action: 'SmartEntry rejects no_price' },
        { boundary: 'gmgn_lotto_policy', source: 'GMGN readonly enrichment', unavailable: 'fail_soft', action: 'allow base route, disable GMGN boost/tiny rescue' },
        { boundary: 'lotto_defense_snapshot', source: 'DexScreener + Helius', unavailable: 'defensive_wait_or_expire', action: 'do not treat missing liquidity/activity as a positive signal' },
        { boundary: 'watchlist_optional_fire_guard', source: 'DexScreener', unavailable: 'warn_fail_open', action: 'do not block solely on missing optional MC/liquidity guard' },
        { boundary: 'exit_trigger_price', source: 'fresh SOL/token price snapshot', unavailable: 'hold_and_log', action: 'do not close from stale or missing mark price' },
        { boundary: 'exit_execution_quote', source: 'Jupiter/shared quote', unavailable: 'fail_safe_after_retries', action: 'record quote failure; trapped/no-route fail-safe can synthetic-close later' },
      ],
    }, null, 2));
    return;
  } else if (url.pathname === '/api/paper/price-unit-audit') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const limit = boundedIntParam(url, 'limit', 50, 1, 100);
      const sinceTs = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_trades')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }
      const cols = getTableColumns(paperDb, 'paper_trades');
      const where = sinceTs ? 'WHERE entry_ts >= @since' : '';
      const rows = paperDb.prepare(`
        SELECT id, token_ca, symbol, entry_ts, exit_ts, entry_price,
               ${cols.has('trigger_price') ? 'trigger_price' : 'NULL AS trigger_price'},
               exit_price, pnl_pct,
               ${cols.has('synthetic_close') ? 'synthetic_close' : '0 AS synthetic_close'},
               ${cols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json'},
               ${cols.has('exit_execution_audit_json') ? 'exit_execution_audit_json' : 'NULL AS exit_execution_audit_json'},
               ${cols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json'}
        FROM paper_trades
        ${where}
        ORDER BY id DESC
        LIMIT @limit
      `).all(sinceTs ? { since: sinceTs, limit } : { limit });
      const audits = rows.map(priceUnitAuditForTrade);
      const counters = {
        sampled_trades: audits.length,
        clean_n: audits.filter((row) => row.warnings.length === 0).length,
        warning_n: audits.filter((row) => row.warnings.length > 0).length,
        entry_sol_per_token_n: audits.filter((row) => row.entry_unit === 'SOL_PER_TOKEN').length,
        exit_sol_per_token_n: audits.filter((row) => row.exit_unit === 'SOL_PER_TOKEN').length,
        pnl_ratio_decimal_n: audits.filter((row) => row.pnl_unit === 'RATIO_DECIMAL').length,
        accounting_sol_n: audits.filter((row) => row.accounting_unit === 'SOL').length,
        synthetic_close_n: audits.filter((row) => row.synthetic_close).length,
      };
      const warningCounts = {};
      for (const audit of audits) {
        for (const warning of audit.warnings) {
          warningCounts[warning] = (warningCounts[warning] || 0) + 1;
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        audit_version: 'v1_price_unit_contract',
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          limit,
        },
        canonical_units: {
          entry_price: 'SOL_PER_TOKEN',
          trigger_price: 'SOL_PER_TOKEN',
          exit_price: 'SOL_PER_TOKEN unless synthetic_close=1',
          pnl_pct: 'RATIO_DECIMAL stored in DB, displayed as percent by APIs',
          accounting: 'SOL',
          market_context: 'USD fields are allowed only as context, not fill/PnL truth',
        },
        status: counters.warning_n > 0 ? 'warn' : 'ok',
        counters,
        warning_counts: warningCounts,
        warnings: audits.filter((row) => row.warnings.length > 0).slice(0, 100),
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/provider-live-health') {
    if (!checkAuth(req, url, res)) return;
    try {
      const timeoutMs = Math.max(1000, Math.min(parseInt(url.searchParams.get('timeout_ms') || '5000', 10) || 5000, 15000));
      const helius = await probeHeliusRpcLive(timeoutMs);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        providers: {
          helius,
        },
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/paper/data-source-health') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let signalDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const sinceTs = boundedWindowedSinceTs(url, 2, 2);
      const nowSec = Math.floor(Date.now() / 1000);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      const health = {
        status: 'ok',
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        window: {
          since_ts: sinceTs,
          since_iso: new Date(sinceTs * 1000).toISOString(),
        },
        fail_modes: {
          fail_closed_entry: [],
          fail_soft_enrichment: [],
          hold_and_log_exit: [],
          warn_fail_open_optional: [],
        },
        counters: {},
        external_alpha_health: [],
        premium_signal_gate_health: null,
        signal_source_freshness_health: readSignalSourceFreshnessHealth(),
        provider_config_health: {
          helius: heliusConfigHealth(),
        },
        signal_db_path: resolvedDbPath,
        open_exit_quote_risk: null,
        missed_attribution_coverage: null,
        notes: [],
      };

      if (tableNames.has('paper_decision_events')) {
        const eventCounts = paperDb.prepare(`
          SELECT
            SUM(CASE WHEN component = 'execution_api' AND event_type = 'entry_quote' AND decision = 'fail' THEN 1 ELSE 0 END) AS entry_quote_fail_n,
            SUM(CASE WHEN component = 'execution_api' AND event_type = 'exit_quote' AND decision = 'fail' THEN 1 ELSE 0 END) AS exit_quote_fail_n,
            SUM(CASE WHEN component = 'smart_entry' AND decision = 'reject' AND reason = 'no_price' THEN 1 ELSE 0 END) AS smart_entry_no_price_n,
            SUM(CASE WHEN component = 'entry_readiness' AND event_type = 'watchlist_fire_deferred' THEN 1 ELSE 0 END) AS readiness_defer_n,
            SUM(CASE WHEN component = 'entry_readiness' AND event_type = 'watchlist_fire_expired' THEN 1 ELSE 0 END) AS readiness_expire_n,
            SUM(CASE WHEN reason LIKE '%rate_limited%' OR reason LIKE '%429%' THEN 1 ELSE 0 END) AS rate_limited_n,
            COUNT(*) AS total_events
          FROM paper_decision_events
          WHERE event_ts >= @since
        `).get({ since: sinceTs });
        health.counters = {
          total_events: eventCounts.total_events || 0,
          entry_quote_fail_n: eventCounts.entry_quote_fail_n || 0,
          exit_quote_fail_n: eventCounts.exit_quote_fail_n || 0,
          smart_entry_no_price_n: eventCounts.smart_entry_no_price_n || 0,
          readiness_defer_n: eventCounts.readiness_defer_n || 0,
          readiness_expire_n: eventCounts.readiness_expire_n || 0,
          rate_limited_n: eventCounts.rate_limited_n || 0,
        };
        health.fail_modes.fail_closed_entry = paperDb.prepare(`
          SELECT component, event_type, reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND (
              (component = 'execution_api' AND event_type = 'entry_quote' AND decision = 'fail')
              OR (component = 'smart_entry' AND decision = 'reject' AND reason = 'no_price')
            )
          GROUP BY component, event_type, reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
        health.fail_modes.hold_and_log_exit = paperDb.prepare(`
          SELECT reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND component = 'execution_api'
            AND event_type = 'exit_quote'
            AND decision = 'fail'
          GROUP BY reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
        health.fail_modes.warn_fail_open_optional = paperDb.prepare(`
          SELECT component, event_type, reason, data_source, COUNT(*) AS n, MAX(event_ts) AS last_event_ts
          FROM paper_decision_events
          WHERE event_ts >= @since
            AND decision IN ('warn', 'wait')
            AND (
              reason LIKE '%data%'
              OR reason LIKE '%dex%'
              OR reason LIKE '%liquidity%'
              OR reason LIKE '%readiness%'
            )
          GROUP BY component, event_type, reason, data_source
          ORDER BY n DESC, last_event_ts DESC
          LIMIT 20
        `).all({ since: sinceTs });
      } else {
        health.notes.push('paper_decision_events table missing; decision-level health unavailable');
      }

      if (tableNames.has('paper_trades')) {
        const tradeCols = getTableColumns(paperDb, 'paper_trades');
        if (tradeCols.has('exit_quote_failures') && tradeCols.has('last_exit_quote_failure')) {
          health.open_exit_quote_risk = paperDb.prepare(`
            SELECT
              COUNT(*) AS open_with_failures_n,
              MAX(exit_quote_failures) AS max_exit_quote_failures,
              SUM(CASE WHEN last_exit_quote_failure = 'no_route' THEN 1 ELSE 0 END) AS no_route_open_n,
              SUM(CASE WHEN last_exit_quote_failure = 'token_not_tradable' THEN 1 ELSE 0 END) AS token_not_tradable_open_n
            FROM paper_trades
            WHERE exit_reason IS NULL
              AND COALESCE(exit_quote_failures, 0) > 0
          `).get();
        }
      }

      if (tableNames.has('external_alpha_health')) {
        health.external_alpha_health = paperDb.prepare(`
          SELECT
            source,
            last_run_ts,
            last_success_ts,
            @now - COALESCE(last_run_ts, 0) AS last_run_age_sec,
            @now - COALESCE(last_success_ts, 0) AS last_success_age_sec,
            candidate_count,
            recorded_count,
            momentum_confirmed_count,
            error_count,
            last_error,
            updated_at
          FROM external_alpha_health
          ORDER BY updated_at DESC
        `).all({ now: nowSec });
        health.fail_modes.fail_soft_enrichment = health.external_alpha_health
          .filter((row) => !row.last_success_ts || row.last_success_age_sec > 15 * 60 || row.last_error)
          .map((row) => ({
            source: row.source,
            last_success_age_sec: row.last_success_age_sec,
            candidate_count: row.candidate_count,
            recorded_count: row.recorded_count,
            error_count: row.error_count,
            last_error: row.last_error,
          }));
      } else {
        health.notes.push('external_alpha_health table missing; GMGN scout health unavailable');
      }

      if (fs.existsSync(resolvedDbPath)) {
        try {
          signalDb = openDashboardSqlite(resolvedDbPath, { readonly: true });
          const signalTables = new Set(signalDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
          if (signalTables.has('premium_signals')) {
            const signalCols = getTableColumns(signalDb, 'premium_signals');
            const timestampExpr = signalCols.has('timestamp')
              ? "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
              : "0";
            const gateRows = signalDb.prepare(`
              SELECT
                id,
                symbol,
                token_ca,
                ${signalCols.has('timestamp') ? 'timestamp' : 'NULL AS timestamp'},
                ${signalCols.has('hard_gate_status') ? 'hard_gate_status' : 'NULL AS hard_gate_status'},
                ${signalCols.has('gate_result') ? 'gate_result' : 'NULL AS gate_result'}
              FROM premium_signals
              WHERE ${timestampExpr} >= @since
              ORDER BY ${timestampExpr} DESC, id DESC
              LIMIT 500
            `).all({ since: sinceTs });
            health.premium_signal_gate_health = summarizePremiumSignalGateHealth(gateRows);
            if (health.premium_signal_gate_health.status !== 'ok') {
              health.notes.push('premium_signals gate_result shows upstream provider issues');
            }
          } else {
            health.notes.push('premium_signals table missing; upstream signal gate health unavailable');
          }
        } catch (e) {
          health.notes.push(`premium_signals gate health unavailable: ${e.message}`);
        }
      } else {
        health.notes.push('sentiment database missing; upstream signal gate health unavailable');
      }

      if (tableNames.has('paper_missed_signal_attribution')) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const tradableMissedExpr = missedCols.has('tradable_missed')
          ? "SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END)"
          : "NULL";
        health.missed_attribution_coverage = paperDb.prepare(`
          SELECT
            COUNT(*) AS total_n,
            SUM(CASE WHEN baseline_price IS NOT NULL THEN 1 ELSE 0 END) AS baseline_n,
            SUM(CASE WHEN status = 'baseline_missing' THEN 1 ELSE 0 END) AS baseline_missing_n,
            SUM(CASE WHEN pnl_5m IS NOT NULL THEN 1 ELSE 0 END) AS pnl_5m_n,
            ${tradableMissedExpr} AS tradable_missed_n
          FROM paper_missed_signal_attribution
          WHERE COALESCE(signal_ts, created_event_ts, baseline_ts, 0) >= @since
        `).get({ since: sinceTs });
      }

      const warnReasons = [];
      if ((health.counters.entry_quote_fail_n || 0) > 0) warnReasons.push('entry_quote_failures_present');
      if ((health.counters.smart_entry_no_price_n || 0) > 5) warnReasons.push('smart_entry_no_price_spike');
      if ((health.counters.exit_quote_fail_n || 0) > 0) warnReasons.push('exit_quote_failures_present');
      if (health.signal_source_freshness_health?.fail_closed) warnReasons.push('signal_source_freshness_fail_closed');
      if (health.signal_source_freshness_health?.status === 'stale_warn') warnReasons.push('signal_source_freshness_warn');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.rate_limited_n || 0) > 0) warnReasons.push('premium_signal_provider_rate_limited');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.invalid_api_key_n || 0) > 0) warnReasons.push('premium_signal_provider_auth_failed');
      if (health.premium_signal_gate_health && (health.premium_signal_gate_health.counters.unknown_data_blocked_n || 0) > 0) warnReasons.push('premium_signal_unknown_data_blocks_present');
      if (health.fail_modes.fail_soft_enrichment.length > 0) warnReasons.push('external_alpha_degraded');
      if (health.open_exit_quote_risk && (health.open_exit_quote_risk.open_with_failures_n || 0) > 0) warnReasons.push('open_positions_have_exit_quote_failures');
      if (health.missed_attribution_coverage && health.missed_attribution_coverage.total_n > 0) {
        const baselinePct = (health.missed_attribution_coverage.baseline_n || 0) / health.missed_attribution_coverage.total_n;
        if (baselinePct < 0.8) warnReasons.push('missed_attribution_baseline_coverage_low');
      }
      if (warnReasons.length) {
        health.status = warnReasons.some((reason) => reason.includes('entry_quote') || reason.includes('open_positions')) ? 'warn' : 'degraded';
        health.warn_reasons = warnReasons;
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(health, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
      try { if (signalDb) signalDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/raw-dog-discovery') {
    if (!checkAuth(req, url, res)) return;
    try {
      const liveRequested = ['1', 'true', 'yes', 'on'].includes(String(url.searchParams.get('live') || '').toLowerCase())
        || ['0', 'false', 'no', 'off'].includes(String(url.searchParams.get('materialized') || '').toLowerCase());
      const coverageTargetPct = boundedIntParam(url, 'coverage_target_pct', 80, 0, 100);
      const includeRows = ['1', 'true', 'yes', 'on'].includes(String(url.searchParams.get('include_rows') || '').toLowerCase())
        || ['rows', 'raw_dogs', 'full'].includes(String(url.searchParams.get('include') || '').toLowerCase());
      const rawDogRowsLimit = boundedIntParam(url, 'rows_limit', 50000, 1, 50000);
      if (!liveRequested) {
        const requestedHours = parseWindowHoursParam(url.searchParams.get('window'))
          || boundedIntParam(url, 'hours', 24, 1, 168);
        const materializedLimit = boundedIntParam(url, 'limit', 50, 1, 500);
        const workerStatus = global.__rawDogDiscoveryWorkerStatus || null;
        if (workerStatus?.running) {
          res.writeHead(202, apiJsonHeaders());
          res.end(JSON.stringify({
            schema_version: 'raw_dog_discovery_api.v1',
            materialized: true,
            live_query: false,
            refresh_in_progress: true,
            source: 'raw_dog_discovery_worker_status',
            worker: {
              running: true,
              pid: workerStatus.pid || null,
              started_at: workerStatus.started_at || null,
              last_completed_at: workerStatus.last_completed_at || null,
              next_run_at: workerStatus.next_run_at || null,
            },
            summary: workerStatus.last_summary?.summary || null,
            diagnostics: {
              worker_last_error: workerStatus.last_error || null,
            },
            notes: {
              reason: 'raw dog discovery refresh is running in an isolated worker; API returns worker status instead of waiting on SQLite locks.',
              retry: 'Retry after the worker completes, or inspect / root health raw_dog_discovery_worker.',
            },
          }, null, 2));
          return;
        }
        const snapshot = readRawDogDiscoveryApiSnapshot({
          hours: requestedHours,
          limit: materializedLimit,
          includeRows,
          rowsLimit: rawDogRowsLimit,
          coverageTargetPct,
        });
        res.writeHead(snapshot.available ? 200 : 202, apiJsonHeaders());
        res.end(JSON.stringify(snapshot, null, 2));
        return;
      }
      const guard = livePaperQueryGuard(url, 'raw_dog_discovery_live', {
        defaultHours: 1,
        maxHours: 2,
        defaultLimit: 500,
        maxLimit: 1000,
      });
      if (!guard.allowed) {
        res.writeHead(400, apiJsonHeaders());
        res.end(JSON.stringify(guard, null, 2));
        return;
      }
      const signalDb = getDb();
      const sinceTs = guard.since_ts;
      const limit = guard.limit;
      const nowTs = parseUnixishTime(url.searchParams.get('now_ts')) || Math.floor(Date.now() / 1000);
      const horizonSec = boundedIntParam(url, 'horizon_sec', 7200, 300, 24 * 3600);
      const baselineMaxLagSec = boundedIntParam(url, 'baseline_max_lag_sec', 300, 0, 3600);
      const persist = !['0', 'false', 'no'].includes(String(url.searchParams.get('persist') || '1').toLowerCase());
      const snapshot = buildRawDogDiscoverySnapshot({
        signalDb,
        sinceTs,
        limit,
        nowTs,
        horizonSec,
        baselineMaxLagSec,
        coverageTargetPct,
        persist,
      });
      const liveRawDogRows = includeRows
        ? (snapshot.report?.outcomes || [])
            .filter((row) => row
              && (row.raw_primary_tier === 'gold' || row.raw_primary_tier === 'silver')
              && row.observation_status === 'matured'
              && Boolean(row.kline_covered)
              && ['high', 'medium'].includes(String(row.baseline_confidence || ''))
              && Boolean(row.same_source_path)
              && !Boolean(row.outlier_flag)
              && Boolean(row.sustained_evaluable))
            .slice(0, rawDogRowsLimit)
        : [];
      res.writeHead(snapshot.available ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        schema_version: 'raw_dog_discovery_api.v1',
        materialized: false,
        live_query: true,
        live_guard: guard,
        ...snapshot,
        summary: snapshot.report?.summary || null,
        decision_funnel: snapshot.report?.decision_funnel || null,
        coverage: snapshot.report?.coverage || null,
        top_raw_dogs: snapshot.report?.top_raw_dogs || [],
        missed_raw_dogs: snapshot.report?.missed_raw_dogs || [],
        raw_dogs: liveRawDogRows,
        raw_dog_rows: {
          included: includeRows,
          limit: includeRows ? rawDogRowsLimit : 0,
          returned_event_rows: liveRawDogRows.length,
          rows_complete_against_summary: includeRows
            ? liveRawDogRows.length >= Number(snapshot.report?.summary?.raw_sustained_gold_silver_event_rows || 0)
            : null,
        },
        coverage_gap_tokens: snapshot.report?.coverage_gap_tokens || [],
        pending_outcomes: snapshot.report?.pending_outcomes || [],
        notes: {
          capture_definition: 'raw_dog_entered means a paper trade touched the raw sustained dog before peak; raw_dog_realized means the trade held to silver/gold peak. Entered alone is not capture.',
          censoring: 'Signals whose full horizon has not matured are right_censored_open and excluded from denominators.',
          denominator: 'Only high/medium baseline confidence, same-source path, non-outlier, sustained-evaluable outcomes enter the main raw dog denominator.',
          zero_denominator: '0/0 rates are null/undefined, never interpreted as 0%.',
        },
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }, null, 2));
    }
    return;
  } else if (url.pathname === '/api/paper/premium-signal-outcome-audit') {
    if (!checkAuth(req, url, res)) return;
    let signalDb;
    let paperDb;
    try {
      const sinceTs = reportSinceTs(url, '6h');
      const limit = boundedIntParam(url, 'limit', 5000, 1, 20000);
      signalDb = getDb();
      if (!signalDb) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'sentiment database unavailable' }));
        return;
      }
      const signalTables = new Set(
        signalDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (!signalTables.has('premium_signals')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'premium_signals table not found' }));
        return;
      }
      const rawKlineHorizonSec = boundedIntParam(url, 'raw_kline_horizon_sec', 3600, 300, 24 * 3600);
      const rawDiscoveryHorizonSec = boundedIntParam(url, 'raw_discovery_horizon_sec', 7200, 300, 24 * 3600);
      const rawKlineBaselineMaxLagSec = boundedIntParam(url, 'raw_kline_baseline_max_lag_sec', 300, 0, 3600);
      const signalCols = getTableColumns(signalDb, 'premium_signals');
      const timestampExpr = signalCols.has('timestamp')
        ? "CASE WHEN timestamp > 1000000000000 THEN CAST(timestamp / 1000 AS INTEGER) ELSE CAST(timestamp AS INTEGER) END"
        : "0";
      const signalWhere = sinceTs && signalCols.has('timestamp')
        ? `WHERE ((timestamp > 1000000000000 AND timestamp >= @sinceMs) OR (timestamp <= 1000000000000 AND timestamp >= @since))`
        : (sinceTs ? `WHERE ${timestampExpr} >= @since` : '');
      const signalRows = signalDb.prepare(`
        SELECT
          id,
          symbol,
          token_ca,
          ${signalCols.has('timestamp') ? 'timestamp' : 'NULL AS timestamp'},
          ${timestampExpr} AS timestamp_sec,
          ${signalCols.has('created_at') ? 'created_at' : 'NULL AS created_at'},
          ${signalCols.has('signal_type') ? 'signal_type' : 'NULL AS signal_type'},
          ${signalCols.has('market_cap') ? 'market_cap' : 'NULL AS market_cap'},
          ${signalCols.has('hard_gate_status') ? 'hard_gate_status' : 'NULL AS hard_gate_status'},
          ${signalCols.has('gate_result') ? 'gate_result' : 'NULL AS gate_result'},
          ${signalCols.has('ai_action') ? 'ai_action' : 'NULL AS ai_action'},
          ${signalCols.has('downstream_trade_id') ? 'downstream_trade_id' : 'NULL AS downstream_trade_id'},
          ${signalCols.has('downstream_lifecycle_id') ? 'downstream_lifecycle_id' : 'NULL AS downstream_lifecycle_id'}
        FROM premium_signals
        ${signalWhere}
        ORDER BY ${timestampExpr} DESC, id DESC
        LIMIT @limit
      `).all(sinceTs ? { since: sinceTs, sinceMs: sinceTs * 1000, limit } : { limit });
      let klineRows = [];
      const klineDiagnostics = {
        available: false,
        table: 'kline_1m',
        rows: 0,
        unique_signal_tokens: 0,
        queried_tokens: 0,
        query_chunks: 0,
        coverage_window: {
          since_ts: sinceTs,
          until_ts: Math.floor(Date.now() / 1000) + rawKlineHorizonSec,
          horizon_sec: rawKlineHorizonSec,
          baseline_max_lag_sec: rawKlineBaselineMaxLagSec,
        },
      };
      if (signalTables.has('kline_1m')) {
        const klineCols = getTableColumns(signalDb, 'kline_1m');
        if (klineCols.has('token_ca') && klineCols.has('timestamp') && klineCols.has('high') && klineCols.has('low') && klineCols.has('close')) {
          const uniqueSignalTokens = [...new Set(signalRows.map((row) => String(row.token_ca || '').trim()).filter(Boolean))];
          klineDiagnostics.available = true;
          klineDiagnostics.unique_signal_tokens = uniqueSignalTokens.length;
          const klineSinceTs = sinceTs || Math.floor(Date.now() / 1000) - 6 * 3600;
          const klineUntilTs = Math.floor(Date.now() / 1000) + rawKlineHorizonSec;
          klineDiagnostics.coverage_window.since_ts = klineSinceTs;
          klineDiagnostics.coverage_window.until_ts = klineUntilTs;
          const chunkSize = 250;
          for (let i = 0; i < uniqueSignalTokens.length; i += chunkSize) {
            const chunk = uniqueSignalTokens.slice(i, i + chunkSize);
            if (!chunk.length) continue;
            const placeholders = chunk.map(() => '?').join(',');
            const rows = signalDb.prepare(`
              SELECT
                token_ca,
                timestamp,
                open,
                high,
                low,
                close,
                ${klineCols.has('source') ? 'source' : 'NULL AS source'}
              FROM kline_1m
              WHERE timestamp >= ?
                AND timestamp <= ?
                AND token_ca IN (${placeholders})
              ORDER BY token_ca ASC, timestamp ASC
            `).all(klineSinceTs, klineUntilTs, ...chunk);
            klineRows.push(...rows);
            klineDiagnostics.query_chunks += 1;
          }
          klineDiagnostics.queried_tokens = uniqueSignalTokens.length;
          klineDiagnostics.rows = klineRows.length;
        } else {
          klineDiagnostics.note = 'kline_1m missing required token_ca/timestamp/high/low/close columns';
        }
      } else {
        klineDiagnostics.note = 'kline_1m table missing';
      }

      const paperDbPath = getPaperDbPath();
      let paperTrades = [];
      let missedAttributions = [];
      if (fs.existsSync(paperDbPath)) {
        paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
        const paperTables = new Set(
          paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
        );
        if (paperTables.has('paper_trades')) {
          const tradeCols = getTableColumns(paperDb, 'paper_trades');
          const trustedPeakExpr = trustedTradePeakSqlExpr(tradeCols);
          const markPeakExpr = markTradePeakSqlExpr(tradeCols);
          const tradeWhere = sinceTs ? 'WHERE COALESCE(entry_ts, exit_ts, 0) >= @since OR COALESCE(exit_ts, 0) >= @since' : '';
          paperTrades = paperDb.prepare(`
            SELECT
              id,
              token_ca,
              symbol,
              entry_ts,
              exit_ts,
              pnl_pct,
              ${trustedPeakExpr} AS peak_pnl,
              ${markPeakExpr} AS mark_peak_pnl,
              ${tradeCols.has('peak_trust_status') ? 'peak_trust_status' : "'legacy_peak' AS peak_trust_status"},
              position_size_sol,
              signal_route,
              ${tradeCols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode'},
              ${tradeCols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json'},
              ${tradeCols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json'}
            FROM paper_trades
            ${tradeWhere}
            ORDER BY entry_ts DESC, id DESC
            LIMIT @limit
          `).all(sinceTs ? { since: sinceTs, limit } : { limit });
        }
        if (paperTables.has('paper_missed_signal_attribution')) {
          const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
          const missedColumn = (name, fallback = 'NULL') => missedCols.has(name) ? `m.${name}` : fallback;
          const missedEventTsExpr = `COALESCE(${[
            missedCols.has('created_event_ts') ? 'm.created_event_ts' : null,
            missedCols.has('signal_ts') ? 'm.signal_ts' : null,
            missedCols.has('baseline_ts') ? 'm.baseline_ts' : null,
            '0',
          ].filter(Boolean).join(', ')})`;
          const missedWhereTsExpr = missedCols.has('created_event_ts')
            ? 'm.created_event_ts'
            : (missedCols.has('signal_ts') ? 'm.signal_ts' : missedEventTsExpr);
          const maxPnlExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
          const markPnlExpr = markMissedPeakSqlExpr(missedCols, 'm');
          const hasSourceResonance = paperTables.has('source_resonance_candidates');
          const sourceCols = hasSourceResonance ? getTableColumns(paperDb, 'source_resonance_candidates') : new Set();
          const missedSignalTs = missedCols.has('signal_ts') ? 'COALESCE(m.signal_ts, 0)' : '0';
          const sourceSignalTs = sourceCols.has('signal_ts') ? 'COALESCE(sr.signal_ts, 0)' : '0';
          const sourceUpdatedTs = sourceCols.has('updated_at') ? 'COALESCE(sr.updated_at, 0)' : '0';
          const sourceMatchOrder = `
            CASE
              WHEN ${sourceSignalTs} = ${missedSignalTs} THEN 0
              WHEN ${sourceSignalTs} <= ${missedSignalTs} THEN 1
              ELSE 2
            END,
            ABS(${sourceSignalTs} - ${missedSignalTs}) ASC,
            ${sourceUpdatedTs} DESC
          `;
          const sourceColumn = (name, alias = name, fallback = 'NULL') => (
            hasSourceResonance && sourceCols.has(name)
              ? `sr.${name} AS ${alias}`
              : `${fallback} AS ${alias}`
          );
          const sourceResonanceJoin = hasSourceResonance
            ? 'LEFT JOIN source_resonance_candidates sr ON sr.token_ca = m.token_ca'
            : '';
          const sourceMatchRank = hasSourceResonance ? `
              ROW_NUMBER() OVER (
                PARTITION BY
                  m.token_ca,
                  ${missedColumn('signal_id')},
                  ${missedColumn('signal_ts')},
                  ${missedEventTsExpr},
                  ${missedColumn('component')},
                  ${missedColumn('reject_reason')}
                ORDER BY ${sourceMatchOrder}
              ) AS source_match_rn` : '1 AS source_match_rn';
          const sourceResonanceSelect = `
              ${sourceColumn('cohort', 'source_resonance_cohort')},
              ${sourceColumn('resonance_level', 'source_resonance_level')},
              ${sourceColumn('resonance_score', 'source_resonance_score')},
              ${sourceColumn('gmgn_pre_seen', 'gmgn_pre_seen', '0')},
              ${sourceColumn('gmgn_lead_time_sec', 'gmgn_lead_time_sec')},`;
          const quoteCleanExpr = missedCols.has('tradable_missed')
            ? `CASE
                WHEN COALESCE(m.tradable_missed, 0) = 1
                 AND COALESCE(${missedCols.has('would_stop_before_peak') ? 'm.would_stop_before_peak' : '0'}, 0) != 1
                THEN 1 ELSE 0
              END`
            : 'NULL';
          missedAttributions = paperDb.prepare(`
            WITH
            raw AS (
              SELECT
                m.token_ca,
                ${missedColumn('symbol')} AS symbol,
                ${missedColumn('signal_id')} AS signal_id,
                ${missedColumn('signal_ts')} AS signal_ts,
                ${missedColumn('route')} AS route,
                ${missedColumn('component')} AS component,
                ${missedColumn('decision')} AS decision,
                ${missedColumn('reject_reason')} AS reject_reason,
                ${missedColumn('tradability_status')} AS tradability_status,
                ${missedColumn('tradability_reason')} AS tradability_reason,
                ${missedColumn('tradable_missed')} AS tradable_missed,
                ${missedColumn('tradable_peak_pnl')} AS tradable_peak_pnl,
                ${missedColumn('would_stop_before_peak')} AS would_stop_before_peak,
                ${missedColumn('first_tradable_pnl')} AS first_tradable_pnl,
                ${quoteCleanExpr} AS quote_clean,
                ${maxPnlExpr} AS row_max_pnl,
                ${markPnlExpr} AS row_mark_pnl,
                CASE
                  WHEN ${maxPnlExpr} >= 0.25 THEN 'trusted_peak'
                  WHEN ${markPnlExpr} >= 0.25 THEN 'mark_only_peak_untrusted'
                  ELSE 'sub25_or_unknown'
                END AS peak_trust_status,
                ${missedEventTsExpr} AS event_ts,
                CASE
                  WHEN m.component = 'source_resonance_probe' THEN 'source_resonance_tiny_probe'
                  WHEN m.component = 'hard_gate_pass_probe' THEN 'hard_gate_pass_tiny_probe'
                  WHEN m.component = 'pre_pass_resonance_probe' THEN 'pre_pass_resonance_tiny_probe'
                  WHEN m.component IN ('lotto_upstream_realtime_scout', 'lotto_upstream_realtime_probe') THEN 'lotto_upstream_realtime_tiny_scout'
                  ELSE NULL
                END AS entry_mode_candidate,
                ${sourceResonanceSelect}
                ${sourceMatchRank}
              FROM paper_missed_signal_attribution m
              ${sourceResonanceJoin}
              ${sinceTs ? `WHERE ${missedWhereTsExpr} >= @since` : ''}
            ),
            base AS (
              SELECT
                raw.*,
                ROW_NUMBER() OVER (
                  PARTITION BY raw.token_ca
                  ORDER BY raw.row_max_pnl DESC, raw.event_ts DESC
                ) AS rn
              FROM raw
              WHERE raw.source_match_rn = 1
            ),
            counts AS (
              SELECT token_ca, COUNT(*) AS n, MAX(row_max_pnl) AS max_pnl
              FROM base
              GROUP BY token_ca
            )
            SELECT
              base.*,
              counts.n,
              counts.max_pnl
            FROM base
            JOIN counts ON counts.token_ca = base.token_ca
            WHERE base.rn = 1
            ORDER BY counts.max_pnl DESC, base.event_ts DESC
            LIMIT @limit
          `).all(sinceTs ? { since: sinceTs, limit } : { limit });
        }
      }

      const audit = buildPremiumSignalOutcomeAudit({
        signals: signalRows,
        paperTrades,
        missedAttributions,
        klineRows,
        klineOptions: {
          horizonSec: rawKlineHorizonSec,
          baselineMaxLagSec: rawKlineBaselineMaxLagSec,
        },
        sinceTs,
      });
      const rawDiscovery = buildRawSignalOutcomeReport({
        signals: signalRows,
        paperTrades,
        klineRows,
        nowTs: Math.floor(Date.now() / 1000),
        horizonSec: rawDiscoveryHorizonSec,
        baselineMaxLagSec: rawKlineBaselineMaxLagSec,
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...audit,
        raw_discovery: {
          summary: rawDiscovery.summary,
          coverage: rawDiscovery.coverage,
          top_raw_dogs: rawDiscovery.top_raw_dogs,
          missed_raw_dogs: rawDiscovery.missed_raw_dogs,
          coverage_gap_tokens: rawDiscovery.coverage_gap_tokens,
          pending_outcomes: rawDiscovery.pending_outcomes,
        },
        db_path: paperDbPath,
        premium_signal_query_limit: limit,
        kline_diagnostics: klineDiagnostics,
        notes: {
          audit_goal: 'Compare upstream premium signal raw kline and market-cap outcomes with paper trade coverage.',
          missed_recovery_difference: 'missed-recovery-summary only covers paper_missed_signal_attribution; this endpoint starts from premium_signals and therefore exposes coverage gaps.',
          raw_kline_difference: 'raw_kline_* metrics measure discovery from signal to 1m kline path high; quote-clean metrics still measure executable capture separately.',
          raw_discovery_difference: 'raw_discovery uses a matured-only, sustained-only denominator; entered != captured unless held_to_silver/gold is true.',
        },
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/closed-loop-report') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let signalDb;
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const queryStartedAt = Date.now();
      const nowSec = Math.floor(Date.now() / 1000);
      const limit = boundedIntParam(url, 'limit', 30, 1, 200);
      const includeRaw72h = String(url.searchParams.get('include_raw_72h') || '').toLowerCase() === '1';
      const includeHeavy72hMissed = includeRaw72h
        || String(url.searchParams.get('include_72h_missed') || '').toLowerCase() === '1';
      const includeHeavy72hProbes = includeRaw72h
        || String(url.searchParams.get('include_72h_probes') || '').toLowerCase() === '1';
      const includeTiming = String(url.searchParams.get('include_timing') || '').toLowerCase() === '1';
      const paperDbTimeoutMs = boundedIntParam(url, 'paper_db_timeout_ms', 750, 0, 5000);
      const timings = includeTiming ? {} : null;
      const windows = [6, 24];
      signalDb = getDb();
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: paperDbTimeoutMs });
      const byWindow = {};
      for (const hours of windows) {
        const sinceTs = nowSec - hours * 3600;
        byWindow[`${hours}h`] = buildClosedLoopWindowReport({
          signalDb,
          paperDb,
          sinceTs,
          limit,
          timings,
          timingPrefix: `${hours}h`,
        });
      }
      const report72h = buildClosedLoopWindowReport({
        signalDb,
        paperDb,
        sinceTs: nowSec - 72 * 3600,
        limit,
        includeMissedSummary: includeHeavy72hMissed,
        includeMissedDetails: includeRaw72h,
        includeSourceSummary: includeRaw72h,
        includeProbeSummary: includeHeavy72hProbes,
        includePaperPnlDetails: includeRaw72h,
        includeDecisionEventDetails: includeRaw72h,
        timings,
        timingPrefix: '72h',
      });
      const responseBody = {
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        query_ms: Date.now() - queryStartedAt,
        filters: {
          windows: ['6h', '24h'],
          decision_window: '72h',
          paper_db_timeout_ms: paperDbTimeoutMs,
          tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
          quote_clean_definition: 'tradable_missed=1 and would_stop_before_peak!=1',
        },
        guardrails: {
          execution_scope: 'paper_only',
          live_execution_requires_env: 'PREMIUM_LIVE_EXECUTION_ENABLED=true',
          blocked_entry_modes_remain_blocked: true,
        },
        windows: byWindow,
        decision_72h: buildClosedLoopDecision(report72h),
        notes: {
          endpoint_goal: '6h/24h closed-loop report for premium signal coverage, paper probe fills/rejects, missed dog blockers, and 72h paper-only decision rules.',
          final_blocker_rule: 'top_missed_dogs exposes exactly one route/component/reason blocker per unique token, chosen by highest observed missed PnL in the window.',
          raw_72h: includeRaw72h ? 'included' : 'omitted by default; pass include_raw_72h=1 for the heavier 72h detail payload',
          missed_72h: includeHeavy72hMissed ? 'included' : 'omitted by default; pass include_72h_missed=1 for the heavier 72h missed-dog count',
          probes_72h: includeHeavy72hProbes ? 'included' : 'omitted by default; pass include_72h_probes=1 for the heavier 72h probe coverage count',
        },
      };
      if (includeRaw72h) responseBody.raw_72h = report72h;
      if (includeTiming) responseBody.timings_ms = timings;
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(responseBody, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/review-snapshot/refresh-status') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, apiJsonHeaders());
    res.end(JSON.stringify({
      schema_version: 'paper_review_snapshot_refresh_status_api.v1',
      generated_at: new Date().toISOString(),
      runner_status: readPaperReviewSnapshotRefreshStatus(),
      promotion_allowed: false,
      strategy_change_allowed: false,
      automatic_runtime_change_allowed: false,
      paper_enablement_allowed: false,
    }, null, 2));
    return;
  } else if (url.pathname === '/api/paper/review-snapshot/refresh') {
    if (!checkAuth(req, url, res)) return;
    if (!requirePost(req, res)) return;
    try {
      const result = triggerPaperReviewSnapshotRefresh(url);
      res.writeHead(result.accepted ? 202 : 409, apiJsonHeaders());
      res.end(JSON.stringify(result, null, 2));
    } catch (error) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({
        error: error.message,
        error_code: 'paper_review_snapshot_refresh_failed',
        promotion_allowed: false,
        strategy_change_allowed: false,
        automatic_runtime_change_allowed: false,
        paper_enablement_allowed: false,
      }, null, 2));
    }
    return;
  } else if (url.pathname === '/api/paper/review-snapshot') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let signalDb;
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const generatedAt = new Date().toISOString();
      const untilTs = Math.floor(Date.now() / 1000);
      const sinceTs = reportSinceTs(url, '8h');
      const windowLabel = String(url.searchParams.get('window') || url.searchParams.get('hours') || '8h');
      const limit = boundedIntParam(url, 'limit', 120, 1, 500);
      const tradeLimit = boundedIntParam(url, 'trade_limit', 2000, 1, 20000);
      const includeDetails = !['0', 'false', 'no'].includes(String(url.searchParams.get('include_details') || '1').toLowerCase());
      const includeSignals = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_signals') || '0').toLowerCase());
      const includeClosedLoop = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_closed_loop') || '0').toLowerCase());
      const includeMissedSummary = !['0', 'false', 'no'].includes(String(url.searchParams.get('include_missed_summary') || '1').toLowerCase());
      const includeSourceSummary = !['0', 'false', 'no'].includes(String(url.searchParams.get('include_source_summary') || '1').toLowerCase());
      const includeProbeSummary = !['0', 'false', 'no'].includes(String(url.searchParams.get('include_probe_summary') || '1').toLowerCase());
      const includeTableStats = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_table_stats') || '0').toLowerCase());
      const includeLatency = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_latency') || '0').toLowerCase());
      const freeze = ['1', 'true', 'yes'].includes(String(url.searchParams.get('freeze') || '0').toLowerCase());
      const persist = freeze || !['0', 'false', 'no'].includes(String(url.searchParams.get('persist') || '1').toLowerCase());
      const materializeLive = ['1', 'true', 'yes', 'on'].includes(String(url.searchParams.get('materialize_live') || '').toLowerCase());
      const materialized = ['1', 'true', 'yes'].includes(String(url.searchParams.get('materialized') || '').toLowerCase())
        || (includeClosedLoop && !includeDetails && !includeProbeSummary && !includeSourceSummary && String(windowLabel).match(/^(8|24)$/));
      if (materialized) {
        const liveSnapshot = readLivePaperReview(windowLabel);
        if (liveSnapshot && !liveSnapshot.error) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({
            ...liveSnapshot,
            materialized: true,
            materialized_path: livePaperReviewPath(windowLabel),
          }, null, 2));
          return;
        }
        res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          available: false,
          materialized: true,
          reason: liveSnapshot?.error || 'materialized_snapshot_not_ready',
          path: livePaperReviewPath(windowLabel),
        }, null, 2));
        return;
      }
      const paperDbTimeoutMs = boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000);
      const timings = {};

      signalDb = includeSignals && includeClosedLoop ? getDb() : null;
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: paperDbTimeoutMs });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );

      const closedLoop = includeClosedLoop
        ? buildClosedLoopWindowReport({
          signalDb,
          paperDb,
          sinceTs,
          limit,
          includeMissedSummary,
          includeMissedDetails: includeDetails,
          includeSourceSummary,
          includeProbeSummary,
          includePaperPnlDetails: true,
          includeDecisionEventDetails: includeDetails,
          timings,
          timingPrefix: 'review',
        })
        : {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          premium_signals: { available: false, skipped: true, skip_reason: 'live_fast_snapshot' },
          probes: skippedClosedLoopProbeSummary('live_fast_snapshot'),
          source_resonance: skippedClosedLoopSourceSummary('live_fast_snapshot'),
          missed_dogs: skippedClosedLoopMissedDogSummary('live_fast_snapshot'),
        };

      let registrySummary = null;
      try {
        registrySummary = summarizeEntryModeRegistry(loadEntryModeRegistry());
      } catch (error) {
        registrySummary = { error: error.message };
      }

      const tradeRows = loadReviewTradeRows(paperDb, tableNames, sinceTs, tradeLimit, {
        fastRecent: !includeClosedLoop,
      });
      const tradeReview = buildTradeReviewSummary(tradeRows);
      const snapshot = buildPaperReviewSnapshot({
        generatedAt,
        commit: runtimeCommitFingerprint(),
        policyFingerprint: reviewPolicyFingerprint(registrySummary),
        window: {
          label: windowLabel,
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          until_ts: untilTs,
          until_iso: new Date(untilTs * 1000).toISOString(),
        },
        dbPath: paperDbPath,
        closedLoop,
        tradeReview,
        latencySummary: includeLatency ? buildReviewLatencySummary(paperDb, tableNames, sinceTs) : [],
        tableCoverage: buildReviewTableCoverage(paperDb, tableNames, { includeStats: includeTableStats }),
        sourceHealth: buildReviewHealthRows(paperDb, tableNames, 'source_resonance_health'),
        externalAlphaHealth: buildReviewHealthRows(paperDb, tableNames, 'external_alpha_health'),
        registrySummary,
        notes: [
          'This snapshot is persisted so review windows can be compared across commits and policy fingerprints.',
          freeze
            ? 'freeze=1 requested: this artifact is the immutable review source for follow-up discussion.'
            : 'Pass freeze=1 when starting a review so every number has a stable snapshot_id.',
          'All execution metrics in this endpoint are paper-trader metrics; live execution remains out of scope.',
          'Missed dog counts use quote-clean fields when available, and should not be mixed with theoretical mark-only peaks.',
          includeClosedLoop
            ? 'Heavy closed-loop scans were enabled for this snapshot.'
            : 'Heavy closed-loop scans are skipped by default for live safety; pass include_closed_loop=1 for explicit heavy review.',
          includeTableStats
            ? 'Full table coverage stats were enabled for this snapshot.'
            : 'Full table coverage stats are skipped by default for live safety; pass include_table_stats=1 for explicit table scans.',
          includeSignals
            ? 'Premium signal table scan was requested; it only runs with include_closed_loop=1.'
            : 'Premium signal table scan is skipped by default for live safety; pass include_signals=1 for an explicit heavy scan.',
        ],
      });

      let persistedFiles = null;
      if (persist) {
        persistedFiles = writePaperReviewSnapshotFiles(snapshot, { dir: getPaperReviewDir() });
      }
      let liveMaterializedPath = null;
      if (materializeLive) {
        const materializedHours = Math.max(1, Math.min(24, Number.parseInt(String(windowLabel).replace(/[^0-9]/g, ''), 10) || 24));
        liveMaterializedPath = writeLivePaperReviewSnapshot(materializedHours, snapshot);
      }

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...snapshot,
        freeze,
        persisted_files: persistedFiles,
        materialize_live: materializeLive,
        live_materialized_path: liveMaterializedPath,
        query_ms: Date.now() - startedAt,
        timings_ms: timings,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/learning-audit') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const sinceTs = reportSinceTs(url, '24h');
      const tradeLimit = boundedIntParam(url, 'trade_limit', 500, 1, 5000);
      const limit = boundedIntParam(url, 'limit', 50, 1, 200);
      const includePathSamples = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_path_samples') || '0').toLowerCase());
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const trades = loadReviewTradeRows(paperDb, tableNames, sinceTs, tradeLimit, { fastRecent: true });
      const pathSamplesByTrade = includePathSamples
        ? loadPathSamplesByTrade(paperDb, tableNames, trades.map((row) => row.id), boundedIntParam(url, 'path_sample_limit', 100, 1, 500))
        : new Map();
      const tradeCols = tableNames.has('paper_trades') ? getTableColumns(paperDb, 'paper_trades') : new Set();
      const tierExpr = tradeCols.has('capital_tier')
        ? "COALESCE(capital_tier, 'unknown')"
        : `CASE
            WHEN COALESCE(position_size_sol, 0) > 0 AND COALESCE(position_size_sol, 0) <= 0.005 THEN 'tiny_probe'
            WHEN COALESCE(position_size_sol, 0) >= 0.02 THEN 'stage1_main'
            ELSE 'unknown'
          END`;
      const regimeExpr = tradeCols.has('regime_tag') ? "COALESCE(regime_tag, 'unknown')" : "COALESCE(market_regime, 'unknown')";
      const trustedPeakExpr = trustedTradePeakSqlExpr(tradeCols);
      const capitalLifecycle = tableNames.has('paper_trades') ? paperDb.prepare(`
        SELECT
          ${tierExpr} AS capital_tier,
          COALESCE(entry_mode, strategy_stage, 'unknown') AS entry_mode,
          ${regimeExpr} AS regime_tag,
          COUNT(*) AS trades,
          SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
          AVG(pnl_pct) AS avg_pnl,
          AVG(${trustedPeakExpr}) AS avg_peak,
          AVG(CASE WHEN pnl_pct IS NOT NULL THEN MAX(${trustedPeakExpr} - pnl_pct, 0) ELSE NULL END) AS avg_giveback
        FROM paper_trades
        WHERE COALESCE(entry_ts, exit_ts, 0) >= @since
        GROUP BY capital_tier, entry_mode, regime_tag
        ORDER BY trades DESC, avg_giveback DESC
        LIMIT @limit
      `).all({ since: sinceTs || 0, limit }) : [];
      const modeRows = tableNames.has('paper_trades') ? paperDb.prepare(`
        SELECT
          COALESCE(entry_mode, 'unknown') AS entry_mode,
          COUNT(*) AS fills,
          SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
          AVG(pnl_pct) AS avg_pnl
        FROM paper_trades
        WHERE COALESCE(entry_ts, exit_ts, 0) >= @since
        GROUP BY COALESCE(entry_mode, 'unknown')
        ORDER BY fills DESC
      `).all({ since: sinceTs || 0 }) : [];
      let executableMissed = { available: false };
      if (tableNames.has('paper_missed_signal_attribution')) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const peakExpr = `COALESCE(${[
          missedCols.has('executable_peak_pnl') ? 'executable_peak_pnl' : null,
          missedCols.has('quote_clean_peak_pnl') ? 'quote_clean_peak_pnl' : null,
          missedCols.has('tradable_peak_pnl') ? 'tradable_peak_pnl' : null,
          missedCols.has('theoretical_peak_pnl') ? 'theoretical_peak_pnl' : null,
          missedCols.has('max_pnl_recorded') ? 'max_pnl_recorded' : null,
          '0',
        ].filter(Boolean).join(', ')})`;
        const executableExpr = missedCols.has('executable_peak_pnl') ? 'executable_peak_pnl IS NOT NULL' : '0';
        const quoteCleanExpr = missedCols.has('quote_clean_peak_pnl') ? 'quote_clean_peak_pnl IS NOT NULL' : '0';
        const executableSortExpr = missedCols.has('executable_peak_pnl') && missedCols.has('quote_clean_peak_pnl')
          ? 'CASE WHEN executable_peak_pnl IS NOT NULL THEN 0 WHEN quote_clean_peak_pnl IS NOT NULL THEN 1 ELSE 2 END'
          : missedCols.has('executable_peak_pnl')
            ? 'CASE WHEN executable_peak_pnl IS NOT NULL THEN 0 ELSE 2 END'
            : (missedCols.has('quote_clean_peak_pnl') ? 'CASE WHEN quote_clean_peak_pnl IS NOT NULL THEN 1 ELSE 2 END' : '2');
        executableMissed = {
          available: true,
          summary: paperDb.prepare(`
            SELECT
              COUNT(DISTINCT token_ca) AS unique_tokens,
              COUNT(DISTINCT CASE WHEN ${executableExpr} THEN token_ca ELSE NULL END) AS executable_unique,
              COUNT(DISTINCT CASE WHEN ${quoteCleanExpr} THEN token_ca ELSE NULL END) AS quote_clean_unique,
              MAX(${peakExpr}) AS max_peak
            FROM paper_missed_signal_attribution
            WHERE COALESCE(created_event_ts, signal_ts, baseline_ts, 0) >= @since
          `).get({ since: sinceTs || 0 }),
          top: paperDb.prepare(`
            SELECT token_ca, symbol, route, component, reject_reason,
                   ${peakExpr} AS ranked_peak,
                   ${missedCols.has('theoretical_peak_pnl') ? 'theoretical_peak_pnl' : 'NULL AS theoretical_peak_pnl'},
                   ${missedCols.has('quote_clean_peak_pnl') ? 'quote_clean_peak_pnl' : 'NULL AS quote_clean_peak_pnl'},
                   ${missedCols.has('executable_peak_pnl') ? 'executable_peak_pnl' : 'NULL AS executable_peak_pnl'},
                   ${missedCols.has('executable_peak_source') ? 'executable_peak_source' : 'NULL AS executable_peak_source'}
            FROM paper_missed_signal_attribution
            WHERE COALESCE(created_event_ts, signal_ts, baseline_ts, 0) >= @since
            ORDER BY ${executableSortExpr} ASC, ranked_peak DESC
            LIMIT @limit
          `).all({ since: sinceTs || 0, limit }).map((row) => ({
            ...row,
            ranked_peak_pct: roundNumber(Number(row.ranked_peak || 0) * 100, 2),
            theoretical_peak_pct: row.theoretical_peak_pnl == null ? null : roundNumber(Number(row.theoretical_peak_pnl) * 100, 2),
            quote_clean_peak_pct: row.quote_clean_peak_pnl == null ? null : roundNumber(Number(row.quote_clean_peak_pnl) * 100, 2),
            executable_peak_pct: row.executable_peak_pnl == null ? null : roundNumber(Number(row.executable_peak_pnl) * 100, 2),
          })),
        };
      }
      const oldTs = Math.floor(Date.now() / 1000) - 30 * 24 * 3600;
      const missedArchiveTs = Math.floor(Date.now() / 1000) - 24 * 3600;
      const aging = {
        decision_events_older_30d: tableNames.has('paper_decision_events')
          ? paperDb.prepare('SELECT COUNT(*) AS n FROM paper_decision_events WHERE event_ts < @oldTs').get({ oldTs }).n
          : null,
        complete_missed_attribution_older_24h: tableNames.has('paper_missed_signal_attribution')
          ? paperDb.prepare("SELECT COUNT(*) AS n FROM paper_missed_signal_attribution WHERE COALESCE(created_event_ts, signal_ts, baseline_ts, 0) < @missedArchiveTs AND COALESCE(status, '') = 'complete'").get({ missedArchiveTs }).n
          : null,
        cleanup_policy: {
          quarantine_hard_loss_gate_ttl_days: 7,
          missed_attribution_archive_after_hours: 24,
          decision_event_cold_storage_after_days: 30,
          live_action: 'report_only',
        },
      };
      const stage1Rows = capitalLifecycle.filter((row) => String(row.capital_tier || '') === 'stage1_main');
      const stage1Trades = stage1Rows.reduce((sum, row) => sum + Number(row.trades || 0), 0);
      const stage1AvgPnl = stage1Trades
        ? stage1Rows.reduce((sum, row) => sum + Number(row.avg_pnl || 0) * Number(row.trades || 0), 0) / stage1Trades
        : null;
      const stage1Governance = {
        scope: 'report_only',
        capital_tier: 'stage1_main',
        trades: stage1Trades,
        avg_pnl_pct: stage1AvgPnl == null ? null : roundNumber(stage1AvgPnl * 100, 2),
        warnings: [
          stage1Trades > 0 && stage1AvgPnl != null && stage1AvgPnl < 0 ? 'stage1_negative_window_pnl' : null,
          stage1Trades === 0 ? 'stage1_no_samples_in_window' : null,
        ].filter(Boolean),
        next_actions: [
          'compare_24h_7d_14d_stage1_by_commit_before_size_changes',
          'do_not_apply_tiny_trail_parameters_to_stage1_without_stage1_counterfactual',
        ],
      };
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        window: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
        },
        time_to_quote: buildReviewLatencySummary(paperDb, tableNames, sinceTs),
        shadow_trail_audit: buildShadowTrailAudit({ trades, pathSamplesByTrade }),
        fast_fail_counterfactual: buildFastFailCounterfactualAudit({ trades, pathSamplesByTrade }),
        executable_missed_dog_audit: executableMissed,
        sample_governance: buildSampleGovernance(modeRows),
        capital_tier_lifecycle: capitalLifecycle.map((row) => ({
          ...row,
          win_rate_pct: row.trades ? roundNumber(Number(row.wins || 0) / Number(row.trades) * 100, 1) : null,
          avg_pnl_pct: row.avg_pnl == null ? null : roundNumber(Number(row.avg_pnl) * 100, 2),
          avg_peak_pct: row.avg_peak == null ? null : roundNumber(Number(row.avg_peak) * 100, 2),
          avg_giveback_pct: row.avg_giveback == null ? null : roundNumber(Number(row.avg_giveback) * 100, 2),
        })),
        stage1_governance: stage1Governance,
        table_aging: aging,
        notes: {
          paper_only: true,
          scope: 'learning audit: latency/drift, shadow trail, fast-fail counterfactual, executable missed dogs, sample governance, capital tier lifecycle, table aging',
          fast_fail_counterfactual: 'post-exit regret requires post-exit path samples; missing samples are reported explicitly instead of guessed',
          live_safety: includePathSamples
            ? 'include_path_samples=1 requested; this can be heavier on production.'
            : 'post-exit path sample scans are skipped by default for live safety; pass include_path_samples=1 for explicit heavy counterfactuals.',
        },
        query_ms: Date.now() - startedAt,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/mode-ev') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 2,
        maxHours: 2,
        defaultLimit: 500,
        maxLimit: 1000,
        defaultBootstrapIterations: 1000,
        maxBootstrapIterations: 3000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const limit = liveGuard.limit;
      const sinceTs = liveGuard.since_ts;
      const clean = String(url.searchParams.get('clean') || 'all').toLowerCase() === 'quote' ? 'quote' : 'all';
      const quoteGapMaxPctRaw = Number(url.searchParams.get('quote_gap_max_pct') || '8');
      const extraCostPctRaw = Number(url.searchParams.get('extra_cost_pct') || '0');
      const quoteGapMaxPct = Number.isFinite(quoteGapMaxPctRaw) ? quoteGapMaxPctRaw : 8;
      const extraCostPct = Number.isFinite(extraCostPctRaw) ? extraCostPctRaw : 0;
      const policyVersion = String(url.searchParams.get('policy_version') || '').trim();
      const revivalCanaryRaw = String(url.searchParams.get('revival_canary') || '').trim().toLowerCase();
      const revivalCanary = ['1', 'true', 'yes'].includes(revivalCanaryRaw)
        ? true
        : (['0', 'false', 'no'].includes(revivalCanaryRaw) ? false : null);
      const bootstrapIterations = Math.max(250, liveGuard.bootstrap_iterations);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const hasTable = paperDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_trades'").get();
      if (!hasTable) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }
      const cols = getTableColumns(paperDb, 'paper_trades');
      const selectCols = [
        'id',
        'symbol',
        'token_ca',
        'lifecycle_id',
        'entry_ts',
        'exit_ts',
        'exit_reason',
        'pnl_pct',
        `${trustedTradePeakSqlExpr(cols)} AS peak_pnl`,
        `${markTradePeakSqlExpr(cols)} AS mark_peak_pnl`,
        cols.has('peak_trust_status') ? 'peak_trust_status' : "'legacy_peak' AS peak_trust_status",
        'position_size_sol',
        'signal_route',
        'strategy_stage',
        cols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode',
        cols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json',
        cols.has('lotto_state_json') ? 'lotto_state_json' : 'NULL AS lotto_state_json',
        cols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json',
        cols.has('exit_execution_audit_json') ? 'exit_execution_audit_json' : 'NULL AS exit_execution_audit_json',
        cols.has('accounting_source') ? 'accounting_source' : 'NULL AS accounting_source',
        cols.has('exit_quote_mark_gap_pct') ? 'exit_quote_mark_gap_pct' : 'NULL AS exit_quote_mark_gap_pct',
        cols.has('max_path_quote_gap_pct') ? 'max_path_quote_gap_pct' : 'NULL AS max_path_quote_gap_pct',
      ];
      const where = ['pnl_pct IS NOT NULL', '(exit_ts IS NOT NULL OR exit_reason IS NOT NULL)'];
      const params = { limit };
      if (sinceTs) {
        where.push('entry_ts >= @since');
        params.since = sinceTs;
      }
      const rows = paperDb.prepare(`
        SELECT ${selectCols.join(', ')}
        FROM paper_trades
        WHERE ${where.join(' AND ')}
        ORDER BY entry_ts DESC, id DESC
        LIMIT @limit
      `).all(params);
      const report = buildModeEvReport(rows, {
        clean,
        quoteGapMaxPct,
        extraCostPct,
        bootstrapIterations,
        policyVersion,
        revivalCanary,
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          window: url.searchParams.get('window') || null,
          clean,
          policy_version: policyVersion || null,
          revival_canary: revivalCanary,
          limit,
          quote_gap_max_pct: quoteGapMaxPct,
          extra_cost_pct: extraCostPct,
          bootstrap_iterations: bootstrapIterations,
          unit_economics_min_net_pct: 1.5,
          unit_economics_min_net_sol: 0.000045,
        },
        query_ms: Date.now() - startedAt,
        ...report,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/lotto-quote-gap-audit') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const limit = boundedIntParam(url, 'limit', 200, 1, 1000);
      const recentLimit = boundedIntParam(url, 'recent_limit', 25, 0, 100);
      const sinceTs = boundedWindowedSinceTs(url, 2, 24);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_decision_events')) {
        res.writeHead(404, apiJsonHeaders());
        res.end(JSON.stringify({ error: 'paper_decision_events table not found' }));
        return;
      }
      const rows = paperDb.prepare(`
        SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
               signal_ts, strategy_stage, route, component, event_type, decision,
               reason, data_source, lifecycle_state, vitality_score, entry_bias, payload_json
        FROM paper_decision_events
        WHERE component = 'lotto_quote_gap_audit'
          AND event_type = 'point_in_time_quote_curve'
          AND event_ts >= @since
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all({ since: sinceTs, limit });
      const report = buildLottoQuoteGapAuditSummary(rows, { recentLimit });
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: new Date(sinceTs * 1000).toISOString(),
          hours: boundedIntParam(url, 'hours', 2, 1, 24),
          limit,
          recent_limit: recentLimit,
        },
        query_ms: Date.now() - startedAt,
        ...report,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/lotto-quote-gap-winner-join') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const limit = boundedIntParam(url, 'limit', 500, 1, 2000);
      const missedLimit = boundedIntParam(url, 'missed_limit', 5000, 1, 20000);
      const recentLimit = boundedIntParam(url, 'recent_limit', 25, 0, 100);
      const topLimit = boundedIntParam(url, 'top_limit', 25, 1, 100);
      const maxJoinDeltaSec = boundedIntParam(url, 'join_delta_sec', 3600, 60, 86400);
      const requestedHours = boundedIntParam(url, 'hours', 2, 1, 72);
      const sinceTs = boundedWindowedSinceTs(url, 2, 72);
      const missedSinceTs = Math.max(0, sinceTs - maxJoinDeltaSec);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_decision_events')) {
        res.writeHead(404, apiJsonHeaders());
        res.end(JSON.stringify({ error: 'paper_decision_events table not found' }));
        return;
      }
      if (!tableNames.has('paper_missed_signal_attribution')) {
        res.writeHead(404, apiJsonHeaders());
        res.end(JSON.stringify({ error: 'paper_missed_signal_attribution table not found' }));
        return;
      }
      const auditRows = paperDb.prepare(`
        SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
               signal_ts, strategy_stage, route, component, event_type, decision,
               reason, data_source, lifecycle_state, vitality_score, entry_bias, payload_json
        FROM paper_decision_events
        WHERE component = 'lotto_quote_gap_audit'
          AND event_type = 'point_in_time_quote_curve'
          AND event_ts >= @since
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all({ since: sinceTs, limit });
      const auditTokens = Array.from(new Set(auditRows.map((row) => row.token_ca).filter(Boolean).map(String)));
      let missedRows = [];
      let fastLaneRescueRows = [];
      let fastLaneQueueRows = [];
      if (auditTokens.length > 0) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const missedColumn = (name, fallback = 'NULL') => missedCols.has(name) ? `m.${name}` : fallback;
        const missedEventTsExpr = `COALESCE(${[
          missedCols.has('created_event_ts') ? 'm.created_event_ts' : null,
          missedCols.has('signal_ts') ? 'm.signal_ts' : null,
          missedCols.has('baseline_ts') ? 'm.baseline_ts' : null,
          '0',
        ].filter(Boolean).join(', ')})`;
        const trustedPeakExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
        missedRows = paperDb.prepare(`
          SELECT
            ${missedCols.has('id') ? 'm.id' : 'm.rowid'} AS id,
            ${missedColumn('created_event_ts')} AS created_event_ts,
            m.token_ca,
            COALESCE(${missedColumn('symbol')}, substr(m.token_ca, 1, 8), '?') AS symbol,
            ${missedColumn('signal_ts')} AS signal_ts,
            ${missedColumn('baseline_ts')} AS baseline_ts,
            ${missedColumn('first_tradable_ts')} AS first_tradable_ts,
            ${missedColumn('updated_at')} AS updated_at,
            ${missedColumn('route')} AS route,
            ${missedColumn('component')} AS component,
            ${missedColumn('reject_reason')} AS reject_reason,
            ${missedColumn('tradable_missed')} AS tradable_missed,
            ${missedColumn('would_stop_before_peak')} AS would_stop_before_peak,
            ${missedColumn('tradability_status')} AS tradability_status,
            ${missedColumn('tradability_reason')} AS tradability_reason,
            ${missedColumn('tradable_peak_pnl')} AS tradable_peak_pnl,
            ${missedColumn('quote_clean_peak_pnl')} AS quote_clean_peak_pnl,
            ${missedColumn('executable_peak_pnl')} AS executable_peak_pnl,
            ${missedColumn('max_pnl_recorded')} AS max_pnl_recorded,
            ${missedColumn('pnl_5m')} AS pnl_5m,
            ${missedColumn('pnl_15m')} AS pnl_15m,
            ${missedColumn('pnl_60m')} AS pnl_60m,
            ${missedColumn('pnl_24h')} AS pnl_24h,
            ${trustedPeakExpr} AS trusted_peak_pnl,
            ${missedEventTsExpr} AS event_ts
          FROM paper_missed_signal_attribution m
          WHERE ${missedEventTsExpr} >= @missedSince
            AND m.token_ca IN (${sqlInList(auditTokens)})
          ORDER BY ${missedEventTsExpr} DESC, ${missedCols.has('id') ? 'm.id' : 'm.rowid'} DESC
          LIMIT @missedLimit
        `).all({ missedSince: missedSinceTs, missedLimit });
        if (tableNames.has('paper_fast_missed_rescue_state')) {
          const rescueCols = getTableColumns(paperDb, 'paper_fast_missed_rescue_state');
          const rescueColumn = (name, fallback = 'NULL') => rescueCols.has(name) ? `r.${name}` : fallback;
          const missedIds = missedRows.map((row) => row.id).filter((id) => id != null);
          const rescueWhereParts = [];
          if (missedIds.length > 0) {
            rescueWhereParts.push(`r.missed_attribution_id IN (${sqlInList(missedIds)})`);
          }
          if (rescueCols.has('token_ca') && auditTokens.length > 0) {
            rescueWhereParts.push(`r.token_ca IN (${sqlInList(auditTokens)})`);
          }
          const rescueWhere = rescueWhereParts.length > 0
            ? `(${rescueWhereParts.join(' OR ')})`
            : null;
          if (rescueWhere) {
            fastLaneRescueRows = paperDb.prepare(`
              SELECT
                r.missed_attribution_id,
                ${rescueColumn('rescue_signature')} AS rescue_signature,
                ${rescueColumn('last_status')} AS last_status,
                ${rescueColumn('last_reason')} AS last_reason,
                ${rescueColumn('last_action_at')} AS last_action_at,
                ${rescueColumn('updated_at')} AS updated_at,
                ${rescueColumn('token_ca')} AS token_ca,
                ${rescueColumn('entry_branch')} AS entry_branch,
                ${rescueColumn('entry_mode_hint')} AS entry_mode_hint,
                ${rescueColumn('policy_version')} AS policy_version,
                ${rescueColumn('state')} AS state,
                ${rescueColumn('blocker')} AS blocker,
                ${rescueColumn('first_seen_at')} AS first_seen_at,
                ${rescueColumn('last_clean_quote_ts')} AS last_clean_quote_ts,
                ${rescueColumn('last_tradable_ts')} AS last_tradable_ts,
                ${rescueColumn('eligibility_json')} AS eligibility_json
              FROM paper_fast_missed_rescue_state r
              WHERE ${rescueWhere}
            `).all();
          }
        }
        if (tableNames.has('paper_fast_entry_queue')) {
          const queueCols = getTableColumns(paperDb, 'paper_fast_entry_queue');
          const queueColumn = (name, fallback = 'NULL') => queueCols.has(name) ? `q.${name}` : fallback;
          const queueUpdatedExpr = queueCols.has('updated_at')
            ? 'q.updated_at'
            : (queueCols.has('created_at') ? 'q.created_at' : 'q.id');
          fastLaneQueueRows = paperDb.prepare(`
            SELECT
              q.id,
              q.token_ca,
              ${queueColumn('status')} AS status,
              ${queueColumn('last_error')} AS last_error,
              ${queueColumn('first_error')} AS first_error,
              ${queueColumn('source_type')} AS source_type,
              ${queueColumn('entry_branch')} AS entry_branch,
              ${queueColumn('entry_mode_hint')} AS entry_mode_hint,
              ${queueColumn('created_at')} AS created_at,
              ${queueColumn('updated_at', 'q.created_at')} AS updated_at
            FROM paper_fast_entry_queue q
            WHERE q.token_ca IN (${sqlInList(auditTokens)})
            ORDER BY q.token_ca ASC, ${queueUpdatedExpr} DESC, q.id DESC
          `).all();
        }
      }
      const fastLaneRescueByMissedId = new Map(
        fastLaneRescueRows
          .filter((row) => row.missed_attribution_id != null)
          .map((row) => [Number(row.missed_attribution_id), row])
      );
      const fastLaneRescueByToken = new Map();
      for (const row of fastLaneRescueRows) {
        if (!row.token_ca) continue;
        const tokenKey = String(row.token_ca);
        const current = fastLaneRescueByToken.get(tokenKey);
        const rowTs = parseUnixishTime(row.updated_at || row.last_action_at || row.first_seen_at) || 0;
        const currentTs = current ? (parseUnixishTime(current.updated_at || current.last_action_at || current.first_seen_at) || 0) : -1;
        if (!current || rowTs >= currentTs) fastLaneRescueByToken.set(tokenKey, row);
      }
      const fastLaneQueueByToken = latestActionableFastLaneQueueByToken(fastLaneQueueRows);
      const report = buildLottoQuoteGapWinnerJoinReport(auditRows, missedRows, {
        recentLimit,
        topLimit,
        maxJoinDeltaSec,
        fastLaneRescueByMissedId,
        fastLaneRescueByToken,
        fastLaneQueueByToken,
      });
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: new Date(sinceTs * 1000).toISOString(),
          missed_since_ts: missedSinceTs,
          missed_since_iso: new Date(missedSinceTs * 1000).toISOString(),
          hours: requestedHours,
          limit,
          missed_limit: missedLimit,
          recent_limit: recentLimit,
          top_limit: topLimit,
          join_delta_sec: maxJoinDeltaSec,
          max_window_hours: 72,
        },
        query_ms: Date.now() - startedAt,
        ...report,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/mode-registry') {
    if (!checkAuth(req, url, res)) return;
    try {
      const registry = loadEntryModeRegistry();
      const summary = summarizeEntryModeRegistry(registry);
      const includeModes = String(url.searchParams.get('include_modes') || 'true').toLowerCase() !== 'false';
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        registry_path: DEFAULT_ENTRY_MODE_REGISTRY_PATH,
        summary,
        tiers: registry.tiers || {},
        promotion_policy: registry.promotion_policy || {},
        decision_gates: registry.decision_gates || {},
        modes_by_tier: includeModes ? registryModesByTier(registry) : undefined,
        virtual_modes: includeModes ? (registry.virtual_modes || {}) : undefined,
        note: 'Registry is observational/governance only for now. Non-live tiers remain blocked by entry_mode_quality until isolated paper caps are implemented and NOT_ATH watch evidence passes the gate.',
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/paper/lotto-reclaim-markov-backtest') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const days = boundedIntParam(url, 'days', 7, 1, 30);
      const minSample = boundedIntParam(url, 'min_sample', 20, 0, 5000);
      const maxCandidates = boundedIntParam(url, 'max_candidates', 1000, 1, 10000);
      const maxTrainingOutcomes = boundedIntParam(url, 'max_training_outcomes', 2000, 100, 50000);
      const includeRows = ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_rows') || '').toLowerCase());
      const parseBoundedFloat = (name, defaultValue, minValue, maxValue) => {
        const raw = Number.parseFloat(url.searchParams.get(name) || String(defaultValue));
        const value = Number.isFinite(raw) ? raw : defaultValue;
        return Math.max(minValue, Math.min(value, maxValue));
      };
      const args = [
        'scripts/backtest_lotto_reclaim_markov.py',
        '--db', paperDbPath,
        '--days', String(days),
        '--min-sample', String(minSample),
        '--min-peak30-prob', String(parseBoundedFloat('min_peak30_prob', 0.12, 0, 1)),
        '--max-stop-prob', String(parseBoundedFloat('max_stop_prob', 0.55, 0, 1)),
        '--min-edge', String(parseBoundedFloat('min_edge', 0.02, -1, 1)),
        '--max-candidates', String(maxCandidates),
        '--max-training-outcomes', String(maxTrainingOutcomes),
        '--json',
      ];
      if (includeRows) args.push('--include-rows');
      execFile('python3', args, {
        cwd: projectRoot,
        env: {
          ...process.env,
          PYTHONUNBUFFERED: '1',
          PAPER_DB: paperDbPath,
        },
        timeout: Math.max(5000, boundedIntParam(url, 'timeout_ms', 60000, 5000, 180000)),
        maxBuffer: 30 * 1024 * 1024,
      }, (error, stdout, stderr) => {
        try { if (releasePaperReport) releasePaperReport(); } catch {}
        if (error) {
          res.writeHead(500, apiJsonHeaders());
          res.end(JSON.stringify({
            error: error.message,
            stderr: String(stderr || '').slice(-4000),
            query_ms: Date.now() - startedAt,
          }, null, 2));
          return;
        }
        try {
          const report = JSON.parse(String(stdout || '{}'));
          res.writeHead(200, apiJsonHeaders());
          res.end(JSON.stringify({
            generated_at: new Date().toISOString(),
            endpoint: url.pathname,
            query_ms: Date.now() - startedAt,
            ...report,
          }, null, 2));
        } catch (parseError) {
          res.writeHead(500, apiJsonHeaders());
          res.end(JSON.stringify({
            error: `failed_to_parse_backtest_json: ${parseError.message}`,
            stdout: String(stdout || '').slice(0, 4000),
            stderr: String(stderr || '').slice(-4000),
            query_ms: Date.now() - startedAt,
          }, null, 2));
        }
      });
    } catch (e) {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/paper/not-ath-reclaim-funnel') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 6,
        maxHours: 24,
        defaultLimit: 5000,
        maxLimit: 20000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      paperDb = openDashboardSqlite(paperDbPath, {
        readonly: true,
        timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000),
      });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const report = buildNotAthReclaimFunnelReport(paperDb, tableNames, liveGuard.since_ts, {
        limit: liveGuard.limit,
        entryMode: url.searchParams.get('entry_mode') || undefined,
        entryBranch: url.searchParams.get('entry_branch') || undefined,
      });
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        window_hours: liveGuard.window_hours,
        requested_window_hours: Number.parseInt(url.searchParams.get('hours') || String(liveGuard.window_hours), 10) || liveGuard.window_hours,
        query_ms: Date.now() - startedAt,
        live_query: true,
        ...report,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/candidate-shadow-summary') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 12,
        maxHours: 24,
        defaultLimit: 1000,
        maxLimit: 1000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 10000, 1000, 60000) });
      const tables = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tables.has('candidate_shadow_observations') || !tables.has('candidate_shadow_virtual_trades')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          error: 'candidate_shadow_tables_not_found',
          has_observations: tables.has('candidate_shadow_observations'),
          has_virtual_trades: tables.has('candidate_shadow_virtual_trades'),
        }, null, 2));
        return;
      }
      const since = liveGuard.since_ts;
      const coverage = paperDb.prepare(`
        SELECT
          COUNT(DISTINCT signal_id) AS signals,
          COUNT(*) AS observation_rows,
          COUNT(DISTINCT candidate_id) AS candidate_ids,
          ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT signal_id), 0), 2) AS rows_per_signal
        FROM candidate_shadow_observations
        WHERE observed_at >= @since
      `).get({ since });
      const badSignals = paperDb.prepare(`
        SELECT signal_id, COUNT(*) AS n
        FROM candidate_shadow_observations
        WHERE observed_at >= @since
        GROUP BY signal_id
        HAVING n != 84
        ORDER BY n ASC, signal_id DESC
        LIMIT 50
      `).all({ since });
      const observationRows = paperDb.prepare(`
        SELECT
          candidate_id,
          family,
          COUNT(*) AS observation_rows,
          SUM(CASE WHEN matched THEN 1 ELSE 0 END) AS matched_observations,
          COUNT(DISTINCT token_ca) AS unique_observation_tokens
        FROM candidate_shadow_observations
        WHERE observed_at >= @since
        GROUP BY candidate_id, family
      `).all({ since });
      const byCandidate = new Map(observationRows.map((row) => [row.candidate_id, row]));
      const candidates = paperDb.prepare(`
        SELECT
          candidate_id,
          family,
          COUNT(*) AS virtual_rows,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' THEN 1 ELSE 0 END) AS closed_n,
          SUM(CASE WHEN status = 'VIRTUAL_OPEN' THEN 1 ELSE 0 END) AS open_n,
          SUM(CASE WHEN status NOT IN ('VIRTUAL_CLOSED', 'VIRTUAL_OPEN') THEN 1 ELSE 0 END) AS waiting_n,
          COUNT(DISTINCT token_ca) AS unique_tokens,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
          AVG(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS avg_net_pnl_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct ELSE 0 END) AS total_net_pnl_pct,
          MIN(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS worst_net_pnl_pct,
          MAX(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS best_net_pnl_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct > 0 THEN net_pnl_pct ELSE 0 END) AS gross_win_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct < 0 THEN -net_pnl_pct ELSE 0 END) AS gross_loss_pct,
          AVG(CASE WHEN status = 'VIRTUAL_CLOSED' AND exit_ts IS NOT NULL AND entry_ts IS NOT NULL THEN (exit_ts - entry_ts) / 60.0 END) AS avg_hold_minutes
        FROM candidate_shadow_virtual_trades
        WHERE observed_at >= @since
        GROUP BY candidate_id, family
        ORDER BY closed_n DESC, avg_net_pnl_pct DESC
      `).all({ since }).map((row) => {
        const obs = byCandidate.get(row.candidate_id) || {};
        const closedN = Number(row.closed_n || 0);
        const virtualRows = Number(row.virtual_rows || 0);
        const grossLoss = Number(row.gross_loss_pct || 0);
        return {
          candidate_id: row.candidate_id,
          family: row.family,
          observation_rows: Number(obs.observation_rows || 0),
          matched_observations: Number(obs.matched_observations || 0),
          match_rate_pct: obs.observation_rows ? roundNumber(Number(obs.matched_observations || 0) / Number(obs.observation_rows) * 100, 2) : null,
          unique_observation_tokens: Number(obs.unique_observation_tokens || 0),
          virtual_rows: virtualRows,
          closed_n: closedN,
          open_n: Number(row.open_n || 0),
          waiting_n: Number(row.waiting_n || 0),
          waiting_rate_pct: virtualRows ? roundNumber(Number(row.waiting_n || 0) / virtualRows * 100, 2) : null,
          unique_tokens: Number(row.unique_tokens || 0),
          wins: Number(row.wins || 0),
          win_rate_pct: closedN ? roundNumber(Number(row.wins || 0) / closedN * 100, 2) : null,
          avg_net_pnl_pct: roundNullableNumber(row.avg_net_pnl_pct, 4),
          total_net_pnl_pct: roundNullableNumber(row.total_net_pnl_pct, 4),
          worst_net_pnl_pct: roundNullableNumber(row.worst_net_pnl_pct, 4),
          best_net_pnl_pct: roundNullableNumber(row.best_net_pnl_pct, 4),
          profit_factor: grossLoss > 0 ? roundNumber(Number(row.gross_win_pct || 0) / grossLoss, 4) : null,
          avg_hold_minutes: roundNullableNumber(row.avg_hold_minutes, 2),
        };
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        schema_version: 'candidate_shadow_summary.v1',
        generated_at: new Date().toISOString(),
        window_hours: liveGuard.window_hours,
        since_ts: since,
        since_iso: new Date(since * 1000).toISOString(),
        coverage: {
          ...coverage,
          rows_per_signal_ok: Number(coverage?.rows_per_signal || 0) === 84,
          candidate_count_ok: Number(coverage?.candidate_ids || 0) === 84,
          bad_signal_count: badSignals.length,
          bad_signal_sample: badSignals,
        },
        candidates,
        query_ms: Date.now() - startedAt,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/candidate-shadow-cross-summary') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    const dim = url.searchParams.get('dimension') || 'signal_type';
    const dimSql = {
      signal_type: "COALESCE(json_extract(o.payload_json, '$.signal_type'), 'UNKNOWN')",
      hard_gate_status: "COALESCE(json_extract(o.payload_json, '$.hard_gate_status'), 'UNKNOWN')",
      not_ath: "CASE WHEN COALESCE(json_extract(o.payload_json, '$.is_not_ath_new_trending'), 0) THEN 'NOT_ATH_NEW_TRENDING' ELSE 'OTHER' END",
      fbr_time_legal: "CASE WHEN COALESCE(json_extract(o.payload_json, '$.fbr_time_legal'), 0) THEN 'true' ELSE 'false' END",
      fbr_lookahead_warning: "CASE WHEN COALESCE(json_extract(o.payload_json, '$.fbr_lookahead_warning'), 0) THEN 'true' ELSE 'false' END",
      entry_bar_color: "CASE WHEN COALESCE(json_extract(o.payload_json, '$.entry_bar_green'), 0) THEN 'green' WHEN COALESCE(json_extract(o.payload_json, '$.entry_bar_red'), 0) THEN 'red' ELSE 'unknown' END",
      candle_pattern: "COALESCE(json_extract(o.payload_json, '$.candle_pattern'), 'UNKNOWN')",
      volume_profile: "COALESCE(json_extract(o.payload_json, '$.volume_profile'), 'UNKNOWN')",
      source_resonance_state: "COALESCE(json_extract(o.payload_json, '$.source_resonance_state'), 'UNKNOWN')",
      source_quote_clean: "CASE WHEN json_extract(o.payload_json, '$.source_quote_clean_seen') IS NULL THEN 'unknown' WHEN COALESCE(json_extract(o.payload_json, '$.source_quote_clean_seen'), 0) THEN 'true' ELSE 'false' END",
      source_quote_executable_proxy: "CASE WHEN json_extract(o.payload_json, '$.source_quote_executable_proxy') IS NULL THEN 'unknown' WHEN COALESCE(json_extract(o.payload_json, '$.source_quote_executable_proxy'), 0) THEN 'true' ELSE 'false' END",
      market_cap_bucket: `
        CASE
          WHEN json_extract(o.payload_json, '$.market_cap') IS NULL THEN 'unknown'
          WHEN CAST(json_extract(o.payload_json, '$.market_cap') AS REAL) < 5000 THEN '<5k'
          WHEN CAST(json_extract(o.payload_json, '$.market_cap') AS REAL) < 10000 THEN '5k-10k'
          WHEN CAST(json_extract(o.payload_json, '$.market_cap') AS REAL) < 30000 THEN '10k-30k'
          WHEN CAST(json_extract(o.payload_json, '$.market_cap') AS REAL) < 100000 THEN '30k-100k'
          ELSE '>=100k'
        END`,
      fbr_bucket: `
        CASE
          WHEN NOT COALESCE(json_extract(o.payload_json, '$.fbr_time_legal'), 0) THEN 'not_time_legal'
          WHEN json_extract(o.payload_json, '$.first_bar_return_pct') IS NULL THEN 'unknown'
          WHEN CAST(json_extract(o.payload_json, '$.first_bar_return_pct') AS REAL) < 0 THEN '<0'
          WHEN CAST(json_extract(o.payload_json, '$.first_bar_return_pct') AS REAL) < 1 THEN '0-1'
          WHEN CAST(json_extract(o.payload_json, '$.first_bar_return_pct') AS REAL) < 2 THEN '1-2'
          WHEN CAST(json_extract(o.payload_json, '$.first_bar_return_pct') AS REAL) < 5 THEN '2-5'
          ELSE '>=5'
        END`,
      first3_mom_bucket: `
        CASE
          WHEN json_extract(o.payload_json, '$.first3_momentum_pct') IS NULL THEN 'unknown'
          WHEN CAST(json_extract(o.payload_json, '$.first3_momentum_pct') AS REAL) < -10 THEN '<-10'
          WHEN CAST(json_extract(o.payload_json, '$.first3_momentum_pct') AS REAL) < 0 THEN '-10-0'
          WHEN CAST(json_extract(o.payload_json, '$.first3_momentum_pct') AS REAL) < 20 THEN '0-20'
          WHEN CAST(json_extract(o.payload_json, '$.first3_momentum_pct') AS REAL) < 50 THEN '20-50'
          ELSE '>=50'
        END`,
    }[dim];
    if (!dimSql) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'unsupported_dimension', supported_dimensions: [
        'signal_type', 'hard_gate_status', 'not_ath', 'fbr_time_legal',
        'fbr_lookahead_warning', 'entry_bar_color', 'candle_pattern',
        'volume_profile', 'source_resonance_state', 'source_quote_clean',
        'source_quote_executable_proxy', 'market_cap_bucket', 'fbr_bucket', 'first3_mom_bucket',
      ] }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 12,
        maxHours: 24,
        defaultLimit: 500,
        maxLimit: 2000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const minClosed = boundedIntParam(url, 'min_closed', 20, 0, 1000);
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 10000, 1000, 60000) });
      const rows = paperDb.prepare(`
        WITH joined AS (
          SELECT
            v.candidate_id,
            v.family,
            v.token_ca,
            v.status,
            v.net_pnl_pct,
            v.entry_ts,
            v.exit_ts,
            ${dimSql} AS slice_value
          FROM candidate_shadow_virtual_trades v
          JOIN candidate_shadow_observations o
            ON o.signal_id = v.signal_id AND o.candidate_id = v.candidate_id
          WHERE v.observed_at >= @since AND o.observed_at >= @since
        )
        SELECT
          candidate_id,
          family,
          CAST(slice_value AS TEXT) AS slice_value,
          COUNT(*) AS virtual_rows,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' THEN 1 ELSE 0 END) AS closed_n,
          COUNT(DISTINCT token_ca) AS unique_tokens,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
          AVG(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS avg_net_pnl_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct ELSE 0 END) AS total_net_pnl_pct,
          MIN(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS worst_net_pnl_pct,
          MAX(CASE WHEN status = 'VIRTUAL_CLOSED' THEN net_pnl_pct END) AS best_net_pnl_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct > 0 THEN net_pnl_pct ELSE 0 END) AS gross_win_pct,
          SUM(CASE WHEN status = 'VIRTUAL_CLOSED' AND net_pnl_pct < 0 THEN -net_pnl_pct ELSE 0 END) AS gross_loss_pct,
          AVG(CASE WHEN status = 'VIRTUAL_CLOSED' AND exit_ts IS NOT NULL AND entry_ts IS NOT NULL THEN (exit_ts - entry_ts) / 60.0 END) AS avg_hold_minutes
        FROM joined
        GROUP BY candidate_id, family, CAST(slice_value AS TEXT)
        HAVING closed_n >= @minClosed
        ORDER BY avg_net_pnl_pct DESC
        LIMIT @limit
      `).all({ since: liveGuard.since_ts, minClosed, limit: liveGuard.limit }).map((row) => {
        const closedN = Number(row.closed_n || 0);
        const grossLoss = Number(row.gross_loss_pct || 0);
        return {
          candidate_id: row.candidate_id,
          family: row.family,
          dimension: dim,
          slice_value: row.slice_value,
          virtual_rows: Number(row.virtual_rows || 0),
          closed_n: closedN,
          unique_tokens: Number(row.unique_tokens || 0),
          wins: Number(row.wins || 0),
          win_rate_pct: closedN ? roundNumber(Number(row.wins || 0) / closedN * 100, 2) : null,
          avg_net_pnl_pct: roundNullableNumber(row.avg_net_pnl_pct, 4),
          total_net_pnl_pct: roundNullableNumber(row.total_net_pnl_pct, 4),
          worst_net_pnl_pct: roundNullableNumber(row.worst_net_pnl_pct, 4),
          best_net_pnl_pct: roundNullableNumber(row.best_net_pnl_pct, 4),
          profit_factor: grossLoss > 0 ? roundNumber(Number(row.gross_win_pct || 0) / grossLoss, 4) : null,
          avg_hold_minutes: roundNullableNumber(row.avg_hold_minutes, 2),
        };
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        schema_version: 'candidate_shadow_cross_summary.v1',
        generated_at: new Date().toISOString(),
        window_hours: liveGuard.window_hours,
        since_ts: liveGuard.since_ts,
        since_iso: new Date(liveGuard.since_ts * 1000).toISOString(),
        dimension: dim,
        min_closed: minClosed,
        rows,
        query_ms: Date.now() - startedAt,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/entry-mode-performance') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      let limit = Math.max(1, Math.min(parseInt(url.searchParams.get('limit') || '1000', 10) || 1000, 10000));
      let sinceTs = parseUnixishTime(url.searchParams.get('since') || url.searchParams.get('since_ts'));
      const requestedHours = Number.parseInt(url.searchParams.get('hours') || '0', 10);
      const forceLive = ['1', 'true', 'yes'].includes(String(url.searchParams.get('live') || '').toLowerCase())
        || ['0', 'false', 'no'].includes(String(url.searchParams.get('materialized') || '').toLowerCase());
      const wantsMaterialized = (
        ['1', 'true', 'yes'].includes(String(url.searchParams.get('materialized') || '').toLowerCase())
        || (Number.isFinite(requestedHours) && requestedHours > 2)
      );
      if (wantsMaterialized && !forceLive) {
        const snapshotHours = Number.isFinite(requestedHours) && requestedHours > 0 ? requestedHours : 8;
        const liveSnapshot = readLivePaperReview(snapshotHours);
        if (liveSnapshot && !liveSnapshot.error && liveSnapshot.entry_mode_performance) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify(entryModePerformanceFromLiveSnapshot(liveSnapshot, {
            dbPath: paperDbPath,
            requestedHours: snapshotHours,
            limit,
          }), null, 2));
          return;
        }
        res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          available: false,
          materialized: true,
          reason: liveSnapshot?.error || 'materialized_entry_mode_performance_not_ready',
          path: livePaperReviewPath(snapshotHours),
        }, null, 2));
        return;
      }
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 2,
        maxHours: 2,
        defaultLimit: 1000,
        maxLimit: 1000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      limit = liveGuard.limit;
      sinceTs = liveGuard.since_ts;
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const hasTable = paperDb.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_trades'").get();
      if (!hasTable) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }
      const cols = getTableColumns(paperDb, 'paper_trades');
      const selectCols = [
        'id', 'symbol', 'token_ca', 'entry_ts', 'exit_ts', 'exit_reason', 'pnl_pct',
        `${trustedTradePeakSqlExpr(cols)} AS peak_pnl`,
        `${markTradePeakSqlExpr(cols)} AS mark_peak_pnl`,
        cols.has('peak_trust_status') ? 'peak_trust_status' : "'legacy_peak' AS peak_trust_status",
        'position_size_sol', 'signal_route', 'signal_type', 'strategy_stage',
        cols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode',
        cols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json',
        cols.has('lotto_state_json') ? 'lotto_state_json' : 'NULL AS lotto_state_json',
        cols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json',
      ];
      const whereSql = sinceTs ? 'WHERE entry_ts >= @since' : '';
      const rows = paperDb.prepare(`
        SELECT ${selectCols.join(', ')}
        FROM paper_trades
        ${whereSql}
        ORDER BY entry_ts DESC, id DESC
        LIMIT @limit
      `).all(sinceTs ? { since: sinceTs, limit } : { limit });

      const groups = new Map();
      const recent = [];
      for (const row of rows) {
        const entryAudit = parseJsonObject(row.entry_execution_audit_json);
        const monitorState = parseJsonObject(row.monitor_state_json);
        const entryMode = inferEntryMode(row);
        const bucket = entryModeBucket(entryMode, row.position_size_sol);
        const key = `${bucket}:${entryMode}`;
        const athRecoveryFamily = athRecoveryFamilyFor(entryMode, monitorState);
        const lottoRecoveryFamily = firstValue(monitorState.lottoRecoveryFamily, (monitorState.lottoState || {}).lottoRecoveryFamily);
        const parentBlockReason = firstValue(monitorState.parentBlockReason, monitorState.parent_block_reason);
        const recoveryProbeReason = firstValue(monitorState.recoveryProbeReason, monitorState.recovery_probe_reason);
        const closed = row.exit_ts != null || row.exit_reason != null;
        const pnl = row.pnl_pct == null ? null : Number(row.pnl_pct);
        const peak = trustedPeakRatio(row);
        const entryQuoteSuccess = entryAudit.success === true || entryAudit.routeAvailable === true;
        const entryQuoteFailure = Boolean(entryAudit.failureReason) || entryAudit.success === false || entryAudit.routeAvailable === false;
        if (!groups.has(key)) {
          groups.set(key, {
            bucket,
            entry_mode: entryMode,
            total: 0,
            open: 0,
            closed: 0,
            wins: 0,
            losses: 0,
            total_pnl: 0,
            total_peak: 0,
            pnl_n: 0,
            peak_n: 0,
            total_position_size_sol: 0,
            position_n: 0,
            est_pnl_sol: 0,
            entry_quote_success_n: 0,
            entry_quote_failure_n: 0,
            ath_recovery_family: athRecoveryFamily,
            lotto_recovery_family: lottoRecoveryFamily,
            parent_block_reasons: {},
            recovery_probe_reasons: {},
          });
        }
        const g = groups.get(key);
        g.total += 1;
        if (closed) g.closed += 1;
        else g.open += 1;
        if (pnl != null && Number.isFinite(pnl)) {
          g.pnl_n += 1;
          g.total_pnl += pnl;
          if (closed && pnl > 0) g.wins += 1;
          if (closed && pnl <= 0) g.losses += 1;
          if (row.position_size_sol) g.est_pnl_sol += pnl * Number(row.position_size_sol || 0);
        }
        if (peak != null && Number.isFinite(peak)) {
          g.peak_n += 1;
          g.total_peak += peak;
        }
        if (row.position_size_sol != null) {
          g.position_n += 1;
          g.total_position_size_sol += Number(row.position_size_sol || 0);
        }
        if (entryQuoteSuccess) g.entry_quote_success_n += 1;
        if (entryQuoteFailure) g.entry_quote_failure_n += 1;
        if (athRecoveryFamily && !g.ath_recovery_family) g.ath_recovery_family = athRecoveryFamily;
        if (lottoRecoveryFamily && !g.lotto_recovery_family) g.lotto_recovery_family = lottoRecoveryFamily;
        if (parentBlockReason) g.parent_block_reasons[parentBlockReason] = (g.parent_block_reasons[parentBlockReason] || 0) + 1;
        if (recoveryProbeReason) g.recovery_probe_reasons[recoveryProbeReason] = (g.recovery_probe_reasons[recoveryProbeReason] || 0) + 1;
        if (recent.length < 50) {
          recent.push({
            id: row.id,
            symbol: row.symbol,
            token_ca: row.token_ca,
            entry_ts: row.entry_ts,
            exit_ts: row.exit_ts,
            exit_reason: row.exit_reason,
            signal_route: row.signal_route,
            strategy_stage: row.strategy_stage,
            entry_mode: entryMode,
            bucket,
            position_size_sol: row.position_size_sol,
            pnl_pct: pnl == null ? null : roundNumber(pnl * 100, 2),
            peak_pnl_pct: peak == null ? null : roundNumber(peak * 100, 2),
            mark_peak_pnl_pct: row.mark_peak_pnl == null ? null : roundNumber(Number(row.mark_peak_pnl) * 100, 2),
            peak_trust_status: row.peak_trust_status || null,
            entry_quote_success: entryQuoteSuccess,
            entry_quote_failure_reason: entryAudit.failureReason || null,
            ath_recovery_family: athRecoveryFamily,
            lotto_recovery_family: lottoRecoveryFamily,
            parent_block_reason: parentBlockReason,
            recovery_probe_reason: recoveryProbeReason,
          });
        }
      }
      const byMode = Array.from(groups.values()).map((g) => ({
        bucket: g.bucket,
        entry_mode: g.entry_mode,
        total: g.total,
        open: g.open,
        closed: g.closed,
        wins: g.wins,
        losses: g.losses,
        win_rate_pct: g.closed ? roundNumber((g.wins / g.closed) * 100, 1) : null,
        avg_pnl_pct: g.pnl_n ? roundNumber((g.total_pnl / g.pnl_n) * 100, 2) : null,
        avg_peak_pnl_pct: g.peak_n ? roundNumber((g.total_peak / g.peak_n) * 100, 2) : null,
        avg_position_size_sol: g.position_n ? roundNumber(g.total_position_size_sol / g.position_n, 4) : null,
        est_pnl_sol: roundNumber(g.est_pnl_sol, 5),
        avg_ev_sol_per_trade: g.total ? roundNumber(g.est_pnl_sol / g.total, 6) : null,
        ath_recovery_family: g.ath_recovery_family || null,
        lotto_recovery_family: g.lotto_recovery_family || null,
        parent_block_reasons: g.parent_block_reasons,
        recovery_probe_reasons: g.recovery_probe_reasons,
        entry_quote_success_n: g.entry_quote_success_n,
        entry_quote_failure_n: g.entry_quote_failure_n,
        entry_quote_success_rate_pct: (g.entry_quote_success_n + g.entry_quote_failure_n)
          ? roundNumber((g.entry_quote_success_n / (g.entry_quote_success_n + g.entry_quote_failure_n)) * 100, 1)
          : null,
      })).sort((a, b) => {
        if (a.bucket !== b.bucket) return a.bucket.localeCompare(b.bucket);
        return b.total - a.total;
      });
      const bucketSummary = {};
      for (const g of byMode) {
        if (!bucketSummary[g.bucket]) bucketSummary[g.bucket] = { total: 0, closed: 0, open: 0, est_pnl_sol: 0 };
        bucketSummary[g.bucket].total += g.total;
        bucketSummary[g.bucket].closed += g.closed;
        bucketSummary[g.bucket].open += g.open;
        bucketSummary[g.bucket].est_pnl_sol += g.est_pnl_sol || 0;
      }
      for (const summary of Object.values(bucketSummary)) {
        summary.est_pnl_sol = roundNumber(summary.est_pnl_sol, 5);
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          limit,
        },
        bucket_summary: bucketSummary,
        by_entry_mode: byMode,
        recent,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/trade-replay') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const startedAt = Date.now();
      const tradeIdRaw = parseInt(url.searchParams.get('trade_id') || url.searchParams.get('id') || '', 10);
      const tradeId = Number.isFinite(tradeIdRaw) && tradeIdRaw > 0 ? tradeIdRaw : null;
      const sinceTs = tradeId ? null : boundedWindowedSinceTs(url, 1, 2);
      const limit = boundedIntParam(url, 'limit', tradeId ? 1 : 25, 1, 80);
      const pathLimit = boundedIntParam(url, 'path_limit', 240, 1, 500);
      const eventLimit = boundedIntParam(url, 'event_limit', 240, 1, 500);
      const hasExplicitReplayWindow = (
        url.searchParams.has('since_ts')
        || url.searchParams.has('since')
        || url.searchParams.has('hours')
      );
      const defaultLossOnly = tradeId || hasExplicitReplayWindow ? '0' : '1';
      const lossOnly = !['0', 'false', 'no'].includes(String(url.searchParams.get('loss_only') || defaultLossOnly).toLowerCase());
      const includeTimeline = !['0', 'false', 'no'].includes(String(url.searchParams.get('include_timeline') || (tradeId ? '1' : '0')).toLowerCase());
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_trades')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_trades table not found' }));
        return;
      }

      const where = [];
      const params = { limit };
      if (tradeId) {
        where.push('id = @tradeId');
        params.tradeId = tradeId;
      } else if (sinceTs) {
        where.push('entry_ts >= @since');
        params.since = sinceTs;
      }
      if (lossOnly) {
        where.push('exit_ts IS NOT NULL');
        where.push('COALESCE(pnl_pct, 0) < 0');
      }
      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
      const trades = paperDb.prepare(`
        SELECT *
        FROM paper_trades
        ${whereSql}
        ORDER BY entry_ts DESC, id DESC
        LIMIT @limit
      `).all(params);

      const eventStmt = tableNames.has('paper_decision_events') ? paperDb.prepare(`
        SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
               signal_ts, strategy_stage, route, component, event_type, decision,
               reason, data_source, lifecycle_state, vitality_score, entry_bias, payload_json
        FROM paper_decision_events
        WHERE trade_id = @tradeId
           OR (lifecycle_id IS NOT NULL AND lifecycle_id = @lifecycleId)
           OR (token_ca = @tokenCa AND event_ts BETWEEN @startTs AND @endTs)
        ORDER BY event_ts ASC, id ASC
        LIMIT @eventLimit
      `) : null;
      const pathStmt = tableNames.has('paper_trade_path_samples') ? paperDb.prepare(`
        SELECT id, trade_id, lifecycle_id, token_ca, symbol, strategy_stage, sample_ts,
               action, reason, mark_price, mark_pnl, quote_price, quote_pnl, peak_pnl,
               sold_pct, mark_source, quote_success, quote_failure_reason, quote_out_sol,
               partial_realized_sol, remaining_cost_basis_sol, blended_mark_pnl,
               blended_quote_pnl, payload_json
        FROM paper_trade_path_samples
        WHERE trade_id = @tradeId
        ORDER BY sample_ts ASC, id ASC
        LIMIT @pathLimit
      `) : null;

      const replays = [];
      for (const trade of trades) {
        const signalTs = parseUnixishTime(trade.signal_ts);
        const entryTs = Number(trade.entry_ts || signalTs || Math.floor(Date.now() / 1000));
        const exitTs = Number(trade.exit_ts || Math.floor(Date.now() / 1000));
        const eventParams = {
          tradeId: trade.id,
          lifecycleId: trade.lifecycle_id || '__no_lifecycle__',
          tokenCa: trade.token_ca || '__no_token__',
          startTs: Math.max(0, Math.min(signalTs || entryTs, entryTs) - 900),
          endTs: Math.max(exitTs, entryTs) + 900,
          eventLimit,
        };
        const events = eventStmt ? eventStmt.all(eventParams) : [];
        const pathSamples = pathStmt ? pathStmt.all({ tradeId: trade.id, pathLimit }) : [];
        replays.push(buildTradeReplay(trade, pathSamples, events, { includeTimeline }));
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          trade_id: tradeId,
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          limit,
          loss_only: lossOnly,
          include_timeline: includeTimeline,
          path_limit: pathLimit,
          event_limit: eventLimit,
        },
        query_ms: Date.now() - startedAt,
        summary: summarizeTradeReplays(replays),
        replay_notes: [
          'decision_events show why the trade was allowed or blocked before entry',
          'path_samples show mark/quote PnL while the position was alive',
          'loss_attribution is rule-based diagnosis, not a strategy change',
        ],
        trades: replays,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/lifecycle-summary') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const limit = boundedIntParam(url, 'limit', 80, 1, 120);
      const eventLimit = Math.max(limit, boundedIntParam(url, 'event_limit', 3000, 100, 8000));
      const sinceTs = boundedWindowedSinceTs(url, 1, 2);
      const statusFilter = (url.searchParams.get('status') || 'all').toLowerCase();
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('paper_decision_events')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_decision_events table not found' }));
        return;
      }
      const eventWhere = sinceTs ? 'WHERE event_ts >= @since' : '';
      const events = paperDb.prepare(`
        SELECT id, event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
               signal_ts, strategy_stage, route, component, event_type, decision,
               reason, data_source, lifecycle_state, vitality_score, entry_bias, payload_json
        FROM paper_decision_events
        ${eventWhere}
        ORDER BY event_ts DESC, id DESC
        LIMIT @eventLimit
      `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });

      const summaries = new Map();
      const latestAthByToken = new Map();
      for (const event of events) {
        const key = lifecycleSummaryKey(event);
        const payload = parseJsonObject(event.payload_json);
        const routeLabel = String(firstValue(event.route, payload.watchlist_type, event.reason) || '').toUpperCase();
        const signalTsMs = normalizeUnixishMs(event.signal_ts);
        if (event.token_ca && routeLabel === 'ATH' && signalTsMs) {
          const previous = latestAthByToken.get(event.token_ca);
          if (!previous || signalTsMs > previous.signal_ts_ms) {
            latestAthByToken.set(event.token_ca, {
              signal_id: event.signal_id,
              signal_ts: event.signal_ts,
              signal_ts_ms: signalTsMs,
              lifecycle_id: event.lifecycle_id,
              symbol: usableSymbol(event.symbol) || (previous || {}).symbol || event.symbol || null,
              event_id: event.id,
              event_ts: event.event_ts,
            });
          } else if (previous && !usableSymbol(previous.symbol) && usableSymbol(event.symbol)) {
            previous.symbol = usableSymbol(event.symbol);
          }
        }
        const entryMode = firstValue(
          payload.entry_mode,
          payload.entryMode,
          (payload.entryDecisionContract || {}).entry_mode,
          (payload.entry_readiness_policy || {}).entry_mode
        );
        if (!summaries.has(key)) {
          summaries.set(key, {
            key,
            lifecycle_id: event.lifecycle_id,
            token_ca: event.token_ca,
            symbol: event.symbol,
            signal_id: event.signal_id,
            signal_ts: event.signal_ts,
            route: event.route,
            strategy_stage: event.strategy_stage,
            first_event_ts: event.event_ts,
            last_event_ts: event.event_ts,
            event_count: 0,
            final_status: decisionStatus(event),
            final_decision: event.decision,
            final_component: event.component,
            final_event_type: event.event_type,
            final_reason: event.reason,
            final_data_source: event.data_source,
            final_event_id: event.id,
            final_blocker: null,
            lifecycle_state: event.lifecycle_state,
            vitality_score: event.vitality_score,
            entry_bias: event.entry_bias,
            entry_mode: entryMode,
            has_trade: false,
            trade_count: 0,
            max_missed_pnl_pct: null,
            tradable_missed: null,
            tradability_status: null,
          });
        }
        const summary = summaries.get(key);
        if (routeLabel === 'ATH' && signalTsMs) {
          const summarySignalTsMs = normalizeUnixishMs(summary.signal_ts);
          if (!summarySignalTsMs || signalTsMs >= summarySignalTsMs) {
            summary.signal_ts = event.signal_ts;
            summary.signal_id = event.signal_id || summary.signal_id;
            summary.route = 'ATH';
            summary.lifecycle_id = event.lifecycle_id || summary.lifecycle_id;
            if (usableSymbol(event.symbol)) summary.symbol = usableSymbol(event.symbol);
          }
        }
        summary.final_blocker = chooseFinalBlocker(
          summary.final_blocker,
          finalBlockerFromEvent(event, payload)
        );
        summary.event_count += 1;
        summary.first_event_ts = Math.min(summary.first_event_ts, event.event_ts);
        summary.last_event_ts = Math.max(summary.last_event_ts, event.event_ts);
        if (!summary.entry_mode && entryMode) summary.entry_mode = entryMode;
      }

      if (tableNames.has('paper_trades')) {
        const tradeCols = getTableColumns(paperDb, 'paper_trades');
        const tradeWhere = sinceTs ? 'WHERE entry_ts >= @since' : '';
        const trustedPeakExpr = trustedTradePeakSqlExpr(tradeCols);
        const markPeakExpr = markTradePeakSqlExpr(tradeCols);
        const tradeRows = paperDb.prepare(`
          SELECT id, lifecycle_id, token_ca, symbol, signal_ts, signal_route, signal_type,
                 strategy_stage, entry_ts, exit_ts, exit_reason, pnl_pct,
                 ${trustedPeakExpr} AS peak_pnl,
                 ${markPeakExpr} AS mark_peak_pnl,
                 ${tradeCols.has('peak_trust_status') ? 'peak_trust_status' : "'legacy_peak' AS peak_trust_status"},
                 position_size_sol,
                 ${tradeCols.has('entry_mode') ? 'entry_mode' : 'NULL AS entry_mode'},
                 ${tradeCols.has('monitor_state_json') ? 'monitor_state_json' : 'NULL AS monitor_state_json'},
                 ${tradeCols.has('lotto_state_json') ? 'lotto_state_json' : 'NULL AS lotto_state_json'},
                 ${tradeCols.has('entry_execution_audit_json') ? 'entry_execution_audit_json' : 'NULL AS entry_execution_audit_json'}
          FROM paper_trades
          ${tradeWhere}
          ORDER BY entry_ts DESC, id DESC
          LIMIT @eventLimit
        `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });
        for (const trade of tradeRows) {
          const key = lifecycleSummaryKey(trade);
          if (!summaries.has(key)) {
            summaries.set(key, {
              key,
              lifecycle_id: trade.lifecycle_id,
              token_ca: trade.token_ca,
              symbol: trade.symbol,
              signal_ts: trade.signal_ts,
              route: trade.signal_route || trade.signal_type,
              strategy_stage: trade.strategy_stage,
              first_event_ts: trade.entry_ts,
              last_event_ts: trade.exit_ts || trade.entry_ts,
              event_count: 0,
              final_status: trade.exit_ts || trade.exit_reason ? 'closed' : 'entered',
              final_decision: trade.exit_ts || trade.exit_reason ? 'closed' : 'filled_paper',
              final_component: 'paper_trades',
              final_event_type: trade.exit_ts || trade.exit_reason ? 'trade_closed' : 'trade_open',
              final_reason: trade.exit_reason || 'open_position',
              final_blocker: finalBlockerFromTrade(trade),
              has_trade: true,
              trade_count: 0,
            });
          }
          const summary = summaries.get(key);
          summary.has_trade = true;
          summary.trade_count = (summary.trade_count || 0) + 1;
          summary.trade_id = trade.id;
          summary.entry_ts = trade.entry_ts;
          summary.exit_ts = trade.exit_ts;
          summary.exit_reason = trade.exit_reason;
          summary.pnl_pct = trade.pnl_pct == null ? null : roundNumber(Number(trade.pnl_pct) * 100, 2);
          summary.peak_pnl_pct = trade.peak_pnl == null ? null : roundNumber(Number(trade.peak_pnl) * 100, 2);
          summary.mark_peak_pnl_pct = trade.mark_peak_pnl == null ? null : roundNumber(Number(trade.mark_peak_pnl) * 100, 2);
          summary.peak_trust_status = trade.peak_trust_status || null;
          summary.position_size_sol = trade.position_size_sol;
          if (summary.entry_mode) summary.event_entry_mode = summary.event_entry_mode || summary.entry_mode;
          summary.entry_mode = inferEntryMode(trade);
          summary.entry_mode_bucket = entryModeBucket(summary.entry_mode, trade.position_size_sol);
          summary.final_status = trade.exit_ts || trade.exit_reason ? 'closed' : 'entered';
          summary.final_decision = summary.final_status;
          summary.final_component = 'paper_trades';
          summary.final_event_type = summary.final_status === 'closed' ? 'trade_closed' : 'trade_open';
          summary.final_reason = trade.exit_reason || 'open_position';
          summary.final_blocker = finalBlockerFromTrade(trade);
        }
      }

      if (tableNames.has('paper_missed_signal_attribution')) {
        const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
        const missedWhere = sinceTs ? 'WHERE COALESCE(signal_ts, created_event_ts, baseline_ts, 0) >= @since' : '';
        const missedRows = paperDb.prepare(`
          SELECT lifecycle_id, token_ca, symbol, signal_ts, route, component,
                 reject_reason, COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, NULL) AS max_pnl,
                 ${missedCols.has('tradable_missed') ? 'tradable_missed' : 'NULL AS tradable_missed'},
                 ${missedCols.has('tradability_status') ? 'tradability_status' : 'NULL AS tradability_status'},
                 ${missedCols.has('would_stop_before_peak') ? 'would_stop_before_peak' : 'NULL AS would_stop_before_peak'}
          FROM paper_missed_signal_attribution
          ${missedWhere}
          ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) DESC
          LIMIT @eventLimit
        `).all(sinceTs ? { since: sinceTs, eventLimit } : { eventLimit });
        for (const missed of missedRows) {
          const key = lifecycleSummaryKey(missed);
          if (!summaries.has(key)) {
            summaries.set(key, {
              key,
              lifecycle_id: missed.lifecycle_id,
              token_ca: missed.token_ca,
              symbol: missed.symbol,
              signal_ts: missed.signal_ts,
              route: missed.route,
              first_event_ts: null,
              last_event_ts: null,
              event_count: 0,
              final_status: 'missed_only',
              final_component: missed.component,
              final_reason: missed.reject_reason,
              final_blocker: finalBlockerFromMissed(missed),
              has_trade: false,
              trade_count: 0,
            });
          }
          const summary = summaries.get(key);
          if (!summary.has_trade) {
            summary.final_blocker = chooseFinalBlocker(summary.final_blocker, finalBlockerFromMissed(missed));
          }
          const maxPnl = missed.max_pnl == null ? null : Number(missed.max_pnl);
          if (maxPnl != null && Number.isFinite(maxPnl)) {
            const prev = summary.max_missed_pnl_pct == null ? -Infinity : summary.max_missed_pnl_pct / 100;
            if (maxPnl > prev) {
              summary.max_missed_pnl_pct = roundNumber(maxPnl * 100, 2);
              summary.missed_component = missed.component;
              summary.missed_reason = missed.reject_reason;
              summary.tradable_missed = missed.tradable_missed;
              summary.tradability_status = missed.tradability_status;
              summary.would_stop_before_peak = missed.would_stop_before_peak;
            }
          }
        }
      }

      for (const summary of summaries.values()) {
        const latestAth = summary.token_ca ? latestAthByToken.get(summary.token_ca) : null;
        if (!latestAth) continue;
        summary.latest_ath_signal_id = latestAth.signal_id;
        summary.latest_ath_signal_ts = latestAth.signal_ts;
        summary.latest_ath_signal_ts_ms = latestAth.signal_ts_ms;
        summary.latest_ath_lifecycle_id = latestAth.lifecycle_id;
        summary.latest_ath_symbol = latestAth.symbol;

        if (String(summary.route || '').toUpperCase() === 'ATH') {
          const anchorTsMs = normalizeUnixishMs(summary.signal_ts);
          summary.anchor_signal_ts_ms = anchorTsMs;
          summary.anchor_is_latest_ath = Boolean(anchorTsMs && latestAth.signal_ts_ms && anchorTsMs >= latestAth.signal_ts_ms);
          summary.anchor_lag_sec = anchorTsMs && latestAth.signal_ts_ms && latestAth.signal_ts_ms > anchorTsMs
            ? Math.round((latestAth.signal_ts_ms - anchorTsMs) / 1000)
            : 0;
        }
      }

      let list = Array.from(summaries.values());
      for (const item of list) {
        applyFinalBlocker(item);
      }
      if (statusFilter !== 'all') {
        list = list.filter((item) => String(item.final_status || '').toLowerCase() === statusFilter);
      }
      const staleAthAnchorCount = list.filter((item) => item.anchor_is_latest_ath === false).length;
      const anchorMismatchCount = list.filter((item) => (
        item.anchor_is_latest_ath === false
        && !item.has_trade
        && !['closed', 'entered'].includes(String(item.final_status || '').toLowerCase())
      )).length;
      const counts = {};
      const byFinalGate = {};
      const byFinalBlocker = {};
      for (const item of list) {
        counts[item.final_status || 'unknown'] = (counts[item.final_status || 'unknown'] || 0) + 1;
        const blocker = item.final_blocker || {};
        const gateKey = `${blocker.component || item.final_component || '-'}:${blocker.reason || item.final_reason || '-'}`;
        const blockerKey = item.final_blocker_key || `${blocker.stage || 'unknown'}:${gateKey}`;
        if (!byFinalGate[gateKey]) {
          byFinalGate[gateKey] = {
            component: blocker.component || item.final_component || '-',
            reason: blocker.reason || item.final_reason || '-',
            n: 0,
            max_missed_pnl_pct: null,
            tradable_n: 0,
          };
        }
        if (!byFinalBlocker[blockerKey]) {
          byFinalBlocker[blockerKey] = {
            key: blockerKey,
            status: blocker.status || item.final_status || 'unknown',
            stage: blocker.stage || 'unknown',
            component: blocker.component || item.final_component || '-',
            reason: blocker.reason || item.final_reason || '-',
            n: 0,
            max_missed_pnl_pct: null,
            tradable_n: 0,
          };
        }
        byFinalGate[gateKey].n += 1;
        byFinalBlocker[blockerKey].n += 1;
        if (item.max_missed_pnl_pct != null) {
          byFinalGate[gateKey].max_missed_pnl_pct = Math.max(
            byFinalGate[gateKey].max_missed_pnl_pct == null ? -Infinity : byFinalGate[gateKey].max_missed_pnl_pct,
            item.max_missed_pnl_pct
          );
          byFinalBlocker[blockerKey].max_missed_pnl_pct = Math.max(
            byFinalBlocker[blockerKey].max_missed_pnl_pct == null ? -Infinity : byFinalBlocker[blockerKey].max_missed_pnl_pct,
            item.max_missed_pnl_pct
          );
        }
        if (Number(item.tradable_missed || 0) === 1) byFinalGate[gateKey].tradable_n += 1;
        if (Number(item.tradable_missed || 0) === 1) byFinalBlocker[blockerKey].tradable_n += 1;
      }
      for (const gate of Object.values(byFinalGate)) {
        if (gate.max_missed_pnl_pct === -Infinity) gate.max_missed_pnl_pct = null;
      }
      for (const blocker of Object.values(byFinalBlocker)) {
        if (blocker.max_missed_pnl_pct === -Infinity) blocker.max_missed_pnl_pct = null;
      }
      list.sort((a, b) => {
        const missedDelta = (b.max_missed_pnl_pct ?? -99999) - (a.max_missed_pnl_pct ?? -99999);
        if (missedDelta !== 0) return missedDelta;
        return (b.last_event_ts || b.entry_ts || 0) - (a.last_event_ts || a.entry_ts || 0);
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          status: statusFilter,
          limit,
          event_limit: eventLimit,
        },
        status_counts: counts,
        anchor_mismatch_count: anchorMismatchCount,
        stale_ath_anchor_count: staleAthAnchorCount,
        anchor_mismatch_definition: 'active/unfilled ATH lifecycles whose anchor is older than the latest ATH signal for the same token; closed or filled older ATH lifecycles are counted only in stale_ath_anchor_count',
        by_final_blocker: Object.values(byFinalBlocker).sort((a, b) => b.n - a.n).slice(0, 100),
        by_final_gate: Object.values(byFinalGate).sort((a, b) => b.n - a.n).slice(0, 100),
        lifecycles: list.slice(0, limit),
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/missed-attribution') {
    // Paper missed-dog attribution summary — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const limit = boundedIntParam(url, 'limit', 25, 1, 80);
      const scanLimit = boundedIntParam(url, 'scan_limit', 5000, 100, 50000);
      const sinceTs = boundedWindowedSinceTs(url, 2, 24);
      const queryStartedAt = Date.now();
      const requestedHours = Number.parseInt(url.searchParams.get('hours') || '2', 10);
      const summaryOnly = ['1', 'true', 'yes'].includes(String(url.searchParams.get('summary_only') || '').toLowerCase())
        || (Number.isFinite(requestedHours) && requestedHours > 2 && !['1', 'true', 'yes'].includes(String(url.searchParams.get('include_details') || '').toLowerCase()));
      const completeWindow = ['1', 'true', 'yes'].includes(String(url.searchParams.get('complete') || '').toLowerCase())
        || (Number.isFinite(requestedHours) && requestedHours > 2)
        || Boolean(url.searchParams.get('since') || url.searchParams.get('since_ts'));
      if (summaryOnly && Number.isFinite(requestedHours) && requestedHours > 2) {
        const liveSnapshot = readLivePaperReview(requestedHours);
        if (liveSnapshot && !liveSnapshot.error) {
          const missed = liveSnapshot.missed || {};
          const overall = missed.overall || {};
          const byGate = missed.by_gate || [];
          const topDogs = missed.top_dogs || [];
          const summary = {
            total_n: overall.unique_tokens ?? null,
            gold_n: overall.gold_unique ?? null,
            silver_n: overall.silver_unique ?? null,
            bronze_n: overall.bronze_unique ?? null,
            sub25_n: overall.unique_tokens == null ? null : Math.max(
              0,
              Number(overall.unique_tokens || 0)
                - Number(overall.gold_unique || 0)
                - Number(overall.silver_unique || 0)
                - Number(overall.bronze_unique || 0)
            ),
            tradable_n: overall.tradable_unique ?? null,
            clean_tradable_n: overall.quote_executable_unique ?? null,
            quote_executable_proxy_n: overall.quote_executable_unique ?? null,
            stop_before_peak_n: overall.stop_before_peak_unique ?? null,
          };
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({
            generated_at: new Date().toISOString(),
            db_path: paperDbPath,
            materialized: true,
            materialized_snapshot_id: liveSnapshot.snapshot_id,
            materialized_generated_at: liveSnapshot.generated_at,
            materialized_path: livePaperReviewPath(requestedHours),
            filters: {
              since_ts: liveSnapshot.window?.since_ts ?? sinceTs,
              since_iso: liveSnapshot.window?.since_iso ?? (sinceTs ? new Date(sinceTs * 1000).toISOString() : null),
              complete_window: true,
              summary_only: true,
              max_window_hours: 24,
              tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
            },
            query_ms: 0,
            tier_summary: {
              event_rows: summary,
              unique_tokens: summary,
              ath_event_rows: null,
              ath_unique_tokens: null,
            },
            top_dogs: topDogs,
            top_unique_dogs: topDogs,
            by_gate: byGate,
            ath_recovery_actions: [],
          }, null, 2));
          return;
        }
        res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          available: false,
          materialized: true,
          reason: liveSnapshot?.error || 'materialized_snapshot_not_ready',
          path: livePaperReviewPath(requestedHours),
        }, null, 2));
        return;
      }
      const missedEventTsExpr = 'COALESCE(created_event_ts, signal_ts, baseline_ts, 0)';
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (!tableNames.has('paper_missed_signal_attribution')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_missed_signal_attribution table not found' }));
        return;
      }
      const missedCols = new Set(
        paperDb.prepare("PRAGMA table_info(paper_missed_signal_attribution)").all().map((row) => row.name)
      );
      const trustedMissedPeakExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
      const maxMissedId = Number(paperDb.prepare('SELECT COALESCE(MAX(id), 0) AS max_id FROM paper_missed_signal_attribution').get().max_id || 0);
      const idFloor = Math.max(0, maxMissedId - scanLimit);
      const whereSql = sinceTs
        ? `${completeWindow ? 'WHERE' : 'WHERE id >= @idFloor AND'} ${missedEventTsExpr} >= @since`
        : 'WHERE id >= @idFloor';
      const whereParams = sinceTs ? { since: sinceTs, idFloor } : { idFloor };
      const hasTradability = missedCols.has('tradable_missed');
      const hasDecisionEvents = tableNames.has('paper_decision_events');
      const spreadAbortExistsSql = '0';
      const quoteExecutableBaseExpression = hasTradability ? `
          CASE
            WHEN tradable_missed = 1
             AND COALESCE(would_stop_before_peak, 0) != 1
             AND NOT (${spreadAbortExistsSql})
            THEN 1 ELSE 0
          END` : 'NULL';
      const quoteExecutableSelect = 'quote_executable_proxy,';
      const tradabilitySelect = hasTradability ? `
          tradable_missed,
          tradability_status,
          tradability_reason,
          tradable_peak_pnl,
          tradable_peak_horizon,
          time_to_peak_sec,
          mae_before_peak_pnl,
          would_stop_before_peak,
          stop_floor_pnl,
          first_tradable_horizon,
          first_tradable_pnl,
          ${quoteExecutableSelect}` : `
          NULL AS tradable_missed,
          NULL AS tradability_status,
          NULL AS tradability_reason,
          NULL AS tradable_peak_pnl,
          NULL AS tradable_peak_horizon,
          NULL AS time_to_peak_sec,
          NULL AS mae_before_peak_pnl,
          NULL AS would_stop_before_peak,
          NULL AS stop_floor_pnl,
          NULL AS first_tradable_horizon,
          NULL AS first_tradable_pnl,
          NULL AS quote_executable_proxy,`;
      const tradabilityAgg = hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN tradability_status = 'would_stop_before_peak' THEN 1 ELSE 0 END) AS stop_before_peak_n,` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n,`;
      const maxPnlExpr = 'COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999)';
      const topDogs = summaryOnly ? [] : paperDb.prepare(`
        SELECT
          symbol,
          token_ca,
          route,
          component,
          reject_reason,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl_recorded,
          min_pnl_recorded,
          ${tradabilitySelect}
          status,
          updated_at
        FROM (
          SELECT
            *,
            ${quoteExecutableBaseExpression} AS quote_executable_proxy
          FROM paper_missed_signal_attribution
          ${whereSql}
        ) paper_missed_signal_attribution
        ORDER BY ${maxPnlExpr} DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      const byGate = paperDb.prepare(`
        SELECT
          COALESCE(route, '-') AS route,
          component,
          reject_reason,
          COUNT(*) AS n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 0.5 THEN 1 ELSE 0 END) AS dog50_n,
          SUM(CASE WHEN COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0) >= 1.0 THEN 1 ELSE 0 END) AS dog100_n,
          ${tradabilityAgg}
          AVG(pnl_5m) AS avg_5m,
          AVG(pnl_15m) AS avg_15m,
          AVG(pnl_60m) AS avg_60m,
          AVG(pnl_24h) AS avg_24h
        FROM paper_missed_signal_attribution
        ${whereSql}
        GROUP BY COALESCE(route, '-'), component, reject_reason
        ORDER BY dog100_n DESC, dog50_n DESC, n DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      const eventTierSummary = paperDb.prepare(`
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM paper_missed_signal_attribution
        ${whereSql}
      `).get(whereParams);
      const uniqueTierSummary = paperDb.prepare(`
        WITH per_token AS (
          SELECT
            token_ca,
            COALESCE(MAX(symbol), '?') AS symbol,
            MIN(COALESCE(signal_ts, created_event_ts, baseline_ts, 0)) AS first_event_ts,
            MAX(COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)) AS max_pnl,
            ${hasTradability ? `
	            MAX(COALESCE(tradable_missed, 0)) AS tradable_missed,
	            MAX(COALESCE(would_stop_before_peak, 0)) AS would_stop_before_peak,
	            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_proxy,
	            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy` : `
	            NULL AS tradable_missed,
	            NULL AS would_stop_before_peak,
	            NULL AS clean_tradable_proxy,
	            NULL AS quote_executable_proxy`}
          FROM paper_missed_signal_attribution
          ${whereSql}
          GROUP BY token_ca
        )
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('max_pnl')},
	          ${hasTradability ? `
	          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
	          SUM(CASE WHEN clean_tradable_proxy = 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
	          SUM(CASE WHEN quote_executable_proxy = 1 THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
	          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM per_token
      `).get(whereParams);
      const athEventTierSummary = paperDb.prepare(`
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)')},
          ${hasTradability ? `
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM paper_missed_signal_attribution
        ${whereSql ? `${whereSql} AND COALESCE(route, '') = 'ATH'` : "WHERE COALESCE(route, '') = 'ATH'"}
      `).get(whereParams);
      const athUniqueTierSummary = paperDb.prepare(`
        WITH per_token AS (
          SELECT
            token_ca,
            MAX(COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m, 0)) AS max_pnl,
            ${hasTradability ? `
	            MAX(COALESCE(tradable_missed, 0)) AS tradable_missed,
	            MAX(COALESCE(would_stop_before_peak, 0)) AS would_stop_before_peak,
	            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_proxy,
	            MAX(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 AND NOT (${spreadAbortExistsSql}) THEN 1 ELSE 0 END) AS quote_executable_proxy` : `
	            NULL AS tradable_missed,
	            NULL AS would_stop_before_peak,
	            NULL AS clean_tradable_proxy,
	            NULL AS quote_executable_proxy`}
          FROM paper_missed_signal_attribution
          ${whereSql ? `${whereSql} AND COALESCE(route, '') = 'ATH'` : "WHERE COALESCE(route, '') = 'ATH'"}
          GROUP BY token_ca
        )
        SELECT
          COUNT(*) AS total_n,
          ${tierCaseSql('max_pnl')},
	          ${hasTradability ? `
	          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_n,
	          SUM(CASE WHEN clean_tradable_proxy = 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
	          SUM(CASE WHEN quote_executable_proxy = 1 THEN 1 ELSE 0 END) AS quote_executable_proxy_n,
	          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_n` : `
          NULL AS tradable_n,
          NULL AS clean_tradable_n,
          NULL AS quote_executable_proxy_n,
          NULL AS stop_before_peak_n`}
        FROM per_token
      `).get(whereParams);
      let athRecoveryActions = [];
      if (hasDecisionEvents && !summaryOnly) {
        const recoveryWhere = sinceTs ? 'AND event_ts >= @since' : '';
        athRecoveryActions = paperDb.prepare(`
          SELECT
            reason AS recovery_action,
            decision,
            COUNT(*) AS n,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component = 'ath_recovery'
            ${recoveryWhere}
          GROUP BY reason, decision
          ORDER BY n DESC
          LIMIT @limit
        `).all({ ...whereParams, limit });
      }
      const topUniqueDogs = summaryOnly ? [] : paperDb.prepare(`
        WITH ranked AS (
          SELECT
            *,
            ${quoteExecutableBaseExpression} AS quote_executable_proxy,
            COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) AS max_pnl,
            ROW_NUMBER() OVER (
              PARTITION BY token_ca
              ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m, -999) DESC
            ) AS rn
          FROM paper_missed_signal_attribution
          ${whereSql}
        )
        SELECT
          symbol,
          token_ca,
          route,
          component,
          reject_reason,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl_recorded,
          min_pnl_recorded,
          ${tradabilitySelect}
          status,
          updated_at
        FROM ranked
        WHERE rn = 1
        ORDER BY max_pnl DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        limit,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          complete_window: completeWindow,
          summary_only: summaryOnly,
          max_window_hours: 24,
          tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
          quote_executable_proxy_note: 'spread-abort timing is not checked in this endpoint so the summary stays non-blocking; use missed-recovery-summary for token-window spread filtering',
        },
        query_ms: Date.now() - queryStartedAt,
        tier_summary: {
          event_rows: eventTierSummary,
          unique_tokens: uniqueTierSummary,
          ath_event_rows: athEventTierSummary,
          ath_unique_tokens: athUniqueTierSummary,
        },
        top_dogs: topDogs,
        top_unique_dogs: topUniqueDogs,
        by_gate: byGate,
        ath_recovery_actions: athRecoveryActions,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/missed-recovery-summary') {
    // Fast missed-dog recovery loop summary: unique clean/quote-executable dogs by route/blocker.
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const limit = boundedIntParam(url, 'limit', 50, 1, 80);
      const sinceTs = boundedWindowedSinceTs(url, 2, 24);
      const queryStartedAt = Date.now();
      const includeRecoveryActions = String(url.searchParams.get('include_actions') || '').toLowerCase() === '1';
      const requestedHours = Number.parseInt(url.searchParams.get('hours') || '2', 10);
      const forceLive = ['1', 'true', 'yes'].includes(String(url.searchParams.get('live') || '').toLowerCase())
        || ['0', 'false', 'no'].includes(String(url.searchParams.get('materialized') || '').toLowerCase());
      if (shouldUseMaterializedMissedRecoverySummary(requestedHours, forceLive)) {
        const liveSnapshot = readLivePaperReview(requestedHours);
        if (liveSnapshot && !liveSnapshot.error) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify(missedRecoverySummaryFromLiveSnapshot(liveSnapshot, {
            dbPath: paperDbPath,
            requestedHours,
            limit,
          }), null, 2));
          return;
        }
        res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          available: false,
          materialized: true,
          reason: liveSnapshot?.error || 'materialized_snapshot_not_ready',
          path: livePaperReviewPath(requestedHours),
        }, null, 2));
        return;
      }
      const eventTsExpr = 'COALESCE(m.created_event_ts, m.signal_ts, m.baseline_ts, 0)';
      const whereParams = sinceTs ? { since: sinceTs } : {};
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (!tableNames.has('paper_missed_signal_attribution')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_missed_signal_attribution table not found' }));
        return;
      }
      const missedCols = new Set(
        paperDb.prepare("PRAGMA table_info(paper_missed_signal_attribution)").all().map((row) => row.name)
      );
      const trustedMissedPeakExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
      const tradeCols = tableNames.has('paper_trades')
        ? new Set(paperDb.prepare("PRAGMA table_info(paper_trades)").all().map((row) => row.name))
        : new Set();
      const tradeWindowPredicates = [
        tradeCols.has('entry_ts') ? 'COALESCE(pt.entry_ts, 0) >= @since' : null,
        tradeCols.has('exit_ts') ? 'COALESCE(pt.exit_ts, 0) >= @since' : null,
        tradeCols.has('accounting_outcome') ? "COALESCE(pt.accounting_outcome, '') = 'open'" : null,
        tradeCols.has('created_at') ? 'COALESCE(strftime(\'%s\', pt.created_at), 0) >= @since' : null,
      ].filter(Boolean);
      const tradeWindowFilter = sinceTs && tradeWindowPredicates.length > 0
        ? `AND (${tradeWindowPredicates.join(' OR ')})`
        : '';
      const caughtTokenFilter = tableNames.has('paper_trades') && tradeCols.has('token_ca')
        ? `AND NOT EXISTS (
            SELECT 1
            FROM paper_trades pt
            WHERE pt.token_ca = m.token_ca
              ${tradeWindowFilter}
          )`
        : '';
      const whereSql = `
        WHERE 1 = 1
          ${sinceTs ? 'AND m.created_event_ts >= @since' : ''}
          ${caughtTokenFilter}
      `;
      const hasTradability = missedCols.has('tradable_missed');
      const hasDecisionEvents = tableNames.has('paper_decision_events');
      const quoteExecExpr = hasTradability ? `
          CASE
            WHEN COALESCE(m.tradable_missed, 0) = 1
             AND COALESCE(m.would_stop_before_peak, 0) != 1
            THEN 1 ELSE 0
          END` : 'NULL';
      const tradableSelect = hasTradability ? `
            COALESCE(m.tradable_missed, 0) AS tradable_missed,
            COALESCE(m.would_stop_before_peak, 0) AS would_stop_before_peak,
            ${missedCols.has('tradability_status') ? 'm.tradability_status' : 'NULL'} AS tradability_status,
            ${missedCols.has('tradability_reason') ? 'm.tradability_reason' : 'NULL'} AS tradability_reason,
            ${missedCols.has('first_tradable_ts') ? 'm.first_tradable_ts' : 'NULL'} AS first_tradable_ts,
            ${missedCols.has('first_tradable_pnl') ? 'm.first_tradable_pnl' : 'NULL'} AS first_tradable_pnl,
            ${missedCols.has('tradable_peak_pnl') ? 'm.tradable_peak_pnl' : 'NULL'} AS tradable_peak_pnl,` : `
            NULL AS tradable_missed,
            NULL AS would_stop_before_peak,
            NULL AS tradability_status,
            NULL AS tradability_reason,
            NULL AS first_tradable_ts,
            NULL AS first_tradable_pnl,
            NULL AS tradable_peak_pnl,`;
      const baseCte = `
        WITH base AS (
          SELECT
            m.token_ca,
            COALESCE(m.symbol, substr(m.token_ca, 1, 8), '?') AS symbol,
            COALESCE(m.route, '-') AS route,
            COALESCE(m.component, '-') AS component,
            COALESCE(m.reject_reason, '-') AS reject_reason,
            ${eventTsExpr} AS event_ts,
            ${trustedMissedPeakExpr} AS max_pnl,
            m.pnl_5m,
            m.pnl_15m,
            m.pnl_60m,
            m.pnl_24h,
            ${tradableSelect}
            ${quoteExecExpr} AS quote_exec
          FROM paper_missed_signal_attribution m
          ${whereSql}
        ),
        per_token AS (
          SELECT
            token_ca,
            MAX(symbol) AS symbol,
            MAX(route) AS route,
            MAX(component) AS component,
            MAX(reject_reason) AS reject_reason,
            MAX(event_ts) AS event_ts,
            MAX(max_pnl) AS max_pnl,
            MAX(pnl_5m) AS pnl_5m,
            MAX(pnl_15m) AS pnl_15m,
            MAX(pnl_60m) AS pnl_60m,
            MAX(pnl_24h) AS pnl_24h,
            MAX(tradable_missed) AS tradable_missed,
            MAX(would_stop_before_peak) AS would_stop_before_peak,
            MAX(tradability_status) AS tradability_status,
            MAX(tradability_reason) AS tradability_reason,
            MAX(first_tradable_ts) AS first_tradable_ts,
            MAX(first_tradable_pnl) AS first_tradable_pnl,
            MAX(tradable_peak_pnl) AS tradable_peak_pnl,
            MAX(quote_exec) AS quote_exec
          FROM base
          GROUP BY token_ca
        ),
        per_route_token AS (
          SELECT
            route,
            token_ca,
            MAX(max_pnl) AS max_pnl,
            MAX(tradable_missed) AS tradable_missed,
            MAX(would_stop_before_peak) AS would_stop_before_peak,
            MAX(quote_exec) AS quote_exec
          FROM base
          GROUP BY route, token_ca
        ),
        per_blocker_token AS (
          SELECT
            route,
            component,
            reject_reason,
            token_ca,
            MAX(max_pnl) AS max_pnl,
            MAX(tradable_missed) AS tradable_missed,
            MAX(would_stop_before_peak) AS would_stop_before_peak,
            MAX(quote_exec) AS quote_exec
          FROM base
          GROUP BY route, component, reject_reason, token_ca
        )`;
      const queueActionCte = (() => {
        if (!tableNames.has('paper_fast_entry_queue')) {
          return `,
        queue_action AS (
          SELECT
            NULL AS token_ca,
            NULL AS fast_queue_status,
            NULL AS fast_queue_reason,
            NULL AS fast_queue_branch,
            NULL AS fast_queue_source_type,
            NULL AS fast_queue_updated_at,
            NULL AS recovery_action_status
          WHERE 0
        )`;
        }
        const queueCols = getTableColumns(paperDb, 'paper_fast_entry_queue');
        const queueUpdatedExpr = queueCols.has('updated_at') ? 'updated_at' : 'created_at';
        const queueReasonExpr = queueCols.has('first_error')
          ? (queueCols.has('last_error') ? "COALESCE(first_error, last_error, 'none')" : "COALESCE(first_error, 'none')")
          : (queueCols.has('last_error') ? "COALESCE(last_error, 'none')" : "'none'");
        const queueBranchExpr = queueCols.has('entry_branch') ? 'entry_branch' : (queueCols.has('source_type') ? 'source_type' : "'unknown'");
        const queueSourceExpr = queueCols.has('source_type') ? 'source_type' : "'unknown'";
        const queueSignalTsExpr = queueCols.has('source_signal_ts') ? 'COALESCE(source_signal_ts, 0)' : '0';
        const queueSinceFilter = sinceTs ? `WHERE ${queueUpdatedExpr} >= @since OR ${queueSignalTsExpr} >= @since` : '';
        return `,
        queue_ranked AS (
          SELECT
            token_ca,
            status AS fast_queue_status,
            ${queueReasonExpr} AS fast_queue_reason,
            ${queueBranchExpr} AS fast_queue_branch,
            ${queueSourceExpr} AS fast_queue_source_type,
            ${queueUpdatedExpr} AS fast_queue_updated_at,
            ROW_NUMBER() OVER (
              PARTITION BY token_ca
              ORDER BY ${queueUpdatedExpr} DESC, id DESC
            ) AS rn
          FROM paper_fast_entry_queue
          ${queueSinceFilter}
        ),
        queue_action AS (
          SELECT
            token_ca,
            fast_queue_status,
            fast_queue_reason,
            fast_queue_branch,
            fast_queue_source_type,
            fast_queue_updated_at,
            CASE
              WHEN fast_queue_status = 'entered' THEN 'entered'
              WHEN fast_queue_status IN ('queued', 'claimed', 'retry_watch') THEN 'active_queue'
              WHEN fast_queue_status IN ('watch_only', 'counterfactual_only') THEN fast_queue_status
              WHEN fast_queue_status IS NULL THEN 'no_recovery_action'
              ELSE fast_queue_status
            END AS recovery_action_status
          FROM queue_ranked
          WHERE rn = 1
        )`;
      })();
      const summaryRows = paperDb.prepare(`
        ${baseCte}
        SELECT * FROM (
          SELECT
            'overall' AS section,
            NULL AS route,
            NULL AS component,
            NULL AS reject_reason,
            COUNT(*) AS unique_tokens,
            ${tierCaseSql('max_pnl')},
            ${hasTradability ? `
            SUM(CASE WHEN quote_exec = 1 THEN 1 ELSE 0 END) AS quote_executable_unique,
            SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
            SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
            SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique,` : `
            NULL AS quote_executable_unique,
            NULL AS clean_tradable_unique,
            NULL AS tradable_unique,
            NULL AS stop_before_peak_unique,`}
            MAX(max_pnl) AS max_pnl,
            AVG(max_pnl) AS avg_max_pnl
          FROM per_token
          UNION ALL
          SELECT
            'by_route' AS section,
            route,
            NULL AS component,
            NULL AS reject_reason,
            COUNT(*) AS unique_tokens,
            ${tierCaseSql('max_pnl')},
            ${hasTradability ? `
            SUM(CASE WHEN quote_exec = 1 THEN 1 ELSE 0 END) AS quote_executable_unique,
            SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
            SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
            SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique,` : `
            NULL AS quote_executable_unique,
            NULL AS clean_tradable_unique,
            NULL AS tradable_unique,
            NULL AS stop_before_peak_unique,`}
            MAX(max_pnl) AS max_pnl,
            AVG(max_pnl) AS avg_max_pnl
          FROM per_route_token
          GROUP BY route
          UNION ALL
          SELECT
            'by_blocker_clean_quote' AS section,
            route,
            component,
            reject_reason,
            COUNT(*) AS unique_tokens,
            ${tierCaseSql('max_pnl')},
            COUNT(*) AS quote_executable_unique,
            SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
            SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
            SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique,
            MAX(max_pnl) AS max_pnl,
            AVG(max_pnl) AS avg_max_pnl
          FROM per_blocker_token
          WHERE quote_exec = 1
          GROUP BY route, component, reject_reason
          UNION ALL
          SELECT
            'by_blocker_all_unique' AS section,
            route,
            component,
            reject_reason,
            COUNT(*) AS unique_tokens,
            ${tierCaseSql('max_pnl')},
          ${hasTradability ? `
          SUM(CASE WHEN quote_exec = 1 THEN 1 ELSE 0 END) AS quote_executable_unique,
          SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
          SUM(CASE WHEN tradable_missed = 1 THEN 1 ELSE 0 END) AS tradable_unique,
          SUM(CASE WHEN COALESCE(would_stop_before_peak, 0) = 1 THEN 1 ELSE 0 END) AS stop_before_peak_unique` : `
          NULL AS quote_executable_unique,
          NULL AS clean_tradable_unique,
          NULL AS tradable_unique,
          NULL AS stop_before_peak_unique`}
            ,
            MAX(max_pnl) AS max_pnl,
            AVG(max_pnl) AS avg_max_pnl
          FROM per_blocker_token
          GROUP BY route, component, reject_reason
        )
        ORDER BY
          CASE section
            WHEN 'overall' THEN 0
            WHEN 'by_route' THEN 1
            WHEN 'by_blocker_clean_quote' THEN 2
            ELSE 3
          END,
          gold_n DESC,
          silver_n DESC,
          bronze_n DESC,
          unique_tokens DESC,
          max_pnl DESC
      `).all(whereParams);
      const overall = summaryRows.find((row) => row.section === 'overall') || null;
      const byRoute = summaryRows.filter((row) => row.section === 'by_route');
      const byBlockerCleanQuote = summaryRows.filter((row) => row.section === 'by_blocker_clean_quote').slice(0, limit);
      const byBlockerAllUnique = summaryRows.filter((row) => row.section === 'by_blocker_all_unique').slice(0, limit);
      const topCleanQuoteDogs = paperDb.prepare(`
        ${baseCte}
        ${queueActionCte}
        SELECT
          p.symbol,
          p.token_ca,
          p.route,
          p.component,
          p.reject_reason,
          p.max_pnl,
          p.pnl_5m,
          p.pnl_15m,
          p.pnl_60m,
          p.pnl_24h,
          p.tradability_status,
          p.tradability_reason,
          p.first_tradable_ts,
          p.first_tradable_pnl,
          p.tradable_peak_pnl,
          p.event_ts,
          q.fast_queue_status,
          q.fast_queue_reason,
          q.fast_queue_branch,
          q.fast_queue_source_type,
          q.fast_queue_updated_at,
          COALESCE(q.recovery_action_status, 'no_recovery_action') AS recovery_action_status
        FROM per_token
        p
        LEFT JOIN queue_action q ON q.token_ca = p.token_ca
        WHERE quote_exec = 1
        ORDER BY max_pnl DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      const recoveryActionability = paperDb.prepare(`
        ${baseCte}
        ${queueActionCte}
        SELECT
          COALESCE(q.recovery_action_status, 'no_recovery_action') AS recovery_action_status,
          COALESCE(q.fast_queue_reason, 'none') AS fast_queue_reason,
          COUNT(*) AS unique_tokens,
          ${tierCaseSql('p.max_pnl')},
          MAX(p.max_pnl) AS max_pnl
        FROM per_token p
        LEFT JOIN queue_action q ON q.token_ca = p.token_ca
        WHERE p.quote_exec = 1
        GROUP BY
          COALESCE(q.recovery_action_status, 'no_recovery_action'),
          COALESCE(q.fast_queue_reason, 'none')
        ORDER BY unique_tokens DESC, gold_n DESC, silver_n DESC, bronze_n DESC, max_pnl DESC
        LIMIT @limit
      `).all({ ...whereParams, limit });
      let recoveryActions = [];
      if (hasDecisionEvents && includeRecoveryActions) {
        const recoveryWhere = sinceTs ? 'AND event_ts >= @since' : '';
        recoveryActions = paperDb.prepare(`
          SELECT
            component,
            decision,
            reason,
            COUNT(*) AS n,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component IN ('ath_recovery', 'discovery_tracking', 'lotto_upstream_probe_live')
            ${recoveryWhere}
            AND reason IN (
              'ath_reclaim_after_failure_pass',
              'ath_reclaim_after_failure_price_not_recovered',
              'ath_reclaim_after_failure_buy_pressure_weak',
              'ath_reclaim_after_failure_tx_low',
              'ath_reclaim_after_failure_quote_not_executable',
              'ath_matrix_dissonance_pass',
              'ath_matrix_dissonance_not_live_confirmed',
              'ath_matrix_dissonance_quote_not_executable',
              'ath_micro_reclaim_probe_pass',
              'ath_micro_reclaim_bounce_not_confirmed',
              'ath_micro_reclaim_buy_pressure_weak',
              'ath_micro_reclaim_quote_not_executable',
              'tracking_ttl_expired',
              'tracking_ttl_final_reclaim_check'
            )
          GROUP BY component, decision, reason
          ORDER BY n DESC
          LIMIT @limit
        `).all({ ...whereParams, limit });
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
          clean_quote_definition: 'tradable_missed=1 and would_stop_before_peak!=1',
          quote_executable_proxy_note: 'spread-abort timing is not checked in this endpoint so the dashboard stays non-blocking; use offline analysis for exact per-row spread timing',
          include_actions: includeRecoveryActions,
        },
        query_ms: Date.now() - queryStartedAt,
        overall_unique: overall,
        by_route: byRoute,
        by_blocker_clean_quote: byBlockerCleanQuote,
        by_blocker_all_unique: byBlockerAllUnique,
        top_clean_quote_dogs: topCleanQuoteDogs,
        recovery_actionability: recoveryActionability,
        recovery_actions: recoveryActions,
        notes: {
          endpoint_goal: 'fast closed-loop summary for missed gold/silver/bronze recovery, avoiding full attribution scans',
          recovery_actions: includeRecoveryActions ? 'included' : 'omitted by default; pass include_actions=1 for the slower decision-events breakdown',
          anchor_mismatch: 'not computed here; use lifecycle-summary for trade anchor audit',
        },
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/not-ath-watch-shadow-backtest') {
    // Shadow backtest for LOTTO NOT_ATH watch blockers.
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const queryStartedAt = Date.now();
      const sinceTs = reportSinceTs(url, '7d');
      const limit = boundedIntParam(url, 'limit', 25, 1, 100);
      const min5m = Number(url.searchParams.get('min_5m') || '0.05');
      const min15m = Number(url.searchParams.get('min_15m') || '0.10');
      const retention = Number(url.searchParams.get('retention') || '0.50');
      const params = {
        since: sinceTs || 0,
        min5m: Number.isFinite(min5m) ? min5m : 0.05,
        min15m: Number.isFinite(min15m) ? min15m : 0.10,
        retention: Number.isFinite(retention) ? retention : 0.50,
        limit,
      };
      const whereSql = sinceTs ? 'AND m.created_event_ts >= @since' : '';
      const eventWhereSql = sinceTs ? 'AND event_ts >= @since' : '';
      const snapshotWhereSql = sinceTs ? 'AND snapshot_ts >= @since' : '';
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      if (!tableNames.has('paper_missed_signal_attribution')) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'paper_missed_signal_attribution table not found' }));
        return;
      }
      const missedCols = getTableColumns(paperDb, 'paper_missed_signal_attribution');
      const trustedMissedPeakExpr = trustedMissedPeakSqlExpr(missedCols, 'm');
      const baseCte = `
        WITH base AS (
          SELECT
            m.id,
            m.token_ca,
            COALESCE(m.symbol, substr(m.token_ca, 1, 8), '?') AS symbol,
            m.signal_ts,
            m.created_event_ts,
            m.baseline_price,
            m.pnl_5m,
            m.pnl_15m,
            m.pnl_60m,
            m.pnl_24h,
            ${trustedMissedPeakExpr} AS max_pnl,
            COALESCE(m.tradable_missed, 0) AS tradable_missed,
            COALESCE(m.would_stop_before_peak, 0) AS would_stop_before_peak,
            m.tradability_status,
            CASE
              WHEN m.pnl_5m IS NULL OR m.pnl_15m IS NULL THEN 'awaiting_two_horizon_confirmation'
              WHEN m.pnl_5m < @min5m THEN 'not_ath_watch_5m_reclaim_weak'
              WHEN m.pnl_15m < @min15m THEN 'not_ath_watch_15m_reclaim_weak'
              WHEN m.pnl_15m < m.pnl_5m * @retention THEN 'not_ath_watch_reclaim_decayed'
              ELSE 'not_ath_two_horizon_reclaim_confirmed'
            END AS shadow_reason,
            CASE
              WHEN m.pnl_5m IS NOT NULL
               AND m.pnl_15m IS NOT NULL
               AND m.pnl_5m >= @min5m
               AND m.pnl_15m >= @min15m
               AND m.pnl_15m >= m.pnl_5m * @retention
              THEN 1 ELSE 0
            END AS would_enter
          FROM paper_missed_signal_attribution m
          WHERE m.route = 'LOTTO'
            AND m.component IN ('upstream_gate', 'lotto_entry_gate')
            AND (${NOT_ATH_WATCH_MISSED_REJECT_MATCH_SQL})
            AND m.baseline_price IS NOT NULL
            ${whereSql}
        ),
        ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY token_ca
              ORDER BY max_pnl DESC, created_event_ts DESC
            ) AS rn
          FROM base
        ),
        unique_token AS (
          SELECT * FROM ranked WHERE rn = 1
        )`;
      const summary = paperDb.prepare(`
        ${baseCte}
        SELECT
          COUNT(*) AS unique_tokens,
          SUM(CASE WHEN pnl_5m IS NOT NULL THEN 1 ELSE 0 END) AS has_5m,
          SUM(CASE WHEN pnl_15m IS NOT NULL THEN 1 ELSE 0 END) AS has_15m,
          SUM(CASE WHEN would_enter = 1 THEN 1 ELSE 0 END) AS would_enter_unique,
          SUM(CASE WHEN would_enter = 1 AND tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END) AS would_enter_clean_tradable,
          SUM(CASE WHEN tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
          SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END) AS gold_n,
          SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS silver_n,
          SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS bronze_n,
          SUM(CASE WHEN would_enter = 1 AND max_pnl >= 1.0 THEN 1 ELSE 0 END) AS would_enter_gold_n,
          SUM(CASE WHEN would_enter = 1 AND max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS would_enter_silver_n,
          SUM(CASE WHEN would_enter = 1 AND max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS would_enter_bronze_n,
          MAX(max_pnl) AS max_pnl,
          AVG(max_pnl) AS avg_max_pnl
        FROM unique_token
      `).get(params);
      const byReason = paperDb.prepare(`
        ${baseCte}
        SELECT
          shadow_reason,
          COUNT(*) AS unique_tokens,
          SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END) AS gold_n,
          SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS silver_n,
          SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS bronze_n,
          SUM(CASE WHEN tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END) AS clean_tradable_unique,
          MAX(max_pnl) AS max_pnl
        FROM unique_token
        GROUP BY shadow_reason
        ORDER BY unique_tokens DESC, gold_n DESC
      `).all(params);
      const topWouldEnter = paperDb.prepare(`
        ${baseCte}
        SELECT
          symbol,
          token_ca,
          signal_ts,
          created_event_ts,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl,
          tradable_missed,
          would_stop_before_peak,
          tradability_status,
          shadow_reason
        FROM unique_token
        WHERE would_enter = 1
        ORDER BY max_pnl DESC
        LIMIT @limit
      `).all(params);
      const topMissedStillRejected = paperDb.prepare(`
        ${baseCte}
        SELECT
          symbol,
          token_ca,
          signal_ts,
          created_event_ts,
          pnl_5m,
          pnl_15m,
          pnl_60m,
          pnl_24h,
          max_pnl,
          tradable_missed,
          would_stop_before_peak,
          tradability_status,
          shadow_reason
        FROM unique_token
        WHERE would_enter = 0
        ORDER BY max_pnl DESC
        LIMIT @limit
      `).all(params);
      let shadowEvents = [];
      let shadowOutcomeSummary = null;
      let shadowOutcomeByReason = [];
      let shadowSnapshotSummary = null;
      let shadowSnapshotByReason = [];
      let relaxedShadowCohorts = {
        available: false,
        cohorts: [],
        top_hits: [],
        note: 'Not evaluated.',
      };
      if (tableNames.has('paper_decision_events')) {
        shadowEvents = paperDb.prepare(`
          SELECT
            event_type,
            decision,
            reason,
            COUNT(*) AS n,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component = 'lotto_not_ath_watch_shadow'
            ${eventWhereSql}
          GROUP BY event_type, decision, reason
          ORDER BY n DESC
        `).all(params);
        const shadowOutcomeCte = `
          WITH terminal_ranked AS (
            SELECT
              e.id AS event_id,
              e.event_ts,
              e.token_ca,
              e.symbol AS event_symbol,
              e.signal_ts,
              e.event_type,
              e.decision,
              e.reason,
              ROW_NUMBER() OVER (
                PARTITION BY e.token_ca, COALESCE(e.signal_ts, 0)
                ORDER BY
                  CASE e.event_type
                    WHEN 'would_enter' THEN 3
                    WHEN 'watch_rejected' THEN 2
                    ELSE 1
                  END DESC,
                  e.event_ts DESC,
                  e.id DESC
              ) AS terminal_rn
            FROM paper_decision_events e
            WHERE e.component = 'lotto_not_ath_watch_shadow'
              ${eventWhereSql}
              AND e.event_type IN ('would_enter', 'watch_rejected', 'watch_expired')
          ),
          terminal AS (
            SELECT
              event_id,
              event_ts,
              token_ca,
              event_symbol,
              signal_ts,
              event_type,
              decision,
              reason
            FROM terminal_ranked
            WHERE terminal_rn = 1
          ),
          joined AS (
            SELECT
              t.*,
              COALESCE(m.symbol, t.event_symbol, substr(t.token_ca, 1, 8), '?') AS symbol,
              ${trustedMissedPeakExpr} AS max_pnl,
              COALESCE(m.tradable_missed, 0) AS tradable_missed,
              COALESCE(m.would_stop_before_peak, 0) AS would_stop_before_peak,
              m.tradability_status,
              ROW_NUMBER() OVER (
                PARTITION BY t.event_id
                ORDER BY ${trustedMissedPeakExpr} DESC
              ) AS rn
            FROM terminal t
            LEFT JOIN paper_missed_signal_attribution m
              ON m.token_ca = t.token_ca
             AND COALESCE(m.signal_ts, 0) = COALESCE(t.signal_ts, 0)
             AND m.route = 'LOTTO'
             AND (${NOT_ATH_WATCH_MISSED_REJECT_MATCH_SQL})
          ),
          one AS (
            SELECT * FROM joined WHERE rn = 1
          )`;
        shadowOutcomeSummary = paperDb.prepare(`
          ${shadowOutcomeCte}
          SELECT
            COUNT(*) AS terminal_events,
            COUNT(DISTINCT token_ca) AS unique_tokens,
            COALESCE(SUM(CASE WHEN event_type = 'would_enter' THEN 1 ELSE 0 END), 0) AS would_enter_n,
            COUNT(DISTINCT CASE WHEN event_type = 'would_enter' THEN token_ca END) AS would_enter_unique,
            COALESCE(SUM(CASE WHEN event_type = 'would_enter' AND tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END), 0) AS would_enter_clean_tradable,
            COALESCE(SUM(CASE WHEN event_type = 'would_enter' AND max_pnl >= 1.0 THEN 1 ELSE 0 END), 0) AS would_enter_gold_n,
            COALESCE(SUM(CASE WHEN event_type = 'would_enter' AND max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END), 0) AS would_enter_silver_n,
            COALESCE(SUM(CASE WHEN event_type = 'would_enter' AND max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END), 0) AS would_enter_bronze_n,
            COALESCE(SUM(CASE WHEN event_type != 'would_enter' AND max_pnl >= 1.0 THEN 1 ELSE 0 END), 0) AS rejected_gold_n,
            COALESCE(SUM(CASE WHEN event_type != 'would_enter' AND max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END), 0) AS rejected_silver_n,
            COALESCE(SUM(CASE WHEN event_type != 'would_enter' AND max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END), 0) AS rejected_bronze_n,
            MAX(max_pnl) AS max_pnl
          FROM one
        `).get(params);
        shadowOutcomeByReason = paperDb.prepare(`
          ${shadowOutcomeCte}
          SELECT
            event_type,
            decision,
            reason,
            COUNT(*) AS n,
            COUNT(DISTINCT token_ca) AS unique_tokens,
            SUM(CASE WHEN tradable_missed = 1 AND would_stop_before_peak != 1 THEN 1 ELSE 0 END) AS clean_tradable_n,
            SUM(CASE WHEN max_pnl >= 1.0 THEN 1 ELSE 0 END) AS gold_n,
            SUM(CASE WHEN max_pnl >= 0.5 AND max_pnl < 1.0 THEN 1 ELSE 0 END) AS silver_n,
            SUM(CASE WHEN max_pnl >= 0.25 AND max_pnl < 0.5 THEN 1 ELSE 0 END) AS bronze_n,
            MAX(max_pnl) AS max_pnl
          FROM one
          GROUP BY event_type, decision, reason
          ORDER BY n DESC, gold_n DESC
        `).all(params);
      }
      if (tableNames.has('lotto_not_ath_watch_shadow_snapshots')) {
        shadowSnapshotSummary = paperDb.prepare(`
          WITH base AS (
            SELECT *
            FROM lotto_not_ath_watch_shadow_snapshots
            WHERE parent_blocker IN (${NOT_ATH_WATCH_PARENT_BLOCKER_SQL})
              ${snapshotWhereSql}
          ),
          pass_pairs AS (
            SELECT
              s1.token_ca,
              COALESCE(s1.signal_ts, 0) AS signal_ts
            FROM base s1
            JOIN base s2
              ON s2.token_ca = s1.token_ca
             AND COALESCE(s2.signal_ts, 0) = COALESCE(s1.signal_ts, 0)
             AND s2.horizon_sec = s1.horizon_sec + 300
            WHERE s1.snapshot_pass = 1
              AND s2.snapshot_pass = 1
            GROUP BY s1.token_ca, COALESCE(s1.signal_ts, 0)
          )
          SELECT
            COUNT(*) AS snapshots,
            COUNT(DISTINCT token_ca) AS unique_tokens,
            COALESCE(SUM(CASE WHEN quote_clean = 1 THEN 1 ELSE 0 END), 0) AS quote_clean_n,
            COUNT(DISTINCT CASE WHEN quote_clean = 1 THEN token_ca END) AS quote_clean_unique,
            COALESCE(SUM(CASE WHEN snapshot_pass = 1 THEN 1 ELSE 0 END), 0) AS snapshot_pass_n,
            COUNT(DISTINCT CASE WHEN snapshot_pass = 1 THEN token_ca END) AS snapshot_pass_unique,
            (SELECT COUNT(*) FROM pass_pairs) AS two_snapshot_confirmations,
            AVG(quote_gap_pct) AS avg_quote_gap_pct,
            MAX(ABS(quote_gap_pct)) AS max_abs_quote_gap_pct,
            AVG(spread_pct) AS avg_spread_pct,
            MAX(spread_pct) AS max_spread_pct,
            AVG(liquidity_usd) AS avg_liquidity_usd,
            MIN(snapshot_ts) AS first_snapshot_ts,
            MAX(snapshot_ts) AS last_snapshot_ts
          FROM base
        `).get(params);
        shadowSnapshotByReason = paperDb.prepare(`
          SELECT
            reason,
            COUNT(*) AS snapshots,
            COUNT(DISTINCT token_ca) AS unique_tokens,
            COALESCE(SUM(CASE WHEN quote_clean = 1 THEN 1 ELSE 0 END), 0) AS quote_clean_n,
            COALESCE(SUM(CASE WHEN snapshot_pass = 1 THEN 1 ELSE 0 END), 0) AS snapshot_pass_n,
            AVG(quote_gap_pct) AS avg_quote_gap_pct,
            AVG(spread_pct) AS avg_spread_pct,
            AVG(liquidity_usd) AS avg_liquidity_usd
          FROM lotto_not_ath_watch_shadow_snapshots
          WHERE parent_blocker IN (${NOT_ATH_WATCH_PARENT_BLOCKER_SQL})
            ${snapshotWhereSql}
          GROUP BY reason
          ORDER BY snapshots DESC, snapshot_pass_n DESC
        `).all(params);
      }
      relaxedShadowCohorts = buildNotAthRelaxedShadowCohorts(paperDb, {
        sinceTs,
        limit,
        snapshotIntervalSec: 300,
        strictConfirmBySec: 30 * 60,
        relaxedLiquidityUsd: Number(url.searchParams.get('relaxed_liq') || '2500'),
        maxQuoteGapPct: Number(url.searchParams.get('max_quote_gap_pct') || '8'),
        maxSpreadPct: Number(url.searchParams.get('max_spread_pct') || '5'),
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        filters: {
          since_ts: sinceTs,
          since_iso: sinceTs ? new Date(sinceTs * 1000).toISOString() : null,
          parent_blocker: 'LOTTO/upstream_gate/not_ath_watch_parent_blockers',
          parent_blockers: NOT_ATH_WATCH_PARENT_BLOCKERS,
          min_5m_pnl: params.min5m,
          min_15m_pnl: params.min15m,
          min_retention: params.retention,
          tier_definition: 'gold>=100%, silver=50-100%, bronze=25-50% max/peak pnl',
          caveat: 'historical proxy uses stored 5m/15m missed-attribution samples; live promotion still requires quote-clean robust EV',
        },
        query_ms: Date.now() - queryStartedAt,
        summary,
        by_reason: byReason,
        top_would_enter: topWouldEnter,
        top_missed_still_rejected: topMissedStillRejected,
        shadow_events: shadowEvents,
        shadow_outcomes: {
          summary: shadowOutcomeSummary,
          by_reason: shadowOutcomeByReason,
          note: 'Uses only actual lotto_not_ath_watch_shadow terminal events; this is the future shadow promotion view, not the historical proxy.',
        },
        shadow_snapshots: {
          summary: shadowSnapshotSummary,
          by_reason: shadowSnapshotByReason,
          note: 'Real future quote-clean snapshot collection for not_ath_v17 and prebuy-kline NOT_ATH blockers; promotion requires two consecutive 5m snapshot_pass samples.',
        },
        relaxed_shadow_cohorts: relaxedShadowCohorts,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/stats/missed-gates') {
    // Most expensive gates: which SmartEntry reject reasons caused the most missed gold/silver dogs.
    // Query paper_decision_events (timing rejects) joined against paper_missed_signal_attribution.
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const hoursBack = boundedIntParam(url, 'hours', 24, 1, 24);
      const sinceTs = Math.floor(Date.now() / 1000) - hoursBack * 3600;
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name)
      );
      const hasEvents = tableNames.has('paper_decision_events');
      const hasMissed = tableNames.has('paper_missed_signal_attribution');
      const missedCols = hasMissed
        ? new Set(paperDb.prepare('PRAGMA table_info(paper_missed_signal_attribution)').all().map((r) => r.name))
        : new Set();
      const hasTradability = missedCols.has('tradable_missed') && missedCols.has('tradable_peak_pnl');

      // Gate stats from decision_events
      let gateRows = [];
      if (hasEvents) {
        gateRows = paperDb.prepare(`
          SELECT
            reason,
            COUNT(*) AS total_rejects,
            COUNT(DISTINCT token_ca) AS unique_tokens
          FROM paper_decision_events
          WHERE component = 'smart_entry'
            AND event_type = 'timing_decision'
            AND decision = 'reject'
            AND event_ts >= @since
          GROUP BY reason
          ORDER BY total_rejects DESC
        `).all({ since: sinceTs });
      }

      // Missed attribution by reject_reason
      let missedRows = [];
      if (hasMissed && hasTradability) {
        missedRows = paperDb.prepare(`
          SELECT
            reject_reason,
            COUNT(*) AS missed_n,
            SUM(CASE WHEN tradable_missed = 1 AND COALESCE(would_stop_before_peak, 0) = 0 THEN 1 ELSE 0 END) AS clean_missed,
            SUM(CASE WHEN COALESCE(tradable_peak_pnl, 0) >= 0.5 THEN 1 ELSE 0 END) AS gold_missed,
            SUM(CASE WHEN COALESCE(tradable_peak_pnl, 0) >= 0.25 AND COALESCE(tradable_peak_pnl, 0) < 0.5 THEN 1 ELSE 0 END) AS silver_missed,
            ROUND(MAX(COALESCE(tradable_peak_pnl, 0)) * 100, 1) AS max_missed_pct
          FROM paper_missed_signal_attribution
          WHERE COALESCE(signal_ts, created_event_ts, 0) >= @since
            AND reject_reason IS NOT NULL
          GROUP BY reject_reason
          ORDER BY gold_missed DESC, silver_missed DESC, clean_missed DESC
        `).all({ since: sinceTs });
      }

      // Merge: attach missed-attribution counts to gate stats
      const missedByReason = {};
      for (const r of missedRows) missedByReason[r.reject_reason] = r;
      const merged = gateRows.map((g) => {
        const m = missedByReason[g.reason] || {};
        return {
          reason: g.reason,
          total_rejects: g.total_rejects,
          unique_tokens: g.unique_tokens,
          missed_n: m.missed_n || 0,
          clean_missed: m.clean_missed || 0,
          gold_missed: m.gold_missed || 0,
          silver_missed: m.silver_missed || 0,
          max_missed_pct: m.max_missed_pct || null,
        };
      });
      // Add any missed-attribution reasons not in decision_events
      for (const r of missedRows) {
        if (!gateRows.find((g) => g.reason === r.reject_reason)) {
          merged.push({
            reason: r.reject_reason,
            total_rejects: 0,
            unique_tokens: 0,
            missed_n: r.missed_n,
            clean_missed: r.clean_missed,
            gold_missed: r.gold_missed,
            silver_missed: r.silver_missed,
            max_missed_pct: r.max_missed_pct,
          });
        }
      }
      merged.sort((a, b) => (b.gold_missed + b.silver_missed) - (a.gold_missed + a.silver_missed) || b.total_rejects - a.total_rejects);

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        window_hours: hoursBack,
        since_ts: sinceTs,
        gates: merged,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/external-alpha-health') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const stateCount = tableNames.has('external_alpha_state')
        ? paperDb.prepare("SELECT COUNT(*) AS n FROM external_alpha_state").get().n
        : null;
      const snapshotCount = tableNames.has('external_alpha_snapshots')
        ? paperDb.prepare("SELECT COUNT(*) AS n FROM external_alpha_snapshots").get().n
        : null;
      const health = tableNames.has('external_alpha_health')
        ? paperDb.prepare("SELECT * FROM external_alpha_health ORDER BY updated_at DESC").all()
        : [];
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        state_count: stateCount,
        snapshot_count: snapshotCount,
        health,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/source-resonance') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const limit = boundedIntParam(url, 'limit', 50, 1, 200);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const health = tableNames.has('source_resonance_health')
        ? paperDb.prepare("SELECT * FROM source_resonance_health ORDER BY updated_at DESC").all()
        : [];
      const cohortSummary = tableNames.has('source_resonance_candidates')
        ? paperDb.prepare(`
            SELECT
              cohort,
              COUNT(*) AS n,
              COUNT(DISTINCT token_ca) AS unique_tokens,
              SUM(CASE WHEN gmgn_pre_seen = 1 THEN 1 ELSE 0 END) AS gmgn_pre_seen_n,
              SUM(CASE WHEN quote_clean_seen = 1 THEN 1 ELSE 0 END) AS quote_clean_n,
              AVG(resonance_score) AS avg_resonance_score,
              MAX(updated_at) AS latest_updated_at
            FROM source_resonance_candidates
            GROUP BY cohort
            ORDER BY n DESC
          `).all()
        : [];
      const recent = tableNames.has('source_resonance_candidates')
        ? paperDb.prepare(`
            SELECT
              token_ca, symbol, signal_ts, telegram_signal_id, signal_type,
              gmgn_pre_seen, gmgn_lead_time_sec, gmgn_momentum_rounds,
              gmgn_momentum_confirmed, quote_clean_seen, two_quote_clean_snapshots,
              source_count, resonance_level, resonance_score, cohort, updated_at
            FROM source_resonance_candidates
            ORDER BY updated_at DESC, signal_ts DESC
            LIMIT @limit
          `).all({ limit })
        : [];
      const latencySummary = tableNames.has('latency_audit_events')
        ? paperDb.prepare(`
            SELECT
              stage,
              COUNT(*) AS n,
              AVG(lag_from_source_ms) AS avg_lag_from_source_ms,
              MAX(lag_from_source_ms) AS max_lag_from_source_ms,
              AVG(lag_from_receive_ms) AS avg_lag_from_receive_ms
            FROM latency_audit_events
            GROUP BY stage
            ORDER BY stage
          `).all()
        : [];
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: tableNames.has('source_resonance_candidates'),
        health,
        cohort_summary: cohortSummary,
        latency_summary: latencySummary,
        recent,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/route-health') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    const requestedHours = boundedIntParam(url, 'hours', 8, 1, 24);
    const limit = boundedIntParam(url, 'limit', 120, 1, 300);
    const liveSnapshot = readLivePaperReview(requestedHours);
    if (liveSnapshot && !liveSnapshot.error && liveSnapshot.route_health) {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(routeHealthFromLiveSnapshot(liveSnapshot, {
        dbPath: paperDbPath,
        requestedHours,
        limit,
      }), null, 2));
      return;
    }
    res.writeHead(202, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      available: false,
      materialized: true,
      reason: liveSnapshot?.error || 'materialized_route_health_not_ready',
      path: livePaperReviewPath(requestedHours),
    }, null, 2));
    return;
  } else if (url.pathname === '/api/paper/storage-health') {
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(buildStorageHealthSnapshot({
      includeDisk: ['1', 'true', 'yes'].includes(String(url.searchParams.get('disk') || '').toLowerCase()),
      includeFileStats: ['1', 'true', 'yes'].includes(String(url.searchParams.get('files') || '').toLowerCase()),
      includePreflightTail: ['1', 'true', 'yes'].includes(String(url.searchParams.get('tail') || '').toLowerCase()),
    }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/incident-artifacts') {
    if (!checkAuth(req, url, res)) return;
    const snapshot = buildIncidentArtifactSnapshot({
      scope: url.searchParams.get('scope') || 'all',
      maxFiles: boundedIntParam(url, 'limit', 80, 1, 500),
      maxDepth: boundedIntParam(url, 'depth', 5, 0, 8),
      includePreviews: ['1', 'true', 'yes'].includes(String(url.searchParams.get('preview') || '').toLowerCase()),
      previewBytes: boundedIntParam(url, 'preview_bytes', 2000, 1, 8000),
    });
    res.writeHead(snapshot.error_code ? 400 : 200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(snapshot, null, 2));
    return;
  } else if (url.pathname === '/api/paper/incident-artifact/download') {
    if (!checkAuth(req, url, res)) return;
    const resolved = resolveIncidentArtifactPath(url.searchParams.get('scope'), url.searchParams.get('path'));
    if (!resolved.ok) {
      res.writeHead(resolved.statusCode || 400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(resolved, null, 2));
      return;
    }
    let stats;
    try {
      stats = fs.statSync(resolved.path);
    } catch (error) {
      res.writeHead(error?.code === 'ENOENT' ? 404 : 500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        error: error?.code === 'ENOENT' ? 'Incident artifact not found' : error.message,
        error_code: error?.code === 'ENOENT' ? 'incident_artifact_not_found' : 'incident_artifact_stat_failed',
      }, null, 2));
      return;
    }
    if (!stats.isFile()) {
      res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        error: 'Incident artifact path is not a file',
        error_code: 'incident_artifact_not_file',
        scope: resolved.scope,
        relative_path: resolved.relative_path,
      }, null, 2));
      return;
    }
    res.writeHead(200, {
      'Content-Type': incidentArtifactContentType(resolved.path),
      'Content-Disposition': `attachment; filename="${basename(resolved.path).replace(/"/g, '')}"`,
      'Content-Length': stats.size,
      'Cache-Control': 'no-store',
    });
    fs.createReadStream(resolved.path).pipe(res);
    return;
  } else if (url.pathname === '/api/paper/v27-read-model-health') {
    if (!checkAuth(req, url, res)) return;
    const health = readV27DenominatorReadModelHealth();
    const strict = ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase());
    const status = health.available
      ? strict && !health.dashboard_safe
        ? 503
        : 200
      : 202;
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(health, null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-read-model-refresh') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_read_model_refresh',
      payload: {
        include_records: ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_records') || '').toLowerCase()),
        strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      },
    })) return;
    const refresh = triggerV27ReadModelRefresh({
      includeRecords: ['1', 'true', 'yes'].includes(String(url.searchParams.get('include_records') || '').toLowerCase()),
      strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
    });
    res.writeHead(refresh.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_read_model_refresh.v1', refresh, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-recovery-control-mirror') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_recovery_control_mirror',
      payload: {
        environment_id: url.searchParams.get('environment_id') || null,
        recovery_version: url.searchParams.get('recovery_version') || null,
      },
    })) return;
    const refresh = triggerV27RecoveryControlMirror({
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
      environmentId: url.searchParams.get('environment_id') || undefined,
      recoveryVersion: url.searchParams.get('recovery_version') || undefined,
    });
    res.writeHead(refresh.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_recovery_control_mirror.v1', refresh, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-raw-provider-evidence-mirror') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_raw_provider_evidence_mirror',
      payload: {
        dry_run: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
        strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
        trusted_only: url.searchParams.get('trusted_only') || null,
      },
    })) return;
    const mirror = triggerV27RawProviderEvidenceMirror({
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
      limit: url.searchParams.has('limit') ? boundedIntParam(url, 'limit', 500, 1, 5000) : undefined,
      sinceId: url.searchParams.has('since_id') ? boundedIntParam(url, 'since_id', 1, 1, 1000000000) : undefined,
      untilId: url.searchParams.has('until_id') ? boundedIntParam(url, 'until_id', 1, 1, 1000000000) : undefined,
      cursorOverlapIds: url.searchParams.has('cursor_overlap_ids') ? boundedIntParam(url, 'cursor_overlap_ids', 100, 0, 1000000) : undefined,
      dryRun: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
      strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      trustedOnly: url.searchParams.has('trusted_only')
        ? ['1', 'true', 'yes'].includes(String(url.searchParams.get('trusted_only') || '').toLowerCase())
        : true,
      evidenceVersion: url.searchParams.get('evidence_version') || undefined,
      defaultProvider: url.searchParams.get('default_provider') || undefined,
      defaultEndpoint: url.searchParams.get('default_endpoint') || undefined,
    });
    res.writeHead(mirror.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_raw_provider_evidence_mirror.v1', mirror, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-raw-provider-probe-evidence') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_raw_provider_probe_evidence',
      payload: {
        run_id: url.searchParams.get('run_id') || null,
        dry_run: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
        strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      },
    })) return;
    const record = triggerV27RawProviderProbeEvidence({
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
      runId: url.searchParams.get('run_id') || undefined,
      endpointBase: url.searchParams.get('endpoint_base') || undefined,
      inputMint: url.searchParams.get('input_mint') || undefined,
      outputMint: url.searchParams.get('output_mint') || undefined,
      outputSymbol: url.searchParams.get('output_symbol') || undefined,
      amountRaw: url.searchParams.get('amount_raw') || undefined,
      slippageBps: url.searchParams.has('slippage_bps') ? boundedIntParam(url, 'slippage_bps', 0, 0, 10000) : undefined,
      timeoutSec: url.searchParams.has('probe_timeout_sec') ? boundedIntParam(url, 'probe_timeout_sec', 10, 1, 60) : undefined,
      dryRun: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
      strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      evidenceVersion: url.searchParams.get('evidence_version') || undefined,
      provider: url.searchParams.get('provider') || undefined,
      endpoint: url.searchParams.get('endpoint') || undefined,
    });
    res.writeHead(record.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_raw_provider_probe_evidence.v1', record, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-randomness-control-mirror') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    const statusesRaw = url.searchParams.getAll('status')
      .flatMap((value) => String(value).split(','))
      .map((value) => value.trim())
      .filter(Boolean);
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_randomness_control_mirror',
      payload: {
        statuses: statusesRaw,
        dry_run: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
        strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      },
    })) return;
    const mirror = triggerV27RandomnessControlMirror({
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
      limit: url.searchParams.has('limit') ? boundedIntParam(url, 'limit', 500, 1, 5000) : undefined,
      sinceCreatedAt: url.searchParams.get('since_created_at') || undefined,
      untilCreatedAt: url.searchParams.get('until_created_at') || undefined,
      statuses: statusesRaw,
      dryRun: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
      strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      auditVersion: url.searchParams.get('audit_version') || undefined,
      defaultRandomizationUnit: url.searchParams.get('default_randomization_unit') || undefined,
      environmentId: url.searchParams.get('environment_id') || undefined,
    });
    res.writeHead(mirror.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_randomness_control_mirror.v1', mirror, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-normal-tiny-ops-evidence') {
    if (!requirePost(req, res)) return;
    if (!checkAuth(req, url, res)) return;
    if (!requireDashboardAuditEvent(req, res, url, {
      required_role: 'dashboard_operator',
      token_scope: 'v27:evidence_mutation',
      danger_level: 'operator_mutation',
      action: 'v27_normal_tiny_ops_evidence',
      payload: {
        run_id: url.searchParams.get('run_id') || null,
        dry_run: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
        strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
        worker_roles: url.searchParams.getAll('worker_role'),
      },
    })) return;
    const record = triggerV27NormalTinyOpsEvidence({
      timeoutMs: boundedIntParam(url, 'timeout_ms', 600000, 30000, 1800000),
      runId: url.searchParams.get('run_id') || undefined,
      scratchDir: url.searchParams.get('scratch_dir') || undefined,
      dryRun: ['1', 'true', 'yes'].includes(String(url.searchParams.get('dry_run') || '').toLowerCase()),
      strict: ['1', 'true', 'yes'].includes(String(url.searchParams.get('strict') || '').toLowerCase()),
      workerRoles: url.searchParams.getAll('worker_role').flatMap((value) => String(value).split(',')).map((value) => value.trim()).filter(Boolean),
    });
    res.writeHead(record.accepted ? 202 : 409, apiJsonHeaders());
    res.end(JSON.stringify(buildV27ManualEvidenceApiResponse('v2.7.0.manual_normal_tiny_ops_evidence.v1', record, { endpoint: url.pathname }), null, 2));
    return;
  } else if (url.pathname === '/api/goal/rolling-24h') {
    if (!checkAuth(req, url, res)) return;
    const requestedHours = boundedIntParam(url, 'hours', 24, 1, 24);
    const materializedHours = nearestLivePaperReviewHours(requestedHours);
    const liveSnapshot = readLivePaperReview(materializedHours);
    const paperDbHealth = readPaperDbRuntimeHealth();
    const rawDiscovery = readRawDogDiscoveryApiSnapshot({
      hours: requestedHours,
      coverageTargetPct: boundedIntParam(url, 'raw_coverage_target_pct', 80, 0, 100),
      limit: boundedIntParam(url, 'raw_limit', 50, 1, 200),
    });
    const status = buildRolling24hGoalStatusFromLiveSnapshot(liveSnapshot, {
      requestedHours,
      materializedHours,
      paperDbHealth,
      maxSnapshotAgeMinutes: boundedIntParam(url, 'max_snapshot_age_minutes', 30, 1, 1440),
      targetRealizedWinRate: Number(url.searchParams.get('target_win_rate') || 0.60),
      targetGoldSilverCaptureRate: Number(url.searchParams.get('target_capture') || 0.60),
      targetStrategyBucketRoi: Number(url.searchParams.get('target_roi') || 2.0),
      maxSingleTradeLossPct: Number(url.searchParams.get('max_single_loss_pct') || 20),
      minClosedTrades: Number(url.searchParams.get('min_closed_trades') || 20),
      minGoldSilverCandidates: Number(url.searchParams.get('min_gold_silver_candidates') || 5),
      dogPeakRatio: Number(url.searchParams.get('dog_peak') || 0.50),
      winPeakRatio: Number(url.searchParams.get('win_peak') || 0.30),
    });
    status.raw_discovery = rawDiscovery;
    status.metrics = {
      ...status.metrics,
      raw_discovery_denominator_status: rawDiscovery.summary?.denominator_status || null,
      raw_kline_coverage_pct: rawDiscovery.summary?.raw_kline_coverage_pct ?? null,
      raw_sustained_gold_silver_unique: rawDiscovery.summary?.raw_sustained_gold_silver_unique ?? null,
      raw_dog_entered_rate: rawDiscovery.summary?.raw_dog_entered_rate ?? null,
      raw_dog_realized_rate: rawDiscovery.summary?.raw_dog_realized_rate ?? null,
      raw_dog_no_decision_record: rawDiscovery.summary?.decision_funnel?.no_decision_record ?? null,
      raw_dog_has_decision_record: rawDiscovery.summary?.decision_funnel?.has_decision_record ?? null,
      raw_dog_quote_clean: rawDiscovery.summary?.decision_funnel?.quote_clean ?? null,
    };
    res.writeHead(status.available && !status.shadow_pending ? 200 : 202, apiJsonHeaders());
    res.end(JSON.stringify(status, null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-kpi-proof-status') {
    const proofStatus = buildV27KpiProofStatus({
      requestedHours: boundedIntParam(url, 'hours', 24, 1, 72),
      maxSnapshotAgeMinutes: boundedIntParam(url, 'max_snapshot_age_minutes', 30, 1, 1440),
      targetCatchRate: Number(url.searchParams.get('target_capture') || 0.60),
      targetWinRate: Number(url.searchParams.get('target_win_rate') || 0.55),
      targetRoi: Number(url.searchParams.get('target_roi') || 2.0),
      dogPeakRatio: Number(url.searchParams.get('dog_peak') || 0.50),
      winPeakRatio: Number(url.searchParams.get('win_peak') || 0.30),
    });
    res.writeHead(200, apiJsonHeaders());
    res.end(JSON.stringify(proofStatus, null, 2));
    return;
  } else if (url.pathname === '/api/paper/v27-mode-readiness') {
    if (!checkAuth(req, url, res)) return;
    const readiness = readV27ModeReadiness();
    res.writeHead(readiness.available ? 200 : 202, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(readiness, null, 2));
    return;
  } else if (url.pathname === '/api/paper/dog-catch-goal') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    const live = ['1', 'true', 'yes'].includes(String(url.searchParams.get('live') || '').toLowerCase());
    let paperDb;
    let releasePaperReport;
    try {
      const requestedHours = boundedIntParam(url, 'hours', 24, 1, 72);
      const sinceTs = Math.floor(Date.now() / 1000) - requestedHours * 3600;
      if (!live) {
        const liveSnapshot = readLivePaperReview(Math.min(requestedHours, 24));
        const options = {
          targetCatchRate: Number(url.searchParams.get('target_capture') || 0.60),
          targetWinRate: Number(url.searchParams.get('target_win_rate') || 0.55),
          targetRoi: Number(url.searchParams.get('target_roi') || 2.0),
          dogPeakRatio: Number(url.searchParams.get('dog_peak') || 0.50),
          winPeakRatio: Number(url.searchParams.get('win_peak') || 0.30),
        };
        const materializedProgress = liveSnapshot && !liveSnapshot.error
          ? dogCatchGoalFromLiveSnapshot(liveSnapshot, { dbPath: paperDbPath, requestedHours, options })
          : null;
        res.writeHead(liveSnapshot && !liveSnapshot.error ? 200 : 202, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          generated_at: new Date().toISOString(),
          db_path: paperDbPath,
          window_hours: requestedHours,
          materialized: true,
          live_query: false,
          materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
          materialized_generated_at: liveSnapshot?.generated_at || null,
          available: Boolean(liveSnapshot && !liveSnapshot.error),
          reason: liveSnapshot?.error || (liveSnapshot ? null : 'materialized_snapshot_not_ready'),
          ...(materializedProgress || {}),
          note: 'Pass live=1 for an on-demand DB calculation; default stays materialized so dashboard health cannot be blocked by SQLite.',
        }, null, 2));
        return;
      }
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 2,
        maxHours: 2,
        defaultLimit: 1000,
        maxLimit: 1000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));
      const progress = buildDogCatchGoalProgress(paperDb, tableNames, liveGuard.since_ts, {
        targetCatchRate: Number(url.searchParams.get('target_capture') || 0.60),
        targetWinRate: Number(url.searchParams.get('target_win_rate') || 0.55),
        targetRoi: Number(url.searchParams.get('target_roi') || 2.0),
        dogPeakRatio: Number(url.searchParams.get('dog_peak') || 0.50),
        winPeakRatio: Number(url.searchParams.get('win_peak') || 0.30),
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        window_hours: liveGuard.window_hours,
        requested_window_hours: requestedHours,
        materialized: false,
        live_query: true,
        ...progress,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/paper/fast-lane') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    let releasePaperReport;
    try {
      const liveGuard = livePaperQueryGuard(url, url.pathname, {
        defaultHours: 2,
        maxHours: 2,
        defaultLimit: 1000,
        maxLimit: 1000,
      });
      if (!liveGuard.allowed) {
        rejectLivePaperQuery(res, liveGuard);
        return;
      }
      releasePaperReport = beginLivePaperReport(res, url.pathname);
      if (!releasePaperReport) return;
      const hours = liveGuard.window_hours;
      const sinceTs = liveGuard.since_ts;
      const requestedHours = Number.parseInt(url.searchParams.get('hours') || String(hours), 10) || hours;
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(
        paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name)
      );
      const queueColumns = tableNames.has('paper_fast_entry_queue')
        ? getTableColumns(paperDb, 'paper_fast_entry_queue')
        : new Set();
      const tradeColumns = tableNames.has('paper_trades')
        ? getTableColumns(paperDb, 'paper_trades')
        : new Set();
      const trustedFastPeakExpr = trustedTradePeakSqlExpr(tradeColumns);
      const tradeTsCol = ['entry_ts', 'signal_ts', 'exit_ts'].find((name) => tradeColumns.has(name));
      const tradeFilterTsExpr = tradeTsCol || '0';
      const tradeSessionTsExpr = tradeTsCol || '0';
      const tradeBranchExpr = tradeColumns.has('entry_branch') ? "COALESCE(entry_branch, 'unknown')" : "'unknown'";
      const tradeModeExpr = tradeColumns.has('entry_mode') ? "COALESCE(entry_mode, 'unknown')" : "'unknown'";
      const queueUpdatedExpr = queueColumns.has('updated_at') ? 'updated_at' : 'created_at';
      const queueBranchExpr = queueColumns.has('entry_branch') ? 'entry_branch' : (queueColumns.has('source_type') ? 'source_type' : "'unknown'");
      const firstErrorExpr = queueColumns.has('first_error')
        ? (queueColumns.has('last_error') ? "COALESCE(first_error, last_error, 'none')" : "COALESCE(first_error, 'none')")
        : (queueColumns.has('last_error') ? "COALESCE(last_error, 'none')" : "'none'");
      const marketSessionExpr = queueColumns.has('market_session') ? "COALESCE(market_session, 'unknown')" : "'unknown'";
      const optionalQueueSelect = (name, fallback = 'NULL') => queueColumns.has(name) ? name : `${fallback} AS ${name}`;
      const queueStatus = tableNames.has('paper_fast_entry_queue')
        ? paperDb.prepare(`
            SELECT status, COUNT(*) AS n, MAX(${queueUpdatedExpr}) AS latest_updated_at
            FROM paper_fast_entry_queue
            WHERE created_at >= @sinceTs OR ${queueUpdatedExpr} >= @sinceTs
            GROUP BY status
            ORDER BY n DESC
          `).all({ sinceTs })
        : [];
      const branchSummary = tableNames.has('paper_fast_entry_queue')
        ? paperDb.prepare(`
            SELECT COALESCE(${queueBranchExpr}, 'unknown') AS entry_branch, status, COUNT(*) AS n
            FROM paper_fast_entry_queue
            WHERE created_at >= @sinceTs OR ${queueUpdatedExpr} >= @sinceTs
            GROUP BY COALESCE(${queueBranchExpr}, 'unknown'), status
            ORDER BY n DESC
            LIMIT 50
          `).all({ sinceTs })
        : [];
      const reasonSummary = tableNames.has('paper_fast_entry_queue')
        ? paperDb.prepare(`
            SELECT status, ${firstErrorExpr} AS reason, COUNT(*) AS n
            FROM paper_fast_entry_queue
            WHERE created_at >= @sinceTs OR ${queueUpdatedExpr} >= @sinceTs
            GROUP BY status, ${firstErrorExpr}
            ORDER BY n DESC
            LIMIT 50
          `).all({ sinceTs })
        : [];
      const sessionSummary = tableNames.has('paper_fast_entry_queue')
        ? paperDb.prepare(`
            SELECT ${marketSessionExpr} AS market_session, status, COUNT(*) AS n
            FROM paper_fast_entry_queue
            WHERE created_at >= @sinceTs OR ${queueUpdatedExpr} >= @sinceTs
            GROUP BY ${marketSessionExpr}, status
            ORDER BY n DESC
            LIMIT 50
          `).all({ sinceTs })
        : [];
      const recentQueue = tableNames.has('paper_fast_entry_queue')
        ? paperDb.prepare(`
            SELECT id, created_at, ${optionalQueueSelect('updated_at', 'created_at')}, status,
                   ${optionalQueueSelect('last_error')},
                   ${optionalQueueSelect('first_error')},
                   ${optionalQueueSelect('first_error_at')},
                   ${optionalQueueSelect('market_session', "'unknown'")},
                   ${optionalQueueSelect('status_history_json')},
                   token_ca, symbol, source_type, entry_mode_hint, entry_branch,
                   source_signal_ts, signal_receive_ts, signal_recorded_ts,
                   priority, claimed_by, claimed_at
            FROM paper_fast_entry_queue
            WHERE created_at >= @sinceTs OR ${queueUpdatedExpr} >= @sinceTs
            ORDER BY ${queueUpdatedExpr} DESC, id DESC
            LIMIT 40
          `).all({ sinceTs })
        : [];
      const recentFastTrades = tableNames.has('paper_trades')
        ? paperDb.prepare(`
            SELECT id, symbol, token_ca, entry_ts, exit_ts, exit_reason,
                   entry_mode, entry_branch, pnl_pct * 100.0 AS pnl_pct,
                   ${trustedFastPeakExpr} * 100.0 AS peak_pct,
                   position_size_sol,
                   signal_to_quote_latency_ms,
                   signal_to_quote_drift_pct,
                   quote_spread_pct
            FROM paper_trades
            WHERE replay_source = 'paper_fast_lane'
              AND entry_ts >= @sinceTs
            ORDER BY entry_ts DESC, id DESC
            LIMIT 30
          `).all({ sinceTs })
        : [];
      const fastTrades = tableNames.has('paper_trades')
        ? paperDb.prepare(`
            SELECT
              entry_branch,
              entry_mode,
              COUNT(*) AS fills,
              SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed,
              AVG(pnl_pct) * 100.0 AS avg_pnl_pct,
              AVG(${trustedFastPeakExpr}) * 100.0 AS avg_peak_pct,
              AVG(COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms)) AS avg_fast_lane_sla_ms,
              MAX(COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms)) AS max_fast_lane_sla_ms,
              AVG(COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms)) AS avg_signal_to_quote_ms,
              MAX(COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms)) AS max_signal_to_quote_ms,
              AVG(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.original_signal_to_quote_latency_ms')) AS avg_original_signal_to_quote_ms,
              MAX(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.original_signal_to_quote_latency_ms')) AS max_original_signal_to_quote_ms,
              AVG(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_queue_to_quote_latency_ms')) AS avg_queue_to_quote_ms,
              MAX(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_queue_to_quote_latency_ms')) AS max_queue_to_quote_ms
            FROM paper_trades
            WHERE replay_source = 'paper_fast_lane'
              AND entry_ts >= @sinceTs
            GROUP BY entry_branch, entry_mode
            ORDER BY fills DESC
          `).all({ sinceTs })
        : [];
      const branchEvRows = tableNames.has('paper_trades') && tradeColumns.has('entry_branch') && tradeColumns.has('pnl_pct')
        ? paperDb.prepare(`
            SELECT
              ${tradeBranchExpr} AS entry_branch,
              ${tradeModeExpr} AS entry_mode,
              ${tradeSessionTsExpr} AS session_ts,
              COALESCE(pnl_pct, 0) AS pnl_pct,
              ${trustedFastPeakExpr} AS trusted_peak_pnl
            FROM paper_trades
            WHERE pnl_pct IS NOT NULL
              AND ${tradeFilterTsExpr} >= @sinceTs
          `).all({ sinceTs })
        : [];
      const branchEvMap = new Map();
      for (const row of branchEvRows) {
        const session = marketSessionForTs(row.session_ts);
        const key = `${row.entry_branch || 'unknown'}\u0000${row.entry_mode || 'unknown'}\u0000${session}`;
        if (!branchEvMap.has(key)) {
          branchEvMap.set(key, {
            entry_branch: row.entry_branch || 'unknown',
            entry_mode: row.entry_mode || 'unknown',
            market_session: session,
            pnls: [],
            wins: 0,
            trusted_dog_capture_n: 0,
          });
        }
        const group = branchEvMap.get(key);
        const pnl = Number(row.pnl_pct || 0);
        group.pnls.push(pnl);
        if (pnl > 0) group.wins += 1;
        if (Number(row.trusted_peak_pnl || 0) >= 0.25) group.trusted_dog_capture_n += 1;
      }
      const branchEvSummary = Array.from(branchEvMap.values()).map((group) => {
        const closedN = group.pnls.length;
        const avg = closedN ? group.pnls.reduce((sum, value) => sum + value, 0) / closedN : 0;
        return {
          entry_branch: group.entry_branch,
          entry_mode: group.entry_mode,
          market_session: group.market_session,
          closed_n: closedN,
          wins: group.wins,
          win_rate_pct: closedN ? roundNumber((group.wins / closedN) * 100, 2) : null,
          avg_pnl_pct: roundNumber(avg * 100, 2),
          p10_pnl_pct: closedN ? roundNumber(percentileLinear(group.pnls, 0.10) * 100, 2) : null,
          p90_pnl_pct: closedN ? roundNumber(percentileLinear(group.pnls, 0.90) * 100, 2) : null,
          max_loss_pct: closedN ? roundNumber(Math.min(...group.pnls) * 100, 2) : null,
          trusted_dog_capture_n: group.trusted_dog_capture_n,
          auto_action: closedN >= 20 && avg < -0.03 ? 'downgrade_to_watch_only' : 'allow_or_observe',
        };
      }).sort((a, b) => (
        Number(b.closed_n || 0) - Number(a.closed_n || 0)
        || Math.abs(Number(b.avg_pnl_pct || 0)) - Math.abs(Number(a.avg_pnl_pct || 0))
      )).slice(0, 80);
      const latencyRows = tableNames.has('paper_trades')
        ? paperDb.prepare(`
            SELECT COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms) AS ms
            FROM paper_trades
            WHERE replay_source = 'paper_fast_lane'
              AND entry_ts >= @sinceTs
              AND COALESCE(json_extract(entry_execution_audit_json, '$.entryLatencyAudit.fast_lane_sla_latency_ms'), signal_to_quote_latency_ms) IS NOT NULL
            ORDER BY ms ASC
          `).all({ sinceTs }).map((row) => Number(row.ms)).filter((ms) => Number.isFinite(ms))
        : [];
      const originalLatencyRows = tableNames.has('paper_trades')
        ? paperDb.prepare(`
            SELECT json_extract(entry_execution_audit_json, '$.entryLatencyAudit.original_signal_to_quote_latency_ms') AS ms
            FROM paper_trades
            WHERE replay_source = 'paper_fast_lane'
              AND entry_ts >= @sinceTs
              AND json_extract(entry_execution_audit_json, '$.entryLatencyAudit.original_signal_to_quote_latency_ms') IS NOT NULL
            ORDER BY ms ASC
          `).all({ sinceTs }).map((row) => Number(row.ms)).filter((ms) => Number.isFinite(ms))
        : [];
      const percentile = (values, p) => {
        if (!values.length) return null;
        const idx = Math.min(values.length - 1, Math.max(0, Math.ceil((p / 100) * values.length) - 1));
        return values[idx];
      };
      const latencySummary = {
        n: latencyRows.length,
        p50_fast_lane_sla_ms: percentile(latencyRows, 50),
        p90_fast_lane_sla_ms: percentile(latencyRows, 90),
        p99_fast_lane_sla_ms: percentile(latencyRows, 99),
        original_signal_n: originalLatencyRows.length,
        p50_original_signal_to_quote_ms: percentile(originalLatencyRows, 50),
        p90_original_signal_to_quote_ms: percentile(originalLatencyRows, 90),
        p99_original_signal_to_quote_ms: percentile(originalLatencyRows, 99),
        note: 'fast_lane_sla_ms is receive/rescue-created to quote; original_signal_to_quote_ms is signal aging and is not the worker SLA.',
      };
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: tableNames.has('paper_fast_entry_queue'),
        window_hours: hours,
        requested_window_hours: requestedHours,
        queue_status: queueStatus,
        branch_summary: branchSummary,
        reason_summary: reasonSummary,
        session_summary: sessionSummary,
        branch_ev_summary: branchEvSummary,
        fast_trades: fastTrades,
        recent_queue: recentQueue,
        recent_fast_trades: recentFastTrades,
        latency_summary: latencySummary,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (releasePaperReport) releasePaperReport(); } catch {}
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/a-class/status') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    const requestedHours = boundedIntParam(url, 'hours', 6, 1, 168);
    const forceLive = ['1', 'true', 'yes'].includes(String(url.searchParams.get('live') || '').toLowerCase())
      || ['0', 'false', 'no'].includes(String(url.searchParams.get('materialized') || '').toLowerCase());
    const limit = boundedIntParam(url, 'limit', 30, 1, 200);
    const paperDbHealth = readPaperDbRuntimeHealth({ paperDbPath });
    if (!paperDbHealthIsUsable(paperDbHealth)) {
      res.writeHead(202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: false,
        materialized: !forceLive,
        live_query: forceLive,
        requested_window_hours: requestedHours,
        reason: 'live_paper_db_unavailable',
        paper_db_health: paperDbHealth,
        note: 'Materialized A_CLASS evidence is suppressed while the live paper DB is missing, empty, malformed, or integrity-marked.',
      }, null, 2));
      return;
    }
    if (!forceLive) {
      const materializedHours = nearestLivePaperReviewHours(Math.min(requestedHours, 24));
      const liveSnapshot = readLivePaperReview(materializedHours);
      if (liveSnapshot && !liveSnapshot.error && liveSnapshot.a_class?.available) {
        const status = aClassStatusFromLiveSnapshot(liveSnapshot, {
          dbPath: paperDbPath,
          requestedHours,
          materializedHours,
          limit,
        });
        res.writeHead(status.available && !status.shadow_pending ? 200 : 202, apiJsonHeaders());
        res.end(JSON.stringify(status, null, 2));
        return;
      }
      res.writeHead(202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: false,
        materialized: true,
        live_query: false,
        requested_window_hours: requestedHours,
        materialized_window_hours: materializedHours,
        materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
        materialized_generated_at: liveSnapshot?.generated_at || null,
        reason: liveSnapshot?.error || (liveSnapshot ? 'a_class_materialized_section_not_ready' : 'materialized_snapshot_not_ready'),
        path: livePaperReviewPath(materializedHours),
        note: 'Pass live=1 to force a DB scan; default stays materialized so dashboard health cannot be blocked by SQLite.',
      }, null, 2));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 6, 168, { allowAll: true });
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('a_class_decision_events')) {
        res.writeHead(200, apiJsonHeaders());
        res.end(JSON.stringify({
          generated_at: new Date().toISOString(),
          db_path: paperDbPath,
          available: false,
          reason: 'a_class_decision_events table not found',
        }, null, 2));
        return;
      }
      const aceColumns = getTableColumns(paperDb, 'a_class_decision_events');
      const p0ColumnsReady = [
        'would_action',
        'expected_rr',
        'expected_rr_detail_json',
        'denominator_key',
        'discovery_exit_json',
      ].every((name) => aceColumns.has(name));
      const optionalAceSelect = (name, fallback = 'NULL') => aceColumns.has(name) ? name : `${fallback} AS ${name}`;
      const where = sinceTs == null ? '' : 'WHERE event_ts >= @sinceTs';
      const params = sinceTs == null ? {} : { sinceTs };
      const actionSummary = paperDb.prepare(`
        SELECT action, COUNT(*) AS n, MAX(event_ts) AS latest_event_ts,
               AVG(score) AS avg_score,
               SUM(CASE WHEN action = 'WOULD_ENTER' THEN size_sol ELSE 0 END) AS would_enter_size_sol
        FROM a_class_decision_events
        ${where}
        GROUP BY action
        ORDER BY n DESC
      `).all(params);
      const gradeSummary = paperDb.prepare(`
        SELECT COALESCE(grade, 'UNKNOWN') AS grade, action, COUNT(*) AS n, AVG(score) AS avg_score
        FROM a_class_decision_events
        ${where}
        GROUP BY COALESCE(grade, 'UNKNOWN'), action
        ORDER BY n DESC
      `).all(params);
      const sourceSummary = paperDb.prepare(`
        SELECT COALESCE(source_table, 'unknown') AS source_table,
               COALESCE(source_component, 'unknown') AS source_component,
               action,
               COUNT(*) AS n,
               AVG(score) AS avg_score,
               SUM(CASE WHEN action = 'WOULD_ENTER' THEN size_sol ELSE 0 END) AS would_enter_size_sol,
               MAX(event_ts) AS latest_event_ts
        FROM a_class_decision_events
        ${where}
        GROUP BY COALESCE(source_table, 'unknown'), COALESCE(source_component, 'unknown'), action
        ORDER BY n DESC
        LIMIT 80
      `).all(params);
      const reasonSummary = paperDb.prepare(`
        SELECT COALESCE(source_table, 'unknown') AS source_table,
               COALESCE(source_component, 'unknown') AS source_component,
               COALESCE(source_reason, 'unknown') AS source_reason,
               action,
               COUNT(*) AS n,
               MAX(score) AS max_score,
               MAX(event_ts) AS latest_event_ts
        FROM a_class_decision_events
        ${where}
        GROUP BY COALESCE(source_table, 'unknown'), COALESCE(source_component, 'unknown'), COALESCE(source_reason, 'unknown'), action
        ORDER BY n DESC
        LIMIT 80
      `).all(params);
      const blockerRows = paperDb.prepare(`
        SELECT hard_blockers_json
        FROM a_class_decision_events
        ${where}
      `).all(params);
      const blockerCounts = new Map();
      for (const row of blockerRows) {
        const blockers = parseJsonValue(row.hard_blockers_json, []);
        if (!Array.isArray(blockers)) continue;
        for (const blocker of blockers) {
          blockerCounts.set(blocker, (blockerCounts.get(blocker) || 0) + 1);
        }
      }
      const recentEvents = paperDb.prepare(`
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, route_bucket,
               normalized_mode, source_table, source_id, source_component,
               source_reason, action, grade, size_sol, score, reason,
               hard_blockers_json, soft_notes_json,
               freshness_json, budget_json, risk_json,
               ${optionalAceSelect('would_action')},
               ${optionalAceSelect('expected_rr')},
               ${optionalAceSelect('expected_upside_pct')},
               ${optionalAceSelect('defined_risk_pct')},
               ${optionalAceSelect('bottom_ticket_size_sol')},
               ${optionalAceSelect('expected_rr_detail_json')},
               ${optionalAceSelect('matrix_json')},
               ${optionalAceSelect('ai_review_json')},
               ${optionalAceSelect('controller_action_json')},
               ${optionalAceSelect('denominator_key')},
               ${optionalAceSelect('discovery_exit_json')}
        FROM a_class_decision_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all({ ...params, limit }).map(aClassEventRow);
      const runtimeSafety = aClassRuntimeSafetyFromDb(paperDb, tableNames, sinceTs);
      res.writeHead(p0ColumnsReady ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: p0ColumnsReady,
        status: p0ColumnsReady ? 'shadow_ready' : 'shadow_pending',
        shadow_pending: !p0ColumnsReady,
        materialized: false,
        live_query: true,
        p0_schema_columns_ready: p0ColumnsReady,
        enabled_env: String(process.env.A_CLASS_ENABLED || 'false').toLowerCase() === 'true',
        shadow_eval_enabled_env: String(process.env.A_CLASS_SHADOW_EVAL_ENABLED || 'true').toLowerCase() !== 'false',
        since_ts: sinceTs,
        action_summary: actionSummary.map((row) => ({
          ...row,
          avg_score: roundNullableNumber(row.avg_score, 2),
          would_enter_size_sol: roundNullableNumber(row.would_enter_size_sol, 6),
        })),
        grade_summary: gradeSummary.map((row) => ({
          ...row,
          avg_score: roundNullableNumber(row.avg_score, 2),
        })),
        source_summary: sourceSummary.map((row) => ({
          ...row,
          avg_score: roundNullableNumber(row.avg_score, 2),
          would_enter_size_sol: roundNullableNumber(row.would_enter_size_sol, 6),
        })),
        reason_summary: reasonSummary.map((row) => ({
          ...row,
          max_score: roundNullableNumber(row.max_score, 2),
        })),
        hard_blockers: Array.from(blockerCounts.entries())
          .map(([blocker, n]) => ({ blocker, n }))
          .sort((a, b) => b.n - a.n),
        recent_events: recentEvents,
        runtime_safety: runtimeSafety,
        loss_cap_breach_n: runtimeSafety.loss_cap_breach_n,
        mode_circuit_broken: runtimeSafety.mode_circuit_broken,
        downgraded_modes: runtimeSafety.downgraded_modes,
        next_safe_action: runtimeSafety.next_safe_action,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/a-class/block-causes') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 6, 168, { allowAll: true });
      const recentLimit = boundedIntParam(url, 'limit', 50, 0, 500);
      const sourceFilter = String(url.searchParams.get('source') || 'all').trim().toLowerCase();
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      const sourceIssues = [];
      const rows = [];
      const params = sinceTs == null ? {} : { sinceTs };
      const where = sinceTs == null ? '' : 'WHERE event_ts >= @sinceTs';

      if ((sourceFilter === 'all' || sourceFilter === 'a_class' || sourceFilter === 'a_class_decision_events') && tableNames.has('a_class_decision_events')) {
        const cols = getTableColumns(paperDb, 'a_class_decision_events');
        const optional = (name, fallback = 'NULL') => cols.has(name) ? name : `${fallback} AS ${name}`;
        const expr = (name, fallback = 'NULL') => cols.has(name) ? name : fallback;
        rows.push(...paperDb.prepare(`
          SELECT
            id,
            'a_class_decision_events' AS source_kind,
            event_ts,
            token_ca,
            symbol,
            lifecycle_id,
            route_bucket,
            source_table,
            source_component,
            source_reason,
            action,
            ${optional('would_action')},
            reason,
            hard_blockers_json,
            ${optional('risk_json')},
            ${optional('candidate_json')},
            ${optional('denominator_key')},
            ${optional('expected_rr')},
            ${optional('score')},
            ${optional('grade')},
            ${optional('size_sol')},
            ${optional('block_cause')},
            ${optional('recoverability')},
            ${optional('classification_reason')},
            ${optional('blocker_classifications_json')},
            ${optional('quote_available')},
            ${optional('quote_executable')},
            ${optional('quote_clean')},
            ${optional('route_available')},
            ${optional('quote_source')},
            ${optional('quote_age_sec')},
            ${optional('data_confidence')},
            NULL AS provider_data_state,
            ${optional('provider_reason')},
            ${optional('evidence_status')},
            ${optional('quote_failure_reason')},
            ${optional('route_failure_reason')},
            ${optional('liquidity_usd')},
            ${optional('spread_pct')},
            NULL AS would_enter_a_class,
            NULL AS did_enter,
            ${expr('provider_hydrate_outcome', 'NULL')} AS hydrate_outcome,
            CASE WHEN ${expr('provider_hydrate_outcome', 'NULL')} IN ('success', 'cache_hit_success') THEN 1 ELSE 0 END AS hydrate_success
          FROM a_class_decision_events
          ${where}
          ORDER BY event_ts DESC, id DESC
        `).all(params));
      } else if (sourceFilter === 'all' || sourceFilter === 'a_class' || sourceFilter === 'a_class_decision_events') {
        sourceIssues.push('a_class_decision_events_missing');
      }

      if ((sourceFilter === 'all' || sourceFilter === 'opportunity' || sourceFilter === 'opportunity_events') && tableNames.has('opportunity_events')) {
        const cols = getTableColumns(paperDb, 'opportunity_events');
        const optional = (name, fallback = 'NULL') => cols.has(name) ? name : `${fallback} AS ${name}`;
        const expr = (name, fallback = 'NULL') => cols.has(name) ? name : fallback;
        const opportunityRows = paperDb.prepare(`
          SELECT
            id,
            'opportunity_events' AS source_kind,
            event_ts,
            token_ca,
            symbol,
            lifecycle_id,
            route_bucket,
            source_type AS source_table,
            source_component,
            source_reason,
            CASE
              WHEN COALESCE(${expr('did_enter', '0')}, 0) = 1 THEN 'ENTER'
              WHEN COALESCE(${expr('would_enter_a_class', '0')}, 0) = 1 THEN 'WOULD_ENTER'
              ELSE 'BLOCK'
            END AS action,
            NULL AS would_action,
            ${expr('quote_failure_reason', 'NULL')} AS reason,
            hard_blockers_json,
            NULL AS risk_json,
            raw_payload_json AS candidate_json,
            NULL AS denominator_key,
            expected_rr,
            matrix_score AS score,
            NULL AS grade,
            NULL AS size_sol,
            ${optional('quote_available', 'NULL')},
            ${optional('quote_executable', 'NULL')},
            ${optional('quote_clean', 'NULL')},
            ${optional('route_available', 'NULL')},
            ${optional('quote_source', 'NULL')},
            ${optional('quote_age_sec', 'NULL')},
            ${optional('data_confidence', 'NULL')},
            ${optional('provider_data_state', 'NULL')},
            ${optional('provider_reason', 'NULL')},
            ${optional('evidence_status', 'NULL')},
            ${optional('quote_failure_reason', 'NULL')},
            ${optional('liquidity_usd', 'NULL')},
            ${optional('spread_pct', 'NULL')},
            ${optional('would_enter_a_class', '0')},
            ${optional('did_enter', '0')},
            ${optional('block_cause', 'NULL')},
            ${optional('recoverability', 'NULL')},
            ${optional('classification_reason', 'NULL')},
            ${optional('blocker_classifications_json', 'NULL')},
            ${optional('hydrate_outcome', 'NULL')},
            ${optional('hydrate_success', '0')}
          FROM opportunity_events
          ${where}
          ORDER BY event_ts DESC, id DESC
        `).all(params);
        rows.push(...opportunityRows);
      } else if (sourceFilter === 'all' || sourceFilter === 'opportunity' || sourceFilter === 'opportunity_events') {
        sourceIssues.push('opportunity_events_missing');
      }

      rows.sort((a, b) => Number(b.event_ts || 0) - Number(a.event_ts || 0) || Number(b.id || 0) - Number(a.id || 0));
      const breakdown = buildAClassBlockCauseBreakdown(rows, { limit: recentLimit });
      res.writeHead(rows.length || sourceIssues.length === 0 ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        ...breakdown,
        db_path: paperDbPath,
        available: rows.length > 0,
        source_issues: sourceIssues,
        source_filter: sourceFilter,
        since_ts: sinceTs,
        since_iso: sinceTs == null ? null : new Date(sinceTs * 1000).toISOString(),
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/opportunity/evidence') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 24, 24 * 120, { allowAll: true });
      const limit = boundedIntParam(url, 'limit', 100, 1, 500);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('opportunity_events')) {
        res.writeHead(202, apiJsonHeaders());
        res.end(JSON.stringify({
          generated_at: new Date().toISOString(),
          db_path: paperDbPath,
          available: false,
          reason: 'opportunity_events table not found',
          summary: {},
          events: [],
        }, null, 2));
        return;
      }
      const cols = getTableColumns(paperDb, 'opportunity_events');
      const optional = (name, fallback = 'NULL') => cols.has(name) ? name : `${fallback} AS ${name}`;
      const expr = (name, fallback = 'NULL') => cols.has(name) ? name : fallback;
      const filters = [];
      const params = { limit };
      if (sinceTs != null) {
        filters.push('event_ts >= @sinceTs');
        params.sinceTs = sinceTs;
      }
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
      const summary = paperDb.prepare(`
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT token_ca) AS unique_tokens,
               SUM(CASE WHEN COALESCE(${expr('quote_available', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS quote_available_n,
               SUM(CASE WHEN COALESCE(${expr('quote_executable', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS quote_executable_n,
               SUM(CASE WHEN COALESCE(${expr('route_available', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS route_available_n,
               SUM(CASE WHEN COALESCE(${expr('quote_clean', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS quote_clean_n,
               SUM(CASE WHEN COALESCE(${expr('path_sample_count', '0')}, 0) > 0 THEN 1 ELSE 0 END) AS path_sampled_n,
               SUM(CASE WHEN COALESCE(${expr('would_enter_a_class', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS would_enter_a_class_n,
               SUM(CASE WHEN COALESCE(${expr('did_enter', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS did_enter_n
        FROM opportunity_events
        ${where}
      `).get(params);
      const evidenceStatus = cols.has('evidence_status')
        ? paperDb.prepare(`
            SELECT COALESCE(evidence_status, 'unknown') AS evidence_status, COUNT(*) AS n
            FROM opportunity_events
            ${where}
            GROUP BY COALESCE(evidence_status, 'unknown')
            ORDER BY n DESC
            LIMIT 30
          `).all(params)
        : [];
      const sourceSummary = paperDb.prepare(`
        SELECT COALESCE(source_type, 'unknown') AS source_type,
               COALESCE(source_component, 'unknown') AS source_component,
               COUNT(*) AS n,
               SUM(CASE WHEN COALESCE(${expr('quote_clean', '0')}, 0) = 1 THEN 1 ELSE 0 END) AS quote_clean_n,
               SUM(CASE WHEN COALESCE(${expr('path_sample_count', '0')}, 0) > 0 THEN 1 ELSE 0 END) AS path_sampled_n,
               MAX(event_ts) AS latest_event_ts
        FROM opportunity_events
        ${where}
        GROUP BY COALESCE(source_type, 'unknown'), COALESCE(source_component, 'unknown')
        ORDER BY n DESC
        LIMIT 50
      `).all(params);
      const pathSummary = tableNames.has('opportunity_event_path_samples')
        ? paperDb.prepare(`
            SELECT COUNT(*) AS samples,
                   SUM(CASE WHEN COALESCE(quote_clean, 0) = 1 THEN 1 ELSE 0 END) AS quote_clean_samples,
                   SUM(CASE WHEN COALESCE(no_route_flag, 0) = 1 THEN 1 ELSE 0 END) AS no_route_samples,
                   SUM(CASE WHEN COALESCE(trapped_flag, 0) = 1 THEN 1 ELSE 0 END) AS trapped_samples,
                   MAX(sample_ts) AS latest_sample_ts
            FROM opportunity_event_path_samples
            ${sinceTs == null ? '' : 'WHERE sample_ts >= @sinceTs'}
          `).get(sinceTs == null ? {} : { sinceTs })
        : { samples: 0, quote_clean_samples: 0, no_route_samples: 0, trapped_samples: 0, latest_sample_ts: null };
      const events = paperDb.prepare(`
        SELECT id, opportunity_key, event_ts, token_ca, symbol, lifecycle_id,
               source_type, source_component, source_reason, route_bucket,
               quote_available, quote_executable, quote_clean, route_available,
               liquidity_usd, spread_pct, market_cap,
               ${optional('quote_source')},
               ${optional('quote_age_sec')},
               ${optional('data_confidence')},
               ${optional('provider_data_state')},
               ${optional('provider_reason')},
               ${optional('evidence_status')},
               ${optional('path_sample_count', '0')},
               expected_rr, defined_risk_pct, would_enter_a_class, did_enter,
               linked_trade_id
        FROM opportunity_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all(params);
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: true,
        since_ts: sinceTs,
        summary,
        evidence_status: evidenceStatus,
        source_summary: sourceSummary,
        path_summary: pathSummary,
        events,
        note: 'Opportunity evidence is the executable denominator bridge for A_CLASS and triple-barrier counterfactual replay.',
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/a-class/events') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 6, 168, { allowAll: true });
      const limit = boundedIntParam(url, 'limit', 100, 1, 500);
      // Read-only keyset pagination cursor for bounded evidence pulls.
      const beforeTs = parseUnixishTime(url.searchParams.get('before_ts'));
      const beforeId = boundedIntParam(url, 'before_id', 0, 0, Number.MAX_SAFE_INTEGER);
      const action = String(url.searchParams.get('action') || '').trim().toUpperCase();
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('a_class_decision_events')) {
        res.writeHead(200, apiJsonHeaders());
        res.end(JSON.stringify({ generated_at: new Date().toISOString(), db_path: paperDbPath, available: false, events: [] }, null, 2));
        return;
      }
      const aceColumns = getTableColumns(paperDb, 'a_class_decision_events');
      const p0ColumnsReady = [
        'would_action',
        'expected_rr',
        'expected_rr_detail_json',
        'denominator_key',
        'discovery_exit_json',
      ].every((name) => aceColumns.has(name));
      const optionalAceSelect = (name, fallback = 'NULL') => aceColumns.has(name) ? name : `${fallback} AS ${name}`;
      const filters = [];
      const params = { limit };
      if (sinceTs != null) {
        filters.push('event_ts >= @sinceTs');
        params.sinceTs = sinceTs;
      }
      if (action) {
        filters.push('UPPER(action) = @action');
        params.action = action;
      }
      if (beforeTs != null && beforeId > 0) {
        filters.push('(event_ts < @beforeTs OR (event_ts = @beforeTs AND id < @beforeId))');
        params.beforeTs = beforeTs;
        params.beforeId = beforeId;
      } else if (beforeId > 0) {
        filters.push('id < @beforeId');
        params.beforeId = beforeId;
      }
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
      const events = paperDb.prepare(`
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, route_bucket,
               normalized_mode, source_table, source_id, source_component,
               source_reason, action, grade, size_sol, score, reason,
               hard_blockers_json, soft_notes_json,
               freshness_json, budget_json, risk_json,
               ${optionalAceSelect('would_action')},
               ${optionalAceSelect('expected_rr')},
               ${optionalAceSelect('expected_upside_pct')},
               ${optionalAceSelect('defined_risk_pct')},
               ${optionalAceSelect('bottom_ticket_size_sol')},
               ${optionalAceSelect('expected_rr_detail_json')},
               ${optionalAceSelect('matrix_json')},
               ${optionalAceSelect('ai_review_json')},
               ${optionalAceSelect('controller_action_json')},
               ${optionalAceSelect('denominator_key')},
               ${optionalAceSelect('discovery_exit_json')}
        FROM a_class_decision_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all(params).map(aClassEventRow);
      res.writeHead(p0ColumnsReady ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: p0ColumnsReady,
        status: p0ColumnsReady ? 'shadow_ready' : 'shadow_pending',
        shadow_pending: !p0ColumnsReady,
        since_ts: sinceTs,
        action: action || null,
        before_ts: beforeTs,
        before_id: beforeId || null,
        events,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/a-class/trades' || url.pathname === '/api/ledger/trades') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 24, 24 * 120, { allowAll: true });
      const limit = boundedIntParam(url, 'limit', 100, 1, 1000);
      const mode = String(url.searchParams.get('mode') || '').trim();
      const aClassOnly = url.pathname === '/api/a-class/trades' || String(url.searchParams.get('a_class') || '').toLowerCase() === 'true';
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('canonical_trade_ledger')) {
        res.writeHead(200, apiJsonHeaders());
        res.end(JSON.stringify({ generated_at: new Date().toISOString(), db_path: paperDbPath, available: false, trades: [] }, null, 2));
        return;
      }
      const filters = [];
      const params = { limit };
      if (sinceTs != null) {
        filters.push('COALESCE(entry_ts, exit_ts, created_at, 0) >= @sinceTs');
        params.sinceTs = sinceTs;
      }
      if (aClassOnly) {
        filters.push('is_a_class_fastlane = 1');
      }
      if (mode) {
        filters.push('normalized_mode = @mode');
        params.mode = mode;
      }
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
      const trades = paperDb.prepare(`
        SELECT id, trade_id, token_ca, symbol, lifecycle_id, route_bucket,
               entry_mode, normalized_mode, strategy_family, entry_ts, exit_ts,
               entry_size_sol, realized_exit_sol, realized_pnl_sol, realized_pnl_pct,
               peak_quote_pnl_pct, max_drawdown_pct, exit_reason, accounting_source,
               trapped_flag, no_route_flag, stale_flag, outlier_flag, outlier_reason,
               is_a_class_fastlane, a_class_grade, a_class_score, a_class_size_rule
        FROM canonical_trade_ledger
        ${where}
        ORDER BY COALESCE(entry_ts, exit_ts, created_at, 0) DESC, id DESC
        LIMIT @limit
      `).all(params).map(canonicalLedgerTradeRow);
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: true,
        since_ts: sinceTs,
        mode: mode || null,
        a_class_only: aClassOnly,
        trades,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/scorecard/a-class' || url.pathname === '/api/scorecard/entry-modes') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const aClassScorecard = url.pathname === '/api/scorecard/a-class';
      const requestedHours = boundedIntParam(url, 'hours', 24, 1, 24 * 120);
      const forceLive = ['1', 'true', 'yes'].includes(String(url.searchParams.get('live') || '').toLowerCase())
        || ['0', 'false', 'no'].includes(String(url.searchParams.get('materialized') || '').toLowerCase());
      if (aClassScorecard && !forceLive) {
        const materializedHours = nearestLivePaperReviewHours(Math.min(requestedHours, 24));
        const liveSnapshot = readLivePaperReview(materializedHours);
        const p0Discovery = aClassP0DiscoveryFromSnapshot(liveSnapshot);
        const response = {
          generated_at: new Date().toISOString(),
          db_path: paperDbPath,
          available: p0Discovery.available,
          status: p0Discovery.available ? 'shadow_ready' : 'shadow_pending',
          shadow_pending: !p0Discovery.available,
          materialized: true,
          live_query: false,
          requested_window_hours: requestedHours,
          materialized_window_hours: materializedHours,
          materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
          materialized_generated_at: liveSnapshot?.generated_at || null,
          scorecard: 'a_class',
          rows: p0Discovery.available ? [{
            bucket: 'A_CLASS_P0_SHADOW_DISCOVERY',
            quote_clean_gold_silver_seen_count: p0Discovery.quote_clean_gold_silver_seen_count,
            quote_clean_gold_silver_would_enter_count: p0Discovery.quote_clean_gold_silver_would_enter_count,
            would_enter_no_route_rate: p0Discovery.would_enter_no_route_rate,
            would_enter_trapped_rate: p0Discovery.would_enter_trapped_rate,
            unknown_data_rate: p0Discovery.unknown_data_rate,
            outlier_trimmed_would_rr: p0Discovery.outlier_trimmed_would_rr,
            advisory: p0Discovery.discovery_exit?.advisory || p0Discovery.discovery_exit?.advisory_action || null,
            advisory_only: true,
            requires_human_approval: p0Discovery.discovery_exit?.requires_human_approval ?? true,
          }] : [],
          p0_discovery: p0Discovery,
          note: 'Default A_CLASS scorecard reads the Python materialized P0 discovery evidence; pass live=1 for the legacy canonical ledger scorecard.',
        };
        res.writeHead(response.available ? 200 : 202, apiJsonHeaders());
        res.end(JSON.stringify(response, null, 2));
        return;
      }
      const sinceTs = boundedWindowedSinceTs(url, 24 * 7, 24 * 120, { allowAll: true });
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('canonical_trade_ledger')) {
        res.writeHead(200, apiJsonHeaders());
        res.end(JSON.stringify({ generated_at: new Date().toISOString(), db_path: paperDbPath, available: false, rows: [] }, null, 2));
        return;
      }
      const filters = [];
      const params = {};
      if (sinceTs != null) {
        filters.push('COALESCE(entry_ts, exit_ts, created_at, 0) >= @sinceTs');
        params.sinceTs = sinceTs;
      }
      if (aClassScorecard) {
        filters.push('is_a_class_fastlane = 1');
      }
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
      const groupExpr = aClassScorecard
        ? "COALESCE(a_class_grade, 'UNKNOWN')"
        : "COALESCE(normalized_mode, 'UNKNOWN')";
      const rows = paperDb.prepare(`
        SELECT ${groupExpr} AS bucket,
               COUNT(*) AS trades,
               SUM(CASE WHEN exit_ts IS NOT NULL THEN 1 ELSE 0 END) AS closed_trades,
               SUM(CASE WHEN realized_pnl_sol > 0 THEN 1 ELSE 0 END) AS wins,
               SUM(COALESCE(realized_pnl_sol, 0)) AS total_pnl_sol,
               AVG(realized_pnl_sol) AS avg_pnl_sol,
               AVG(realized_pnl_pct) AS avg_pnl_pct,
               SUM(CASE WHEN COALESCE(peak_quote_pnl_pct, 0) <= 0 THEN 1 ELSE 0 END) AS doa_n,
               SUM(CASE WHEN COALESCE(peak_quote_pnl_pct, 0) >= 0.20 THEN 1 ELSE 0 END) AS peak20_n,
               SUM(CASE WHEN COALESCE(peak_quote_pnl_pct, 0) >= 0.50 THEN 1 ELSE 0 END) AS peak50_n,
               SUM(CASE WHEN COALESCE(peak_quote_pnl_pct, 0) >= 1.00 THEN 1 ELSE 0 END) AS peak100_n,
               SUM(CASE WHEN no_route_flag = 1 THEN 1 ELSE 0 END) AS no_route_n,
               SUM(CASE WHEN trapped_flag = 1 THEN 1 ELSE 0 END) AS trapped_n,
               SUM(CASE WHEN outlier_flag = 1 THEN 1 ELSE 0 END) AS outlier_n
        FROM canonical_trade_ledger
        ${where}
        GROUP BY ${groupExpr}
        ORDER BY trades DESC
      `).all(params).map((row) => ({
        bucket: row.bucket,
        trades: row.trades,
        closed_trades: row.closed_trades,
        win_rate_pct: row.closed_trades ? roundNumber((Number(row.wins || 0) / Number(row.closed_trades)) * 100, 2) : null,
        total_pnl_sol: roundNullableNumber(row.total_pnl_sol, 6),
        avg_pnl_sol: roundNullableNumber(row.avg_pnl_sol, 6),
        avg_pnl_pct: row.avg_pnl_pct == null ? null : roundNumber(Number(row.avg_pnl_pct) * 100, 3),
        doa_rate_pct: row.trades ? roundNumber((Number(row.doa_n || 0) / Number(row.trades)) * 100, 2) : null,
        peak20_rate_pct: row.trades ? roundNumber((Number(row.peak20_n || 0) / Number(row.trades)) * 100, 2) : null,
        peak50_rate_pct: row.trades ? roundNumber((Number(row.peak50_n || 0) / Number(row.trades)) * 100, 2) : null,
        peak100_rate_pct: row.trades ? roundNumber((Number(row.peak100_n || 0) / Number(row.trades)) * 100, 2) : null,
        no_route_rate_pct: row.trades ? roundNumber((Number(row.no_route_n || 0) / Number(row.trades)) * 100, 2) : null,
        trapped_rate_pct: row.trades ? roundNumber((Number(row.trapped_n || 0) / Number(row.trades)) * 100, 2) : null,
        outlier_n: row.outlier_n,
      }));
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: true,
        since_ts: sinceTs,
        scorecard: aClassScorecard ? 'a_class' : 'entry_modes',
        rows,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/a-class/matrix' || url.pathname === '/api/a-class/ai-reviews') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    if (!fs.existsSync(paperDbPath)) {
      res.writeHead(404, apiJsonHeaders());
      res.end(JSON.stringify({ error: 'Paper trades database not found' }));
      return;
    }
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 24, 168, { allowAll: true });
      const limit = boundedIntParam(url, 'limit', 100, 1, 500);
      paperDb = openDashboardSqlite(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('a_class_decision_events')) {
        res.writeHead(202, apiJsonHeaders());
        res.end(JSON.stringify({
          generated_at: new Date().toISOString(),
          available: false,
          status: 'shadow_pending',
          reason: 'a_class_decision_events table not found',
          events: [],
        }, null, 2));
        return;
      }
      const aceColumns = getTableColumns(paperDb, 'a_class_decision_events');
      const optionalAceSelect = (name, fallback = 'NULL') => aceColumns.has(name) ? name : `${fallback} AS ${name}`;
      const where = sinceTs == null ? '' : 'WHERE event_ts >= @sinceTs';
      const params = sinceTs == null ? { limit } : { sinceTs, limit };
      const events = paperDb.prepare(`
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, route_bucket,
               normalized_mode, source_table, source_id, source_component,
               source_reason, action, grade, size_sol, score, reason,
               hard_blockers_json, soft_notes_json,
               freshness_json, budget_json, risk_json,
               ${optionalAceSelect('would_action')},
               ${optionalAceSelect('expected_rr')},
               ${optionalAceSelect('expected_upside_pct')},
               ${optionalAceSelect('defined_risk_pct')},
               ${optionalAceSelect('bottom_ticket_size_sol')},
               ${optionalAceSelect('expected_rr_detail_json')},
               ${optionalAceSelect('matrix_json')},
               ${optionalAceSelect('ai_review_json')},
               ${optionalAceSelect('controller_action_json')},
               ${optionalAceSelect('denominator_key')},
               ${optionalAceSelect('discovery_exit_json')}
        FROM a_class_decision_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all(params).map(aClassEventRow);
      if (url.pathname === '/api/a-class/matrix') {
        const summary = summarizeAClassMatrixEvents(events);
        res.writeHead(summary.available ? 200 : 202, apiJsonHeaders());
        res.end(JSON.stringify({
          generated_at: new Date().toISOString(),
          db_path: paperDbPath,
          available: summary.available,
          status: summary.available ? 'shadow_ready' : 'shadow_pending',
          since_ts: sinceTs,
          summary,
          events: events.filter((event) => event.matrix && event.matrix.matrix_version),
        }, null, 2));
        return;
      }
      const reviews = events
        .filter((event) => event.ai_review && event.ai_review.schema_version)
        .map((event) => ({
          id: event.id,
          event_ts: event.event_ts,
          event_iso: event.event_iso,
          symbol: event.symbol,
          token_ca: event.token_ca,
          action: event.action,
          grade: event.grade,
          expected_rr: event.expected_rr,
          matrix_grade: event.matrix?.matrix_grade || null,
          ai_review: event.ai_review,
        }));
      res.writeHead(reviews.length ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: reviews.length > 0,
        status: reviews.length > 0 ? 'shadow_ready' : 'shadow_pending',
        since_ts: sinceTs,
        reviews,
      }, null, 2));
    } catch (e) {
      res.writeHead(500, apiJsonHeaders());
      res.end(JSON.stringify({ error: e.message }));
    } finally {
      try { if (paperDb) paperDb.close(); } catch {}
    }
    return;
  } else if (url.pathname === '/api/missed-dog/ai-review' || url.pathname === '/api/counterfactual/ai-audit' || url.pathname === '/api/goal/controller-actions') {
    if (!checkAuth(req, url, res)) return;
    const paperDbPath = getPaperDbPath();
    const requestedHours = boundedIntParam(url, 'hours', 24, 1, 24);
    const materializedHours = nearestLivePaperReviewHours(Math.min(requestedHours, 24));
    const liveSnapshot = readLivePaperReview(materializedHours);
    const p0Discovery = aClassP0DiscoveryFromSnapshot(liveSnapshot);
    const missedDogReview = liveSnapshot?.ai_strategy_review?.missed_dog_review
      || liveSnapshot?.a_class?.ai_strategy_review?.missed_dog_review
      || buildMissedDogAiReviewFromP0(p0Discovery);
    const counterfactualAudit = liveSnapshot?.ai_strategy_review?.counterfactual_audit
      || liveSnapshot?.a_class?.ai_strategy_review?.counterfactual_audit
      || buildCounterfactualAiAuditFromP0(p0Discovery);
    if (url.pathname === '/api/missed-dog/ai-review') {
      res.writeHead(p0Discovery.available ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        materialized: true,
        live_query: false,
        available: p0Discovery.available,
        materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
        materialized_generated_at: liveSnapshot?.generated_at || null,
        review: missedDogReview,
      }, null, 2));
      return;
    }
    if (url.pathname === '/api/counterfactual/ai-audit') {
      res.writeHead(p0Discovery.available ? 200 : 202, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        materialized: true,
        live_query: false,
        available: p0Discovery.available,
        materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
        materialized_generated_at: liveSnapshot?.generated_at || null,
        audit: counterfactualAudit,
      }, null, 2));
      return;
    }
    const rollingGoalStatus = buildRolling24hGoalStatusFromLiveSnapshot(liveSnapshot, {
      dbPath: paperDbPath,
      requestedHours,
      materializedHours,
    });
    const controller = liveSnapshot?.strategy_goal_controller
      || liveSnapshot?.ai_strategy_review?.controller_actions
      || buildGoalControllerActions({
        rollingGoalStatus,
        p0Discovery,
        counterfactualAudit,
        missedDogReview,
      });
    res.writeHead(p0Discovery.available ? 200 : 202, apiJsonHeaders());
    res.end(JSON.stringify({
      generated_at: new Date().toISOString(),
      db_path: paperDbPath,
      materialized: true,
      live_query: false,
      available: p0Discovery.available,
      materialized_snapshot_id: liveSnapshot?.snapshot_id || null,
      materialized_generated_at: liveSnapshot?.generated_at || null,
      controller,
      actions: controller.actions || [],
      next_safe_action: controller.next_safe_action || 'keep_a_class_shadow',
    }, null, 2));
    return;
  } else if (url.pathname === '/api/download/paper_trades') {
    // Paper trades数据库下载 — 默认走 SQLite backup，确保 WAL 内 attribution/ledger 一起下载。
    const paperDbPath = getPaperDbPath();
    await downloadSqliteDatabase(req, res, url, paperDbPath, 'paper_trades.db', 'Paper trades database', 'paper_trades_download');
    return;
  } else if (url.pathname === '/api/export') {
    // v10: 导出所有DB数据为JSON（用于回测分析） — 需要 token 认证
    if (!checkAuth(req, url, res)) return;
    try {
      const database = getDb();
      if (!database) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Database not available' }));
        return;
      }
      const tables = ['premium_signals', 'tokens', 'trades', 'live_positions', 'rejected_signals', 'passed_signals', 'hunter_signals', 'signal_source_performance', 'autonomy_runs', 'strategy_experiments', 'paper_trade_records'];
      const exportData = { exported_at: new Date().toISOString(), tables: {} };
      // 支持分页: ?before_id=X 拉取 id < X 的历史数据
      const beforeId = url.searchParams.get('before_id');
      const exportLimit = parseInt(url.searchParams.get('limit') || '1000');
      for (const table of tables) {
        try {
          let rows;
          if (beforeId && table === 'premium_signals') {
            rows = database.prepare(`SELECT * FROM ${table} WHERE id < ? ORDER BY id DESC LIMIT ?`).all(parseInt(beforeId), exportLimit);
          } else {
            rows = database.prepare(`SELECT * FROM ${table} ORDER BY rowid DESC LIMIT ?`).all(exportLimit);
          }
          exportData.tables[table] = { count: rows.length, rows };
        } catch (e) {
          exportData.tables[table] = { count: 0, rows: [], error: e.message };
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(exportData, null, 2));
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/signals/stream') {
    // SSE (Server-Sent Events) endpoint for real-time signal streaming
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write('data: {"event":"connected","timestamp":"' + new Date().toISOString() + '"}\n\n');

    // Register this SSE client
    if (!global.__sseClients) global.__sseClients = new Set();
    global.__sseClients.add(res);
    console.log(`📡 SSE client connected (total: ${global.__sseClients.size})`);

    // Keep-alive ping every 30s
    const keepAlive = setInterval(() => {
      try { res.write(':ping\n\n'); } catch (e) { /* client disconnected */ }
    }, 30000);

    req.on('close', () => {
      clearInterval(keepAlive);
      if (global.__sseClients) global.__sseClients.delete(res);
      console.log(`📡 SSE client disconnected (total: ${global.__sseClients?.size || 0})`);
    });
    return;
  } else if (url.pathname === '/api/wallet-balance') {
    // 钱包 SOL 余额查询
    if (!checkAuth(req, url, res)) return;
    try {
      const executor = global.__executor;
      if (executor && executor.walletAddress) {
        const balance = await executor.getSolBalance();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ balance: +balance.toFixed(4), wallet: executor.walletAddress.substring(0, 8) + '...' + executor.walletAddress.slice(-4) }));
        return;
      }
      // fallback: 用环境变量
      const walletAddr = process.env.TRADE_WALLET_ADDRESS || process.env.WALLET_ADDRESS || '';
      const rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
      if (!walletAddr) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ balance: null, error: 'TRADE_WALLET_ADDRESS not set' }));
        return;
      }
      const rpcBody = JSON.stringify({
        jsonrpc: '2.0', id: 1, method: 'getBalance',
        params: [walletAddr, { commitment: 'confirmed' }]
      });
      const rpcRes = await new Promise((resolve, reject) => {
        const isHttps = rpcUrl.startsWith('https');
        const urlObj = new URL(rpcUrl);
        const mod = isHttps ? https : http;
        const req = mod.request({
          hostname: urlObj.hostname, port: urlObj.port || (isHttps ? 443 : 80),
          path: urlObj.pathname, method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(rpcBody) }
        }, r => {
          let data = '';
          r.on('data', c => data += c);
          r.on('end', () => resolve(JSON.parse(data)));
        });
        req.on('error', reject);
        req.setTimeout(5000, () => { req.destroy(); reject(new Error('RPC timeout')); });
        req.write(rpcBody);
        req.end();
      });
      const lamports = rpcRes?.result?.value ?? null;
      const balance = lamports !== null ? +(lamports / 1e9).toFixed(4) : null;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ balance, wallet: walletAddr.substring(0, 8) + '...' + walletAddr.slice(-4) }));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ balance: null, error: e.message }));
    }
    return;
  } else if (url.pathname === '/api/logs') {
    // 最近日志 API（JSON格式）
    if (!checkAuth(req, url, res)) return;
    const limit = parseInt(url.searchParams?.get('limit') || '100');
    const level = url.searchParams?.get('level'); // 可选过滤: INFO, ERROR, WARN
    let logs = logBuffer.slice(-limit);
    if (level) {
      logs = logs.filter(l => l.level === level.toUpperCase());
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ count: logs.length, logs }, null, 2));
    return;
  } else if (url.pathname === '/api/logs/download') {
    // 日志下载端点（完整日志文件）
    if (!checkAuth(req, url, res)) return;
    if (fs.existsSync(runtimeLogPath)) {
      const stats = fs.statSync(runtimeLogPath);
      res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': `attachment; filename="runtime-logs-${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.txt"`,
        'Content-Length': stats.size
      });
      const fileStream = fs.createReadStream(runtimeLogPath);
      fileStream.pipe(res);
    } else {
      // fallback 到内存缓冲
      const content = logBuffer.map(l => `[${l.timestamp}] [${l.level}] ${l.message}`).join('\n');
      res.writeHead(200, {
        'Content-Type': 'text/plain; charset=utf-8',
        'Content-Disposition': `attachment; filename="runtime-logs-${new Date().toISOString().slice(0,19).replace(/:/g,'-')}.txt"`
      });
      res.end(content);
    }
    return;
  } else if (url.pathname === '/api/logs/paper-trader') {
    // Paper trader Python 进程日志
    if (!checkAuth(req, url, res)) return;
    const paperTraderLogPath = process.env.PAPER_TRADER_LOG || '/app/data/paper-trader.log';
    const tailLines = boundedIntParam(url, 'lines', 500, 1, 5000);
    if (fs.existsSync(paperTraderLogPath)) {
      try {
        execFile('tail', ['-n', String(tailLines), paperTraderLogPath], { maxBuffer: 1024 * 1024 * 50 }, (error, stdout, stderr) => {
          if (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: error.message }));
            return;
          }
          res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
          res.end(stdout);
        });
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `paper-trader.log not found at ${paperTraderLogPath}` }));
    }
    return;
  } else if (
    resolveDashboardLogPath(url.pathname)
  ) {
    if (!checkAuth(req, url, res)) return;
    const logPath = resolveDashboardLogPath(url.pathname);
    const tailLines = boundedIntParam(url, 'lines', 500, 1, 5000);
    if (fs.existsSync(logPath)) {
      try {
        execFile('tail', ['-n', String(tailLines), logPath], { maxBuffer: 1024 * 1024 * 20 }, (error, stdout, stderr) => {
          if (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: error.message }));
            return;
          }
          res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
          res.end(stdout);
        });
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: `log not found at ${logPath}` }));
    }
    return;
  } else if (url.pathname === '/logs') {
    // 日志查看页面（HTML）
    if (!checkAuth(req, url, res)) return;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`<!DOCTYPE html>
<html><head><title>Runtime Logs</title>
<style>
body{font-family:monospace;background:#1a1a2e;color:#e4e4e4;padding:20px;margin:0}
h1{color:#00d9ff}pre{background:#111;padding:15px;border-radius:8px;overflow-x:auto;max-height:80vh;overflow-y:auto}
.INFO{color:#00ff88}.ERROR{color:#ff4444}.WARN{color:#ffda44}
.controls{margin-bottom:15px}
.controls button,.controls select{padding:8px 16px;margin-right:10px;border-radius:4px;border:none;cursor:pointer}
.controls button{background:#00d9ff;color:#1a1a2e;font-weight:bold}
</style></head><body>
<h1>📋 Runtime Logs</h1>
<div class="controls">
  <button onclick="refresh()">🔄 刷新</button>
  <button onclick="download()">📥 下载</button>
  <select id="level" onchange="refresh()">
    <option value="">全部级别</option>
    <option value="INFO">INFO</option>
    <option value="WARN">WARN</option>
    <option value="ERROR">ERROR</option>
  </select>
  <select id="limit" onchange="refresh()">
    <option value="100">最近100条</option>
    <option value="500">最近500条</option>
    <option value="1000">全部(1000)</option>
  </select>
</div>
<pre id="logs">加载中...</pre>
<script>
const _token=new URLSearchParams(window.location.search).get('token')||'';
const _a=_token?'&token='+encodeURIComponent(_token):'';
async function refresh(){
  const level=document.getElementById('level').value;
  const limit=document.getElementById('limit').value;
  const res=await fetch('/api/logs?limit='+limit+(level?'&level='+level:'')+_a);
  const data=await res.json();
  document.getElementById('logs').innerHTML=data.logs.map(l=>
    '<span class="'+l.level+'">['+l.timestamp.slice(11,19)+'] ['+l.level+'] '+l.message.replace(/</g,'&lt;').replace(/>/g,'&gt;')+'</span>'
  ).join('\\n');
}
function download(){window.location='/api/logs/download';}
refresh();setInterval(refresh,10000);
</script></body></html>`);
    return;
  } else if (url.pathname === '/health') {
    // 健康检查 + 数据库状态
    try {
      const d = getDb();
      let dbStatus = { connected: false };

      if (d) {
        const shadowCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl').get();
        const latestShadow = d.prepare('SELECT MAX(entry_time) as latest FROM shadow_pnl').get();
        const tradesCount = d.prepare('SELECT COUNT(*) as cnt FROM trades').get();
        const closedCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl WHERE closed=1').get();
        const openCount = d.prepare('SELECT COUNT(*) as cnt FROM shadow_pnl WHERE closed=0').get();

        const latestTime = latestShadow?.latest ? new Date(latestShadow.latest).toISOString() : null;

        dbStatus = {
          connected: true,
          shadow_pnl: {
            total: shadowCount?.cnt || 0,
            closed: closedCount?.cnt || 0,
            open: openCount?.cnt || 0,
            latest_entry: latestTime
          },
          trades: tradesCount?.cnt || 0
        };
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        db: dbStatus,
        uptime_seconds: Math.floor(process.uptime())
      }, null, 2));
    } catch (e) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        timestamp: new Date().toISOString(),
        db: { connected: false, error: e.message },
        uptime_seconds: Math.floor(process.uptime())
      }, null, 2));
    }
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

/**
 * Premium Channel Dashboard 页面
 */
function renderPremiumDashboard() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Premium Channel Dashboard</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:linear-gradient(135deg,#1a1a2e,#16213e);color:#e4e4e4;min-height:100vh;padding:20px}
    .container{max-width:1400px;margin:0 auto}
    h1{text-align:center;margin-bottom:20px;color:#00d9ff;font-size:2em;text-shadow:0 0 20px rgba(0,217,255,0.3)}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:15px;margin-bottom:25px}
    .card{background:rgba(255,255,255,0.05);border-radius:12px;padding:18px;border:1px solid rgba(255,255,255,0.1)}
    .card h2{color:#00d9ff;margin-bottom:12px;font-size:1.1em}
    .big-num{font-size:2.2em;font-weight:bold;text-align:center}
    .green{color:#00ff88}.red{color:#ff4444}.yellow{color:#ffda44}.blue{color:#00d9ff}.orange{color:#ff9944}
    .label{text-align:center;color:#888;font-size:0.85em;margin-top:4px}
    table{width:100%;border-collapse:collapse;font-size:0.85em}
    th{color:#00d9ff;text-align:left;padding:8px 6px;border-bottom:1px solid rgba(255,255,255,0.1)}
    td{padding:6px;border-bottom:1px solid rgba(255,255,255,0.05)}
    .pnl-pos{color:#00ff88}.pnl-neg{color:#ff4444}
    .badge{padding:2px 8px;border-radius:10px;font-size:0.8em}
    .badge-green{background:rgba(0,255,136,0.15);color:#00ff88}
    .badge-red{background:rgba(255,68,68,0.15);color:#ff4444}
    .badge-yellow{background:rgba(255,218,68,0.15);color:#ffda44}
    .refresh-btn{position:fixed;top:15px;right:15px;background:#00d9ff;color:#1a1a2e;border:none;padding:8px 16px;border-radius:8px;cursor:pointer;font-weight:bold}
    .open-tag{background:rgba(0,217,255,0.15);color:#00d9ff;padding:2px 6px;border-radius:4px;font-size:0.75em}
    .live-tag{background:rgba(255,153,68,0.2);color:#ff9944;padding:2px 6px;border-radius:4px;font-size:0.75em}
  </style>
</head>
<body>
  <button class="refresh-btn" onclick="loadData()">刷新</button>
  <div class="container">
    <h1>💎 Premium Channel Dashboard</h1>
    <!-- 钱包余额 - 始终显示 -->
    <div style="text-align:center;margin-bottom:15px">
      <span style="font-size:1.8em;font-weight:bold;color:#00d9ff" id="wallet-sol">--</span>
      <span style="color:#888;font-size:0.9em;margin-left:6px">SOL</span>
    </div>
    <!-- 交易控制面板 -->
    <div style="display:flex;gap:10px;justify-content:center;margin-bottom:15px;align-items:center">
      <span id="trading-status" style="font-size:0.9em;color:#888">加载中...</span>
      <button id="btn-pause-p" onclick="toggleTrading('pause')" style="padding:6px 16px;cursor:pointer;background:#ff4757;border:none;border-radius:6px;color:#fff;font-weight:bold;display:none">⏸ 暂停交易</button>
      <button id="btn-resume-p" onclick="toggleTrading('resume')" style="padding:6px 16px;cursor:pointer;background:#2ed573;border:none;border-radius:6px;color:#fff;font-weight:bold;display:none">▶ 恢复交易</button>
      <button onclick="resetDailyLoss()" style="padding:6px 16px;cursor:pointer;background:#ffa502;border:none;border-radius:6px;color:#fff;font-weight:bold;" title="重置今日亏损统计起点，不删除历史数据">🔄 重置今日亏损</button>
    </div>

    <!-- 实盘交易 -->
    <div id="live-content">
      <div class="grid" id="live-summary"></div>
      <div class="card" style="margin-bottom:20px"><h2>🟢 实盘持仓</h2><table id="live-open-table"><thead><tr><th>代币</th><th>入场MC</th><th>仓位(SOL)</th><th>已卖/剩余</th><th>已收回SOL</th><th>TP状态</th><th>最高</th><th>最低</th><th>⏱️持有时间</th></tr></thead><tbody></tbody></table></div>
      <div class="card"><h2>📋 实盘交易记录</h2><table id="live-recent-table"><thead><tr><th>代币</th><th>入场MC</th><th>仓位</th><th>实际PnL</th><th>峰值</th><th>损失</th><th>捕获率</th><th>出场原因</th><th>⏱️持仓</th><th>时间</th></tr></thead><tbody></tbody></table></div>
      <div style="text-align:center;margin-top:20px"><button id="btn-reset" onclick="resetLiveData()" style="padding:8px 20px;cursor:pointer;background:#ff6348;border:none;border-radius:6px;color:#fff;font-weight:bold">🗑 清空实盘数据重新开始</button></div>
    </div>
  </div>
  <script>

    const _token=new URLSearchParams(window.location.search).get('token')||'';
    const _q=_token?'?token='+encodeURIComponent(_token):'';
    async function loadData(){
      try{
        // 实盘数据
        const liveRes=await fetch('/api/live-positions'+_q);
        const live=await liveRes.json();
        const ls=live.summary;
        const solSpent=(ls.totalSolSpent||0);
        const solRecv=(ls.totalSolReceived||0);
        const netSol=solRecv-solSpent;
        document.getElementById('live-summary').innerHTML=
          '<div class="card"><div class="big-num '+(ls.winRate>=60?'green':ls.winRate>=40?'yellow':'red')+'">'+(ls.winRate||0)+'%</div><div class="label">胜率 ('+(ls.wins||0)+'W/'+(ls.losses||0)+'L / '+(ls.total||0)+'笔)</div></div>'+
          '<div class="card"><div class="big-num '+(netSol>=0?'green':'red')+'">'+(netSol>=0?'+':'')+netSol.toFixed(4)+'</div><div class="label">净盈亏 SOL</div></div>'+
          '<div class="card"><div class="big-num orange">'+(solSpent).toFixed(4)+'</div><div class="label">总投入 SOL</div></div>'+
          '<div class="card"><div class="big-num '+(solRecv>=solSpent?'green':'red')+'">'+(solRecv).toFixed(4)+'</div><div class="label">总收回 SOL</div></div>'+
          '<div class="card"><div class="big-num blue">'+live.open.length+'</div><div class="label">当前持仓</div></div>';

        const lotb=document.querySelector('#live-open-table tbody');
        lotb.innerHTML=live.open.map(r=>{
          const holdSec=r.entry_time?Math.floor((Date.now()-new Date(r.entry_time).getTime())/1000):0;
          const holdStr=holdSec>=3600?Math.floor(holdSec/3600)+'h'+Math.floor((holdSec%3600)/60)+'m':(holdSec>=60?Math.floor(holdSec/60)+'m'+holdSec%60+'s':holdSec+'s');
          const soldPct=r.sold_pct||0;
          const remainPct=100-soldPct;
          const solRecv=(r.total_sol_received||0).toFixed(4);
          const tps=[];
          if(r.tp1_triggered)tps.push('TP1✅');
          if(r.tp2_triggered)tps.push('TP2✅');
          if(r.tp3_triggered)tps.push('TP3✅');
          if(r.tp4_triggered)tps.push('TP4✅');
          if(r.moonbag_active)tps.push('🌙');
          const tpStr=tps.length?tps.join(' '):'—';
          const soldColor=soldPct>0?'color:#ffa502':'color:#666';
          const recvColor=(r.total_sol_received||0)>0?'color:#2ed573':'color:#666';
          return '<tr><td>$'+r.symbol+' <span class="live-tag">LIVE</span></td><td>$'+r.entry_mc_k+'K</td><td>'+r.entry_sol+'</td><td style="'+soldColor+'">'+soldPct+'%已卖 / '+remainPct+'%剩余</td><td style="'+recvColor+'">'+solRecv+'</td><td>'+tpStr+'</td><td class="pnl-pos">+'+(r.high_pnl||0).toFixed(1)+'%</td><td class="pnl-neg">'+(r.low_pnl||0).toFixed(1)+'%</td><td style="color:#00d9ff;font-weight:bold">'+holdStr+'</td></tr>';
        }).join('');

        const lrtb=document.querySelector('#live-recent-table tbody');
        lrtb.innerHTML=live.recent.map(r=>{
          const realPnlCls=(r.real_pnl||0)>0?'pnl-pos':'pnl-neg';
          const captureCls=(r.capture_rate||0)>70?'pnl-pos':(r.capture_rate||0)>40?'':'pnl-neg';
          const t=r.closed_at?new Date(r.closed_at).toLocaleString('zh-CN',{hour:'2-digit',minute:'2-digit'}):'';
          let holdStr='-';
          if(r.entry_time&&r.closed_at){const hs=Math.floor((new Date(r.closed_at).getTime()-new Date(r.entry_time).getTime())/1000);holdStr=hs>=3600?Math.floor(hs/3600)+'h'+Math.floor((hs%3600)/60)+'m':(hs>=60?Math.floor(hs/60)+'m'+(hs%60)+'s':hs+'s');}
          return '<tr><td>$'+r.symbol+'</td><td>$'+r.entry_mc_k+'K</td><td>'+r.entry_sol+'</td><td class="'+realPnlCls+'">'+(r.real_pnl>=0?'+':'')+(r.real_pnl||0).toFixed(1)+'%</td><td class="pnl-pos">+'+(r.high_pnl||0).toFixed(1)+'%</td><td class="pnl-neg">-'+(r.loss||0).toFixed(1)+'%</td><td class="'+captureCls+'">'+(r.capture_rate||0).toFixed(0)+'%</td><td>'+(r.exit_reason||'-')+'</td><td>'+holdStr+'</td><td>'+t+'</td></tr>';
        }).join('');
      }catch(e){document.getElementById('summary').innerHTML='<div class="card"><div class="big-num red">加载失败</div></div>';}
    }

    // 交易状态轮询
    async function refreshTradingStatus(){
      try{
        const r=await fetch('/api/trading-status'+_q);
        const d=await r.json();
        const el=document.getElementById('trading-status');
        const btnP=document.getElementById('btn-pause-p');
        const btnR=document.getElementById('btn-resume-p');
        const canTrade=d.canTrade;
        const isPaused=d.paused;
        const isBlocked=canTrade&&!canTrade.allowed;
        const lossInfo=d.consecutiveLosses?' | 连亏:'+d.consecutiveLosses:'';
        const dailyInfo=d.dailyNetPnl!==undefined?' | 今日:'+(d.dailyNetPnl>=0?'+':'')+d.dailyNetPnl+'/'+(-d.dailyLossLimit)+' SOL':'';
        if(isPaused){
          const until=d.pausedUntil?new Date(d.pausedUntil).toLocaleString('zh-CN'):'';
          el.innerHTML='🔴 <span style="color:#ff4757">交易已暂停</span>'+(until?' (至 '+until+')':'')+lossInfo+dailyInfo;
          btnP.style.display='none';btnR.style.display='inline-block';
        }else if(isBlocked){
          el.innerHTML='🟡 <span style="color:#ffa502">交易受限</span> — '+(canTrade.reason||'')+lossInfo+dailyInfo;
          btnP.style.display='none';btnR.style.display='inline-block';
        }else{
          el.innerHTML='🟢 <span style="color:#2ed573">交易正常</span>'+lossInfo+dailyInfo;
          btnP.style.display='inline-block';btnR.style.display='none';
        }
      }catch(e){}
    }

    async function toggleTrading(action){
      if(action==='pause'&&!confirm('确认暂停交易？将暂停4小时。'))return;
      if(action==='resume'&&!confirm('确认恢复交易？连亏计数将重置。'))return;
      try{
        const ep=action==='pause'?'/api/pause-trading':'/api/resume-trading';
        const r=await fetch(ep+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);refreshTradingStatus();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function resetDailyLoss(){
      if(!confirm('确认重置今日亏损统计起点？\n历史数据不会被删除，仅从当前时间重新开始计算。'))return;
      try{
        const r=await fetch('/api/reset-daily-loss'+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);location.reload();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function resetLiveData(){
      if(!confirm('⚠️ 确认清空所有实盘交易数据？此操作不可撤销！'))return;
      if(!confirm('再次确认：将删除所有实盘交易记录并重置风控状态'))return;
      try{
        const r=await fetch('/api/reset-live-data'+_q,{method:'POST'});
        const d=await r.json();
        if(d.success){alert(d.message);location.reload();}
        else alert('失败: '+(d.error||'未知'));
      }catch(e){alert('请求失败: '+e.message);}
    }

    async function refreshWallet(){
      try{
        const r=await fetch('/api/wallet-balance'+_q);
        const d=await r.json();
        const el=document.getElementById('wallet-sol');
        if(d.balance!==null&&d.balance!==undefined){
          el.textContent=Number(d.balance).toFixed(4);
          el.style.color='#00d9ff';
        }else{
          el.textContent=d.error||'无法获取';
          el.style.color='#ff4757';
          el.style.fontSize='0.8em';
        }
      }catch(e){
        document.getElementById('wallet-sol').textContent='连接失败';
      }
    }

    loadData();
    refreshTradingStatus();
    refreshWallet();
    setInterval(loadData,15000);
    setInterval(refreshTradingStatus,10000);
    setInterval(refreshWallet,8000);
  </script>
</body>
</html>`;
}

/**
 * 启动服务器
 */
export function startDashboardServer(attempt = 0) {
  // Always use the configured PORT — never increment. On cloud platforms (Zeabur),
  // PORT is fixed and health checks only probe that exact port. Binding to PORT+N
  // means health checks fail, causing an infinite restart loop.
  const targetPort = parseInt(PORT);
  const MAX_ATTEMPTS = 10;
  const retryDelayMs = Math.min(3000 * (attempt + 1), 15000);

  server.removeAllListeners('error');

  server.on('error', (error) => {
    if (error?.code === 'EADDRINUSE' || error?.code === 'EPERM') {
      if (attempt < MAX_ATTEMPTS) {
        console.warn(`⚠️ Port ${targetPort} in use (${error.code}), retry ${attempt + 1}/${MAX_ATTEMPTS} in ${retryDelayMs}ms...`);
        setTimeout(() => startDashboardServer(attempt + 1), retryDelayMs);
      } else {
        console.error(`❌ Failed to bind port ${targetPort} after ${MAX_ATTEMPTS} attempts — health checks will fail.`);
      }
      return;
    }
    console.error(`❌ Dashboard server error: ${error.message}`);
  });

  try {
    server.listen(targetPort, '0.0.0.0', () => {
      console.log(`🌐 Dashboard server running at http://0.0.0.0:${targetPort}`);
      startRawDogDiscoveryObserver();
    });
  } catch (error) {
    console.error(`❌ Sync listen error:`, error);
  }

  return server;
}

// 直接运行时启动服务器
// 兼容 PM2 启动方式 (process.env.name 在 ecosystem.config.cjs 中定义)
if (import.meta.url === `file://${process.argv[1]}` || process.env.name === 'dashboard') {
  startDashboardServer();
}

export default { startDashboardServer };
