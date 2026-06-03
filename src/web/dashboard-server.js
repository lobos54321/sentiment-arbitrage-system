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
const logsDir = join(projectRoot, 'logs');
const runtimeLogPath = join(logsDir, 'runtime.log');
const DASHBOARD_AUDIT_SCHEMA_VERSION = 'v2.7.0.audit_log_integrity.v1';
const DASHBOARD_AUDIT_GENESIS_HASH = 'GENESIS';
export const LOG_REDACTION_PATTERN_SET = 'v2.7.0.secret_pattern_set.dashboard_runtime.v1';

// 确保日志目录存在
try {
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
} catch (e) { /* ignore */ }

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
function getDb() {
  if (!db) {
    try {
      db = new Database(resolvedDbPath);
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

function tempSqliteDownloadPath(prefix) {
  const safePrefix = String(prefix || 'sqlite_download').replace(/[^a-z0-9_-]/gi, '_').slice(0, 48);
  return join('/tmp', `${safePrefix}_${process.pid}_${Date.now()}_${randomUUID()}.db`);
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
    sourceDb = new Database(sourcePath, { readonly: true, fileMustExist: true, timeout });
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
    await sourceDb.backup(snapshotPath);
    return {
      path: snapshotPath,
      cleanupPath: snapshotPath,
      mode: 'sqlite_backup_snapshot',
      note: 'WAL-safe SQLite backup snapshot',
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
      });
    }
    streamDownloadFile(res, download.path, filename, download.cleanupPath, {
      'X-SQLite-Download-Mode': download.mode,
      'X-SQLite-Download-Note': download.note,
      'X-SQLite-Source-Path': sourcePath,
    });
  } catch (error) {
    res.writeHead(error.statusCode || 500, apiJsonHeaders());
    res.end(JSON.stringify({ error: `${label} backup failed: ${error.message}`, path: sourcePath }));
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

export function readPaperFastLaneHealth(options = {}) {
  const healthPath = options.healthPath || paperFastLaneHealthPath(options.env || process.env);
  try {
    if (!fs.existsSync(healthPath)) {
      return {
        available: false,
        path: healthPath,
        status: 'paper_fast_lane_health_missing',
      };
    }
    const payload = JSON.parse(fs.readFileSync(healthPath, 'utf8'));
    return {
      available: true,
      path: healthPath,
      status: payload?.missed_rescue?.last_error ? 'paper_fast_lane_scan_error' : 'ok',
      schema_version: payload?.schema_version || null,
      updated_at: payload?.updated_at || null,
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

export function resolveDashboardLogPath(pathname, env = process.env) {
  const logPathByEndpoint = {
    '/api/logs/source-resonance': env.SOURCE_RESONANCE_LOG || '/app/data/source-resonance.log',
    '/api/logs/gmgn-scout': env.GMGN_SCOUT_LOG || '/app/data/gmgn-scout.log',
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
    'node.log',
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

  const paperDb = new Database(paperDbPath);
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
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: global.__startupError || paperDbHealth.status === 'paper_db_integrity_marker_present' ? 'degraded' : 'ok',
      message: 'Sentiment Arbitrage API Running',
      timestamp: Date.now(),
      commit: runtimeCommitFingerprint(),
      startup_error: global.__startupError || null,
      shadow_sidecars: {
        available: shadowSidecars.length > 0,
        running: shadowSidecars.filter((worker) => worker.running === true).length,
        total: shadowSidecars.length,
        workers: shadowSidecars,
      },
      paper_fast_lane_health: paperFastLaneHealth,
      paper_db_health: paperDbHealth,
    }));
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
        paper_trades_db: `${origin}/api/data/download/paper-trades?token=${tokenHint}`,
        kline_cache_db: `${origin}/api/data/download/kline-cache?token=${tokenHint}`,
        lifecycle_tracks_db: `${origin}/api/data/download/lifecycle-tracks?token=${tokenHint}`,
        canonical_ledger_json: `${origin}/api/data/download/canonical-ledger?token=${tokenHint}`,
      },
      review_apis: {
        a_class_status: `${origin}/api/a-class/status?token=${tokenHint}&hours=24`,
        a_class_events: `${origin}/api/a-class/events?token=${tokenHint}&hours=24&limit=500`,
        a_class_scorecard: `${origin}/api/scorecard/a-class?token=${tokenHint}&hours=168`,
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
      const limit = boundedIntParam(url, 'limit', 10000, 1, 100000);
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 5000, 1000, 30000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      const tables = {};
      for (const table of ['canonical_trade_ledger', 'a_class_decision_events', 'paper_missed_signal_attribution']) {
        if (!tableNames.has(table)) {
          tables[table] = { available: false, rows: [] };
          continue;
        }
        const orderCol = table === 'canonical_trade_ledger'
          ? 'COALESCE(entry_ts, exit_ts, created_at, 0)'
          : (table === 'a_class_decision_events' ? 'event_ts' : 'COALESCE(created_event_ts, 0)');
        const rows = paperDb.prepare(`
          SELECT *
          FROM ${table}
          ORDER BY ${orderCol} DESC, id DESC
          LIMIT @limit
        `).all({ limit });
        tables[table] = { available: true, count: rows.length, rows };
      }
      const payload = {
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        export_type: 'canonical_ledger_and_a_class_evidence',
        limit,
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
          signalDb = new Database(resolvedDbPath, { readonly: true });
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

      const paperDbPath = getPaperDbPath();
      let paperTrades = [];
      let missedAttributions = [];
      if (fs.existsSync(paperDbPath)) {
        paperDb = new Database(paperDbPath, { readonly: true });
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
        sinceTs,
      });
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...audit,
        db_path: paperDbPath,
        premium_signal_query_limit: limit,
        notes: {
          audit_goal: 'Compare upstream premium signal market-cap outcomes with paper trade coverage.',
          missed_recovery_difference: 'missed-recovery-summary only covers paper_missed_signal_attribution; this endpoint starts from premium_signals and therefore exposes coverage gaps.',
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: paperDbTimeoutMs });
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: paperDbTimeoutMs });
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

      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({
        ...snapshot,
        freeze,
        persisted_files: persistedFiles,
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, {
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true });
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
    let paperDb;
    try {
      const sinceTs = boundedWindowedSinceTs(url, 6, 168, { allowAll: true });
      const limit = boundedIntParam(url, 'limit', 30, 1, 200);
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
               freshness_json, budget_json, risk_json
        FROM a_class_decision_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all({ ...params, limit }).map(aClassEventRow);
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: true,
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
      const action = String(url.searchParams.get('action') || '').trim().toUpperCase();
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
      const tableNames = new Set(paperDb.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((row) => row.name));
      if (!tableNames.has('a_class_decision_events')) {
        res.writeHead(200, apiJsonHeaders());
        res.end(JSON.stringify({ generated_at: new Date().toISOString(), db_path: paperDbPath, available: false, events: [] }, null, 2));
        return;
      }
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
      const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
      const events = paperDb.prepare(`
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, route_bucket,
               normalized_mode, source_table, source_id, source_component,
               source_reason, action, grade, size_sol, score, reason,
               hard_blockers_json, soft_notes_json,
               freshness_json, budget_json, risk_json
        FROM a_class_decision_events
        ${where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
      `).all(params).map(aClassEventRow);
      res.writeHead(200, apiJsonHeaders());
      res.end(JSON.stringify({
        generated_at: new Date().toISOString(),
        db_path: paperDbPath,
        available: true,
        since_ts: sinceTs,
        action: action || null,
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
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
      const sinceTs = boundedWindowedSinceTs(url, 24 * 7, 24 * 120, { allowAll: true });
      const aClassScorecard = url.pathname === '/api/scorecard/a-class';
      paperDb = new Database(paperDbPath, { readonly: true, timeout: boundedIntParam(url, 'paper_db_timeout_ms', 1500, 0, 5000) });
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
