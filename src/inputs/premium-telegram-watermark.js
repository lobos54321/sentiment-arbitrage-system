import fs from 'fs';
import path from 'path';

export const WATERMARK_SCHEMA_VERSION = 'premium_telegram_ingestion_watermark.v1';
export const GAP_STAGING_SCHEMA_VERSION = 'premium_telegram_gap_staging.v1';

export function messageIdString(value) {
  if (value === null || value === undefined || value === '') return null;
  const raw = value?.value !== undefined ? value.value : value;
  const normalized = String(raw);
  return /^\d+$/.test(normalized) ? normalized : null;
}

export function compareMessageIds(left, right) {
  const a = messageIdString(left);
  const b = messageIdString(right);
  if (a === b) return 0;
  if (a === null) return -1;
  if (b === null) return 1;
  const aa = BigInt(a);
  const bb = BigInt(b);
  return aa < bb ? -1 : 1;
}

export function premiumSignalKind(text) {
  const value = String(text || '');
  if (value.includes('🔥') && value.includes('New Trending')) return 'NEW_TRENDING';
  if (value.includes('📈') && value.includes('ATH')) return 'ATH';
  return null;
}

export function normalizeTelegramMessage(message, channelId) {
  const messageId = messageIdString(message?.id);
  if (!messageId) return null;
  const text = String(message?.message || message?.text || '');
  const rawDate = Number(message?.date || 0);
  const messageTs = Number.isFinite(rawDate) && rawDate > 0
    ? new Date(rawDate * 1000).toISOString()
    : null;
  return {
    channel_id: String(channelId),
    message_id: messageId,
    message_ts: messageTs,
    signal_kind: premiumSignalKind(text),
    raw_text: text,
    execution_allowed: false,
    allowed_use: 'research_only',
  };
}

export function defaultWatermarkState(channelId) {
  return {
    schema_version: WATERMARK_SCHEMA_VERSION,
    channel_id: String(channelId),
    status: 'uninitialized',
    updated_at: null,
    last_live_message_id: null,
    last_live_message_ts: null,
    last_history_message_id: null,
    last_history_message_ts: null,
    pending_history_message_id: null,
    pending_lag_checks: 0,
    last_staged_message_id: null,
    last_staged_at: null,
    reconnect_count: 0,
    last_error: null,
  };
}

export function readJsonFile(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

export function writeJsonAtomic(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tempPath, filePath);
}

export function readWatermarkState(filePath, channelId) {
  const existing = readJsonFile(filePath, null);
  if (!existing || existing.schema_version !== WATERMARK_SCHEMA_VERSION) {
    return defaultWatermarkState(channelId);
  }
  if (String(existing.channel_id) !== String(channelId)) {
    return defaultWatermarkState(channelId);
  }
  return {
    ...defaultWatermarkState(channelId),
    ...existing,
    channel_id: String(channelId),
  };
}

export function writeWatermarkState(filePath, state) {
  const payload = {
    ...state,
    schema_version: WATERMARK_SCHEMA_VERSION,
    updated_at: new Date().toISOString(),
  };
  writeJsonAtomic(filePath, payload);
  return payload;
}

export function mergeGapStaging(filePath, rows, context = {}) {
  const existing = readJsonFile(filePath, null);
  const priorRows = existing?.schema_version === GAP_STAGING_SCHEMA_VERSION
    && Array.isArray(existing.rows)
    ? existing.rows
    : [];
  const merged = new Map();
  for (const row of [...priorRows, ...(rows || [])]) {
    if (!row?.message_id) continue;
    const key = `${row.channel_id || context.channel_id || 'unknown'}:${row.message_id}`;
    merged.set(key, {
      ...row,
      execution_allowed: false,
      allowed_use: 'research_only',
    });
  }
  const mergedRows = [...merged.values()].sort((left, right) => {
    const byTime = String(left.message_ts || '').localeCompare(String(right.message_ts || ''));
    return byTime || compareMessageIds(left.message_id, right.message_id);
  });
  const payload = {
    schema_version: GAP_STAGING_SCHEMA_VERSION,
    generated_at: new Date().toISOString(),
    execution_allowed: false,
    allowed_use: 'research_only',
    emitted_to_signal_callbacks: false,
    written_to_premium_signals: false,
    channel_id: String(context.channel_id || existing?.channel_id || ''),
    incident_since: context.incident_since || existing?.incident_since || null,
    reason: context.reason || existing?.reason || 'telegram_live_stream_gap',
    row_count: mergedRows.length,
    rows: mergedRows,
  };
  writeJsonAtomic(filePath, payload);
  return payload;
}
