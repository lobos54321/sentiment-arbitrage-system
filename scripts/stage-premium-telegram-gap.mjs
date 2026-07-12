#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import Database from 'better-sqlite3';
import dotenv from 'dotenv';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';

import {
  mergeGapStaging,
  normalizeTelegramMessage,
  writeJsonAtomic,
} from '../src/inputs/premium-telegram-watermark.js';

dotenv.config();

const DEFAULT_CHANNEL_ID = 3636518327;
const DEFAULT_DATA_DIR = process.env.DATA_DIR || '/app/data';

function parseArgs(argv) {
  const args = {
    since: null,
    limit: 5000,
    signalDb: process.env.SENTIMENT_DB || path.join(DEFAULT_DATA_DIR, 'sentiment_arb.db'),
    out: process.env.TELEGRAM_INCIDENT_GAP_STAGING_PATH
      || path.join(DEFAULT_DATA_DIR, 'recovery', 'telegram-gap-incident-staging.json'),
    summaryOut: process.env.TELEGRAM_GAP_STAGING_SUMMARY_PATH
      || path.join(DEFAULT_DATA_DIR, 'recovery', 'telegram-gap-staging-summary.json'),
  };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--since') args.since = argv[++index];
    else if (value === '--limit') args.limit = Number.parseInt(argv[++index], 10);
    else if (value === '--signal-db') args.signalDb = argv[++index];
    else if (value === '--out') args.out = argv[++index];
    else if (value === '--summary-out') args.summaryOut = argv[++index];
  }
  args.limit = Math.max(10, Math.min(20000, Number(args.limit) || 5000));
  return args;
}

export function latestPremiumSignalTimestamp(signalDbPath) {
  if (!signalDbPath || !fs.existsSync(signalDbPath)) return null;
  const db = new Database(signalDbPath, { readonly: true, fileMustExist: true });
  try {
    const columns = new Set(db.prepare('PRAGMA table_info(premium_signals)').all().map((row) => row.name));
    if (!columns.has('timestamp')) return null;
    const expression = columns.has('source_message_ts')
      ? 'COALESCE(source_message_ts, timestamp)'
      : 'timestamp';
    const row = db.prepare(`SELECT MAX(${expression}) AS latest_ts FROM premium_signals`).get();
    const latest = Number(row?.latest_ts || 0);
    return Number.isFinite(latest) && latest > 0 ? new Date(latest).toISOString() : null;
  } finally {
    db.close();
  }
}

export function parseSinceTimestamp(value) {
  const parsed = Date.parse(String(value || ''));
  if (!Number.isFinite(parsed)) throw new Error(`Invalid --since timestamp: ${value}`);
  return parsed;
}

export function selectGapRows(messages, channelId, sinceMs) {
  return messages
    .map((message) => normalizeTelegramMessage(message, channelId))
    .filter((row) => row?.signal_kind && row.message_ts && Date.parse(row.message_ts) > sinceMs)
    .sort((left, right) => String(left.message_ts).localeCompare(String(right.message_ts)));
}

async function run(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const channelId = Number(process.env.PREMIUM_CHANNEL_ID || DEFAULT_CHANNEL_ID);
  const sinceIso = args.since || latestPremiumSignalTimestamp(args.signalDb);
  if (!sinceIso) throw new Error('No --since value and no premium_signals watermark available');
  const sinceMs = parseSinceTimestamp(sinceIso);

  const apiId = Number(process.env.TELEGRAM_API_ID || 0);
  const apiHash = process.env.TELEGRAM_API_HASH || '';
  const sessionString = process.env.TELEGRAM_SESSION || '';
  if (!apiId || !apiHash || !sessionString) {
    throw new Error('Telegram credentials are unavailable');
  }

  const client = new TelegramClient(
    new StringSession(sessionString),
    apiId,
    apiHash,
    { connectionRetries: 5 },
  );
  try {
    await client.connect();
    const dialogs = await client.getDialogs({ limit: 300 });
    const target = dialogs.find((dialog) => {
      const id = dialog.entity?.id?.value ?? dialog.entity?.id;
      return String(id) === String(channelId);
    });
    if (!target?.entity) throw new Error(`Premium channel ${channelId} was not found`);

    const messages = await client.getMessages(target.entity, { limit: args.limit });
    const rows = selectGapRows(messages, channelId, sinceMs);
    const staging = mergeGapStaging(args.out, rows, {
      channel_id: channelId,
      incident_since: new Date(sinceMs).toISOString(),
      reason: 'manual_incident_gap_recovery',
    });
    const summary = {
      schema_version: 'premium_telegram_gap_staging_summary.v1',
      generated_at: new Date().toISOString(),
      execution_allowed: false,
      emitted_to_signal_callbacks: false,
      written_to_premium_signals: false,
      channel_id: String(channelId),
      channel_title: target.title || target.entity.title || null,
      since: new Date(sinceMs).toISOString(),
      fetched_messages: messages.length,
      selected_signal_messages: rows.length,
      staged_unique_rows: staging.row_count,
      earliest_staged_ts: staging.rows[0]?.message_ts || null,
      latest_staged_ts: staging.rows[staging.rows.length - 1]?.message_ts || null,
      staging_path: args.out,
    };
    writeJsonAtomic(args.summaryOut, summary);
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    return summary;
  } finally {
    await client.disconnect().catch(() => {});
  }
}

const isMain = process.argv[1]
  && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (isMain) {
  run().catch((error) => {
    console.error(`[telegram-gap-staging] ${error.message}`);
    process.exitCode = 1;
  });
}
