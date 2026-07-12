import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';

import { PremiumChannelListener } from '../src/inputs/premium-channel-listener.js';
import {
  GAP_STAGING_SCHEMA_VERSION,
  mergeGapStaging,
  normalizeTelegramMessage,
} from '../src/inputs/premium-telegram-watermark.js';
import { selectGapRows } from '../scripts/stage-premium-telegram-gap.mjs';

const CHANNEL_ID = 3636518327;
const SIGNAL_TEXT = '🔥 Test Token New Trending\nSYMBOL: $TEST\nCA: So11111111111111111111111111111111111111112';

function telegramMessage(id, iso, text = SIGNAL_TEXT) {
  return {
    id,
    date: Math.floor(Date.parse(iso) / 1000),
    message: text,
  };
}

test('gap staging is research-only and deduplicates Telegram message ids', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-gap-stage-'));
  try {
    const output = path.join(dir, 'staging.json');
    const row = normalizeTelegramMessage(
      telegramMessage(101, '2026-07-12T01:00:00Z'),
      CHANNEL_ID,
    );
    mergeGapStaging(output, [row, row], {
      channel_id: CHANNEL_ID,
      incident_since: '2026-07-09T16:41:57Z',
    });
    const payload = JSON.parse(fs.readFileSync(output, 'utf8'));
    assert.equal(payload.schema_version, GAP_STAGING_SCHEMA_VERSION);
    assert.equal(payload.row_count, 1);
    assert.equal(payload.execution_allowed, false);
    assert.equal(payload.emitted_to_signal_callbacks, false);
    assert.equal(payload.written_to_premium_signals, false);
    assert.equal(payload.rows[0].execution_allowed, false);
    assert.equal(payload.rows[0].allowed_use, 'research_only');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('manual gap selection only includes signal messages after the incident watermark', () => {
  const rows = selectGapRows([
    telegramMessage(1, '2026-07-09T16:40:00Z'),
    telegramMessage(2, '2026-07-09T16:42:00Z', 'ordinary channel update'),
    telegramMessage(3, '2026-07-09T16:43:00Z'),
  ], CHANNEL_ID, Date.parse('2026-07-09T16:41:57Z'));
  assert.deepEqual(rows.map((row) => row.message_id), ['3']);
});

test('watermark watchdog stages and requests reconnect after repeated live-stream lag', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-watermark-'));
  try {
    const statePath = path.join(dir, 'watermark.json');
    const stagingPath = path.join(dir, 'staging.json');
    const listener = new PremiumChannelListener({
      watermarkEnabled: true,
      watermarkStatePath: statePath,
      gapStagingPath: stagingPath,
      watermarkLagThreshold: 2,
      watermarkHistoryLimit: 20,
    });
    let emittedSignals = 0;
    listener.onSignal(() => { emittedSignals += 1; });
    listener.channelEntity = { id: CHANNEL_ID };
    let messages = [telegramMessage(200, '2026-07-12T01:00:00Z', 'status')];
    listener.client = {
      getMessages: async () => messages,
    };

    const baseline = await listener._checkMessageWatermark();
    assert.equal(baseline.baseline_initialized, true);

    messages = [
      telegramMessage(201, '2026-07-12T01:02:00Z'),
      telegramMessage(200, '2026-07-12T01:00:00Z', 'status'),
    ];
    const firstLag = await listener._checkMessageWatermark();
    assert.equal(firstLag.lag_observed, true);
    assert.equal(firstLag.lag_checks, 1);
    assert.equal(fs.existsSync(stagingPath), false);

    const secondLag = await listener._checkMessageWatermark();
    assert.equal(secondLag.reconnect, true);
    assert.equal(secondLag.latest_message_id, '201');
    const staging = JSON.parse(fs.readFileSync(stagingPath, 'utf8'));
    assert.equal(staging.row_count, 1);
    assert.equal(staging.rows[0].message_id, '201');
    assert.equal(staging.execution_allowed, false);
    assert.equal(emittedSignals, 0);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a delivered live event prevents false gap recovery', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-watermark-live-'));
  try {
    const statePath = path.join(dir, 'watermark.json');
    const stagingPath = path.join(dir, 'staging.json');
    const listener = new PremiumChannelListener({
      watermarkEnabled: true,
      watermarkStatePath: statePath,
      gapStagingPath: stagingPath,
      watermarkLagThreshold: 1,
    });
    listener.channelEntity = { id: CHANNEL_ID };
    let messages = [telegramMessage(300, '2026-07-12T02:00:00Z', 'status')];
    listener.client = { getMessages: async () => messages };
    await listener._checkMessageWatermark();

    const delivered = telegramMessage(301, '2026-07-12T02:02:00Z');
    listener._recordLiveMessage(delivered);
    messages = [delivered, ...messages];
    const result = await listener._checkMessageWatermark();
    assert.equal(result.healthy, true);
    assert.equal(fs.existsSync(stagingPath), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('listener primes Telegram update state after registering the event handler', async () => {
  const listener = new PremiumChannelListener({ watermarkEnabled: false });
  const calls = [];
  listener.client = {
    getMe: async (inputPeer) => calls.push(['getMe', inputPeer]),
    invoke: async (request) => calls.push(['invoke', request.className]),
  };
  await listener._primeUpdateStream();
  assert.deepEqual(calls, [
    ['getMe', true],
    ['invoke', 'updates.GetState'],
  ]);
});
