import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';

import { readTelegramIngestionHealth } from '../src/web/dashboard-server.js';

test('dashboard reads the child runtime Telegram watermark without exposing staged message text', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'telegram-dashboard-health-'));
  try {
    const healthPath = path.join(dir, 'watermark.json');
    fs.writeFileSync(healthPath, JSON.stringify({
      schema_version: 'premium_telegram_ingestion_watermark.v1',
      status: 'live_stream_healthy',
      updated_at: '2026-07-12T04:00:00Z',
      channel_id: '3636518327',
      last_live_message_id: '108975',
      last_live_message_ts: '2026-07-12T03:51:11Z',
      last_history_message_id: '108975',
      last_history_message_ts: '2026-07-12T03:51:11Z',
      raw_text: 'must not be exposed',
    }));
    const result = readTelegramIngestionHealth({ healthPath });
    assert.equal(result.available, true);
    assert.equal(result.status, 'live_stream_healthy');
    assert.equal(result.last_live_message_id, '108975');
    assert.equal(Object.hasOwn(result, 'raw_text'), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
