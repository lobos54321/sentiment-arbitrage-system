import test from 'node:test';
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';

import { PremiumSignalEngine } from '../src/engines/premium-signal-engine.js';

test('premium signal records persist parsed narrative links and score', () => {
  const db = new Database(':memory:');
  const engine = Object.create(PremiumSignalEngine.prototype);
  engine.db = db;
  engine.initDB();

  const id = engine.saveSignalRecord({
    token_ca: 'TokenNarrative111111111111111111111111111111',
    symbol: 'TOOL',
    market_cap: 42000,
    description: 'AI agent CLI https://x.com/example/status/1 https://github.com/acme/tool',
    timestamp: 1_777_900_000,
    is_ath: false,
  }, 'NOT_ATH_V17', null, false);

  assert.ok(id > 0);
  const row = db.prepare(`
    SELECT signal_links_json, narrative_features_json, narrative_score, narrative_confidence, narrative_tags
    FROM premium_signals
    WHERE id = ?
  `).get(id);

  const links = JSON.parse(row.signal_links_json);
  const features = JSON.parse(row.narrative_features_json);
  assert.equal(links.length, 2);
  assert.ok(row.narrative_score > 10);
  assert.ok(row.narrative_confidence > 30);
  assert.ok(row.narrative_tags.includes('x_link'));
  assert.ok(features.tags.includes('github_link'));

  db.close();
});
