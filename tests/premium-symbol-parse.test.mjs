import test from 'node:test';
import assert from 'node:assert/strict';
import { PremiumChannelListener } from '../src/inputs/premium-channel-listener.js';

// _parseSignal and its helpers are pure prototype methods — no constructor needed.
const listener = Object.create(PremiumChannelListener.prototype);

const CA = 'So11111111111111111111111111111111111111112';

test('parses symbol from current 🪙 SYMBOL：$xxx format (full-width colon)', () => {
  const msg = `🔥 **emilio** New Trending \n\n🪙 SYMBOL：$emilio\n🏦 **MC:** 19.16K|**Age:** 1M|**Holders:** 120\nCA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(sig, 'signal should parse');
  assert.equal(sig.symbol, 'emilio');
});

test('parses symbol from legacy 📍 SYMBOL：$xxx format', () => {
  const msg = `🔥 New Trending\n📍 SYMBOL：$KIRBY\n🏦 MC: 21.83K\nCA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(sig);
  assert.equal(sig.symbol, 'KIRBY');
});

test('parses symbol with half-width colon and no emoji', () => {
  const msg = `New Trending\nSYMBOL: $Neko\nCA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(sig);
  assert.equal(sig.symbol, 'Neko');
});

test('falls back to bold header when SYMBOL field is absent', () => {
  const msg = `🔥 **Chaton** New Trending \n\n🏦 **MC:** 54.47K\nCA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(sig);
  assert.equal(sig.symbol, 'Chaton');
});

test('still UNKNOWN when neither field nor header exists (no false extraction)', () => {
  const msg = `Some unrelated message with an address CA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(sig);
  assert.equal(sig.symbol, 'UNKNOWN');
});

test('parse_missing_fields no longer reports symbol for 🪙 format', () => {
  const msg = `🔥 **emilio** New Trending \n\n🪙 SYMBOL：$emilio\n🏦 MC: 19.16K\nCA: ${CA}`;
  const sig = listener._parseSignal(msg);
  assert.ok(!sig.parse_missing_fields.includes('symbol'));
});
