import test from 'node:test';
import assert from 'node:assert/strict';

import { PremiumChannelListener } from '../src/inputs/premium-channel-listener.js';

const TOKEN_CA = 'Aw5SxKyYhXFdZj2BHCqs11UaV5ohwpFQjauB9jFhpump';

test('ATH parser reads Super Index when current value has no current label', () => {
  const listener = new PremiumChannelListener();
  const signal = listener._parseATHSignal(`
📈 ATH $Apple **12.02X**
💎 \`${TOKEN_CA}\`
🏦 MarketCap  $42.13K —> $506.24K
✡ Super Index：(signal)87 --> 244 🔺180%
AI Index：(signal)45 --> 55 🔺22%
`);

  assert.equal(signal.indices.super_index.signal, 87);
  assert.equal(signal.indices.super_index.current, 244);
  assert.equal(signal.indices.ai_index.signal, 45);
  assert.equal(signal.indices.ai_index.current, 55);
});

test('ATH parser still reads older current-labeled Super Index format', () => {
  const listener = new PremiumChannelListener();
  const signal = listener._parseATHSignal(`
📈New ATH $OOO is up **4.20X** 📈
💎 \`${TOKEN_CA}\`
🏦 MarketCap  $12.86K --> $54.01K
✡ Super Index：(signal)116🔮 --> (current)124🔮 🔺6%
`);

  assert.equal(signal.indices.super_index.signal, 116);
  assert.equal(signal.indices.super_index.current, 124);
});
