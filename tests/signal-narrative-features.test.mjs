import test from 'node:test';
import assert from 'node:assert/strict';

import { extractSignalLinks, scoreNarrativeFeatures } from '../src/scoring/signal-narrative-features.js';

test('signal narrative parser extracts X and GitHub links', () => {
  const links = extractSignalLinks('X https://x.com/example/status/1 GitHub https://github.com/acme/tool.');

  assert.equal(links.length, 2);
  assert.equal(links[0].type, 'x');
  assert.equal(links[1].type, 'github');
  assert.equal(links[1].url, 'https://github.com/acme/tool');
});

test('signal narrative score treats links as soft features', () => {
  const result = scoreNarrativeFeatures({
    symbol: 'TOOL',
    description: 'AI agent terminal CLI on pump.fun https://x.com/example/status/1 https://github.com/acme/tool',
  });

  assert.ok(result.score > 10);
  assert.ok(result.confidence > 30);
  assert.ok(result.tags.includes('x_link'));
  assert.ok(result.tags.includes('github_link'));
  assert.ok(result.tags.includes('ai_agent'));
  assert.ok(result.tags.includes('dev_tool'));
});

test('signal narrative parser does not treat AI Index metric as ai agent narrative', () => {
  const result = scoreNarrativeFeatures({
    symbol: 'HOPE',
    name: 'A New Hope',
    rawMessage: `
🔥 **A New Hope** **New Trending** | #SOLANA

🔗 🐦 https://twitter.com/WhiteHouse/status/2051412229250056458 • DexScreener

✡ **Super Index**： 70

AI        Index：35
Trade     Index：10
Security  Index：10
Address   Index：0
Sentiment Index：10
Media     Index：5
Organic Buyers：12
`,
  });

  assert.ok(result.tags.includes('x_link'));
  assert.ok(!result.tags.includes('ai_agent'));
});
