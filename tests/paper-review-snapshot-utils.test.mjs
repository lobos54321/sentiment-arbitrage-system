import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  buildPaperReviewMarkdown,
  buildPaperReviewSnapshot,
  buildTradeReviewSummary,
  capitalTierForTrade,
  reviewSnapshotBaseName,
  writePaperReviewSnapshotFiles,
} from '../src/web/paper-review-snapshot-utils.js';

test('capitalTierForTrade separates tiny probes from stage1 size', () => {
  assert.equal(capitalTierForTrade({
    entry_mode: 'hard_gate_pass_tiny_probe',
    strategy_stage: 'stage1',
    position_size_sol: 0.002,
  }), 'tiny_probe');
  assert.equal(capitalTierForTrade({
    entry_mode: 'stage1',
    strategy_stage: 'stage1',
    position_size_sol: 0.06,
  }), 'stage1_main');
  assert.equal(capitalTierForTrade({
    entry_mode: 'lotto_fast_lane',
    position_size_sol: 0.01,
  }), 'lotto');
});

test('buildTradeReviewSummary calculates giveback and tier rollups', () => {
  const summary = buildTradeReviewSummary([
    {
      id: 1,
      symbol: 'DOG1',
      token_ca: 'A',
      entry_mode: 'hard_gate_pass_tiny_probe',
      entry_ts: 100,
      exit_ts: 130,
      exit_reason: 'trail',
      position_size_sol: 0.002,
      pnl_pct: 0.04,
      peak_pnl: 10.0,
      mark_peak_pnl: 10.0,
      trusted_peak_pnl: 0.15,
      peak_trust_status: 'peak_untrusted_mark_spike',
    },
    {
      id: 2,
      symbol: 'DOG2',
      token_ca: 'B',
      entry_mode: 'stage1',
      strategy_stage: 'stage1',
      entry_ts: 200,
      exit_ts: 260,
      exit_reason: 'stop',
      position_size_sol: 0.06,
      pnl_pct: -0.05,
      peak_pnl: 0.02,
    },
  ]);

  assert.equal(summary.totals.total, 2);
  assert.equal(summary.totals.closed, 2);
  assert.equal(summary.totals.win_rate_pct, 50);
  assert.equal(summary.totals.avg_giveback_pct, 9);
  assert.equal(summary.totals.mark_only_peak_spikes, 1);
  assert.equal(summary.by_capital_tier.find((row) => row.capital_tier === 'tiny_probe').avg_giveback_pct, 11);
  assert.equal(summary.by_capital_tier.find((row) => row.capital_tier === 'stage1_main').avg_pnl_pct, -5);
  assert.equal(summary.top_giveback_trades[0].symbol, 'DOG1');
  assert.equal(summary.top_giveback_trades[0].peak_pnl_pct, 15);
  assert.equal(summary.top_giveback_trades[0].mark_peak_pnl_pct, 1000);
});

test('buildPaperReviewSnapshot and markdown preserve review fingerprints', () => {
  const tradeReview = buildTradeReviewSummary([
    {
      id: 1,
      symbol: 'DOG1',
      token_ca: 'A',
      entry_mode: 'hard_gate_pass_tiny_probe',
      entry_ts: 100,
      exit_ts: 130,
      exit_reason: 'trail',
      position_size_sol: 0.002,
      pnl_pct: 0.04,
      peak_pnl: 0.15,
    },
  ]);
  const snapshot = buildPaperReviewSnapshot({
    generatedAt: '2026-05-14T00:00:00.000Z',
    commit: 'abcdef1234567890',
    window: {
      label: '8h',
      since_ts: 100,
      since_iso: '2026-05-13T16:00:00.000Z',
      until_ts: 200,
      until_iso: '2026-05-14T00:00:00.000Z',
    },
    dbPath: '/tmp/paper.db',
    closedLoop: {
      premium_signals: { premium_signal_rows: 10, premium_unique_tokens: 7, hard_gate_pass_unique: 3 },
      source_resonance: { unique_tokens: 4, gmgn_pre_seen_unique: 2, quote_clean_unique: 1 },
      probes: {
        paper_pnl_by_entry_mode: [
          { entry_mode: 'hard_gate_pass_tiny_probe', fills: 1, fill_unique: 1, wins: 1, win_rate: 1, avg_pnl_pct: 4, max_peak_pnl_pct: 15 },
        ],
        by_mode: {
          hard_gate_pass_tiny_probe: { fills: 1, avg_pnl_pct: 4 },
        },
      },
      missed_dogs: { unique_tokens: 2, quote_clean_dog_unique: 1, gold_unique: 0, silver_unique: 1, bronze_unique: 0, top_missed_dogs: [], by_final_blocker: [] },
    },
    tradeReview,
    notes: ['paper-only review snapshot'],
  });

  assert.equal(snapshot.summary.premium_signal_rows, 10);
  assert.equal(snapshot.summary.hard_gate_pass_probe_fills, 1);
  assert.equal(snapshot.summary.paper_avg_giveback_pct, 11);
  assert.match(reviewSnapshotBaseName(snapshot), /^paper_review_20260514_000000000Z_8h_abcdef123456/);
  assert.match(buildPaperReviewMarkdown(snapshot), /paper-only review snapshot/);
});

test('writePaperReviewSnapshotFiles writes json and markdown atomically', () => {
  const dir = fs.mkdtempSync(join(os.tmpdir(), 'paper-review-test-'));
  const snapshot = buildPaperReviewSnapshot({
    generatedAt: '2026-05-14T00:00:00.000Z',
    commit: 'abcdef123456',
    window: { label: '1h', since_iso: null, until_iso: '2026-05-14T00:00:00.000Z' },
    dbPath: '/tmp/paper.db',
    closedLoop: {},
    tradeReview: buildTradeReviewSummary([]),
  });

  const files = writePaperReviewSnapshotFiles(snapshot, { dir });
  assert.ok(fs.existsSync(files.json_path));
  assert.ok(fs.existsSync(files.markdown_path));
  assert.equal(JSON.parse(fs.readFileSync(files.json_path, 'utf8')).schema_version, 1);
  assert.match(fs.readFileSync(files.markdown_path, 'utf8'), /Paper Review Snapshot/);
});
