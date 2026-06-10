#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { pathToFileURL } from 'url';

const execFileAsync = promisify(execFile);

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    rawDogJson: process.env.RAW_DOG_DISCOVERY_JSON || '',
    tokensFile: '',
    out: process.env.GMGN_DOG_TOUCH_OUT || './data/gmgn-dog-touch-results.json',
    limit: 64,
    preSec: 60,
    postSec: 7200,
    sleepMs: 4000,
    resolution: '1m',
    gmgnCli: process.env.GMGN_CLI || 'gmgn-cli',
    timeoutMs: 30000,
    dryRun: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--raw-dog-json') { args.rawDogJson = next; i += 1; continue; }
    if (key === '--tokens-file') { args.tokensFile = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--limit') { args.limit = Number(next); i += 1; continue; }
    if (key === '--pre-sec') { args.preSec = Number(next); i += 1; continue; }
    if (key === '--post-sec') { args.postSec = Number(next); i += 1; continue; }
    if (key === '--sleep-ms') { args.sleepMs = Number(next); i += 1; continue; }
    if (key === '--resolution') { args.resolution = next; i += 1; continue; }
    if (key === '--gmgn-cli') { args.gmgnCli = next; i += 1; continue; }
    if (key === '--timeout-ms') { args.timeoutMs = Number(next); i += 1; continue; }
    if (key === '--dry-run') { args.dryRun = true; continue; }
    if (key === '--help' || key === '-h') {
      args.help = true;
      continue;
    }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return `Usage:
  node scripts/run-gmgn-dog-touch-audit.js --raw-dog-json <raw-dog-discovery.json> [--out out.json]
  node scripts/run-gmgn-dog-touch-audit.js --tokens-file <token|signal_ts lines> [--out out.json]

Options:
  --limit <n>          Max anchors to probe, default 64
  --pre-sec <n>        Seconds before signal_ts, default 60
  --post-sec <n>       Seconds after signal_ts, default 7200
  --sleep-ms <n>       Delay between GMGN calls, default 4000
  --resolution <res>   GMGN kline resolution, default 1m
  --dry-run            Build anchor list and output planned probes without calling GMGN

Requires GMGN_API_KEY in the environment or gmgn-cli config. Does not print secrets.`;
}

function isGoldSilver(row = {}) {
  return ['gold', 'silver'].includes(String(row.raw_sustained_tier || row.raw_primary_tier || '').toLowerCase());
}

function normalizeAnchor(row = {}) {
  const tokenCa = String(row.token_ca || row.token || row.address || '').trim();
  const signalTs = Number(row.signal_ts ?? row.timestamp_sec ?? row.ts);
  if (!tokenCa || !Number.isFinite(signalTs) || signalTs <= 0) return null;
  return {
    token_ca: tokenCa,
    signal_ts: Math.floor(signalTs),
    signal_id: row.signal_id ?? null,
    symbol: row.symbol || null,
    signal_iso: row.signal_iso || new Date(Math.floor(signalTs) * 1000).toISOString(),
    tier: row.raw_sustained_tier || row.raw_primary_tier || null,
    provider: row.provider || row.path_provider || null,
    source_kind: row.source_kind || row.path_source_kind || null,
    coverage_reason: row.coverage_reason || null,
  };
}

function uniqueAnchors(rows = []) {
  const byKey = new Map();
  for (const row of rows) {
    const anchor = normalizeAnchor(row);
    if (!anchor) continue;
    const key = `${anchor.token_ca}:${anchor.signal_ts}`;
    if (!byKey.has(key)) byKey.set(key, anchor);
  }
  return [...byKey.values()].sort((a, b) => a.signal_ts - b.signal_ts || a.token_ca.localeCompare(b.token_ca));
}

export function loadAnchorsFromRawDogJson(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  const report = parsed.report || parsed;
  const candidates = [
    ...(Array.isArray(report.top_raw_dogs) ? report.top_raw_dogs : []),
    ...(Array.isArray(report.missed_raw_dogs) ? report.missed_raw_dogs : []),
    ...(Array.isArray(report.outcomes) ? report.outcomes.filter(isGoldSilver) : []),
  ];
  return uniqueAnchors(candidates.filter(isGoldSilver));
}

export function loadAnchorsFromTokensFile(filePath) {
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  const rows = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const [token, ts, symbol = null] = trimmed.split(/[|,\t]/).map((part) => part.trim());
    rows.push({ token_ca: token, signal_ts: Number(ts), symbol });
  }
  return uniqueAnchors(rows);
}

function normalizeGmgnList(raw) {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.list)) return raw.list;
  if (Array.isArray(raw?.data?.list)) return raw.data.list;
  if (Array.isArray(raw?.data)) return raw.data;
  return [];
}

function normalizeGmgnBar(item) {
  if (Array.isArray(item)) {
    const rawTime = Number(item[0]);
    return {
      time_ms: rawTime > 1e10 ? rawTime : rawTime * 1000,
      open: Number(item[1]),
      high: Number(item[2]),
      low: Number(item[3]),
      close: Number(item[4]),
      volume_usd: Number(item[5] || 0),
      amount: Number(item[6] || 0),
    };
  }
  const rawTime = Number(item?.time || item?.timestamp || 0);
  return {
    time_ms: rawTime > 1e10 ? rawTime : rawTime * 1000,
    open: Number(item?.open),
    high: Number(item?.high),
    low: Number(item?.low),
    close: Number(item?.close),
    volume_usd: Number(item?.volume || 0),
    amount: Number(item?.amount || 0),
  };
}

export function summarizeGmgnRaw(raw, { signalTs, preSec = 60, postSec = 7200 } = {}) {
  const startTs = Math.floor(Number(signalTs) - Number(preSec || 0));
  const endTs = Math.floor(Number(signalTs) + Number(postSec || 0));
  const bars = normalizeGmgnList(raw)
    .map(normalizeGmgnBar)
    .filter((bar) => (
      Number.isFinite(bar.time_ms)
      && Number.isFinite(bar.close)
      && bar.time_ms > 0
    ))
    .map((bar) => ({ ...bar, ts: Math.floor(bar.time_ms / 1000) }))
    .filter((bar) => bar.ts >= startTs && bar.ts <= endTs)
    .sort((a, b) => a.ts - b.ts);
  const early15End = Math.floor(Number(signalTs) + 900);
  const early15 = bars.filter((bar) => bar.ts >= Number(signalTs) && bar.ts <= early15End);
  const nonzero = bars.filter((bar) => Number(bar.volume_usd || 0) > 0);
  const earlyNonzero = early15.filter((bar) => Number(bar.volume_usd || 0) > 0);
  const first = bars[0] || null;
  const firstNonzero = nonzero[0] || null;
  return {
    bars: bars.length,
    nonzero_volume_bars: nonzero.length,
    early_15m_bars: early15.length,
    early_15m_nonzero_volume_bars: earlyNonzero.length,
    volume_usd_sum: Number(nonzero.reduce((sum, bar) => sum + Number(bar.volume_usd || 0), 0).toFixed(6)),
    amount_sum: Number(bars.reduce((sum, bar) => sum + Number(bar.amount || 0), 0).toFixed(6)),
    first_bar_lag_sec: first ? first.ts - Number(signalTs) : null,
    first_nonzero_volume_lag_sec: firstNonzero ? firstNonzero.ts - Number(signalTs) : null,
    first_bar_ts: first?.ts ?? null,
    first_nonzero_volume_ts: firstNonzero?.ts ?? null,
    price_min: bars.length ? Math.min(...bars.map((bar) => bar.low).filter(Number.isFinite)) : null,
    price_max: bars.length ? Math.max(...bars.map((bar) => bar.high).filter(Number.isFinite)) : null,
    sample_bars: bars.slice(0, 3).map((bar) => ({
      ts: bar.ts,
      close: bar.close,
      volume_usd: bar.volume_usd,
      amount: bar.amount,
    })),
  };
}

async function sleep(ms) {
  if (!ms || ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function runGmgnKline({ gmgnCli, tokenCa, fromTs, toTs, resolution, timeoutMs }) {
  const { stdout } = await execFileAsync(gmgnCli, [
    'market',
    'kline',
    '--chain',
    'sol',
    '--address',
    tokenCa,
    '--resolution',
    resolution,
    '--from',
    String(fromTs),
    '--to',
    String(toTs),
    '--raw',
  ], {
    env: process.env,
    timeout: timeoutMs,
    maxBuffer: 4 * 1024 * 1024,
  });
  return JSON.parse(String(stdout || '{}'));
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.rawDogJson && !args.tokensFile) {
    throw new Error('Provide --raw-dog-json or --tokens-file');
  }
  const anchors = (args.tokensFile
    ? loadAnchorsFromTokensFile(args.tokensFile)
    : loadAnchorsFromRawDogJson(args.rawDogJson)
  ).slice(0, Math.max(1, Number(args.limit || 64)));

  const out = {
    schema_version: 'gmgn_dog_touch_audit.v1',
    generated_at: new Date().toISOString(),
    raw_dog_json: args.rawDogJson || null,
    tokens_file: args.tokensFile || null,
    dry_run: Boolean(args.dryRun),
    resolution: args.resolution,
    pre_sec: args.preSec,
    post_sec: args.postSec,
    anchors_n: anchors.length,
    results: [],
  };

  for (const [index, anchor] of anchors.entries()) {
    const fromTs = anchor.signal_ts - args.preSec;
    const toTs = anchor.signal_ts + args.postSec;
    const row = {
      index,
      token_ca: anchor.token_ca,
      token_tail: anchor.token_ca.slice(-8),
      signal_ts: anchor.signal_ts,
      signal_iso: anchor.signal_iso,
      symbol: anchor.symbol,
      tier: anchor.tier,
      existing_provider: anchor.provider,
      existing_source_kind: anchor.source_kind,
      existing_coverage_reason: anchor.coverage_reason,
      from_ts: fromTs,
      to_ts: toTs,
    };
    if (args.dryRun) {
      out.results.push({ ...row, status: 'planned' });
      continue;
    }
    try {
      const raw = await runGmgnKline({
        gmgnCli: args.gmgnCli,
        tokenCa: anchor.token_ca,
        fromTs,
        toTs,
        resolution: args.resolution,
        timeoutMs: args.timeoutMs,
      });
      out.results.push({
        ...row,
        status: 'ok',
        ...summarizeGmgnRaw(raw, {
          signalTs: anchor.signal_ts,
          preSec: args.preSec,
          postSec: args.postSec,
        }),
      });
    } catch (error) {
      out.results.push({
        ...row,
        status: 'error',
        error: String(error?.message || error || 'gmgn_touch_failed').replace(/GMGN_API_KEY=[^\s]+/g, 'GMGN_API_KEY=<redacted>').slice(0, 500),
      });
    }
    await sleep(args.sleepMs);
  }

  const ok = out.results.filter((row) => row.status === 'ok');
  const withBars = ok.filter((row) => Number(row.bars || 0) > 0);
  const withNonzero = ok.filter((row) => Number(row.nonzero_volume_bars || 0) > 0);
  const withEarlyNonzero = ok.filter((row) => Number(row.early_15m_nonzero_volume_bars || 0) > 0);
  out.summary = {
    ok_n: ok.length,
    error_n: out.results.filter((row) => row.status === 'error').length,
    bars_available_n: withBars.length,
    nonzero_volume_available_n: withNonzero.length,
    early_15m_nonzero_volume_available_n: withEarlyNonzero.length,
    nonzero_volume_available_pct: ok.length ? Number((withNonzero.length / ok.length * 100).toFixed(2)) : null,
    early_15m_nonzero_volume_available_pct: ok.length ? Number((withEarlyNonzero.length / ok.length * 100).toFixed(2)) : null,
    verdict: ok.length === 0
      ? 'no_successful_gmgn_responses'
      : (withNonzero.length / ok.length >= 1 / 3
        ? 'gmgn_reachable_fix_wiring_or_priority'
        : (withNonzero.length / ok.length < 1 / 5
          ? 'gmgn_insufficient_consider_onchain_bonding_curve'
          : 'gmgn_partial_coverage_needs_targeted_routing')),
  };

  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, JSON.stringify(out, null, 2));
  console.log(JSON.stringify({
    out: args.out,
    anchors_n: out.anchors_n,
    summary: out.summary,
  }, null, 2));
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
