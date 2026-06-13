#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    out: '',
    dedupe: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--dedupe') { args.dedupe = true; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-v10-curve-feature-worklist.js --dogs rebuilt-clean-dogs.json --duds rebuilt-clean-duds.json --out worklist.txt',
    '',
    'Writes token_ca|signal_ts|label rows for signal-anchor pump.fun curve feature decoding.',
    'By default it preserves signal-level rows. Use --dedupe only for RPC smoke tests.',
  ].join('\n');
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function normalizeTs(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function rowKey(row) {
  return `${row.token_ca}|${normalizeTs(row.signal_ts)}`;
}

function toWorklistRows(rows, label, { dedupe = false } = {}) {
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const token = String(row.token_ca || '').trim();
    const ts = normalizeTs(row.signal_ts);
    if (!token || ts == null) continue;
    const key = dedupe ? token : rowKey(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      token_ca: token,
      signal_ts: ts,
      label,
      return_domain: row.return_domain || 'unknown',
      effective_tier: row.effective_tier || row.tier || 'unknown',
    });
  }
  return out;
}

function main() {
  const args = parseArgs();
  if (args.help || !args.dogs || !args.duds || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const dogs = readJson(args.dogs);
  const duds = readJson(args.duds);
  const rows = [
    ...toWorklistRows(dogs, 'dog', args),
    ...toWorklistRows(duds, 'dud', args),
  ].sort((a, b) => a.signal_ts - b.signal_ts || a.token_ca.localeCompare(b.token_ca));
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, rows.map((row) => `${row.token_ca}|${row.signal_ts}|${row.label}`).join('\n') + '\n');
  const manifest = {
    schema_version: 'v10_curve_feature_worklist.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      dogs: args.dogs,
      duds: args.duds,
      dedupe: args.dedupe,
    },
    outputs: {
      out: args.out,
    },
    rows: rows.length,
    dogs: rows.filter((row) => row.label === 'dog').length,
    duds: rows.filter((row) => row.label === 'dud').length,
    unique_tokens: new Set(rows.map((row) => row.token_ca)).size,
  };
  fs.writeFileSync(`${args.out}.manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(JSON.stringify(manifest, null, 2));
}

main();
