#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    dogs: '',
    duds: '',
    out: '',
    dogsPerDomain: 50,
    dudsPerDomain: 50,
    totalDogs: 150,
    totalDuds: 150,
    seed: 42,
    dedupeTokens: true,
    domains: '',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--dogs') { args.dogs = next; i += 1; continue; }
    if (key === '--duds') { args.duds = next; i += 1; continue; }
    if (key === '--out') { args.out = next; i += 1; continue; }
    if (key === '--dogs-per-domain') { args.dogsPerDomain = Number(next); i += 1; continue; }
    if (key === '--duds-per-domain') { args.dudsPerDomain = Number(next); i += 1; continue; }
    if (key === '--total-dogs') { args.totalDogs = Number(next); i += 1; continue; }
    if (key === '--total-duds') { args.totalDuds = Number(next); i += 1; continue; }
    if (key === '--seed') { args.seed = Number(next); i += 1; continue; }
    if (key === '--domains') { args.domains = next; i += 1; continue; }
    if (key === '--keep-token-duplicates') { args.dedupeTokens = false; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/sample-v10-curve-feature-worklist.js --dogs rebuilt-clean-dogs.json --duds rebuilt-clean-duds.json --out sample.txt',
    '',
    'Builds a balanced, return-domain-stratified dog/dud worklist for curve-stage feature decoding.',
    'Default output is around 150 dogs + 150 duds, capped per return_domain, and token-deduped per label.',
    '',
    'Options:',
    '  --dogs-per-domain <n>       Dog cap per return_domain, default 50',
    '  --duds-per-domain <n>       Dud cap per return_domain, default 50',
    '  --total-dogs <n>            Overall dog cap, default 150',
    '  --total-duds <n>            Overall dud cap, default 150',
    '  --domains <csv>             Optional return_domain allow-list',
    '  --seed <n>                  Deterministic sample seed, default 42',
    '  --keep-token-duplicates     Preserve repeated token signals instead of deduping per label',
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

function mulberry32(seed) {
  let t = seed >>> 0;
  return function rand() {
    t += 0x6D2B79F5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffle(rows, rand) {
  const out = [...rows];
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function toRows(rows, label, { dedupeTokens, domainAllow }) {
  const out = [];
  const seenTokens = new Set();
  const seenSignals = new Set();
  for (const row of rows) {
    const token = String(row.token_ca || '').trim();
    const signalTs = normalizeTs(row.signal_ts);
    const returnDomain = String(row.return_domain || 'unknown');
    if (!token || signalTs == null) continue;
    if (domainAllow && !domainAllow.has(returnDomain)) continue;
    const signalKey = `${token}|${signalTs}|${label}`;
    if (seenSignals.has(signalKey)) continue;
    seenSignals.add(signalKey);
    if (dedupeTokens) {
      const tokenKey = `${token}|${label}`;
      if (seenTokens.has(tokenKey)) continue;
      seenTokens.add(tokenKey);
    }
    out.push({
      token_ca: token,
      signal_ts: signalTs,
      label,
      return_domain: returnDomain,
      effective_tier: row.effective_tier || row.tier || 'unknown',
      corrected_peak_pct: Number.isFinite(Number(row.corrected_peak_pct)) ? Number(row.corrected_peak_pct) : null,
    });
  }
  return out;
}

function groupBy(rows, key) {
  const out = new Map();
  for (const row of rows) {
    const value = row[key] || 'unknown';
    if (!out.has(value)) out.set(value, []);
    out.get(value).push(row);
  }
  return out;
}

function sampleByDomain(rows, perDomain, total, rand) {
  const groups = groupBy(rows, 'return_domain');
  const domains = [...groups.keys()].sort();
  const selected = [];
  const domainCounts = {};
  for (const domain of domains) {
    const sample = shuffle(groups.get(domain), rand).slice(0, perDomain);
    domainCounts[domain] = {
      available: groups.get(domain).length,
      selected: sample.length,
    };
    selected.push(...sample);
  }
  const trimmed = shuffle(selected, rand).slice(0, total);
  const selectedSet = new Set(trimmed.map((row) => `${row.token_ca}|${row.signal_ts}|${row.label}`));
  for (const domain of domains) {
    domainCounts[domain].selected_after_total_cap = groups.get(domain)
      .filter((row) => selectedSet.has(`${row.token_ca}|${row.signal_ts}|${row.label}`)).length;
  }
  return { rows: trimmed, domainCounts };
}

function interleaveByTime(dogs, duds) {
  const combined = [...dogs, ...duds].sort((a, b) => {
    const d = a.return_domain.localeCompare(b.return_domain);
    if (d !== 0) return d;
    if (a.signal_ts !== b.signal_ts) return a.signal_ts - b.signal_ts;
    if (a.label !== b.label) return a.label === 'dog' ? -1 : 1;
    return a.token_ca.localeCompare(b.token_ca);
  });
  return combined;
}

function main() {
  const args = parseArgs();
  if (args.help || !args.dogs || !args.duds || !args.out) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const domainAllow = args.domains
    ? new Set(String(args.domains).split(',').map((value) => value.trim()).filter(Boolean))
    : null;
  const rand = mulberry32(Number.isFinite(args.seed) ? args.seed : 42);
  const dogs = toRows(readJson(args.dogs), 'dog', {
    dedupeTokens: args.dedupeTokens,
    domainAllow,
  });
  const duds = toRows(readJson(args.duds), 'dud', {
    dedupeTokens: args.dedupeTokens,
    domainAllow,
  });
  const dogSample = sampleByDomain(dogs, args.dogsPerDomain, args.totalDogs, rand);
  const dudSample = sampleByDomain(duds, args.dudsPerDomain, args.totalDuds, rand);
  const rows = interleaveByTime(dogSample.rows, dudSample.rows);
  fs.mkdirSync(path.dirname(path.resolve(args.out)), { recursive: true });
  fs.writeFileSync(args.out, rows.map((row) => `${row.token_ca}|${row.signal_ts}|${row.label}`).join('\n') + '\n');
  const manifest = {
    schema_version: 'v10_curve_feature_stratified_worklist.v1',
    generated_at: new Date().toISOString(),
    inputs: {
      dogs: args.dogs,
      duds: args.duds,
      dedupe_tokens: args.dedupeTokens,
      domains: domainAllow ? [...domainAllow].sort() : null,
      seed: args.seed,
      dogs_per_domain: args.dogsPerDomain,
      duds_per_domain: args.dudsPerDomain,
      total_dogs: args.totalDogs,
      total_duds: args.totalDuds,
    },
    outputs: { out: args.out },
    rows: rows.length,
    dogs: rows.filter((row) => row.label === 'dog').length,
    duds: rows.filter((row) => row.label === 'dud').length,
    unique_tokens: new Set(rows.map((row) => row.token_ca)).size,
    dog_domain_counts: dogSample.domainCounts,
    dud_domain_counts: dudSample.domainCounts,
  };
  fs.writeFileSync(`${args.out}.manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(JSON.stringify(manifest, null, 2));
}

main();
