#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    labelAudit: '',
    outDir: './data/audits/clean-rawdog-pack',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--label-audit') { args.labelAudit = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-clean-rawdog-pack.js --label-audit label-cleaning.json --out-dir out',
  ].join('\n');
}

function isGoldSilver(row = {}) {
  return ['gold', 'silver'].includes(String(row.tier || row.raw_sustained_tier || row.raw_primary_tier || '').toLowerCase());
}

function tokenLine(row = {}) {
  return `${row.token_ca}|${Math.floor(Number(row.signal_ts || 0))}|${row.symbol || row.tier || ''}`;
}

function dedupeByTokenSignal(rows = []) {
  const out = new Map();
  for (const row of rows) {
    const token = String(row.token_ca || '').trim();
    const ts = Math.floor(Number(row.signal_ts || 0));
    if (!token || !ts) continue;
    const key = `${token}:${ts}`;
    if (!out.has(key)) out.set(key, row);
  }
  return [...out.values()].sort((a, b) => Number(a.signal_ts) - Number(b.signal_ts) || String(a.token_ca).localeCompare(String(b.token_ca)));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeTokenFile(filePath, rows) {
  fs.writeFileSync(filePath, `${rows.map(tokenLine).join('\n')}${rows.length ? '\n' : ''}`);
}

export function buildPack(labelAudit = {}) {
  const rows = Array.isArray(labelAudit.rows) ? labelAudit.rows : [];
  const cleanRows = rows.filter((row) => row.label_status === 'clean');
  const quarantineRows = rows.filter((row) => row.label_status !== 'clean');
  const noBarsRows = quarantineRows.filter((row) => row.label_cleaning_reason === 'no_native_bars');
  const pollutedRows = quarantineRows.filter((row) => row.label_cleaning_reason === 'label_unit_corrupt');
  const cleanDogs = dedupeByTokenSignal(cleanRows.filter(isGoldSilver));
  const cleanDuds = dedupeByTokenSignal(cleanRows.filter((row) => !isGoldSilver(row)));
  const quarantine = dedupeByTokenSignal(quarantineRows);
  return {
    schema_version: 'clean_rawdog_pack.v1',
    generated_at: new Date().toISOString(),
    source_label_audit_schema: labelAudit.schema_version || null,
    summary: {
      input_rows_n: rows.length,
      clean_rows_n: cleanRows.length,
      quarantine_rows_n: quarantineRows.length,
      clean_dog_unique_n: cleanDogs.length,
      clean_dud_unique_n: cleanDuds.length,
      quarantine_unique_n: quarantine.length,
      polluted_rows_n: pollutedRows.length,
      no_bars_rows_n: noBarsRows.length,
    },
    clean_dogs: cleanDogs,
    clean_duds: cleanDuds,
    quarantine_rows: quarantine,
    polluted_rows: pollutedRows,
    no_bars_rows: noBarsRows,
  };
}

function writePack(pack, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  writeJson(path.join(outDir, 'clean-rawdog-pack.json'), {
    schema_version: pack.schema_version,
    generated_at: pack.generated_at,
    source_label_audit_schema: pack.source_label_audit_schema,
    summary: pack.summary,
  });
  writeJson(path.join(outDir, 'clean-dogs.json'), pack.clean_dogs);
  writeJson(path.join(outDir, 'clean-duds.json'), pack.clean_duds);
  writeJson(path.join(outDir, 'quarantine-rows.json'), pack.quarantine_rows);
  writeJson(path.join(outDir, 'polluted-rows.json'), pack.polluted_rows);
  writeJson(path.join(outDir, 'no-bars-rows.json'), pack.no_bars_rows);
  writeTokenFile(path.join(outDir, 'clean-dog-tokens.txt'), pack.clean_dogs);
  writeTokenFile(path.join(outDir, 'clean-dud-tokens.txt'), pack.clean_duds);
  writeTokenFile(path.join(outDir, 'quarantine-tokens.txt'), pack.quarantine_rows);
  writeTokenFile(path.join(outDir, 'polluted-tokens.txt'), pack.polluted_rows);
  const manifest = {
    schema_version: 'clean_rawdog_pack_manifest.v1',
    generated_at: pack.generated_at,
    files: [
      'clean-rawdog-pack.json',
      'clean-dogs.json',
      'clean-duds.json',
      'quarantine-rows.json',
      'polluted-rows.json',
      'no-bars-rows.json',
      'clean-dog-tokens.txt',
      'clean-dud-tokens.txt',
      'quarantine-tokens.txt',
      'polluted-tokens.txt',
    ],
    summary: pack.summary,
  };
  writeJson(path.join(outDir, 'manifest.json'), manifest);
  return manifest;
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.labelAudit) throw new Error('Provide --label-audit');
  const labelAudit = JSON.parse(fs.readFileSync(args.labelAudit, 'utf8'));
  const pack = buildPack(labelAudit);
  const manifest = writePack(pack, args.outDir);
  console.log(JSON.stringify({ out_dir: args.outDir, summary: manifest.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

