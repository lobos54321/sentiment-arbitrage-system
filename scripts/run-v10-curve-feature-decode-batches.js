#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { spawnSync } from 'child_process';

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    worklist: '',
    outDir: '',
    rpcUrlFile: '',
    batchSize: 5,
    batchTimeoutMs: 300000,
    startBatch: 0,
    maxBatches: 0,
    preSec: 900,
    postSec: 0,
    maxPages: 5,
    pageSize: 100,
    perTokenTimeoutMs: 60000,
    rpcTxDelayMs: 50,
    progressEvery: 1,
    continueOnRateLimit: true,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--worklist') { args.worklist = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--rpc-url-file') { args.rpcUrlFile = next; i += 1; continue; }
    if (key === '--batch-size') { args.batchSize = Number(next); i += 1; continue; }
    if (key === '--batch-timeout-ms') { args.batchTimeoutMs = Number(next); i += 1; continue; }
    if (key === '--start-batch') { args.startBatch = Number(next); i += 1; continue; }
    if (key === '--max-batches') { args.maxBatches = Number(next); i += 1; continue; }
    if (key === '--pre-sec') { args.preSec = Number(next); i += 1; continue; }
    if (key === '--post-sec') { args.postSec = Number(next); i += 1; continue; }
    if (key === '--max-pages') { args.maxPages = Number(next); i += 1; continue; }
    if (key === '--page-size') { args.pageSize = Number(next); i += 1; continue; }
    if (key === '--per-token-timeout-ms') { args.perTokenTimeoutMs = Number(next); i += 1; continue; }
    if (key === '--rpc-tx-delay-ms') { args.rpcTxDelayMs = Number(next); i += 1; continue; }
    if (key === '--progress-every') { args.progressEvery = Number(next); i += 1; continue; }
    if (key === '--stop-on-rate-limit') { args.continueOnRateLimit = false; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/run-v10-curve-feature-decode-batches.js --worklist sample.txt --out-dir ./decode-batches --rpc-url-file ~/.alchemy_rpc',
    '',
    'Runs pump.fun curve feature decoding in small process-isolated batches.',
    'Each batch has a hard wall-clock timeout; row-level checkpoint JSONL remains durable.',
    '',
    'Common options:',
    '  --batch-size <n>              Rows per decoder process, default 5',
    '  --batch-timeout-ms <n>        Hard timeout per batch process, default 300000',
    '  --max-batches <n>             Stop after n batches, default 0 (all)',
    '  --start-batch <n>             Zero-based batch index to start from, default 0',
    '  --pre-sec <n>                 Decode window before signal, default 900',
    '  --post-sec <n>                Decode window after signal, default 0',
    '  --max-pages <n>               Signature page cap per row, default 5',
    '  --page-size <n>               Signature page size, default 100',
  ].join('\n');
}

function readRows(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'));
}

function writeBatchFile(outDir, index, rows) {
  const filePath = path.join(outDir, 'batches', `batch-${String(index).padStart(4, '0')}.txt`);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, rows.join('\n') + '\n');
  return filePath;
}

function main() {
  const args = parseArgs();
  if (args.help || !args.worklist || !args.outDir) {
    console.log(usage());
    process.exit(args.help ? 0 : 1);
  }
  const rows = readRows(args.worklist);
  const batchSize = Math.max(1, Math.floor(Number(args.batchSize || 1)));
  const totalBatches = Math.ceil(rows.length / batchSize);
  const startBatch = Math.max(0, Math.floor(Number(args.startBatch || 0)));
  const maxBatches = Math.max(0, Math.floor(Number(args.maxBatches || 0)));
  fs.mkdirSync(args.outDir, { recursive: true });

  const batchResults = [];
  const startedAt = new Date().toISOString();
  let processed = 0;
  for (let batchIndex = startBatch; batchIndex < totalBatches; batchIndex += 1) {
    if (maxBatches > 0 && processed >= maxBatches) break;
    const batchRows = rows.slice(batchIndex * batchSize, (batchIndex + 1) * batchSize);
    if (!batchRows.length) continue;
    const batchFile = writeBatchFile(args.outDir, batchIndex, batchRows);
    const out = path.join(args.outDir, `decode-batch-${String(batchIndex).padStart(4, '0')}.json`);
    const checkpoint = `${out}.jsonl`;
    const childArgs = [
      'scripts/run-helius-pumpfun-curve-decode-audit.js',
      '--tokens-file', batchFile,
      '--out', out,
      '--checkpoint-out', checkpoint,
      '--pre-sec', String(args.preSec),
      '--post-sec', String(args.postSec),
      '--max-pages', String(args.maxPages),
      '--page-size', String(args.pageSize),
      '--per-token-timeout-ms', String(args.perTokenTimeoutMs),
      '--rpc-tx-delay-ms', String(args.rpcTxDelayMs),
      '--rpc-mode', 'raw',
      '--resume',
      '--progress-every', String(args.progressEvery),
    ];
    if (args.rpcUrlFile) childArgs.push('--rpc-url-file', args.rpcUrlFile);
    if (args.continueOnRateLimit) childArgs.push('--continue-on-rate-limit');

    const t0 = Date.now();
    const result = spawnSync(process.execPath, childArgs, {
      cwd: process.cwd(),
      stdio: 'inherit',
      timeout: Number(args.batchTimeoutMs || 0) || undefined,
    });
    const durationMs = Date.now() - t0;
    const timedOut = Boolean(result.error && result.error.code === 'ETIMEDOUT');
    const row = {
      batch_index: batchIndex,
      rows: batchRows.length,
      batch_file: batchFile,
      out,
      checkpoint,
      status: timedOut ? 'timeout' : result.status === 0 ? 'ok' : 'error',
      exit_status: result.status,
      signal: result.signal || null,
      error: result.error ? String(result.error.message || result.error) : null,
      duration_ms: durationMs,
    };
    batchResults.push(row);
    processed += 1;
    fs.writeFileSync(path.join(args.outDir, 'batch-run-manifest.json'), `${JSON.stringify({
      schema_version: 'v10_curve_feature_decode_batch_run.v1',
      started_at: startedAt,
      updated_at: new Date().toISOString(),
      inputs: args,
      total_rows: rows.length,
      total_batches: totalBatches,
      processed_batches: processed,
      batch_results: batchResults,
    }, null, 2)}\n`);
    if (timedOut) {
      console.error(`[batch ${batchIndex}] timed out after ${durationMs}ms; checkpoint preserved at ${checkpoint}`);
    }
  }

  const manifestPath = path.join(args.outDir, 'batch-run-manifest.json');
  const finalManifest = {
    schema_version: 'v10_curve_feature_decode_batch_run.v1',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    inputs: args,
    total_rows: rows.length,
    total_batches: totalBatches,
    processed_batches: processed,
    batch_results: batchResults,
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(finalManifest, null, 2)}\n`);
  console.log(JSON.stringify({
    manifest: manifestPath,
    total_rows: rows.length,
    total_batches: totalBatches,
    processed_batches: processed,
    ok_batches: batchResults.filter((row) => row.status === 'ok').length,
    timeout_batches: batchResults.filter((row) => row.status === 'timeout').length,
    error_batches: batchResults.filter((row) => row.status === 'error').length,
  }, null, 2));
}

main();
