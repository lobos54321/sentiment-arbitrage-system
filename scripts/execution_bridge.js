#!/usr/bin/env node
// Canonical paper bridge path: paper_trade_monitor.py -> execution_bridge.js -> paper-live-position-monitor.js
// IMPORTANT: suppress_stdout.js MUST be imported first to redirect console before
// any other module-level code (e.g. SessionManager singleton) writes to stdout.
import './suppress_stdout.js';
import ParityExecutor from '../src/execution/parity-executor.js';
import { evaluatePaperLiveManagedPosition } from '../src/execution/paper-live-position-monitor.js';
import { SharedPoolOhlcvClient } from '../src/market-data/shared-pool-ohclv-client.js';
import { SharedQuoteClient } from '../src/market-data/shared-quote-client.js';
import { SharedMarketRuntime, applyMarketDataProcessOverride } from '../src/market-data/shared-market-runtime.js';
import http from 'http';

async function readStdin() {
  return await new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => resolve(data.trim()));
    process.stdin.on('error', reject);
  });
}

async function createMarketDataBridge() {
  const runtime = new SharedMarketRuntime({ namespace: 'paper-monitor:bridge' });
  const quoteClient = new SharedQuoteClient(undefined, { runtime: new SharedMarketRuntime({ namespace: 'market-data:quotes' }) });
  const poolClient = new SharedPoolOhlcvClient(undefined, { runtime: new SharedMarketRuntime({ namespace: 'market-data:pool-ohclv' }) });
  return { runtime, quoteClient, poolClient };
}

async function main() {

  const command = process.argv[2];
  if (!command) {
    throw new Error('missing command');
  }

  const raw = await readStdin();
  const payload = raw ? JSON.parse(raw) : {};
  const processFlag = payload.mode === 'paper' ? 'MARKET_DATA_UNIFIED_PAPER_MONITOR' : 'MARKET_DATA_UNIFIED_PREMIUM';
  applyMarketDataProcessOverride(processFlag);
  const executor = new ParityExecutor({ mode: payload.mode || 'paper' }).initialize();
  const marketDataBridge = await createMarketDataBridge();

  if (command === 'daemon') {
    return await runDaemon(executor, marketDataBridge);
  }

  let result;
  result = await processCommand(command, payload, executor, marketDataBridge);

  const json = JSON.stringify(result ?? { success: false, failureReason: 'bridge_undefined_result' });
  process.stdout.write(json + '\n');
  await Promise.allSettled([
    marketDataBridge.runtime.close(),
    marketDataBridge.quoteClient.close(),
    marketDataBridge.poolClient.close()
  ]);
}

async function processCommand(command, payload, executor, marketDataBridge) {
  switch (command) {
    case 'quote-buy':
      return await executor.quoteBuy(payload.tokenCA, payload.amountSol, payload.options || {});
    case 'quote-sell':
      return await executor.quoteSell(payload.tokenCA, payload.tokenAmountRaw, payload.options || {});
    case 'simulate-buy':
      return await executor.simulateBuy(payload.tokenCA, payload.amountSol, payload.options || {});
    case 'simulate-sell':
      return await executor.simulateSell(payload.tokenCA, payload.tokenAmountRaw, payload.options || {});
    case 'evaluate-paper-exit':
      return await evaluatePaperLiveManagedPosition({ ...payload, executor });
    case 'shared-runtime': {
      const { method, payload: runtimePayload = {} } = payload;
      if (method === 'getCache') {
        return await marketDataBridge.runtime.getCache(runtimePayload.key);
      } else if (method === 'setCache') {
        await marketDataBridge.runtime.setCache(runtimePayload.key, runtimePayload.value, runtimePayload.ttlMs || 0);
        return true;
      } else if (method === 'getSharedCooldown') {
        return await marketDataBridge.runtime.getSharedCooldown(runtimePayload.provider);
      } else if (method === 'getSwapQuote') {
        return await marketDataBridge.quoteClient.getSwapQuote(runtimePayload, runtimePayload.options || {});
      } else if (method === 'getBestDexPair') {
        return await marketDataBridge.quoteClient.getBestDexPair(runtimePayload.tokenCA, runtimePayload.options || {});
      } else if (method === 'resolvePool') {
        return await marketDataBridge.poolClient.resolvePool(runtimePayload.tokenCa || runtimePayload.tokenCA, runtimePayload.options || {});
      } else if (method === 'fetchRecentOhlcvByPool') {
        return await marketDataBridge.poolClient.fetchRecentOhlcvByPool(runtimePayload.tokenCa || runtimePayload.tokenCA, runtimePayload.poolAddress, runtimePayload.options || {});
      } else if (method === 'close') {
        return true;
      } else {
        throw new Error(`unsupported shared-runtime method: ${method}`);
      }
    }
    default:
      throw new Error(`unsupported command: ${command}`);
  }
}

async function runDaemon(executor, marketDataBridge) {
  const server = http.createServer((req, res) => {
    if (req.method !== 'POST') {
      res.writeHead(405);
      res.end();
      return;
    }
    
    let body = '';
    req.on('data', chunk => body += chunk.toString());
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body);
        const command = payload._command;
        const cmdPayload = payload.payload || {};
        const result = await processCommand(command, cmdPayload, executor, marketDataBridge);
        const resJson = JSON.stringify(result ?? { success: false, failureReason: 'bridge_undefined_result' });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(resJson);
      } catch (err) {
        const resJson = JSON.stringify({ success: false, failureReason: err.message || 'daemon_eval_failed' });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(resJson);
      }
    });
  });

  server.listen(38942, '127.0.0.1', () => {
    // Signal ready to Python parent
    process.stdout.write(JSON.stringify({ status: 'daemon_ready' }) + '\n');
  });
}

main().catch((error) => {
  const msg = JSON.stringify({ success: false, failureReason: error.message || 'bridge_failed' });
  process.stderr.write(msg);
  process.exit(1);
});
