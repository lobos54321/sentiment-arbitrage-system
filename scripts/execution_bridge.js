#!/usr/bin/env node
// Canonical paper bridge path: paper_trade_monitor.py -> execution_bridge.js -> paper-live-position-monitor.js
import ParityExecutor from '../src/execution/parity-executor.js';
import { evaluatePaperLiveManagedPosition } from '../src/execution/paper-live-position-monitor.js';
import { SharedPoolOhlcvClient } from '../src/market-data/shared-pool-ohclv-client.js';
import { SharedQuoteClient } from '../src/market-data/shared-quote-client.js';
import { SharedMarketRuntime, applyMarketDataProcessOverride } from '../src/market-data/shared-market-runtime.js';

function redirectConsoleToStderr() {
  const write = (args) => {
    try {
      process.stderr.write(`${args.map((item) => {
        if (typeof item === 'string') return item;
        try {
          return JSON.stringify(item);
        } catch {
          return String(item);
        }
      }).join(' ')}\n`);
    } catch {
      // noop
    }
  };

  console.log = (...args) => write(args);
  console.info = (...args) => write(args);
  console.warn = (...args) => write(args);
  console.error = (...args) => write(args);
}

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
  redirectConsoleToStderr();

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

  let result;
  switch (command) {
    case 'quote-buy':
      result = await executor.quoteBuy(payload.tokenCA, payload.amountSol, payload.options || {});
      break;
    case 'quote-sell':
      result = await executor.quoteSell(payload.tokenCA, payload.tokenAmountRaw, payload.options || {});
      break;
    case 'simulate-buy':
      result = await executor.simulateBuy(payload.tokenCA, payload.amountSol, payload.options || {});
      break;
    case 'simulate-sell':
      result = await executor.simulateSell(payload.tokenCA, payload.tokenAmountRaw, payload.options || {});
      break;
    case 'evaluate-paper-exit':
      result = await evaluatePaperLiveManagedPosition({ ...payload, executor });
      break;
    case 'shared-runtime': {
      const { method, payload: runtimePayload = {} } = payload;
      if (method === 'getCache') {
        result = await marketDataBridge.runtime.getCache(runtimePayload.key);
      } else if (method === 'setCache') {
        await marketDataBridge.runtime.setCache(runtimePayload.key, runtimePayload.value, runtimePayload.ttlMs || 0);
        result = true;
      } else if (method === 'getSharedCooldown') {
        result = await marketDataBridge.runtime.getSharedCooldown(runtimePayload.provider);
      } else if (method === 'getSwapQuote') {
        result = await marketDataBridge.quoteClient.getSwapQuote(runtimePayload, runtimePayload.options || {});
      } else if (method === 'getBestDexPair') {
        result = await marketDataBridge.quoteClient.getBestDexPair(runtimePayload.tokenCA, runtimePayload.options || {});
      } else if (method === 'resolvePool') {
        result = await marketDataBridge.poolClient.resolvePool(runtimePayload.tokenCa || runtimePayload.tokenCA, runtimePayload.options || {});
      } else if (method === 'fetchRecentOhlcvByPool') {
        result = await marketDataBridge.poolClient.fetchRecentOhlcvByPool(runtimePayload.tokenCa || runtimePayload.tokenCA, runtimePayload.poolAddress, runtimePayload.options || {});
      } else if (method === 'close') {
        result = true;
      } else {
        throw new Error(`unsupported shared-runtime method: ${method}`);
      }
      break;
    }
    default:
      throw new Error(`unsupported command: ${command}`);
  }

  process.stdout.write(JSON.stringify(result));
  await Promise.allSettled([
    marketDataBridge.runtime.close(),
    marketDataBridge.quoteClient.close(),
    marketDataBridge.poolClient.close()
  ]);
}

main().catch((error) => {
  process.stderr.write(JSON.stringify({ success: false, failureReason: error.message || 'bridge_failed' }));
  process.exit(1);
});
