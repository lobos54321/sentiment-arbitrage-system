#!/usr/bin/env node
import ParityExecutor from '../src/execution/parity-executor.js';
import { evaluatePaperLiveManagedPosition } from '../src/execution/paper-live-position-monitor.js';

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

async function main() {
  redirectConsoleToStderr();

  const command = process.argv[2];
  if (!command) {
    throw new Error('missing command');
  }

  const raw = await readStdin();
  const payload = raw ? JSON.parse(raw) : {};
  const executor = new ParityExecutor({ mode: payload.mode || 'paper' }).initialize();

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
    default:
      throw new Error(`unsupported command: ${command}`);
  }

  process.stdout.write(JSON.stringify(result));
}

main().catch((error) => {
  process.stderr.write(JSON.stringify({ success: false, failureReason: error.message || 'bridge_failed' }));
  process.exit(1);
});
