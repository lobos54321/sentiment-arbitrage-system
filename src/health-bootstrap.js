/**
 * Lightweight cloud bootstrap.
 *
 * Keep the dashboard/health server in the parent process and run the heavier
 * trading runtime as a supervised child. This preserves Zeabur's public health
 * endpoint when the runtime worker exits, restarts, or hits a transient volume
 * issue.
 */

import fs from 'fs';
import { dirname } from 'path';
import { spawn } from 'child_process';
import { startDashboardServer } from './web/dashboard-server.js';

const falsey = new Set(['0', 'false', 'no', 'off']);
const embeddedDashboardEnabled = !falsey.has(
  String(process.env.EMBEDDED_DASHBOARD_ENABLED ?? 'true').trim().toLowerCase(),
);
const childRuntimeEnabled = !falsey.has(
  String(process.env.HEALTH_BOOTSTRAP_CHILD_RUNTIME_ENABLED ?? 'true').trim().toLowerCase(),
);
const restartDelayMs = Math.max(
  1000,
  Number(process.env.HEALTH_BOOTSTRAP_RUNTIME_RESTART_MS || 15000) || 15000,
);
const runtimeLogPath = process.env.NODE_RUNTIME_LOG_PATH || '/app/data/node.log';

let runtimeChild = null;
let shuttingDown = false;

function appendRuntimeOutput(chunk, stream = process.stdout) {
  const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk || '');
  try {
    stream.write(text);
  } catch {}
  try {
    fs.mkdirSync(dirname(runtimeLogPath), { recursive: true });
    fs.appendFileSync(runtimeLogPath, text);
  } catch {}
}

function runtimeMode() {
  return process.argv.includes('--premium') || process.env.PREMIUM_MODE_ENABLED === 'true'
    ? 'premium'
    : 'default';
}

function runtimeArgs() {
  const modeArgs = process.argv.slice(2);
  return [
    ...process.execArgv,
    'src/index.js',
    ...modeArgs,
  ];
}

function updateRuntimeStatus(patch = {}) {
  global.__runtimeWorkerStatus = {
    schema_version: 'health_bootstrap_runtime_worker.v1',
    mode: runtimeMode(),
    child_runtime_enabled: childRuntimeEnabled,
    pid: runtimeChild?.pid || null,
    running: Boolean(runtimeChild && runtimeChild.exitCode == null && runtimeChild.signalCode == null),
    updated_at: new Date().toISOString(),
    ...(global.__runtimeWorkerStatus || {}),
    ...patch,
  };
}

function startRuntimeChild() {
  if (shuttingDown) return;
  const args = runtimeArgs();
  updateRuntimeStatus({
    running: false,
    last_start_attempt_at: new Date().toISOString(),
    command: `${process.execPath} ${args.join(' ')}`,
  });
  appendRuntimeOutput(`[health-bootstrap] ${new Date().toISOString()} starting runtime child: ${args.join(' ')}\n`);
  runtimeChild = spawn(process.execPath, args, {
    cwd: process.cwd(),
    env: {
      ...process.env,
      EMBEDDED_DASHBOARD_ENABLED: 'false',
      HEALTH_BOOTSTRAP_CHILD: '1',
      DASHBOARD_RUNTIME_LOG_DIR: process.env.DASHBOARD_RUNTIME_LOG_DIR || '/app/data',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  updateRuntimeStatus({
    pid: runtimeChild.pid,
    running: true,
    started_at: new Date().toISOString(),
    last_error: null,
  });
  runtimeChild.stdout.on('data', (chunk) => appendRuntimeOutput(chunk, process.stdout));
  runtimeChild.stderr.on('data', (chunk) => appendRuntimeOutput(chunk, process.stderr));
  runtimeChild.on('error', (error) => {
    global.__startupError = {
      message: error?.message || String(error),
      stack: error?.stack || null,
      at: new Date().toISOString(),
      mode: runtimeMode(),
      component: 'runtime_child_spawn',
    };
    updateRuntimeStatus({
      running: false,
      last_error: error?.message || String(error),
      last_exit_at: new Date().toISOString(),
    });
    appendRuntimeOutput(`[health-bootstrap] runtime child spawn error: ${error?.message || error}\n`, process.stderr);
  });
  runtimeChild.on('exit', (code, signal) => {
    const exitedAt = new Date().toISOString();
    updateRuntimeStatus({
      running: false,
      pid: null,
      last_exit_at: exitedAt,
      last_exit_code: code,
      last_exit_signal: signal || null,
      restart_delay_ms: shuttingDown ? null : restartDelayMs,
    });
    appendRuntimeOutput(
      `[health-bootstrap] ${exitedAt} runtime child exited code=${code} signal=${signal || ''}${shuttingDown ? '' : `; restarting in ${restartDelayMs}ms`}\n`,
      code === 0 ? process.stdout : process.stderr,
    );
    runtimeChild = null;
    if (!shuttingDown) {
      setTimeout(startRuntimeChild, restartDelayMs);
    }
  });
}

function stopRuntimeAndExit(signal) {
  shuttingDown = true;
  updateRuntimeStatus({
    shutdown_signal: signal,
    shutdown_at: new Date().toISOString(),
  });
  if (runtimeChild && runtimeChild.exitCode == null && runtimeChild.signalCode == null) {
    try {
      runtimeChild.kill('SIGTERM');
    } catch {}
  }
  setTimeout(() => process.exit(0), 5000).unref();
}

process.env.DASHBOARD_RUNTIME_ROLE ||= childRuntimeEnabled
  ? 'dashboard_supervisor'
  : 'standalone_or_embedded_dashboard';
global.__dashboardStarted = true;
if (embeddedDashboardEnabled) {
  startDashboardServer();
} else {
  console.log('[health-bootstrap] embedded dashboard disabled; runtime will not bind PORT');
}

process.on('SIGTERM', () => stopRuntimeAndExit('SIGTERM'));
process.on('SIGINT', () => stopRuntimeAndExit('SIGINT'));

if (childRuntimeEnabled) {
  updateRuntimeStatus();
  startRuntimeChild();
} else {
  try {
    const runtime = await import('./index.js');
    await runtime.main();
  } catch (error) {
    global.__startupError = {
      message: error?.message || String(error),
      stack: error?.stack || null,
      at: new Date().toISOString(),
      mode: runtimeMode(),
    };
    console.error('❌ Runtime bootstrap failed:', error);
  }
}
