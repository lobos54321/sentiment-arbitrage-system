#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import autonomyConfig from '../src/config/autonomy-config.js';

const statusPath = process.env.AUTONOMY_DAEMON_STATUS_PATH || path.join(autonomyConfig.dataDir, 'autonomy-daemon-status.json');
const lockPath = process.env.AUTONOMY_DAEMON_LOCK_PATH || path.join(autonomyConfig.dataDir, 'autonomy-daemon.lock');

function pidAlive(pid) {
  if (!pid || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

const status = fs.existsSync(statusPath) ? JSON.parse(fs.readFileSync(statusPath, 'utf8')) : null;
const lock = fs.existsSync(lockPath) ? JSON.parse(fs.readFileSync(lockPath, 'utf8')) : null;
const lockPidRunning = pidAlive(lock?.pid);
const statusPidRunning = pidAlive(status?.pid);
const staleStatus = Boolean(status && !statusPidRunning && !lockPidRunning);

console.log(JSON.stringify({
  hasStatus: Boolean(status),
  hasLock: Boolean(lock),
  pidRunning: lockPidRunning || statusPidRunning,
  staleStatus,
  daemon: status ? {
    pid: status.pid || null,
    state: status.state || null,
    currentStep: status.currentStep || null,
    queueDepth: status.queueDepth ?? null,
    processedEvents: status.processedEvents ?? null,
    consecutiveFailures: status.consecutiveFailures ?? null,
    lastSuccessAt: status.lastSuccessAt || null,
    nextRetryAt: status.nextRetryAt || null,
    pauseState: status.pauseState || null
  } : null,
  strategy: status ? {
    baseline: status.baseline || null,
    challenger: status.challenger || null,
    lastAdvancementAction: status.lastAdvancementAction || null,
    lastRejectionReason: status.lastRejectionReason || null
  } : null,
  latestRun: status?.latestRun || null,
  recentEvents: status?.recentEvents || [],
  rawStatus: status,
  lock
}, null, 2));
