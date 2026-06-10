import assert from 'node:assert/strict';
import net from 'node:net';
import { once } from 'node:events';
import { test } from 'node:test';

async function getAvailablePort() {
  const probe = net.createServer();
  await new Promise((resolve, reject) => {
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', resolve);
  });
  const { port } = probe.address();
  await new Promise((resolve, reject) => probe.close((error) => (error ? reject(error) : resolve())));
  return port;
}

test('raw dog discovery API fails fast with worker status while refresh is running', async () => {
  const port = await getAvailablePort();
  process.env.PORT = String(port);
  process.env.DASHBOARD_TOKEN = 'unit-raw-dog-api-token';
  process.env.RAW_DOG_DISCOVERY_OBSERVER_ENABLED = '0';

  const { startDashboardServer } = await import(`../src/web/dashboard-server.js?worker-status-test=${Date.now()}`);

  global.__rawDogDiscoveryWorkerStatus = {
    running: true,
    pid: 12345,
    started_at: '2026-06-10T00:00:00.000Z',
    last_completed_at: '2026-06-09T23:55:00.000Z',
    next_run_at: '2026-06-10T00:05:00.000Z',
    last_summary: {
      summary: {
        total_signals: 42,
        raw_kline_coverage_pct: 12.5,
      },
    },
  };

  const server = startDashboardServer();
  if (!server.listening) await once(server, 'listening');

  try {
    const response = await fetch(
      `http://127.0.0.1:${port}/api/paper/raw-dog-discovery?window=24h&limit=20000&token=unit-raw-dog-api-token`,
    );
    const payload = await response.json();

    assert.equal(response.status, 202);
    assert.equal(payload.materialized, true);
    assert.equal(payload.live_query, false);
    assert.equal(payload.refresh_in_progress, true);
    assert.equal(payload.source, 'raw_dog_discovery_worker_status');
    assert.equal(payload.worker.running, true);
    assert.equal(payload.worker.pid, 12345);
    assert.equal(payload.summary.total_signals, 42);
  } finally {
    delete global.__rawDogDiscoveryWorkerStatus;
    await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  }
});
