/**
 * Lightweight cloud bootstrap.
 *
 * Start the dashboard/health server before importing the heavier trading
 * runtime so platform health checks can succeed while runtime modules load.
 */

import { startDashboardServer } from './web/dashboard-server.js';

const embeddedDashboardEnabled = !['0', 'false', 'no', 'off'].includes(
  String(process.env.EMBEDDED_DASHBOARD_ENABLED ?? 'true').trim().toLowerCase(),
);

global.__dashboardStarted = true;
if (embeddedDashboardEnabled) {
  startDashboardServer();
} else {
  console.log('[health-bootstrap] embedded dashboard disabled; runtime will not bind PORT');
}

try {
  const runtime = await import('./index.js');
  await runtime.main();
} catch (error) {
  global.__startupError = {
    message: error?.message || String(error),
    stack: error?.stack || null,
    at: new Date().toISOString(),
    mode: process.argv.includes('--premium') || process.env.PREMIUM_MODE_ENABLED === 'true' ? 'premium' : 'default',
  };
  console.error('❌ Runtime bootstrap failed:', error);
}
