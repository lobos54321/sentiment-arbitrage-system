/**
 * Lightweight cloud bootstrap.
 *
 * Start the dashboard/health server before importing the heavier trading
 * runtime so platform health checks can succeed while runtime modules load.
 */

import { startDashboardServer } from './web/dashboard-server.js';

global.__dashboardStarted = true;
startDashboardServer();

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
