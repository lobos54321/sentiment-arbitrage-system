import fs from 'fs';
import crypto from 'crypto';
import { dirname, isAbsolute, join } from 'path';

export const PAPER_MODE_SAFETY_RUNTIME_SCHEMA_VERSION = 'v2.7.0.paper_mode_safety_runtime.v1';

export const LIVE_SECRET_ENV_NAMES = [
  'TRADE_WALLET_PRIVATE_KEY',
  'LIVE_PRIVATE_KEY',
  'WALLET_PRIVATE_KEY',
  'SOLANA_PRIVATE_KEY',
  'BSC_PRIVATE_KEY',
];

const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on']);

function boolFlag(value, defaultValue = false) {
  if (value == null || value === '') return defaultValue;
  return TRUE_VALUES.has(String(value).trim().toLowerCase());
}

function resolvePath(raw, projectRoot) {
  const path = raw || join(projectRoot, 'data', 'v27_read_models', 'paper_mode_safety.json');
  return isAbsolute(path) ? path : join(projectRoot, path);
}

export function defaultV27PaperModeSafetyPath({ env = process.env, projectRoot = process.cwd() } = {}) {
  if (env.V27_PAPER_MODE_SAFETY_PATH) {
    return resolvePath(env.V27_PAPER_MODE_SAFETY_PATH, projectRoot);
  }
  if (env.V27_READ_MODEL_DIR) {
    return resolvePath(join(env.V27_READ_MODEL_DIR, 'paper_mode_safety.json'), projectRoot);
  }
  return resolvePath(null, projectRoot);
}

function secretPresenceHash(presentNames) {
  return crypto
    .createHash('sha256')
    .update(JSON.stringify([...presentNames].sort()))
    .digest('hex');
}

export function buildV27PaperModeSafetyRuntimeEvidence({
  config = {},
  env = process.env,
  processRole = 'premium-channel-system',
  stage = 'startup',
  liveComponents = {},
  now = new Date(),
  pid = process.pid,
  commit = env.GIT_COMMIT || env.COMMIT_SHA || env.ZEABUR_GIT_COMMIT_SHA || env.GITHUB_SHA || null,
} = {}) {
  const premiumLiveExecutionEnabled = boolFlag(env.PREMIUM_LIVE_EXECUTION_ENABLED, false)
    || config.PREMIUM_LIVE_EXECUTION_ENABLED === true;
  const paperOnlyMode = typeof config.PAPER_ONLY_MODE === 'boolean'
    ? config.PAPER_ONLY_MODE
    : !premiumLiveExecutionEnabled;
  const presentLiveSecretNames = LIVE_SECRET_ENV_NAMES.filter((name) => Boolean(env[name]));
  const evidence = {
    runtime_evidence_schema_version: PAPER_MODE_SAFETY_RUNTIME_SCHEMA_VERSION,
    generated_at: now.toISOString(),
    generated_at_ms: now.getTime(),
    pid,
    process_role: processRole,
    stage,
    commit,
    paper_mode_required: true,
    paper_only_mode: paperOnlyMode,
    premium_live_execution_enabled: premiumLiveExecutionEnabled,
    live_private_key_present: presentLiveSecretNames.length > 0,
    present_live_secret_names: presentLiveSecretNames,
    live_secret_presence_hash: secretPresenceHash(presentLiveSecretNames),
    live_swap_endpoint_enabled: boolFlag(env.LIVE_SWAP_ENDPOINT_ENABLED, false),
    real_order_router_enabled: boolFlag(env.REAL_ORDER_ROUTER_ENABLED, false),
    network_transaction_signing_enabled: boolFlag(env.NETWORK_TRANSACTION_SIGNING_ENABLED, false),
    jupiter_executor_initialized: Boolean(liveComponents.jupiterExecutor),
    live_execution_executor_initialized: Boolean(liveComponents.liveExecutionExecutor),
    live_position_monitor_initialized: Boolean(liveComponents.livePositionMonitor),
    quote_client_initialized: Boolean(liveComponents.quoteClient),
    live_price_monitor_initialized: Boolean(liveComponents.livePriceMonitor),
  };
  evidence.violations = [
    evidence.premium_live_execution_enabled ? 'premium_live_execution_enabled' : null,
    evidence.paper_only_mode === false ? 'paper_only_mode_false' : null,
    evidence.live_private_key_present ? 'live_private_key_present' : null,
    evidence.live_swap_endpoint_enabled ? 'live_swap_endpoint_enabled' : null,
    evidence.real_order_router_enabled ? 'real_order_router_enabled' : null,
    evidence.network_transaction_signing_enabled ? 'network_transaction_signing_enabled' : null,
    evidence.jupiter_executor_initialized ? 'jupiter_executor_initialized' : null,
    evidence.live_execution_executor_initialized ? 'live_execution_executor_initialized' : null,
    evidence.live_position_monitor_initialized ? 'live_position_monitor_initialized' : null,
  ].filter(Boolean);
  evidence.paper_live_boundary_ok = evidence.violations.length === 0;
  return evidence;
}

export function writeV27PaperModeSafetyRuntimeEvidence({
  path,
  projectRoot = process.cwd(),
  ...options
} = {}) {
  const outputPath = path || defaultV27PaperModeSafetyPath({ env: options.env || process.env, projectRoot });
  const evidence = buildV27PaperModeSafetyRuntimeEvidence(options);
  fs.mkdirSync(dirname(outputPath), { recursive: true });
  const tmpPath = `${outputPath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
  fs.renameSync(tmpPath, outputPath);
  return { path: outputPath, evidence };
}
