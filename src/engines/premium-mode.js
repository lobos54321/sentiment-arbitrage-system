export function boolFlag(value, defaultValue = false) {
  if (value == null || value === '') return defaultValue;
  return ['1', 'true', 'yes', 'on'].includes(String(value).trim().toLowerCase());
}

export function resolvePremiumPaperOnlyMode(config = {}, env = process.env) {
  if (typeof config.PAPER_ONLY_MODE === 'boolean') return config.PAPER_ONLY_MODE;
  if (typeof config.PREMIUM_LIVE_EXECUTION_ENABLED === 'boolean') {
    return !config.PREMIUM_LIVE_EXECUTION_ENABLED;
  }
  return !boolFlag(env.PREMIUM_LIVE_EXECUTION_ENABLED, false);
}
