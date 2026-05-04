function parseGateResult(value) {
  if (!value || typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function normalizeUnixishSeconds(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n);
}

function pushCount(map, key) {
  const k = String(key || '').trim() || 'unknown';
  map[k] = (map[k] || 0) + 1;
}

function fieldText(...values) {
  return values
    .filter((v) => v !== undefined && v !== null && String(v).trim() !== '')
    .map((v) => String(v))
    .join(' | ');
}

export function summarizePremiumSignalGateHealth(rows = []) {
  const counters = {
    sampled_n: rows.length,
    unknown_data_blocked_n: 0,
    fail_closed_n: 0,
    rate_limited_n: 0,
    invalid_api_key_n: 0,
    by_status: {},
    by_gate_reason: {},
    by_provider_data_state: {},
  };
  const samples = [];

  for (const row of rows) {
    const gate = parseGateResult(row.gate_result);
    const observability = gate.observability || {};
    const backfill = gate.backfill || {};
    const status = gate.status || row.hard_gate_status || 'unknown';
    const gateReason = gate.gateReason || gate.reason || gate.gate_reason || '';
    const providerDataState = observability.providerDataState || '';
    const backfillReason = backfill.reason || '';
    const text = fieldText(status, gateReason, providerDataState, backfillReason, observability.localWaitError);

    pushCount(counters.by_status, status);
    if (gateReason) pushCount(counters.by_gate_reason, gateReason);
    if (providerDataState) pushCount(counters.by_provider_data_state, providerDataState);

    const unknownDataBlocked = (
      gate.unknownDataBlocked === true
      || String(status).includes('UNKNOWN_DATA')
      || String(gate.gateDecision || '').toUpperCase() === 'UNKNOWN_DATA'
    );
    const failClosed = gate.failClosedApplied === true || observability.failClosedApplied === true;
    const rateLimited = /rate[_ -]?limited|429/i.test(text);
    const invalidApiKey = /invalid api key|http\s*401|\b401\b|-32401/i.test(text);

    if (unknownDataBlocked) counters.unknown_data_blocked_n += 1;
    if (failClosed) counters.fail_closed_n += 1;
    if (rateLimited) counters.rate_limited_n += 1;
    if (invalidApiKey) counters.invalid_api_key_n += 1;

    if ((unknownDataBlocked || rateLimited || invalidApiKey) && samples.length < 30) {
      samples.push({
        id: row.id,
        symbol: row.symbol,
        token_ca: row.token_ca,
        timestamp_sec: normalizeUnixishSeconds(row.timestamp),
        status,
        gate_reason: gateReason || null,
        provider_data_state: providerDataState || null,
        backfill_reason: backfillReason || null,
        provider: gate.provider || backfill.provider || null,
        pool_address: gate.poolAddress || backfill.poolAddress || null,
      });
    }
  }

  let status = 'ok';
  const warn_reasons = [];
  if (counters.invalid_api_key_n > 0) {
    status = 'degraded';
    warn_reasons.push('premium_signal_provider_auth_failed');
  } else if (counters.rate_limited_n > 0) {
    status = 'degraded';
    warn_reasons.push('premium_signal_provider_rate_limited');
  }
  if (counters.unknown_data_blocked_n > 0) {
    if (status === 'ok') status = 'warn';
    warn_reasons.push('premium_signal_unknown_data_blocks_present');
  }

  return {
    status,
    counters,
    samples,
    warn_reasons,
  };
}
