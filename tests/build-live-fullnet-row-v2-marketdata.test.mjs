import assert from 'node:assert/strict';

import {
  klineProvenance,
  featureAvailability,
  quoteProvenance,
  marketDataConfidence,
  marketDataRepairDetail,
  augmentMarketDataRow,
  buildClosureReport,
  evGateMd,
  EXPECTED_FEATURES,
} from '../scripts/build-live-fullnet-row-v2-marketdata.js';

// klineProvenance — authoritative kline_covered + precise missing reasons + cache hit
{
  const covered = klineProvenance({ kline_covered: 1, coverage_reason: 'covered', provider: 'geckoterminal', pool_found: 0, early_15m_bar_count: 8, early_15m_bar_coverage_pct: 53.3, first_bar_lag_sec: 21 });
  assert.equal(covered.kline_seen, true);
  assert.equal(covered.kline_source, 'geckoterminal');
  assert.equal(covered.kline_bars_n, 8);
  assert.equal(covered.kline_window_sec, 900);
  assert.equal(covered.kline_cache_hit, false);
  assert.equal(covered.kline_missing_reason, null);
  assert.equal(covered.raw_provider_seen, true);

  const noToken = klineProvenance({ kline_covered: 0, coverage_reason: 'no_kline_for_token', provider: null, pool_found: 0, early_15m_bar_count: 0 });
  assert.equal(noToken.kline_seen, false);
  assert.equal(noToken.kline_missing_reason, 'kline_pool_unresolved'); // pool_found=0 + 0 bars
  assert.equal(noToken.raw_provider_missing_reason, 'raw_provider_not_recorded');

  const baselineOnly = klineProvenance({ kline_covered: 0, coverage_reason: 'covered', provider: 'gmgn', pool_found: 1, early_15m_bar_count: 0 });
  assert.equal(baselineOnly.kline_missing_reason, 'kline_baseline_only_no_early_bars');

  const cacheHit = klineProvenance({ kline_covered: 1, coverage_reason: 'covered', provider: 'local_cache', pool_found: 1, early_15m_bar_count: 12 });
  assert.equal(cacheHit.kline_cache_hit, true);

  const absent = klineProvenance(null);
  assert.equal(absent.kline_seen, false);
  assert.equal(absent.kline_missing_reason, 'raw_signal_outcomes_row_absent_for_signal');
  assert.equal(absent.raw_provider_missing_reason, 'raw_signal_outcomes_row_absent_for_signal');
}

// featureAvailability — field-level present/missing
{
  const f = featureAvailability({ market_cap: 1000, volume_24h: 50, holders: null, top10_pct: null, narrative_score: 7 });
  assert.equal(f.feature_vector_seen, true);
  assert.deepEqual(f.feature_vector_fields_present, ['market_cap', 'volume_24h', 'narrative_score']);
  assert.deepEqual(f.feature_vector_fields_missing, ['holders', 'top10_pct']);
  assert.equal(f.feature_vector_missing_reason, 'feature_vector_missing_field:holders|top10_pct');

  const absent = featureAvailability(null);
  assert.equal(absent.feature_vector_seen, false);
  assert.deepEqual(absent.feature_vector_fields_missing, EXPECTED_FEATURES);
  assert.equal(absent.feature_vector_missing_reason, 'source_to_raw_row_absent_for_signal');
}

// quoteProvenance — from v1 row
{
  assert.equal(quoteProvenance({ quote_source: 'jupiter' }).quote_provider_seen, true);
  const miss = quoteProvenance({ quote_source: null, has_decision: true, route_missing_reason: 'quote_source_missing' });
  assert.equal(miss.quote_provider_seen, false);
  assert.equal(miss.quote_provider_missing_reason, 'quote_source_missing');
  assert.equal(quoteProvenance({ quote_source: null, has_decision: false }).quote_provider_missing_reason, 'no_decision_event_for_signal');
}

// marketDataConfidence — HIGH/MEDIUM/LOW
{
  assert.equal(marketDataConfidence({ kline_seen: true, raw_provider_seen: true, feature_vector_seen: true, quote_provider_seen: true, feature_vector_fields_missing: [] }).market_data_provenance_confidence, 'HIGH');
  assert.equal(marketDataConfidence({ kline_seen: true, raw_provider_seen: true, feature_vector_seen: true, quote_provider_seen: false, feature_vector_fields_missing: ['holders'] }).market_data_provenance_confidence, 'MEDIUM');
  assert.equal(marketDataConfidence({ kline_seen: false, raw_provider_seen: false, feature_vector_seen: false, quote_provider_seen: false, feature_vector_fields_missing: ['x'] }).market_data_provenance_confidence, 'LOW');
}

// marketDataRepairDetail — only for the market-data owner; precise
{
  const owner = 'FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP';
  assert.equal(marketDataRepairDetail({ final_repair_owner_v2: 'ENTRY_BRIDGE_GAP' }, {}), null);
  assert.equal(marketDataRepairDetail({ final_repair_owner_v2: owner }, { kline_seen: false, kline_missing_reason: 'kline_not_available_for_token' }), 'kline_not_available_for_token');
  assert.equal(marketDataRepairDetail({ final_repair_owner_v2: owner }, { kline_seen: true, kline_pool_found: true, feature_vector_fields_missing: ['holders', 'top10_pct'] }), 'feature_missing:holders|top10_pct');
}

// augmentMarketDataRow — additive + deterministic
{
  const row = { token_ca: 'T', signal_ts: 1, class: 'dog', quote_source: null, has_decision: true, final_repair_owner_v2: 'DIRECT_STRATEGY_DECISION_MODULE_GAP' };
  const rso = { kline_covered: 1, coverage_reason: 'covered', provider: 'gmgn', pool_found: 1, early_15m_bar_count: 5 };
  const src = { market_cap: 1000, volume_24h: 20 };
  const a = augmentMarketDataRow(row, rso, src);
  assert.equal(a.token_ca, 'T');          // v1/v2 fields preserved
  assert.equal(a.kline_seen, true);
  assert.equal(a.feature_vector_seen, true);
  assert.ok(['HIGH', 'MEDIUM', 'LOW'].includes(a.market_data_provenance_confidence));
  assert.deepEqual(augmentMarketDataRow(row, rso, src), a); // deterministic
}

// buildClosureReport — proof-gated reclassification (no silent flips)
{
  const proveRows = [
    { kline_seen: true, kline_missing_reason: null, feature_vector_seen: true, raw_provider_seen: true },
    { kline_seen: false, kline_missing_reason: 'kline_not_available_for_token', feature_vector_seen: false, raw_provider_seen: false },
  ];
  const rep = buildClosureReport(proveRows);
  assert.equal(rep.every_module_mapped_to_A_G, true);
  assert.equal(rep.bucket_G_all_intentionally_excluded, true);
  const reclassed = rep.reclassified_from_blocked.map((x) => x.module_group).sort();
  assert.deepEqual(reclassed, ['feature_availability', 'kline_cache', 'raw_provider_evidence']);
  assert.equal(rep.module_count_total, 60);
  // covered count must increase by exactly the 3 reclassified (22 -> 25)
  assert.equal(rep.coverage_status_counts.covered, 25);
  assert.equal(rep.coverage_status_counts.blocked, 27);
  assert.equal(rep.coverage_status_counts.intentionally_excluded, 8);

  // no-proof case: nothing flips (stays 22 covered)
  const noProof = [{ kline_seen: false, kline_missing_reason: 'x', feature_vector_seen: false, raw_provider_seen: false }];
  const rep2 = buildClosureReport(noProof);
  assert.equal(rep2.reclassified_from_blocked.length, 0);
  assert.equal(rep2.coverage_status_counts.covered, 22);
}

// evGateMd — fail-closed unless an ev_eligible row carries net_pnl
{
  assert.equal(evGateMd([{ ev_eligible: false }, { ev_eligible: false }]).actual_net_ev_pct, null);
  assert.equal(evGateMd([{ ev_eligible: false }]).gate, 'BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER');
  const withEv = evGateMd([{ ev_eligible: true, net_pnl_pct: 18 }]);
  assert.equal(withEv.actual_net_ev_pct, 18);
  assert.equal(withEv.gate, 'ACTUAL_NET_EV_AVAILABLE');
}

console.log('build-live-fullnet-row-v2-marketdata tests passed');
