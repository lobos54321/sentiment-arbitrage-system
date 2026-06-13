#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

import {
  computeUnitAwareReturn,
  tierFromReturnPct,
} from './baseline-return-utils.js';

const PUMPFUN_GRADUATION_PRICE_SOL = 4.1088018e-7;
const CURVE_PHYSICAL_MAX_PRICE_SOL = 4.5e-7;
const SUSPICIOUS_NATIVE_RETURN_PCT = 15;

function isPumpfunMint(tokenCa) {
  return String(tokenCa || '').endsWith('pump');
}

function isPhysicallyImpossibleCurvePrice(tokenCa, price) {
  const value = numeric(price);
  return isPumpfunMint(tokenCa) && value != null && value > CURVE_PHYSICAL_MAX_PRICE_SOL;
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    labelAudit: '',
    baselineRouted: '',
    gmgnFullWindow: '',
    peakWindow: '',
    outDir: './data/audits/chain-truth-cohort-rebuild',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--label-audit') { args.labelAudit = next; i += 1; continue; }
    if (key === '--baseline-routed') { args.baselineRouted = next; i += 1; continue; }
    if (key === '--gmgn-full-window') { args.gmgnFullWindow = next; i += 1; continue; }
    if (key === '--peak-window') { args.peakWindow = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/rebuild-chain-truth-rawdog-cohort.js \\',
    '    --label-audit audits/raw-dog-label-cleaning.json \\',
    '    --baseline-routed baseline-unit-router-v2/baseline-unit-routed.jsonl \\',
    '    --gmgn-full-window gmgn-full-window-v1/gmgn-full-window-touch.json \\',
    '    --peak-window peak-window-full-v2/chain-truth-tier1-peak.json \\',
    '    --out-dir cohort-rebuild-v1',
    '',
    'Rebuilds raw-dog labels using unit-aware chain-truth artifacts. This is an',
    'offline research tool only; it never changes strategy gates or live state.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readJsonl(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function signalKey(row = {}) {
  const token = String(row.token_ca || '').trim();
  const ts = Math.floor(Number(row.signal_ts ?? row.anchor_ts ?? 0));
  return `${token}|${ts}`;
}

function anchorKey(row = {}) {
  const token = String(row.token_ca || '').trim();
  const ts = Math.floor(Number(row.anchor_ts ?? row.signal_ts ?? 0));
  return `${token}|${ts}`;
}

function loadRowsFromJson(filePath, field = 'rows') {
  const parsed = readJson(filePath);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed[field])) return parsed[field];
  if (Array.isArray(parsed.results)) return parsed.results;
  throw new Error(`Unsupported JSON shape for ${filePath}`);
}

function indexBySignal(rows = []) {
  const out = new Map();
  for (const row of rows) out.set(signalKey(row), row);
  return out;
}

function indexByAnchor(rows = []) {
  const out = new Map();
  for (const row of rows) out.set(anchorKey(row), row);
  return out;
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows) {
    const key = row[field] ?? 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function isGoldSilverTier(tier) {
  return ['gold', 'silver'].includes(String(tier || '').toLowerCase());
}

function maxNumeric(values = []) {
  const clean = values.map(numeric).filter((value) => value != null && value > 0);
  return clean.length ? Math.max(...clean) : null;
}

function curvePeakFromPeakWindow(row = {}) {
  const bars = Array.isArray(row.bars) ? row.bars : [];
  const prices = [];
  for (const bar of bars) {
    prices.push(bar.high, bar.close, bar.open, bar.low);
  }
  const peak = maxNumeric(prices);
  if (peak == null) return null;
  if (isPhysicallyImpossibleCurvePrice(row.token_ca, peak)) return null;
  return {
    peak_domain: 'sol_curve',
    peak_price_sol_curve: peak,
    peak_source: 'chain_truth_peak_window_exact',
    peak_window_bars_n: bars.length,
    peak_window_history_reached_start: row.history_reached_start ?? null,
  };
}

function observedNativePeak(row = {}) {
  const peak = numeric(row.observed_max_price);
  if (peak == null || peak <= 0) return null;
  if (isPhysicallyImpossibleCurvePrice(row.token_ca, peak)) return null;
  return {
    peak_domain: 'sol_curve',
    peak_price_sol_curve: peak,
    peak_source: 'native_bars_observed_peak',
  };
}

function nativeObservedReturnPct(row = {}) {
  const baseline = numeric(row.baseline_price);
  const peak = numeric(row.observed_max_price);
  if (baseline == null || baseline <= 0 || peak == null || peak <= 0) return null;
  return (peak / baseline) - 1;
}

function nativeReturnNeedsExternalTruth(row = {}, route = null) {
  if (route) return false;
  const returnPct = nativeObservedReturnPct(row);
  return returnPct != null && returnPct > SUSPICIOUS_NATIVE_RETURN_PCT;
}

function gmgnFullPeak(row = {}) {
  const peak = numeric(row.price_max);
  if (peak == null || peak <= 0) return null;
  return {
    peak_domain: 'usd_gmgn',
    peak_price_usd_gmgn: peak,
    peak_source: 'gmgn_full_window_usd_peak',
    gmgn_full_window_bars: row.bars ?? null,
    gmgn_full_window_from_ts: row.from_ts ?? null,
    gmgn_full_window_to_ts: row.to_ts ?? null,
  };
}

function physicalRefutation(row = {}) {
  const baseline = numeric(row.baseline_price);
  const multiple = numeric(row.recorded_peak_multiple);
  const observedPeak = numeric(row.observed_max_price);
  const observedPeakImpossible = isPhysicallyImpossibleCurvePrice(row.token_ca, observedPeak);
  if (baseline == null || baseline <= 0 || multiple == null || multiple <= 0) {
    return {
      refuted_by_physics: observedPeakImpossible,
      recorded_peak_price_sol_claim: null,
      observed_peak_physically_impossible: observedPeakImpossible,
    };
  }
  const claim = baseline * multiple;
  const claimImpossible = isPhysicallyImpossibleCurvePrice(row.token_ca, claim);
  return {
    refuted_by_physics: claimImpossible || observedPeakImpossible,
    recorded_peak_price_sol_claim: claim,
    observed_peak_physically_impossible: observedPeakImpossible,
  };
}

function buildBaseline(row = {}, route = null, gmgn = null) {
  if (route?.unit_domain === 'usd_gmgn') {
    return {
      ...route,
      unit_domain: 'usd_gmgn',
      baseline_source_resolved: 'baseline_unit_router_gmgn_usd',
    };
  }

  if (route?.unit_domain === 'sol_curve') {
    return {
      ...route,
      unit_domain: 'sol_curve',
      graduation_price_sol_curve: PUMPFUN_GRADUATION_PRICE_SOL,
      baseline_source_resolved: 'baseline_unit_router_curve_sol',
    };
  }

  const gmgnAnchor = numeric(gmgn?.entry_0m_price);
  if (nativeReturnNeedsExternalTruth(row, route) && gmgnAnchor != null && gmgnAnchor > 0) {
    return {
      unit_domain: 'usd_gmgn',
      baseline_price_usd_gmgn: gmgnAnchor,
      baseline_unit_route: 'native_observed_high_return_gmgn_anchor',
      baseline_source_resolved: 'native_observed_high_return_gmgn_anchor',
    };
  }

  const originalBaseline = numeric(row.baseline_price);
  if (originalBaseline != null && originalBaseline > 0) {
    return {
      unit_domain: 'sol_curve',
      baseline_price_sol_curve: originalBaseline,
      graduation_price_sol_curve: PUMPFUN_GRADUATION_PRICE_SOL,
      baseline_unit_route: 'original_native_baseline',
      baseline_source_resolved: 'original_native_baseline',
    };
  }

  return null;
}

function candidateReturns({ row = {}, route = null, gmgn = null, peakWindow = null } = {}) {
  const baseline = buildBaseline(row, route, gmgn);
  const candidates = [];
  const peakCandidates = [
    nativeReturnNeedsExternalTruth(row, route) ? null : observedNativePeak(row),
    curvePeakFromPeakWindow(peakWindow || {}),
    gmgnFullPeak(gmgn || {}),
  ].filter(Boolean);

  if (!baseline) {
    return {
      candidates,
      selected: null,
      reason: 'missing_baseline',
      peak_candidates: peakCandidates,
    };
  }

  for (const peak of peakCandidates) {
    if (baseline.unit_domain === 'sol_curve' && peak.peak_domain === 'usd_gmgn') {
      const bridge = {
        ...baseline,
        graduation_price_sol_curve: baseline.graduation_price_sol_curve || PUMPFUN_GRADUATION_PRICE_SOL,
        graduation_price_usd_gmgn: numeric(gmgn?.entry_0m_price),
      };
      const result = computeUnitAwareReturn(bridge, {
        ...peak,
        graduation_price_sol_curve: bridge.graduation_price_sol_curve,
        graduation_price_usd_gmgn: bridge.graduation_price_usd_gmgn,
      });
      candidates.push({
        ...result,
        peak_source: peak.peak_source,
        baseline_source: baseline.baseline_source_resolved,
        baseline_unit_route: baseline.baseline_unit_route ?? null,
        peak_price: peak.peak_price_usd_gmgn,
        peak_domain: peak.peak_domain,
      });
      continue;
    }

    const result = computeUnitAwareReturn(baseline, peak);
    candidates.push({
      ...result,
      peak_source: peak.peak_source,
      baseline_source: baseline.baseline_source_resolved,
      baseline_unit_route: baseline.baseline_unit_route ?? null,
      peak_price: peak.peak_price_usd_gmgn ?? peak.peak_price_sol_curve ?? null,
      peak_domain: peak.peak_domain,
    });
  }

  const evaluable = candidates
    .filter((candidate) => candidate.evaluable && numeric(candidate.return_pct) != null)
    .sort((a, b) => Number(b.return_pct) - Number(a.return_pct));

  return {
    candidates,
    selected: evaluable[0] || null,
    reason: evaluable.length ? null : 'missing_compatible_peak_or_bridge',
    peak_candidates: peakCandidates,
  };
}

function rebuildRow(row = {}, maps = {}) {
  const key = signalKey(row);
  const route = maps.routeByKey.get(key) || null;
  const gmgn = maps.gmgnByKey.get(key) || null;
  const peakWindow = maps.peakByKey.get(key) || null;
  const refutation = physicalRefutation(row);
  const returns = candidateReturns({ row, route, gmgn, peakWindow });

  const base = {
    ...row,
    adjudication_key: key,
    adjudication_schema: 'chain_truth_rawdog_cohort_rebuild.v1',
    chain_truth_route_present: Boolean(route),
    gmgn_full_window_present: Boolean(gmgn),
    peak_window_present: Boolean(peakWindow),
    refuted_by_physics: refutation.refuted_by_physics,
    recorded_peak_price_sol_claim: refutation.recorded_peak_price_sol_claim,
    observed_peak_physically_impossible: refutation.observed_peak_physically_impossible,
    route_unit_domain: route?.unit_domain ?? null,
    route_baseline_unit_route: route?.baseline_unit_route ?? null,
  };

  if (returns.selected) {
    const tier = tierFromReturnPct(returns.selected.return_pct);
    const externalTruthPresent = Boolean(route || gmgn || peakWindow);
    const adjudicationStatus = (
      row.label_status === 'clean'
      && !externalTruthPresent
      && returns.selected.peak_source === 'native_bars_observed_peak'
    ) ? 'clean_recomputed_from_native_bars' : 'clean_chain_adjudicated';
    return {
      ...base,
      label_status: 'clean',
      previous_label_status: row.label_status || null,
      previous_effective_tier: row.effective_tier || row.tier || null,
      effective_tier: tier,
      corrected_peak_pct: returns.selected.return_pct,
      corrected_peak_multiple: returns.selected.return_pct + 1,
      label_cleaning_reason: row.label_cleaning_reason || null,
      label_adjudication_status: adjudicationStatus,
      label_adjudication_reason: returns.selected.return_calculation_rule,
      return_domain: returns.selected.return_domain,
      return_calculation_rule: returns.selected.return_calculation_rule,
      return_peak_source: returns.selected.peak_source,
      return_baseline_source: returns.selected.baseline_source,
      return_baseline_unit_route: returns.selected.baseline_unit_route,
      return_peak_domain: returns.selected.peak_domain,
      candidate_return_count: returns.candidates.length,
      evaluable_return_count: returns.candidates.filter((candidate) => candidate.evaluable).length,
      label_unit_suspect_active: false,
      coverage_incomplete: false,
    };
  }

  if (row.label_status === 'clean' && !refutation.refuted_by_physics && !nativeReturnNeedsExternalTruth(row, route)) {
    return {
      ...base,
      label_status: 'clean',
      label_adjudication_status: 'clean_original_label',
      label_adjudication_reason: row.label_cleaning_reason || 'original_clean',
      corrected_peak_pct: numeric(row.max_sustained_peak_pct),
      corrected_peak_multiple: numeric(row.recorded_peak_multiple),
      effective_tier: row.effective_tier || row.tier || null,
      label_unit_suspect_active: false,
      coverage_incomplete: false,
    };
  }

  return {
    ...base,
    label_status: 'quarantine',
    label_adjudication_status: refutation.refuted_by_physics ? 'quarantine_refuted_by_physics_needs_corrected_peak' : 'quarantine_incomplete',
    label_adjudication_reason: nativeReturnNeedsExternalTruth(row, route) && !returns.selected ? 'suspicious_native_return_needs_external_truth' : (returns.reason || 'not_evaluable'),
    effective_tier: 'unknown',
    candidate_return_count: returns.candidates.length,
    evaluable_return_count: returns.candidates.filter((candidate) => candidate.evaluable).length,
    label_unit_suspect_active: refutation.refuted_by_physics,
    coverage_incomplete: true,
  };
}

function dedupeByTokenSignal(rows = []) {
  const out = new Map();
  for (const row of rows) {
    const key = signalKey(row);
    if (!key.endsWith('|0') && !out.has(key)) out.set(key, row);
  }
  return [...out.values()].sort((a, b) => Number(a.signal_ts) - Number(b.signal_ts) || String(a.token_ca).localeCompare(String(b.token_ca)));
}

export function rebuildCohort({ labelRows = [], baselineRows = [], gmgnRows = [], peakRows = [] } = {}) {
  const maps = {
    routeByKey: indexBySignal(baselineRows),
    gmgnByKey: indexBySignal(gmgnRows),
    peakByKey: indexByAnchor(peakRows),
  };
  const rows = labelRows.map((row) => rebuildRow(row, maps));
  const cleanRows = rows.filter((row) => row.label_status === 'clean');
  const quarantineRows = rows.filter((row) => row.label_status !== 'clean');
  const cleanDogs = dedupeByTokenSignal(cleanRows.filter((row) => isGoldSilverTier(row.effective_tier)));
  const cleanDuds = dedupeByTokenSignal(cleanRows.filter((row) => !isGoldSilverTier(row.effective_tier)));
  return {
    schema_version: 'chain_truth_rawdog_cohort_rebuild.v1',
    generated_at: new Date().toISOString(),
    rows,
    cleanRows,
    quarantineRows,
    cleanDogs,
    cleanDuds,
    summary: {
      input_rows_n: labelRows.length,
      rebuilt_rows_n: rows.length,
      clean_rows_n: cleanRows.length,
      quarantine_rows_n: quarantineRows.length,
      clean_dog_unique_n: cleanDogs.length,
      clean_dud_unique_n: cleanDuds.length,
      quarantine_unique_n: dedupeByTokenSignal(quarantineRows).length,
      by_label_adjudication_status: countBy(rows, 'label_adjudication_status'),
      by_effective_tier: countBy(rows, 'effective_tier'),
      by_return_domain: countBy(rows.filter((row) => row.return_domain), 'return_domain'),
      by_route_unit_domain: countBy(rows.filter((row) => row.route_unit_domain), 'route_unit_domain'),
      refuted_by_physics_n: rows.filter((row) => row.refuted_by_physics).length,
      active_label_unit_suspect_n: rows.filter((row) => row.label_unit_suspect_active).length,
      clean_active_label_unit_suspect_n: rows.filter((row) => row.label_status === 'clean' && row.label_unit_suspect_active).length,
      quarantine_active_label_unit_suspect_n: rows.filter((row) => row.label_status !== 'clean' && row.label_unit_suspect_active).length,
      coverage_incomplete_n: rows.filter((row) => row.coverage_incomplete).length,
    },
  };
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function writeJsonl(filePath, rows = []) {
  fs.writeFileSync(filePath, `${rows.map((row) => JSON.stringify(row)).join('\n')}${rows.length ? '\n' : ''}`);
}

function writeReport(report, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  writeJson(path.join(outDir, 'rebuild-summary.json'), {
    schema_version: report.schema_version,
    generated_at: report.generated_at,
    summary: report.summary,
  });
  writeJsonl(path.join(outDir, 'rebuilt-rows.jsonl'), report.rows);
  writeJson(path.join(outDir, 'rebuilt-clean-dogs.json'), report.cleanDogs);
  writeJson(path.join(outDir, 'rebuilt-clean-duds.json'), report.cleanDuds);
  writeJson(path.join(outDir, 'rebuilt-quarantine.json'), report.quarantineRows);
  writeJson(path.join(outDir, 'validation.json'), {
    schema_version: 'chain_truth_rawdog_cohort_rebuild_validation.v1',
    generated_at: report.generated_at,
    checks: {
      no_clean_active_label_unit_suspect: report.summary.clean_active_label_unit_suspect_n === 0,
      rows_conserved: report.summary.input_rows_n === report.summary.rebuilt_rows_n,
    },
    summary: report.summary,
  });
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  for (const [name, value] of Object.entries({
    labelAudit: args.labelAudit,
    baselineRouted: args.baselineRouted,
    gmgnFullWindow: args.gmgnFullWindow,
    peakWindow: args.peakWindow,
  })) {
    if (!value) throw new Error(`Provide --${name.replace(/[A-Z]/g, (m) => `-${m.toLowerCase()}`)}`);
  }

  const labelRows = loadRowsFromJson(args.labelAudit, 'rows');
  const baselineRows = readJsonl(args.baselineRouted);
  const gmgnRows = loadRowsFromJson(args.gmgnFullWindow, 'results');
  const peakRows = loadRowsFromJson(args.peakWindow, 'results');
  const report = rebuildCohort({ labelRows, baselineRows, gmgnRows, peakRows });
  const outDir = path.resolve(args.outDir);
  writeReport(report, outDir);
  console.log(JSON.stringify({ out_dir: outDir, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}
