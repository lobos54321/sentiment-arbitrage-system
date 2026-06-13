#!/usr/bin/env node
import fs from 'fs';
import path from 'path';
import { pathToFileURL } from 'url';

const UNIT_RATIO_SUSPECT_MIN = 80;
const UNIT_RATIO_SUSPECT_MAX = 300;

function parseArgs(argv = process.argv.slice(2)) {
  const args = {
    baselineRouted: '',
    gmgnTouch: '',
    outDir: './data/audits/baseline-unit-router',
  };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const next = argv[i + 1];
    if (key === '--baseline-routed') { args.baselineRouted = next; i += 1; continue; }
    if (key === '--gmgn-touch') { args.gmgnTouch = next; i += 1; continue; }
    if (key === '--out-dir') { args.outDir = next; i += 1; continue; }
    if (key === '--help' || key === '-h') { args.help = true; continue; }
    throw new Error(`Unknown argument: ${key}`);
  }
  return args;
}

function usage() {
  return [
    'Usage:',
    '  node scripts/build-baseline-unit-router.js \\',
    '    --baseline-routed baseline-route-v1/chain-truth-tier1-baseline-routed.jsonl \\',
    '    --gmgn-touch baseline-route-v1/graduated-route-gmgn-amm-touch.json \\',
    '    --out-dir baseline-unit-router-v1',
    '',
    'Builds a unit-aware baseline routing artifact. Curve rows stay in SOL/native;',
    'graduated rows use GMGN/AMM USD baselines; cross-domain returns must be',
    'computed with an explicit graduation bridge and never by direct SOL/USD division.',
  ].join('\n');
}

function numeric(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function readJsonl(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return [];
  return raw.split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function loadGmgnRows(filePath) {
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Array.isArray(parsed?.results) ? parsed.results : [];
}

function anchorKey(row = {}) {
  return `${row.token_ca}|${row.anchor_ts ?? row.signal_ts}`;
}

function gmgnKey(row = {}) {
  return `${row.token_ca}|${row.signal_ts ?? row.anchor_ts}`;
}

function countBy(rows = [], field) {
  const out = {};
  for (const row of rows) {
    const key = row[field] ?? 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function percentile(values = [], p) {
  const clean = values.map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  if (!clean.length) return null;
  return clean[Math.min(clean.length - 1, Math.max(0, Math.floor((clean.length - 1) * p)))];
}

function directUnitRatio(row = {}, gmgnRow = {}) {
  const gmgnUsd = numeric(gmgnRow.entry_0m_price);
  const curveSol = numeric(row.baseline_price_sol_chain);
  if (gmgnUsd == null || curveSol == null || curveSol <= 0) return null;
  return gmgnUsd / curveSol;
}

function classifyBaselineUnit(row = {}, gmgnRow = null) {
  const route = row.baseline_route_v1 || '';
  const curvePrice = numeric(row.baseline_price_sol_chain);
  const gmgnUsd = numeric(gmgnRow?.entry_0m_price);
  const ratio = gmgnRow ? directUnitRatio(row, gmgnRow) : null;
  const ratioSuspect = ratio != null && ratio >= UNIT_RATIO_SUSPECT_MIN && ratio <= UNIT_RATIO_SUSPECT_MAX;

  function gmgnUsdBaseline(routeName, routeReason) {
    return {
      ...row,
      unit_domain: 'usd_gmgn',
      baseline_unit_route: routeName,
      baseline_price_usd_gmgn: gmgnUsd,
      baseline_price_sol_chain_for_reference: curvePrice,
      gmgn_entry_0m_ts: gmgnRow.entry_0m_ts ?? null,
      gmgn_first_bar_lag_sec: gmgnRow.first_bar_lag_sec ?? null,
      gmgn_first_nonzero_volume_lag_sec: gmgnRow.first_nonzero_volume_lag_sec ?? null,
      gmgn_early_15m_volume_usd_sum: gmgnRow.early_15m_volume_usd_sum ?? null,
      gmgn_bars: gmgnRow.bars ?? null,
      gmgn_nonzero_volume_bars: gmgnRow.nonzero_volume_bars ?? null,
      direct_usd_per_sol_like_ratio: ratio,
      label_unit_suspect: ratioSuspect,
      return_calculation_rule: `${routeReason}; compare against GMGN USD peak only`,
    };
  }

  if (route === 'graduated_route_gmgn_amm') {
    if (gmgnUsd == null || gmgnUsd <= 0) {
      return {
        ...row,
        unit_domain: 'missing_gmgn_anchor_price',
        baseline_unit_route: 'graduated_missing_gmgn',
        baseline_price_usd_gmgn: null,
        baseline_price_sol_chain_for_reference: curvePrice,
        direct_usd_per_sol_like_ratio: ratio,
        label_unit_suspect: ratioSuspect,
        return_calculation_rule: 'not_evaluable_until_gmgn_anchor_price_available',
      };
    }
    return {
      ...row,
      unit_domain: 'usd_gmgn',
      baseline_unit_route: 'graduated_gmgn_amm',
      baseline_price_usd_gmgn: gmgnUsd,
      baseline_price_sol_chain_for_reference: curvePrice,
      gmgn_entry_0m_ts: gmgnRow.entry_0m_ts ?? null,
      gmgn_first_bar_lag_sec: gmgnRow.first_bar_lag_sec ?? null,
      gmgn_first_nonzero_volume_lag_sec: gmgnRow.first_nonzero_volume_lag_sec ?? null,
      gmgn_early_15m_volume_usd_sum: gmgnRow.early_15m_volume_usd_sum ?? null,
      gmgn_bars: gmgnRow.bars ?? null,
      gmgn_nonzero_volume_bars: gmgnRow.nonzero_volume_bars ?? null,
      direct_usd_per_sol_like_ratio: ratio,
      label_unit_suspect: ratioSuspect,
      return_calculation_rule: 'compare_against_gmgn_usd_peak_only; never divide GMGN USD by curve SOL',
    };
  }

  if (route === 'curve_active_curve_baseline' || route === 'near_graduation_curve_baseline') {
    return {
      ...row,
      unit_domain: 'sol_curve',
      baseline_unit_route: route === 'curve_active_curve_baseline' ? 'curve_active_native' : 'near_graduation_native',
      baseline_price_sol_curve: curvePrice,
      return_calculation_rule: 'curve-domain returns use native SOL prices; cross-graduation returns require explicit graduation bridge',
      spliced_return_required_if_peak_domain_is_gmgn: true,
      label_unit_suspect: false,
    };
  }

  if (route === 'quiet_no_curve_trade_near_anchor') {
    if (gmgnUsd != null && gmgnUsd > 0) {
      return gmgnUsdBaseline(
        'quiet_no_curve_trade_gmgn_anchor',
        'no nearby curve trade; GMGN provides anchor baseline in USD domain',
      );
    }
    return {
      ...row,
      unit_domain: 'missing_curve_baseline',
      baseline_unit_route: 'quiet_no_curve_trade_near_anchor',
      return_calculation_rule: 'not_evaluable_without alternate baseline source or deeper history',
      label_unit_suspect: false,
    };
  }

  if (route === 'hot_tail_history_incomplete_dune') {
    if (gmgnUsd != null && gmgnUsd > 0) {
      return gmgnUsdBaseline(
        'history_incomplete_gmgn_anchor',
        'curve history incomplete; GMGN provides anchor baseline in USD domain',
      );
    }
    return {
      ...row,
      unit_domain: 'history_incomplete',
      baseline_unit_route: 'hot_tail_needs_dune_or_slow_history',
      return_calculation_rule: 'not_evaluable_until history reaches anchor window',
      label_unit_suspect: false,
    };
  }

  if (route === 'venue_other_needs_other_decoder') {
    if (gmgnUsd != null && gmgnUsd > 0) {
      return gmgnUsdBaseline(
        'venue_other_gmgn_anchor',
        'non-pump venue; GMGN provides anchor baseline in USD domain',
      );
    }
    return {
      ...row,
      unit_domain: 'venue_other',
      baseline_unit_route: 'needs_other_decoder',
      return_calculation_rule: 'not_evaluable_by pumpfun curve decoder',
      label_unit_suspect: false,
    };
  }

  return {
    ...row,
    unit_domain: 'unknown',
    baseline_unit_route: 'unknown',
    return_calculation_rule: 'not_evaluable_until_routed',
    label_unit_suspect: false,
  };
}

function buildUnitRouter({ baselineRows = [], gmgnRows = [] } = {}) {
  const gmgnByKey = new Map(gmgnRows.map((row) => [gmgnKey(row), row]));
  const rows = baselineRows.map((row) => classifyBaselineUnit(row, gmgnByKey.get(anchorKey(row))));
  const graduated = rows.filter((row) => row.baseline_unit_route === 'graduated_gmgn_amm');
  const ratios = graduated.map((row) => numeric(row.direct_usd_per_sol_like_ratio)).filter(Number.isFinite);
  return {
    rows,
    summary: {
      schema_version: 'baseline_unit_router.v1',
      generated_at: new Date().toISOString(),
      rows_n: rows.length,
      by_unit_domain: countBy(rows, 'unit_domain'),
      by_baseline_unit_route: countBy(rows, 'baseline_unit_route'),
      graduated_rows_n: rows.filter((row) => row.baseline_route_v1 === 'graduated_route_gmgn_amm').length,
      graduated_gmgn_price_rows_n: graduated.length,
      missing_gmgn_anchor_price_n: rows.filter((row) => row.baseline_unit_route === 'graduated_missing_gmgn').length,
      gmgn_recovered_incomplete_route_rows_n: rows.filter((row) => ['history_incomplete_gmgn_anchor', 'quiet_no_curve_trade_gmgn_anchor', 'venue_other_gmgn_anchor'].includes(row.baseline_unit_route)).length,
      direct_usd_per_sol_like_ratio_summary: {
        n: ratios.length,
        min: ratios.length ? Math.min(...ratios) : null,
        p10: percentile(ratios, 0.1),
        p50: percentile(ratios, 0.5),
        p90: percentile(ratios, 0.9),
        max: ratios.length ? Math.max(...ratios) : null,
      },
      label_unit_suspect_n: rows.filter((row) => row.label_unit_suspect === true).length,
      note: 'GMGN prices are USD-domain. Curve prices are SOL/native-domain. Direct USD/SOL-like ratios are diagnostics only, not returns.',
    },
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    console.log(usage());
    return;
  }
  if (!args.baselineRouted || !args.gmgnTouch) {
    throw new Error('Provide --baseline-routed and --gmgn-touch');
  }
  const baselineRows = readJsonl(args.baselineRouted);
  const gmgnRows = loadGmgnRows(args.gmgnTouch);
  const report = buildUnitRouter({ baselineRows, gmgnRows });
  const outDir = path.resolve(args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const outJsonl = path.join(outDir, 'baseline-unit-routed.jsonl');
  const outSummary = path.join(outDir, 'summary.json');
  fs.writeFileSync(outJsonl, `${report.rows.map((row) => JSON.stringify(row)).join('\n')}\n`);
  fs.writeFileSync(outSummary, `${JSON.stringify(report.summary, null, 2)}\n`);
  console.log(JSON.stringify({ outJsonl, outSummary, summary: report.summary }, null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || String(error));
    process.exit(1);
  });
}

export {
  buildUnitRouter,
  classifyBaselineUnit,
  directUnitRatio,
};
