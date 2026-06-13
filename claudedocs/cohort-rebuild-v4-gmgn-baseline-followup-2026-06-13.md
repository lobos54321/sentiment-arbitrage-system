# Cohort Rebuild v4 — GMGN Baseline Follow-up裁决结果(2026-06-13)

状态: 离线研究结论 / 不改 live gate、exit、size。

## 结论

在 `cohort-rebuild-v3-gmgn-followup` 的基础上,对剩余最大 baseline 桶跑 GMGN anchor follow-up,并把 baseline router 扩展为 v3:

- `history_incomplete` / `quiet_no_curve_trade_near_anchor` 行只有在 GMGN 提供 `entry_0m_price` 时才转为 `usd_gmgn` baseline;
- 没有 GMGN anchor price 的行继续保留 incomplete,不硬编。

结果:

- GMGN baseline follow-up: `150/150` ok,`150/150` 有 bars 和非零 volume;
- baseline router v3 将 `150` 行 history-incomplete baseline 恢复为 `usd_gmgn`;
- cohort quarantine 从 `252` 行降到 `83` 行。

## 数字演进

| 版本 | clean rows | quarantine rows | clean dog unique | quarantine unique |
|---|---:|---:|---:|---:|
| v2 physical guard | 539 | 1185 | 219 | 989 |
| v3 GMGN peak follow-up | 1472 | 252 | 375 | 215 |
| v4 GMGN baseline follow-up | 1641 | 83 | 440 | 65 |

`v4` 是当前最强的离线审计基线。旧的 `653`、`357`、`219` 都不能再单独引用为当前 dog 分母;它们只代表不同阶段的中间口径。

## v4 Sanity

- clean active label-unit suspect: `0`
- pump `sol_curve` clean 物理出带峰: `0`
- clean dog rows: `585`
- clean dog unique: `440`

按 return domain:

- `spliced_curve_to_gmgn`: `296` rows / `209` unique
- `sol_curve`: `123` rows / `85` unique
- `usd_gmgn`: `166` rows / `146` unique

## 剩余Quarantine

v4 剩余:

- rows: `83`
- unique signals: `65`
- unique tokens: `39`

follow-up actions:

- `curve_baseline_reconstruction`: `36`
- `venue_baseline_decoder_required`: `28`
- `curve_peak_or_splice_bridge_adjudication`: `18`
- `venue_peak_decoder_required`: `1`

换句话说,大规模 GMGN 可裁决债已经处理完;剩余主要是:

1. pump.fun 曲线基线仍缺失的少量行;
2. 非 pump / other venue 行,不能套 pump.fun 物理上限;
3. 少数 bridge/peak 缺口。

## 当前不可做的事

- 不拿 v4 直接调 live gate / matrix / RR / exit / live size。
- 不用 v4 代替 decision-anchor pack 重算 matched signal-vs-decision 天花板。
- 不把剩余 83 行强行归 dog 或 dud;它们仍是 quarantine。

## 下一步

1. 对 `curve_baseline_reconstruction` 的 36 行继续跑 curve baseline/bridge 补算。
2. 对 `venue_baseline_decoder_required` + `venue_peak_decoder_required` 的 29 行单独设计非 pump venue decoder。
3. 下一份 frozen pack 必须包含完整 `decision_ts` / decision records,用于正式 matched ceiling 和 dog-vs-dud。
4. 等剩余 quarantine 进一步收窄后,再跑分层 dog-vs-dud、policy-allowed capture、matched 双锚天花板。

策略层继续冻结: gate / matrix / RR / exit / live size 不动。
