# Cohort Rebuild v3 — GMGN Follow-up裁决结果(2026-06-13)

状态: 离线研究结论 / 不改 live gate、exit、size。

## 结论

`cohort-rebuild-v2` 里最大的 quarantine 桶不是死数据。对 `needs_legal_peak_no_external_truth` 桶跑 GMGN full-window follow-up 后,`933` 行未决中的绝大多数可以被合法 USD 峰值裁决。

当前可信基线从:

- `cohort-rebuild-v2`: clean `539` 行, quarantine `1185` 行, clean dog unique `219`

推进到:

- `cohort-rebuild-v3-gmgn-followup`: clean `1472` 行, quarantine `252` 行, clean dog unique `375`

这说明此前 `653` 过高,`219` 是严格物理绊网下的保守下界,而 GMGN 合法峰值补齐后,当前证据支持的 clean dog unique 量级是 `375`。剩余 `252` 行仍必须留在 quarantine,不能硬归 dog 或 dud。

## 关键验证

- GMGN follow-up full:
  - anchors `774`
  - ok `774`
  - bars available `774`
  - nonzero volume `774`
  - early 15m nonzero volume `383` (`49.48%`)
- v3 sanity:
  - clean active label-unit suspect: `0`
  - pump `sol_curve` clean 行中物理出带峰: `0`
  - v2 → v3 转换:
    - quarantine → clean gold: `198`
    - quarantine → clean silver: `38`
    - quarantine → clean bronze: `33`
    - quarantine → clean sub25: `664`

这不是单向把 dud 抬成 dog;GMGN 合法峰值同时把大量未决行压回 `sub25`。

## Clean Dog分域

v3 clean dog:

- total clean dog rows: `507`
- clean dog unique: `375`

按 return domain:

- `spliced_curve_to_gmgn`: `296` rows / `209` unique
- `sol_curve`: `123` rows / `85` unique
- `usd_gmgn`: `88` rows / `81` unique

`spliced_curve_to_gmgn` 成为主域,说明很多 pump.fun 曲线期基线 + GMGN 毕业后峰值的跨域拼接被合法化。

## 剩余Quarantine

v3 剩余:

- rows: `252`
- unique signals: `215`
- unique tokens: `154`

follow-up actions:

- `baseline_deep_backfill_or_external_route`: `169`
- `curve_baseline_reconstruction`: `36`
- `venue_baseline_decoder_required`: `28`
- `curve_peak_or_splice_bridge_adjudication`: `18`
- `venue_peak_decoder_required`: `1`

这些是后续裁决工作单,不是策略信号。

## 当前不可做的事

- 不用旧 `653` 当 dog 分母。
- 不用 `357` 当最终 dog 分母。
- 不拿 v3 直接调 gate/matrix/RR/exit/live size。
- 不在缺 decision-anchor pack 的情况下重算正式 matched 双锚天花板。

## 下一步

1. 以 `cohort-rebuild-v3-gmgn-followup` 为当前 frozen 审计基线。
2. 继续裁决剩余 `252` 行 quarantine,优先:
   - baseline deep backfill/external route (`169`)
   - curve baseline reconstruction (`36`)
   - venue-specific decoder (`29`)
3. 下一份 data pack 必须包含完整 `decision_ts` / decision records,否则不能做正式 matched signal-vs-decision 天花板。
4. 裁决收窄后再做分层 dog-vs-dud、capture ceiling、policy-allowed KPI。

策略层继续冻结: gate / matrix / RR / exit / live size 不动。
