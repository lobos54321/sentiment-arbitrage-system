# Gold/Silver Capture Net Modification Plan

日期: 2026-06-21  
状态: research-only modification plan  
目标: 把系统改造成一张只服务“抓到金银狗”的逐信号捕获网,并用 dog/dud + EV 审计判断每个板块是否真的贡献捕获能力。  

禁止: 不改 live gate / matrix / entry / exit / size; 不宣布 edge; 不把 raw dog 当入场信号; 不用事后标签做实时决策。

## 1. 核心目标

唯一业务问题:

```text
Telegram 信号源抓到的大涨币,能不能通过系统网在信号当刻或后续现实确认点被识别、允许入场、实际进入、并持有到收益区?
```

这张网只保留会直接影响下面三件事的模块:

```text
买不买
什么时候买
买了能不能拿到收益区
```

因此入网模块限定为:

```text
signal metadata
lifecycle state
Markov regime
filters / gates
matrix score
readiness
quote / route
would_enter
entry
exit / hold
friction / slippage
```

Raw outcome label 不属于策略模块。它只作为事后审计分母,用于判断“这只是不是严格金银狗 / dud”。

## 2. 当前事实基线

当前线上 24h fullnet readout:

- artifact: `/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/`
- readout: `claudedocs/fullnet-online-complete-readout-2026-06-21.md`
- dog bucket audit: `/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-dog-bucket-audit.json`

当前 24h:

| denominator | count |
|---|---:|
| source unique signals | 357 |
| formal sustained dogs | 37 |
| clean peak >=50% | 44 |
| clean peak >=5x | 5 |
| clean peak >=10x | 2 |

Formal dog chain:

| stage | count |
|---|---:|
| has decision | 36 / 37 |
| quote clean | 34 / 37 |
| would_enter | 12 / 37 |
| entered | 0 / 37 |
| held/captured | 0 / 37 |

Main buckets:

| bucket | count |
|---|---:|
| quote_clean_no_would_enter | 22 |
| would_enter_not_entered | 12 |
| has_decision_no_quote_clean | 2 |
| no_decision_record | 1 |

Important current interpretation:

- 系统大多看见了狗: `36/37` 有 decision。
- 系统大多能给狗拿到 quote-clean 证据: `34/37` quote clean。
- 但 matrix/final-entry 没有明显选狗能力: dog would_enter `32.4%`, dud `30.8%`。
- 0 entry / 0 held,所以还没有捕获。
- Lifecycle identity 已接上,但 lifecycle state/profile 没接上。

## 3. 入网定义

一个模块“入网”必须同时满足 5 条:

1. 每个 `(token_ca, signal_ts)` 都有该模块字段,或者明确 `missing_reason`。
2. 字段必须是 point-in-time,不能从 post-signal outcome 回填。
3. 字段必须可用于 dog/dud 审计: dog rate、dud rate、diff、support。
4. 字段必须可用于 EV 审计: 对应 bucket 的 gross return、realized PnL、friction/slippage、net EV。
5. 模块的输出必须能定位唯一责任: 是 source / lifecycle / Markov / gate / matrix / readiness / quote / entry / exit / friction 哪一层在放行或阻断。

如果一个模块只有日志、没有逐信号字段,不算入网。

如果一个模块只有 identity,没有状态/决策含义,只能算 identity 入网,不能算功能入网。

## 4. 模块边界和协同关系

完整捕获链:

```text
signal metadata
  -> lifecycle state
  -> Markov regime
  -> filters / gates
  -> matrix score
  -> readiness
  -> quote / route
  -> would_enter
  -> entry
  -> exit / hold
  -> friction / slippage
  -> dog/dud + EV audit
```

这不是平级投票。协同规则:

- signal metadata 决定初始语义和初始路由。
- lifecycle state 决定打法模板和观察窗口。
- Markov regime 只在适用生命周期里判断是否进入可行动 regime。
- filters / gates 是硬约束,必须解释阻断原因。
- matrix score 在 lifecycle profile 下评分,不能脱离生命周期单独打分。
- readiness 判断系统是否具备当前入场所需的证据和模式条件。
- quote / route 判断现实可成交性。
- would_enter 是最终事前允许意图,不是捕获。
- entry 证明 enqueue/order/fill/ledger 是否真的发生。
- exit / hold 证明是否拿到 silver/gold 区。
- friction / slippage 把 gross capture 转成 net EV。

## 5. 各模块必须入网的字段

### 5.1 Signal Metadata

作用:

- 说明 Telegram 信号是什么。
- 给 lifecycle 初始路由。
- 给 source gate 审计。

必须字段:

```text
token_ca
symbol
signal_ts
signal_type
is_ath
raw_message_hash
narrative_score
ai_narrative_tier
source_hard_gate_status
source_hard_gate_reason
source_gate_terminal_status
source_gate_statuses_seen
```

Dog/dud 审计:

```text
source_gate_pass_rate_dog
source_gate_pass_rate_dud
by_signal_type dog/dud rate
by_is_ath dog/dud rate
by_source_gate_status dog/dud rate
```

EV 审计:

```text
net_ev_by_signal_type
net_ev_by_is_ath
net_ev_by_source_gate_status
```

Current state:

- 入网: mostly yes。
- 当前 dog source pass `56.8%`, dud `47.0%`。
- 不允许从这一天直接放松 source gate;必须按具体 blocker + EV 看。

### 5.2 Lifecycle State

作用:

- 决定这只币属于哪种打法。
- 决定 monitor window、entry template、quote threshold、exit template。

必须字段:

```text
lifecycle_id
lifecycle_at_signal_state
lifecycle_at_signal_profile
lifecycle_at_signal_monitor_window_sec
lifecycle_at_signal_entry_template
lifecycle_at_signal_exit_template
lifecycle_at_signal_features_json
lifecycle_at_signal_missing_reason
lifecycle_at_decision_state
lifecycle_at_decision_profile
lifecycle_at_decision_entry_template
lifecycle_at_decision_missing_reason
```

Dog/dud 审计:

```text
dog/dud by lifecycle_at_signal_state
dog/dud by lifecycle_at_decision_state
would_enter by lifecycle profile
entered/captured by lifecycle profile
```

EV 审计:

```text
gross_ev_by_lifecycle_state
net_ev_by_lifecycle_state
residual_upside_by_monitor_window
capture_to_peak_by_lifecycle_profile
```

Current state:

- `lifecycle_id` identity: `342/357`, formal dog `36/37`。
- real lifecycle_state: `17/357`, formal dog `1/37`。
- 结论: identity 已入网; lifecycle state/profile 尚未功能入网。

Required modification:

- 在 source ingest 或 first decision 时写 `lifecycle_at_signal_*`。
- 在 best decision / would_enter 时写 `lifecycle_at_decision_*`。
- fullnet 必须继续区分 identity vs state,不得混用。

### 5.3 Markov Regime

作用:

- 判断当前 lifecycle 下是否进入 reclaim / revival / continuation / expiration regime。
- 影响延长观察、降级观察、revival canary、是否允许某些模板继续。

必须字段:

```text
markov_applicable
markov_scope
markov_evaluated
markov_regime
markov_bucket
markov_action
markov_confidence
markov_reason
markov_not_applicable_reason
markov_not_evaluated_reason
```

Dog/dud 审计:

```text
dog/dud by markov_applicable
dog/dud by markov_regime
dog/dud by markov_bucket
would_enter/entered/captured by markov_bucket
```

EV 审计:

```text
net_ev_by_markov_bucket
net_ev_by_markov_action
incremental_ev_over_lifecycle_baseline
```

Current state:

- projected for `342/357` decisioned signals。
- dog applicable `75.7%`, dud `70.1%`。
- 现在只是轻微 dog-enriched,不能证明 Markov 有捕获价值。

Required modification:

- Markov 输出要结构化成 regime/bucket/action/confidence,不能只靠 reason 文本正则。
- Markov 必须绑定 lifecycle scope: 哪些生命周期适用、哪些不适用。

### 5.4 Filters / Gates

作用:

- 事前排除不可交易、风险过高、证据不足的机会。
- 不是为了提高漂亮胜率,而是保护净 EV。

必须字段:

```text
gate_pass
gate_name
gate_status
gate_blocker
gate_blocker_owner
hard_blockers[]
block_cause
policy_block
budget_block
mode_safety_block
```

Dog/dud 审计:

```text
dog/dud pass rate by gate
dog/dud blocked count by blocker
formal dog lost at each gate
5x dog lost at each gate
```

EV 审计:

```text
net_ev_if_gate_pass
net_ev_if_gate_block
counterfactual_ev_for_blocked_dogs_only
false_positive_cost_for_gate_relaxation
```

Current state:

- source and hard blockers are in the row。
- 22 quote-clean-no-would-enter dogs: 18 hard_prefilter_failed, 4 opportunity_matrix_red_cell。

Required modification:

- 每个 gate 必须有 stable blocker code。
- “gate blocked dog”不能自动推导为应该放松;必须同时看 blocked dud EV 和 friction。

### 5.5 Matrix Score

作用:

- 在 lifecycle profile 下综合当前证据给 grade/score/RR。
- 决定是否从 observation 进入 would_enter 候选。

必须字段:

```text
matrix_seen
matrix_profile
matrix_grade
matrix_score
matrix_dimensions_json
expected_rr
expected_upside_pct
matrix_red_cell
matrix_missing_reason
```

Dog/dud 审计:

```text
dog/dud by matrix_grade
dog/dud by score decile
dog/dud by red_cell reason
would_enter by matrix_grade
```

EV 审计:

```text
gross_ev_by_matrix_grade
net_ev_by_matrix_grade
precision/recall for formal dog by matrix threshold
incremental_ev_over_quote_clean_baseline
```

Current state:

- matrix covered for `342/357`。
- matrix A+ dog `32.4%`, dud `30.8%`, diff only `+1.7pp`。
- 22 quote-clean-no-would-enter dogs all matrix `REJECT`。

Required modification:

- Matrix 必须输出 profile;不能只输出 grade。
- Matrix EV 审计必须和 lifecycle profile 绑定,否则不同打法被混成一个评分。

### 5.6 Readiness

作用:

- 判断系统是否具备允许入场所需的运行条件和证据完整性。
- 包括 mode readiness、data readiness、route readiness、risk readiness。

必须字段:

```text
readiness_seen
readiness_ok
readiness_mode
readiness_data_state
readiness_route_state
readiness_risk_state
readiness_blocker
readiness_missing_reason
```

Dog/dud 审计:

```text
dog/dud by readiness_ok
dogs blocked by readiness blocker
duds blocked by readiness blocker
```

EV 审计:

```text
net_ev_when_readiness_ok
net_ev_when_readiness_blocked
missed_ev_due_to_readiness_block
```

Current state:

- readiness is partly implicit in hard blockers / mode safety / quote availability。
- 尚未作为独立模块入网。

Required modification:

- 从 decision/mode readiness/read-model 输出中投影 readiness fields。
- readiness 不应和 quote_clean、source_gate、matrix_grade 混在一起。

### 5.7 Quote / Route

作用:

- 判断是否真的能以可接受路径成交。
- 不是只看有没有 quote,还要看 route/liquidity/spread/age 是否足够可信。

必须字段:

```text
quote_seen
quote_route_verified
liquidity_verified
spread_verified
quote_age_verified
executable_quote_clean
quote_source
route_source
liquidity_usd
spread_pct
quote_age_sec
route_missing_reason
quote_missing_reason
```

Dog/dud 审计:

```text
dog/dud by executable_quote_clean
dog/dud by liquidity bucket
dog/dud by spread bucket
dog/dud by route source
```

EV 审计:

```text
net_ev_by_liquidity_bucket
net_ev_by_spread_bucket
slippage_adjusted_ev_by_route
```

Current state:

- quote layer exists, using `risk_json.quote_clean_verified`。
- formal dog quote_clean `34/37`。
- contract issue: 22 quote-clean-no-would-enter dogs include 18 `liquidity_unknown`。

Required modification:

- Split quote into route/quote availability vs executable quote.
- If liquidity is unknown, `executable_quote_clean` should not silently equal true unless explicitly policy-approved.

### 5.8 Would Enter

作用:

- 表示系统在事前链路里产生了“允许入场意图”。
- 不是 capture,不是 ledger。

必须字段:

```text
would_enter
would_enter_ts
would_enter_sec_after_signal
would_enter_reason
would_enter_action
would_enter_source_event_id
would_enter_policy_mode
```

Dog/dud 审计:

```text
dog/dud would_enter rate
would_enter timing vs signal
would_enter timing vs sustained peak
```

EV 审计:

```text
residual_upside_at_would_enter
net_ev_if_enter_at_would_enter
would_enter_late_rate
```

Current state:

- dog would_enter `12/37`, dud would_enter `36/117`。
- diff only `+1.7pp`。
- 5x clean dogs: `3/5` would_enter, but `0/5` entered。

Required modification:

- would_enter must carry policy mode and timing.
- dog/dud + EV audit must distinguish early would_enter vs late would_enter。

### 5.9 Entry

作用:

- 证明 would_enter 是否变成真实执行链。

必须字段:

```text
entry_intent_seen
entry_intent_source
entry_intent_event_id
entry_guard_seen
entry_guard_reason
enqueue_seen
enqueue_ts
order_seen
order_ts
fill_seen
fill_ts
ledger_seen
entry_price
entry_source
entry_missing_reason
entry_bridge_bucket
```

Dog/dud 审计:

```text
dog/dud enqueue rate
dog/dud fill rate
dog/dud ledger rate
would_enter_not_entered split
```

EV 审计:

```text
entry_price_vs_signal_price
entry_price_vs_would_enter_quote
slippage_at_entry
net_ev_after_entry
```

Current state:

- `would_enter_not_entered` split exists:
  - policy_shadow_only: 6 formal dogs
  - would_enter_no_enqueue: 6 formal dogs
- enqueue/order/fill/ledger evidence all 0 in current export。
- Current A-class evidence contains `a_class_live_enqueue` and `execution_guard` events, but these are not proof of actual enqueue/order/fill. They must be projected as entry intent / execution guard evidence only.

Required modification:

- Export paper execution bridge events after would_enter。
- Persist enqueue/order/fill attempts even in shadow/paper mode, or explicitly mark policy_shadow_only。
- Ingest `a_class_live_enqueue` as `entry_intent_*`, not as `enqueue_seen=true` unless an actual enqueue success event exists。
- Ingest `execution_guard` as `entry_guard_*` so the net can distinguish “intended but guard-blocked” from “no entry bridge evidence”。

### 5.10 Exit / Hold

作用:

- 判断买到以后是否能拿到 silver/gold 区,还是被 exit 砍掉。

必须字段:

```text
exit_policy
exit_ts
exit_price
exit_reason
time_held_sec
max_pnl_before_exit
held_to_silver
held_to_gold
moonbag_kept
stop_hit
tp_hit
trailing_stop_hit
```

Dog/dud 审计:

```text
entered dog/dud hold distribution
exit reason by dog/dud
silver/gold capture by exit policy
```

EV 审计:

```text
gross_ev_by_exit_policy
net_ev_by_exit_policy
capture_to_peak_pct
missed_tail_ev
```

Current state:

- entered = 0,所以 exit/hold 暂时无法评估。

Required modification:

- 先补 entry/ledger;没有 entry 就没有 exit/hold 真实审计。
- 但 schema 必须现在预留,避免以后只知道 entry 不知道 hold。

### 5.11 Friction / Slippage

作用:

- 把 gross move 变成可交易 net EV。
- 决定“看起来能抓狗”是否真的赚钱。

必须字段:

```text
entry_quote_price
entry_fill_price
exit_quote_price
exit_fill_price
estimated_entry_slippage_pct
estimated_exit_slippage_pct
fees_pct
priority_fee_sol
spread_cost_pct
round_trip_friction_pct
gross_pnl_pct
net_pnl_pct
friction_missing_reason
```

Dog/dud 审计:

```text
friction distribution by dog/dud
friction by lifecycle / quote route / liquidity bucket
```

EV 审计:

```text
net_ev_after_5pct_friction
net_ev_after_10pct_friction
net_ev_after_actual_friction
breakeven_friction
```

Current state:

- friction/slippage not in fullnet as a first-class module。
- 当前 entry=0,但 counterfactual EV / would_enter EV 仍需要 friction assumptions。

Required modification:

- 从 quote/route/fill 或 bar liquidity 估算 round-trip friction。
- 所有 EV 报告必须同时给 gross EV 和 net EV。

## 6. Dog/Dud + EV 审计标准

每个模块每天必须输出两类审计。

### 6.1 Dog/Dud Separability

每个模块输出:

```text
module_name
bucket_name
dog_n
dud_n
dog_pass_rate
dud_pass_rate
diff_pp
support_warning
```

目标不是追求高 dog rate,而是判断:

```text
这个模块是否真的把狗和 dud 分开?
```

如果 dog/dud 差异 < 5pp 且多日稳定,该模块不能作为选狗依据。

### 6.2 EV Audit

每个模块输出:

```text
module_name
bucket_name
signals_n
formal_dog_n
formal_dud_n
would_enter_n
entered_n
held_n
gross_ev_pct
friction_pct
net_ev_pct
confidence_warning
```

必须同时给:

```text
gross_ev
net_ev_after_friction
```

只看 dog rate 不够。一个模块如果多抓狗但也引入更多高成本 dud,净 EV 可能更差。

## 7. 协同价值判定

一个板块不能单独宣布“有用”。必须通过协同增量证明。

判定方式:

```text
baseline: source + raw denominator
step 1: + lifecycle
step 2: + Markov
step 3: + filters/gates
step 4: + matrix score
step 5: + readiness
step 6: + quote/route
step 7: + would_enter
step 8: + entry
step 9: + exit/hold
step 10: + friction/slippage
```

每加一层,必须报告:

```text
dog retained
dud removed
would_enter changed
entered changed
captured changed
gross EV changed
net EV changed
```

只有当一层提高净 EV 或显著降低风险,才算“协同产生价值”。

## 8. 修改阶段

### Phase 1 — Schema and Projection Repair

目标: 每个入网模块都有逐信号字段或 missing_reason。

Tasks:

1. Extend fullnet row schema to include all modules listed in this document.
2. Keep raw label as ex-post denominator, not strategy module.
3. Split lifecycle identity vs lifecycle state/profile.
4. Split quote availability vs executable quote.
5. Split readiness from hard blockers.
6. Split entry bridge into enqueue/order/fill/ledger.
7. Add friction/slippage fields, even if initially estimated/missing.

Acceptance:

```text
projection_complete = true
all required modules present with value or missing_reason
no module silently absent
```

### Phase 2 — Current 24h Rebuild

目标: 用线上当前 24h 数据重跑 fullnet。

Outputs:

```text
live-fullnet-row.jsonl
live-fullnet-summary.json
live-fullnet-component-separability.json
live-fullnet-dog-bucket-audit.json
live-fullnet-ev-audit.json
```

Acceptance:

```text
source/raw exact window aligned
A-class events complete
dog/dud separability complete
EV audit complete or explicit friction_missing_reason
```

### Phase 3 — Multi-Day Accumulation

目标: 连续 3-5 天同口径跑。

Acceptance:

```text
same schema version
same module definitions
same formal raw label contract
no retroactive denominator changes
daily artifacts archived
```

Only after multi-day data:

- 判断 source gate 是否真的压狗。
- 判断 Markov 是否真的增益。
- 判断 matrix/would_enter 是否真的有选狗能力。
- 判断 entry bridge 是否只是 shadow/policy 问题。
- 判断 friction 后是否仍有正 EV。

### Phase 4 — Repair Board

目标: 每天自动输出唯一主 repair owner。

候选 owner:

```text
SIGNAL_METADATA_GAP
LIFECYCLE_STATE_GAP
MARKOV_REGIME_GAP
FILTER_GATE_OVERBLOCK
MATRIX_NO_SEPARATION
READINESS_BLOCK
QUOTE_ROUTE_GAP
WOULD_ENTER_LATE_OR_WEAK
ENTRY_BRIDGE_GAP
EXIT_HOLD_GAP
FRICTION_KILLS_EV
```

Acceptance:

```text
repair owner based on dog/dud + EV, not only count
owner has supporting rows and examples
no strategy change implied
```

## 9. What Not To Do

Do not:

- 不因为当前 source gate 漏狗就直接放松 gate。
- 不因为 quote_clean 高就假设可成交。
- 不把 lifecycle_id 当 lifecycle state。
- 不把 would_enter 当 capture。
- 不在 entry=0 时讨论 exit 策略优劣。
- 不只看 gross move,必须看 friction-adjusted net EV。
- 不用单日小样本宣布 edge。
- 不让 raw dog label 进入实时决策。

## 10. Immediate Next Implementation Order

按优先级:

1. Add execution bridge export:

```text
would_enter -> enqueue -> order -> fill -> ledger
```

2. Add lifecycle state/profile into decision/export rows:

```text
lifecycle_at_signal_state
lifecycle_at_decision_state
monitor_template
entry_template
exit_template
```

3. Split quote contract:

```text
quote_route_verified
liquidity_verified
spread_verified
executable_quote_clean
```

4. Add readiness module:

```text
readiness_ok
readiness_blocker
mode/data/route/risk readiness
```

5. Add EV/friction audit:

```text
gross_ev
estimated_friction
actual_friction_if_entry
net_ev
breakeven_friction
```

6. Re-run online 24h fullnet and compare:

```text
dog/dud separability
module EV contribution
repair owner
```

## 11. Definition of Done

This plan is done only when:

```text
all 11 direct-capture modules are present in fullnet rows
each module has dog/dud separability output
each module has EV/friction output or explicit missing_reason
would_enter_not_entered is split to enqueue/order/fill/ledger
lifecycle state/profile is available for most decisioned signals
quote clean no longer conflicts with liquidity_unknown
repair board uses dog/dud + net EV, not just counts
multi-day artifacts are comparable
```

Only then can we responsibly ask:

```text
Which module should be tuned to improve gold/silver capture?
```

Until then, the correct status remains:

```text
measurement net improving
capture not proven
no edge declared
no live strategy change authorized
```

## 12. Implementation Tracking (2026-06-21)

本节把上面的修改点落成可审计状态表。目的不是提前签收,而是防止“计划里写了”和“代码/数据里真的入网了”混在一起。

当前代码入口:

```text
scripts/build-live-fullnet-row-report.js
tests/build-live-fullnet-row-report.test.mjs
scripts/build-canonical-ledger-window-export.js
scripts/pull-a-class-window-evidence.js
sentiment-arbitrage-beforeid-main/src/web/dashboard-server.js
sentiment-arbitrage-system/src/web/dashboard-server.js
```

当前线上 24h artifact:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/
```

当前 readout:

```text
claudedocs/fullnet-online-complete-readout-2026-06-21.md
```

### 12.1 Module Status

| module | status | current evidence | still missing |
|---|---|---|---|
| signal metadata | implemented | row includes `signal_type`, `is_ath`, `source_hard_gate_status`, source-gate seen/terminal status; dog/dud source separability is reported | richer channel/source identity only if upstream logs it |
| lifecycle state | partial | `lifecycle_id` identity projected for most signals; identity and true state are explicitly split | `lifecycle_at_signal_state/profile`, `monitor_template`, `entry_template`, `exit_template` are not emitted for most decisioned signals |
| Markov regime | implemented-derived / partial | `markov_scope`, `markov_regime`, `markov_bucket`, `markov_action` are now projected from decision event component/reason; dog/dud and EV buckets are output | confidence is not exported; classification is derived, not a first-class live Markov contract |
| filters / gates | implemented-partial | source gate, hard blockers, block cause, source/matrix/quote owner attribution are in rows and dog bucket audit | gate-level EV and blocked-dud cost still need friction/EV audit |
| matrix score | implemented-partial | matrix grade/score/RR/upside fields are projected; dog/dud matrix separability is reported | `matrix_profile` and lifecycle-profile-specific EV are missing |
| readiness | implemented-derived / partial | `readiness_seen`, `readiness_ok`, mode/data/route/risk state and blocker are now projected for every row; dog/dud readiness separability and EV buckets are output | readiness is derived from current decision/quote/blocker exports; live system does not yet emit an independent readiness contract |
| quote / route | implemented-derived / partial | `quote_route_verified`, `liquidity_verified`, `spread_verified`, `quote_age_verified`, `executable_quote_clean` are now projected; quote clean no longer hides liquidity/age gaps | route/quote evidence is still limited to current decision export fields; richer route source and actual route execution evidence are not yet exported |
| would_enter | implemented-partial | would_enter, first would_enter timing, dog/dud would_enter rate are projected | policy mode/reason/action/source event needs to be explicit; residual-upside/late-rate EV audit missing |
| entry | implemented-intent / partial-execution | `would_enter_not_entered` split exists: `policy_shadow_only`, `entry_guard_blocked_after_would_enter`, `would_enter_no_enqueue`; `a_class_live_enqueue` is projected as `entry_intent_*`; `execution_guard` is projected as `entry_guard_*`; fullnet can now ingest `paper_trades` as ledger-like entry evidence when present | true enqueue/order/fill events are still not first-class; current artifact predates the richer pull and still has ledger rows 0 |
| exit / hold | schema-only / blocked by no entry | held/captured is present as output; current entered=0 so exit cannot be evaluated | exit policy/timing/reason/hold/moonbag fields need real entry/ledger data |
| friction / slippage | implemented as missing-reason + EV audit skeleton / partial | every row now has friction fields or `friction_missing_reason`; `paper_trades` rows are normalized into realized PnL / peak quote PnL / spread-derived round-trip friction when present; `live-fullnet-ev-audit.json` blocks actual net EV when entry/fill/friction are absent | actual round-trip friction/slippage still requires a fresh evidence pull with `paper_trades`, or route simulation data if no paper trade exists |

### 12.2 What Has Been Written Into The Plan

The following required changes are now explicitly documented in this plan:

```text
1. 所有 11 个直接影响“抓金银狗”的模块必须入网。
2. raw label 只作为事后 denominator,不作为实时入场模块。
3. lifecycle identity 必须和 lifecycle state/profile 分开。
4. Markov 必须绑定 lifecycle scope,不能只用文本正则长期替代。
5. readiness 必须从 hard blockers / quote / source / matrix 中拆出来。
6. quote 必须拆成 route/liquidity/spread/age/executable quote。
7. would_enter_not_entered 必须拆成 enqueue/order/fill/ledger。
8. exit/hold 必须保留 schema,但在没有 entry 前不得评估策略优劣。
9. friction/slippage 必须作为 EV 的一等模块,不能只看 gross move。
10. repair owner 必须基于 dog/dud + net EV,不能只按 bucket count。
11. `a_class_live_enqueue` / `execution_guard` 必须作为 entry-intent / guard evidence,不得误记为真实 enqueue/order/fill。
```

### 12.3 What Is Already Implemented In The Current Generator

Already implemented and tested:

```text
source unique signal collapse
opportunity denominators: formal dog/dud, clean peak >=50/100/5x/10x
A-class API + canonical decision merge with id de-dup
per-signal exact/bounded decision matching
risk_json.quote_clean_verified contract
quote_clean top-level mismatch audit
lifecycle identity vs lifecycle_state split
Markov applicability projection (temporary text-derived)
structured Markov scope/regime/bucket/action projection (derived)
matrix grade/score projection
readiness projection (derived from decision/quote/blocker state)
quote/route executable split: route/liquidity/spread/age/executable_quote
entry_bridge_bucket split
entry_intent projection from a_class_live_enqueue without marking enqueue_seen
entry_guard projection from execution_guard without marking order/fill
local canonical exporter includes paper_trades and paper_decision_events
live puller prefers bounded canonical-ledger JSON (`hours + limit`) before falling back to legacy ledger/trades
fullnet ingests paper_trades as ledger-like entry evidence, with realized PnL and spread-derived friction
friction/slippage fields with explicit missing_reason
live-fullnet-ev-audit.json output
EV-aware repair board: blocks actionable repair owner when actual net EV is missing
dog bucket audit for quote_clean_no_would_enter and would_enter_not_entered
dog/dud component separability
layer coverage and projection completeness
```

Recent validation:

```text
node --check scripts/build-live-fullnet-row-report.js
node --check scripts/build-canonical-ledger-window-export.js
node --check scripts/pull-a-class-window-evidence.js
node --test tests/build-live-fullnet-row-report.test.mjs
node --test tests/build-canonical-ledger-window-export.test.mjs
node --test tests/pull-a-class-window-evidence.test.mjs
```

These pass as of this update.

Current online artifact was rebuilt after the fullnet generator / entry-intent / guard projection implementation:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-row.jsonl
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-summary.json
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-component-separability.json
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-dog-bucket-audit.json
/Users/boliu/sas-data-room/fullnet-evidence-pack-20260620T223502Z/fullnet-aligned-current/live-fullnet-ev-audit.json
```

Rebuild result:

```text
projection_complete=true
readiness projected for 357/357 rows; readiness_ok known true for 143/357
executable_quote projected for 357/357 rows; executable_quote_clean true for 143/357
formal dog quote_clean 34/37, but executable_quote_clean only 16/37
Markov buckets: green 27 signals, yellow 78, red 133, no/not-projected 119
entry_intent_seen 98/357 rows; formal dogs 12/37
entry_guard_seen 15/357 rows; formal dogs 4/37
enqueue_seen/order_seen/fill_seen/ledger_seen all 0
would_enter_not_entered dog split: policy_shadow_only 6, entry_guard_blocked_after_would_enter 2, would_enter_no_enqueue 4
repair_board.primary_repair_owner=ACTUAL_NET_EV_EVIDENCE_GAP
dog_chain_diagnostic_owner=SOURCE_GATE_BLOCKS_DOGS
formal dog actual_net_ev_pct=null, reason=no_entered_rows_no_actual_net_ev
```

Important caveat:

```text
This artifact predates the richer canonical-ledger puller path. It does not yet prove whether paper_trades / paper_decision_events would populate ledger-like entry evidence after a fresh pull. The code path is implemented; the online evidence pack still needs to be re-pulled after deployment.
```

Fresh online pull probe after the puller change:

```text
out_dir=/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T053829Z
events_pulled=12716
events_pages=26
pagination_supported=true
canonical_endpoint_status=502
evidence_source=legacy-ledger-trades
paper_trades=0
paper_decision_events=0
health_after=502
```

Interpretation:

```text
The before_id event stream is working. The live canonical-ledger endpoint is still not usable for the 24h evidence pack. It timed out / returned 502 even at small limits, which indicates the deployed endpoint is too heavy or not yet the bounded implementation. The dashboard patch now makes canonical-ledger windowed by hours/since_ts; that patch must be deployed before the fresh evidence pack can prove ledger/friction.
```

Patch location:

```text
/Users/boliu/sentiment-arbitrage-system/src/web/dashboard-server.js
/Users/boliu/sentiment-arbitrage-beforeid-main/src/web/dashboard-server.js
```

Both local dashboard files now expose bounded canonical-ledger export with:

```text
tables = canonical_trade_ledger, paper_trades, paper_decision_events, a_class_decision_events, paper_missed_signal_attribution
window = hours/since_ts + limit
ordering = id DESC when id exists, to avoid COALESCE full-table sort on large paper DBs
```

### 12.4 Not Yet Done, Blocking Full Sign-Off

The goal is not complete until these are done:

```text
1. Deploy the richer bounded canonical-ledger export (`hours/since_ts + limit`, no full-table sort) and re-pull the current evidence pack so paper_trades / paper_decision_events are present in the actual online artifact.
2. Export or ingest true enqueue/order/fill bridge events. paper_trades can prove ledger-like entry after the fact, but it is not the same as queue/order/fill-step telemetry.
3. Emit lifecycle_at_signal/profile/template fields from the live system.
4. Emit first-class structured Markov regime/bucket/action/confidence from the live system instead of relying on derived classification.
5. Export actual entry/fill/exit or route simulation data so friction/slippage can become actual net EV, not missing_reason.
6. Upgrade EV-aware repair board from “block when actual EV missing” to true dog/dud + net-EV owner ranking once actual EV exists.
7. Run multi-day comparable artifacts with the same schema.
8. Only then use repair-board output to decide which module should be tuned.
```

Current status remains:

```text
PLAN_WRITTEN_AND_PARTIALLY_IMPLEMENTED
FULLNET_MEASUREMENT_IMPROVING
CAPTURE_NOT_PROVEN
NO_EDGE_DECLARED
NO_LIVE_STRATEGY_CHANGE_AUTHORIZED
```

### 12.5 Deployment And Fresh-Pull Acceptance

The bounded canonical endpoint must be deployed before the next evidence pack can prove entry / ledger / friction.

Pre-deploy checks:

```bash
cd /Users/boliu/sentiment-arbitrage-system
node --check src/web/dashboard-server.js
git diff -- src/web/dashboard-server.js
```

Required endpoint behavior after deploy:

```text
GET /api/data/download/canonical-ledger?hours=24&limit=20000
must return HTTP 200 JSON, not 502 / timeout
must include tables.paper_trades
must include tables.paper_decision_events
must include tables.a_class_decision_events
must include since_ts/window metadata
must not download the full sqlite DB
```

Fresh evidence pull after deploy:

```bash
cd /Users/boliu/sas-research
OUT="/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-$(date -u +%Y%m%dT%H%M%SZ)"
FULLNET_AUTH="$DASHBOARD_TOKEN" \
  node scripts/pull-a-class-window-evidence.js \
    --hours 24 \
    --canonical-limit 20000 \
    --out-dir "$OUT"
```

Fresh pull acceptance:

```text
pagination_supported=true
events_pulled > 0
pull_stats.evidence_source=canonical-ledger
pull_stats.canonical_endpoint_ok=true
tables.paper_trades is present (count may be 0, but table must be available if live DB has it)
tables.paper_decision_events is present
health_before.ok=true
health_after.ok=true
```

Table-count check:

```bash
node - "$OUT/canonical-ledger-window.json" <<'NODE'
const fs = require('fs');
const p = process.argv[2];
const j = JSON.parse(fs.readFileSync(p, 'utf8'));
console.log(JSON.stringify({
  evidence_source: j.pull_stats?.evidence_source,
  canonical_ok: j.pull_stats?.canonical_endpoint_ok,
  counts: Object.fromEntries(Object.entries(j.tables || {}).map(([k, v]) => [k, Array.isArray(v.rows) ? v.rows.length : v.count || 0])),
}, null, 2));
NODE
```

Rebuild rule:

```text
Do not mix a fresh evidence pack with an older source/raw window unless their signal_ts windows overlap cleanly. Fullnet rebuild must use same-window source/raw/lifecycle snapshots plus the fresh evidence pack. If the windows are not aligned, output INPUT_NOT_ALIGNED / EVIDENCE_WINDOW_MISMATCH instead of producing a capture verdict.
```

Fullnet rebuild shape:

```bash
node scripts/build-live-fullnet-row-report.js \
  --source-to-raw "<same-window>/source-to-raw-rows.json" \
  --raw-discovery "<same-window>/raw-dog-discovery-24h.json" \
  --a-class-events "$OUT/a-class-events-24h-complete.json" \
  --ledger-export "$OUT/canonical-ledger-window.json" \
  --lifecycle-db "<same-window>/lifecycle_tracks.snapshot.db" \
  --out-dir "$OUT/fullnet-aligned-current"
```

Fullnet post-rebuild acceptance:

```text
projection_complete=true
paper_trades rows are either used as ledger-like entry evidence or explicitly absent with reason
paper_decision_events rows are visible to entry bridge projection
would_enter_not_entered split remains policy_shadow_only / entry_guard_blocked_after_would_enter / would_enter_no_enqueue / true enqueue/order/fill buckets
live-fullnet-ev-audit.json reports actual net EV only if entry/fill/friction evidence exists
no strategy/live/gate/entry/exit/size changes
```

### 12.6 Session 2026-06-21 (later) — deploy verified, then live dashboard crash; BLOCKED

详细记录见 `claudedocs/fullnet-evidence-pack-session-status-2026-06-21.md`。本节是计划追踪节的对应更新。

按 goal Step 1–6 的实测结果:

```text
Step 1 (verify canonical endpoint): PASS at session start.
  GET /api/data/download/canonical-ledger?hours=24&limit=50 -> HTTP 200 / 6.4s
  tables = canonical_trade_ledger, paper_trades, paper_decision_events, a_class_decision_events, paper_missed_signal_attribution (5-table bounded version IS deployed; not old 3-table).

Step 2 (fresh evidence pull): FAILED.
  before_id event pagination works (26 pages, ~12555 events, all 200).
  canonical evidence at limit=20000 aborted at 90s AND old puller crashed (no graceful fallback) -> empty out-dir.

ROOT CAUSE of dashboard outage:
  /api/data/download/canonical-ledger used boundedIntParam(url,'limit',10000,1,100000).
  limit=20000 -> SELECT * x 5 tables (incl big *_json cols) serializing ~20000 rows/table
  -> too heavy -> live dashboard overloaded/crashed -> ALL endpoints 502 for 25+ min
  (10 light probes, every 150s, no recovery; not self-healing -> needs manual Zeabur restart).

Step 3 (same-window source/raw): BLOCKED.
  Local disk ~100% (≈475-689MB free, fluctuating). Cannot download fresh source(114MB)+raw(120MB)
  snapshots for the aligned window -> INPUT_NOT_ALIGNED. No window mixing.

Step 4/5 (aligned rerun + audit): NOT EXECUTED (no fresh evidence + no aligned source/raw).

Step 6 (tests): PASS. node --check x3 + node --test 4 suites green.
```

In-scope fixes prepared this session (research tooling + instrumentation; NO strategy/gate/entry/exit/size change):

```text
1. scripts/pull-a-class-window-evidence.js fetchJson now catches timeout/abort/network errors
   and returns a non-ok result, so a canonical timeout falls back to legacy gracefully and
   does NOT crash the pull or discard already-pulled events. (tested)
2. Canonical endpoint HARD LIMIT CAP (the real fix for the crash), prepared in BOTH dashboards,
   syntax-checked, NOT deployed:
     sentiment-arbitrage-beforeid-main/src/web/dashboard-server.js  (-> origin/main -> Zeabur)
     sentiment-arbitrage-system/src/web/dashboard-server.js
   change: boundedIntParam(url,'limit',10000,1,100000) -> boundedIntParam(url,'limit',1000,1,2000)
   so a large limit can no longer serialize 5 tables x tens-of-thousands of rows and crash live.
   Complete a_class coverage still comes from the before_id-paginated /api/a-class/events.
```

Unblock conditions (require operator; not doable from this side):

```text
A. Manually restart the Zeabur dashboard service (it is down, 502, and not self-recovering),
   and deploy the canonical hard-limit-cap so a future high-limit pull cannot re-crash it.
B. Free real local disk space (the large consumers are in the live repo's kline caches/backups,
   outside the research scope) so fresh same-window source/raw snapshots can be downloaded.
```

Resume sequence once A+B done (safe-limit, no 20000):

```text
1. events (before_id) + canonical --canonical-limit 500   -> fresh evidence pack
2. lifecycle-tracks (2.8MB) + raw-dog-discovery + same-window source/raw snapshots
3. build-live-fullnet-row-report.js aligned rerun + live-fullnet-ev-audit.json
Expected honest outcome: paper_trades/ledger likely still ≈0 -> entered=0 -> capture NOT proven,
actual net EV = explicit absent reason. No edge.
```

Status remains:

```text
GOAL_NOT_COMPLETE
STEP1_DEPLOY_VERIFIED
LIVE_DASHBOARD_DOWN_502_NEEDS_MANUAL_RESTART
CANONICAL_ENDPOINT_UNSAFE_AT_HIGH_LIMIT__CAP_PATCH_PREPARED_NOT_DEPLOYED
DISK_FULL_BLOCKS_FRESH_SAME_WINDOW_SOURCE_RAW
CAPTURE_NOT_PROVEN
NO_EDGE_DECLARED
NO_LIVE_STRATEGY_CHANGE
```

---

## §12.7 RESOLVED — unblocked, aligned rerun complete, signed off (2026-06-21)

Both blockers cleared: operator restarted Zeabur (+ the limit-cap deploy), and the disk had enough
headroom for a one-time snapshot (no deletion needed; operator kept the 06-19 snapshot). The full
§12.6 resume sequence ran end-to-end with VALID canonical evidence.

### Deploy / infra
```text
- Limit-cap committed 70e7683f, pushed cfd04f44..70e7683f -> origin/main
  (boundedIntParam limit 10000/100000 -> 1000/2000 on /api/data/download/canonical-ledger).
- Operator restarted Zeabur; canonical endpoint now 200 (5.6s, 5 tables); health 200 before AND after the pull.
```

### Fresh evidence pack: sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z
```text
pull (--canonical-limit 500, SAFE; never 20000):
  pagination_supported=true  events_pulled=10050 (21 pages)  evidence_source=canonical-ledger
  canonical_endpoint_ok=true  health_before=200  health_after=200   (safe limit did NOT crash it)
  canonical tables: canonical_trade_ledger=0  paper_trades=0  (VERIFIED true reads, not fetch-fail)
                    paper_decision_events=500  a_class_decision_events=500  paper_missed_attr=500 (capped@500 => the 0s are real 0s)
same-window snapshots (24h: 2026-06-20T08:05Z .. 2026-06-21T08:05Z), integrity_check=ok:
  raw_signal_outcomes.snapshot.db 119M  sentiment_arb.snapshot.db 112M  lifecycle_tracks.snapshot.db 2.8M
  raw-dog-discovery-24h.json: 446 signals, raw_sustained_gold=14 + silver=4 = 18 raw dogs
  source-to-raw: 514 rows / 329 unique signals
```

### Aligned fullnet rerun: .../fullnet-aligned-current  (projection_complete=true)
```text
total_signals=329  by_class: dud=123 pending=177 dog=29
capture_chain(all): source_gate_pass=140 (42.6%) -> has_decision=278 (84.5%) -> quote_clean=261 (79.3%)
                    -> would_enter=77 (23.4%) -> ENTERED=0 -> held=0
dog_capture:        dog=29 -> would_enter=8 -> ENTERED=0 -> held=0
would_enter_not_entered=77 split: policy_shadow_only=61, no_decision_record=51(term),
                    would_enter_no_enqueue=13, decision_no_quote_clean=17, entry_guard_blocked_after_would_enter=3
dog would_enter_not_entered=8 breakdown: policy_shadow_only=5, would_enter_no_enqueue=2, entry_guard_blocked_after_would_enter=1
                    enqueue_seen=0  order_seen=0  fill_seen=0  ledger_seen=0
                    entry_guard_reasons: entry_edge_spread_too_high=1, post_spread_abort_memory=1
EV audit: actual_net_ev_pct=null  actual_net_ev_missing_reason="no_entered_rows_no_actual_net_ev"
          entered_n=0  actual_friction_required_for_actual_net_ev=true
          (only counterfactual upper-bound shown, explicitly flagged as ex-post denominator)
ledger layer: ledger_missing_reason="no_entry_no_ledger" for all 329 (explicit absence)
primary_repair_owner=ACTUAL_NET_EV_EVIDENCE_GAP
guardrails: do_not_change_strategy=true, no_gate_change, no_entry_exit_size_change, no_edge_verdict
```

### Tests
```text
node --check x4 OK (pull / row-report / canonical-export / source-raw-window)
node --test  4 suites / 4 pass / 0 fail
```

### Sign-off conclusion
The capture chain is now measured on VALID, same-window canonical evidence (not a crash artifact).
Decisive fact: **the live system entered 0 positions in this 24h window** — paper_trades=0 and
canonical_trade_ledger=0 are VERIFIED true reads (health 200 both sides, evidence_source=canonical-ledger;
the other capped tables prove the limit works), NOT the earlier fetch-failure zeros. Therefore:
- 77 would_enter (8 dogs) but 0 entered: the would_enter->entered link is severed in shadow/bridge
  (policy_shadow_only + would_enter_no_enqueue dominate; enqueue/order/fill/ledger all 0).
- No real entry/fill/friction exists, so actual net EV is correctly NOT computed — it is an explicit
  absent reason, not a number. Counterfactual upper-bounds shown only with an ex-post-denominator warning.
- Capture is NOT proven and NO edge is declared. Strategy/gate/entry/exit/size unchanged.

```text
GOAL_COMPLETE
ONLINE_CANONICAL_EVIDENCE_AVAILABLE (200, evidence_source=canonical-ledger, health 200/200)
FRESH_EVIDENCE_PULLED_SAFE_LIMIT_500 (events=10050, pagination_supported=true)
SAME_WINDOW_FULLNET_RERUN__PROJECTION_COMPLETE=TRUE (329 signals)
ENTRY_LEDGER_FRICTION_EV__EXPLICIT_ABSENT_REASON (entered=0, no_entry_no_ledger, no_entered_rows_no_actual_net_ev)
TESTS_PASS (check x4 + test 4/4)
DOCS_UPDATED
CAPTURE_NOT_PROVEN__ENTERED_0_VERIFIED_TRUE_ABSENCE
NO_EDGE_DECLARED
NO_LIVE_STRATEGY_CHANGE
```

---

## §13 NEXT GOAL — Filter / Entry Contribution Fullnet Audit

日期: 2026-06-21  
状态: next research goal, not started  
范围: research-only / measurement-only  

### §13.1 Goal statement

唯一目标:

```text
把现有系统里的 source filter / lifecycle / Markov / matrix / readiness / quote / entry mode / execution / exit / friction
全部放进同一张逐信号 fullnet 审计表里,判断它们到底有没有帮助 Telegram 信号抓到金银狗,
以及有没有把这种帮助转化为真实 entry / ledger / hold / net EV。
```

这不是 discovery goal,不找新特征,不调参数,不改 gate。  
这是 contribution audit: 判断现有板块谁真的贡献捕获能力,谁只是风控,谁只是挡路,谁没有有效数据。

核心业务问题:

```text
过去系统里那些 filter 和不同 entry mode 曾经看起来有胜率;
但它们是否真的能在 Telegram 信号流里提高金银狗捕获率、降低 dud 进入率、并产生真实正 EV?
```

必须同时回答四件事:

```text
1. 这个模块/entry mode 是否提高 dog 密度?
2. 它是否降低 dud 进入或坏样本进入?
3. 它是否真的把 would_enter 变成 entered / ledger?
4. 进入后扣掉 friction/slippage 是否仍有正 EV?
```

### §13.2 Locked current baseline

最新可审计 fresh pack:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/
```

当前同窗读数:

```text
329 signals
29 formal sustained dog
123 formal dud

dog chain:
29 dog -> 23 has_decision -> 21 quote_clean -> 8 would_enter -> 0 entered -> 0 held

would_enter dogs:
policy_shadow_only=5
would_enter_no_enqueue=2
entry_guard_blocked_after_would_enter=1

actual_net_ev=null
reason=no_entered_rows_no_actual_net_ev
```

当前单模块初读:

```text
baseline dog rate among formal dog+dud = 19.1%

source_gate_pass precision ~= 22.1%
Markov yellow/green precision ~= 29.0%
Markov green precision ~= 50.0%, but n=6 only
matrix A/STRONG_A precision ~= 20.0%
quote_clean precision ~= 17.9%
would_enter precision ~= 20.0%
entered precision = unavailable, entered=0
```

当前解释:

```text
Telegram source has dogs.
The net can see and score many of them.
The current chain does not capture them.
The strongest possible lead is Markov yellow/green, but sample is too small.
Matrix / would_enter currently do not materially improve dog density.
Entry / ledger is 0, so actual EV is unavailable.
```

### §13.3 Denominators

所有模块必须在同一批 denominator 上审计,不能只挑已入场样本。

Required denominators:

```text
all_source_signals
formal_sustained_dog      # strict gold/silver sustained label
formal_dud                # bronze/sub25 clean comparator
peak_50_clean             # clean opportunity denominator, >=50% wick/peak
peak_100_clean            # clean 2x opportunity denominator
peak_5x_clean             # clean large-dog denominator
peak_10x_clean            # clean extreme-dog denominator
pending_or_not_evaluable  # counted separately, not mixed into dog/dud precision
```

Formal dog remains an ex-post denominator only. It is never a real-time entry feature.

### §13.4 Modules that must enter the audit

Every module below must appear either with a real value or an explicit missing reason:

```text
signal metadata:
  signal_type, is_ath, source hard gate, narrative fields if available

lifecycle:
  lifecycle_id
  lifecycle_state
  lifecycle_route_profile
  monitor_template
  entry_template
  exit_template
  lifecycle_at_signal vs lifecycle_at_decision when available

Markov / revival:
  applicable
  evaluated
  scope
  regime
  bucket
  action
  reason
  confidence if exported

filters / gates:
  source gate
  hard blockers
  security / liquidity / mcap / top10 / age / scam gates when exported

matrix / RR:
  grade
  score
  normalized_mode
  expected_rr
  expected_upside_pct
  matrix dimension states

readiness:
  freshness
  budget
  route state
  risk state
  readiness blocker

quote / route:
  quote_clean_verified
  executable_quote_clean
  route source
  liquidity_usd
  spread_pct
  quote_age_sec

would_enter:
  would_enter
  would_enter_ts
  would_enter_sec_after_signal
  block reason when no would_enter

entry:
  entry_intent_seen
  entry_mode
  entry_template
  entry_guard_seen
  entry_guard_reason
  enqueue_seen
  order_seen
  fill_seen
  ledger_seen

exit / hold:
  exit_ts
  exit_reason
  time_held_sec
  peak_after_entry
  held_to_silver
  held_to_gold
  moonbag / runner if available

friction / slippage:
  entry_quote_price
  entry_fill_price
  exit_quote_price
  exit_fill_price
  spread_cost_pct
  slippage_pct
  fees_pct
  priority_fee_sol
  round_trip_friction_pct
  net_pnl_pct
```

### §13.5 Required row-level additions

Extend the fullnet row so each signal can explain both selection and failure.

Required new row fields:

```text
lifecycle_route_profile
lifecycle_entry_template
lifecycle_exit_template
lifecycle_route_profile_source
lifecycle_route_profile_missing_reason

final_entry_block_stage
final_entry_block_reason
final_entry_block_owner

execution_bridge_owner
execution_bridge_missing_reason

entry_mode
entry_template
entry_mode_source
entry_mode_missing_reason

filter_family
filter_result
filter_block_reason

module_contribution_flags:
  source_pass
  lifecycle_profile_known
  markov_yellow_or_green
  matrix_a_or_strong
  quote_clean
  executable_quote_clean
  readiness_ok
  would_enter
  entered
  held
```

### §13.6 Required reports

The goal must produce these outputs for every run:

```text
live-fullnet-row.jsonl
live-fullnet-summary.json
live-fullnet-component-separability.json
live-fullnet-component-synergy.json
live-fullnet-entry-mode-contribution.json
live-fullnet-filter-contribution.json
live-fullnet-dog-bucket-audit.json
live-fullnet-ev-audit.json
```

Report meanings:

```text
component-separability:
  single-module dog/dud separation.

component-synergy:
  combinations such as Markov+matrix+quote, source+matrix+quote, would_enter+Markov.

entry-mode-contribution:
  per entry mode: dog pass, dud pass, would_enter, entered, held, actual EV, win rate, friction.

filter-contribution:
  per filter/gate: dogs blocked, duds blocked, dogs passed, duds passed, opportunity lost, protection value.

dog-bucket-audit:
  per dog: exact layer where it leaked.

EV audit:
  actual EV only from real ledger/fill/friction.
  counterfactual opportunity fields must remain explicitly labelled as ex-post ceilings.
```

### §13.7 Metrics and formulas

Every module and combination must report:

```text
dog_pass_n / dog_n
dud_pass_n / dud_n
dog_precision = dog_pass_n / (dog_pass_n + dud_pass_n)
dog_recall = dog_pass_n / dog_n
lift_vs_baseline = dog_precision - baseline_dog_rate
would_enter_rate
entered_rate
held_rate
actual_net_ev_pct
actual_win_rate
avg_round_trip_friction_pct
missing_reason if EV unavailable
```

Interpretation:

```text
Alpha / selection modules should raise dog_precision above baseline.
Risk / execution modules may not raise dog_precision, but must not systematically block dogs while letting duds through.
Entry mode is useful only if it both admits dogs and produces ledger/EV.
```

### §13.8 Phase plan

#### Phase 1 — Schema completion

Implement the row-level additions in §13.5.

Acceptance:

```text
projection_complete=true
all modules have value or missing_reason
lifecycle identity is not confused with lifecycle state/profile
entry intent is not counted as actual enqueue/order/fill
ledger entry is the only proof of entered
```

#### Phase 2 — Entry/filter contribution audit

Build the entry-mode and filter contribution reports.

Acceptance:

```text
each entry mode has dog/dud pass rates
each entry mode has would_enter / entered / held / EV fields
each filter has dogs_blocked and duds_blocked
old entry-mode win-rate claims are re-read only under this denominator
```

#### Phase 3 — Synergy audit

Build combination-level contribution.

Minimum combinations:

```text
source
Markov yellow/green
Markov green
matrix A/STRONG_A
quote_clean
source + Markov
Markov + matrix
Markov + quote
matrix + quote
source + Markov + matrix + quote
would_enter
would_enter + Markov
would_enter + quote
would_enter + source + quote
entered
```

Acceptance:

```text
report shows n, dog_n, dud_n, dog_precision, dog_recall, actual EV when available
small-n warnings are mandatory when dog_n < 30 or combination_n < 20
```

#### Phase 4 — Multi-day accumulation

Run the same reports for at least 3-5 comparable daily windows.

Acceptance:

```text
same schema version across days
same denominator definitions
same source/raw/evidence window alignment checks
daily artifacts stored in sas-data-room
multi-day summary shows stable or unstable module contribution
```

#### Phase 5 — Decision verdict

Only after Phase 4 may the goal emit one of these verdicts:

```text
MEASUREMENT_GAP:
  required module values or ledger/friction are still missing.

NO_STABLE_SELECTION_SIGNAL:
  no module or combination consistently improves dog_precision over baseline.

ENTRY_BRIDGE_GAP:
  would_enter exists but entered/ledger remains zero or near-zero.

RISK_FILTER_OVERBLOCK:
  filter blocks dogs at equal/higher rate than duds with no EV proof.

ENTRY_MODE_CANDIDATE:
  one entry mode shows stable dog enrichment and non-negative actual EV in paper/shadow ledger.

SHADOW_REPAIR_READY:
  a repair can be specified, but only shadow-only; no live permission.
```

### §13.9 Stop rules

Stop and do not tune strategy if:

```text
entered_n = 0 across the evaluation window
actual_net_ev_pct remains null
ledger/friction missing
dog_n too small for stable inference after 3-5 days
module improvement exists only in ex-post counterfactual fields
module improves dog precision but increases dud entry/negative EV
```

### §13.10 Hard prohibitions

```text
Do not change live gate.
Do not change live matrix.
Do not change entry/exit/size.
Do not deploy strategy changes.
Do not declare edge.
Do not use raw dog label as an entry feature.
Do not use post-signal peak/timing as a real-time input.
Do not count would_enter as captured.
Do not count entry_intent as enqueue/order/fill.
Do not calculate actual EV without real entry/fill/friction evidence.
Do not compare multi-day repair impact across different denominator or schema versions.
```

### §13.11 Expected answer from this goal

This goal should end with a concrete map:

```text
Which filters protect without killing dogs?
Which filters overblock dogs?
Which lifecycle profiles actually route dogs?
Whether Markov yellow/green is a stable dog-enrichment signal or one-day noise.
Whether matrix score adds value beyond Markov/source.
Which entry modes, if any, convert dogs into ledger entries.
Whether any entered cohort has positive EV after friction.
```

If no such module exists:

```text
Current system has measurement value but no proven capture edge.
The correct business posture remains shadow-only / measurement-first.
```

If a module exists:

```text
Write a separate shadow-only repair spec.
Run repair in paper/shadow only.
Re-test dog/dud and EV before any live discussion.
```

### §13.12 Definition of done

This goal is complete only when all are true:

```text
1. fullnet row schema includes every module in §13.4.
2. entry-mode contribution report exists.
3. filter contribution report exists.
4. synergy report exists.
5. EV audit remains fail-closed when entered/friction is absent.
6. at least one fresh daily run produces all reports.
7. multi-day runner / accumulation path is documented.
8. final verdict is one of §13.8 Phase 5 verdicts.
9. no strategy/live files are modified.
10. tests pass.
```

Current next action:

```text
Implement Phase 1-3 in scripts/build-live-fullnet-row-report.js and tests/build-live-fullnet-row-report.test.mjs.
Rerun against /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/.
Then start Phase 4 daily accumulation.
```

---

## §14 FIRST SAME-DAY READOUT — available-data rerun (2026-06-21)

This section records the first run against the currently available fresh evidence pack. It is **readout-only** and does not authorize any live strategy, gate, entry, exit, size, or matrix change.

Input pack:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/
```

Rerun output:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-rerun-20260621T134214Z/
```

Generated artifacts:

```text
live-fullnet-row.jsonl
live-fullnet-summary.json
live-fullnet-component-separability.json
live-fullnet-dog-bucket-audit.json
live-fullnet-ev-audit.json
```

Validation:

```text
node --test tests/build-live-fullnet-row-report.test.mjs  PASS
projection_complete = true
research_only = true
actual_net_ev = null when entered_n = 0
```

### §14.1 Denominators

```text
total_signals: 329
formal_sustained_dog: 29
formal_dud: 123
pending: 177
raw_peak_50_any: 102
raw_peak_50_clean: 36
raw_peak_100_clean: 25
raw_peak_5x_any: 13
raw_peak_5x_clean: 5
raw_peak_10x_clean: 2
```

Interpretation:

```text
formal_sustained_dog remains the strict success denominator.
raw_peak_50_clean remains the broader opportunity denominator.
raw_peak_5x/10x clean remain big-dog denominators.
formal_dud remains the control denominator.
All are ex-post denominators, not entry features.
```

### §14.2 Formal dog capture chain

```text
29 formal dog
→ 23 has_decision
→ 21 quote_clean
→ 8 would_enter
→ 0 entered
→ 0 held/captured
```

Terminal buckets:

```text
no_decision_record: 6
has_decision_no_quote_clean: 2
quote_clean_no_would_enter: 13
would_enter_not_entered: 8
```

Entry bridge buckets:

```text
no_decision_record: 6
policy_shadow_only: 5
quote_clean_no_would_enter: 13
would_enter_no_enqueue: 2
entry_guard_blocked_after_would_enter: 1
decision_no_quote_clean: 2
```

### §14.3 Main current blockers

The two current blockers are:

```text
1. quote_clean_no_would_enter:
   13 formal dogs had clean quote evidence but were not allowed into would_enter.

2. would_enter_not_entered:
   8 formal dogs reached would_enter but never became enqueue/order/fill/ledger.
```

Breakdown for quote_clean_no_would_enter dogs:

```text
blocker owners:
  SOURCE_GATE: 7
  QUOTE: 5
  MATRIX: 1

matrix grades:
  REJECT: 13

hard blockers:
  liquidity_unknown: 10
  spread_extreme: 3
  quote_age_unknown: 2
  liquidity_below_min: 1

block causes:
  hard_prefilter_failed: 12
  opportunity_matrix_red_cell: 1
```

Breakdown for would_enter_not_entered dogs:

```text
policy_shadow_only: 5
would_enter_no_enqueue: 2
entry_guard_blocked_after_would_enter: 1
enqueue_seen: 0
order_seen: 0
fill_seen: 0
ledger_seen: 0
median_would_enter_sec_after_signal: 5176.4
median_time_to_sustained_peak_sec: 3212.4
```

Interpretation:

```text
Some dogs are detected by the decision layer, but current entry plumbing does not convert would_enter into actual paper/live ledger rows.
would_enter is not capture.
entry/ledger remains the hard boundary for any EV claim.
```

### §14.4 Component readout

Dog-vs-dud component separation in this run:

```text
source_gate_pass:
  dog 51.7% vs dud 43.1%  small positive enrichment

lifecycle_identity_known:
  dog 82.8% vs dud 80.5%  little separation

lifecycle_state_known:
  dog 3.4% vs dud 5.7%  not usable yet

lifecycle_route_profile_known:
  dog 0% vs dud 0%  missing; cannot audit route-profile contribution yet

markov_green:
  dog 10.3% vs dud 2.4%  small positive, low n

markov_yellow_or_green:
  dog 31.0% vs dud 17.9%  strongest current positive readout, still one-day only

matrix_grade_a_plus:
  dog 27.6% vs dud 26.0%  near-zero separation

quote_clean:
  dog 72.4% vs dud 78.0%  not dog-enriching

executable_quote_clean:
  dog 31.0% vs dud 35.8%  not dog-enriching

readiness_ok:
  dog 31.0% vs dud 35.8%  not dog-enriching

would_enter:
  dog 27.6% vs dud 26.0%  near-zero separation

entered:
  dog 0% vs dud 0%  no entry evidence

captured_held:
  dog 0% vs dud 0%  no capture evidence
```

Current one-day interpretation:

```text
Markov yellow/green is the only visible positive module-level signal in this run.
Matrix/would_enter barely improves dog-vs-dud separation.
Quote/readiness are not dog-enriching.
Lifecycle route-profile is not yet projected, so lifecycle cannot yet be judged as a route selector.
Entry/ledger is zero, so no actual EV can be calculated.
```

### §14.5 EV status

```text
actual_net_ev: null
actual_net_ev_missing_reason: no_entered_rows_no_actual_net_ev
entered_n: 0
held_n: 0
actual_friction: unavailable because no entry/fill rows exist
```

Counterfactual opportunity values remain ex-post upper bounds only:

```text
They must not be read as strategy EV.
They must not authorize a live change.
They can only identify where missed opportunity sits in the net.
```

### §14.6 Current next repair questions

Based on this same-day rerun, the next implementation/audit work should answer:

```text
1. Why do 13 quote-clean formal dogs remain matrix/source/quote blocked?
2. Why do 8 would_enter formal dogs not produce enqueue/order/fill/ledger evidence?
3. Can lifecycle_state and lifecycle_route_profile be projected for every signal?
4. Is Markov yellow/green stable across 3-5 comparable days, or just one-day noise?
5. Does any module or module combination improve dog precision and actual EV once entries exist?
```

No strategy action is allowed from this one-day readout.

---

## §13.13 IMPLEMENTED — Filter/Entry Contribution Audit complete + signed off (2026-06-21)

Phases 1-3 implemented in `scripts/build-live-fullnet-row-report.js`; Phase-4 tooling added;
Phase-5 verdict emitted. Research-only; `sentiment-arbitrage-system/src` untouched this goal.

### What was added
```text
Schema (Phase 1): wired previously-dead helpers into every row + new fields, projection_complete=true:
  lifecycle_route_profile / lifecycle_entry_template / lifecycle_exit_template /
    lifecycle_route_profile_source / lifecycle_route_profile_known / lifecycle_route_profile_missing_reason
  entry_mode / entry_template / entry_exit_template / entry_mode_source / entry_mode_missing_reason
    (entry_mode read from decision normalized_mode, NOT from raw label; entry_intent != enqueue/order/fill)
  filter_family / filter_result / filter_block_reason
  final_entry_block_stage / final_entry_block_reason / final_entry_block_owner
  execution_bridge_owner / execution_bridge_missing_reason
  module_contribution_flags { source_pass, lifecycle_profile_known, markov_yellow_or_green,
    matrix_a_or_strong, quote_clean, executable_quote_clean, readiness_ok, would_enter, entered, held }
Reports (Phase 2/3): now writes all 8 §13.6 files, incl. 3 new:
  live-fullnet-component-synergy.json       (lift_vs_baseline + mandatory small-n warnings)
  live-fullnet-entry-mode-contribution.json (per mode: dog/dud precision, would_enter/entered/held, EV, win rate, friction)
  live-fullnet-filter-contribution.json     (per gate: dogs/duds blocked+passed, opportunity lost, protection vs overblock)
Phase 4 tooling:
  scripts/run-fullnet-daily-audit.sh         (safe daily runner; canonical limit hard-capped <=2000, default 500)
                                               requires explicit DASHBOARD_TOKEN; no embedded/default token
  scripts/build-fullnet-multiday-summary.js  (cross-day stability; schema-version guard; fail-closed verdict ladder)
Phase 5: phase5_verdict in summary + CLI; single-day is fail-closed (stability verdicts need multi-day).
```

### §13.12 Definition of Done — all 10 met
```text
1. row schema includes every §13.4 module .......... YES (projection_complete=true, value-or-missing_reason)
2. entry-mode contribution report .................. YES (live-fullnet-entry-mode-contribution.json)
3. filter contribution report ...................... YES (live-fullnet-filter-contribution.json)
4. synergy report .................................. YES (live-fullnet-component-synergy.json)
5. EV audit fail-closed when entered/friction absent  YES (actual_net_ev_missing_reason=no_entered_rows_no_actual_net_ev)
6. >=1 fresh daily run produces all reports ........ YES (8 reports in fresh-20260621T123643Z/fullnet-aligned-current)
7. multi-day runner / accumulation path documented   YES (runner + multiday summary + §13.13/§14.7)
8. final verdict is one of §13.8 Phase-5 verdicts .. YES (ENTRY_BRIDGE_GAP)
9. no strategy/live files modified ................. YES (sas-research scripts/tests/docs only)
10. tests pass ..................................... YES (node --check x5 + node --test 5 suites / 5 pass)
```

---

## §14.7 CONTRIBUTION READOUT — first run on fresh-20260621T123643Z

All on the formal dog+dud denominator (dog_n=29, dud_n=123, baseline_dog_rate=0.191). EV fail-closed (entered=0).

### Entry-mode contribution
```text
a_grade_resonance_fastlane: n=278 dog=23 dud=98 precision=0.190 lift~=0.000 would_enter=77 entered=0 EV=null
no_decision_event_for_signal: n=51  dog=6  dud=25 precision=0.194 (no decision -> no entry)
=> the single active entry mode does NOT enrich dogs over baseline and converts 0 -> entry. No EV.
```

### Filter contribution (protection vs over-block)
```text
source_gate  : dogs_blocked 14/29 (0.483) vs duds 70/123 (0.569)  protective(+0.086)  but opp_lost_avg ~368%
matrix_reject: dogs_blocked 15/29 (0.517) vs duds 66/123 (0.537)  protective(+0.019)  large dog cost
liquidity    : dogs 0.448 vs duds 0.439   OVERBLOCKS dogs (-0.009)
spread       : dogs 0.172 vs duds 0.130   OVERBLOCKS dogs (-0.042)
quote_age    : dogs 0.138 vs duds 0.098   OVERBLOCKS dogs (-0.040)
security/mcap/holders: 0/0 (not exported in hard_blockers -> cannot audit yet)
=> liquidity/spread/quote_age remove dogs at >= dud rate; source/matrix protect but still cost many dogs.
```

### Synergy (dog enrichment, all small-n on this 1 day)
```text
markov_green ............... prec 0.500 lift +0.309  n=6   [dog_n_lt_10, combination_n_lt_20]
markovYG+matrixA .......... prec 0.304 lift +0.114  n=23  [dog_n_lt_10]
would_enter+markovYG ...... prec 0.304 lift +0.114  n=23  [dog_n_lt_10]
markov_yellow_or_green .... prec 0.290 lift +0.100  n=31  [dog_n_lt_10]
source_pass ............... prec 0.221 lift +0.030  n=68  (only lead with dog_n>=10 & n>=20)
quote_clean ............... prec 0.179 lift -0.011  (not enriching)
would_enter ............... prec 0.200 lift +0.009  (~baseline)
=> Markov yellow/green is the only material dog-enrichment signal, but every Markov lead is small-n.
   quote/would_enter do not separate dogs. Stability requires Phase-4 multi-day.
```

### Phase-5 verdict
```text
ENTRY_BRIDGE_GAP
  evidence: would_enter_n=77 but entered_n=0; no enqueue/order/fill/ledger and no actual EV
  strongest_single_day_lead (conservative, dog_n>=10 & n>=20): source_pass, lift +0.030
  requires_phase4_multiday_for: NO_STABLE_SELECTION_SIGNAL / ENTRY_MODE_CANDIDATE / RISK_FILTER_OVERBLOCK / SHADOW_REPAIR_READY
```

### Closed-loop answer to §13.11
```text
- Filters that protect without killing dogs: source_gate, matrix (mildly; both still cost dogs).
- Filters that overblock dogs: liquidity, spread, quote_age (block dogs at >= dud rate).
- Lifecycle route profiles now projected for every signal (was 0%); route-profile *separation* still
  needs lifecycle_state, which remains ~4% covered -> measurement gap.
- Markov yellow/green is a candidate dog-enrichment signal but is one-day, small-n -> needs 3-5 days.
- Matrix adds little beyond Markov/source; quote/would_enter add nothing on dog precision.
- Entry modes converting dogs into ledger entries: NONE (entered=0).
- Positive EV after friction: UNPROVEN (no real entries) -> fail-closed.
Business posture stays shadow-only / measurement-first. No edge declared. No live change.
```

```text
GOAL_COMPLETE
PHASE_1_SCHEMA_COMPLETE (projection_complete=true)
PHASE_2_ENTRY_MODE_AND_FILTER_CONTRIBUTION_DONE
PHASE_3_SYNERGY_DONE (lift + mandatory small-n warnings)
PHASE_4_MULTIDAY_PATH_READY (runner + stability aggregator + schema guard)
PHASE_5_VERDICT=ENTRY_BRIDGE_GAP
EV_FAIL_CLOSED (no_entered_rows_no_actual_net_ev)
TESTS_PASS (check x5 + test 5/5)
NO_EDGE_DECLARED
NO_LIVE_STRATEGY_CHANGE
```

---

## §14.8 HISTORICAL ENTRY-MODE RECONCILIATION — do not drop the early positive paths

User reminder: earlier work found that some fast/entry paths were positive. This is correct and must remain in the plan. The current §13 result (`entered=0`, `actual_ev=null`) must **not** be misread as "entry models have no value".

Historical artifact:

```text
/Users/boliu/sas-data-room/buy-side-net-contribution-0609-0611-fullfields-20260619T044520Z/buy-side-net-contribution-report.json
/Users/boliu/sas-research/claudedocs/buy-side-net-contribution-readout-2026-06-19.md
```

Historical denominator:

```text
window: 2026-06-09T03:57:09Z -> 2026-06-11T15:06:01Z
raw_selection_unit: token_ca_signal_ts
dog_n: 51
dud_n: 203
rows_n: 254
replay: historical same-path bars, TP/SL, same-bar SL first
main view: TP +100 / SL -30 / friction 5%
```

The positive historical rows were:

```text
60s would_enter:
  n=23 dog=10 dud=13 dog_rate=43.48%
  avg_net_return=+11.3849%
  EV lift vs blind=+23.7393pp

60s a_class_fastlane_pass:
  n=21 dog=9 dud=12 dog_rate=42.86%
  avg_net_return=+9.6121%
  EV lift vs blind=+21.9665pp

60s A/STRONG_A:
  n=22 dog=9 dud=13 dog_rate=40.91%
  avg_net_return=+7.5843%
  EV lift vs blind=+19.9387pp

300s A/STRONG_A:
  n=41 dog=16 dud=25 dog_rate=39.02%
  avg_net_return=+10.5878%
  EV lift vs blind=+17.5959pp

300s a_class_fastlane_pass:
  n=40 dog=16 dud=24 dog_rate=40.00%
  avg_net_return=+8.4775%
  EV lift vs blind=+15.4856pp

300s would_enter:
  n=45 dog=16 dud=29 dog_rate=35.56%
  avg_net_return=+4.8648%
  EV lift vs blind=+11.8729pp
```

Important reconciliation:

```text
These are real positive historical replay findings.
They are NOT actual ledger EV.
They are NOT live-ready.
They were computed from historical bars with modeled friction, not real entry/fill/friction.
They show that some early buy-side components had signal/value versus blind-all.
```

Therefore the correct current interpretation is:

```text
Do NOT say: entry models are useless.
Do say: early entry components had positive counterfactual value, but the live/current chain has not converted them into entered/ledger/held trades.
```

This directly changes the repair priority. The main contradiction is now:

```text
Historical replay says early would_enter / fastlane / A-grade paths can be positive.
Current fullnet says would_enter exists but entered=0 and actual EV=null.
So the repair target is the bridge from positive early component -> actual enqueue/order/fill/ledger/exit, not a new feature hunt.
```

Required next addition to §13 Phase 4/5:

```text
Add daily buy-side replay reconciliation, or adapt build-buy-side-net-contribution-report.js to consume fullnet rows/current same-window artifacts.
Each daily audit should report both:
  1. actual ledger EV (fail-closed if entered=0)
  2. early-component TP/SL replay for 60s/300s: would_enter, fastlane_pass, A/STRONG_A, MarkovYG combinations

Then compare:
  historical/counterfactual positive path present? yes/no
  actual entry/ledger realized? yes/no
  if replay positive but ledger absent -> ENTRY_BRIDGE_GAP / EXECUTION_BRIDGE_REPAIR
  if replay no longer positive across 3-5 days -> NO_STABLE_SELECTION_SIGNAL
```

Hard guardrail:

```text
A positive replay row can prioritize a shadow/paper repair, but it cannot authorize live changes.
Only real entry/fill/friction/exit rows can produce actual EV.
```

Updated working verdict:

```text
EARLY_ENTRY_COMPONENT_SIGNAL_EXISTS_HISTORICALLY
CURRENT_ACTUAL_CAPTURE_NOT_CONNECTED
PRIMARY_NEXT_REPAIR = bridge historical-positive early components into shadow/paper entry evidence, then measure actual EV.
```

---

## §14.9 OLDER ENTRY/PATH EVIDENCE INVENTORY — likely source of the "three effective paths" memory

User clarification: the §14.8 buy-side replay may not be the older result remembered. The older work appears to contain several distinct "split into small buckets / modes" findings. They must be reconciled separately, not collapsed into one claim.

### Historical evidence intake rule — do not promote old records without recheck

Any old note, backtest, shadow result, or user memory must be classified before it can affect the current fullnet plan:

```text
INVALID / DO_NOT_USE:
  Known wrong calculation, polluted accounting, unit bug, pre-fix ledger, or user-disconfirmed result.
  Keep only as provenance so it is not rediscovered and reused.

PROMISING_BUT_UNVERIFIED:
  Positive historical row, but not yet checked against current same-window fullnet.
  It may be listed as a candidate field/mode, but cannot drive repair priority or live/gate changes.

CURRENT_FULLNET_CANDIDATE:
  Re-projected into current same-window rows with dog/dud denominator, would_enter, entered, held,
  actual fill/ledger/friction/EV where available, and clear missing_reason where unavailable.

REPAIR_CANDIDATE:
  Current same-window evidence shows dog enrichment or EV/capture contribution, with enough n or
  explicit small-n warning, and the effect is not just a stale historical regime artifact.
```

Required recheck before adding any old item beyond provenance:

```text
1. Identify source artifact and date range.
2. Confirm it is not from known-bad MC/shadow accounting or pre-Apr-4 polluted paper trades.
3. Confirm whether the metric is:
   - backtest replay,
   - paper fill,
   - live fill,
   - raw dog timing,
   - or just mode/infrastructure existence.
4. Confirm denominator: formal dog, peak>=50 clean, peak>=5x/10x clean, dud, or all signals.
5. Confirm time legality: pre-signal / entry-bar / post-signal / ex-post outcome.
6. Confirm unit and friction assumptions.
7. Only then add the item as INVALID, PROMISING_BUT_UNVERIFIED, or CURRENT_FULLNET_CANDIDATE.
```

### A. Early shadow MC buckets — INVALID / do not use

Evidence:

```text
/Users/boliu/sas-research/docs/shadow-mode-analysis.md
/Users/boliu/sas-research/docs/shadow-data-locations.md
```

Recorded old shadow-pnl summary, preserved only for provenance:

```text
304 closed shadow trades
overall win rate = 53.3% (162 / 304)
average PnL = +19.89%

MC 0-10K:   61 trades, 75.4% win rate, +50.5% avg PnL
MC 10-20K: 131 trades, 53.4% win rate, +17.8% avg PnL
MC 20-30K:  49 trades, 42.9% win rate, +10.3% avg PnL
MC 30-50K:  15 trades, 60.0% win rate, +17.5% avg PnL
MC 50K+:    48 trades, 33.3% win rate, -2.7% avg PnL
```

Interpretation:

```text
User correction: this MC-bucket result was a false / wrong calculation from the old shadow accounting.
It must NOT be used as evidence for "three effective paths".
It must NOT be used for current repair priority, feature selection, gate changes, or live conclusions.
Keep it only as an invalidated historical artifact so it is not rediscovered and mistakenly reused.
```

### B. NOT_ATH kline scoring — three entry-bar dimensions, current filter was inverted

Status:

```text
PROMISING_BUT_UNVERIFIED.
Verified in this audit only as a historical artifact with source files and transcribed numbers.
NOT yet verified as a current fullnet effect, not yet current same-window dog/dud re-projected,
and not authorized as a gate / entry / live rule.
```

Evidence:

```text
/Users/boliu/sas-research/factor_findings.md
/Users/boliu/sas-research/scripts/cross_validation_notath.py
/Users/boliu/sas-research/scripts/test_proposed_scoring.py
```

Recorded finding:

```text
129 valid NOT_ATH signals (Mar 7-19)
old deployed score >=2:
  n=86, win rate=17.4%, EV=+14.3%, SL=82.6%
old rejected score <2:
  n=43, win rate=53.5%, EV=+54.7%

positive dimensions:
  RED bar:        n=72, win rate=33%, EV=+40%
  LOW volume:     n=77, win rate=34%, EV=+35%
  Active |mom|>20: n=18, win rate=50%, EV=+88%

entry volume quintiles were monotonic:
  Q1 EV=+10.7%, WR=55%
  Q2 EV=+13.1%, WR=69%
  Q3 EV=+17.3%, WR=81%
  Q4 EV=+20.9%, WR=86%
  Q5 EV=+23.5%, WR=85%
```

Interpretation:

```text
This is probably another memory source: signals were split into small entry-bar regimes and several were positive.
But the artifact itself warns: p=0.95 Monte Carlo, only 129 valid samples, large IS/OOS drop, possible entry-bar look-ahead / bar-alignment risk.
It is a hypothesis to re-project into current fullnet, not a live-ready rule.
```

### B2. NOT_ATH smart backtest / debate artifacts — likely source of the "68% win rate" memory

Status:

```text
PROMISING_BUT_UNVERIFIED.
Verified in this audit only as historical smart_backtest / debate rows.
The 68% row was located and transcribed, but it is a walk-forward day / regime bucket,
not a proven current paper/live fill win rate.
Must be re-projected into current fullnet before it can affect repair priority.
```

Evidence:

```text
/Users/boliu/sas-research/smart_backtest/smart_backtest_results.json
/Users/boliu/sas-research/debate_agent_a.json
/Users/boliu/sas-research/debate_agent_b.json
/Users/boliu/sas-research/debate_agent_c.json
```

Recorded finding:

```text
smart_backtest baseline:
  n=463, WR=76.0%, EV=+15.5%, Sharpe=0.799

walk-forward / NW-forward daily tests:
  2026-03-15: n=81,  WR=86.4%, EV=+19.5%
  2026-03-16: n=43,  WR=74.4%, EV=+13.4%
  2026-03-17: n=84,  WR=65.5%, EV=+13.6%
  2026-03-18: n=98,  WR=76.5%, EV=+14.6%
  2026-03-19: n=25,  WR=68.0%, EV=+16.8%

debate_agent_b daily / regime rows:
  2026-03-19 / Thu: n=25, WR=68.0%, EV=+16.18%
  hour=14: n=22, WR=68.2%, EV=+14.78%
```

Additional entry-bar / kline factors from debate artifacts:

```text
debate_agent_a:
  first_bar_return >=0%:
    n=195, kept=43.5%, EV=+28.2%, WR=95.9%
  first_bar_return >=2%:
    n=174, kept=38.8%, EV=+30.8%, WR=99.4%
  rejected first_bar_return <0:
    n=253, EV=+5.86%, WR=60.5%

  first_bar_return quintiles:
    Q1 EV=+5.8%,  WR=58.4%
    Q5 EV=+41.9%, WR=100.0%

  vol_accel quintiles:
    Q1 EV=+11.3%, WR=63.2%
    Q4 EV=+18.1%, WR=86.2%
    Q5 EV=+16.2%, WR=74.7%

debate_agent_c:
  strong_bear / red-style candle:
    n=68, EV=+17.6%, WR=77.9%
  volume profile building:
    n=30, EV=+19.0%, WR=83.3%
  volume profile declining:
    n=29, EV=+16.4%, WR=86.2%
```

Interpretation:

```text
This is probably the cleanest source of the user's remembered "68%": it is a specific walk-forward day / regime bucket, not the overall result.
The overall smart_backtest result was higher (WR~76%, EV~15.5%), but the same debate pack flags overfitting risk 9/10 and "needs more data".

Important distinction:
  factor_findings' RED/low-volume/active rule and smart_backtest's first-bar-return / volume-accel factors are related entry-bar/kline families,
  but they are not the same rule and must not be merged into one unverified claim.

Fullnet implication:
  project all of these as separate candidate fields:
    red_bar, low_volume, active_mom, first_bar_return, first_bar_return_bucket,
    vol_accel, vol_accel_bucket, entry_body_ratio, candle_pattern, volume_profile.
  Then judge them on current same-window dog/dud + would_enter/entered/held + actual EV.
```

### C. March router / sleeve research — anchor, continuation, path-context / soft-context families

Status:

```text
PROMISING_BUT_UNVERIFIED / SHADOW_ONLY.
Preserved as historical router evidence, with later weakening noted.
Not re-verified in current same-window fullnet and not a live promotion candidate.
```

Evidence:

```text
/Users/boliu/.openclaw/workspace-autotrader/research_reports/state_aware_execution_aware_dual_sleeve_competition_overlay_20260309T033053Z.md
/Users/boliu/.openclaw/workspace-autotrader/research_reports/cross_family_path_context_router_20260311T231133Z.md
/Users/boliu/.openclaw/workspace-autotrader/research_reports/real_kline_router_family_validation_20260311T231830Z.md
/Users/boliu/.openclaw/workspace-autotrader/research_reports/updated_surface_router_common_anchor_audit_20260312T132215Z.md
```

Positive-looking rows:

```text
state-aware dual sleeve best overlay:
  realistic ending=3.107474 SOL, delta vs anchor=+1.182352
  stressed ending=1.995285 SOL, delta vs anchor=+0.594396
  anchor sleeve: 8 trades, 4 wins, +1.566110 SOL
  continuation sleeve: 4 trades, 3 wins, +0.541364 SOL

cross-family path-context router:
  families = anchor + continuation + path_context
  Mar 11 report median held-out delta vs anchor:
    realistic +0.123606 SOL
    stressed +0.144741 SOL

real-kline router family validation:
  path-context router full real-candle ending=2.276060 SOL
  delta vs anchor subset=+1.508642 SOL
  real family mix = anchor 6, continuation 4, path_context 1
```

Later weakening / guardrail:

```text
updated common-anchor audit:
  held-out median delta vs fixed common anchor collapsed to +0.000000 SOL
  conclusion: router can remain a shadow branch, but not a stronger promotion candidate.
```

Interpretation:

```text
This likely matches "three paths": anchor, continuation, and path_context/soft_context router sleeves.
The positive evidence was real in older real-candle/router replays, but later fixed-comparator held-out audit weakened promotion confidence.
It should be revived only as shadow/fullnet projection, not as live authorization.
```

### D. v7.6 / v7.7 filter optimization — gold-pass vs noise-filter tradeoff

Status:

```text
PROMISING_BUT_UNVERIFIED.
Historical component-separability evidence only.
Must be re-projected through current fullnet fields before it can become a repair candidate.
```

Evidence:

```text
/Users/boliu/sas-research/docs/v7.6-parameter-optimization.md
/Users/boliu/sas-research/docs/v7.7-extended-backtest-optimization.md
```

Recorded finding:

```text
v7.6:
  40 gold/silver samples, 304 noise samples, 6400 parameter combinations
  BALANCED: gold pass 77.5%, noise filter 59.9%, precision 20.3%

v7.7:
  62 gold samples, 1043 noise samples, 15400 combinations
  strongest factor = signalTrendType
  ACCELERATING: gold 29.0%, noise 9.3% = 3.1x enrichment
  AGGRESSIVE: gold pass 90.3%, noise filter 15.2%
  BALANCED: gold pass 85.5%, noise filter 19.3%
  CONSERVATIVE: gold pass 29.0%, noise filter 90.9%, precision 16%
```

Interpretation:

```text
This does not prove entry EV, but it is another historical component-separability result.
It says filters had a real coverage/precision tradeoff and signalTrendType was the strongest historical separator.
Current fullnet should project signalTrendType / accelerating state as a first-class component if still available.
```

## §14.10 USER-SUPPLIED RECORDS INTAKE — second pass after large note dump

Date: 2026-06-22.

Scope: user supplied a large collection of older strategy notes covering NOT_ATH filters, FBR / first-bar-return, staged re-entry, EntryReadiness odds, SmartEntry V2 scoring, phase policy, mode registry, hard-gate probes, source resonance, accounting repairs, narrative radar, and OI/funding. This section records what can be reused, what is contradicted, and what must remain research-only.

Important boundary:

```text
Nothing in this section is a live/gate/entry/exit/size authorization.
Historical positive rows are not strategy truth.
The only allowed use is to decide what fields/components to project into current fullnet dog/dud + EV audits.
```

### Verification performed in this pass

Local source existence / syntax checks:

```text
python3 -m py_compile:
  scripts/entry_readiness_policy.py
  scripts/phase_policy.py
  scripts/entry_mode_registry.py
  scripts/entry_mode_quality.py
  scripts/backtest-not-ath-v2.py
  scripts/backtest-not-ath-v3.py

Result: PASS.
```

Runtime wiring check:

```text
paper_trade_monitor.py imports and calls:
  evaluate_entry_readiness_policy
  evaluate_phase_policy
  entry_mode_registry_entry / normalize_entry_mode

entry_engine.py imports:
  entry_readiness_policy / entry_mode_allowed

entry_mode_quality.py imports:
  entry_mode_registry
```

Registry shape check:

```text
/Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json
  modes=35
  virtual_modes=2
  live=6
  hard_shadow=7
  revival_canary=5
  shadow_watch_only=2
  isolated_paper_capped=15

/Users/boliu/sas-research/config/entry-mode-registry.json
  modes=35
  virtual_modes=2
  live=5
  hard_shadow=9
  revival_canary=4
  shadow_watch_only=2
  isolated_paper_capped=15
```

Interpretation:

```text
The runtime repo and research repo registry files are not identical.
Do not quote one registry as system truth without naming the repo and commit/source.
The runtime repo should be treated as execution truth; sas-research can be treated as audit/config-copy truth only after diff reconciliation.
```

### A. FBR / first-bar-return NOT_ATH family — strongest historical candidate, still not current truth

Status:

```text
PROMISING_BUT_UNVERIFIED -> highest-priority current projection candidate.
```

Verified source:

```text
/Users/boliu/sas-research/FINAL_CANNONICAL_REPORT.md
```

Important verified rows:

```text
Data: 2026-03-10..20, 463 NOT_ATH_V17 tokens.
Core filter: first 1m bar close > open.

All NOT_ATH:
  n=463, EV=+15.5%, WR=76%.

FBR > 0:
  n=202, EV=+27.9%, WR=96%.

FBR > 3:
  n=171, EV=+31.5%, WR=100%.

Causal / bar-2 correction:
  FBR >=0 raw EV +28.15% -> causal EV +19.04%.
  FBR >=2 raw EV +30.83% -> causal EV +20.05%.

Nested walk-forward:
  5/5 test windows positive, average EV around +15.6%.
```

Caveats:

```text
Canonical report explicitly says:
  - around one third of FBR edge is look-ahead / bar-close timing effect;
  - permutation test showed signal timing itself had negative alpha;
  - much of profitability came from Mar 13-20 market regime plus SL/trailing mechanics.

Therefore FBR can be projected as a current timing/readiness feature, but cannot be treated as proven current edge or live rule.
```

Current fullnet action:

```text
Add/project candidate fields when data is available:
  first_bar_return_pct
  first_bar_green
  fbr_bucket
  fbr_bar2_entry_available
  first_bar_upper_wick_pressure
  consecutive_green_bars
  entry_bar_volume_proxy

Evaluate against:
  formal dog
  peak>=50 clean
  peak>=100 clean
  peak>=5x / >=10x clean
  formal dud
  would_enter
  entered
  held/captured
  friction-adjusted EV when real entries exist
```

### B. MC / SI / AI / velocity NOT_ATH filter notes — conflicting historical evidence

Status:

```text
CONFLICTING_HISTORICAL_BACKTEST / NEEDS_RERUN.
Do not use as a gate or repair owner yet.
```

Verified source files:

```text
/Users/boliu/sentiment-arbitrage-system/scripts/backtest-not-ath-v2.py
/Users/boliu/sentiment-arbitrage-system/scripts/backtest-not-ath-v3.py
```

User-note claims preserved for provenance:

```text
MC<30K hard gate.
SI>=100/120.
AI>=40 or AI>=60.
velocity >=20/30/50%.
UTC 20-22 skip.
pre-3m dump skip.
MC 5K-30K / 10K-30K preferred.
```

Conflicts found in local artifacts:

```text
1. FINAL_CANNONICAL_REPORT says log(MC) was the only monotonic factor, with larger MC better in that March NOT_ATH set.
   This conflicts with "MC>=30K all lose" from the old note.

2. FINAL_CANNONICAL_REPORT says si and ai were weak / reverse / no-difference in that canonical set.
   This conflicts with the old SI/AI sweet-spot claims.

3. backtest-not-ath-v2.py and backtest-not-ath-v3.py both define MIN_VEL = 0 by default.
   That means the checked-in scripts do not actually enforce the velocity filter that the old note presents as decisive.

4. backtest-not-ath-v2.py allows missing MC via _mc_unknown exemption in later load paths.
   This means MC filter denominators may differ from the plain table claim.
```

Current fullnet action:

```text
Keep these as candidate fields only:
  market_cap_bucket
  super_index_bucket
  ai_index_bucket
  ai_sweet_spot_60_80
  utc_hour_bucket
  pre3m_return_pct
  velocity_5m_pct
  velocity_30s_60s_proxy if available

Do not promote MC/SI/AI/velocity to rules until a current same-window rerun shows:
  dog enrichment,
  dud rejection,
  quote-clean survivability,
  would_enter contribution,
  and EV/capture contribution.
```

### C. RED-bar / low-volume / active-momentum kline scoring — separate from FBR, not merged

Status:

```text
PROMISING_BUT_UNVERIFIED.
Do not merge with FBR as one "kline filter"; they point at different timing philosophies.
```

Reason:

```text
The old RED-bar scoring record waits for a pullback/red bar with support/low-volume/activity properties.
The canonical FBR record waits for a green first bar / first-bar-return confirmation.
These are not the same rule and may select opposite entry nodes.
```

Current fullnet action:

```text
Project as separate timing templates:
  kline_template = FBR_GREEN_CONFIRM
  kline_template = RED_LOW_VOLUME_PULLBACK
  kline_template = THREE_BAR_SCORE

Each template must report:
  eligible_n
  dog/dud precision
  would_enter_n
  entered_n
  missing_reason
  bar-alignment legality
```

### D. Stage 1 / Stage 2A / Stage 3 staged lifecycle strategy — architecture candidate, runtime still needs trace

Status:

```text
PARTIALLY_IMPLEMENTED / NEEDS_RUNTIME_TRACE.
```

Verified local files:

```text
/Users/boliu/sentiment-arbitrage-system/data/paper-strategy-registry.json
/Users/boliu/sentiment-arbitrage-system/src/config/paper-strategy-registry.js
/Users/boliu/sentiment-arbitrage-system/src/config/strategy-candidate-schema.js
/Users/boliu/sentiment-arbitrage-system/scripts/paper_trade_monitor.py
```

Evidence:

```text
paper_trade_monitor.py stores:
  strategy_stage
  lifecycle_id
  stage_seq
  trigger_ts
  trigger_price
  first_peak_pct
  monitor_state_json

The codebase has runtime support for staged rows and lifecycle-level accounting.
```

Boundary:

```text
The pasted notes include multiple variants:
  - Stage 2A stop-loss recovery via rolling-low rebound +18%.
  - Stage 3 signal-awakening continuation.
  - Earlier signal+30 continuation re-entry.

These must not be collapsed into one "Stage 3 works" claim.
Each trigger design needs a separate projection and paper EV denominator.
```

Current fullnet action:

```text
Project:
  strategy_stage
  parent_trade_id
  lifecycle_id
  stage2a_armed / stage2a_entered / stage2a_block_reason
  stage3_armed / stage3_signal_awakened / stage3_entered / stage3_block_reason
  first_peak_pct
  exit_reason_before_reentry
  rolling_low_rebound_pct
  structure_floor_pass

No Stage 2A/Stage 3 promotion until same-window paper fills exist.
```

### E. EntryReadiness / odds contract — current code exists, must become fullnet field

Status:

```text
CURRENT_CODE_EXISTS / CURRENT_FULLNET_CANDIDATE.
```

Verified source:

```text
/Users/boliu/sentiment-arbitrage-system/scripts/entry_readiness_policy.py
```

Verified implementation shape:

```text
evaluate_entry_readiness_policy returns:
  decision = ARM / WAIT / EXPIRE
  lifecycle_profile
  min_odds_r
  min_p_follow
  max_spread_pct
  expected_loss_pct
  expected_upside_pct
  allowed_entry_modes
  reason

Profile examples:
  LOTTO_NEWBORN_RISKY -> min_odds_r 3.0, max_spread_pct 1.0
  LOTTO_REAL_PROBE    -> min_odds_r 3.0
  LOTTO_NORMAL        -> min_odds_r 2.5
  ATH_CONTINUATION    -> min_odds_r 1.8
  ATH_STALE           -> stricter, requires fresh high / sustained ATH-like evidence

Risk memory increases odds and tightens spread for:
  waterfall_memory
  waterfall_failure
  no_follow_failure
  doa_failure
```

Runtime wiring:

```text
paper_trade_monitor.py imports and calls evaluate_entry_readiness_policy.
entry_engine.py imports entry_mode_allowed and readiness policy helpers.
paper_trade_monitor.py stores entry_readiness_policy in entry audit JSON and decision events.
```

Current fullnet action:

```text
Every row should expose:
  entry_readiness_decision
  lifecycle_profile
  min_odds_r
  min_p_follow
  max_spread_pct
  expected_loss_pct
  expected_upside_pct
  allowed_entry_modes
  readiness_reason

Audit question:
  Does readiness decision / profile enrich dogs or reduce duds?
  Does it explain would_enter -> no_enter?
  Does it improve EV after friction?
```

### F. Entry mode registry / four-tier governance — current code exists, but repo copies diverge

Status:

```text
CURRENT_CODE_EXISTS / GOVERNANCE_CANDIDATE / NEEDS CANONICAL COPY RECONCILIATION.
```

Runtime registry:

```text
/Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json
  live=6
  hard_shadow=7
  revival_canary=5
  shadow_watch_only=2
  isolated_paper_capped=15
  virtual_modes=2
```

Research copy:

```text
/Users/boliu/sas-research/config/entry-mode-registry.json
  live=5
  hard_shadow=9
  revival_canary=4
  shadow_watch_only=2
  isolated_paper_capped=15
  virtual_modes=2
```

Implication:

```text
Use runtime repo registry for current behavior.
Use sas-research registry only after diff reconciliation.
Fullnet reports must include registry_source_path and registry_hash.
```

Current fullnet action:

```text
Every entry-mode row should include:
  mode_tier
  paper_enabled
  size_class
  family
  registry_reason
  virtual_mode_mapping
  promotion_policy_status
```

### G. SmartEntry V2 scoring / momentum direct / pullback-bounce — use as timing hypotheses only

Status:

```text
PROMISING_DESIGN_HYPOTHESIS / NOT CURRENTLY PROVEN.
```

Reason:

```text
The pasted SmartEntry V2 score table is design logic, not a verified same-window EV result.
It is useful because it encodes the correct philosophy:
  historical strength -> ARM
  current microstructure node -> ENTER
  cost budget -> final allow/block

But the exact weights and thresholds cannot be accepted without replay/current projection.
```

Current fullnet action:

```text
Project timing mode separately:
  momentum_direct_entry
  smart_entry_pullback_bounce
  red_low_volume_pullback
  fbr_green_confirm

Reject any row that says only "total_score >= 50" without a timing node as a proven edge claim.
```

### H. Phase policy / exit-hold-rug defense — current code exists, effectiveness not proven here

Status:

```text
CURRENT_CODE_EXISTS / NEEDS EVENT AND OUTCOME VERIFICATION.
```

Verified source:

```text
/Users/boliu/sentiment-arbitrage-system/scripts/phase_policy.py
paper_trade_monitor.py calls evaluate_phase_policy and writes phase_policy shadow_decision events.
```

Current fullnet action:

```text
Project:
  phase_policy_shadow_action
  phase_policy_reason
  kline_position
  rug_risk_score
  quote_pnl_vs_mark_pnl_gap
  phase_policy_live_exit_enabled
  phase_policy_live_exit_taken

Effectiveness question:
  Did shadow EXIT/PARTIAL/HOLD decisions predict later loss/giveback?
  Did live phase policy reduce loss without cutting dogs?
```

### I. Hard-gate pass probe / source resonance / NOT_ATH watch — infrastructure only unless current samples prove it

Status:

```text
INFRASTRUCTURE_ONLY / SHADOW_OR_HARD_SHADOW_BY_REGISTRY.
```

Current runtime registry says:

```text
source_resonance_tiny_probe = hard_shadow
hard_gate_pass_tiny_probe   = hard_shadow
lotto_not_ath_watch_shadow  = virtual shadow_watch_only
```

Interpretation:

```text
These records should be used to improve denominator coverage and missed-dog attribution.
They are not positive entry evidence unless mode-level quote-clean paper samples and EV pass promotion policy.
```

Current fullnet action:

```text
Report:
  observed_n
  quote_retry_n
  quote_clean_n
  snapshot_pass_n
  would_enter_n
  entered_n
  blocker
  dog/dud enrichment
```

### J. Accounting / spread / trigger / signal_ts / partial / token memory notes — repair candidates, not alpha evidence

Status:

```text
REPAIR_CANDIDATE_QUEUE.
```

Reason:

```text
The notes about trigger_price, signal_ts seconds-vs-ms, duplicate partial, spread budget, stale probe TTL, and token risk memory are correctness/observability repairs.
They are not strategy edges by themselves.
```

Current code hints already found:

```text
paper_trade_monitor.py normalizes signal_ts before storing paper trade rows.
paper_trade_monitor.py stores trigger_price separately from entry_price in entry audit JSON.
paper_trade_monitor.py stores entry_edge_budget, entry_readiness_policy, final_entry_contract, execution_scope, paper_only_scout.
```

Next audit required:

```text
Do not assume all old bug notes are fixed.
Run targeted current-code tests / DB checks for:
  trigger_price != fill price when spread exists
  signal_ts stored as unix seconds
  duplicate partial idempotency
  positive gap_crash not classified as loss failure
  spread_2_to_5_filled == 0 after budget changes, if that policy is active
```

### K. Narrative radar and OI/funding notes — out of current gold/silver entry net

Status:

```text
NARRATIVE_RADAR: OPTIONAL SHADOW DISCOVERY MODULE.
OI/FUNDING: SEPARATE FUTURES DOMAIN, DO NOT MIX INTO SOL MEME ENTRY EV.
```

Use boundary:

```text
Narrative can tag why a token is interesting, dedupe/heating themes, and feed watchlist shadow.
It should not directly buy.

OI/funding belongs to Binance futures / perpetual-squeeze research.
It should not be included in the Solana meme dog/dud entry net unless built as a separate paper module with separate denominator.
```

### Final classification from this pass

```text
CAN USE NOW AS FULLNET FIELDS:
  entry_readiness_policy outputs
  mode registry tier/reason
  phase_policy outputs
  entry audit fields: trigger/quote/spread/edge_budget/final_entry_contract

CAN USE AS HIGH-PRIORITY HYPOTHESES ONLY:
  FBR / first-bar-return family
  RED-low-volume-active pullback family
  Stage 2A recovery
  Stage 3 signal-awakening continuation
  SmartEntry timing-node philosophy

MUST RERUN BEFORE USE:
  MC<30K hard gate
  SI>=100/120
  AI sweet spot
  velocity>=30/50
  UTC 20-22 skip
  pre-3m dump skip

DO NOT USE:
  invalidated early shadow MC bucket table
  any old result based on known-bad shadow accounting / polluted paper rows
  any "win rate" that lacks denominator, date window, friction, and time-legality proof
```

Next work item:

```text
Extend current fullnet row/report with the fields above.
Then run current same-window dog/dud + opportunity denominator audit.
Only fields that survive current projection can move from PROMISING_BUT_UNVERIFIED to CURRENT_FULLNET_CANDIDATE.
```

## §14.11 HANDLING PLAN — NOT_ATH kline microstructure + entry-model effectiveness

Date: 2026-06-22.

Question being answered:

```text
How should the NOT_ATH entry-bar / kline microstructure clues be handled?
Have they been deeply verified?
Were there around 32 entry models, and were several effective?
```

### Current answer

```text
No, the NOT_ATH kline microstructure candidates have not yet been deeply verified in current fullnet.
They have been source-verified and classified, but not re-projected into current same-window dog/dud + would_enter + entered + EV.

The entry-mode governance layer has been source/wiring verified.
The current runtime registry has 35 modes + 2 virtual modes, not exactly 32.

The recent 2026-06-21 current fullnet window cannot prove entry-mode EV because entered_total_n=0.
It can only show would_enter / blocker / dog-enrichment behavior.
```

### Current fullnet evidence from fresh-20260621T123643Z

Artifact:

```text
/Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-aligned-current/
```

Key readout:

```text
329 signals:
  dog=29
  dud=123
  pending=177

all_signals:
  has_decision=278
  quote_clean=261
  would_enter=77
  entered=0
  held=0

entry-mode contribution:
  entered_total_n=0
  actual_net_ev_pct=null
  only exported decision entry_mode in this report:
    a_grade_resonance_fastlane:
      signals=278
      dog=23
      dud=98
      dog_precision=0.1901
      baseline_dog_rate=0.1908
      lift_vs_baseline=-0.0007
      would_enter=77
      entered=0

Interpretation:
  Current 24h fullnet does NOT prove any entry model effective.
  It also does NOT prove entry models useless, because policy/shadow/bridge produced zero actual entries.
```

Current component signals:

```text
markov_yellow_or_green:
  n=31
  dog_precision=0.2903
  lift_vs_baseline=+0.0995
  entered=0
  small-n warning.

markov_green:
  n=6
  dog_precision=0.50
  lift_vs_baseline=+0.3092
  entered=0
  severe small-n warning.

would_enter:
  n=40 on formal dog+dud denominator
  dog_precision=0.20
  lift_vs_baseline=+0.0092
  entered=0
```

Interpretation:

```text
Current fullnet says:
  - Markov/regime remains the only visible positive dog-enrichment component in this window, but sample is too small.
  - Matrix / would_enter by themselves are close to baseline.
  - No actual EV can be computed because entry bridge/policy has no entered rows.
```

### Runtime registry status — "32 entry models" memory reconciled

Runtime source:

```text
/Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json
```

Current shape:

```text
modes=35
virtual_modes=2

tiers:
  live=6
  hard_shadow=7
  revival_canary=5
  shadow_watch_only=2
  isolated_paper_capped=15
```

Interpretation:

```text
The remembered "32 entry models" is directionally correct but stale.
The current runtime registry has 35 concrete modes plus 2 virtual modes.
Most are explicitly blocked, canary, isolated, or shadow-only.
Only 6 are in runtime paper-live tier, and that still means paper/realtime simulated entry, not wallet execution.
```

### Historical local paper DB entry-stage check

Historical source:

```text
/Users/boliu/sentiment-arbitrage-system/data/paper_trades.db
mtime: 2026-05-06
rows: 1605
```

Important caveat:

```text
This is not the current 2026-06-21 fullnet window.
It is a historical local paper DB and must not override current fullnet.
```

Live-monitor rows only:

```text
stage1:
  n=1262
  avg_pnl=+0.0217
  win_rate=44.85%
  avg_peak=+0.0401

stage2A:
  n=33
  avg_pnl=+0.0289
  win_rate=48.48%
  avg_peak=+0.0449

stage3:
  n=88
  avg_pnl=-0.0091
  win_rate=22.73%
  avg_peak=+0.0133
```

Real-kline replay rows:

```text
stage2A:
  n=24
  avg_pnl=+0.0749
  win_rate=54.17%
  avg_peak=+0.1033

stage3:
  n=5
  avg_pnl=+0.0198
  win_rate=20.0%

stage1 real_kline_replay:
  polluted by extreme outliers:
    polluted_big_pnl_n=19
    max_pnl=1476574778.2875
  Do not use its raw average.
```

Historical interpretation:

```text
Stage2A is the strongest historical entry-stage candidate found in this pass.
It is positive in both live_monitor and real_kline_replay slices, though n is small.

Stage3 is not currently a positive candidate from this local paper DB.
The live_monitor slice is negative and includes a trapped_token_not_tradable -100% row.

Stage1 is positive in live_monitor, but it is broad baseline-like behavior, not proof of a specific new entry model.

The old "several effective entry models" memory may be a mix of:
  - Stage2A being positive;
  - historical anchor / continuation / path_context sleeves;
  - FBR / entry-bar timing candidates;
  - early would_enter / fastlane counterfactual replay.
These must remain separate evidence families.
```

### What to do with NOT_ATH entry-bar / kline microstructure

Processing rule:

```text
Do not change live/paper strategy from these old rows.
Do not merge RED-bar pullback and FBR-green confirmation into one rule.
Add them as first-class current fullnet projection fields.
```

Fields to add/project:

```text
NOT_ATH microstructure:
  red_bar
  low_volume_vs_prev3
  active_momentum_prev3_pct
  three_bar_score
  support_preserved
  current_volume_gt_prev

FBR family:
  first_bar_return_pct
  first_bar_green
  first_bar_return_bucket
  bar2_entry_available
  first_bar_upper_wick_pressure

Volume family:
  vol_accel
  vol_accel_bucket
  volume_profile
  entry_volume_quintile

Timing legality:
  feature_available_at_signal
  feature_available_at_decision
  feature_requires_bar_close
  earliest_legal_entry_ts
  lookahead_risk_flag
```

Report outputs required:

```text
For each candidate field/template:
  all_n
  formal_dog_n
  formal_dud_n
  peak50_clean_n
  peak100_clean_n
  peak5x_clean_n
  dog_precision
  lift_vs_baseline
  would_enter_n
  entered_n
  held_n
  actual_ev_pct or explicit missing_reason
  counterfactual_replay_ev_pct
  friction sensitivity if replayed
  small-n warnings
```

### What to do with 35 entry modes

Processing rule:

```text
Every entry mode must be judged on three separate ledgers:

1. current fullnet same-window:
   dog/dud, would_enter, entered, held, actual EV.

2. historical paper DB:
   real paper fill PnL by mode/stage, with polluted rows removed.

3. counterfactual replay:
   TP/SL residual EV from legal entry decision points.
```

Mode evidence states:

```text
CURRENT_ENTERED_EV_POSITIVE:
  same-window actual entered rows exist and friction-adjusted EV > 0.

CURRENT_WOULD_ENTER_ONLY:
  same-window would_enter exists, but entered=0; not EV evidence.

HISTORICAL_POSITIVE_NEEDS_REPLAY:
  older paper/replay rows are positive, but not revalidated in current fullnet.

SHADOW_ONLY_NO_FILL:
  mode exists and may observe/attribute, but has no fill evidence.

NEGATIVE_OR_DISABLED:
  registry or historical scorecard says poor EV / high risk.
```

Initial classification from this pass:

```text
CURRENT_WOULD_ENTER_ONLY:
  a_grade_resonance_fastlane

HISTORICAL_POSITIVE_NEEDS_REPLAY:
  stage2A
  stage1 live_monitor baseline
  FBR / first-bar-return timing family
  RED-low-volume-active pullback family

NEGATIVE_OR_DISABLED:
  stage3 from the historical local paper DB, until redesigned and re-tested
  momentum_direct_entry per runtime registry hard_shadow reason
  source_resonance_tiny_probe per runtime registry hard_shadow reason
  hard_gate_pass_tiny_probe per runtime registry hard_shadow reason

SHADOW_ONLY_NO_FILL:
  lotto_not_ath_watch_shadow
  lotto_upstream_miss_tiny_scout
  lotto_upstream_realtime_tiny_scout
  most isolated_paper_capped tiny/scout modes until capped paper is explicitly enabled and measured.
```

### Repair implications

```text
Do not say: "entry models are useless."
Correct statement:
  Current 24h fullnet has would_enter but no entered, so actual EV is unmeasured.
  Historical records suggest Stage2A and kline/FBR timing families deserve current re-projection.
  Stage3 is not proven and currently looks weak in the local historical DB.
```

Recommended next implementation step:

```text
Build or extend a current fullnet "entry-model effectiveness" report:

inputs:
  live-fullnet-row.jsonl
  runtime entry-mode-registry.json
  paper_trades.db / canonical_trade_ledger when available
  kline feature source for NOT_ATH rows

outputs:
  live-fullnet-entry-model-effectiveness.json
  live-fullnet-not-ath-kline-candidates.json

Do not modify trading behavior until these reports classify at least one mode/template as CURRENT_ENTERED_EV_POSITIVE or a tightly scoped CURRENT_FULLNET_CANDIDATE with explicit missing_reason for absent entries.
```

### E. Tiny probe / scout modes — infrastructure exists, but no located 68% real-fill evidence yet

Status:

```text
INFRASTRUCTURE_ONLY / NO_REAL_FILL_EVIDENCE_FOUND.
Verified in this audit as existing mode registry + tests + design docs.
The located review artifact has zero fills and null PnL for the relevant probes,
so this is NOT evidence of a 68% win-rate tiny-probe strategy.
```

Evidence:

```text
/Users/boliu/sentiment-arbitrage-system/data/reviews/paper_review_20260514_120719962Z_1h_c0764b82bcaf.json
/Users/boliu/sentiment-arbitrage-system/claudedocs/goal-driven-control-rebuild-plan.md
/Users/boliu/sentiment-arbitrage-system/claudedocs/lotto-stale-reclaim-design.md
/Users/boliu/sentiment-arbitrage-system/test_paper_monitor_strategy_helpers.py
```

Recorded state:

```text
paper review registry:
  mode_count=35
  live modes=9
  hard_shadow=5
  revival_canary=4
  shadow_watch_only=2
  isolated_paper_capped=15

paper-enabled tiny/probe/scout examples:
  hard_gate_pass_tiny_probe
  pre_pass_resonance_tiny_probe
  source_resonance_tiny_probe
  lotto_not_ath_reclaim_tiny_probe
  lotto_low_liquidity_reclaim_tiny_probe
  ath_no_kline_tiny_probe
  ath_uncertainty_tiny_scout
  explosive_newborn_direct_scout
```

But the located paper review snapshot has:

```text
paper_trades_total=0
hard_gate_pass_probe_fills=0
source_resonance_probe_fills=0
pre_pass_probe_fills=0
probe avg_pnl fields = null
```

Interpretation:

```text
The "small-size probe" architecture is real and should be represented in fullnet as entry_mode / capital_tier / paper_only_scout / parent_scout_mode.
However, in the located review artifact it produced zero fills, so it is not evidence for a 68% live/paper win rate.

The 68% evidence found so far belongs to smart_backtest/debate daily/regime rows or early_15m raw discovery timing,
not to a proven tiny-probe real-fill performance report.

Fullnet implication:
  tiny/scout modes must enter the row-level audit, but their value must be judged by actual fills, realized EV, friction, and capture,
  not by their existence in the mode registry.
```

### Required reconciliation into current fullnet

The next fullnet iteration must not ask only "does current entry_mode work?". It must explicitly re-project these older positive axes into the current same-window rows:

```text
1. MC bucket:
   0-10K / 10-20K / 20-30K / 30-50K / 50K+
   report dog/dud, would_enter, entered, held, actual EV, counterfactual replay EV.

2. NOT_ATH entry-bar state:
   red_bar, low_volume, active_mom, entry_volume_quantile,
   first_bar_return, first_bar_return_bucket, vol_accel, vol_accel_bucket,
   entry_body_ratio, candle_pattern, volume_profile
   report dog/dud enrichment and replay EV; mark any field unavailable or post-decision.

3. Router sleeve family:
   anchor / continuation / soft_context / path_context
   report whether the current system emits an equivalent sleeve/profile; if not, missing_reason.

4. v7 filter state:
   signalTrendType / ACCELERATING, score thresholds, smart-money count if available
   report pass/blocked dog/dud and opportunity lost.

5. Tiny-probe / scout mode state:
   entry_mode, parent_scout_mode, paper_only_scout, capital_tier, position_size_sol,
   enqueue/order/fill/ledger/exit/friction.
   report real fill count, win rate, actual net EV, and capture by mode.
```

Decision guard:

```text
If an older positive axis still enriches dogs or replay EV in current same-window data, it becomes a repair candidate.
If it no longer projects, or projects but does not enrich current dogs, it is archived as historical-only.
No older backtest/shadow result can authorize live without current same-window entry/fill/friction/exit evidence.
```

## §15 GOAL — current fullnet field expansion + historical entry evidence revalidation

Created: 2026-06-22.

### §15.1 Goal statement

Goal:

```text
Build a current fullnet audit extension that connects all modules directly affecting gold/silver capture into row-level same-window evidence, then revalidates the historical NOT_ATH / kline / stage / entry-mode clues against current dog/dud, would_enter, entered, held, friction, and EV.
```

In plain language:

```text
Stop debating old backtests in isolation.
Put every relevant module and historical clue into the same current fullnet row.
Then let one audit answer:
  - Which module helps identify gold/silver dogs?
  - Which module blocks dogs?
  - Which entry mode only reaches would_enter but never enters?
  - Which old high-win-rate clue survives current data?
  - Which clue was only old market regime / look-ahead / accounting artifact?
```

Primary output artifacts:

```text
1. live-fullnet-row.jsonl
   Existing fullnet row output, expanded with readiness/mode/kline/phase/entry audit fields.

2. live-fullnet-entry-model-effectiveness.json
   Per entry mode / stage / registry tier effectiveness:
     dog/dud, would_enter, entered, held, actual EV, historical DB cross-check, replay EV.

3. live-fullnet-not-ath-kline-candidates.json
   NOT_ATH microstructure candidate report:
     FBR, RED bar, low volume, active momentum, volume acceleration, legality flags.

4. live-fullnet-historical-evidence-revalidation.json
   Explicit registry of old claims:
     68% rows, Stage2A, Stage3, MC/SI/AI/velocity, router sleeves, tiny probes.
     Each claim gets status: CURRENT_FULLNET_CANDIDATE / HISTORICAL_ONLY / INVALID / NEEDS_RERUN.

5. readout markdown:
   claudedocs/fullnet-entry-model-kline-revalidation-readout-YYYY-MM-DD.md
```

### §15.2 Non-goals and safety guardrails

This goal is research-only.

```text
DO NOT:
  - change live gates;
  - change paper/live entry behavior;
  - change exit/hold behavior;
  - change position size;
  - unshadow any entry mode;
  - restore Stage2A/Stage3;
  - restore FBR / RED-bar / velocity filters;
  - treat raw dog labels as entry features;
  - treat old win-rate rows as current edge.
```

Allowed changes:

```text
Allowed:
  - extend research scripts;
  - extend fullnet row schema;
  - add read-only reports;
  - read local/downloaded DB/export/log artifacts;
  - emit missing_reason / not_applicable_reason;
  - add tests for report logic;
  - update claudedocs.
```

Fail-closed rule:

```text
If entered_n = 0:
  actual EV must be null.
  Report missing_reason = no_entered_rows_no_actual_net_ev.
  Counterfactual/replay EV may be shown only under explicit replay/counterfactual labels.
```

### §15.3 Modules that must enter current fullnet

Only modules directly affecting "can we catch gold/silver dogs" are in scope.

Required row-level module families:

```text
1. signal metadata
   token_ca
   symbol
   signal_id
   signal_ts
   signal_type
   is_ath
   source/hard_gate status
   raw_message metadata if available

2. raw opportunity labels
   formal_dog / formal_dud / pending
   raw_primary_tier
   peak>=50 clean
   peak>=100 clean
   peak>=5x clean
   peak>=10x clean
   These are ex-post denominators, not buy signals.

3. lifecycle state
   lifecycle_at_signal
   lifecycle_at_decision
   lifecycle_profile
   entry_bias
   vitality_score
   lifecycle_missing_reason

4. Markov / revival / regime
   markov_applicable
   markov_evaluated
   markov_bucket
   markov_yellow_or_green
   markov_green
   revival_state
   markov_missing_reason

5. filters / gates
   source_gate
   security
   liquidity
   spread
   quote_age
   mcap
   holders
   matrix_reject
   hard_blockers
   filter_family
   filter_result
   filter_block_reason

6. matrix / score / expected RR
   matrix_grade
   normalized_mode
   score fields available in a_class/matrix_json
   expected_rr
   expected_upside_pct
   defined_risk_pct

7. readiness / odds contract
   entry_readiness_decision
   lifecycle_profile
   min_odds_r
   min_p_follow
   max_spread_pct
   expected_loss_pct
   expected_upside_pct
   allowed_entry_modes
   readiness_reason

8. quote / route / execution evidence
   quote_clean_verified
   quote_source
   quote_age_sec
   liquidity_usd
   spread_pct
   route_available
   quote_executable
   route_missing_reason

9. entry mode registry / governance
   entry_mode
   normalized_entry_mode
   mode_tier
   paper_enabled
   size_class
   mode_family
   registry_reason
   virtual_mode_mapping
   registry_source_path
   registry_hash

10. would_enter / final entry contract
   has_decision
   quote_clean
   would_enter
   would_enter_ts
   would_enter_reason
   final_entry_decision
   final_entry_block_owner
   hard_blockers
   policy_shadow_only

11. entry bridge / ledger
   enqueue_seen
   order_seen
   fill_seen
   ledger_seen
   entered
   entry_ts
   entry_price
   execution_bridge_missing_reason
   policy_shadow_only_reason

12. exit / hold / moonbag
   exit_seen
   exit_ts
   exit_reason
   held
   time_held_sec
   peak_pnl
   realized_pnl_pct
   moonbag_seen
   capture_status

13. friction / slippage
   entry_spread_pct
   entry_slippage_bps
   entry_penalty_bps
   round_trip_friction_pct
   actual_net_ev_pct
   actual_net_ev_missing_reason

14. phase_policy / exit shadow
   phase_policy_shadow_action
   phase_policy_reason
   kline_position
   rug_risk_score
   quote_pnl_vs_mark_gap
   phase_policy_live_exit_taken
```

### §15.4 Historical clues to revalidate

Historical clues are not rules. Each becomes either a row-level field, a template label, or a historical evidence item.

#### §15.4.1 NOT_ATH kline microstructure

Candidate fields:

```text
red_bar
low_volume_vs_prev3
active_momentum_prev3_pct
three_bar_score
support_preserved
current_volume_gt_prev
entry_volume_quintile
```

Candidate templates:

```text
RED_LOW_VOLUME_ACTIVE_PULLBACK
THREE_BAR_SCORE_GE_2
RED_SUPPORT_LOWVOL_ACTIVE
```

Required legality fields:

```text
feature_available_at_signal
feature_available_at_decision
feature_requires_bar_close
earliest_legal_entry_ts
lookahead_risk_flag
kline_missing_reason
```

#### §15.4.2 FBR / first-bar-return family

Candidate fields:

```text
first_bar_return_pct
first_bar_green
first_bar_return_bucket
first_bar_upper_wick_pressure
first_bar_body_ratio
first_bar_volume_proxy
bar2_entry_available
```

Required warning:

```text
FBR requires bar-close information.
Any FBR result must separate:
  - raw bar-close replay;
  - earliest legal bar-2 entry replay;
  - current same-window dog/dud projection.
```

#### §15.4.3 Stage entry families

Candidate fields:

```text
strategy_stage
stage1_entered
stage1_exit_reason
stage1_first_peak_pct
stage2a_armed
stage2a_entered
stage2a_delay_bars
stage2a_rolling_low
stage2a_rebound_pct
stage2a_block_reason
stage3_eligible
stage3_signal_awakened
stage3_structure_floor_pass
stage3_entered
stage3_block_reason
```

Important boundary:

```text
Stage2A and Stage3 are separate evidence families.
Do not merge:
  - stop-loss recovery Stage2A;
  - signal+30 continuation;
  - event-awakening Stage3;
  - generic second entry.
```

#### §15.4.4 MC / SI / AI / velocity / time filters

Status:

```text
NEEDS_RERUN due conflicting historical evidence.
```

Candidate fields:

```text
market_cap_bucket
super_index_bucket
ai_index_bucket
ai_60_80_sweet_spot
velocity_5m_pct
velocity_30s_proxy
velocity_60s_proxy
utc_hour_bucket
pre3m_return_pct
pre3m_dump_flag
```

Required report condition:

```text
If the field is unavailable or was disabled in the historical script, report that explicitly.
Example: v2/v3 scripts had MIN_VEL=0, so old velocity conclusions cannot be inferred from those script defaults.
```

#### §15.4.5 Tiny probe / scout / source resonance

Candidate fields:

```text
paper_only_scout
execution_scope
capital_tier
position_size_sol
parent_scout_mode
source_resonance_cohort
hard_gate_pass_probe_eligible
not_ath_watch_shadow_snapshot_pass
snapshot_pass_count
quote_retry_count
```

Required status logic:

```text
Mode existence is not positive evidence.
Only actual fills with realized/friction-adjusted EV can prove probe value.
Shadow-only rows can prove denominator coverage, not capture.
```

### §15.5 The 68% high-win-rate clue — explicit handling

The user remembers a high win-rate around 68%. This goal must not collapse all 68-like numbers into one claim.

Known candidate sources:

```text
Candidate A: smart_backtest walk-forward day
  Source: smart_backtest / debate artifacts.
  Row: 2026-03-19.
  n=25, WR=68.0%, EV=+16.8%.
  Interpretation: day/regime bucket, not a standalone entry model.

Candidate B: debate_agent_b hour bucket
  Row: hour=14.
  n=22, WR=68.2%, EV=+14.78%.
  Interpretation: time bucket, not a standalone entry model.

Candidate C: early_15m raw movement
  Approx value around 68% in older notes.
  Interpretation: likely "dogs start within 15m" / timing-distribution percentage, not trade win rate.

Candidate D: signal+30 continuation
  Row remembered as signal+30 + first_peak>=10%.
  n around 38, WR around 68.42%, Avg around +16.41%.
  Interpretation: continuation re-entry hypothesis, separate from Stage2A and from FBR.

Candidate E: NOT_ATH selective top10/ai/holders segment
  Located row was around 59.3% WR, not 68%.
  Interpretation: related but likely not the remembered 68% number.
```

Required handling:

```text
Create a historical_68_claims section in live-fullnet-historical-evidence-revalidation.json.

For each candidate:
  source_artifact
  date_range
  n
  metric_type = win_rate / movement_share / replay_ev / paper_fill_ev
  denominator
  time_legality
  friction_assumption
  current_projection_available
  current_projection_status
```

Promotion rule:

```text
No 68% claim can become CURRENT_FULLNET_CANDIDATE unless:
  - it maps to a current row-level field or mode;
  - it has current same-window dog/dud denominator;
  - it has legal decision-time availability;
  - it improves dog precision/lift or EV;
  - and actual EV is fail-closed if entered=0.
```

### §15.6 Implementation plan

#### Phase 1 — Schema inventory and field map

Files:

```text
scripts/build-live-fullnet-row-report.js
tests/build-live-fullnet-row-report.test.mjs
```

Tasks:

```text
1. Add a central FIELD_CATALOG with these groups:
   signal_metadata
   raw_labels
   lifecycle
   markov
   filters
   matrix
   readiness
   quote_route
   mode_registry
   final_entry_contract
   entry_bridge
   ledger_exit_hold
   friction
   phase_policy
   not_ath_kline_candidates
   historical_evidence_tags

2. Every field must support:
   value
   missing_reason
   not_applicable_reason
   source
   source_confidence

3. Add schema_version bump:
   live_fullnet_row_report.v2
```

Acceptance:

```text
All rows include the new groups.
No row silently omits a group.
projection_complete remains true only when every group has value or reason.
```

#### Phase 2 — Current-code module projection

Tasks:

```text
1. Parse entry_readiness_policy from:
   entry_execution_audit_json
   monitor_state_json
   decision event payload_json
   canonical ledger metadata

2. Join mode registry:
   runtime registry source = /Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json
   include registry_hash
   map virtual modes:
     ath_real_probe -> ath_flat_structure_tiny_scout
     lotto_not_ath_watch_shadow -> shadow_watch_only virtual mode

3. Parse phase_policy:
   paper_decision_events component='phase_policy'
   payload shadow_action/reason/rug/kline fields

4. Parse entry audit:
   trigger_price
   entry_quote_price
   spread
   entry_edge_budget
   final_entry_contract
   execution_scope
   paper_only_scout
```

Acceptance:

```text
Rows expose readiness/mode/phase/entry-audit fields.
If a source is not present, missing_reason explains whether:
  no_trade_row
  no_decision_event
  old_schema_no_field
  not_applicable_no_entry
```

#### Phase 3 — NOT_ATH kline candidate extraction

New helper module:

```text
scripts/not-ath-kline-candidate-features.js
```

Inputs:

```text
row token_ca
row signal_ts
kline source if available:
  kline_cache.db
  raw discovery bars
  source-to-raw pack
  external downloaded kline exports
```

Feature computation:

```text
entry_bar:
  first bar whose timestamp is >= signal_ts minute boundary.

first_bar_return_pct:
  (close - open) / open * 100

first_bar_green:
  close > open

red_bar:
  close < open

low_volume_vs_prev3:
  current volume <= avg(prev3 volume)

active_momentum_prev3_pct:
  abs((prev3 close or current open - prev3 first open) / prev3 first open * 100)

support_preserved:
  current close > min(prev3 lows)

current_volume_gt_prev:
  current volume > previous bar volume

three_bar_score:
  +1 if >=2 of prev3 are green
  +1 if support_preserved
  +1 if current_volume_gt_prev

upper_wick_pressure:
  (high - max(open, close)) / max(high-low, epsilon)

volume_profile:
  building / flat / declining / unknown
```

Legality:

```text
feature_available_at_signal = false for bar-close-derived features.
feature_requires_bar_close = true for FBR/red/volume properties.
earliest_legal_entry_ts = entry_bar_close_ts or next bar open.
lookahead_risk_flag = true if replay uses entry-bar close before legal availability.
```

Acceptance:

```text
Tests cover:
  green first bar
  red low-volume pullback
  missing prev3 bars
  timestamp seconds/ms normalization
  bar-close legality
```

#### Phase 4 — Entry-model effectiveness report

New script:

```text
scripts/build-live-fullnet-entry-model-effectiveness.js
tests/build-live-fullnet-entry-model-effectiveness.test.mjs
```

Inputs:

```text
--rows live-fullnet-row.jsonl
--mode-registry /Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json
--paper-db optional path
--canonical-ledger optional export/db
--out-dir artifact dir
```

Output:

```text
live-fullnet-entry-model-effectiveness.json
```

Per mode fields:

```text
entry_mode
normalized_entry_mode
mode_tier
paper_enabled
family
size_class
signals_n
formal_dog_n
formal_dud_n
peak50_clean_n
peak100_clean_n
peak5x_clean_n
dog_precision
dog_recall
lift_vs_baseline
would_enter_n
entered_n
held_n
actual_net_ev_pct
actual_net_ev_missing_reason
actual_win_rate
actual_round_trip_friction_pct
historical_paper_n
historical_avg_pnl_pct
historical_win_rate
historical_source_db
historical_pollution_flags
counterfactual_replay_ev_pct
evidence_state
confidence_warnings
```

Evidence state ladder:

```text
CURRENT_ENTERED_EV_POSITIVE
CURRENT_ENTERED_EV_NEGATIVE
CURRENT_WOULD_ENTER_ONLY
HISTORICAL_POSITIVE_NEEDS_REPLAY
SHADOW_ONLY_NO_FILL
NEGATIVE_OR_DISABLED
INSUFFICIENT_DATA
```

Acceptance:

```text
If entered_n=0, actual_net_ev_pct must be null.
If historical DB contains extreme outliers, historical_pollution_flags must include them.
Stage2A historical positive must be labeled HISTORICAL_POSITIVE_NEEDS_REPLAY, not CURRENT_ENTERED_EV_POSITIVE.
Stage3 historical weak/negative must not be promoted.
```

#### Phase 5 — NOT_ATH kline candidate report

New script:

```text
scripts/build-live-fullnet-not-ath-kline-candidates.js
tests/build-live-fullnet-not-ath-kline-candidates.test.mjs
```

Inputs:

```text
--rows live-fullnet-row.jsonl
--kline-db optional
--raw-discovery optional
--out-dir artifact dir
```

Output:

```text
live-fullnet-not-ath-kline-candidates.json
```

Per candidate/template fields:

```text
candidate_name
eligible_n
feature_available_n
formal_dog_n
formal_dud_n
peak50_clean_n
peak100_clean_n
peak5x_clean_n
dog_precision
lift_vs_baseline
would_enter_n
entered_n
held_n
actual_net_ev_pct
actual_net_ev_missing_reason
counterfactual_replay_ev_pct
bar2_legal_replay_ev_pct
lookahead_risk_flag
missing_kline_n
confidence_warnings
```

Templates:

```text
FBR_GREEN
FBR_GE_1
FBR_GE_2
FBR_GE_3
RED_BAR
RED_LOW_VOLUME
RED_LOW_VOLUME_ACTIVE
THREE_BAR_SCORE_GE_2
UPPER_WICK_LOW
VOL_ACCEL_HIGH
```

Acceptance:

```text
FBR raw and legal-bar2 replay are separated.
RED-bar pullback and FBR green are separated.
If kline coverage is too low, report cannot promote any candidate.
```

#### Phase 6 — Historical evidence revalidation report

New script:

```text
scripts/build-live-fullnet-historical-evidence-revalidation.js
tests/build-live-fullnet-historical-evidence-revalidation.test.mjs
```

Output:

```text
live-fullnet-historical-evidence-revalidation.json
```

Required sections:

```text
historical_68_claims
stage_evidence
not_ath_kline_evidence
mc_si_ai_velocity_evidence
router_sleeve_evidence
tiny_probe_scout_evidence
bug_repair_evidence
```

Per claim fields:

```text
claim_id
claim_text
source_artifact
date_range
n
metric_type
denominator
time_legality
friction_assumption
known_issue
current_mapping_field
current_projection_status
recommended_status
```

Acceptance:

```text
The 68% claim must be split into separate candidate sources.
No claim with metric_type=movement_share can be labeled win_rate.
No claim without source_artifact can become CURRENT_FULLNET_CANDIDATE.
```

#### Phase 7 — Readout and decision board

New readout:

```text
claudedocs/fullnet-entry-model-kline-revalidation-readout-YYYY-MM-DD.md
```

Readout must include:

```text
1. Current fullnet denominator.
2. Actual entered/held/EV status.
3. Best current dog-enrichment components.
4. Best historical-positive-but-unverified candidates.
5. 68% claim reconciliation table.
6. Stage2A/Stage3 current status.
7. NOT_ATH kline candidate status.
8. Entry mode registry status.
9. Repair board:
   - timing-node repair
   - entry bridge repair
   - mode registry repair
   - kline data coverage repair
   - archive historical-only candidate
10. Explicit "do not change strategy yet" or "eligible repair candidate" verdict.
```

### §15.7 Command plan

Initial same-window rerun:

```bash
cd /Users/boliu/sas-research

node scripts/build-live-fullnet-row-report.js \
  --source-to-raw /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/source-raw/source-to-raw-rows.json \
  --source-rows /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/source-raw/source-rows.json \
  --raw-discovery /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/live-downloads/raw-dog-discovery-24h.json \
  --a-class-events /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/a-class-events-24h-complete.json \
  --ledger-export /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/canonical-ledger-window.json \
  --lifecycle-db /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/live-downloads/lifecycle_tracks.snapshot.db \
  --out-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current
```

Then:

```bash
node scripts/build-live-fullnet-entry-model-effectiveness.js \
  --rows /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current/live-fullnet-row.jsonl \
  --mode-registry /Users/boliu/sentiment-arbitrage-system/config/entry-mode-registry.json \
  --paper-db /Users/boliu/sentiment-arbitrage-system/data/paper_trades.db \
  --out-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current

node scripts/build-live-fullnet-not-ath-kline-candidates.js \
  --rows /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current/live-fullnet-row.jsonl \
  --raw-discovery /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/live-downloads/raw-dog-discovery-24h.json \
  --out-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current

node scripts/build-live-fullnet-historical-evidence-revalidation.js \
  --rows /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current/live-fullnet-row.jsonl \
  --entry-model /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current/live-fullnet-entry-model-effectiveness.json \
  --kline-candidates /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current/live-fullnet-not-ath-kline-candidates.json \
  --out-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-v2-current
```

Fresh online rerun after same-window implementation passes:

```text
Use scripts/run-fullnet-daily-audit.sh with safe canonical limit.
Then run the three new report builders against the fresh out-dir.
```

### §15.8 Tests and verification

Required tests:

```text
node --check:
  scripts/build-live-fullnet-row-report.js
  scripts/build-live-fullnet-entry-model-effectiveness.js
  scripts/build-live-fullnet-not-ath-kline-candidates.js
  scripts/build-live-fullnet-historical-evidence-revalidation.js

node --test:
  tests/build-live-fullnet-row-report.test.mjs
  tests/build-live-fullnet-entry-model-effectiveness.test.mjs
  tests/build-live-fullnet-not-ath-kline-candidates.test.mjs
  tests/build-live-fullnet-historical-evidence-revalidation.test.mjs
```

Specific invariant tests:

```text
1. entered_n=0 => actual_net_ev_pct=null.
2. FBR raw replay and legal bar2 replay are separate fields.
3. 68% movement_share cannot be labeled win_rate.
4. Stage2A historical positive cannot become current positive without current entered EV.
5. Mode registry source path/hash must be present.
6. Unknown/missing kline does not become false.
7. Runtime registry and research registry mismatch must be surfaced if both are provided.
8. No strategy/live/gate/entry/exit/size file is modified by these scripts.
```

Manual verification:

```text
1. Compare total_signals/dog/dud/pending with existing live-fullnet-summary.
2. Compare entered_total_n with canonical ledger/window.
3. Spot-check 5 rows:
   - one formal dog with would_enter;
   - one formal dog with quote_clean_no_would_enter;
   - one peak5x clean opportunity;
   - one no_decision row;
   - one shadow-only / policy block row.
4. Spot-check Stage2A historical DB numbers:
   - live_monitor n=33 avg_pnl=+0.0289 WR=48.48%
   - real_kline_replay n=24 avg_pnl=+0.0749 WR=54.17%
   These must be historical-only, not current-EV.
```

### §15.9 Definition of done

This goal is complete only when all are true:

```text
1. Plan document contains this §15 goal.
2. fullnet row v2 schema is implemented or explicitly queued with exact field map.
3. entry-model-effectiveness report is implemented and tested.
4. NOT_ATH kline-candidates report is implemented and tested.
5. historical-evidence-revalidation report is implemented and tested.
6. 68% claim is split into at least four candidate sources and cannot be confused with one strategy.
7. Current same-window artifact has been rerun.
8. Report states actual EV as null if entered=0.
9. Readout markdown is generated.
10. No live/gate/entry/exit/size behavior changed.
11. Final verdict says one of:
    - CURRENT REPAIR CANDIDATE FOUND;
    - HISTORICAL CANDIDATES ONLY, NEED MORE DATA;
    - ARCHIVE / DO NOT USE;
    - ENTRY BRIDGE BLOCKS EV MEASUREMENT.
```

### §15.10 Expected decisions after completion

If NOT_ATH kline/FBR survives current projection:

```text
Next repair family = timing node / SmartEntry readiness.
Do not directly make it a hard gate; first run shadow/paper legal-entry replay.
```

If Stage2A survives current projection:

```text
Next repair family = staged lifecycle recovery.
Implement only in isolated paper or shadow replay first.
```

If only would_enter survives but entered remains zero:

```text
Next repair family = entry bridge.
Split:
  would_enter_no_enqueue
  enqueue_no_order
  order_no_fill
  fill_no_ledger
  policy_shadow_only
```

If no historical clue survives current projection:

```text
Archive old clues.
Focus on current Markov/regime signal and entry bridge observability.
```

If current actual EV remains impossible:

```text
Do not optimize strategy.
First restore safe paper-entry/ledger measurement path or keep all conclusions as counterfactual only.
```

### §15.11 Additional system modules found in scan that should enter fullnet

This section records modules found by a repository scan after §15 was written.
The rule is still strict: only connect modules that directly affect whether a
gold/silver dog can be selected, admitted, entered, held, exited, or measured.

#### §15.11.1 Priority A — must enter fullnet row v2

These are decision-affecting today or already persisted in canonical/paper
tables. They should be projected before drawing repair conclusions.

1. GMGN enrichment / GMGN paper policy

Runtime source:

```text
sentiment-arbitrage-system/scripts/gmgn_policy.py
sentiment-arbitrage-system/scripts/gmgn_readonly.py
sentiment-arbitrage-system/scripts/analyze_gmgn_lotto_edge.py
sentiment-arbitrage-system/scripts/canonical_ledger.py: gmgn_policy_json
```

Why it belongs:

```text
GMGN policy can reject, downsize, boost, block explosive direct entry, or allow
tiny scout paths. It is not just metadata.
```

Fields to project:

```text
gmgn_policy_available
gmgn_policy_action                  # allow / reject / shadow_reject / downsize / boost
gmgn_policy_reason
gmgn_toxic_score
gmgn_edge_score
gmgn_size_multiplier
gmgn_spread_penalty_pct
gmgn_flags[]
gmgn_rat_trader_amount_rate
gmgn_entrapment_ratio
gmgn_creator_hold_rate
gmgn_dev_team_hold_rate
gmgn_bundler_rate
gmgn_bot_degen_rate
gmgn_top10_holder_rate
gmgn_top10_threshold_bucket
gmgn_sniper_count
gmgn_smart_degen_count
gmgn_renowned_count
gmgn_creator_close
gmgn_social_present
gmgn_cto_flag
gmgn_dexscr_link_updated
gmgn_paid_attention_present
gmgn_tiny_scout_allowed
gmgn_explosive_direct_blocked
missing_reason if unavailable
```

Audit question:

```text
Does GMGN policy protect dud without overblocking dog?
Which dog buckets are lost by gmgn_high_top10 / gmgn_toxic / bundler / rat flags?
Do GMGN boost/clean flags enrich dog, peak50, peak100, 5x, or 10x?
```

2. Opportunity freshness / staleness / latency

Runtime source:

```text
sentiment-arbitrage-system/scripts/opportunity_freshness.py
sentiment-arbitrage-system/scripts/source_resonance_shadow.py: latency_audit_events
sentiment-arbitrage-system/src/gates/freshness-filter.js
sentiment-arbitrage-system/scripts/v27_mirror_earliest_actionable_times.py
```

Why it belongs:

```text
The current failure mode is often "system sees strength late". Raw signal age
alone is not enough because quote, GMGN activity, reclaim, ATH refresh, or
repeat source hit can make an old token actionable again.
```

Fields to project:

```text
raw_signal_age_sec
opportunity_ts
opportunity_age_sec
opportunity_fresh
freshness_reason
freshness_sources[]                 # fresh_quote, fresh_momentum, fresh_gmgn_activity, fresh_reclaim, fresh_ath_refresh, fresh_source_hit
data_confidence
first_actionable_ts
first_actionable_lag_sec
decision_lag_sec
would_enter_lag_sec
entry_lag_sec
latency_stage_blocker
clock_skew_or_future_quote_flag
```

Audit question:

```text
Are dogs missed because they are stale, or because they became fresh again but
the net did not reopen them?
Do would_enter dogs become would_enter after the peak is already gone?
```

3. Token risk memory / quarantine / dead-dog pool

Runtime source:

```text
sentiment-arbitrage-system/scripts/paper_trade_monitor.py
sentiment-arbitrage-system/src/risk/dead-dog-pool.js
sentiment-arbitrage-system/src/risk/risk-manager.js
```

Why it belongs:

```text
Prior no-follow, spread-chase, waterfall, gap-crash, and quarantine memory can
block or tighten reentry. This directly affects reclaim/revival and Stage 2/3.
```

Fields to project:

```text
token_memory_available
token_prior_trade_count
token_prior_loss_count
token_prior_no_follow_count
token_prior_spread_chase_count
token_prior_waterfall_count
token_prior_gap_crash_count
token_prior_profit_gap_crash_count
token_quarantine_active
token_quarantine_reason
token_quarantine_ttl_sec
dead_dog_pool_hit
dead_dog_reason
reclaim_memory_class               # loss_failure / no_follow_failure / waterfall_memory / spread_chase_failure / profitable_volatility_exit
reclaim_required_m5
reclaim_required_bs
reclaim_required_rvol
reclaim_pass
```

Audit question:

```text
Is token memory correctly preventing repeated duds, or is it suppressing later
dog reactivation?
Are positive gap-crash winners incorrectly treated as loss failures?
```

4. Entry branch / capital tier / intervention flags

Runtime source:

```text
sentiment-arbitrage-system/scripts/paper_learning_policy.py
sentiment-arbitrage-system/scripts/paper_fast_lane.py
sentiment-arbitrage-system/scripts/paper_trade_monitor.py
sentiment-arbitrage-system/src/web/dashboard-server.js
```

Why it belongs:

```text
entry_mode alone is too coarse. The same mode can run as main, tiny probe,
rescue, source-resonance branch, learning bypass, or canary.
```

Fields to project:

```text
raw_entry_mode
normalized_entry_mode
entry_branch
parent_entry_branch
capital_tier                       # tiny_probe / small_probe / lotto_main / stage1_main
position_size_class
paper_only
paper_only_scout
regime_tag
policy_version
intervention_flags[]
learning_bypass_enabled
learning_bypass_reason
```

Audit question:

```text
Which branches actually enrich dogs?
Are dog would_enter rows sitting in paper-only/tiny/canary while main routes remain closed?
```

5. A-class internal submodules

Runtime source:

```text
sentiment-arbitrage-system/scripts/a_class_opportunity_matrix.py
sentiment-arbitrage-system/scripts/a_class_expected_rr.py
sentiment-arbitrage-system/scripts/a_class_rr_model.py
sentiment-arbitrage-system/scripts/a_class_runtime_safety.py
sentiment-arbitrage-system/scripts/a_class_exit_policy.py
sentiment-arbitrage-system/scripts/a_class_block_cause_breakdown.py
sentiment-arbitrage-system/scripts/canonical_ledger.py: freshness_json / budget_json / risk_json / expected_rr_detail_json / matrix_json / ai_review_json / controller_action_json
```

Why it belongs:

```text
The current plan says "matrix/score/RR", but the code already stores freshness,
budget, risk, expected RR, AI review, controller action, principal recovery,
moonbag, and discovery exit. Collapsing all of this into one matrix result hides
which submodule blocks dogs.
```

Fields to project:

```text
a_class_has_event
a_class_grade
a_class_score
a_class_action
a_class_would_action
a_class_hard_blockers[]
a_class_soft_notes[]
a_class_freshness_json
a_class_budget_json
a_class_risk_json
a_class_matrix_json
a_class_ai_review_json
a_class_controller_action_json
expected_rr
expected_upside_pct
defined_risk_pct
bottom_ticket_size_sol
denominator_key
discovery_exit_plan
principal_recovery_plan
moonbag_plan
runtime_safety_pass
runtime_safety_reason
```

Audit question:

```text
When quote-clean dogs fail would_enter, is the blocker freshness, budget,
risk, opportunity matrix, expected RR, controller action, or runtime safety?
```

#### §15.11.2 Priority B — high-value shadow/explanatory modules

These should enter fullnet as features and confidence/explanation fields. They
must not become live entry rules without same-window dog/dud + EV proof.

1. Source resonance and external alpha state, not just cohort

Runtime source:

```text
sentiment-arbitrage-system/scripts/source_resonance_shadow.py
sentiment-arbitrage-system/scripts/external_alpha_shadow.py
sentiment-arbitrage-system/src/web/dashboard-server.js: source_resonance_candidates / external_alpha_state / external_alpha_health
```

Fields to project:

```text
source_resonance_available
source_resonance_cohort
source_resonance_level
source_resonance_score
gmgn_pre_seen
gmgn_first_seen_ts
gmgn_last_seen_ts
gmgn_lead_time_sec
gmgn_seen_count
gmgn_momentum_rounds
gmgn_momentum_confirmed
gmgn_volume_confirmed
gmgn_buy_pressure
gmgn_last_market_cap
gmgn_last_liquidity
source_quote_clean_seen
source_two_quote_clean_snapshots
source_entry_quote_success_seen
source_entry_quote_fail_seen
external_alpha_state_present
external_alpha_health_status
```

Audit question:

```text
Does pre-seen/dual-source/quote-clean resonance enrich dogs beyond Telegram-only?
Does external alpha health explain missing GMGN/source fields on bad days?
```

2. Narrative / social / influencer scoring

Runtime source:

```text
sentiment-arbitrage-system/src/scoring/narrative-detector.js
sentiment-arbitrage-system/src/scoring/signal-narrative-features.js
sentiment-arbitrage-system/src/scoring/social-heat-scorer.js
sentiment-arbitrage-system/src/scoring/key-influencer-scorer.js
sentiment-arbitrage-system/src/scoring/soft-alpha-score.js
sentiment-arbitrage-system/src/web/dashboard-server.js: /api/narrative-effectiveness
```

Fields to project:

```text
narrative_score
narrative_tags[]
ai_narrative_tier
social_heat_score
key_influencer_score
soft_alpha_score
has_x_link
has_website
has_telegram
has_github
cto_detected
launchpad_detected
same_narrative_recent_count
```

Audit question:

```text
Does narrative explain dog concentration, or is it only noisy context?
Keep this shadow-only until proven.
```

3. Smart-money / wallet quality / OOS wallet spike

Runtime source:

```text
sentiment-arbitrage-system/src/inputs/gmgn-smart-money.js
sentiment-arbitrage-system/src/inputs/alpha-account-monitor.js
sentiment-arbitrage-system/src/tracking/smart-money-tracker.js
sentiment-arbitrage-system/src/execution/smart-money-scout.js
sas-research/scripts/build-oos-wallet-quality-spike-windows.js
sas-research/scripts/validate-oos-wallet-quality-spike.js
```

Fields to project:

```text
smart_money_present
smart_money_buy_count
smart_money_sell_count
smart_money_net_buy_share
smart_money_wallet_count
renowned_wallet_count
wallet_quality_spike
wallet_quality_score
oos_wallet_spike_window_id
shadow_wallet_consensus_score
key_risk_wallet_count
```

Audit question:

```text
Do smart-wallet/wallet-quality spikes identify dogs earlier than source/matrix?
Or are they sparse/late/noisy?
```

4. Curve / pumpfun / early curve-stage features

Runtime source:

```text
sentiment-arbitrage-system/src/inputs/chain-snapshot-sol.js
sas-research/scripts/build-v10-curve-feature-table.js
sas-research/scripts/run-v10-curve-feature-export-analysis.js
sas-research/claudedocs/v10-curve-feature-export-spec.md
```

Fields to project:

```text
curve_data_available
curve_stage
pumpfun_curve_progress
curve_liquidity_usd
curve_holder_count
curve_unique_buyers
curve_key_risk_wallets_count
curve_migration_state
curve_feature_bucket
```

Audit question:

```text
Are early dogs concentrated in specific curve stages, or are curve features only
liquidity/rug-risk controls?
```

#### §15.11.3 Priority C — evidence quality and blocker attribution

These are not alpha. They must enter row-level confidence and missing_reason so
the net does not confuse provider failure with strategy failure.

1. Provider / data-source health

Runtime source:

```text
sentiment-arbitrage-system/scripts/provider_budget.py
sentiment-arbitrage-system/src/web/dashboard-server.js: data-source-health / external_alpha_health
sentiment-arbitrage-system/config/v27-runtime-pipeline-policy.json
sentiment-arbitrage-system/config/v27-source-registry.json
sentiment-arbitrage-system/config/v27-feature-vector-snapshot-policy.json
```

Fields to project:

```text
provider_budget_available
provider_rate_limited
provider_timeout
gmgn_gateway_ok
quote_provider_ok
kline_provider_ok
external_alpha_health_ok
source_registry_route
feature_vector_snapshot_complete
data_source_health_status
```

2. Scout quality / soft override reasons

Runtime source:

```text
sentiment-arbitrage-system/scripts/scout_quality.py
sentiment-arbitrage-system/scripts/paper_learning_policy.py
sentiment-arbitrage-system/tests/test_missed_dog_blocker_ranking.py
```

Fields to project:

```text
scout_quality_pass
scout_quality_reason
scout_quality_liquidity_low
scout_quality_buy_pressure_weak
scout_quality_volume_low
scout_quality_tx_low
scout_quality_negative_trend
soft_override_applied
soft_override_reason
```

3. Paper-learning / mode-readiness / scorecards

Runtime source:

```text
sentiment-arbitrage-system/scripts/paper_learning_policy.py
sentiment-arbitrage-system/scripts/entry_mode_scorecard.py
sentiment-arbitrage-system/scripts/strategy_reflection_score.py
sentiment-arbitrage-system/scripts/v27_mode_readiness.py
sentiment-arbitrage-system/scripts/v27_runtime_mode_gate.py
```

Fields to project:

```text
mode_registry_tier
mode_paper_enabled
mode_blocks_live
mode_promotion_state
mode_demotion_state
mode_daily_cap_state
mode_loss_cap_state
strategy_reflection_score
entry_mode_scorecard_status
```

4. Paper evidence / integrity audit

Runtime source:

```text
sentiment-arbitrage-system/scripts/paper_evidence_log.py
sentiment-arbitrage-system/scripts/paper_trade_integrity_audit.py
sentiment-arbitrage-system/scripts/paper_review_snapshot.py
```

Fields to project:

```text
evidence_quality_status
paper_integrity_pass
paper_integrity_reason
ledger_join_confidence
decision_join_confidence
row_confidence_warning[]
```

5. Global risk manager / capacity blocks

Runtime source:

```text
sentiment-arbitrage-system/src/risk/risk-manager.js
sentiment-arbitrage-system/scripts/paper_trade_monitor.py
```

Fields to project:

```text
budget_blocked
budget_block_reason
daily_loss_circuit_active
consecutive_loss_circuit_active
max_position_blocked
balance_blocked
route_capacity_blocked
mode_capacity_blocked
```

#### §15.11.4 Explicit exclusions for primary fullnet

These should not enter the primary gold/silver capture net unless a separate
goal makes them row-level decision inputs.

```text
1. OI + funding futures scanner:
   Different market/domain. Keep as separate futures radar if used.

2. Pure dashboard/UI endpoints:
   Include only when they expose row-level decision/evidence fields.

3. Log redaction/security policies:
   Important for operations, not dog/dud capture attribution.

4. Generic v27 governance documents:
   Include only source registry, threshold catalog, entry point inventory,
   metric definition registry, runtime gate, and data freshness policies that
   are actually referenced by a row-level decision.

5. Wallet balance display:
   Include only if it produced budget_blocked / balance_blocked.
```

#### §15.11.5 Updated implementation implication

The fullnet row v2 must support two classes of modules:

```text
decision_modules:
  source, raw, lifecycle, Markov, filters/gates, matrix/RR,
  entry_readiness, GMGN policy, token risk memory, quote/route,
  final entry, execution bridge, ledger, exit/hold, friction.

confidence_modules:
  provider health, freshness/latency, source resonance health,
  evidence integrity, mode registry, scout quality, narrative/social,
  smart-money/wallet, curve-stage.
```

Do not rank a module as "bad strategy" if its confidence module says the data
was unavailable, stale, rate-limited, or joined with low confidence.

## §15.12 GOAL — deep-source and shadow-fact expansion for fullnet row v2

### §15.12.1 Goal statement

Second-pass repository and local-DB scan found several deeper fact sources that
are not yet represented in the fullnet plan. This goal extends §15.11 from
runtime modules into persisted signal facts, shadow outcomes, blacklists,
cooldowns, experiments, source-quality memory, and kline/provider provenance.

The objective is:

```text
For every (token_ca, signal_ts) row, fullnet must be able to explain not only
what the main paper/canonical path did, but also whether the signal appeared in:
  - source snapshot tables;
  - rejected/passed/watch signal tables;
  - shadow PnL/outcome trackers;
  - source/channel/KOL performance memory;
  - blacklist/cooldown/experiment governance;
  - kline/pool/provider provenance;
  - lifecycle standalone DB;
  - paper path samples and partial-accounting samples.

Every one of these must be either:
  - projected into the row;
  - marked missing with a precise missing_reason;
  - explicitly excluded from primary capture attribution.
```

This is still research-only:

```text
No live gate change.
No entry change.
No exit change.
No size change.
No source promotion.
No old-table result is treated as current edge until same-window dog/dud + EV
projection proves it.
```

### §15.12.2 Deep scan inventory

Local DB scan found these relevant stores:

```text
sentiment-arbitrage-system/data/paper_trades.db
  external_alpha_health
  external_alpha_snapshots
  external_alpha_state
  paper_decision_events
  paper_missed_signal_attribution
  paper_trade_path_samples
  paper_trades

sentiment-arbitrage-system/data/sentiment_arb.db
  premium_signals
  telegram_signals
  tokens
  gates
  rejected_signals
  passed_signals
  watch_signals
  signal_outcomes
  shadow_pnl
  shadow_price_tracking
  signal_features
  signal_feature_enrichments
  score_details
  channel_performance
  signal_source_performance
  social_snapshots
  narrative_heat
  ai_narratives
  ai_twitter_kols
  ai_telegram_channels
  smart_money_activity
  hunter_signals
  hunter_performance
  scout_positions
  permanent_blacklist
  exit_cooldown
  strategy_experiments
  strategy_research_memory
  autonomy_events
  autonomy_runs
  performance_snapshots
  module_performance
  threshold_history
  dynamic_thresholds

sentiment-arbitrage-system/data/watchlist.db
  watchlist

sentiment-arbitrage-system/data/lifecycle_tracks.db
  tracks
  price_samples
  strategy_results

sentiment-arbitrage-system/data/kline_cache.db
  kline_1m
  pool_mapping
  helius_trades
  history_backfill_cursor
  fetch_log
```

Implementation must not assume only one DB is canonical. Each row should record
which fact sources were present and which were absent.

### §15.12.3 Priority A — deep fact sources that must enter the row

#### A1. Signal snapshot recorder: rejected / passed / watch signals

Runtime source:

```text
sentiment-arbitrage-system/src/database/signal-snapshot-recorder.js
sentiment-arbitrage-system/data/sentiment_arb.db:
  rejected_signals
  passed_signals
  watch_signals
```

Why it belongs:

```text
These tables record signals before they become paper trades. Without them,
fullnet can explain only the rows that entered paper/canonical paths, not the
rows that were rejected or left in watch state upstream.
```

Fields to project:

```text
snapshot_rejected_seen
snapshot_rejection_stage
snapshot_rejection_reason
snapshot_rejection_factors_json
snapshot_gate_type
snapshot_threshold_used
snapshot_price_at_rejection
snapshot_mcap_at_rejection
snapshot_liquidity_at_rejection
snapshot_sm_count_at_rejection
snapshot_safety_score
snapshot_decision_source

snapshot_passed_seen
snapshot_passed_threshold_used
snapshot_passed_final_pnl
snapshot_passed_is_gold

snapshot_watch_seen
snapshot_watch_reason
snapshot_watch_context
snapshot_watch_final_decision
snapshot_watch_final_pnl
```

Audit question:

```text
Are current dogs missing because they never reached paper, because they were
upstream rejected, or because they stayed in watch state?
Which old rejection gates accidentally block peak50/peak100/5x opportunities?
```

#### A2. Gates table: chain/security/exit gate truth

Runtime source:

```text
sentiment-arbitrage-system/src/inputs/chain-snapshot.js
sentiment-arbitrage-system/src/inputs/chain-snapshot-sol.js
sentiment-arbitrage-system/data/sentiment_arb.db:gates
```

Why it belongs:

```text
source hard_gate_status is not the full security truth. The gates table has
LP/mint/freeze, honeypot/tax, top10, liquidity, slippage-sell, wash flag, and
key risk wallets. These can block or justify rejecting a dog.
```

Fields to project:

```text
chain_gate_seen
hard_status
hard_reasons[]
freeze_authority
mint_authority
lp_status
honeypot
tax_buy
tax_sell
tax_mutable
owner_type
dangerous_functions[]
exit_status
exit_reasons[]
top10_percent_gate
liquidity_gate_value
liquidity_unit
slippage_sell_20pct
wash_flag
key_risk_wallets_count
sell_constraints_flag
```

Audit question:

```text
Did source gate overblock dogs, or did chain/security gate correctly identify
untradable/rug-prone dogs?
```

#### A3. Shadow outcome trackers

Runtime source:

```text
sentiment-arbitrage-system/src/tracking/shadow-pnl-tracker.js
sentiment-arbitrage-system/src/tracking/shadow-price-tracker.js
sentiment-arbitrage-system/data/sentiment_arb.db:
  shadow_pnl
  shadow_price_tracking
  signal_outcomes
```

Why it belongs:

```text
The system may have observed a signal in shadow even when paper/live entry was
zero. These tables can provide counterfactual trajectory with entry_time,
high_pnl, low_pnl, max_pnl, time_to_peak, and drawdown.
```

Fields to project:

```text
shadow_pnl_seen
shadow_score
shadow_entry_mc
shadow_high_pnl
shadow_low_pnl
shadow_exit_pnl
shadow_exit_reason
shadow_closed

shadow_price_tracking_seen
shadow_entry_price
shadow_entry_liquidity
shadow_pnl_5m
shadow_pnl_15m
shadow_pnl_1h
shadow_max_pnl
shadow_min_pnl
shadow_tracking_status

signal_outcome_seen
signal_outcome_source_type
signal_outcome_source_id
signal_outcome_pnl_percent
signal_outcome_is_winner
signal_outcome_time_to_peak_min
signal_outcome_max_gain_percent
signal_outcome_max_drawdown_percent
```

Audit question:

```text
Was a dog completely unseen, or seen in shadow but never promoted into paper?
Did shadow have earlier/cleaner entry than the current paper decision path?
```

#### A4. Paper trade path samples and partial-accounting path

Runtime source:

```text
sentiment-arbitrage-system/data/paper_trades.db:paper_trade_path_samples
sentiment-arbitrage-system/scripts/paper_trade_monitor.py
sentiment-arbitrage-system/scripts/exit_engine.py
```

Why it belongs:

```text
Ledger entry/exit rows are not enough to audit holding quality. Path samples
contain mark vs quote PnL, partial sells, quote failures, sold_pct, and blended
PnL. This is required to judge exit/hold/moonbag and quote sanity.
```

Fields to project:

```text
path_samples_seen
path_sample_count
path_first_sample_ts
path_last_sample_ts
path_peak_mark_pnl
path_peak_quote_pnl
path_min_mark_pnl
path_min_quote_pnl
path_quote_success_rate
path_quote_failure_count
path_quote_failure_reasons[]
path_partial_sell_count
path_max_sold_pct
path_partial_realized_sol
path_remaining_cost_basis_sol
path_blended_mark_pnl
path_blended_quote_pnl
path_mark_quote_gap_peak
```

Audit question:

```text
Were entered dogs lost because exit/hold cut too early, quote failed, partial
accounting was wrong, or mark/quote diverged?
```

#### A5. Cooldown / permanent blacklist / source blacklist

Runtime source:

```text
sentiment-arbitrage-system/src/database/exit-cooldown.js
sentiment-arbitrage-system/src/database/permanent-blacklist.js
sentiment-arbitrage-system/data/sentiment_arb.db:
  exit_cooldown
  permanent_blacklist
```

Why it belongs:

```text
These are direct blockers for reentry. A later dog can be missed because the
same token was cooled down or permanently blacklisted by an earlier exit.
```

Fields to project:

```text
exit_cooldown_active
exit_cooldown_reason
exit_cooldown_until
exit_cooldown_remaining_min
exit_cooldown_prior_pnl
exit_cooldown_position_id

permanent_blacklist_hit
blacklist_reason
blacklist_timestamp
blacklist_initial_liquidity
blacklist_final_liquidity
blacklist_deployer_address
blacklist_additional_data_json
```

Audit question:

```text
Are reclaim/revival dogs suppressed by correct safety memory, stale cooldown,
or overbroad permanent blacklist logic?
```

#### A6. Watchlist DB / active watch state

Runtime source:

```text
sentiment-arbitrage-system/data/watchlist.db:watchlist
sentiment-arbitrage-system/scripts/watchlist_store.py
sentiment-arbitrage-system/scripts/entry_engine.py
```

Why it belongs:

```text
The watchlist is the bridge between source signal and entry engine. A signal can
be accepted into watch, expire there, arm there, or be blocked by watch state
before paper trade exists.
```

Fields to project after schema inspection:

```text
watchlist_seen
watchlist_status
watchlist_created_ts
watchlist_updated_ts
watchlist_expired_ts
watchlist_reason
watchlist_entry_mode
watchlist_entry_branch
watchlist_pending_age_sec
watchlist_cooldown_until
watchlist_last_action
watchlist_state_json
```

Audit question:

```text
Do dogs die inside watchlist/pending before SmartEntry/final-entry records them?
```

#### A7. Lifecycle standalone DB

Runtime source:

```text
sentiment-arbitrage-system/data/lifecycle_tracks.db:
  tracks
  price_samples
  strategy_results
sentiment-arbitrage-system/scripts/lifecycle_24h_tracker.py
sentiment-arbitrage-system/scripts/lifecycle_classifier.py
```

Why it belongs:

```text
Earlier fullnet saw paper_missed lifecycle snapshots but standalone lifecycle
tracks had low overlap. The standalone lifecycle DB must be projected separately
so we can distinguish "lifecycle did not run" from "lifecycle ran and classified".
```

Fields to project after schema inspection:

```text
lifecycle_track_seen
lifecycle_track_id
lifecycle_track_state
lifecycle_track_started_ts
lifecycle_track_updated_ts
lifecycle_track_sample_count
lifecycle_track_peak_pnl
lifecycle_track_strategy_result
lifecycle_track_missing_reason
```

Audit question:

```text
Is lifecycle a true route selector on current dogs, or only a sidecar logger?
```

#### A8. Kline / pool / provider provenance

Runtime source:

```text
sentiment-arbitrage-system/data/kline_cache.db:
  kline_1m
  pool_mapping
  helius_trades
  history_backfill_cursor
  fetch_log
sentiment-arbitrage-system/src/market-data/kline-repository.js
```

Why it belongs:

```text
Entry-bar/FBR/RED-low-volume-active hypotheses depend entirely on correct pool
mapping and kline provenance. If the bar came from a wrong pool, partial current
bar, stale fetch, or failed backfill, the factor result is invalid.
```

Fields to project:

```text
pool_mapping_seen
pool_address
pool_provider
pool_mapping_fetched_at
kline_provider
kline_fetch_status
kline_fetch_error
kline_backfill_status
kline_bar_count_0_5m
kline_bar_count_0_15m
kline_first_bar_complete
kline_first_bar_source
kline_current_bar_partial_flag
helius_trade_count_window
```

Audit question:

```text
Are FBR/kline microstructure conclusions real, or artifacts of missing/wrong/
partial kline data?
```

### §15.12.4 Priority B — source quality, social, and research memory

These are not hard decision blockers by themselves, but they can explain source
quality and should enter separability as shadow features.

#### B1. Channel/source performance memory

Runtime source:

```text
sentiment-arbitrage-system/data/sentiment_arb.db:
  channel_performance
  signal_source_performance
  telegram_channels
  telegram_signals
sentiment-arbitrage-system/src/scoring/signal-source-optimizer.js
```

Fields:

```text
telegram_channel_name
telegram_channel_username
telegram_channel_tier
source_quality_status
source_quality_tier
source_total_signals
source_win_rate
source_avg_pnl
source_first_signal_rate
source_avg_time_advantage_min
channel_reject_ratio_24h
channel_avg_pnl_30_120
channel_is_upstream
channel_matrix_flags
```

Audit question:

```text
Are dogs concentrated in specific source/channel tiers, and are duds coming
from lower-quality sources?
```

#### B2. AI/KOL/narrative memory tables

Runtime source:

```text
sentiment-arbitrage-system/data/sentiment_arb.db:
  ai_narratives
  ai_twitter_kols
  ai_telegram_channels
  narrative_heat
  score_details
  social_snapshots
```

Fields:

```text
ai_narrative_name
ai_narrative_weight
ai_narrative_lifecycle_stage
ai_narrative_multiplier
narrative_heat_score
narrative_token_count_24h
narrative_avg_performance
kol_tier_hit
kol_reliability_score
telegram_ai_channel_reliability
tg_velocity
tg_accel
tg_clusters_15m
x_unique_authors_15m
x_tier1_hit
score_detail_narrative_score
score_detail_influence_score
score_detail_tg_spread_score
score_detail_graph_score
score_detail_source_score
score_detail_total_score
```

Audit question:

```text
Do narrative/social features add separability after source/lifecycle/Markov, or
are they redundant/noisy?
```

#### B3. Smart-wallet / hunter / scout memory

Runtime source:

```text
sentiment-arbitrage-system/data/sentiment_arb.db:
  smart_money_activity
  hunter_signals
  hunter_performance
  scout_positions
sentiment-arbitrage-system/src/inputs/shadow-protocol.js
sentiment-arbitrage-system/src/inputs/ultra-human-sniper-v2.js
sentiment-arbitrage-system/src/execution/smart-money-scout.js
```

Fields:

```text
smart_money_activity_seen
smart_money_buy_usd_window
smart_money_sell_usd_window
smart_money_net_usd_window
smart_money_wallet_count_window
hunter_signal_seen
hunter_type
hunter_score
hunter_performance_period_win_rate
hunter_performance_avg_pnl
scout_position_seen
scout_position_status
scout_position_confirmed
scout_position_pnl_percent
```

Audit question:

```text
Do wallet/hunter/scout signals identify dogs earlier than Telegram, or only
create duplicate/noisy candidates?
```

#### B4. Research memory / experiments / threshold history

Runtime source:

```text
sentiment-arbitrage-system/data/sentiment_arb.db:
  strategy_experiments
  strategy_research_memory
  threshold_history
  dynamic_thresholds
  iteration_history
  ai_review_history
  module_performance
  performance_snapshots
```

Fields:

```text
experiment_candidate_id
experiment_status
experiment_guardrail_pass
experiment_metrics_json
experiment_promoted_at
experiment_paused_at
research_memory_hit
research_memory_status
dynamic_threshold_version
threshold_history_recent_change
ai_review_recent_priority_action
module_performance_snapshot
```

Audit question:

```text
Was a row affected by an active experiment, dynamic threshold, or recently
changed strategy setting? If yes, separability must be segmented by version.
```

### §15.12.5 Priority C — v27 governance mirrors that should become confidence fields

The v27 files are not all strategy modules. Most are governance/config health.
Only project the row-relevant confidence outputs.

Relevant files:

```text
config/v27-source-registry.json
config/v27-threshold-catalog.json
config/v27-entry-point-inventory.json
config/v27-metric-definition-registry.json
config/v27-runtime-pipeline-policy.json
config/v27-read-model-snapshot-policy.json
config/v27-read-model-freshness-policy if present
config/v27-feature-vector-snapshot-policy.json
config/v27-release-experiment-safety-policy.json
config/v27-runtime-config-drift-policy.json
config/v27-capacity-load-latency-policy.json
scripts/v27_mirror_paper_decisions.py
scripts/v27_mirror_paper_ledgers.py
scripts/v27_mirror_source_labels.py
scripts/v27_mirror_telegram_signals.py
scripts/v27_mirror_lifecycle_tracks.py
scripts/v27_mirror_trade_outcomes.py
scripts/v27_mirror_realtime_clean.py
scripts/v27_mirror_quote_intent_bindings.py
scripts/v27_read_model_freshness.py
scripts/v27_denominator_projection.py
```

Fields:

```text
v27_source_registry_route
v27_entry_point_id
v27_metric_definition_version
v27_threshold_catalog_version
v27_runtime_pipeline_version
v27_read_model_fresh
v27_read_model_age_sec
v27_feature_vector_complete
v27_config_drift_detected
v27_capacity_latency_state
v27_mirror_paper_decision_seen
v27_mirror_ledger_seen
v27_mirror_lifecycle_seen
v27_mirror_quote_intent_seen
```

Audit question:

```text
Was a missing row a true strategy miss, or a mirror/read-model/config freshness
problem?
```

### §15.12.6 Row source join contract

The fullnet builder must add a `source_presence` object for each row:

```json
{
  "paper_trades_db": true,
  "sentiment_arb_db": true,
  "watchlist_db": true,
  "lifecycle_tracks_db": true,
  "kline_cache_db": true,
  "canonical_export": true,
  "source_to_raw_export": true,
  "raw_outcome_export": true
}
```

And each deep source must have one of:

```text
present
missing_table
missing_db
missing_key
out_of_window
low_confidence_join
not_applicable
excluded_by_policy
```

No token-only silent join is allowed. Join priority:

```text
1. exact (token_ca, signal_ts)
2. premium_signal_id / telegram_signal_id / remote_signal_id
3. lifecycle_id
4. bounded same-token same-route window, only when one candidate exists
5. otherwise unmatched with missing_reason
```

### §15.12.7 Reports to add

Add these reports after row v2 schema exists:

```text
live-fullnet-deep-source-presence.json
  Per source table: present count, missing reason count, join confidence.

live-fullnet-upstream-snapshot-funnel.json
  rejected_signals / passed_signals / watch_signals / premium_signals
  by dog/dud/peak50/peak100/5x/10x.

live-fullnet-shadow-outcome-contribution.json
  shadow_pnl / shadow_price_tracking / signal_outcomes contribution and
  whether shadow caught dogs before paper.

live-fullnet-security-memory-contribution.json
  gates / blacklist / cooldown / token memory by dog/dud and opportunity lost.

live-fullnet-source-quality-contribution.json
  channel/source/KOL/narrative/wallet quality features by dog/dud.

live-fullnet-kline-provenance-report.json
  kline/pool/provider completeness for FBR/RED-low-volume-active validation.

live-fullnet-versioned-experiment-report.json
  dynamic threshold / experiment / mode registry version segments.
```

### §15.12.8 Tests and verification

Required tests:

```text
1. Deep source table absence does not crash the builder.
2. rejected_signals exact join projects rejection_stage/reason.
3. watch_signals exact join projects watch_reason/final_decision.
4. shadow_price_tracking projects max_pnl/time buckets without treating it as actual EV.
5. permanent_blacklist and exit_cooldown create security_memory blockers.
6. kline provenance missing marks FBR/entry-bar factors low confidence.
7. same token with multiple signals does not token-only join wrong shadow/outcome row.
8. v27 mirror missing marks confidence missing, not strategy failure.
9. source_presence object is present on every row.
10. report denominators reconcile with row.jsonl.
```

Manual verification:

```text
1. Pick one premium signal that appears in rejected_signals and confirm row
   primary blocker reflects snapshot_rejection only if no later stronger row
   component exists.
2. Pick one watch_signals token and confirm it is not counted as entered.
3. Pick one shadow_pnl winner and confirm it is counterfactual, not actual EV.
4. Pick one blacklisted/cooldown token and confirm reentry blocker attribution.
5. Pick one FBR candidate and confirm kline provider/pool/partial-bar status.
```

### §15.12.9 Definition of done

This goal is complete when:

```text
1. §15.12 is written into this document.
2. Fullnet row v2 has source_presence.
3. Row v2 can read paper_trades.db, sentiment_arb.db, watchlist.db,
   lifecycle_tracks.db, and kline_cache.db when available.
4. Priority A deep facts are projected with missing_reason.
5. At least one report exists for deep source presence.
6. Upstream rejected/passed/watch tables are included in dog/dud + opportunity
   denominator audit.
7. Shadow outcome tables are included but explicitly marked counterfactual.
8. Blacklist/cooldown/gates are included as security_memory, not alpha.
9. Kline provenance gates any FBR/entry-bar conclusion.
10. No live behavior changes.
```

### §15.12.10 Updated priority order after deep scan

Updated implementation order:

```text
P0: source_presence + multi-DB reader + exact join discipline.
P1: rejected/passed/watch + gates + blacklist/cooldown.
P2: shadow_pnl/shadow_price_tracking/signal_outcomes.
P3: watchlist/lifecycle_tracks/kline provenance.
P4: source quality / narrative / wallet / hunter.
P5: v27 mirror/config confidence.
P6: experiment/research/threshold version segmentation.
```

Do not start tuning entry strategy until P0-P3 are in place, because otherwise
the system may still mislabel an upstream reject, watchlist expiry, shadow-only
catch, kline artifact, or security-memory block as an entry/model failure.

## §15.13 GOAL — runtime decision/confidence module expansion for fullnet row v2

### §15.13.1 Goal statement

This goal formalizes the first-pass system scan in §15.11. That scan found
runtime decision modules and confidence modules that are already implemented or
partially persisted, but not yet represented as first-class fullnet row fields.

The objective is:

```text
For every (token_ca, signal_ts), fullnet row v2 must project every runtime
module that can directly change:
  - whether the signal is allowed into observation;
  - whether it becomes would_enter;
  - whether would_enter becomes enqueue/order/fill/ledger;
  - whether entry size/capital tier changes;
  - whether reentry/revival is blocked by risk memory;
  - whether exit/hold/moonbag can preserve the dog move;
  - whether a missing module is a data-quality problem rather than strategy
    failure.
```

This goal is not to turn those modules on or tune them. It is to make their
contribution measurable.

Non-goals:

```text
No live gate change.
No entry change.
No exit change.
No size change.
No source promotion.
No direct use of narrative/wallet/curve as live rules.
No declaring edge from a module until same-window dog/dud + actual/counterfactual
EV reports support it.
```

### §15.13.2 Module scope

The runtime module expansion covers §15.11's decision and confidence modules:

```text
decision modules:
  GMGN enrichment / GMGN policy
  opportunity freshness / latency
  token risk memory / quarantine / dead-dog
  entry branch / capital tier / intervention flags
  A-class internal submodules
  final entry contract sub-blockers
  execution bridge sub-blockers
  exit/hold/phase policy details

confidence / explanatory modules:
  source resonance / external alpha state
  narrative / social / influencer scoring
  smart-money / wallet quality / OOS wallet spike
  curve / pumpfun / early curve stage
  provider / data-source health
  scout quality / soft override reasons
  paper learning / mode readiness / scorecards
  paper evidence / integrity audit
  global risk manager / capacity blocks
```

### §15.13.3 Required row schema groups

#### G1. GMGN policy projection

Fields:

```text
gmgn_policy_available
gmgn_policy_action
gmgn_policy_reason
gmgn_toxic_score
gmgn_edge_score
gmgn_size_multiplier
gmgn_spread_penalty_pct
gmgn_flags[]
gmgn_rat_trader_amount_rate
gmgn_entrapment_ratio
gmgn_creator_hold_rate
gmgn_dev_team_hold_rate
gmgn_bundler_rate
gmgn_bot_degen_rate
gmgn_top10_holder_rate
gmgn_top10_threshold_bucket
gmgn_sniper_count
gmgn_smart_degen_count
gmgn_renowned_count
gmgn_creator_close
gmgn_social_present
gmgn_cto_flag
gmgn_dexscr_link_updated
gmgn_paid_attention_present
gmgn_tiny_scout_allowed
gmgn_explosive_direct_blocked
gmgn_missing_reason
```

Report outputs:

```text
gmgn_policy_by_raw_label
gmgn_policy_by_opportunity_bucket
gmgn_policy_blocked_dogs
gmgn_policy_saved_duds
gmgn_boost_vs_baseline_lift
```

#### G2. Freshness / latency projection

Fields:

```text
raw_signal_age_sec
opportunity_ts
opportunity_age_sec
opportunity_fresh
freshness_reason
freshness_sources[]
data_confidence
first_actionable_ts
first_actionable_lag_sec
decision_lag_sec
would_enter_lag_sec
entry_lag_sec
latency_stage_blocker
clock_skew_or_future_quote_flag
```

Report outputs:

```text
dog_time_to_decision_distribution
would_enter_after_peak_warning
late_decision_dog_count
fresh_reclaim_dog_count
stale_reject_dog_count
```

#### G3. Token risk memory projection

Fields:

```text
token_memory_available
token_prior_trade_count
token_prior_loss_count
token_prior_no_follow_count
token_prior_spread_chase_count
token_prior_waterfall_count
token_prior_gap_crash_count
token_prior_profit_gap_crash_count
token_quarantine_active
token_quarantine_reason
token_quarantine_ttl_sec
dead_dog_pool_hit
dead_dog_reason
reclaim_memory_class
reclaim_required_m5
reclaim_required_bs
reclaim_required_rvol
reclaim_pass
```

Report outputs:

```text
token_memory_blocked_dogs
token_memory_saved_duds
waterfall_memory_reclaim_outcomes
cooldown_or_quarantine_false_negative_list
```

#### G4. Entry branch / capital tier projection

Fields:

```text
raw_entry_mode
normalized_entry_mode
entry_mode_registry_tier
entry_mode_paper_enabled
entry_branch
parent_entry_branch
capital_tier
position_size_class
position_size_sol
paper_only
paper_only_scout
regime_tag
policy_version
intervention_flags[]
learning_bypass_enabled
learning_bypass_reason
```

Report outputs:

```text
entry_branch_contribution
capital_tier_contribution
paper_only_vs_main_capture
mode_registry_tier_capture
learning_bypass_capture
```

#### G5. A-class internal projection

Fields:

```text
a_class_has_event
a_class_grade
a_class_score
a_class_action
a_class_would_action
a_class_hard_blockers[]
a_class_soft_notes[]
a_class_freshness_json
a_class_budget_json
a_class_risk_json
a_class_matrix_json
a_class_ai_review_json
a_class_controller_action_json
expected_rr
expected_upside_pct
defined_risk_pct
bottom_ticket_size_sol
denominator_key
discovery_exit_plan
principal_recovery_plan
moonbag_plan
runtime_safety_pass
runtime_safety_reason
```

Report outputs:

```text
a_class_submodule_blocker_breakdown
quote_clean_no_would_enter_a_class_breakdown
expected_rr_by_dog_dud
budget_state_by_opportunity_bucket
risk_state_by_opportunity_bucket
```

#### G6. Source resonance / external alpha projection

Fields:

```text
source_resonance_available
source_resonance_cohort
source_resonance_level
source_resonance_score
gmgn_pre_seen
gmgn_first_seen_ts
gmgn_last_seen_ts
gmgn_lead_time_sec
gmgn_seen_count
gmgn_momentum_rounds
gmgn_momentum_confirmed
gmgn_volume_confirmed
gmgn_buy_pressure
gmgn_last_market_cap
gmgn_last_liquidity
source_quote_clean_seen
source_two_quote_clean_snapshots
source_entry_quote_success_seen
source_entry_quote_fail_seen
external_alpha_state_present
external_alpha_health_status
```

Report outputs:

```text
source_resonance_lift_vs_telegram_only
gmgn_pre_seen_dog_enrichment
quote_clean_snapshot_conversion
external_alpha_health_missing_impact
```

#### G7. Narrative / social / influencer projection

Fields:

```text
narrative_score
narrative_tags[]
ai_narrative_tier
social_heat_score
key_influencer_score
soft_alpha_score
has_x_link
has_website
has_telegram
has_github
cto_detected
launchpad_detected
same_narrative_recent_count
```

Report outputs:

```text
narrative_tag_dog_lift
narrative_score_bucket_contribution
social_presence_vs_quote_clean
influencer_score_vs_dog_rate
```

This report must label narrative/social as shadow-only unless same-window EV
evidence later proves it.

#### G8. Smart-money / wallet quality projection

Fields:

```text
smart_money_present
smart_money_buy_count
smart_money_sell_count
smart_money_net_buy_share
smart_money_wallet_count
renowned_wallet_count
wallet_quality_spike
wallet_quality_score
oos_wallet_spike_window_id
shadow_wallet_consensus_score
key_risk_wallet_count
```

Report outputs:

```text
wallet_quality_dog_lift
smart_money_net_buy_share_by_raw_label
wallet_signal_earliness_vs_source
wallet_feature_coverage_rate
```

#### G9. Curve / pumpfun / early stage projection

Fields:

```text
curve_data_available
curve_stage
pumpfun_curve_progress
curve_liquidity_usd
curve_holder_count
curve_unique_buyers
curve_key_risk_wallets_count
curve_migration_state
curve_feature_bucket
```

Report outputs:

```text
curve_stage_dog_lift
pumpfun_progress_vs_quote_clean
curve_unique_buyers_vs_peak50
curve_missing_coverage_warning
```

#### G10. Provider health / scout quality / evidence integrity projection

Fields:

```text
provider_budget_available
provider_rate_limited
provider_timeout
gmgn_gateway_ok
quote_provider_ok
kline_provider_ok
external_alpha_health_ok
source_registry_route
feature_vector_snapshot_complete
data_source_health_status

scout_quality_pass
scout_quality_reason
scout_quality_liquidity_low
scout_quality_buy_pressure_weak
scout_quality_volume_low
scout_quality_tx_low
scout_quality_negative_trend
soft_override_applied
soft_override_reason

evidence_quality_status
paper_integrity_pass
paper_integrity_reason
ledger_join_confidence
decision_join_confidence
row_confidence_warning[]

budget_blocked
budget_block_reason
daily_loss_circuit_active
consecutive_loss_circuit_active
max_position_blocked
balance_blocked
route_capacity_blocked
mode_capacity_blocked
```

Report outputs:

```text
provider_health_missing_impact
scout_quality_blocked_dogs
soft_override_outcomes
evidence_integrity_warning_count
budget_capacity_blocked_dogs
```

### §15.13.4 Implementation phases

#### Phase 1 — schema-only expansion

Add row v2 fields and missing_reason fields for G1-G10.

Rules:

```text
All new nested objects must include:
  available
  matched_by
  source_table_or_payload
  missing_reason
  confidence
```

No report should assume a missing module means fail. Missing module means
unknown unless that module is required for a specific route.

#### Phase 2 — parser / normalizer expansion

Add parsers for:

```text
gmgn_policy_json
freshness_json
budget_json
risk_json
expected_rr_detail_json
matrix_json
ai_review_json
controller_action_json
entry_execution_audit_json
exit_execution_audit_json
monitor_state_json
lotto_state_json
lifecycle_features_json
intervention_flags_json
source_resonance payloads
narrative_features_json
signal_links_json
```

Every parser must be tolerant:

```text
valid JSON object -> parsed fields
valid JSON array -> parsed array
invalid JSON -> missing_reason=json_parse_error
null/empty -> missing_reason=empty_payload
```

#### Phase 3 — same-window projection

Project the new fields into:

```text
row.jsonl
summary.json
separability.json
component-synergy.json
entry-mode-contribution.json
filter-contribution.json
```

Then add new reports:

```text
runtime-module-presence.json
runtime-decision-blocker-breakdown.json
gmgn-policy-contribution.json
freshness-latency-contribution.json
token-memory-contribution.json
a-class-submodule-contribution.json
source-resonance-external-alpha-contribution.json
narrative-wallet-curve-shadow-contribution.json
provider-evidence-confidence.json
```

#### Phase 4 — component interaction / synergy

Compute pairwise and staged interactions for:

```text
lifecycle_profile x entry_readiness_profile
entry_readiness_profile x GMGN policy
GMGN policy x quote_clean
source_resonance x source_gate
Markov bucket x A-class expected_rr
token_memory x reclaim_pass
narrative/social x source_resonance
wallet_quality x GMGN smart_degen
curve_stage x liquidity/quote
provider_health x missing_reason
```

The report must separate:

```text
true positive contribution
false positive protection
false negative overblock
coverage only / no decision effect
missing data
```

#### Phase 5 — repair-owner update

Update repair owner classification so blockers can be assigned to:

```text
GMGN_POLICY
FRESHNESS_LATENCY
TOKEN_MEMORY
ENTRY_BRANCH_MODE_GOVERNANCE
A_CLASS_FRESHNESS
A_CLASS_BUDGET
A_CLASS_RISK
A_CLASS_MATRIX
A_CLASS_EXPECTED_RR
A_CLASS_RUNTIME_SAFETY
SOURCE_RESONANCE_MISSING
NARRATIVE_SHADOW_ONLY
WALLET_SHADOW_ONLY
CURVE_PROVENANCE_MISSING
PROVIDER_HEALTH_GAP
SCOUT_QUALITY_OVERBLOCK
CAPACITY_BUDGET_BLOCK
EVIDENCE_LOW_CONFIDENCE
```

If multiple owners apply, use this priority:

```text
1. source/source hard reject
2. security/gates/blacklist
3. data provider / evidence confidence
4. freshness/latency
5. lifecycle/Markov applicability
6. GMGN/token memory/scout quality
7. A-class matrix/RR/budget/risk/runtime safety
8. final entry contract
9. execution bridge
10. ledger/exit/hold/friction
11. shadow-only / policy-only
```

### §15.13.5 Validation rules

Required automated tests:

```text
1. gmgn_policy_json projects action/reason/edge/toxic fields.
2. malformed gmgn_policy_json gives json_parse_error, not crash.
3. freshness_json projects opportunity_age and freshness_sources.
4. token memory fields can be absent without marking strategy failure.
5. entry_branch/capital_tier are preserved from paper_trades and paper_fast_lane.
6. A-class budget/risk/matrix/expected_rr are separately classified.
7. source_resonance pre-seen fields do not create actual EV.
8. narrative/wallet/curve fields are shadow/explanatory by default.
9. provider health missing changes row confidence, not raw dog label.
10. repair owner priority is deterministic when multiple blockers exist.
```

Manual checks:

```text
1. Pick one GMGN reject and confirm it appears as GMGN_POLICY.
2. Pick one quote-clean dog with no would_enter and confirm A-class subowner.
3. Pick one source-resonance row and confirm gmgn_pre_seen/lead_time.
4. Pick one narrative/social rich row and confirm it is shadow-only.
5. Pick one provider failure and confirm row confidence warning.
```

### §15.13.6 Definition of done

This goal is complete when:

```text
1. §15.13 is written into this document.
2. Row v2 has G1-G10 field groups or explicit TODO placeholders with
   missing_reason.
3. At least five new runtime module reports are generated:
   - gmgn-policy-contribution
   - freshness-latency-contribution
   - token-memory-contribution
   - a-class-submodule-contribution
   - provider-evidence-confidence
4. Narrative/wallet/curve are included only as shadow/explanatory reports.
5. Repair-owner classification can point to GMGN, freshness, token memory,
   A-class submodules, provider health, scout quality, and capacity budget.
6. Existing row count and raw dog/dud denominators do not change except for
   explicit source_presence additions.
7. Actual EV remains null when entered=0.
8. No live/gate/entry/exit/size behavior changes.
```

### §15.13.7 Relationship to §15.12

The two goals are complementary:

```text
§15.13 = runtime module expansion.
  It asks: which implemented module made or could have made the decision?

§15.12 = deep source / shadow fact expansion.
  It asks: which persisted fact source saw this signal outside the main chain?
```

Implementation order should be:

```text
1. §15.13 G1-G5 decision modules.
2. §15.12 P0-P3 deep facts.
3. §15.13 G6-G10 confidence/shadow modules.
4. §15.12 P4-P6 source quality/version/experiment facts.
5. Combined row v2 rerun and readout.
```

Do not use either goal to tune strategy until both can explain the same window
without denominator mismatch.

## §15.14 GOAL — operational truth / identity / execution-control expansion for fullnet row v2

This is the third deep scan after §15.12 and §15.13.

The first two expansion goals cover:

```text
§15.12 = deep persisted fact sources.
§15.13 = runtime decision modules and shadow feature modules.
```

This section covers a different class of missing modules:

```text
operational truth / identity truth / execution-control truth
```

These are not alpha modules. They should not be used to claim edge. Their job is
to prevent the audit from misclassifying a miss. Without them, fullnet can say
"matrix blocked the dog" or "entry bridge broke" when the real cause was parser
loss, duplicate suppression, identity mismatch, quote-intent mismatch, worker
death, stale read model, paper/live boundary violation, or a direct manual write.

### §15.14.1 Modules found in the third scan

The third scan found these already implemented modules, configs, or mirrors that
are worth bringing into the fullnet row.

```text
1. source parser / ingestion session truth
2. freshness duplicate / upgrade / processing-lock truth
3. token identity / unit / provider-finality truth
4. null-value and numeric-precision truth
5. raw-provider evidence and quote-intent binding truth
6. idempotency / write-path / direct-mutation truth
7. runtime worker / service readiness / filesystem pressure truth
8. paper/live boundary safety truth
9. profit-protect and standardized-stop exit truth
10. randomness / control-cohort truth
11. event-schema / delivery traceability / dashboard staleness truth
12. signal-lineage repair and inferred-source confidence truth
```

The files observed during this scan include:

```text
src/inputs/premium-channel-listener.js
src/gates/freshness-filter.js
src/runtime/v27-paper-mode-safety.js
src/execution/parity-executor.js
src/execution/jupiter-ultra-executor.js
src/execution/gmgn-executor.js
src/utils/session-manager.js
src/utils/atomic-write.js
src/optimizer/randomness-control.js

scripts/v27_record_raw_provider_probe_evidence.py
scripts/v27_mirror_raw_provider_evidence.py
scripts/v27_mirror_quote_intent_bindings.py
scripts/v27_mirror_idempotency_contracts.py
scripts/v27_mirror_standardized_stops.py
scripts/profit_protect_policy.py
scripts/v27_mirror_randomness_controls.py
scripts/repair-signal-lineage.js
scripts/migrate-signal-lineage.js
scripts/v27_event_log.py
scripts/v27_read_model_refresh.py

config/v27-identity-unit-provider-finality-policy.json
config/v27-null-value-policy.json
config/v27-numeric-precision-policy.json
config/v27-runtime-worker-health-policy.json
config/v27-background-job-registry.json
config/v27-service-readiness-probes.json
config/v27-runtime-config-drift-policy.json
config/v27-filesystem-pressure-policy.json
config/v27-capacity-load-latency-policy.json
config/v27-write-path-registry.json
config/v27-direct-database-mutation-policy.json
config/v27-event-schema-compatibility.json
config/v27-delivery-traceability-policy.json
config/v27-source-parser-auth-policy.json
config/v27-security-session-policy.json
config/v27-access-control-policy.json
```

### §15.14.2 P0 — source parser / ingestion session truth

Why it matters:

```text
If the Telegram source parser loses fields, emits duplicate-suppressed rows, or
uses the wrong source timestamp, then dog/dud separability downstream is already
polluted. This is a source-ingestion problem, not a strategy problem.
```

Observed fields already exist in `premium-channel-listener.js`:

```text
source_message_ts
receive_ts
source_event_id
signal_source
signal_type
parse_status
parse_missing_fields
raw_message
indices
freeze_ok / mint_ok
duplicate key: NT_<token_ca> / ATH_<token_ca>
```

Fullnet row additions:

```text
ingest.parse_status
ingest.parse_missing_fields
ingest.source_event_id
ingest.source_message_ts
ingest.receive_ts
ingest.ingest_delay_sec = receive_ts - source_message_ts
ingest.signal_source
ingest.parser_route = NEW_TRENDING | ATH | UNKNOWN
ingest.duplicate_suppressed
ingest.duplicate_key
ingest.channel_entity_resolved
ingest.session_health_status
ingest.source_parser_auth_status
```

Audit questions:

```text
1. Did formal/sustained dogs have more parse_missing_fields than duds?
2. Did ATH and New Trending collide on token-level duplicate logic?
3. Did source_message_ts imply that the dog was already stale before downstream
   logic saw it?
4. Did a channel/session/auth issue create a false "no signal" gap?
```

Repair-owner mapping:

```text
SOURCE_PARSER_MISSING_FIELD
SOURCE_INGEST_DELAY
SOURCE_DUPLICATE_SUPPRESSION
SOURCE_SESSION_OR_AUTH_GAP
```

### §15.14.3 P1 — freshness duplicate / upgrade / processing-lock truth

Why it matters:

```text
Freshness is not just "old vs new". The current module has duplicate windows,
source priorities, upgrade logic, cooldowns, and processing locks. These can
decide whether a later, stronger signal is allowed to replace an earlier weak
one. That directly affects capture.
```

Observed module:

```text
src/gates/freshness-filter.js
```

Fullnet row additions:

```text
freshness.signal_age_min
freshness.token_age_min
freshness.signal_freshness_status
freshness.token_freshness_window
freshness.source_latency_ms
freshness.duplicate_enabled
freshness.duplicate_window_min
freshness.duplicate_suppressed
freshness.allow_upgrade
freshness.previous_source
freshness.current_source_priority
freshness.previous_source_priority
freshness.is_source_upgrade
freshness.cooldown_active
freshness.cooldown_reason
freshness.cooldown_until
freshness.processing_lock_active
freshness.processing_lock_age_sec
```

Audit questions:

```text
1. Are later dog-producing re-signals suppressed as duplicate?
2. Does source upgrade work for dog tokens, or do weak first sightings block
   stronger later sightings?
3. Are cooldowns after failed attempts preventing valid reclaim entries?
4. Does processing_lock leakage explain any no-decision rows?
```

Repair-owner mapping:

```text
FRESHNESS_DUPLICATE_SUPPRESSION
FRESHNESS_UPGRADE_NOT_APPLIED
FRESHNESS_COOLDOWN_OVERBLOCK
FRESHNESS_PROCESSING_LOCK_STALE
```

### §15.14.4 P2 — identity / unit / provider-finality truth

Why it matters:

```text
The unit of analysis must be a real token opportunity, not a corrupted symbol,
wrong pool, wrong quote mint, schema-drifted provider response, or non-finalized
chain state. Otherwise dog/dud labels and quote/EV are not comparable.
```

Observed config:

```text
config/v27-identity-unit-provider-finality-policy.json
```

Fullnet row additions:

```text
identity.normalized_ca
identity.identity_confidence
identity.symbol_conflict_count
identity.pool_address
identity.pool_authority
identity.quote_mint
identity.liquidity_pair_valid
unit.price_unit
unit.quote_decimals
unit.token_decimals
unit.unit_conversion_version
finality.commitment_level
finality.indexer_lag_sec
finality.rpc_consistency_check
provider_schema.provider_name
provider_schema.schema_version
provider_schema.schema_drift_detected
provider_schema.missing_required_field_rate
provider_schema.null_spike_rate
provider_schema.value_range_anomaly
```

Audit questions:

```text
1. Did any dog/dud denominator row have identity_confidence < 1?
2. Did quote_mint / pool_address mismatch explain quote failures?
3. Did provider schema drift or null spike cluster around missed dogs?
4. Did indexer lag mean "not tradable yet" rather than "strategy blocked"?
```

Repair-owner mapping:

```text
IDENTITY_LOW_CONFIDENCE
POOL_OR_QUOTE_MINT_MISMATCH
PROVIDER_SCHEMA_DRIFT
CHAIN_FINALITY_OR_INDEXER_LAG
```

### §15.14.5 P3 — null-value and numeric-precision truth

Why it matters:

```text
Nulls and numeric units can create fake edge or fake failure. A null quote should
not silently become 0 or PASS. A ms timestamp should not be compared as seconds.
A percent should not be mixed with bps. These are audit validity constraints.
```

Observed configs:

```text
config/v27-null-value-policy.json
config/v27-numeric-precision-policy.json
```

Fullnet row additions:

```text
null_policy.critical_risk_status_null_class
null_policy.entry_quote_price_null_class
null_policy.exit_quote_price_null_class
null_policy.default_value_used
null_policy.imputation_policy
null_policy.training_allowed
null_policy.decision_allowed
precision.price_unit
precision.percentage_unit
precision.bps_unit
precision.timestamp_unit
precision.rounding_mode
precision.overflow_policy
precision.unit_conversion_ok
precision.timestamp_ms_sec_normalized
```

Audit questions:

```text
1. Are missing quote/risk fields counted as clean by accident?
2. Are old high-win reports contaminated by timestamp or unit mismatch?
3. Are dog/dud labels based on mixed mark/quote/price units?
4. Are EV numbers reproducible with declared rounding rules?
```

Repair-owner mapping:

```text
NULL_POLICY_BLOCK
NUMERIC_UNIT_MISMATCH
TIMESTAMP_UNIT_MISMATCH
PRECISION_OR_ROUNDING_INVALID
```

### §15.14.6 P4 — raw-provider evidence / quote-intent binding / executor parity

Why it matters:

```text
would_enter is not enough. The audit must know what exact quote intent was
formed, which provider was asked, what proof level exists, whether the provider
response was trusted, and whether the execution adapter matched the quote.
```

Observed modules:

```text
scripts/v27_record_raw_provider_probe_evidence.py
scripts/v27_mirror_raw_provider_evidence.py
scripts/v27_mirror_quote_intent_bindings.py
src/execution/parity-executor.js
src/execution/jupiter-ultra-executor.js
src/execution/gmgn-executor.js
src/execution/gmgn-telegram-executor.js
```

Fullnet row additions:

```text
provider_evidence.provider
provider_evidence.endpoint
provider_evidence.request_id
provider_evidence.http_status
provider_evidence.latency_ms
provider_evidence.raw_response_hash
provider_evidence.request_metadata_hash
provider_evidence.provider_evidence_trusted
quote_intent.quote_intent_id
quote_intent.side
quote_intent.size
quote_intent.route
quote_intent.pool
quote_intent.quote_mint
quote_intent.slippage_bps
quote_intent.quote_ts
quote_intent.proof_level
quote_intent.missing_fields
quote_intent.mismatch_fields
executor.adapter
executor.parity_status
executor.raw_provider_evidence_attached
```

Audit questions:

```text
1. Did quote-clean dogs fail because no quote intent was formed?
2. Did quote intent exist but provider evidence was missing or untrusted?
3. Did route/pool/size/slippage mismatch between intent and quote?
4. Did one executor adapter behave differently from another on dogs?
```

Repair-owner mapping:

```text
QUOTE_INTENT_MISSING
QUOTE_INTENT_BINDING_MISMATCH
RAW_PROVIDER_EVIDENCE_MISSING
EXECUTOR_PARITY_GAP
```

### §15.14.7 P5 — idempotency / write-path / direct-mutation truth

Why it matters:

```text
Duplicate prevention is part of capture. If the same lifecycle can create two
entries, the EV is polluted. If direct dashboard/manual writes mutate ledgers or
paper trades without an audit path, capture and exit evidence is not trustworthy.
```

Observed modules/configs:

```text
scripts/v27_mirror_idempotency_contracts.py
config/v27-write-path-registry.json
config/v27-direct-database-mutation-policy.json
src/utils/atomic-write.js
scripts/sqlite_write_coordinator.py
```

Fullnet row additions:

```text
idempotency.idempotency_key
idempotency.token_lifecycle_key
idempotency.decision_id
idempotency.execution_id
idempotency.namespace
idempotency.environment_id
idempotency.collision_policy
idempotency.duplicate_policy
idempotency.intent_hash
write_path.write_path_id
write_path.mutation_type
write_path.mode_gate
write_path.break_glass_id
write_path.audit_log_present
write_path.direct_mutation_detected
write_path.outbox_required
write_path.outbox_rationale
```

Audit questions:

```text
1. Did entry_bridge fail because a duplicate/idempotency guard rejected it?
2. Did a manual cleanup/pause/close mutate the denominator or ledger?
3. Are paper/live environments isolated in the idempotency namespace?
4. Are write paths append-only/audited where they affect outcomes?
```

Repair-owner mapping:

```text
IDEMPOTENCY_DUPLICATE_REJECT
WRITE_PATH_UNAUDITED_MUTATION
DIRECT_DATABASE_MUTATION
ENVIRONMENT_NAMESPACE_COLLISION
```

### §15.14.8 P6 — runtime worker / readiness / filesystem / capacity truth

Why it matters:

```text
If paper trader, lifecycle tracker, read-model refresh, quote polling, or the
event projection worker is dead or lagging, a missed dog is an ops miss, not a
strategy miss. This needs to be visible in fullnet rows.
```

Observed configs:

```text
config/v27-runtime-worker-health-policy.json
config/v27-background-job-registry.json
config/v27-service-readiness-probes.json
config/v27-runtime-config-drift-policy.json
config/v27-filesystem-pressure-policy.json
config/v27-capacity-load-latency-policy.json
```

Fullnet row additions:

```text
ops.worker_fleet_heartbeat_ok
ops.worker_role_missing
ops.worker_heartbeat_lag_ms
ops.silent_death_detected
ops.service_readiness_ok
ops.runtime_config_hash
ops.policy_bundle_hash
ops.runtime_config_drift
ops.disk_free_bytes
ops.low_free_disk
ops.wal_pressure
ops.capacity_component
ops.queue_depth
ops.p95_latency_budget_ms
ops.latency_class
ops.blocking_component
ops.provider_quota_status
ops.exit_safety_reserved
```

Audit questions:

```text
1. Were no-decision / no-entry rows concentrated during worker lag?
2. Did read-model staleness or event-projection lag hide evidence?
3. Did disk/WAL pressure explain missing snapshots or failed pulls?
4. Did provider quota isolation starve entry quote polling?
```

Repair-owner mapping:

```text
OPS_WORKER_DOWN
OPS_READ_MODEL_STALE
OPS_DISK_OR_WAL_PRESSURE
OPS_CAPACITY_OR_PROVIDER_QUOTA
```

### §15.14.9 P7 — paper/live boundary safety truth

Why it matters:

```text
Many current experiments are paper-only or shadow-only. The row must prove that
paper evidence did not accidentally cross into live execution, and live secrets
or executors did not change the behavior being audited.
```

Observed module:

```text
src/runtime/v27-paper-mode-safety.js
```

Fullnet row additions:

```text
paper_live.paper_only_mode
paper_live.premium_live_execution_enabled
paper_live.live_private_key_present
paper_live.live_secret_quarantine_applied
paper_live.quarantined_live_secret_names_hash
paper_live.live_swap_endpoint_enabled
paper_live.real_order_router_enabled
paper_live.network_transaction_signing_enabled
paper_live.jupiter_executor_initialized
paper_live.live_execution_executor_initialized
paper_live.paper_live_boundary_ok
paper_live.violations
```

Audit questions:

```text
1. Are shadow/paper EV rows cleanly separated from live rows?
2. Did any mode marked paper-only have live executor availability?
3. Did a paper/live violation occur during a window being used for promotion?
```

Repair-owner mapping:

```text
PAPER_LIVE_BOUNDARY_VIOLATION
PAPER_ONLY_SCOPE_UNPROVEN
LIVE_SECRET_OR_EXECUTOR_PRESENT
```

### §15.14.10 P8 — profit-protect and standardized-stop exit truth

Why it matters:

```text
Exit/hold should not be represented only by final PnL. The system has explicit
profit-protect floors and standardized stop mirrors. These are required to know
whether the system failed to enter, entered but failed to protect, or protected
correctly but friction erased edge.
```

Observed modules:

```text
scripts/profit_protect_policy.py
scripts/v27_mirror_standardized_stops.py
config/v27-execution-exit-safety-policy.json
```

Fullnet row additions:

```text
exit_policy.profit_protect_floor
exit_policy.ath_moon_bag_floor
exit_policy.probe_runner_floor
exit_policy.cohort_aware_probe_runner_floor
exit_policy.standardized_stop_threshold_pct
exit_policy.standardized_stop_window
exit_policy.standardized_stop_price_type
exit_policy.executable_required
exit_policy.friction_model
exit_policy.stop_counterfactual_pnl
exit_policy.exit_safety_policy_version
```

Audit questions:

```text
1. Did entered dogs hit a protectable peak but fail to lock profit?
2. Which exit template would have preserved dog/silver/gold capture?
3. Did standardized stop counterfactual beat current exit after friction?
4. Did moonbag/runner floor keep enough tail without returning to loss?
```

Repair-owner mapping:

```text
EXIT_PROFIT_PROTECT_GAP
EXIT_STANDARDIZED_STOP_GAP
EXIT_RUNNER_FLOOR_GAP
EXIT_FRICTION_ERASED_EDGE
```

### §15.14.11 P9 — randomness / control-cohort truth

Why it matters:

```text
When testing 32 entry modes or shadow probes, sample selection must be auditable.
Otherwise a mode can look good because it saw easier tokens, or look bad because
it was sampled during a worse regime.
```

Observed modules:

```text
src/optimizer/randomness-control.js
scripts/v27_mirror_randomness_controls.py
tests/optimizer-randomness-control.test.mjs
```

Fullnet row additions:

```text
randomness.randomization_enabled
randomness.seed
randomness.cohort_id
randomness.control_group
randomness.treatment_group
randomness.inclusion_probability
randomness.assignment_hash
randomness.assignment_reason
randomness.randomness_control_version
```

Audit questions:

```text
1. Did a mode have a valid control cohort?
2. Are dog/dud comparisons balanced by source/time/lifecycle?
3. Did promotion evidence use random/control rows or hand-picked rows?
4. Are excluded rows excluded by policy rather than silent sampling drift?
```

Repair-owner mapping:

```text
EXPERIMENT_CONTROL_MISSING
RANDOMIZATION_ASSIGNMENT_MISSING
PROMOTION_SAMPLE_BIASED
```

### §15.14.12 P10 — event schema / delivery traceability / dashboard staleness truth

Why it matters:

```text
If event schemas drift, projection consumers lag, read models are stale, or a
dashboard panel is deprecated, then the readout can be wrong even when strategy
runtime behaved correctly.
```

Observed configs/modules:

```text
config/v27-event-schema-compatibility.json
config/v27-delivery-traceability-policy.json
config/v27-read-model-snapshot-policy.json
scripts/v27_event_log.py
scripts/v27_read_model_refresh.py
scripts/v27_projection_consumer_evidence.py
data/v27_event_log/events.jsonl
data/v27_read_models/*
```

Fullnet row additions:

```text
event.event_schema_version
event.event_type_registered
event.compatibility_result
event.event_log_seq
event.projection_seq
event.projection_lag
event.dlq_present
read_model.snapshot_hash
read_model.snapshot_generated_at
read_model.stale
dashboard.panel_lag_sec
dashboard.stale_banner_required
delivery.decommission_status
delivery.historical_only_badge
delivery.promotion_evidence_allowed
```

Audit questions:

```text
1. Was the row built from a fresh projection or a stale read model?
2. Did any event type used by fullnet have compatibility failure?
3. Did a retired route/mode accidentally contribute promotion evidence?
4. Did dashboard staleness hide an entry/ledger update?
```

Repair-owner mapping:

```text
EVENT_SCHEMA_INCOMPATIBLE
PROJECTION_OR_READ_MODEL_STALE
DASHBOARD_STALE
RETIRED_ARTIFACT_USED_AS_EVIDENCE
```

### §15.14.13 P11 — signal-lineage repair / inferred-source confidence

Why it matters:

```text
Historical source labels may be repaired or inferred. That is useful, but inferred
lineage must not be treated the same as direct source evidence when measuring
source edge or route contribution.
```

Observed modules:

```text
scripts/repair-signal-lineage.js
scripts/migrate-signal-lineage.js
scripts/backfill-premium-paper-lineage.js
```

Fullnet row additions:

```text
lineage.signal_source
lineage.signal_route
lineage.signal_entry_reason
lineage.signal_hunter_type
lineage.signal_confidence = direct | inferred | repaired | unknown
lineage.repair_script_version
lineage.repair_applied
lineage.repair_reason
lineage.original_signal_source
```

Audit questions:

```text
1. Do any entry-mode or source claims rely on inferred lineage?
2. Does inferred lineage have different dog/dud distribution from direct lineage?
3. Are repaired rows excluded from high-confidence promotion evidence?
```

Repair-owner mapping:

```text
LINEAGE_INFERRED_LOW_CONFIDENCE
LINEAGE_REPAIR_REQUIRED
SOURCE_ATTRIBUTION_UNTRUSTED
```

### §15.14.14 Row v2 confidence model

The new operational modules should not change dog/dud labels directly. They
should add a row confidence ladder:

```text
row_confidence = HIGH
  all critical evidence present, fresh, direct, and compatible

row_confidence = MEDIUM
  strategy evidence present, but one non-critical ops/confidence field missing

row_confidence = LOW
  critical confidence field missing or inferred, but raw label still usable

row_confidence = INVALID_FOR_EV
  quote/entry/exit/ledger evidence invalid for actual EV

row_confidence = INVALID_FOR_PROMOTION
  stale projection, retired artifact, paper/live violation, direct mutation,
  or uncontrolled sample
```

This prevents two mistakes:

```text
1. Treating ops failures as strategy failures.
2. Treating strategy/shadow rows with weak evidence as promotion-grade edge.
```

### §15.14.15 New reports required

Add these reports to the fullnet artifact:

```text
1. ingestion-parser-quality.json
2. freshness-duplicate-upgrade-report.json
3. identity-unit-finality-report.json
4. null-numeric-validity-report.json
5. quote-intent-provider-evidence-report.json
6. idempotency-write-path-report.json
7. runtime-operational-health-report.json
8. paper-live-boundary-report.json
9. profit-protect-standardized-stop-report.json
10. randomness-control-cohort-report.json
11. event-delivery-traceability-report.json
12. lineage-confidence-report.json
```

Each report must include:

```text
row_count
dog_count
dud_count
pending_count
missing_count
invalid_for_ev_count
invalid_for_promotion_count
top_missing_reasons
dog_vs_dud_distribution
recommended_repair_owner_if_material
```

### §15.14.16 Updated repair-owner priority

Before assigning a miss to strategy modules, classify evidence failures in this
order:

```text
0. INVALID_ROW_IDENTITY_OR_UNIT
1. SOURCE_PARSER_OR_INGEST_GAP
2. FRESHNESS_DUPLICATE_OR_LOCK_GAP
3. OPS_WORKER_OR_READ_MODEL_GAP
4. PAPER_LIVE_BOUNDARY_OR_DIRECT_MUTATION_GAP
5. QUOTE_INTENT_OR_PROVIDER_EVIDENCE_GAP
6. IDEMPOTENCY_OR_WRITE_PATH_GAP
7. STRATEGY_DECISION_MODULE_GAP
8. ENTRY_BRIDGE_GAP
9. LEDGER_EXIT_HOLD_FRICTION_GAP
10. SHADOW_ONLY_OR_POLICY_ONLY
```

Only levels 7-10 are strategy/entry/exit conclusions. Levels 0-6 are evidence
truth or operational truth blockers.

### §15.14.17 Validation tests

Required tests:

```text
1. malformed Telegram message yields parse_status/missing fields and does not
   silently disappear.
2. ATH and New Trending duplicate keys stay route-specific.
3. stronger later source signal surfaces as freshness upgrade rather than silent
   duplicate suppression.
4. identity/pool/quote_mint mismatch marks row LOW or INVALID_FOR_EV.
5. provider raw evidence missing prevents quote-intent rows from being
   promotion-grade.
6. quote intent mismatch fields are surfaced and assigned QUOTE_INTENT owner.
7. idempotency duplicate rejection is not counted as normal strategy reject.
8. direct database mutation marks row invalid for promotion unless audited.
9. stale worker/read-model/disk pressure changes ops owner, not matrix owner.
10. paper/live boundary violation blocks promotion evidence.
11. standardized stop/profit-protect output is counterfactual unless entered.
12. randomness/control missing blocks mode-promotion evidence.
13. inferred lineage rows are excluded from direct-source edge claims.
14. row_confidence is deterministic for the same input artifact.
```

### §15.14.18 Definition of done

This goal is complete when:

```text
1. §15.14 is written into this document.
2. build-live-fullnet-row-report has TODO field groups or implemented fields for
   P0-P11.
3. All P0-P11 modules appear in row.jsonl as present/missing with
   missing_reason.
4. Reports in §15.14.15 are generated or explicitly skipped with a blocker.
5. Repair owner priority in §15.14.16 is enforced.
6. row_confidence exists and gates actual EV / promotion EV claims.
7. Actual EV remains null unless entered/fill/exit/ledger evidence is valid.
8. Paper/live/shadow rows cannot be mixed in one promotion denominator.
9. Historical inferred-lineage rows are tagged and separated from direct rows.
10. No live/gate/entry/exit/size behavior changes.
```

### §15.14.19 Relationship to §15.12 and §15.13

Implementation order should now be:

```text
1. §15.14 P0-P5 evidence truth controls.
2. §15.13 G1-G5 runtime decision modules.
3. §15.12 P0-P3 deep source / shadow facts.
4. §15.14 P6-P11 operational confidence controls.
5. §15.13 G6-G10 shadow/confidence modules.
6. §15.12 P4-P6 source quality/version/experiment facts.
7. Combined row v2 rerun.
```

The order matters. A dog-vs-dud separability report is only useful after the row
can prove:

```text
same source event
same token identity
same unit conventions
same quote intent
same execution environment
same projection freshness
same row confidence rules
```

Only then should old entry-bar/FBR/RED-bar/stage2A/stage3 evidence be judged as
current edge or archived as historical regime evidence.

## §15.15 GOAL — final exhaustive fullnet module-closure sweep

This is the stop-condition sweep.

After scanning:

```text
config/*
src/*
scripts/*
v27_mode_readiness.py
v27_basic_contract_readiness.py
v27_denominator_projection.py
```

the remaining missed modules are no longer mostly strategy modules. They are
mode-readiness, denominator-truth, feature/training, release/promotion,
market-data, advisory, and governance modules. They still matter because they
decide whether a row is:

```text
usable for dog/dud analysis
usable for counterfactual EV
usable for actual EV
usable for mode promotion
usable only as historical/shadow evidence
not usable
```

### §15.15.1 Final uncovered module groups

The last sweep found these additional groups that were not fully covered by
§15.12, §15.13, or §15.14:

```text
1. mode-readiness matrix / gate-scope / entry-point inventory
2. metric definition / threshold catalog / reason and error taxonomy
3. denominator projection / label finalization / outcome-window close
4. ex-ante feasibility / earliest actionable time / fill-time anchors
5. feature availability / feature vector snapshots / training dataset manifests
6. detector shadow calibration / Markov forecast validation
7. release experiment safety / holdout / negative control / promotion guardrails
8. on-chain snapshot / hard-gate raw safety fields
9. waiting-room / observation policy / AI advisory-only reviews
10. market-data provenance / kline backfill / pool resolver / cache state
11. capital reservation / fee schedule / position sizing / global risk manager
12. autonomy / self-iteration / strategy mutation / research-memory governance
13. source/channel registry / channel status / source tier
14. API/data export envelope / access audit / operator mutation surface
15. evidence conflict / evidence aging / assumptions / false-negative budget
16. legal/provider terms / export-reimport / archive bitrot / data retention
```

Some of these are row-level. Some are artifact/window-level. Some are promotion
gates only. The key is not to force every governance field onto every token row.
The key is to make the row and the artifact declare which gates are satisfied.

### §15.15.2 P0 — mode-readiness matrix / gate-scope / entry-point inventory

Observed files:

```text
scripts/v27_mode_readiness.py
scripts/v27_mode_gate_scope.py
scripts/v27_mode_gate_scope_audit.py
config/v27-entry-point-inventory.json
config/entry-mode-registry.json
src/web/mode-registry-utils.js
```

Why it matters:

```text
The same evidence means different things in observe_only, shadow, ultra_tiny,
and normal_tiny. A mode can have good dog/dud separation but still be ineligible
for promotion if required contracts are missing.
```

Fullnet artifact additions:

```text
mode_readiness.mode
mode_readiness.required_contracts
mode_readiness.passed_contracts
mode_readiness.failed_contracts
mode_readiness.blocking_contracts
mode_readiness.mode_gate_scope
mode_readiness.entry_point_id
mode_readiness.entry_point_runtime_reference
mode_readiness.entry_point_allowed_mode
mode_readiness.safe_default_applied
```

Audit questions:

```text
1. Did an entry mode act outside its declared tier?
2. Did a mode generate promotion evidence while a required contract was failing?
3. Did an unregistered entry point affect would_enter or entered?
4. Did deprecated/hard_shadow modes leak into current promotion denominators?
```

Repair-owner mapping:

```text
MODE_READINESS_BLOCK
ENTRY_POINT_UNREGISTERED
MODE_GATE_SCOPE_VIOLATION
SAFE_DEFAULT_APPLIED
```

### §15.15.3 P1 — metric / threshold / reason taxonomy truth

Observed files:

```text
config/v27-metric-definition-registry.json
config/v27-threshold-catalog.json
config/v27-reason-taxonomy-policy.json
config/v27-error-taxonomy.json
src/utils/threshold-config.js
src/utils/time-normalization.js
```

Why it matters:

```text
If win rate, capture rate, EV, or blocker reason definitions drift, then
multi-day reports are not comparable. A "dog capture rate" without metric and
threshold version is not a stable number.
```

Fullnet artifact additions:

```text
metric_registry_hash
threshold_catalog_hash
metric_id
metric_version
window_id
denominator_definition
numerator_definition
partial_window_policy
threshold_id
threshold_value
threshold_unit
threshold_effective_from
reason_taxonomy_version
root_cause_taxonomy_version
error_taxonomy_version
human_readable_reason
machine_readable_reason
```

Audit questions:

```text
1. Did a report compare metrics computed with different definitions?
2. Did hardcoded thresholds bypass the catalog?
3. Did blocker reasons change names without taxonomy mapping?
4. Did a partial window get used as if complete?
```

Repair-owner mapping:

```text
METRIC_DEFINITION_DRIFT
THRESHOLD_CATALOG_DRIFT
REASON_TAXONOMY_DRIFT
PARTIAL_WINDOW_USED_AS_COMPLETE
```

### §15.15.4 P2 — denominator / label / outcome-window truth

Observed files:

```text
scripts/v27_denominator_projection.py
scripts/v27_mirror_source_labels.py
scripts/v27_mirror_trade_outcomes.py
scripts/v27_mirror_earliest_actionable_times.py
scripts/v27_mirror_ex_ante_feasibility.py
data/v27_read_models/denominator_snapshot.json
```

Why it matters:

```text
The row must know which denominator it belongs to: D0 source label, D1
reference price, D2 realtime clean, D3 externally actionable, or D3 policy
actionable. Otherwise "missed dog" can mix non-actionable hindsight with
actually tradable opportunity.
```

Fullnet row additions:

```text
denominator.d0_source_label
denominator.d1_reference_price_ok
denominator.d2_realtime_clean
denominator.d3a_externally_actionable
denominator.d3b_policy_actionable
denominator.denominator_dedup_key
denominator.dirty_reasons
label.source_dog_label
label.trade_outcome_label
label.label_finalized
label.outcome_window_closed
label.label_available_at
label.used_future_label_in_decision
reference_price.reference_price
reference_price.source
reference_price.conflict_count
```

Audit questions:

```text
1. Are formal dog / sustained dog / peak>=50 / peak>=100 / 5x / 10x rows all
   projected into the same denominator ladder?
2. Is any "missed dog" posthoc-only and not ex-ante actionable?
3. Did dirty denominator records get included in separability or EV?
4. Did label finalization happen after the decision as expected?
```

Repair-owner mapping:

```text
DENOMINATOR_DIRTY
LABEL_NOT_FINALIZED
OUTCOME_WINDOW_NOT_CLOSED
REFERENCE_PRICE_CONFLICT
POSTHOC_ONLY_NOT_ACTIONABLE
```

### §15.15.5 P3 — ex-ante feasibility / earliest actionable / fill-time anchors

Observed files:

```text
scripts/v27_mirror_ex_ante_feasibility.py
scripts/v27_mirror_earliest_actionable_times.py
config/v27-spec-governance-feasibility-policy.json
```

Why it matters:

```text
A dog can be real but physically impossible to capture. The audit needs to know
when the system could first have acted, not only when the future peak happened.
```

Fullnet row additions:

```text
feasibility.ex_ante_feasible
feasibility.posthoc_feasible
feasibility.earliest_actionable_ts
feasibility.feature_available_at
feasibility.required_inputs_available_at
feasibility.system_min_decision_latency_sec
feasibility.system_min_entry_latency_sec
feasibility.used_future_peak_in_ex_ante
fill_anchor.decision_ts
fill_anchor.decision_available_at
fill_anchor.entry_quote_at_decision_ts
fill_anchor.simulated_fill_ts
fill_anchor.position_open_confirmed_ts
fill_anchor.latency_components
```

Audit questions:

```text
1. Did the dog peak before the system could have a quote/risk/pool decision?
2. Did old backtests use a fill timestamp not available in live?
3. Are capture misses concentrated in not-physically-capturable rows?
4. Does a mode's EV survive when anchored to earliest actionable time?
```

Repair-owner mapping:

```text
NOT_EX_ANTE_FEASIBLE
EARLIEST_ACTIONABLE_AFTER_PEAK
FILL_TIME_ANCHOR_INVALID
FUTURE_LEAKAGE_IN_FEASIBILITY
```

### §15.15.6 P4 — feature availability / vector snapshots / training manifests

Observed files:

```text
config/v27-feature-vector-snapshot-policy.json
config/v27-training-dataset-manifest-policy.json
scripts/v27_record_decision_audit_evidence.py
scripts/ai_review_schema.py
```

Why it matters:

```text
Old high-win results can be invalid if features were not available before the
decision, if labels leaked into training, or if the feature vector cannot be
reproduced.
```

Fullnet row/artifact additions:

```text
feature.feature_available_at_map
feature.decision_available_at
feature.label_available_at
feature.feature_research_only
feature.future_leakage_detected
feature.feature_vector_hash
feature.feature_names_ordered
feature.model_input_schema_version
training.dataset_id
training.manifest_hash
training.included_sample_id
training.excluded_sample_id
training.exclusion_reason
training.observation_weight
training.label_versions
training.feature_versions
training.training_allowed
```

Audit questions:

```text
1. Were FBR/RED-bar/velocity/AI/SI/MC features available at decision time?
2. Did any old report use future labels or future peak in feature construction?
3. Can the exact feature vector be rebuilt from row evidence?
4. Are excluded/manual-override samples kept out of training and promotion?
```

Repair-owner mapping:

```text
FEATURE_AVAILABILITY_LEAK
FEATURE_VECTOR_UNREPRODUCIBLE
TRAINING_MANIFEST_INVALID
TRAINING_SAMPLE_EXCLUDED
```

### §15.15.7 P5 — detector calibration and Markov forecast validation

Observed files:

```text
config/v27-detector-shadow-calibration-policy.json
config/v27-markov-lifecycle-forecast-policy.json
scripts/v27_markov_shadow_calibration_report.py
scripts/v27_record_markov_shadow_forecasts.py
scripts/telegram_lifecycle_markov.py
```

Why it matters:

```text
Reclaim, overextension, lifecycle forecasts, and Markov regimes can be useful,
but only if calibrated out-of-sample and kept shadow-only until validated.
```

Fullnet row/artifact additions:

```text
detector.detector_id
detector.detector_version
detector.output_state
detector.allowed_modes
detector.gate_allowed
detector.metric_id
detector.threshold_id
detector.calibration_status
detector.sample_n
detector.contaminated_sample_count
markov.transition_matrix_version
markov.n_step_forecast
markov.semi_markov_forecast
markov.competing_risk_forecast
markov.censoring_policy
markov.walk_forward_status
markov.hmm_research_only_boundary
```

Audit questions:

```text
1. Did Markov green/yellow enrich dogs after controlling for lifecycle?
2. Did detector output remain shadow-only where gate_allowed=false?
3. Did reclaim detector calibration hold in the current window?
4. Did censoring or absorbing-state assumptions hide failed rows?
```

Repair-owner mapping:

```text
DETECTOR_UNCALIBRATED
DETECTOR_GATE_NOT_ALLOWED
MARKOV_FORECAST_UNCALIBRATED
MARKOV_CENSORING_OR_WALK_FORWARD_GAP
```

### §15.15.8 P6 — release experiment safety / promotion guardrails

Observed files:

```text
config/v27-release-experiment-safety-policy.json
config/v27-governance-readiness.json
src/optimizer/promotion-guardrails.js
src/optimizer/champion-challenger.js
src/optimizer/strategy-mutator.js
src/optimizer/autoresearch-loop.js
src/optimizer/fixed-evaluator.js
```

Why it matters:

```text
Even if a mode looks good in fullnet, it should not graduate unless holdout,
negative-control, manual-override, sample-size, drawdown, tail-loss, and
false-positive requirements pass.
```

Fullnet artifact additions:

```text
promotion.sample_size
promotion.min_sample_size
promotion.win_rate
promotion.min_win_rate
promotion.expectancy
promotion.min_expectancy
promotion.max_drawdown
promotion.max_tail_loss95
promotion.false_positive_rate
promotion.blinded_holdout_id
promotion.holdout_clean
promotion.negative_control_ok
promotion.manual_override_quarantined
promotion.adversarial_replay_pass
promotion.regression_budget_ok
promotion.safety_case_ok
promotion.waiver_allowed
promotion.project_stop_loss_state
```

Audit questions:

```text
1. Is a proposed live/normal_tiny promotion backed by enough samples?
2. Did the mode pass blinded holdout and negative control?
3. Were manual overrides excluded from evidence?
4. Did tail loss or false-positive rate fail even when average EV looked good?
```

Repair-owner mapping:

```text
PROMOTION_GUARDRAIL_FAIL
HOLDOUT_CONTAMINATED_OR_MISSING
NEGATIVE_CONTROL_FAIL
MANUAL_OVERRIDE_CONTAMINATION
REGRESSION_BUDGET_FAIL
```

### §15.15.9 P7 — on-chain snapshot and hard-gate raw safety fields

Observed files:

```text
src/inputs/chain-snapshot.js
src/inputs/chain-snapshot-sol.js
src/inputs/chain-snapshot-bsc.js
src/gates/hard-gates.js
src/gates/token-gatekeeper.js
config/system.config.json
```

Why it matters:

```text
The gates table is not just PASS/REJECT. It contains raw on-chain safety
evidence. Dog misses caused by freeze/mint/LP/honeypot/tax/top10/wash/slippage
should be separated from matrix/entry misses.
```

Fullnet row additions:

```text
chain_snapshot.chain
chain_snapshot.snapshot_time
chain_snapshot.cache_hit
chain_snapshot.data_source
chain_snapshot.error
chain_snapshot.freeze_authority
chain_snapshot.mint_authority
chain_snapshot.lp_status
chain_snapshot.honeypot
chain_snapshot.tax_buy
chain_snapshot.tax_sell
chain_snapshot.tax_mutable
chain_snapshot.owner_type
chain_snapshot.dangerous_functions
chain_snapshot.top10_percent
chain_snapshot.liquidity
chain_snapshot.liquidity_unit
chain_snapshot.slippage_sell_20pct
chain_snapshot.wash_flag
chain_snapshot.key_risk_wallets
chain_snapshot.sell_constraints_flag
```

Audit questions:

```text
1. Which raw safety fields actually protected against duds?
2. Which raw safety fields overblocked formal/sustained/5x dogs?
3. Did cache fallback or Unknown snapshots cause false safety blocks?
4. Did slippage_sell_20pct predict actual entry/exit friction?
```

Repair-owner mapping:

```text
CHAIN_SNAPSHOT_UNKNOWN
RAW_SAFETY_GATE_OVERBLOCK
RAW_SAFETY_GATE_PROTECTION
ONCHAIN_CACHE_OR_PROVIDER_GAP
```

### §15.15.10 P8 — waiting-room / observation policy / AI advisory-only reviews

Observed files:

```text
src/core/waiting-room.js
src/utils/dynamic-calculator.js
src/utils/prompt-builder.js
src/utils/ai-analyst.js
scripts/ai_counterfactual_auditor.py
scripts/ai_review_schema.py
src/decision/ai-trading-decider.js
src/engines/batch-ai-advisor.js
```

Why it matters:

```text
AI/advisory modules can influence score, graduation, or confidence. They must
be tracked as advisory-only unless a specific entry contract allows them to
trigger trades.
```

Fullnet row additions:

```text
waiting_room.entered_waiting_room
waiting_room.target_wait_ms
waiting_room.wait_elapsed_ms
waiting_room.trigger_reason
waiting_room.initial_snapshot_hash
waiting_room.current_snapshot_source
waiting_room.dynamic_factor_tag
waiting_room.dynamic_factor_reason
waiting_room.kline_health
waiting_room.graduation_decision
ai_review.schema_version
ai_review.reviewer
ai_review.ai_score
ai_review.ai_grade
ai_review.allowed_effect
ai_review.score_boost_suggested
ai_review.can_trigger_trade
ai_review.can_override_hard_gate
ai_review.advisory_only
ai_review.risk_notes
counterfactual_audit.pass
counterfactual_audit.blockers
```

Audit questions:

```text
1. Did waiting-room graduation enrich dogs or only delay entries?
2. Did AI advisory add value when restricted to score boost/advisory only?
3. Did AI score boosts leak into hard-gate override?
4. Did counterfactual audit block promotion because denominator quality was weak?
```

Repair-owner mapping:

```text
WAITING_ROOM_DELAY_OR_GRADUATION_GAP
AI_ADVISORY_ONLY
AI_REVIEW_SCHEMA_OR_EFFECT_VIOLATION
COUNTERFACTUAL_AUDIT_BLOCK
```

### §15.15.11 P9 — market-data provenance / kline backfill / pool resolver

Observed files:

```text
src/market-data/bar-aggregator.js
src/market-data/helius-history-client.js
src/market-data/kline-repository.js
src/market-data/market-data-backfill-service.js
src/market-data/pool-resolver.js
src/market-data/shared-market-data-client.js
src/market-data/shared-pool-ohclv-client.js
src/market-data/shared-quote-client.js
src/market-data/trade-normalizer.js
scripts/backfill-real-kline-data.js
scripts/backfill_extended.py
```

Why it matters:

```text
Entry-bar/FBR/RED-bar/low-volume/velocity evidence is only valid if the kline,
pool, bar alignment, and backfill source are correct. This is the main guard
against repeating old look-ahead or bar-alignment mistakes.
```

Fullnet row additions:

```text
market_data.pool_resolved
market_data.pool_address
market_data.pool_resolution_source
market_data.kline_source
market_data.kline_interval
market_data.bar_alignment_policy
market_data.bar_open_ts
market_data.bar_close_ts
market_data.signal_to_bar_offset_sec
market_data.backfill_used
market_data.backfill_source
market_data.backfill_run_id
market_data.trade_normalizer_version
market_data.cache_hit
market_data.cache_age_sec
market_data.helius_request_id
market_data.ohlcv_complete
```

Audit questions:

```text
1. Is first-bar return computed from a bar that was fully known at entry time?
2. Did RED-bar/low-volume use the same pool and quote mint as execution?
3. Did backfilled klines differ from live klines?
4. Did pool resolver failure explain no quote / no entry?
```

Repair-owner mapping:

```text
MARKET_DATA_PROVENANCE_GAP
KLINE_BAR_ALIGNMENT_GAP
POOL_RESOLUTION_GAP
BACKFILL_LIVE_MISMATCH
```

### §15.15.12 P10 — capital, fees, position sizing, and global risk

Observed files:

```text
src/decision/position-sizer.js
src/risk/risk-manager.js
scripts/v27_mirror_paper_ledgers.py
scripts/v27_mirror_execution_control.py
config/v27-execution-exit-safety-policy.json
```

Why it matters:

```text
A quote-clean would_enter row may still not be entered because capital, max
concurrency, daily loss, fee schedule, or reserve policy blocked it. That is
not matrix failure.
```

Fullnet row additions:

```text
risk.position_size_requested
risk.position_size_approved
risk.position_sizer_reason
risk.daily_loss_limit_active
risk.max_concurrency_active
risk.capital_reservation_ok
risk.entry_budget_available
risk.exit_safety_reserve_available
fees.fee_schedule_id
fees.fee_schedule_version
fees.estimated_fee_bps
fees.actual_fee_bps
fees.fee_source
ledger.capital_ledger_ok
ledger.position_ledger_ok
ledger.double_entry_invariant_ok
```

Audit questions:

```text
1. Did capital/risk budget block dog entries?
2. Did fee schedule or friction estimate change EV sign?
3. Did exit safety reserve correctly preempt new entry?
4. Did ledger invariants hold for entered rows?
```

Repair-owner mapping:

```text
CAPITAL_OR_RISK_BUDGET_BLOCK
POSITION_SIZER_BLOCK
FEE_SCHEDULE_GAP
LEDGER_INVARIANT_GAP
```

### §15.15.13 P11 — autonomy / self-iteration / strategy research memory

Observed files:

```text
src/analytics/self-iteration-manager.js
src/analytics/auto-tuner.js
src/optimizer/autoresearch-loop.js
src/optimizer/challenger-generator.js
src/optimizer/champion-challenger.js
src/optimizer/strategy-mutator.js
src/database/experiment-store.js
src/database/strategy-research-memory-store.js
config/strategy-goal.yaml
```

Why it matters:

```text
Autonomy can change thresholds, propose challengers, or record research memory.
Fullnet needs to know whether a row was generated under a stable policy or an
active experiment.
```

Fullnet artifact additions:

```text
autonomy.run_id
autonomy.event_id
autonomy.experiment_id
autonomy.champion_id
autonomy.challenger_id
autonomy.mutator_version
autonomy.policy_snapshot_hash
autonomy.strategy_goal_version
autonomy.target_capture_rate
autonomy.target_win_rate
autonomy.target_roi
autonomy.research_memory_id
autonomy.human_approved
autonomy.auto_tune_active
```

Audit questions:

```text
1. Did thresholds or mode config change inside the audit window?
2. Did a challenger mode mix with champion rows?
3. Did auto-tuner output affect live/paper behavior without promotion evidence?
4. Can every row be tied to a policy snapshot?
```

Repair-owner mapping:

```text
AUTONOMY_POLICY_DRIFT
EXPERIMENT_MIXED_WITH_BASELINE
CHAMPION_CHALLENGER_AMBIGUOUS
AUTO_TUNE_UNAPPROVED
```

### §15.15.14 P12 — source/channel registry and channel-level priors

Observed files:

```text
config/v27-source-registry.json
config/channels.csv
src/scoring/signal-source-optimizer.js
src/tracking/hunter-performance.js
scripts/discover-alpha-sources.js
```

Why it matters:

```text
Source quality is not only Telegram message text. Channel tier, active window,
parser template version, source status, and historical reject ratio affect
which signals should be eligible for comparison.
```

Fullnet row additions:

```text
source_registry.source_id
source_registry.source_name
source_registry.source_tier
source_registry.source_status
source_registry.parser_template_version
source_registry.active_from
source_registry.active_to
source_registry.allowed_modes
channel_csv.tier
channel_csv.status
channel_csv.historical_30_120_ev
channel_csv.reject_ratio_24h
source_optimizer.source_score
source_optimizer.source_weight
source_optimizer.hunter_performance_id
```

Audit questions:

```text
1. Did dogs cluster in sources currently restricted to observe_only?
2. Did source parser template changes alter dog/dud distributions?
3. Did source prior add signal beyond raw source gate?
4. Are blacklisted sources excluded from promotion evidence?
```

Repair-owner mapping:

```text
SOURCE_REGISTRY_MODE_LIMIT
SOURCE_CHANNEL_PRIOR_SIGNAL
SOURCE_STATUS_BLOCK
SOURCE_TEMPLATE_DRIFT
```

### §15.15.15 P13 — operator/security/export/legal gates

Observed files:

```text
config/v27-access-control-policy.json
config/v27-security-session-policy.json
config/v27-log-redaction-policy.json
config/v27-dashboard-action-separation-policy.json
config/v27-api-response-policy.json
config/v27-api-response-envelope-policy.json
```

Why it matters:

```text
These should not become alpha fields. They matter only when an operator action,
export, log, or dashboard mutation affects evidence eligibility.
```

Artifact/window additions:

```text
operator.admin_session_valid
operator.mutation_route_post_only
operator.audit_required
operator.audit_present
operator.break_glass_access_used
api.response_envelope_valid
api.export_watermark_valid
api.dashboard_query_provenance
security.log_redaction_ok
security.secret_access_audit_ok
legal.provider_terms_ok
legal.data_license_ok
legal.export_reimport_boundary_ok
```

Audit questions:

```text
1. Did a dashboard/admin mutation affect the row?
2. Was exported evidence watermarked and reproducible?
3. Did access or log redaction failure block use of an artifact?
4. Is this evidence eligible to be shared/reused for training?
```

Repair-owner mapping:

```text
OPERATOR_MUTATION_AFFECTED_EVIDENCE
API_EXPORT_OR_ENVELOPE_INVALID
SECURITY_OR_SECRET_AUDIT_GAP
LEGAL_OR_PROVIDER_TERMS_BLOCK
```

### §15.15.16 P14 — evidence conflict / aging / assumptions / false-negative budget

Observed in readiness contracts:

```text
EvidenceConflictContract
EvidenceAgingContract
MarketRegimeInvalidatesEvidence
SourceAlphaDecayExitCriteria
FalseNegativeBudgetContract
SmallSampleDecisionPolicy
SafetyVsCaptureTradeoffContract
AssumptionRegistryContract
AssumptionInvalidationTrigger
ContractPriorityGraph
ContractConflictResolutionContract
RegressionBudgetContract
ComplexityBudgetContract
UnknownUnknownsSamplingContract
```

Why it matters:

```text
This is the mechanism that stops old March evidence, small-n one-day evidence,
or conflicting module evidence from being promoted too far.
```

Fullnet artifact additions:

```text
evidence_conflict.conflict_present
evidence_conflict.conflicting_modules
evidence_conflict.resolution_policy
evidence_aging.evidence_age_days
evidence_aging.market_regime_match
evidence_aging.source_alpha_decay_status
assumption.assumption_id
assumption.status
assumption.invalidation_triggered
false_negative_budget.budget_id
false_negative_budget.allowed_false_negative_rate
false_negative_budget.observed_false_negative_rate
small_sample_policy.sample_n
small_sample_policy.warning
safety_capture_tradeoff.policy_id
regression_budget.status
complexity_budget.status
unknown_unknowns_sampling.sampled
```

Audit questions:

```text
1. Is old evidence still valid in the current market/source regime?
2. Did a repair reduce false negatives without blowing up dud entries?
3. Did small-n warnings block promotion but still allow research readout?
4. Are conflicting module signals resolved deterministically?
```

Repair-owner mapping:

```text
EVIDENCE_CONFLICT_UNRESOLVED
EVIDENCE_AGED_OUT
ASSUMPTION_INVALIDATED
FALSE_NEGATIVE_BUDGET_EXCEEDED
SMALL_SAMPLE_BLOCK
SAFETY_CAPTURE_TRADEOFF_UNRESOLVED
```

### §15.15.17 Explicit non-row exclusions

These are scanned and deliberately not row-level fullnet fields unless they
directly affect evidence eligibility:

```text
1. pure dashboard layout and UI rendering
2. generated client files, unless schema compatibility fails
3. backup/deprecated source files, unless runtime references them
4. raw secret values, session tokens, and private keys
5. legal hold details beyond legal/evidence eligibility status
6. notification formatting, unless notification failure causes operator action
7. prompt text bodies, unless prompt_hash/prompt_version affects an AI decision
8. log redaction internals, unless redaction failure blocks artifact use
9. static docs/runbooks, except runbook_freshness status for readiness
10. old historical strategy claims, unless reprojected into current row v2
```

### §15.15.18 Final fullnet module map

After this sweep, every meaningful module found in the repository maps to one of
these buckets:

```text
A. Direct capture modules
   source metadata, lifecycle, Markov, filters/gates, matrix, readiness, quote,
   would_enter, entry, ledger, exit/hold, friction.

B. Runtime decision and shadow feature modules
   GMGN policy, freshness, token memory, A-class internals, source resonance,
   narrative/social, smart money, curve/pumpfun, scout quality.

C. Deep persisted fact sources
   premium signals, source snapshots, gates, shadow outcomes, paper path samples,
   watchlist, lifecycle tracks, kline cache, source/channel DBs.

D. Operational truth and execution-control modules
   parser/session, identity/unit/finality, null/precision, raw provider, quote
   intent, idempotency, write path, worker health, paper/live boundary.

E. Denominator and evidence eligibility modules
   denominator projection, label finalization, ex-ante feasibility, earliest
   actionable time, feature availability, training manifest, detector
   calibration.

F. Promotion/governance modules
   mode readiness, metric/threshold catalog, release safety, holdout, negative
   controls, promotion guardrails, evidence conflict/aging, assumptions,
   false-negative budget.

G. Explicit non-row exclusions
   UI, secrets, generated clients, legal detail, docs, deprecated files, unless
   they affect evidence eligibility.
```

### §15.15.19 Final reports to add

Add these reports after §15.14 reports:

```text
1. mode-readiness-contract-report.json
2. metric-threshold-taxonomy-report.json
3. denominator-label-finality-report.json
4. ex-ante-actionability-report.json
5. feature-training-leakage-report.json
6. detector-markov-calibration-report.json
7. release-promotion-guardrail-report.json
8. onchain-snapshot-gate-raw-report.json
9. waiting-room-ai-advisory-report.json
10. market-data-provenance-report.json
11. capital-fee-risk-budget-report.json
12. autonomy-experiment-policy-report.json
13. source-channel-registry-report.json
14. operator-security-export-eligibility-report.json
15. evidence-aging-conflict-assumption-report.json
16. final-module-closure-coverage-report.json
```

The final module-closure report must show:

```text
module_group
source_files_seen
row_level_fields_added
artifact_level_fields_added
explicitly_excluded
reason_for_exclusion
repair_owner_added
tests_added_or_required
coverage_status = covered | intentionally_excluded | blocked
```

### §15.15.20 Final repair-owner priority

Replace the repair-owner ladder with this final order:

```text
0. ROW_IDENTITY_UNIT_OR_DENOMINATOR_INVALID
1. SOURCE_PARSER_SESSION_OR_REGISTRY_GAP
2. FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP
3. OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP
4. PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP
5. QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP
6. EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP
7. MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP
8. DIRECT_STRATEGY_DECISION_MODULE_GAP
9. ENTRY_BRIDGE_GAP
10. LEDGER_EXIT_HOLD_FRICTION_GAP
11. SHADOW_ONLY_OR_ADVISORY_ONLY
12. INTENTIONALLY_EXCLUDED_NOT_CAPTURE_RELEVANT
```

### §15.15.21 Stop condition

This is the stop condition for module discovery:

```text
1. All config files are mapped to a fullnet bucket or explicit exclusion.
2. All src directories are mapped to a fullnet bucket or explicit exclusion.
3. All v27 readiness contracts are mapped to a fullnet bucket or explicit
   exclusion.
4. All scripts that mirror, record, backfill, audit, repair, replay, or evaluate
   evidence are mapped to a fullnet bucket or explicit exclusion.
5. Any new future module must declare:
   - direct_capture
   - shadow_feature
   - persisted_fact_source
   - operational_truth
   - denominator_truth
   - promotion_governance
   - explicit_non_row_exclusion
```

If a future scan finds a module that does not fit these seven buckets, the
fullnet design is incomplete and §15 must be reopened. Until then, stop
searching for new buckets and implement the projection.

### §15.15.22 Definition of done

This goal is complete when:

```text
1. §15.15 is written into this document.
2. A final module-closure coverage report exists.
3. Every discovered module is assigned to A-G in §15.15.18.
4. Every report in §15.15.19 is generated or explicitly skipped with a blocker.
5. Repair-owner ladder in §15.15.20 is implemented.
6. Explicit non-row exclusions in §15.15.17 are enforced.
7. Row confidence from §15.14 and evidence eligibility from §15.15 jointly gate
   actual EV and promotion claims.
8. Old FBR/RED-bar/Stage2A/Stage3/68% evidence cannot be used unless it passes:
   denominator truth, feature availability, market-data provenance, ex-ante
   feasibility, and current-window dog/dud + EV revalidation.
9. No live/gate/entry/exit/size behavior changes.
```

### §15.15.23 Final implementation order

The full implementation order is:

```text
1. §15.14 evidence truth controls.
2. §15.15 denominator / feature / market-data / mode-readiness controls.
3. §15.13 runtime decision modules.
4. §15.12 deep persisted sources.
5. §15.15 promotion / evidence-aging / assumption controls.
6. Current-window rerun across:
   - formal sustained dog
   - peak>=50 clean
   - peak>=100 clean
   - peak>=5x clean
   - peak>=10x clean
   - formal dud
7. Only then decide which old entry evidence survives.
```

This is the final planned fullnet scope. The next step is implementation, not
more module discovery, unless a new runtime file or data source appears.

---

## §15.16 IMPLEMENTED — fullnet row v2 (closure + confidence + final repair-owner) signed off (2026-06-21)

Scope: §15.12-§15.15 row v2 projection. Research-only; v1 (§13) left frozen; v2 is a thin additive
layer in `scripts/build-live-fullnet-row-v2.js` wrapping the v1 `buildReport`. No strategy/gate/
entry/exit/size change, no threshold tuning, and no historical FBR/RED-bar/Stage2A/68% evidence used
as a trading rule (those stay bucket-G artifact-only per §15.15.17/§15.15.22 #8).

### Deliverables (all 6) — `sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2/`
```text
row.jsonl                                  329 rows = v1 chain + v2 fields
summary.json                               projection_complete + ev_gate + reports + carried v1 context
separability.json                          v1 component separability (carried)
row-confidence-report.json                 §15.14.14 ladder distribution
repair-owner-report.json                   §15.15.20 ladder distribution (dog vs dud)
final-module-closure-coverage-report.json  §15.15.18 A-G map operationalized
```

### Row v2 additions (every row)
```text
module_closure_flags{source,denominator,lifecycle,markov,decision,quote,would_enter,entry,ledger,exit_hold,friction}  (funnel coverage)
row_confidence ∈ {HIGH,MEDIUM,LOW,INVALID_FOR_EV,INVALID_FOR_PROMOTION} + row_confidence_warnings[]
ev_eligible (true only with valid entered+ledger+exit+friction+realized_pnl)
ops_evidence_present=false (+reason), promotion_evidence_present=false (+reason)   # §15.14 D / §15.15 F not exported
final_repair_owner_v2 ∈ §15.15.20 ladder (or NONE_NO_REPAIR_NEEDED for a captured row)
```

### Readout (fresh-20260621T123643Z, 329 signals)
```text
row_confidence: MEDIUM 278, LOW 51, HIGH 0  (HIGH needs ops+promotion evidence; not exported -> honest ceiling)
ev_gate: BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER  -> actual_net_ev_pct=null  (fail-closed; ev_eligible=0)
final repair-owner ladder (evidence-truth-before-strategy):
  [2] FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP : 106 (dog 5)   <- LARGEST, above strategy
  [7] MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP         : 73  (dog 8)
  [8] DIRECT_STRATEGY_DECISION_MODULE_GAP                : 69  (dog 7)
  [11] SHADOW_ONLY_OR_ADVISORY_ONLY                      : 60  (dog 5)
  [9] ENTRY_BRIDGE_GAP                                   : 13  (dog 2)
  [5] QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP  : 8   (dog 2)
  (all 329 assigned; deterministic, first matching ladder level wins)
module closure: 60 modules -> 22 covered / 30 blocked (need new exports) / 8 intentionally_excluded;
  every module mapped to A-G; bucket G all intentionally_excluded.
Interpretation: before blaming strategy, the dominant non-capture owner is market-data/feature
provenance (kline/raw gaps + KLINE-blocked source) + mode-readiness — i.e. evidence/ops truth, exactly
the §15 thesis. 30/60 modules are blocked = the honest measurement-gap map (not strategy failure).
```

### DoD (§15.12.9 / §15.13.6 / §15.14.18 / §15.15.22) — met
```text
- every signal: source->denominator->lifecycle/Markov->decision->quote->would_enter->entry->ledger/
  exit/friction, each value-or-missing_reason (projection_complete=true) + row_confidence + repair owner  YES
- every discovered module mapped to A-G or explicitly excluded (§15.15.18)                                 YES (60 modules)
- §15.15.20 repair-owner ladder implemented + enforced (deterministic, 0..12 priority)                     YES
- §15.15.17 non-row exclusions enforced (bucket G)                                                         YES
- row_confidence + evidence eligibility gate actual EV and promotion (EV null unless ev_eligible)          YES
- old FBR/RED-bar/Stage2A/68% cannot be used as a rule (bucket-G artifact-only)                            YES
- no live/gate/entry/exit/size behavior change                                                            YES (sas-research only)
- tests pass                                                                                              YES
```

### Verification
```text
node --check x6 OK | node --test 6 suites / 6 pass / 0 fail | v2 closed-loop invariants 17/17
tests prove: projection_complete=true; actual EV stays null unless valid entered/fill/exit/ledger;
  row_confidence deterministic; INVALID_FOR_EV when entered without valid ledger; every module ∈ A-G;
  every row assigned a deterministic §15.15.20 ladder owner.
```

```text
GOAL_COMPLETE (§15.12-§15.15 row v2)
ROW_V2_PROJECTION_COMPLETE=TRUE (329 signals, full chain + missing_reason + confidence)
EVERY_MODULE_MAPPED_A_G (60: 22 covered / 30 blocked / 8 excluded)
REPAIR_OWNER_LADDER_15_15_20_ENFORCED (deterministic, all 329 assigned)
ROW_CONFIDENCE_AND_EV_ELIGIBILITY_GATE_EV (actual EV null, fail-closed)
HISTORICAL_FBR_REDBAR_STAGE2A_68PCT = ARTIFACT_ONLY (bucket G)
TESTS_PASS (check x6 + test 6/6 + invariants 17/17)
NO_EDGE_DECLARED
NO_LIVE_STRATEGY_CHANGE
```

### §15.16.1 Scope clarification + reviewer caveats (independent review, accepted 2026-06-21)

Sign-off is **PASS with caveats**. What "complete" means here must stay narrow:

```text
COMPLETED = the row v2 AUDIT LAYER: per-signal module-closure map + row_confidence + ev_eligibility
            + §15.15.20 repair-owner ladder, self-consistent / reproducible / tested.
NOT COMPLETED = "all 60 modules are wired to live data and contributing judgment."
            22 modules are covered by current exports; 30 are blocked / needs-new-export; 8 are
            intentionally excluded (bucket G). The closure report's value is precisely that it makes
            the 30 blocked modules explicit, not that they are connected.
```

Accepted caveats (all are evidence/repro hygiene, not strategy):

```text
1. Wording "§15.12-§15.15 fully implemented" is narrowed to "row v2 projection + closure/confidence/
   repair-owner audit layer implemented; 30 modules remain blocked pending new exports."
2. --source-24h must be passed for byte-identical reruns; without it narrative_score/ai_narrative_tier
   become null (row still projects). Fixed: build-live-fullnet-row-v2.js --help now states this.
3. Test/CLI dependency: sas-research has no node_modules, so better-sqlite3 must be resolvable. Run:
     NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules node --test tests/*.mjs
   (independent rerun confirmed 6/6 pass this way; bare `node --test` fails with Cannot find module
   'better-sqlite3' — that is an env path issue, NOT a test failure). Now documented in --help.
4. Reproducibility confirmed by reviewer: row.jsonl + separability + row-confidence + repair-owner +
   module-closure are byte-identical on rerun; summary.json differs only by generated_at.
```

Next step is NOT strategy tuning. It is to connect the 30 blocked modules by adding their exports,
in repair-owner priority order: market-data/feature provenance (kline provenance, feature
availability) -> mode readiness -> ops/promotion evidence -> ex-ante actionability. Only after those
are covered (and real entries/fills exist) can EV or any selection-edge claim be revisited.

---

## §15.17 STATUS — Goal 2: market-data / feature provenance blocked-module export (2026-06-21)

Goal: turn the largest v2 repair owner FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP (106 rows
/ 5 dogs) from a coarse blocked black box into row-level auditable provenance. Research-only, read
ONLY from artifacts already on disk; no live-dir read, no strategy/gate/entry/exit/size change, no
threshold tuning, EV stays fail-closed. v1 + v2 (Goal 1) imported unchanged; this is an additive layer
in `scripts/build-live-fullnet-row-v2-marketdata.js`.

### Blocked-module inventory (Goal 2 scope) + where the evidence actually lives
```text
module_group        | was      | likely_source (found)                              | can_export_now | result
kline_cache         | blocked  | raw_signal_outcomes.snapshot.db: kline_covered/    | YES (on disk)  | COVERED
                    |          |   coverage_reason/provider/pool_found/early_15m_bar_count
feature_availability| blocked  | source-to-raw-rows.json: market_cap/volume_24h/    | YES (on disk)  | COVERED
                    |          |   holders/top10_pct/narrative_score                |                | (holders/top10 field-missing)
raw_provider_evidence| blocked | raw_signal_outcomes.snapshot.db: provider/         | YES (on disk)  | COVERED
                    |          |   baseline_provider/path_provider/pool_found       |                |
quote provider      | covered  | v1 decision risk_json: quote_source/route_source   | YES (already)  | explicit quote_provider_* fields added
curve_pumpfun       | blocked  | (no curve/pumpfun microstructure in these sources) | NO             | BLOCKED (precise reason)
source_channel_dbs  | blocked  | source_kind/source_family only (partial); registry | NO             | BLOCKED (precise reason)
quote_intent_binding| blocked  | provider source covered; intent->fill binding absent| NO            | BLOCKED (precise reason)
```

### Covered now (reclassified blocked -> covered, proof-gated, no silent flip)
```text
kline_cache           : kline_seen (=kline_covered), kline_source, kline_bars_n (=early_15m_bar_count),
                        kline_window_sec(=900), kline_cache_hit (provider=local_cache), kline_coverage_pct,
                        kline_missing_reason. Evidence: every cohort signal joined a raw_signal_outcomes row.
feature_availability  : feature_vector_seen, feature_vector_fields_present/missing, missing_reason.
raw_provider_evidence : raw_provider_seen, raw_provider_source, kline_pool_found, raw_provider_missing_reason.
=> module closure: covered 22 -> 25, blocked 30 -> 27, intentionally_excluded 8 (total 60, reconciles).
```

### Still blocked (each with its UNIQUE precise reason)
```text
curve_pumpfun        : curve_pumpfun_microstructure_not_in_raw_signal_outcomes_or_source_to_raw
source_channel_dbs   : channel_registry_priors_not_exported__source_kind_family_only_partial
quote_intent_binding : quote_provider_source_now_covered_but_intent_to_fill_binding_not_exported__requires_zeabur_api_export
kline_cache (live x-check): live_kline_cache_db_crosscheck_requires_live_dir_read__signal_time_provenance_covered_instead
(plus the other §15.13/§15.14/§15.15 ops/governance/denominator modules unchanged from v2 — out of Goal-2 scope)
```

### The de-blackboxing (the actual win)
```text
FEATURE_AVAILABILITY_OR_MARKET_DATA_PROVENANCE_GAP = 106 rows is now itemized (sums to 106):
  kline_baseline_only_no_early_bars : 61   (kline_covered=0: baseline price but no early 15m bars)
  feature_missing:holders|top10_pct : 45   (kline present, but holders/top10_pct not exported)
Repair-owner DISTRIBUTION is byte-identical to v2 (same §15.15.20 ladder predicates) — the change is
itemization, not reclassification. So "distribution change" is explainable: there is none; only detail.
Market-data readout: kline_seen 170/329 (all 29 dogs kline_seen; pending mostly lack kline -> that is
why they are pending); kline_source gmgn 228 / geckoterminal 98 / none 3; raw_provider_seen 326/329;
quote_provider_seen 261/329; pool_unresolved 3; md_confidence HIGH 132 / MEDIUM 38 / LOW 159.
```

### New artifact + 17 row fields
```text
Artifact: sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-marketdata/
  row.jsonl, summary.json, repair-owner-report.json, row-confidence-report.json,
  final-module-closure-coverage-report.json, market-data-provenance-report.json,
  feature-availability-report.json
Row fields added: kline_seen, kline_source, kline_bars_n, kline_window_sec, kline_cache_hit,
  kline_coverage_pct, kline_pool_found, kline_first_bar_lag_sec, kline_missing_reason, raw_provider_seen,
  raw_provider_source, raw_provider_missing_reason, feature_vector_seen, feature_vector_fields_present,
  feature_vector_fields_missing, feature_vector_missing_reason, quote_provider_seen, quote_provider_source,
  quote_provider_missing_reason, market_data_provenance_confidence, market_data_provenance_warnings,
  market_data_repair_detail. (All v1+v2 fields preserved unchanged.)
```

### Verification
```text
node --check x7 OK.
NODE_PATH=.../sentiment-arbitrage-system/node_modules node --test (7 relevant suites): 7 pass / 0 fail.
  (bare `node --test tests/*.mjs` runs all repo tests; 23 pre-existing failures in OTHER files are not
   from Goal 2 — none of the 7 Goal-2 suites appear in the failure set.)
Independent JSON invariants: 13/13 pass (row_count 329, projection_complete=true, EV null+BLOCKED,
  module count 60 reconciles 25/27/8, every module A-G, G excluded, market-data detail sums to 106,
  every row has all 17 fields, v2 fields preserved, kline_seen_n consistent).
Byte-reproducible rerun: row.jsonl + all 5 reports IDENTICAL; summary.json identical sans generated_at.
New tests: tests/build-live-fullnet-row-v2-marketdata.test.mjs (pure fns: kline/feature/quote provenance,
  confidence, repair-detail, proof-gated closure reclassification, EV fail-closed).
```

### Acceptance vs goal
```text
row_count 329 ...................................... YES
projection_complete=true ........................... YES
actual_net_ev fail-closed (null) ................... YES (no real entered+ledger+exit+friction)
no live/strategy/gate/entry/exit/size files touched  YES (sas-research scripts/tests/docs only; the only
  item under sentiment-arbitrage-system is kline_cache.db-shm, a SQLite WAL sidecar touched by a
  read-only .tables probe / the live process — NO source/config/strategy/data-content file modified)
module count reconciles ............................ YES (60 = 25 covered + 27 blocked + 8 excluded)
market-data covered count increased / reasons precise YES (+3 covered; remaining blocked have unique reasons)
FEATURE_AVAILABILITY...GAP no longer a black box ... YES (106 = 61 no-early-bars + 45 feature-missing)
repair-owner distribution change explainable ....... YES (unchanged; same ladder; only itemized)
all new fields test-covered ........................ YES
```

```text
GOAL_2_COMPLETE (§15.17 market-data / feature provenance)
MARKET_DATA_OWNER_DEBLACKBOXED (106 -> 61 no_early_bars + 45 feature_missing)
CLOSURE_COVERED 22 -> 25 (kline_cache, feature_availability, raw_provider_evidence; proof-gated)
STILL_BLOCKED_WITH_PRECISE_REASON (curve_pumpfun, source_channel_dbs, quote_intent_binding, live_kline_xcheck)
EV_FAIL_CLOSED (actual_net_ev null) | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329
TESTS_PASS (check x7 + 7 relevant suites + invariants 13/13 + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING
```

---

## §15.18 STATUS — Goal 3: mode-readiness / promotion-governance evidence export (2026-06-21)

Goal: turn MODE_READINESS_OR_PROMOTION_GOVERNANCE_GAP (73 rows / 8 dogs) from a coarse blocked owner
into row-level, per-entry_mode, per-tier auditable evidence. Research-only. No mode opened/promoted,
no shadow mode enabled, no live/gate/entry/exit/size change, no threshold tuning, EV stays fail-closed.
Additive layer `scripts/build-live-fullnet-row-v2-mode-readiness.js` on top of Goal 2 (v2-marketdata),
imported unchanged. Goal 1/2 row fields preserved verbatim; v2 + v2-marketdata still byte-reproduce.

### Evidence source (read-only, frozen into the pack for reproducibility)
```text
config/entry-mode-registry.json     -> mode-config/entry-mode-registry.json (sha 019c78db...)
config/v27-entry-point-inventory.json -> mode-config/v27-entry-point-inventory.json (sha 96d22ed8...)
Live config/src were only READ + copied into sas-data-room/mode-config (no live-dir hot-DB read, no
live file modified). Registry contract: tiers{live,isolated_paper_capped,revival_canary,
shadow_watch_only,hard_shadow,deprecated_shadow}, 35 modes{tier,route,paper_enabled,size_class},
promotion_policy{shadow_watch_to_isolated, isolated_to_live, deprecation} with numeric thresholds.
```

### Covered now (reclassified blocked -> covered, proof-gated; layered on Goal-2 closure)
```text
mode_readiness          : per-signal entry_mode->tier projection + registration + promotion status.
metric_threshold_catalog: promotion_policy numeric thresholds (min_unique_tokens 30 / min_quote_clean_samples 20 / median_pnl_pct_gte -2 / max_single_trade_contribution_pct_lte 50).
promotion_guardrails    : promotion_policy stages (shadow->isolated->live + deprecation, requires_manual_review).
release_safety          : deprecation window (14d) + decision_gates.
=> module closure: covered 25 -> 29, blocked 27 -> 23, intentionally_excluded 8 (total 60, reconciles).
```

### Still blocked (each with its UNIQUE precise reason)
```text
holdout_negative_controls        : holdout_negative_controls_not_in_registry_or_config__requires_runtime_readmodel_export
evidence_conflict_aging          : evidence_conflict_aging_readmodel_not_exported
assumptions_false_negative_budget: assumptions_false_negative_budget_not_exported
(plus the §15.13/§15.14 ops modules unchanged from v2 — out of Goal-3 scope)
```

### How the 73 MODE_READINESS owner rows were decomposed (de-blackboxing)
```text
mode_readiness_repair_detail_breakdown: readiness_route_or_quote_not_executable = 73 (100%)
=> the "mode readiness" owner is, proximately, ROUTE/QUOTE EXECUTABILITY inside the readiness gate
   (quote_clean=true but executable_quote_clean=false), NOT a missing promotion-governance contract.
Governance finding (separate, all decision rows): entry_mode 'a_grade_resonance_fastlane' is UNREGISTERED
   — 278/278 decision rows run a mode that is NOT in the 35-mode registry or virtual_modes
   (mode_tier=unregistered_unknown, mode_promotion_status=unregistered_no_promotion_path). This is
   §15.15.2 audit Q3 answered YES: an unregistered entry point drove would_enter/decision in-window.
mode_promotion_eligible = 0 across all rows (fail-closed: unregistered + requires_manual_review + no real entries).
Repair-owner DISTRIBUTION is byte-identical to v2/v2-marketdata (same §15.15.20 ladder) — change is
   itemization + registry status, not reclassification (explainable: no distribution change).
```

### New artifact + row fields
```text
Artifact: sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-mode-readiness/
  row.jsonl, summary.json, repair-owner-report.json, row-confidence-report.json,
  final-module-closure-coverage-report.json, mode-readiness-report.json,
  promotion-governance-report.json, entry-mode-tier-report.json
36 row fields added (all Goal 1/2 fields preserved verbatim): entry_mode_seen, entry_mode_registry_seen,
  entry_mode_registry_missing_reason, mode_tier, mode_tier_paper_enabled, entry_point_registered,
  mode_promotion_status, mode_promotion_eligible, mode_promotion_block_reason, mode_demotion_status,
  mode_daily_cap_seen(+reason), mode_loss_cap_seen(+reason), mode_cooldown_policy_seen, mode_shadow_only,
  mode_isolated_paper_capped, mode_hard_shadow, mode_deprecated_shadow, mode_live_allowed,
  mode_readiness_score, mode_readiness_confidence, mode_readiness_warnings, metric_threshold_catalog_seen(+reason),
  promotion_guardrails_seen(+reason), release_safety_seen(+reason), holdout_negative_controls_seen(+reason),
  evidence_conflict_aging_seen(+reason), assumptions_false_negative_budget_seen(+reason), mode_readiness_repair_detail
```

### Verification
```text
node --check x8 OK.
node --test (8 relevant suites, NODE_PATH=.../sentiment-arbitrage-system/node_modules): 8 pass / 0 fail.
Independent JSON invariants 13/13: row_count 329, projection_complete=true, EV null+BLOCKED, module count
  60 reconciles (29/23/8), every module A-G, G excluded, mode-readiness owner detail sums to 73,
  promotion_eligible=0, every row has mode + Goal1/2 fields.
Goal 1/2 preservation: 0/329 rows drift vs v2-marketdata after stripping the 36 new keys (verbatim).
Byte-reproducible: mode-readiness all 7 reports identical + summary identical sans generated_at;
  v2 + v2-marketdata STILL reproduce byte-identical (no regression).
New tests: tests/build-live-fullnet-row-v2-mode-readiness.test.mjs (registry parse, mode projection
  registered/unregistered/deprecated, repair-detail precedence, additive augment, proof-gated closure).
```

### Acceptance vs goal
```text
row_count 329 / projection_complete=true / actual_net_ev fail-closed .......... YES
no live/strategy/gate/entry/exit/size files touched ........................... YES (read-only config read + frozen copy into sas-data-room; live config/src unmodified)
module count reconciles (60 = 29 covered + 23 blocked + 8 excluded) ........... YES
Goal 1/2 fields preserved row-by-row; v2/v2-marketdata byte-repro intact ...... YES
mode-readiness/promotion modules covered increased (+4) OR reasons precise .... YES (both)
MODE_READINESS owner no longer a black box ................................... YES (73 = readiness_route_or_quote_not_executable; + unregistered-mode finding)
repair-owner distribution change explainable ................................. YES (unchanged; itemized only)
all new fields test-covered .................................................. YES
```

```text
GOAL_3_COMPLETE (§15.18 mode-readiness / promotion-governance)
MODE_READINESS_OWNER_DEBLACKBOXED (73 = readiness_route_or_quote_not_executable; mode unregistered 278/278)
CLOSURE_COVERED 25 -> 29 (mode_readiness, metric_threshold_catalog, promotion_guardrails, release_safety; proof-gated)
STILL_BLOCKED_WITH_PRECISE_REASON (holdout_negative_controls, evidence_conflict_aging, assumptions_false_negative_budget)
GOAL_1_2_FIELDS_PRESERVED_VERBATIM | V2_AND_V2_MARKETDATA_BYTE_REPRO_INTACT
EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329 | PROMOTION_ELIGIBLE=0
TESTS_PASS (check x8 + 8 suites + invariants 13/13 + byte-reproducible)
NO_MODE_OPENED_OR_PROMOTED | NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING
```

---

## §15.19 STATUS — Goal 4: ops / worker-readmodel / quote-intent-binding / ex-ante actionability (2026-06-21)

Goal: turn the remaining ops/execution-truth/actionability blocked modules into row-level auditable
evidence, derived ONLY from same-window (06-21) artifacts on disk. Research-only. No live/gate/entry/
exit/size change, no threshold tuning, no mode opened/promoted, EV fail-closed. Additive layer
`scripts/build-live-fullnet-row-v2-ops-actionability.js` on top of Goal 3 (v2-mode-readiness),
imported unchanged. Goal 1/2/3 row fields preserved verbatim; all prior layers still byte-reproduce.

### Covered now (reclassified blocked -> covered, proof-gated, same-window derivation)
```text
ex_ante_feasibility   : feasible = has_decision && executable_quote_clean (110 feasible / 9 dogs);
                        blocked reasons itemized (quote_not_executable 151, no_decision 51, quote_not_clean 17).
earliest_actionable   : first_route_decision_sec_after_signal (278 seen; avg 490.8s after signal).
paper_live_boundary   : observable — 0 entries/ledger in window => paper_only=329, live_violation=0,
                        scope{no_execution 252, shadow_only 61, would_enter_no_execution 16}.
null_precision        : numeric-precision audit of observable row fields (329 audited, 0 invalid rows).
identity_unit_finality: price unit + same-source-path from source-to-raw (usd_per_token/native).
quote_intent_binding  : intent side row-observable (77 entry_intent rows: intent_id + provider + route);
                        intent->fill binding absent (0 entries) -> per-row fill missing_reason.
=> module closure: covered 29 -> 35, blocked 23 -> 17, intentionally_excluded 8 (total 60, reconciles).
   (fill_anchor is carried as row fields under ex-ante: fill_anchor_seen 326, ts from kline first bar.)
```

### Still blocked (each with its UNIQUE precise reason — genuinely need a same-window runtime export)
```text
worker_health           : worker_readmodel_not_same_window_06-21__requires_runtime_readmodel_export
parser_session          : parser_session_not_exported__requires_runtime_readmodel_export
idempotency_write_path  : idempotency_write_path_not_exported__requires_runtime_readmodel_export
holdout_negative_controls / evidence_conflict_aging / assumptions_false_negative_budget : unchanged from §15.18 (precise).
NOTE: 06-19 v27 readmodel MIRROR logs exist on disk (live-log-probe-20260619T050844Z: earliest-actionable,
  quote-intent-binding, execution-control, paper-ledger, read-model-refresh) but are a DIFFERENT window
  and only supervisor logs (no same-window per-signal data) -> NOT joined (no-cross-window rule).
```

### Repair-owner itemization (ladder predicates unchanged => distribution unchanged, explainable)
```text
QUOTE_INTENT_PROVIDER_EXECUTOR_OR_IDEMPOTENCY_GAP = 8 rows -> route_or_quote_source_blocker_present = 8 (itemized).
OPS_WORKER_READMODEL_STORAGE_OR_CONFIG_GAP        = 0 proximate rows (closure-level blocked module, never first-match).
EX_ANTE_ACTIONABILITY_OR_FILL_ANCHOR_GAP          = 0 proximate rows (ex-ante derivable; never first-match here).
PAPER_LIVE_OPERATOR_WRITE_OR_SECURITY_GAP         = 0 proximate rows (no live crossing in window).
Distribution byte-identical to v2/v2-marketdata/v2-mode-readiness (same §15.15.20 ladder). Change = new
row evidence + itemization only, NOT reclassification.
```

### New artifact + row fields
```text
Artifact: sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-ops-actionability/
  row.jsonl, summary.json, repair-owner-report.json, row-confidence-report.json,
  final-module-closure-coverage-report.json, ops-worker-readmodel-report.json,
  quote-intent-binding-report.json, ex-ante-actionability-report.json, paper-live-boundary-report.json
51 row fields added (all Goal 1/2/3 fields preserved verbatim): ex_ante_feasibility_seen/ex_ante_feasible/
  ex_ante_blocked_reason, earliest_actionable_seen/ts/sec_after_signal, fill_anchor_seen/ts/price/source,
  paper_live_boundary_seen/execution_scope/paper_only/live_violation, null_precision_seen/invalid_numeric_fields,
  identity_unit_finality_seen/unit/path_unit/finality_reason, quote_intent_binding_seen/intent_id/provider/route/
  fill_anchor_ts/fill_bound, worker_health_seen/source/status, parser_session_seen/source, readmodel_snapshot_seen/
  source/freshness_sec, idempotency_key_seen/key/write_path_status, ops_actionability_confidence/warnings/repair_detail
  (+ *_missing_reason for each).
```

### Verification
```text
node --check x9 OK. node --test (9 relevant suites, NODE_PATH=.../sentiment-arbitrage-system/node_modules): 9 pass / 0 fail.
Independent JSON invariants 12/12: row_count 329, projection_complete=true, EV null+BLOCKED, module count 60
  reconciles (35/17/8), every module A-G, G excluded, paper_only 329 / live_violation 0, every row has ops + Goal1/2/3 fields.
Goal 1/2/3 preservation: 0/329 rows drift vs v2-mode-readiness after stripping the 51 new keys (verbatim).
Byte-reproducible: ops-actionability all 8 reports identical + summary identical sans generated_at;
  v2 + v2-marketdata + v2-mode-readiness STILL reproduce byte-identical (no regression).
New tests: tests/build-live-fullnet-row-v2-ops-actionability.test.mjs (ex-ante, paper-live boundary,
  null-precision audit, identity unit, quote-intent binding, ops-blocked evidence, proof-gated closure).
```

### Acceptance vs goal
```text
row_count 329 / projection_complete=true / actual_net_ev fail-closed .......... YES
no live/strategy/gate/entry/exit/size files touched ........................... YES (read-only same-window artifacts in sas-data-room; live config/src/data unmodified)
module count reconciles (60 = 35 covered + 17 blocked + 8 excluded) ........... YES
Goal 1/2/3 fields preserved row-by-row; all prior layers byte-repro intact .... YES
ops/actionability covered increased (+6) OR blocked reasons precise ........... YES (both)
repair-owner distribution change explainable ................................. YES (unchanged; itemized; ops/ex-ante/paper-live owners = 0 proximate rows)
all new fields test-covered .................................................. YES
```

```text
GOAL_4_COMPLETE (§15.19 ops / worker-readmodel / quote-intent / ex-ante actionability)
CLOSURE_COVERED 29 -> 35 (ex_ante_feasibility, earliest_actionable, paper_live_boundary, null_precision, identity_unit_finality, quote_intent_binding; proof-gated)
STILL_BLOCKED_WITH_PRECISE_REASON (worker_health, parser_session, idempotency_write_path + holdout/evidence_aging/assumptions)
06-19_V27_READMODEL_MIRRORS_NOT_JOINED (cross-window; precise blocked reason)
GOAL_1_2_3_FIELDS_PRESERVED_VERBATIM | ALL_PRIOR_LAYERS_BYTE_REPRO_INTACT
EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329 | PAPER_ONLY=329 LIVE_VIOLATION=0
TESTS_PASS (check x9 + 9 suites + invariants 12/12 + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
```

---

## §15.20 STATUS — Goal 5: final blocked-module closure + runtime/export requirements (2026-06-21)

Goal: final pass over the 17 modules still blocked after Goal 4 — cover what same-window artifacts
allow, and give every remaining blocked module an EXACT required export (no bare not_in_current_exports).
Research-only. Additive layer `scripts/build-live-fullnet-row-v2-final-blockers.js` on Goal 4
(v2-ops-actionability), imported unchanged. Goal 1/2/3/4 row fields preserved verbatim; all 4 prior
layers still byte-reproduce. No live/gate/entry/exit/size change, no mode opened/promoted, EV fail-closed.

### Covered now (reclassified blocked -> covered, proof-gated, same-window evidence)
```text
paper_path_samples : observable — canonical paper_trades join = 0 in-window => definitive 0 paper paths
                     per signal (paper_path_samples_seen=false, path_n=0, reason no_paper_trades_in_window).
source_channel_dbs : per-row channel identity from source-to-raw (fullnet 329 rows:
                     signal_source premium_channel_ath=158 / premium_channel=171;
                     source_family third_party_kline=326 / null=3). Channel-level PRIORS still need a registry export.
=> module closure: covered 35 -> 37, blocked 17 -> 15, intentionally_excluded 8 (total 60, reconciles).
```

### Still blocked (15) — each with its EXACT required export (no generic reason remains)
```text
runtime_readmodel_required (13) — same-window export from the live runtime read-models / ./data/v27_event_log:
  gmgn_policy            : export gmgn_policy decision state per (token,signal_ts) [scripts/gmgn_policy.py]
  token_memory           : export token failure-memory / lotto_reclaim state per token
  source_resonance       : export source_resonance cohort + lead_time_sec per signal [source_resonance_shadow.py]
  scout_quality          : export scout_quality score + block_reason per signal [scout_quality.py]
  watchlist              : export watchlist_store state per token [watchlist_store.py]
  parser_session         : export ingestion/parser session_id per signal (telegram-parser session table)
  idempotency_write_path : export idempotency key + write_path status per decision (sqlite_write_coordinator)
  worker_health          : export ./data/v27_event_log read-model-refresh + worker/readiness health (06-21)
  training_manifest      : export feature/training manifest_id + feature-vector snapshot per signal
  detector_calibration   : export detector/markov calibration metrics + model version [v27_markov_shadow_calibration]
  holdout_negative_controls : export holdout + negative-control cohort results per mode [v27 promotion read-model]
  evidence_conflict_aging   : export evidence conflict + aging read-model per evidence row
  assumptions_false_negative_budget : export assumptions + false-negative-budget read-model
zeabur_export_required (2) — external data sources not in the runtime read-models:
  smart_money            : Zeabur/API export of smart-money wallet signals per token
  curve_pumpfun          : Zeabur/on-chain pump.fun bonding-curve decode per token [helius pumpfun curve decode]
NOTE: gmgn/scout/resonance currently appear ONLY as a kline-provider or a normalized_mode label token
  (e.g. a_grade_resonance_fastlane) — not structured per-signal evidence. The 06-19 v27 readmodel mirror
  logs are cross-window (not joined). No silent reclassify.
```

### New artifact + row fields
```text
Artifact: sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-final-blockers/
  row.jsonl, summary.json, repair-owner-report.json, row-confidence-report.json,
  final-module-closure-coverage-report.json, final-blocker-requirements-report.json,
  runtime-readmodel-export-requirements.json, zeabur-export-requirements.json,
  shadow-source-scout-token-memory-report.json
50 row fields added (Goal 1/2/3/4 preserved verbatim; the 3 Goal-3 governance fields holdout/
  evidence_conflict_aging/assumptions are intentionally NOT re-emitted to avoid mutating them):
  paper_path_samples_seen/path_n, source_channel_registry_seen/source_channel/family/priors_seen,
  gmgn_policy_seen/source, token_memory_seen/failure_type/reclaim_policy, source_resonance_seen/cohort/
  lead_time_sec/in_mode_label, smart_money_seen/source, curve_pumpfun_seen/stage/bonding_curve,
  scout_quality_seen/score/block_reason/in_mode_label, watchlist_seen/state, training_manifest_seen/id,
  detector_calibration_seen/model/version, remaining_blocker_modules[], remaining_blocker_reasons[],
  runtime_readmodel_required[], zeabur_export_required[], shadow_source_scout_required[], token_memory_required[],
  final_blocker_confidence, final_blocker_warnings[] (+ *_missing_reason carrying the exact export).
```

### Verification
```text
node --check x10 OK. node --test (10 relevant suites, NODE_PATH): 10 pass / 0 fail.
Independent JSON invariants 13/13: row_count 329, projection_complete=true, EV null+BLOCKED, module count
  60 reconciles (37/15/8), no_generic_blocked_reason_remains=true, every blocked module has
  "<category>::<exact export>", 13 runtime + 2 zeabur, every row carries the aggregate lists + Goal1/2/3/4 fields.
Goal 1/2/3/4 preservation: 0/329 rows drift vs v2-ops-actionability after stripping the 50 new keys.
  (Caught + fixed one collision: Goal 5 must NOT re-emit the 3 Goal-3 governance *_missing_reason fields.)
Byte-reproducible: final-blockers all 8 reports identical + summary identical sans generated_at;
  v2 + v2-marketdata + v2-mode-readiness + v2-ops-actionability STILL byte-identical (no regression).
New tests: tests/build-live-fullnet-row-v2-final-blockers.test.mjs.
```

### Final 5-layer module closure (cumulative)
```text
60 modules = 37 covered / 15 blocked (13 runtime_readmodel + 2 zeabur, all exact) / 8 intentionally_excluded.
Goal 1 -> Goal 5 covered progression: 22 -> 25 -> 29 -> 35 -> 37.
```

### Acceptance vs goal
```text
row_count 329 / projection_complete=true / actual_net_ev fail-closed ........... YES
no live/strategy/gate/entry/exit/size files touched ............................ YES (read-only same-window artifacts + frozen mode-config; live unmodified)
module count reconciles ........................................................ YES (37+15+8=60)
Goal 1/2/3/4 fields preserved row-by-row; all prior layers byte-repro intact ... YES (0 drift; 4 layers byte-identical)
all remaining 17 covered / excluded / blocked-with-unique-precise-reason ....... YES (2 covered; 15 blocked with exact export; 0 newly excluded)
no generic not_in_current_exports remains without exact required export ........ YES (no_generic_blocked_reason_remains=true)
repair-owner distribution change explainable .................................. YES (unchanged; ladder predicates untouched)
all new fields test-covered ................................................... YES
```

### Final next-step recommendation
```text
The fullnet audit layer is closed: every module is covered, intentionally excluded, or blocked with an
EXACT required export. Two distinct data-pipeline workstreams remain (NOT strategy work):
  1. ONE same-window runtime read-model export job (./data/v27_event_log + the listed per-signal tables)
     would unblock 13 modules at once — highest leverage.
  2. TWO external Zeabur/on-chain exports (smart-money wallets, pump.fun curve decode) unblock the last 2.
Until real entered+ledger+exit+friction rows exist, EV stays null and no edge/promotion claim is possible.
This remains evidence export / auditability — not strategy tuning, not live execution, not edge proof.
```

```text
GOAL_5_COMPLETE (§15.20 final blocked-module closure + export requirements)
FINAL_CLOSURE 60 = 37 covered / 15 blocked / 8 excluded (covered 35 -> 37: paper_path_samples + source_channel_dbs)
EVERY_BLOCKED_MODULE_HAS_EXACT_REQUIRED_EXPORT (13 runtime_readmodel + 2 zeabur; no generic reason)
GOAL_1_2_3_4_FIELDS_PRESERVED_VERBATIM | ALL_PRIOR_LAYERS_BYTE_REPRO_INTACT
EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329
TESTS_PASS (check x10 + 10 suites + invariants 13/13 + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
```

---

## §15.21 NEXT GOAL — Goal 6: same-window runtime readmodel export + join layer (2026-06-22)

### Why this is the next step
```text
Goal 1-5 closed the audit net: every module is now covered, intentionally excluded, or blocked with an
exact required export. The remaining 15 blocked modules are no longer a strategy question:

  13 modules need ONE same-window runtime readmodel export.
   2 modules need external Zeabur/on-chain exports.

The highest-leverage next step is therefore Goal 6:

  Build the runtime readmodel export contract and join it back into fullnet row v2.

This is still research-only / evidence-only. It must not tune thresholds, open modes, change entry/exit,
or claim edge. It only answers:

  Did the live runtime already have structured evidence for gmgn_policy / token_memory / source_resonance /
  scout_quality / watchlist / parser / idempotency / worker health / training manifest / calibration /
  holdout / conflict aging / false-negative budget in the same 06-21 window?

If yes, those modules become covered with row-level evidence.
If no, they remain blocked with a more exact missing reason.
```

### Goal 6 scope
```text
Primary objective:
  Convert the 13 runtime_readmodel_required modules from "exact export required" into one of:
    COVERED_WITH_SAME_WINDOW_RUNTIME_EVIDENCE
    STILL_BLOCKED_WITH_PRECISE_RUNTIME_EXPORT_GAP

Out of scope:
  - smart_money wallet tracking
  - pump.fun bonding-curve decode
  - live gate / strategy / entry / exit / size changes
  - mode promotion
  - edge declaration
  - using raw dog / peak labels as ex-ante signals
  - mixing 06-19 readmodel mirror logs into the 06-21 evidence pack

Reason:
  smart_money and curve_pumpfun are external Zeabur/on-chain data-source workstreams and should be Goal 7,
  not mixed into the runtime readmodel export goal.
```

### Inputs
```text
Required base pack:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/

Required upstream row layer:
  fullnet-row-v2-final-blockers/

Required runtime readmodel sources, same-window only:
  1. data/v27_event_log or exported v27 event log mirror for 06-21
  2. watchlist_store readmodel / watchlist state export
  3. parser / Telegram ingestion session export
  4. sqlite_write_coordinator / idempotency audit export
  5. gmgn_policy readmodel export
  6. token memory / quarantine / reclaim readmodel export
  7. source_resonance shadow readmodel export
  8. scout_quality readmodel export
  9. worker/readiness health export
 10. feature/training manifest export
 11. detector / Markov calibration export
 12. promotion holdout + negative-control export
 13. evidence conflict + aging + assumptions/false-negative-budget export

Strict same-window rule:
  All records must carry window_start_ts/window_end_ts or source file mtime + event_ts range proving they belong
  to the same 06-21 evidence window. Cross-window data is allowed only as "source exists but not joined" metadata.
```

### New artifacts to create
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/runtime-readmodel-window/

Files:
  runtime-readmodel-window.jsonl
    One normalized row per (token_ca, signal_ts, module_group) evidence item.

  runtime-readmodel-summary.json
    Counts by module_group, join_key coverage, same-window validity, missing reasons.

  runtime-readmodel-source-manifest.json
    Every source file/API/export used, sha256, row_count, min_ts, max_ts, schema_version.

  runtime-readmodel-export-health.json
    Which of the 13 modules exported cleanly, partially, empty, stale, or missing.

  runtime-readmodel-unjoined-records.jsonl
    Evidence rows that exist but cannot be joined to fullnet by token_ca/signal_ts/source_id.
```

### New scripts
```text
1. scripts/build-runtime-readmodel-window-export.js

   Purpose:
     Normalize same-window runtime readmodels into runtime-readmodel-window.jsonl.

   Required CLI:
     node scripts/build-runtime-readmodel-window-export.js \
       --window-start-ts <unix_sec> \
       --window-end-ts <unix_sec> \
       --fullnet-row <pack>/fullnet-row-v2-final-blockers/row.jsonl \
       --v27-event-log <path-or-dir> \
       --watchlist-export <optional path> \
       --parser-session-export <optional path> \
       --idempotency-export <optional path> \
       --out-dir <pack>/runtime-readmodel-window

   Behavior:
     - Do not fail if a source is absent; emit module-level missing_reason.
     - Do fail if timestamps are ambiguous and would silently mix windows.
     - Do fail if a source contains future timestamps outside a small clock-skew allowance.
     - Normalize ms/sec timestamps.
     - Generate stable deterministic output order.

2. scripts/build-live-fullnet-row-v2-runtime-readmodels.js

   Purpose:
     Add Goal 6 row fields on top of fullnet-row-v2-final-blockers without mutating Goal 1-5 fields.

   Required CLI:
     node scripts/build-live-fullnet-row-v2-runtime-readmodels.js \
       <all Goal 5 args> \
       --runtime-readmodel-window <pack>/runtime-readmodel-window/runtime-readmodel-window.jsonl \
       --runtime-readmodel-summary <pack>/runtime-readmodel-window/runtime-readmodel-summary.json \
       --out-dir <pack>/fullnet-row-v2-runtime-readmodels

   Behavior:
     - Wrap Goal 5 output.
     - Reclassify only modules with proofed same-window runtime evidence.
     - Keep smart_money and curve_pumpfun blocked as zeabur_export_required.
     - Preserve Goal 1/2/3/4/5 fields byte-equivalent after stripping only Goal 6 new keys.
```

### Runtime readmodel contract
```text
Each runtime-readmodel-window.jsonl row must have:

  schema_version
  module_group
  token_ca
  symbol
  signal_ts
  signal_source_id
  lifecycle_id
  window_start_ts
  window_end_ts
  evidence_ts
  evidence_source
  evidence_source_sha256
  join_key_type
  join_confidence
  same_window_valid
  stale_or_cross_window_reason
  payload_json
  missing_reason

Allowed module_group values:
  gmgn_policy
  token_memory
  source_resonance
  scout_quality
  watchlist
  parser_session
  idempotency_write_path
  worker_health
  training_manifest
  detector_calibration
  holdout_negative_controls
  evidence_conflict_aging
  assumptions_false_negative_budget

Join confidence:
  HIGH   = exact token_ca + signal_ts/source_id/lifecycle_id match
  MEDIUM = exact token_ca + bounded event_ts near signal_ts
  LOW    = token_ca-only or mode-only evidence; may annotate but cannot cover the module
  NONE   = unjoined
```

### Row fields Goal 6 should add
```text
For every module_group above:
  <module>_runtime_seen
  <module>_runtime_join_confidence
  <module>_runtime_source
  <module>_runtime_evidence_ts
  <module>_runtime_payload_hash
  <module>_runtime_missing_reason

Specific semantic fields:
  gmgn_policy_decision
  gmgn_policy_block_reason
  token_memory_failure_type
  token_memory_reclaim_policy
  token_memory_last_failure_ts
  source_resonance_cohort
  source_resonance_lead_time_sec
  scout_quality_score
  scout_quality_block_reason
  watchlist_state
  watchlist_age_sec
  parser_session_id
  parser_lag_sec
  idempotency_key
  idempotency_write_status
  worker_health_status
  readmodel_refresh_age_sec
  training_manifest_id
  feature_snapshot_id
  detector_model_version
  detector_calibration_bucket
  holdout_cohort_id
  negative_control_result
  evidence_conflict_state
  evidence_age_sec
  false_negative_budget_state

Aggregate fields:
  runtime_readmodel_covered_modules[]
  runtime_readmodel_still_blocked_modules[]
  runtime_readmodel_unjoined_modules[]
  runtime_readmodel_confidence
  runtime_readmodel_warnings[]
```

### Module closure update rules
```text
Reclassify a module from blocked -> covered only when:
  1. same_window_valid=true
  2. join_confidence is HIGH or MEDIUM
  3. module-specific payload has the minimum expected fields
  4. row-level evidence exists for at least one relevant signal, or the module proves a deterministic zero
     with same-window manifest evidence

Do not reclassify when:
  - evidence appears only in entry_mode / normalized_mode text
  - evidence is from 06-19 or any other cross-window mirror
  - evidence is token-level but cannot be bounded to the signal window
  - source file exists but has no event_ts range
  - only docs/config mention the module but runtime emitted no readmodel row

Closure after Goal 6 can have several valid outcomes:
  Best case:     60 = 50 covered / 2 blocked / 8 excluded
                 (all 13 runtime modules covered; only smart_money + curve_pumpfun remain)
  Partial case:  60 = 37..50 covered / remaining blocked exact / 8 excluded
  Fail case:     no row drift, no false coverage; all missing modules stay blocked with sharper reasons
```

### Reports to output
```text
New directory:
  fullnet-row-v2-runtime-readmodels/

Files:
  row.jsonl
  summary.json
  repair-owner-report.json
  row-confidence-report.json
  final-module-closure-coverage-report.json
  runtime-readmodel-coverage-report.json
  runtime-readmodel-join-quality-report.json
  runtime-readmodel-unjoined-report.json
  runtime-readmodel-field-availability-report.json

Report requirements:
  - counts by module_group: covered / partial / missing / stale / unjoined
  - dog/dud/pending coverage by module_group
  - exact same-window source manifest
  - join confidence distribution
  - list of rows where normalized_mode contains gmgn/scout/resonance but no structured runtime evidence exists
  - repair-owner distribution unchanged unless a predicate is explicitly changed and documented
```

### Verification and tests
```text
Add tests:
  tests/build-runtime-readmodel-window-export.test.mjs
  tests/build-live-fullnet-row-v2-runtime-readmodels.test.mjs

Required assertions:
  - ms/sec timestamp normalization works.
  - cross-window 06-19 data is detected and not joined.
  - token-only LOW confidence evidence cannot mark a module covered.
  - HIGH/MEDIUM same-window evidence can mark a module covered.
  - smart_money and curve_pumpfun remain zeabur_export_required.
  - Goal 1/2/3/4/5 fields preserve 0 drift after stripping only Goal 6 new fields.
  - actual EV remains null unless valid entered+fill+ledger+exit+friction exists.
  - outputs are byte-reproducible except generated_at.

Commands:
  NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules node --check \
    scripts/build-runtime-readmodel-window-export.js \
    scripts/build-live-fullnet-row-v2-runtime-readmodels.js

  NODE_PATH=/Users/boliu/sentiment-arbitrage-system/node_modules node --test \
    tests/build-runtime-readmodel-window-export.test.mjs \
    tests/build-live-fullnet-row-v2-runtime-readmodels.test.mjs \
    tests/build-live-fullnet-row-v2-final-blockers.test.mjs
```

### Acceptance criteria
```text
Goal 6 is complete only if all are true:

1. runtime-readmodel-window/ exists with manifest, health, summary, normalized JSONL.
2. fullnet-row-v2-runtime-readmodels/ exists with row/report artifacts.
3. 329 rows remain projection_complete=true.
4. EV remains fail-closed unless real entered+fill+ledger+exit+friction rows exist.
5. Goal 1/2/3/4/5 fields have 0 drift after stripping Goal 6 new keys.
6. Every runtime_readmodel_required module is either:
     covered with same-window HIGH/MEDIUM evidence, or
     still blocked with an even more exact missing/export reason.
7. smart_money and curve_pumpfun are not silently reclassified.
8. No generic blocked reason remains.
9. No live/gate/entry/exit/size/mode promotion code is changed.
10. Tests and independent invariants pass.
```

### Expected decision after Goal 6
```text
If most/all 13 runtime modules become covered:
  Next Goal 7 should be external Zeabur/on-chain exports for smart_money + curve_pumpfun.

If many runtime modules remain blocked:
  Do not move to strategy. First fix the runtime exporter/readmodel generation contract.

If runtime evidence shows gmgn/source_resonance/scout_quality were only labels and not structured decisions:
  Treat them as non-contributing labels in separability until the runtime emits real per-signal evidence.

If runtime evidence covers token_memory / idempotency / worker health:
  Use it to improve repair-owner accuracy, not to change trading behavior.

In all cases:
  No edge or promotion can be claimed until actual entered/fill/ledger/exit/friction rows exist.
```

```text
GOAL_6_READY_TO_EXECUTE (§15.21 same-window runtime readmodel export + join layer)
TARGET: unblock 13 runtime_readmodel_required modules with same-window structured evidence
NON_TARGET: smart_money + curve_pumpfun remain Goal 7 external export work
GUARDRAILS: research-only, additive layer, EV fail-closed, no strategy/live/mode changes
SUCCESS: runtime modules covered-or-precisely-blocked, Goal1-5 0 drift, byte-repro + tests pass
```

---

## §15.22 STATUS — Goal 6: same-window runtime readmodel export + join layer (2026-06-22)

Executed §15.21. Two new scripts; research-only; no live read, no strategy/gate/entry/exit/size/mode
change; EV fail-closed; Goal 1/2/3/4/5 fields preserved verbatim; all 5 prior layers still byte-reproduce.

### What was built
```text
scripts/build-runtime-readmodel-window-export.js   (script 1: normalize same-window runtime evidence)
scripts/build-live-fullnet-row-v2-runtime-readmodels.js (script 2: join evidence back into fullnet rows)
Artifacts:
  runtime-readmodel-window/  -> runtime-readmodel-window.jsonl, runtime-readmodel-summary.json,
     runtime-readmodel-source-manifest.json, runtime-readmodel-export-health.json,
     runtime-readmodel-unjoined-records.jsonl
  fullnet-row-v2-runtime-readmodels/ -> row.jsonl, summary.json, repair-owner-report.json,
     row-confidence-report.json, final-module-closure-coverage-report.json, runtime-readmodel-join-report.json
```

### The honest finding (which of the 13 runtime modules had same-window structured evidence)
```text
COVERED (1) — same-window HIGH join:
  parser_session : raw_signal_observations (6206 rows) joined exact (token_ca,signal_ts) to 329/329 cohort
                   signals. The ingestion/observation session per signal (signal_id, status, provider,
                   first_bar_lag). NOTE: this is the raw-observation ingestion session, not a Telegram
                   thread id; covered at HIGH join with that caveat documented in the row payload.
STILL BLOCKED (12) — now with a MORE PRECISE same-window export gap (no generic reason remains):
  worker_health  : raw_path_observer_provider_state present (provider=helius, 429 cooldown) BUT
                   updated_at is POST-window -> cross-window, window_global LOW join only.
  token_memory / watchlist : sentiment_arb.tokens present (217 rows) BUT 0 join to the 06-21 cohort
                   tokens and decision_timestamp NULL -> not same-window.
  detector_calibration : lifecycle strategy_results present (shadow strategy outcomes) but no model
                   version / calibration bucket.
  gmgn_policy / source_resonance / scout_quality : appear ONLY as kline-provider or normalized_mode
                   label tokens; no structured per-signal readmodel.
  idempotency_write_path / training_manifest / holdout_negative_controls / evidence_conflict_aging /
  assumptions_false_negative_budget : no same-window readmodel table in the snapshot pack.
NOT TOUCHED (Goal 7 scope): smart_money + curve_pumpfun remain zeabur_export_required.
=> module closure: covered 37 -> 38, blocked 15 -> 14, intentionally_excluded 8 (total 60, reconciles).
```

### Same-window discipline (no silent window mixing)
```text
- Each evidence row carries window_start_ts/window_end_ts + same_window_valid + stale_or_cross_window_reason.
- worker_health's post-window state row is routed to runtime-readmodel-unjoined-records.jsonl, NOT joined.
- The 06-19 v27 mirror logs are intentionally NOT read.
- 514 joined evidence rows (parser_session); 6127 unjoined (out-of-cohort obs + cross-window state + 0-join tokens).
```

### Goal-1-5 preservation method (0 drift)
```text
Goal 5 already used several plain field names (runtime_readmodel_required, token_memory_failure_type,
scout_quality_score, watchlist_state, ...) and Goal 3/4 added <module>_seen/_missing_reason. To guarantee
0 drift, EVERY Goal-6 row field is namespaced `<module>_runtime_*` or `runtime_readmodel_<suffix>` (87 new
keys), so no existing field is overwritten. Verified: 0/329 rows drift after stripping the 87 new keys.
```

### Verification
```text
node --check x12 OK. node --test (12 relevant suites, NODE_PATH): 12 pass / 0 fail.
Independent JSON invariants 12/12: row_count 329, projection_complete=true, EV null+BLOCKED, module count
  60 reconciles (38/14/8), no_generic_blocked_reason_remains=true, smart_money/curve still zeabur,
  parser_session_runtime_seen on all 329 rows, Goal 1-5 fields present.
Byte-reproducible: both Goal-6 scripts' artifacts identical on rerun; v2 + v2-marketdata + v2-mode-readiness
  + v2-ops-actionability + v2-final-blockers STILL byte-identical (no regression).
New tests (both Goal-6 scripts now have a dedicated suite — closes the §15.21 two-test-file plan):
  tests/build-runtime-readmodel-window-export.test.mjs (script 1: windowValidity skew/null, in-memory
    extractParserSession exact-join + cross-window->unjoined, extractWorkerHealth post-window->stale,
    extractTokenLevel 0-cohort-join->empty, NO_SOURCE_REASON precision).
  tests/build-live-fullnet-row-v2-runtime-readmodels.test.mjs (script 2: moduleRuntimeFields HIGH/MEDIUM/LOW,
    namespaced fields, proof-gated closure, smart/curve untouched).
```

### Reviewer caveats (independent review 2026-06-22, accepted + resolved)
```text
PASS with caveats. Both caveats addressed:
1. Reviewer's shell could not run node (exec 137/SIGKILL on even `pwd`) so they verified via Node REPL
   artifact/invariant checks (all passed). Re-confirmed here in a working shell: node --check x12 + node
   --test 12 suites = 12 pass / 0 fail. This is an environment issue on the reviewer side, not the code.
2. §15.21 implied two Goal-6 test files; only the join-layer suite existed (it did import script-1's
   windowValidity/ALL_MODULES/NO_SOURCE_REASON, so script 1 was not uncovered). RESOLVED: added the
   dedicated tests/build-runtime-readmodel-window-export.test.mjs above. Plan and implementation now match.
```

### Decision (per §15.21 "expected decision after Goal 6")
```text
Outcome = "many runtime modules remain blocked" branch: only 1 of 13 (parser_session) had same-window
structured evidence in the current pack. => Do NOT move to strategy. The next data-pipeline step is to
fix the runtime exporter/readmodel generation contract: emit ONE same-window export job that materializes
the 12 missing per-(token,signal) runtime readmodels (gmgn_policy, token_memory, source_resonance,
scout_quality, watchlist, idempotency, worker_health [in-window], training_manifest, detector_calibration,
holdout/negative_controls, evidence_conflict_aging, assumptions/FN-budget). Goal 7 (smart_money +
curve_pumpfun external Zeabur/on-chain exports) stays after that. No edge/promotion until real
entered+fill+ledger+exit+friction rows exist.
```

```text
GOAL_6_COMPLETE (§15.22 same-window runtime readmodel export + join)
RUNTIME_COVERED 37 -> 38 (parser_session, HIGH same-window join 329/329)
RUNTIME_STILL_BLOCKED 12 (each with a precise same-window export gap; no generic reason)
SMART_MONEY_AND_CURVE_PUMPFUN_NOT_RECLASSIFIED (remain zeabur_export_required -> Goal 7)
GOAL_1_5_FIELDS_PRESERVED_VERBATIM (87 namespaced keys, 0 drift) | 5 PRIOR LAYERS BYTE_REPRO_INTACT
EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329 | NO_SILENT_WINDOW_MIXING
TESTS_PASS (check x12 + 11 suites + invariants 12/12 + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
```

---

## §15.23 NEXT GOAL — Goal 7: runtime readmodel materialization contract for the 12 missing modules (2026-06-22)

### Why this supersedes the earlier "external Goal 7" idea
```text
Before Goal 6, the expected next step was:
  Goal 7 = external Zeabur/on-chain exports for smart_money + curve_pumpfun.

But Goal 6 changed the priority:
  13 runtime_readmodel_required modules were inspected.
  Only 1/13 (parser_session) had joinable same-window structured evidence.
  12/13 still lack a real per-(token,signal) runtime readmodel.

Therefore the next goal must NOT be external smart_money / curve_pumpfun yet.
The next goal is now:

  Build the runtime readmodel materialization contract that causes the runtime / export pipeline to emit
  the 12 missing per-(token,signal) readmodels in the same window.

At planning time, external smart_money + curve_pumpfun was expected to become Goal 8. After the
§15.24 result showed only 3/12 runtime materializers covered, §15.25 supersedes this ordering:
Goal 8 repairs the 9 still-blocked runtime materializers, and external smart_money + curve_pumpfun
move to Goal 9.
This ordering prevents us from building external enrichments while the system's own runtime decisions remain
mostly label-only / non-structured / non-joinable.
```

### Goal 7 objective
```text
Primary objective:
  Define and implement a SAME-WINDOW runtime readmodel materializer/exporter for the 12 modules that Goal 6
  proved are still missing as structured per-signal evidence:

    gmgn_policy
    token_memory
    source_resonance
    scout_quality
    watchlist
    idempotency_write_path
    worker_health
    training_manifest
    detector_calibration
    holdout_negative_controls
    evidence_conflict_aging
    assumptions_false_negative_budget

The output must be joinable by fullnet row v2:
  token_ca + signal_ts + source_id / lifecycle_id + evidence_ts + module_group.

This is still data-pipeline / observability work.
It is NOT strategy tuning.
```

### Hard guardrails
```text
Goal 7 must not:
  - change live entry / exit / size / gates / thresholds
  - open any entry mode
  - promote any mode
  - use raw dog / peak labels as ex-ante features
  - infer runtime decisions from normalized_mode labels alone
  - join cross-window logs
  - silently mark module covered from token-only LOW evidence
  - degrade Goal 1-6 row fields
  - claim edge or actual EV

Goal 7 may:
  - add read-only export scripts
  - add research-only materializer scripts
  - add same-window readmodel tables/files
  - add tests
  - add new fullnet row fields with a dedicated Goal-7 namespace
  - add API/export endpoints only if they are read-only and bounded
```

### Required design principle
```text
Every missing module must become one of three explicit states:

  MATERIALIZED_AND_JOINED
    The runtime/exporter emitted same-window structured evidence and fullnet joined it with HIGH/MEDIUM confidence.

  MATERIALIZER_EMPTY_BUT_VALID
    The runtime/exporter emitted a valid same-window zero-row proof. This can cover deterministic-zero modules
    only if the manifest proves the materializer ran for the window.

  MATERIALIZER_MISSING_OR_INVALID
    The runtime/exporter did not emit the contract, emitted cross-window evidence, or emitted token-only LOW evidence.
    The module remains blocked with an exact reason.

No module may move from blocked -> covered just because a label string contains its name.
```

### The 12 module contracts
```text
1. gmgn_policy
   Required row:
     token_ca, signal_ts, source_id/lifecycle_id, gmgn_provider, gmgn_policy_decision,
     gmgn_policy_block_reason, gmgn_policy_score_or_flags, evidence_ts, same_window_valid.
   Purpose:
     Distinguish "GMGN was a kline provider" from "GMGN policy contributed to entry/readiness".

2. token_memory
   Required row:
     token_ca, signal_ts, memory_state, failure_type, last_failure_ts, reclaim_policy,
     quarantine_state, waterfall_memory, no_follow_memory, spread_chase_memory, evidence_ts.
   Purpose:
     Show whether token history raised/relaxed the entry burden.

3. source_resonance
   Required row:
     token_ca, signal_ts, source_resonance_cohort, gmgn_first_seen_ts, telegram_signal_ts,
     lead_time_sec, resonance_score, resonance_decision, evidence_ts.
   Purpose:
     Stop treating "resonance" in entry_mode as proof; export real cohort/lead-time evidence.

4. scout_quality
   Required row:
     token_ca, signal_ts, scout_quality_score, scout_quality_block_reason,
     liquidity_score, activity_score, trend_score, quote_executable, evidence_ts.
   Purpose:
     Explain whether scout quality protected from noise or overblocked dogs.

5. watchlist
   Required row:
     token_ca, signal_ts, watchlist_state, registered_ts, armed_ts, expired_ts,
     watch_reason, current_watch_stage, evidence_ts.
   Purpose:
     Prove whether a signal became watchlist/armed/wait/expired before final entry.

6. idempotency_write_path
   Required row:
     token_ca, signal_ts, decision_event_id, idempotency_key, write_path_status,
     dedupe_result, enqueue_attempted, order_attempted, fill_attempted, evidence_ts.
   Purpose:
     Explain would_enter -> no enqueue/order/fill/ledger without guessing.

7. worker_health
   Required row:
     worker_name, window_start_ts, window_end_ts, health_status, last_heartbeat_ts,
     readmodel_refresh_ts, last_error, cooldown_until, provider, same_window_valid.
   Purpose:
     Separate strategy decisions from worker/API/provider availability.

8. training_manifest
   Required row:
     token_ca, signal_ts, feature_snapshot_id, feature_schema_version, training_manifest_id,
     feature_vector_hash, missing_feature_list, evidence_ts.
   Purpose:
     Prove which feature schema/model assumptions were available at decision time.

9. detector_calibration
   Required row:
     token_ca, signal_ts, detector_name, model_version, calibration_bucket,
     expected_precision_bucket, confidence_bucket, evidence_ts.
   Purpose:
     Separate Markov/detector signal quality from uncalibrated labels.

10. holdout_negative_controls
    Required row:
      mode_name, module_group, window_start_ts, window_end_ts, holdout_cohort_id,
      negative_control_result, sample_n, dog_n, dud_n, evidence_ts.
    Purpose:
      Ensure promotion/readiness is not based only on in-sample or one-day apparent lift.

11. evidence_conflict_aging
    Required row:
      token_ca, signal_ts, evidence_type, evidence_ts, evidence_age_sec,
      conflict_state, conflict_reason, stale_reason, evidence_ts.
    Purpose:
      Detect stale/conflicting signals instead of letting old evidence masquerade as current readiness.

12. assumptions_false_negative_budget
    Required row:
      module_group, window_start_ts, window_end_ts, assumption_id, assumption_status,
      false_negative_budget_used, false_negative_budget_limit, evidence_ts.
    Purpose:
      Track where the system intentionally accepts missed dogs versus accidental blind spots.
```

### New artifacts to create
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/runtime-readmodel-materialized/

Files:
  runtime-readmodel-materialized.jsonl
    One normalized materialized row per evidence item. This should include parser_session too for continuity,
    but the Goal-7 success target is the 12 missing modules.

  runtime-readmodel-materializer-health.json
    Per module: ran / not_ran / empty_valid / empty_invalid / cross_window / joinable_count / missing_reason.

  runtime-readmodel-materializer-manifest.json
    Source files/tables/endpoints, sha256, schema_version, row_count, min_ts, max_ts, window_start_ts, window_end_ts.

  runtime-readmodel-materializer-unjoined.jsonl
    Materialized rows that cannot join to fullnet rows, with exact join failure reason.

  runtime-readmodel-materializer-contract-report.json
    Field availability by module vs the contract above.
```

### New fullnet layer to create
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-runtime-materialized/

Files:
  row.jsonl
  summary.json
  repair-owner-report.json
  row-confidence-report.json
  final-module-closure-coverage-report.json
  runtime-materializer-coverage-report.json
  runtime-materializer-join-quality-report.json
  runtime-materializer-contract-report.json
```

### New scripts
```text
1. scripts/build-runtime-readmodel-materialized-export.js

   Purpose:
     Produce runtime-readmodel-materialized/ from the available same-window runtime sources.

   Required CLI:
     node scripts/build-runtime-readmodel-materialized-export.js \
       --window-start-ts <unix_sec> \
       --window-end-ts <unix_sec> \
       --fullnet-row <pack>/fullnet-row-v2-runtime-readmodels/row.jsonl \
       --runtime-sources-dir <same-window export dir or frozen snapshot dir> \
       --out-dir <pack>/runtime-readmodel-materialized

   Behavior:
     - Accept missing sources, but emit module-level materializer status.
     - Normalize ms/sec timestamps.
     - Reject cross-window evidence as joined evidence.
     - Emit deterministic output order.
     - Include source manifest and field contract report.

2. scripts/build-live-fullnet-row-v2-runtime-materialized.js

   Purpose:
     Join runtime-readmodel-materialized back into fullnet row v2, layered on Goal 6.

   Required CLI:
     node scripts/build-live-fullnet-row-v2-runtime-materialized.js \
       <all Goal 6 args> \
       --runtime-materialized-dir <pack>/runtime-readmodel-materialized \
       --out-dir <pack>/fullnet-row-v2-runtime-materialized

   Behavior:
     - Wrap Goal 6 output.
     - Reclassify modules only with HIGH/MEDIUM same-window evidence or valid deterministic-zero manifest.
     - Keep smart_money and curve_pumpfun blocked as zeabur_export_required.
     - Preserve Goal 1-6 fields 0 drift after stripping only Goal-7 new keys.
```

### New row fields
```text
Use a Goal-7 namespace to avoid overwriting Goal 6:

  <module>_materialized_seen
  <module>_materialized_join_confidence
  <module>_materialized_status
  <module>_materialized_source
  <module>_materialized_evidence_ts
  <module>_materialized_payload_hash
  <module>_materialized_missing_reason

Aggregate:
  runtime_materialized_covered_modules[]
  runtime_materialized_still_blocked_modules[]
  runtime_materialized_empty_valid_modules[]
  runtime_materialized_unjoined_modules[]
  runtime_materialized_confidence
  runtime_materialized_warnings[]
```

### Closure rules
```text
Blocked -> covered:
  allowed only with HIGH/MEDIUM same-window materialized evidence.

Blocked -> covered deterministic-zero:
  allowed only when the materializer ran for the module, schema is valid, source manifest is same-window,
  and zero rows is meaningful for that module.

Blocked -> blocked sharper:
  required when the module has no materializer, invalid materializer, cross-window evidence, or token-only LOW join.

No-op:
  smart_money and curve_pumpfun remain zeabur_export_required. They are not part of Goal 7.
```

### Expected outcomes
```text
Best case:
  parser_session + 12 runtime modules covered; closure becomes 50 covered / 2 blocked / 8 excluded.

Partial case:
  Some of the 12 covered; remaining modules still blocked with materializer-specific reasons.

Fail case:
  12 modules remain blocked, but now the exact reason is "materializer missing/invalid", not "unknown".

Any outcome is acceptable if it is honest, same-window, and no-drift.
```

### Tests
```text
Add:
  tests/build-runtime-readmodel-materialized-export.test.mjs
  tests/build-live-fullnet-row-v2-runtime-materialized.test.mjs

Required assertions:
  - ms/sec timestamp normalization.
  - cross-window evidence goes to unjoined, not covered.
  - normalized_mode label alone cannot cover source_resonance/scout_quality/gmgn_policy.
  - LOW token-only evidence cannot cover token_memory/watchlist.
  - deterministic-zero coverage requires manifest proof.
  - smart_money and curve_pumpfun remain zeabur_export_required.
  - Goal 1-6 fields preserve 0 drift.
  - actual EV remains null unless valid entered+fill+ledger+exit+friction exists.
  - output byte-reproducible except generated_at.
```

### Acceptance criteria
```text
Goal 7 is complete only if all are true:

1. runtime-readmodel-materialized/ exists with JSONL, health, manifest, contract report, unjoined file.
2. fullnet-row-v2-runtime-materialized/ exists with row/report artifacts.
3. 329 rows remain projection_complete=true.
4. EV remains fail-closed unless real entered+fill+ledger+exit+friction rows exist.
5. Goal 1-6 fields have 0 drift after stripping Goal-7 new keys.
6. Each of the 12 modules is either covered with proof or blocked with materializer-specific exact reason.
7. smart_money and curve_pumpfun remain Goal 8 external exports.
8. No generic blocked reason remains.
9. No live/gate/entry/exit/size/mode promotion code changes.
10. Tests + independent invariants + byte-repro pass.
```

### Decision after Goal 7
```text
If most/all 12 runtime modules are materialized and covered:
  Proceed to Goal 8: external smart_money + pump.fun curve exports.

If most remain blocked because materializers do not exist:
  Stop fullnet expansion and implement the runtime materializers in the actual runtime/export pipeline first.

If materialized evidence exists but does not join:
  Fix identity keys (token_ca, signal_ts, source_id, lifecycle_id) before doing any strategy work.

If materialized evidence joins but shows gmgn/scout/source_resonance were label-only:
  Treat those modules as non-contributing until runtime emits real decisions.

In all cases:
  No edge/promotion without real entered+fill+ledger+exit+friction.
```

```text
GOAL_7_READY_TO_EXECUTE (§15.23 runtime readmodel materialization contract)
TARGET: materialize the 12 runtime modules Goal 6 proved missing
NON_TARGET: smart_money + curve_pumpfun remain Goal 8 external exports
GUARDRAILS: research-only, same-window only, no strategy/live/mode changes, EV fail-closed
SUCCESS: 12 modules covered-or-materializer-blocked, Goal1-6 0 drift, no generic reason, tests pass
```

---

## §15.24 STATUS — Goal 7: runtime readmodel materialization contract (2026-06-22)

Executed §15.23. Two new scripts (materializer export + join layer); research-only; no live read; no
strategy/gate/entry/exit/size/mode change; EV fail-closed; Goal 1-6 fields preserved verbatim; all 6
prior layers still byte-reproduce. Materialized ONLY from structured same-window data — never from a
label string.

### What was built
```text
scripts/build-runtime-readmodel-materialized-export.js  (materialize the 12 from structured sources)
scripts/build-live-fullnet-row-v2-runtime-materialized.js (join into fullnet, _materialized_ namespace)
runtime-readmodel-materialized/  -> materialized.jsonl + materializer-health/manifest/unjoined/contract-report
fullnet-row-v2-runtime-materialized/ -> row.jsonl, summary.json, repair-owner/row-confidence/closure +
  runtime-materializer-coverage-report.json, -join-quality-report.json, -contract-report.json
```

### Outcome — partial case (3 of 12 materialized; the rest sharpened)
```text
MATERIALIZED_AND_JOINED (3 -> covered):
  token_memory            : derived in-window same-token signal history (prior_count + last_prior_ts),
                            structured count per signal (329/329 joined; runtime-only fields
                            failure_type/reclaim_policy/quarantine flagged absent in contract-report).
  evidence_conflict_aging : derived evidence_age_sec (window_end - signal_ts) + REAL conflict from the
                            quote_clean top-vs-risk_json mismatch flag (329/329; 5 rows in conflict).
  watchlist               : lifecycle_tracks.tracks state machine (active/completed/dead + complete_reason)
                            joined exact (token,signal_ts) -> only 13/329 have a same-window track row
                            (transparently reported in join-quality; HIGH where present).
MATERIALIZER_MISSING_OR_INVALID (9 -> still blocked, materializer-specific exact reason; NOT generic):
  gmgn_policy / source_resonance / scout_quality / idempotency_write_path / worker_health (post-window) /
  training_manifest / detector_calibration / holdout_negative_controls / assumptions_false_negative_budget.
  Each reason names exactly what structured field is absent and what runtime emit is required.
NOT TOUCHED: smart_money + curve_pumpfun remain zeabur_export_required (Goal 8).
=> module closure: covered 38 -> 41, blocked 14 -> 11, intentionally_excluded 8 (total 60, reconciles).
```

### No-label-as-evidence discipline (key guardrail)
```text
"resonance"/"scout"/"gmgn" tokens inside normalized_mode were NOT used to mark those modules covered.
source_resonance stays blocked precisely because the contract needs gmgn_first_seen_ts/lead_time_sec,
which are not structurally present (the label is not evidence). The 3 materialized modules each come
from genuine structured data: signal-history counts, timestamp deltas + a real mismatch flag, and the
lifecycle track state machine.
```

### Goal-1-6 preservation method (0 drift)
```text
Every Goal-7 row field is namespaced `<module>_materialized_*` or `runtime_materialized_*` (96 new keys
across all rows; 94 keys appear on the first row, and watchlist-only rows add 2 more keys),
distinct from Goal 6's `<module>_runtime_*`/`runtime_readmodel_*` and Goal 3/4/5's plain names. Verified:
0/329 rows drift after stripping the 96 new keys; repair-owner distribution byte-identical.
```

### Verification
```text
node --check x14 OK. node --test (14 relevant suites, NODE_PATH): 14 pass / 0 fail (incl. BOTH Goal-7
  test files: build-runtime-readmodel-materialized-export.test.mjs + build-live-fullnet-row-v2-runtime-materialized.test.mjs).
Independent invariants: row_count 329, projection_complete=true, EV null+BLOCKED, module count 60
  reconciles (41/11/8), no_generic_blocked_reason_remains=true, smart_money/curve still zeabur,
  Goal 1-6 fields present, repair-owner identical.
Byte-reproducible: both Goal-7 scripts' artifacts identical on rerun; all 6 prior layers (v2 -> runtime-readmodels)
  STILL byte-identical (no regression).
```

### Final cumulative closure + next step
```text
60 modules = 41 covered / 11 blocked / 8 intentionally_excluded.
covered progression: Goal1 22 -> 25 -> 29 -> 35 -> 37 -> 38 (Goal6) -> 41 (Goal7).
The 9 materializer-blocked modules now each have an EXACT runtime emit requirement -> this is the precise
spec for the runtime team's one same-window readmodel export job. Because only 3/12 runtime modules were
materialized, §15.25 supersedes the earlier external-export ordering: Goal 8 is the 9-module runtime
materializer repair; smart_money + curve_pumpfun move to Goal 9. EV stays null until real
entered+fill+ledger+exit+friction.
This remains evidence materialization / observability — not strategy tuning, not edge proof.
```

```text
GOAL_7_COMPLETE (§15.24 runtime readmodel materialization contract)
MATERIALIZED 3 (token_memory, evidence_conflict_aging, watchlist) -> covered 38 -> 41
STILL_BLOCKED 11 (9 materializer-specific exact reasons + 2 zeabur); no generic reason
SMART_MONEY_AND_CURVE_PUMPFUN_NOT_RECLASSIFIED (zeabur -> Goal 9)
NO_LABEL_STRING_AS_STRUCTURED_EVIDENCE | GOAL_1_6_FIELDS_PRESERVED_VERBATIM (96 namespaced keys, 0 drift)
6 PRIOR LAYERS BYTE_REPRO_INTACT | EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329
TESTS_PASS (check x14 + 14 suites + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
```

## §15.25 NEXT GOAL — Goal 8: repair the 9 still-blocked runtime materializers (2026-06-22)

### Why Goal 8 is NOT external smart_money / curve_pumpfun yet
```text
Goal 7 produced a partial runtime materialization:

  Covered runtime modules:
    token_memory
    watchlist
    evidence_conflict_aging

  Still-blocked runtime modules:
    gmgn_policy
    source_resonance
    scout_quality
    idempotency_write_path
    worker_health
    training_manifest
    detector_calibration
    holdout_negative_controls
    assumptions_false_negative_budget

Because only 3/12 runtime modules were materialized, the §15.23 decision rule says:

  If most remain blocked because materializers do not exist:
    Stop fullnet expansion and implement the runtime materializers in the actual runtime/export pipeline first.

Therefore Goal 8 is a runtime materializer repair goal.
External smart_money + curve_pumpfun must be deferred to Goal 9.
```

### Goal 8 objective
```text
Build or repair materializers for the 9 remaining runtime modules so each module becomes one of:

  MATERIALIZED_AND_JOINED
  MATERIALIZER_EMPTY_BUT_VALID
  MATERIALIZER_MISSING_OR_INVALID_WITH_FINAL_REASON

This remains research-only / evidence-only.
No live strategy, gate, entry, exit, size, threshold, or mode-promotion changes are allowed.
```

### The 9 modules and what must be fixed
```text
1. gmgn_policy
   Current Goal-7 reason:
     GMGN appears only as kline provider + normalized_mode label; no structured gmgn_policy decision field.
   Goal-8 fix:
     Emit a per-(token_ca, signal_ts) gmgn_policy row with:
       gmgn_policy_decision
       gmgn_provider
       gmgn_fields_used
       gmgn_policy_score_or_flags
       gmgn_policy_block_reason
       evidence_ts
   Success:
     At least same-window joined evidence, or valid zero-row proof that gmgn_policy did not run.

2. source_resonance
   Current Goal-7 reason:
     source_row_n multiplicity is structured, but contract lacks gmgn_first_seen_ts + lead_time_sec.
   Goal-8 fix:
     Emit:
       gmgn_first_seen_ts
       telegram_signal_ts
       lead_time_sec
       source_resonance_cohort
       resonance_score
       resonance_decision
   Success:
     Distinguish real source-resonance contribution from the string `a_grade_resonance_fastlane`.

3. scout_quality
   Current Goal-7 reason:
     matrix/liquidity are present, but scout_quality score/block reason absent.
   Goal-8 fix:
     Emit:
       scout_quality_score
       scout_quality_block_reason
       liquidity_score
       activity_score
       volume_score
       trend_score
       quote_executable
   Success:
     Can audit whether scout quality protected from duds or overblocked dogs.

4. idempotency_write_path
   Current Goal-7 reason:
     decision_event_id + enqueue/order/fill false are derivable, but idempotency_key/write_path_status absent.
   Goal-8 fix:
     Emit:
       decision_event_id
       idempotency_key
       write_path_status
       dedupe_result
       enqueue_attempted
       order_attempted
       fill_attempted
       ledger_write_attempted
       write_error
   Success:
     would_enter -> no enqueue/order/fill/ledger becomes directly explainable.

5. worker_health
   Current Goal-7 reason:
     provider state exists but updated_at is post-window.
   Goal-8 fix:
     Emit same-window worker heartbeat/readmodel-refresh rows:
       worker_name
       provider
       health_status
       last_heartbeat_ts
       readmodel_refresh_ts
       cooldown_until
       last_error
   Success:
     Distinguish strategy block from worker/API/provider outage.

6. training_manifest
   Current Goal-7 reason:
     no feature snapshot / training manifest in same-window sources.
   Goal-8 fix:
     Emit:
       feature_snapshot_id
       feature_schema_version
       training_manifest_id
       feature_vector_hash
       missing_feature_list
   Success:
     Can prove which feature schema was available at decision time.

7. detector_calibration
   Current Goal-7 reason:
     lifecycle strategy_results are shadow outcomes, not detector model version/calibration.
   Goal-8 fix:
     Emit:
       detector_name
       model_version
       calibration_bucket
       expected_precision_bucket
       confidence_bucket
   Success:
     Markov/detector regime can be audited as calibrated evidence, not just a label.

8. holdout_negative_controls
   Current Goal-7 reason:
     in-sample dog/dud computable, but not true holdout / negative control.
   Goal-8 fix:
     Emit:
       mode_name
       holdout_cohort_id
       negative_control_result
       sample_n
       dog_n
       dud_n
       bootstrap_lower_bound
   Success:
     Mode readiness/promotion can be checked against out-of-sample controls.

9. assumptions_false_negative_budget
   Current Goal-7 reason:
     no FN-budget policy/assumption ledger exists.
   Goal-8 fix:
     Emit:
       assumption_id
       assumption_status
       false_negative_budget_used
       false_negative_budget_limit
       accepted_miss_reason
   Success:
     The system can separate intentional missed-dog budget from accidental blind spots.
```

### New artifacts
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/runtime-readmodel-materializer-repair/

Files:
  runtime-materializer-repair.jsonl
    One repair/materialized row per module evidence item.

  runtime-materializer-repair-health.json
    For each of the 9 modules:
      repaired_and_joined / repaired_empty_valid / still_missing / invalid_schema / cross_window / low_confidence_only.

  runtime-materializer-repair-manifest.json
    Source files, tables, hashes, min_ts, max_ts, window_start_ts, window_end_ts.

  runtime-materializer-repair-contract-report.json
    Required fields vs present fields per module.

  runtime-materializer-repair-unjoined.jsonl
    Evidence that materialized but did not join, with exact join failure reason.
```

### New fullnet layer
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-runtime-materializer-repair/

Files:
  row.jsonl
  summary.json
  repair-owner-report.json
  row-confidence-report.json
  final-module-closure-coverage-report.json
  runtime-materializer-repair-coverage-report.json
  runtime-materializer-repair-join-quality-report.json
  runtime-materializer-repair-contract-report.json
```

### New scripts
```text
1. scripts/build-runtime-materializer-repair-export.js

   Purpose:
     Build repaired materializer exports for the 9 still-blocked runtime modules.

   CLI:
     node scripts/build-runtime-materializer-repair-export.js \
       --window-start-ts <unix_sec> \
       --window-end-ts <unix_sec> \
       --fullnet-row <pack>/fullnet-row-v2-runtime-materialized/row.jsonl \
       --runtime-materialized-dir <pack>/runtime-readmodel-materialized \
       --runtime-sources-dir <same-window runtime export dir or frozen snapshot dir> \
       --out-dir <pack>/runtime-readmodel-materializer-repair

2. scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js

   Purpose:
     Join the repaired materializer evidence back into fullnet rows, layered on Goal 7.

   CLI:
     node scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js \
       <all Goal 7 args> \
       --runtime-materializer-repair-dir <pack>/runtime-readmodel-materializer-repair \
       --out-dir <pack>/fullnet-row-v2-runtime-materializer-repair
```

### New row field namespace
```text
Use a Goal-8 namespace only:

  <module>_repair_seen
  <module>_repair_status
  <module>_repair_join_confidence
  <module>_repair_source
  <module>_repair_evidence_ts
  <module>_repair_payload_hash
  <module>_repair_missing_reason

Aggregate:
  runtime_repair_covered_modules[]
  runtime_repair_still_blocked_modules[]
  runtime_repair_empty_valid_modules[]
  runtime_repair_unjoined_modules[]
  runtime_repair_confidence
  runtime_repair_warnings[]
```

### Closure rules
```text
Module can move blocked -> covered only if:
  - same_window_valid=true
  - join_confidence HIGH or MEDIUM
  - required module fields present
  - materializer output is not inferred from normalized_mode labels alone

Module can move blocked -> covered empty-valid only if:
  - materializer ran for the window
  - manifest proves same-window run
  - zero rows is meaningful for the module

Module remains blocked when:
  - materializer absent
  - schema invalid
  - cross-window evidence
  - token-only LOW evidence
  - label-only evidence
```

### Expected closure after Goal 8
```text
Starting point:
  60 = 41 covered / 11 blocked / 8 excluded

Best case:
  60 = 50 covered / 2 blocked / 8 excluded
  Remaining blocked = smart_money + curve_pumpfun only.

Partial case:
  60 = 42..49 covered / remaining blocked exact / 8 excluded.

Fail case:
  60 = 41 covered / 11 blocked / 8 excluded, but every remaining runtime blocker now says exactly
  which materializer contract is still missing or invalid.
```

### Tests
```text
Add:
  tests/build-runtime-materializer-repair-export.test.mjs
  tests/build-live-fullnet-row-v2-runtime-materializer-repair.test.mjs

Required assertions:
  - label-only gmgn/source_resonance/scout_quality cannot cover a module.
  - token-only LOW evidence cannot cover a module.
  - cross-window worker_health cannot cover a module.
  - deterministic-zero requires materializer manifest proof.
  - idempotency_write_path requires idempotency_key and write_path_status, not just enqueue=false.
  - holdout_negative_controls requires holdout cohort, not in-sample dog/dud.
  - Goal 1-7 fields preserve 0 drift after stripping Goal-8 keys.
  - smart_money and curve_pumpfun remain Goal 9 external exports.
  - EV remains null unless valid entered+fill+ledger+exit+friction exists.
```

### Acceptance criteria
```text
Goal 8 is complete only if:

1. runtime-readmodel-materializer-repair/ exists with JSONL, health, manifest, contract, unjoined file.
2. fullnet-row-v2-runtime-materializer-repair/ exists with row/report artifacts.
3. 329 rows remain projection_complete=true.
4. Goal 1-7 fields have 0 drift after stripping Goal-8 new keys.
5. EV remains fail-closed unless real entered+fill+ledger+exit+friction rows exist.
6. Each of the 9 modules is covered or blocked with repair-materializer-specific exact reason.
7. smart_money and curve_pumpfun remain deferred to Goal 9.
8. No generic blocked reason remains.
9. No live/gate/entry/exit/size/mode promotion code changes.
10. Tests + independent invariants + byte-repro pass.
```

### Decision after Goal 8
```text
If closure reaches 50/2/8:
  Proceed to Goal 9: smart_money + curve_pumpfun external Zeabur/on-chain exports.

If closure remains far below 50/2/8:
  Stop expanding fullnet and repair actual runtime emitters/materializers first.

If identity join failures dominate:
  Fix token_ca/signal_ts/source_id/lifecycle_id identity before strategy work.

If the repaired materializers prove some modules are label-only:
  Mark those modules as non-contributing until runtime emits real structured decisions.

No edge/promotion until real entered+fill+ledger+exit+friction rows exist.
```

```text
GOAL_8_READY_TO_EXECUTE (§15.25 runtime materializer repair for 9 still-blocked modules)
TARGET: repair/materialize the 9 runtime modules still blocked after Goal 7
NON_TARGET: smart_money + curve_pumpfun remain Goal 9 external exports
GUARDRAILS: research-only, same-window only, no strategy/live/mode changes, EV fail-closed
SUCCESS: runtime closure moves toward 50/2/8 or every remaining runtime blocker has final materializer-specific reason
```

---

## §15.26 STATUS — Goal 8: repaired the 9 still-blocked runtime materializers (2026-06-22)

Executed §15.25. Two new scripts; research-only; no live read; no strategy/gate/entry/exit/size/mode
change; EV fail-closed; Goal 1-7 fields preserved verbatim; all 7 prior layers still byte-reproduce.
Materialized ONLY from STRUCTURED a-class decision sub-objects (joined with the same v1 matcher as
has_decision) — never from a label string.

### What was built
```text
scripts/build-runtime-materializer-repair-export.js       (mine decision sub-objects for the 9)
scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js (join, _repair_ namespace)
runtime-readmodel-materializer-repair/  -> repair.jsonl + health/manifest/contract-report/unjoined
fullnet-row-v2-runtime-materializer-repair/ -> row.jsonl, summary.json, repair-owner/row-confidence/closure
  + runtime-materializer-repair-coverage-report.json, -join-quality-report.json, -contract-report.json
```

### Outcome — 3 of 9 repaired from structured decision sub-objects; the other 6 now FINAL
```text
REPAIRED -> covered (HIGH same-window join via v1 matcher = has_decision join):
  scout_quality          : source_component='scout_quality' decision events (188 signals) -> score + grade
                           (REJECT/A/...) + block_reason code + liquidity_score (risk.liquidity_usd) +
                           volume_score (source-to-raw volume_24h) + quote_executable. Structured component output.
  detector_calibration   : expected_rr_detail.rr_version (model_version, e.g. v1.a_class_2_to_1_bottom_ticket)
                           + rr_grade (calibration_bucket, e.g. A_PLUS) + expected_rr (110 signals).
  idempotency_write_path : risk.opportunity_key (idempotency key) + duplicate_of_event_id (dedupe result)
                           + decision_event_id + enqueue/order/fill_attempted=false (29 signals).
STILL BLOCKED -> FINAL materializer-specific reason (6; every remaining runtime blocker has a FINAL reason):
  gmgn_policy            : no gmgn_policy source_component (gmgn is kline-provider only).
  source_resonance       : resonance-probe components fire but gmgn_first_seen_ts/lead_time_sec absent.
  worker_health          : only post-window provider state; no in-window heartbeat.
  training_manifest      : no feature_snapshot/schema/manifest/hash anywhere.
  holdout_negative_controls : only in-sample dog/dud; no out-of-sample holdout cohort.
  assumptions_false_negative_budget : budget has daily_loss_budget (loss cap), not a FN/missed-dog budget.
NOT TOUCHED: smart_money + curve_pumpfun remain zeabur_export_required (Goal 9).
=> module closure: covered 41 -> 44, blocked 11 -> 8 (6 runtime FINAL + 2 zeabur), intentionally_excluded 8 (total 60).
```

### No-label discipline + Goal-1-7 preservation
```text
The 3 repairs use STRUCTURED object fields (event.score/grade, expected_rr_detail.rr_version/rr_grade,
risk.opportunity_key/duplicate_of_event_id) joined with the v1 (token,signal_ts/lifecycle_id/bounded
event_ts) matcher — NOT normalized_mode label strings. gmgn_policy/source_resonance stay blocked
precisely because their structured decision fields are absent (the label is not evidence).
Every Goal-8 row field is namespaced `<module>_repair_*`/`runtime_materializer_repair_*` (74 new keys across all rows),
distinct from Goal 6 `_runtime_`, Goal 7 `_materialized_`, and Goal 3/5 bare names. Verified: 0/329 drift.
```

### Verification
```text
node --check x16 OK. node --test (16 relevant suites, NODE_PATH): 16 pass / 0 fail (incl. BOTH Goal-8
  test files; script-1 test uses synthetic temp JSON + the real v1 matcher).
Independent invariants: row_count 329, projection_complete=true, EV null+BLOCKED, module count 60
  reconciles (44/8/8), no_generic_blocked_reason_remains=true, all_remaining_runtime_blockers_have_final_reason=true,
  smart_money/curve still zeabur, repair-owner distribution byte-identical.
Byte-reproducible: both Goal-8 scripts' artifacts identical on rerun; all 7 prior layers STILL byte-identical.
```

### Cumulative closure + decision
```text
60 modules = 44 covered / 8 blocked / 8 intentionally_excluded.
covered progression: 22 -> 25 -> 29 -> 35 -> 37 -> 38 -> 41 -> 44.
The 8 remaining blockers are now FINAL: 6 runtime modules each need a specific runtime emit (gmgn_policy
decision component, source_resonance lead-time, in-window worker heartbeat, feature/training manifest,
out-of-sample holdout, FN-budget/assumption ledger), and 2 external (smart_money, curve_pumpfun) are Goal 9.
§15.25 SUCCESS met: every remaining runtime blocker has a final materializer-specific reason.
EV stays null until real entered+fill+ledger+exit+friction. Evidence materialization / observability — not strategy.
```

```text
GOAL_8_COMPLETE (§15.26 runtime materializer repair)
REPAIRED 3 (scout_quality, detector_calibration, idempotency_write_path) -> covered 41 -> 44
RUNTIME_BLOCKERS_ALL_FINAL (6: gmgn_policy, source_resonance, worker_health, training_manifest, holdout_negative_controls, assumptions_false_negative_budget)
SMART_MONEY_AND_CURVE_PUMPFUN_NOT_RECLASSIFIED (zeabur -> Goal 9)
NO_LABEL_STRING_AS_STRUCTURED_EVIDENCE | GOAL_1_7_FIELDS_PRESERVED_VERBATIM (74 namespaced keys, 0 drift)
7 PRIOR LAYERS BYTE_REPRO_INTACT | EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329
TESTS_PASS (check x16 + 16 suites + byte-reproducible)
NO_LIVE_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
```

---

## §15.27 NEXT GOAL — Goal 9: external smart_money + pump.fun curve exports (2026-06-22)

### Why this is next
```text
Goal 8 leaves 8 blocked modules:

  6 runtime FINAL blockers:
    gmgn_policy
    source_resonance
    worker_health
    training_manifest
    holdout_negative_controls
    assumptions_false_negative_budget

  2 external Zeabur/on-chain blockers:
    smart_money
    curve_pumpfun

The 6 runtime blockers now have final runtime-emitter requirements. They cannot be solved from the frozen
pack without inventing evidence.

Goal 9 is therefore limited to the two external blockers:
  smart_money
  curve_pumpfun
```

### Objective
```text
Build a same-window external evidence export + join layer for:

  smart_money:
    wallet / KOL / smart-money buy-sell influence per token.

  curve_pumpfun:
    pump.fun bonding-curve / curve-stage / migration-state evidence per token.

Each module must end as one of:
  EXTERNAL_EVIDENCE_JOINED
  EXTERNAL_EMPTY_BUT_VALID
  EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON

Still research-only. No strategy, live execution, gate, size, exit, threshold, or mode-promotion change.
```

### Guardrails
```text
Do not:
  - call live trading endpoints
  - use private keys
  - change live gate / entry / exit / size / thresholds
  - use raw dog / peak labels as ex-ante evidence
  - mix windows
  - mark token-only LOW evidence as covered
  - treat missing external data as zero
  - claim edge or actual EV

Allowed:
  - read already-downloaded Zeabur/API/on-chain exports
  - run bounded read-only pullers if endpoint/token already exists
  - produce deterministic same-window artifacts
  - join external evidence into fullnet row v2
  - leave either module blocked with a final external-export reason
```

### smart_money contract
```text
Required row fields:
  schema_version
  module_group = smart_money
  token_ca
  symbol
  signal_ts
  source_id / lifecycle_id
  window_start_ts
  window_end_ts
  evidence_ts
  same_window_valid
  join_confidence
  evidence_source
  evidence_source_sha256

Required payload fields:
  smart_wallet_buy_count
  smart_wallet_sell_count
  smart_wallet_net_buy_count
  smart_wallet_net_sol
  smart_wallet_unique_n
  smart_wallet_top_wallet_concentration
  kol_buy_count
  kol_sell_count
  wallet_signal_score
  wallet_signal_direction
  wallet_signal_missing_reason
```

### curve_pumpfun contract
```text
Required row fields:
  schema_version
  module_group = curve_pumpfun
  token_ca
  symbol
  signal_ts
  source_id / lifecycle_id
  window_start_ts
  window_end_ts
  evidence_ts
  same_window_valid
  join_confidence
  evidence_source
  evidence_source_sha256

Required payload fields:
  curve_platform
  bonding_curve_address
  bonding_curve_progress_pct
  virtual_sol_reserves
  virtual_token_reserves
  real_sol_reserves
  real_token_reserves
  migration_state
  migrated_pool_address
  curve_stage
  curve_stage_confidence
  curve_decode_missing_reason
```

### New artifacts
```text
New directory:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/external-smart-curve-window/

Files:
  external-smart-curve-window.jsonl
  external-smart-curve-summary.json
  external-smart-curve-source-manifest.json
  external-smart-curve-health.json
  external-smart-curve-unjoined.jsonl
  external-smart-curve-contract-report.json

New fullnet layer:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-external-smart-curve/

Files:
  row.jsonl
  summary.json
  repair-owner-report.json
  row-confidence-report.json
  final-module-closure-coverage-report.json
  external-smart-curve-coverage-report.json
  external-smart-curve-join-quality-report.json
  external-smart-curve-contract-report.json
```

### New scripts
```text
1. scripts/build-external-smart-curve-window-export.js

   CLI:
     node scripts/build-external-smart-curve-window-export.js \
       --window-start-ts <unix_sec> \
       --window-end-ts <unix_sec> \
       --fullnet-row <pack>/fullnet-row-v2-runtime-materializer-repair/row.jsonl \
       --smart-money-export <optional path> \
       --curve-pumpfun-export <optional path> \
       --out-dir <pack>/external-smart-curve-window

2. scripts/build-live-fullnet-row-v2-external-smart-curve.js

   CLI:
     node scripts/build-live-fullnet-row-v2-external-smart-curve.js \
       <all Goal 8 args> \
       --external-smart-curve-dir <pack>/external-smart-curve-window \
       --out-dir <pack>/fullnet-row-v2-external-smart-curve
```

### Row field namespace
```text
smart_money_external_seen
smart_money_external_status
smart_money_external_join_confidence
smart_money_external_source
smart_money_external_evidence_ts
smart_money_external_payload_hash
smart_money_external_missing_reason

curve_pumpfun_external_seen
curve_pumpfun_external_status
curve_pumpfun_external_join_confidence
curve_pumpfun_external_source
curve_pumpfun_external_evidence_ts
curve_pumpfun_external_payload_hash
curve_pumpfun_external_missing_reason

external_smart_curve_covered_modules[]
external_smart_curve_still_blocked_modules[]
external_smart_curve_empty_valid_modules[]
external_smart_curve_unjoined_modules[]
external_smart_curve_confidence
external_smart_curve_warnings[]
```

### Closure rules
```text
blocked -> covered:
  same_window_valid=true
  join_confidence HIGH or MEDIUM
  required fields present
  source manifest present

blocked -> covered empty-valid:
  exporter ran for full window
  manifest proves full-window coverage
  zero rows is meaningful for the token/module

still blocked:
  exporter missing
  schema invalid
  cross-window evidence
  token-only LOW evidence
  missing source manifest
  missing bounded timestamp evidence
```

### Expected closure
```text
Starting point:
  60 = 44 covered / 8 blocked / 8 excluded

Best:
  60 = 46 covered / 6 blocked / 8 excluded

Partial:
  60 = 45 covered / 7 blocked / 8 excluded

Fail:
  60 = 44 covered / 8 blocked / 8 excluded, but smart_money and curve_pumpfun both have final external reasons.
```

### Tests
```text
Add:
  tests/build-external-smart-curve-window-export.test.mjs
  tests/build-live-fullnet-row-v2-external-smart-curve.test.mjs

Assertions:
  - missing optional export becomes final blocked reason, not crash
  - stale/cross-window evidence goes to unjoined
  - token-only LOW evidence cannot cover either module
  - empty-valid requires full-window manifest
  - required fields enforced for both contracts
  - Goal 1-8 fields preserve 0 drift after stripping Goal-9 keys
  - EV remains null unless valid entered+fill+ledger+exit+friction exists
  - output byte-reproducible except generated_at
```

### Acceptance criteria
```text
Goal 9 is complete only if:

1. external-smart-curve-window/ exists with JSONL, health, manifest, contract, unjoined file.
2. fullnet-row-v2-external-smart-curve/ exists with row/report artifacts.
3. 329 rows remain projection_complete=true.
4. Goal 1-8 fields have 0 drift after stripping Goal-9 keys.
5. EV remains fail-closed unless real entered+fill+ledger+exit+friction rows exist.
6. smart_money is covered or blocked with final external-export reason.
7. curve_pumpfun is covered or blocked with final external-export reason.
8. No generic blocked reason remains.
9. No live/gate/entry/exit/size/mode promotion code changes.
10. Tests + independent invariants + byte-repro pass.
```

### Decision after Goal 9
```text
If both external modules are covered:
  Closure becomes 46/6/8. Remaining blockers are the 6 runtime FINAL emitters.

If either remains blocked:
  Treat it as external data-source availability gap until the exporter exists.

If entered/fill/ledger/exit/friction is still absent:
  Do not claim edge. Next work is execution/ledger evidence, not strategy tuning.
```

```text
GOAL_9_READY_TO_EXECUTE (§15.27 external smart_money + pump.fun curve exports)
TARGET: cover or finally block smart_money and curve_pumpfun with same-window external evidence
NON_TARGET: the 6 runtime FINAL modules remain runtime-emitter work
GUARDRAILS: research-only, bounded exports, no strategy/live/mode changes, EV fail-closed
SUCCESS: smart_money/curve_pumpfun covered-or-final-blocked, Goal1-8 0 drift, no generic reason
```

---

## §15.28 STATUS — Goal 9: external smart_money + curve_pumpfun resolved (2026-06-22)

Executed §15.27. Two new scripts; research-only; **no live calls** (guardrail); no strategy/gate/entry/exit/size/mode
change; EV fail-closed; Goal 1-8 fields preserved verbatim; all 8 prior layers still byte-reproduce.

### What was built
```text
scripts/build-external-smart-curve-window-export.js        (join already-downloaded same-window wallet + curve exports)
scripts/build-live-fullnet-row-v2-external-smart-curve.js  (join layer, _external_ namespace, reclassify smart/curve)
external-smart-curve-window/    -> window.jsonl + summary + source-manifest + health + unjoined + contract-report
fullnet-row-v2-external-smart-curve/ -> row.jsonl, summary.json, repair-owner/row-confidence/final-module-closure,
  external-smart-curve-coverage-report.json
```

### Outcome — both modules FINAL-blocked (no same-window external evidence; no live calls allowed)
```text
Reality check first (no fabrication): scanned data-room + the frozen pack for any same-window per-(token,signal)
external evidence:
  smart_money  : NONE. No wallet/KOL/smart-money export anywhere; sentiment_arb snapshot has no wallet/smart columns.
  curve_pumpfun: NONE same-window. The on-disk curve artifacts (curve-stage-*, curve-threshold-*) are 06-18,
                 cross-window AND contract/threshold-level, not per-token bonding-curve decode for the 06-21 cohort.
Guardrail forbids live calls, so neither can be fetched now. Both => EXTERNAL_MISSING_OR_INVALID_WITH_FINAL_REASON:
  smart_money   FINAL: requires a bounded read-only Zeabur/API export of per-token wallet buy/sell influence
                (smart_wallet_buy/sell/net_sol/unique_n/top_wallet_concentration, kol_buy/sell, wallet_signal_score/direction).
  curve_pumpfun FINAL: requires a bounded on-chain/Helius export of per-token bonding-curve
                (curve_platform/bonding_curve_address/progress_pct/virtual+real reserves/migration_state/curve_stage).
The scripts are NOT stubs: both accept --smart-money-export / --curve-pumpfun-export; when a same-window export with
HIGH/MEDIUM join + required fields lands, rerun reclassifies the module blocked -> covered (token-only LOW never covers,
cross-window never covers, zero-rows-full-window => EXTERNAL_EMPTY_BUT_VALID). Verified by the script-1 + script-2 tests.
=> module closure UNCHANGED counts: covered 44, blocked 8, intentionally_excluded 8 (total 60). But the 2 external
   blockers move from generic zeabur_export_required -> FINAL external-export reason, so ALL 8 blocked are now FINAL.
```

### Goal-1-8 preservation + namespacing
```text
Every Goal-9 row field is namespaced `<module>_external_*` / `external_smart_curve_*` (20 new keys), distinct from
Goal 6 `_runtime_`, Goal 7 `_materialized_`, Goal 8 `_repair_`, Goal 3/5 bare names. Verified: strip the 20 keys
from each Goal-9 row => byte-identical to the Goal-8 row (0/329 drift). smart_money/curve had no per-row Goal-1-8
fields, so the reclassification lives only in the closure report — no prior field mutated.
```

### Verification
```text
node --check x18 OK. node --test (18 relevant suites, NODE_PATH): 25 sub-tests pass / 0 fail (incl. BOTH Goal-9
  test files; script-1 test joins a synthetic temp export and asserts HIGH-cover / cross-window-reject /
  token-only-LOW-reject / missing-field-reject / empty-but-valid).
Independent invariants: row_count 329, projection_complete=true, EV null + BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER,
  module count 60 reconciles (44/8/8), smart_money_and_curve_pumpfun_resolved=true (both final_blocked),
  all_blocked_have_final_reason=true, no_generic_blocked_reason_remains=true,
  all_remaining_runtime_blockers_have_final_reason=true (the 6 runtime FINAL intact).
Byte-reproducible: both Goal-9 scripts' artifacts identical on rerun; all 8 prior layers STILL byte-identical;
  live config/src/data untouched.
```

### Cumulative closure — all 60 modules now resolved
```text
60 modules = 44 covered / 8 blocked / 8 intentionally_excluded.  EVERY one of the 8 blocked now carries a FINAL reason:
  6 runtime FINAL  : gmgn_policy, source_resonance, worker_health, training_manifest, holdout_negative_controls,
                     assumptions_false_negative_budget   (each needs a specific runtime emitter)
  2 external FINAL : smart_money (bounded wallet/KOL export), curve_pumpfun (bounded on-chain curve decode)
covered progression: 22 -> 25 -> 29 -> 35 -> 37 -> 38 -> 41 -> 44 (stable; Goal 9 added no cover, only finalized the 2).
§15.27 SUCCESS met: smart_money/curve_pumpfun are covered-or-final-blocked (both final-blocked with exact external
contract), Goal 1-8 0 drift, no generic reason remains. EV stays null until real entered+fill+ledger+exit+friction.
This is the END of the fullnet module-closure audit: there is no remaining generic/unknown blocker — every gap is
either covered-with-proof, intentionally excluded, or carries an exact FINAL emit/export requirement. Observability,
not strategy.
```

```text
GOAL_9_COMPLETE (§15.28 external smart_money + curve_pumpfun)
BOTH_RESOLVED -> final_blocked with exact external-export contract (no same-window evidence, no live calls allowed)
ALL_8_BLOCKED_NOW_FINAL (6 runtime emitters + 2 external exports) | NO_GENERIC_REASON_REMAINS
GOAL_1_8_FIELDS_PRESERVED_VERBATIM (20 namespaced keys, 0/329 drift) | 8 PRIOR LAYERS BYTE_REPRO_INTACT
EV_FAIL_CLOSED | PROJECTION_COMPLETE=TRUE | ROW_COUNT=329 | CLOSURE 44/8/8 = 60
TESTS_PASS (check x18 + 18 suites/25 sub-tests + byte-reproducible) | LIVE_UNTOUCHED
NO_LIVE_CALL | NO_STRATEGY_CHANGE | NO_EDGE_DECLARED | NO_THRESHOLD_TUNING | NO_MODE_OPENED
END_OF_MODULE_CLOSURE_AUDIT: 60/60 modules resolved (covered / excluded / FINAL-reason)
```

---

## §15.29 NEXT GOAL — Goal 10: produce real evidence exports for the 8 FINAL blockers (2026-06-22)

Goal 1-9 closed the fullnet module-closure audit. Do **not** build another audit wrapper unless a new evidence
source actually exists. The next useful goal is to produce the missing same-window evidence exports that can turn
the 8 FINAL blockers into covered modules.

### Current state
```text
Current closure:
  60 modules = 44 covered / 8 blocked / 8 intentionally_excluded

The 8 blocked modules are all FINAL, exact, and non-generic:
  Runtime emitter required (6):
    gmgn_policy
    source_resonance
    worker_health
    training_manifest
    holdout_negative_controls
    assumptions_false_negative_budget

  External export required (2):
    smart_money
    curve_pumpfun

EV state:
  ev_eligible = false for 329/329
  actual_net_ev = null for 329/329
  gate = BLOCKED_NO_VALID_ENTERED_FILL_EXIT_LEDGER

Meaning:
  Audit closure is complete.
  Edge/promotion is still impossible until real entered+fill+ledger+exit+friction exists.
```

### Goal 10 objective
```text
Produce bounded, same-window, per-(token,signal) evidence exports for the 8 FINAL blockers, then rerun the existing
Goal 6-9 join scripts without changing strategy/live/gate/entry/exit/size/mode code.

Success target:
  44/8/8 -> as many of the 8 blocked modules as real evidence supports.

Hard rule:
  If evidence does not exist, keep the module FINAL-blocked. Do not synthesize, infer from labels, or parse mode names
  as proof.
```

### Non-goals
```text
Do not:
  - change entry logic
  - open blocked modes
  - tune thresholds
  - change position size
  - modify live execution
  - declare EV/edge
  - treat old backtests as current evidence
  - use cross-window artifacts as same-window proof
  - make unbounded live API calls from the fullnet builder
```

### Evidence contracts

#### Runtime emitter export
```text
Target artifact:
  runtime-final-emitter-window/runtime-final-emitter-window.jsonl

Required identity keys per row:
  token_ca
  signal_ts
  premium_signal_id if available
  module_group
  evidence_ts
  window_start_ts
  window_end_ts
  join_confidence = HIGH | MEDIUM | LOW
  payload_hash
  source

Only HIGH/MEDIUM same-window evidence may cover a module.
LOW, token-only, cross-window, or missing required fields must go to unjoined/final-blocked.
```

Runtime module fields:
```text
gmgn_policy:
  gmgn_policy_decision
  gmgn_policy_reason
  gmgn_policy_source
  gmgn_policy_version

source_resonance:
  gmgn_first_seen_ts
  gmgn_last_seen_ts
  lead_time_sec
  resonance_source
  resonance_score
  timestamp_valid

worker_health:
  worker_name
  worker_status
  heartbeat_ts
  provider_status
  error_count_window
  degraded_reason

training_manifest:
  manifest_id
  feature_schema_version
  model_or_ruleset_version
  generated_at_ts
  training_window_start_ts
  training_window_end_ts

holdout_negative_controls:
  holdout_id
  holdout_window_start_ts
  holdout_window_end_ts
  negative_control_name
  control_result
  leakage_check_pass

assumptions_false_negative_budget:
  budget_id
  false_negative_budget_n
  false_negative_budget_pct
  observed_false_negative_n
  budget_status
  assumption_version
```

#### External export
```text
Target artifact:
  external-smart-curve-window/

Reuse Goal 9 contracts:
  smart_money:
    smart_wallet_buy_count
    smart_wallet_sell_count
    smart_wallet_net_sol
    smart_wallet_unique_n
    top_wallet_concentration
    kol_buy_count
    kol_sell_count
    wallet_signal_score
    wallet_signal_direction

  curve_pumpfun:
    curve_platform
    bonding_curve_address
    bonding_curve_progress_pct
    virtual_sol_reserves
    virtual_token_reserves
    real_sol_reserves
    real_token_reserves
    migration_state
    curve_stage
```

### Implementation plan
```text
1. Add a bounded export job for the 6 runtime emitters.
   Suggested script:
     scripts/export-runtime-final-emitters-window.js

   Output:
     runtime-final-emitter-window/runtime-final-emitter-window.jsonl
     runtime-final-emitter-window/runtime-final-emitter-summary.json
     runtime-final-emitter-window/runtime-final-emitter-health.json
     runtime-final-emitter-window/runtime-final-emitter-unjoined.jsonl
     runtime-final-emitter-window/runtime-final-emitter-contract-report.json

2. Feed the runtime export into the existing Goal 6-8 materializer/join path.
   Prefer reusing:
     scripts/build-runtime-readmodel-window-export.js
     scripts/build-runtime-readmodel-materialized-export.js
     scripts/build-live-fullnet-row-v2-runtime-materialized.js
     scripts/build-live-fullnet-row-v2-runtime-materializer-repair.js

   Only add a tiny adapter if the existing scripts cannot consume the new artifact cleanly.

3. Produce bounded external exports for smart_money and curve_pumpfun.
   Preferred path:
     Use already-downloaded Zeabur/API/on-chain files when present.
     If fetching is required, run a separate bounded exporter, not from the fullnet joiner.

   Output must be same-window and per-(token,signal). Cross-window stays unjoined.

4. Rerun Goal 9 join:
     scripts/build-external-smart-curve-window-export.js
     scripts/build-live-fullnet-row-v2-external-smart-curve.js

5. Rerun final invariants:
   - row_count = 329
   - summary.projection_complete = true
   - Goal 1-9 fields have 0 drift except new evidence fields
   - EV remains fail-closed unless real entered+fill+ledger+exit+friction exists
   - no generic blocked reason returns
   - every newly covered module has same-window HIGH/MEDIUM evidence
```

### Tests
```text
Add the smallest useful checks:

tests/export-runtime-final-emitters-window.test.mjs
  - same-window HIGH/MEDIUM evidence is accepted
  - cross-window evidence goes to unjoined
  - token-only LOW evidence cannot cover
  - missing required module fields cannot cover
  - empty-valid export is valid but covers zero modules

If an adapter script is added:
  tests/build-live-fullnet-row-v2-final-evidence-sources.test.mjs
    - Goal 1-9 drift remains 0 after stripping new keys
    - covered count only increases for modules with proof
    - EV stays null without entered+fill+ledger+exit+friction
```

### Acceptance criteria
```text
Goal 10 is complete only if:

1. Runtime emitter export artifacts exist, even if zero-row valid.
2. smart_money / curve_pumpfun external export artifacts exist or remain FINAL with exact no-export reason.
3. Every one of the 8 FINAL blockers is either:
   - covered with same-window HIGH/MEDIUM proof, or
   - still FINAL-blocked with the exact missing export/emitter reason.
4. No generic/unknown blocker exists.
5. Goal 1-9 fields preserve 0 drift after stripping Goal-10 evidence keys.
6. summary.projection_complete remains true.
7. EV remains fail-closed unless real entered+fill+ledger+exit+friction exists.
8. No strategy/live/gate/entry/exit/size/mode file is changed.
9. Tests + independent invariants pass.
10. The final report states exactly which modules moved from blocked -> covered and why.
```

### Decision after Goal 10
```text
If all 8 become covered:
  Module closure becomes 52 covered / 0 blocked / 8 intentionally_excluded.
  Stop module-closure work. Next required evidence is real execution/ledger/friction.

If some remain blocked:
  Keep them FINAL. Do not write another wrapper. The next task is the missing emitter/export owner, not fullnet logic.

If real entered+fill+ledger+exit+friction appears:
  Start a separate EV goal. Only then evaluate FBR, RED/low-volume/active-momentum, Stage2A/Stage3, and entry modes.
```

```text
GOAL_10_READY_TO_EXECUTE (§15.29 final evidence-source production)
TARGET: produce same-window runtime/external evidence exports for the 8 FINAL blockers
NON_TARGET: no strategy changes, no mode opening, no threshold tuning, no edge declaration
GUARDRAILS: bounded exports, same-window only, proof-gated coverage, EV fail-closed
SUCCESS: each of the 8 modules covered-with-proof or still FINAL-blocked with exact reason; Goal1-9 0 drift
```

---

## §15.30 STATUS — Goal 10: final evidence-source exports resolved fail-closed (2026-06-22)

Implemented the bounded runtime emitter export job from §15.29 and reused the Goal 9 external smart/curve export
contract. This is **not** another fullnet row wrapper and does not change strategy/live/gate/entry/exit/size/mode code.

### What was built
```text
scripts/export-runtime-final-emitters-window.js
tests/export-runtime-final-emitters-window.test.mjs

runtime-final-emitter-window/
  runtime-final-emitter-window.jsonl
  runtime-final-emitter-summary.json
  runtime-final-emitter-health.json
  runtime-final-emitter-unjoined.jsonl
  runtime-final-emitter-contract-report.json
  runtime-final-emitter-source-manifest.json
```

### Result — complete under the fail-closed branch
```text
No same-window runtime-final emitter JSONL was available, so the exporter produced a valid zero-row artifact:
  joined_n = 0
  unjoined_n = 0
  covered_modules = []

The 6 runtime modules remain FINAL with exact emitter requirements:
  gmgn_policy
  source_resonance
  worker_health
  training_manifest
  holdout_negative_controls
  assumptions_false_negative_budget

The 2 external modules remain resolved by Goal 9:
  smart_money   -> FINAL external export required
  curve_pumpfun -> FINAL external export required

Therefore Goal 10 does not change closure counts:
  60 = 44 covered / 8 blocked / 8 intentionally_excluded
```

### Verification
```text
node --check scripts/export-runtime-final-emitters-window.js                         OK
node --test tests/export-runtime-final-emitters-window.test.mjs                      3/3 pass

Independent invariant:
  runtime-final-emitter-window has all 6 runtime modules FINAL
  external-smart-curve-window has both external modules FINAL
  no runtime module was covered without HIGH/MEDIUM same-window proof
  no live calls were made by the exporter
```

### Remaining work
```text
Goal 10 is complete, but it is not an edge/promotion goal. To move any of the 8 modules to covered, an owner must provide a real
same-window export:
  --runtime-final-export for the 6 runtime emitters
  --smart-money-export / --curve-pumpfun-export for the 2 external modules

Until real entered+fill+ledger+exit+friction exists:
  EV remains null.
  FBR / RED-low-volume-active / Stage2A / Stage3 / entry-mode claims remain hypotheses.
```

```text
GOAL_10_COMPLETE (§15.30 final evidence-source exports resolved fail-closed)
RUNTIME_EXPORTER_BUILT | ZERO_ROW_VALID_ARTIFACT_WRITTEN | 6_RUNTIME_FINAL_RETAINED
EXTERNAL_FINAL_RETAINED_FROM_GOAL_9 | CLOSURE_STABLE_44_8_8 | EV_FAIL_CLOSED
NO_STRATEGY_CHANGE | NO_LIVE_CALL | TESTS_PASS
```

---

## §15.31 NEXT GOAL — Goal 11: emit real runtime evidence for the 6 runtime FINAL blockers (2026-06-22)

Goal 10 proved the export contract works, but the runtime emitter artifact is zero-row because no production-side
runtime emitter writes the 6 missing readmodels yet. Goal 11 is the smallest useful production-side change: add a
bounded, research-only runtime evidence writer/export path for the 6 internal modules. This is **not** a strategy
change and does not open modes.

### Target modules
```text
Runtime emitter required (6):
  gmgn_policy
  source_resonance
  worker_health
  training_manifest
  holdout_negative_controls
  assumptions_false_negative_budget

Out of scope for Goal 11:
  smart_money
  curve_pumpfun

Reason:
  smart_money / curve_pumpfun are external Zeabur/on-chain exports. Goal 11 should fix the internal runtime
  emitter gap first. External exports can be Goal 12.
```

### Objective
```text
Add a runtime evidence append-only JSONL/log table that records same-window, per-(token,signal) evidence for the
6 internal runtime modules, then export it into the Goal 10 contract:

  runtime-final-emitter-window/runtime-final-emitter-window.jsonl

The evidence must be emitted by runtime components at the moment they make or observe the relevant decision/state.
Do not reconstruct it later from labels, normalized_mode strings, or hindsight PnL.
```

### Minimal architecture
```text
Add one small helper:
  runtime_final_evidence_writer

It should do only three things:
  1. normalize identity:
       token_ca
       signal_ts
       premium_signal_id if available
       module_group
       evidence_ts
       window_start_ts/window_end_ts or enough timestamp data for export-time windowing
  2. validate required payload fields for the module
  3. append JSONL or insert into an existing research/audit table

Preferred storage:
  append-only JSONL under a research/audit artifact path, or an existing audit SQLite table if one already exists.

Do not add:
  new service
  new queue
  new dependency
  new mode registry
  new dashboard first
```

### Required payloads
```text
gmgn_policy:
  gmgn_policy_decision
  gmgn_policy_reason
  gmgn_policy_source
  gmgn_policy_version

source_resonance:
  gmgn_first_seen_ts
  gmgn_last_seen_ts
  lead_time_sec
  resonance_source
  resonance_score
  timestamp_valid

worker_health:
  worker_name
  worker_status
  heartbeat_ts
  provider_status
  error_count_window
  degraded_reason

training_manifest:
  manifest_id
  feature_schema_version
  model_or_ruleset_version
  generated_at_ts
  training_window_start_ts
  training_window_end_ts

holdout_negative_controls:
  holdout_id
  holdout_window_start_ts
  holdout_window_end_ts
  negative_control_name
  control_result
  leakage_check_pass

assumptions_false_negative_budget:
  budget_id
  false_negative_budget_n
  false_negative_budget_pct
  observed_false_negative_n
  budget_status
  assumption_version
```

### Implementation plan
```text
1. Locate the existing runtime/audit write paths.
   Search targets:
     gmgn policy / gmgn provider decision
     source resonance / pre-pass resonance / lead-time calculation
     worker heartbeat / provider state update
     training or feature manifest constants
     promotion / holdout / negative-control config
     assumptions / missed-dog false-negative budget config

2. Add one writer helper.
   Suggested shape:
     emitRuntimeFinalEvidence(moduleGroup, identity, payload)

   The helper should:
     - reject unknown module_group
     - reject missing required fields
     - append deterministic JSONL rows
     - never throw in the trading path; log warning and continue
     - include source/version/payload_hash

3. Wire the helper into the 6 runtime points.
   The first pass may be sparse:
     - if a module has true runtime evidence, emit it
     - if a module only has static policy/config, emit a window-global row only if it can be joined honestly
     - if no honest runtime source exists, do not emit fake rows

4. Add an export command.
   It should read the runtime evidence store and produce a JSONL acceptable to:
     scripts/export-runtime-final-emitters-window.js --runtime-final-export <jsonl>

5. Rerun Goal 10 exporter with the real runtime-final export.
   Expected result:
     modules with HIGH/MEDIUM same-window evidence move to covered
     modules still missing remain FINAL

6. Update §15.32 STATUS with exact module movement:
     covered_before = 44
     covered_after = 44 + N
     still_blocked_runtime = 6 - N
```

### Guardrails
```text
Hard guardrails:
  - no strategy logic change
  - no threshold tuning
  - no entry/exit/size/mode change
  - no live execution change
  - no PnL/EV inference
  - no label parsing as evidence
  - no cross-window join

Runtime safety:
  - writer must be best-effort and non-fatal
  - if evidence write fails, trading/runtime behavior must continue unchanged
  - export job may fail loudly; runtime path must not
```

### Tests
```text
Add the smallest useful tests:

1. writer helper:
   - accepts a valid module payload
   - rejects unknown module
   - rejects missing required fields
   - writes deterministic JSONL
   - write failure does not throw when called in non-fatal mode

2. export command:
   - same-window HIGH/MEDIUM evidence covers
   - cross-window evidence goes unjoined
   - LOW/token-only evidence cannot cover
   - missing fields cannot cover

3. existing Goal 10 test still passes:
   node --test tests/export-runtime-final-emitters-window.test.mjs
```

### Acceptance criteria
```text
Goal 11 is complete only if:

1. A runtime evidence writer/helper exists.
2. At least one runtime integration point emits real evidence, or the implementation proves no honest runtime source
   exists without fabricating rows.
3. Runtime evidence can be exported as JSONL accepted by Goal 10's exporter.
4. Goal 10 exporter is rerun with the new runtime-final export.
5. Every covered module has same-window HIGH/MEDIUM proof.
6. Every still-blocked module remains FINAL with exact reason.
7. No strategy/live/gate/entry/exit/size/mode logic is changed.
8. EV remains fail-closed unless real entered+fill+ledger+exit+friction exists.
9. Tests and independent invariants pass.
10. §15.32 records exact covered/still-blocked counts.
```

### Decision after Goal 11
```text
If internal runtime modules become covered:
  Proceed to Goal 12: external smart_money + curve_pumpfun real exports.

If zero modules become covered:
  Stop adding fullnet code. The blocker is production instrumentation ownership, not research logic.

If real entered/fill/ledger/exit/friction appears before external exports:
  Start EV analysis separately and keep missing smart/curve/runtime fields as confidence warnings, not edge proof.
```

```text
GOAL_11_READY_TO_EXECUTE (§15.31 runtime final evidence emitters)
TARGET: production-side research-only evidence writer/export for 6 runtime FINAL blockers
NON_TARGET: smart_money/curve_pumpfun, strategy changes, mode opening, EV claims
GUARDRAILS: append-only, non-fatal runtime writes, same-window proof only, no label-derived evidence
SUCCESS: runtime-final export contains real same-window evidence or exact FINAL reasons; Goal10 rerun proves movement
```

---

## §15.32 STATUS — Goal 11: runtime final evidence writer implemented, source_resonance wired (2026-06-22)

Implemented §15.31 with the smallest production-side change: a disabled-by-default, append-only, non-fatal runtime
evidence writer/exporter, plus one honest runtime integration point (`source_resonance_shadow`). No strategy/live/gate/
entry/exit/size/mode logic changed.

### What was built
```text
sentiment-arbitrage-system/scripts/runtime_final_evidence.py
sentiment-arbitrage-system/test_runtime_final_evidence.py

Runtime integration:
  sentiment-arbitrage-system/scripts/source_resonance_shadow.py

Export path:
  python3 scripts/runtime_final_evidence.py export \
    --raw-log <runtime_final_evidence.jsonl> \
    --fullnet-row <fullnet row.jsonl> \
    --window-start-ts <s> \
    --window-end-ts <s> \
    --out <runtime-final-export.jsonl>

Then feed the output into:
  sas-research/scripts/export-runtime-final-emitters-window.js --runtime-final-export <runtime-final-export.jsonl>
```

### Runtime behavior
```text
The writer is disabled unless RUNTIME_FINAL_EVIDENCE_LOG is set.
When enabled, it appends JSONL rows and never throws in the trading/runtime path.

source_resonance_shadow now emits module_group=source_resonance only when the candidate has honest required fields:
  gmgn_first_seen_ts
  gmgn_last_seen_ts
  lead_time_sec
  resonance_source
  resonance_score
  timestamp_valid

If those fields are missing, the helper skips/fails non-fatally. It does not fabricate rows for telegram_only signals.
```

### Current same-window rerun
```text
Current runtime raw log path had no same-window rows:
  /Users/boliu/sentiment-arbitrage-system/data/runtime_final_evidence.jsonl

Export result:
  exported = 0

Goal 10 exporter rerun output:
  runtime-final-emitter-window-goal11/
    runtime-final-emitter-window.jsonl
    runtime-final-emitter-summary.json
    runtime-final-emitter-health.json
    runtime-final-emitter-unjoined.jsonl
    runtime-final-emitter-contract-report.json
    runtime-final-emitter-source-manifest.json

Result:
  joined_n = 0
  covered_modules = []
  still_blocked_runtime = 6
  closure remains 44 covered / 8 blocked / 8 intentionally_excluded
```

### Verification
```text
/usr/bin/python3 -m py_compile scripts/runtime_final_evidence.py scripts/source_resonance_shadow.py     OK

Manual invariant with /usr/bin/python3:
  - valid source_resonance payload writes append-only JSONL
  - missing required fields fail
  - export filters by fullnet token+signal and same-window evidence_ts
  - source_resonance_shadow.upsert_candidate emits source_resonance evidence when RUNTIME_FINAL_EVIDENCE_LOG is set

pytest note:
  repo default python3 points to /Library/Frameworks/Python.framework/Versions/3.9/bin/python3 and hung at startup in
  this shell; /usr/bin/python3 works but has no pytest installed. Therefore the new pytest file was added, and the
  equivalent assertions were run manually with /usr/bin/python3.
```

### Status against Goal 11 acceptance
```text
1. Runtime evidence writer/helper exists: PASS
2. Runtime integration point exists: PASS (source_resonance_shadow)
3. Runtime evidence can be exported as JSONL accepted by Goal 10 exporter: PASS
4. Goal 10 exporter rerun with runtime-final export: PASS
5. Covered modules require HIGH/MEDIUM same-window proof: PASS
6. Still-blocked modules remain FINAL: PASS
7. No strategy/live/gate/entry/exit/size/mode logic changed: PASS
8. EV remains fail-closed: PASS
9. Tests/invariants pass: PASS, with Python environment caveat above
10. Exact movement recorded: PASS
    covered_before = 44
    covered_after = 44
    moved = 0
    reason = no runtime raw evidence existed yet in the current same-window pack
```

### Remaining work
```text
To move counts, deploy/run with:
  RUNTIME_FINAL_EVIDENCE_LOG=/app/data/runtime_final_evidence.jsonl

Then rerun the export chain on a fresh same-window pack. Expected first covered module:
  source_resonance

The other 5 runtime modules still need honest emitters:
  gmgn_policy
  worker_health
  training_manifest
  holdout_negative_controls
  assumptions_false_negative_budget

External modules remain Goal 12:
  smart_money
  curve_pumpfun
```

```text
GOAL_11_COMPLETE (§15.32 runtime final evidence writer/export)
WRITER_BUILT | SOURCE_RESONANCE_WIRED | EXPORT_CHAIN_RERUN | CURRENT_WINDOW_ZERO_ROWS_FAIL_CLOSED
CLOSURE_STABLE_44_8_8 | EV_FAIL_CLOSED | NO_STRATEGY_CHANGE | NO_LIVE_CALL
```

---

## §15.33 NEXT GOAL — Goal 12: wire gmgn_policy + worker_health runtime final emitters (2026-06-22)

Goal 11 added the shared runtime evidence writer and wired the first honest emitter (`source_resonance`). Goal 12
should not expand scope to every remaining blocker. The next smallest useful runtime work is to wire the two internal
modules that already have nearby structured runtime state:

```text
Target modules:
  gmgn_policy
  worker_health

Still out of scope:
  training_manifest
  holdout_negative_controls
  assumptions_false_negative_budget
  smart_money
  curve_pumpfun
```

### Why these two
```text
gmgn_policy:
  Runtime already builds structured policy objects in gmgn_policy.py / entry_engine.py / paper_trade_monitor.py.
  The missing piece is append-only evidence emission, not strategy logic.

worker_health:
  source_resonance_shadow already records health and provider state. The missing piece is exporting a runtime-final
  evidence row with heartbeat/provider/error fields.

The other three runtime modules are governance/manifest artifacts and should not be invented inside trading code.
The two external modules remain Zeabur/on-chain export work.
```

### Objective
```text
Wire runtime_final_evidence.emit_runtime_final_evidence into:
  1. gmgn_policy decision creation / use point
  2. source_resonance worker health write point

Then run the same export chain:
  runtime_final_evidence.py export
  export-runtime-final-emitters-window.js --runtime-final-export

Success target on a fresh window:
  source_resonance + gmgn_policy + worker_health can become covered if they have same-window HIGH/MEDIUM evidence.

On the current historical window:
  fail-closed zero rows are acceptable if no runtime raw log exists.
```

### gmgn_policy contract
```text
Emit module_group = gmgn_policy with:
  gmgn_policy_decision
  gmgn_policy_reason
  gmgn_policy_source
  gmgn_policy_version

Suggested mapping:
  gmgn_policy_decision = policy.action or derived allow/reject/downsize/boost
  gmgn_policy_reason   = policy.reason
  gmgn_policy_source   = "gmgn_policy.evaluate_gmgn_lotto_policy" or actual caller
  gmgn_policy_version  = stable constant, e.g. "gmgn_paper_policy.v1"

Identity:
  token_ca
  signal_ts
  premium_signal_id if available

Rules:
  - emit only when token_ca + signal_ts exist
  - do not parse normalized_mode labels
  - do not change policy output
  - writer failure must not affect entry decisions
```

### worker_health contract
```text
Emit module_group = worker_health with:
  worker_name
  worker_status
  heartbeat_ts
  provider_status
  error_count_window
  degraded_reason

Suggested mapping for source_resonance_shadow:
  worker_name        = "source_resonance_shadow"
  worker_status      = "ok" or "error"
  heartbeat_ts       = run_ts
  provider_status    = "ok" unless error is present
  error_count_window = 0 or 1 for this write
  degraded_reason    = error string or "none"

This can be window-global, but Goal 10 coverage requires per-(token,signal) HIGH/MEDIUM rows. Therefore either:
  - emit per candidate when writing candidates, or
  - keep worker_health FINAL until a real per-signal readmodel exists.

Do not cover worker_health from a stale global heartbeat alone.
```

### Implementation plan
```text
1. Inspect current gmgn_policy call sites:
   - scripts/gmgn_policy.py
   - scripts/entry_engine.py
   - scripts/paper_trade_monitor.py

2. Add the smallest honest gmgn_policy emitter:
   - prefer one wrapper near the point where policy result and token identity are both available
   - pass policy result unchanged
   - emit best-effort evidence only if token_ca + signal_ts are present

3. Add worker_health emitter in source_resonance_shadow:
   - either per candidate in the same batch as source_resonance evidence
   - or record as final-blocked if only global heartbeat exists

4. Extend tests:
   - gmgn_policy evidence row accepts valid policy result
   - missing token_ca/signal_ts does not emit
   - worker_health valid payload emits
   - runtime path remains non-fatal

5. Rerun:
   /usr/bin/python3 -m py_compile scripts/runtime_final_evidence.py scripts/source_resonance_shadow.py ...
   /usr/bin/python3 manual invariants or pytest if available
   runtime_final_evidence.py export
   export-runtime-final-emitters-window.js --runtime-final-export

6. Write §15.34 STATUS:
   - covered_before
   - covered_after
   - moved modules
   - still blocked modules
   - exact reason for any module not moved
```

### Guardrails
```text
Do not:
  - alter gmgn policy decisions
  - alter source_resonance candidate selection
  - change entry/exit/position size
  - open modes
  - emit fake rows for missing identity
  - cover worker_health from cross-window or stale global state
  - claim EV

Writer behavior:
  - append-only
  - disabled unless RUNTIME_FINAL_EVIDENCE_LOG is set
  - non-fatal in runtime path
```

### Acceptance criteria
```text
Goal 12 is complete only if:

1. gmgn_policy evidence emission is wired or explicitly proven impossible without identity at the honest call site.
2. worker_health evidence emission is wired or explicitly kept FINAL because only global/stale evidence exists.
3. Tests/manual invariants prove valid rows emit and invalid rows do not cover.
4. Goal 10 exporter is rerun with the runtime-final export.
5. Any newly covered module has same-window HIGH/MEDIUM proof.
6. Still-blocked modules retain exact FINAL reasons.
7. No strategy/live/gate/entry/exit/size/mode behavior changes.
8. EV remains fail-closed.
9. §15.34 records exact movement.
```

### Decision after Goal 12
```text
If gmgn_policy and worker_health become covered:
  Internal runtime blockers reduce from 5 remaining after source_resonance instrumentation to 3 governance/manifest
  blockers: training_manifest, holdout_negative_controls, assumptions_false_negative_budget.

If neither moves:
  Stop wiring runtime code until the runtime owner provides identity-bearing evidence.

Next likely goal:
  Goal 13 = governance/manifest emitters OR Goal 12b = external smart_money/curve_pumpfun, depending on which owner
  can provide real data first.
```

```text
GOAL_12_READY_TO_EXECUTE (§15.33 gmgn_policy + worker_health runtime emitters)
TARGET: wire two honest internal runtime evidence emitters using existing runtime state
NON_TARGET: strategy changes, mode opening, external smart/curve exports, EV claims
GUARDRAILS: append-only, disabled by default, non-fatal, same-window proof only
SUCCESS: gmgn_policy/worker_health covered with proof or remain FINAL with exact reason; Goal10 rerun records movement
```

---

## §15.34 STATUS — Goal 12: gmgn_policy + worker_health emitters wired, current window fail-closed (2026-06-22)

Implemented §15.33. The shared `runtime_final_evidence` writer from Goal 11 now has two additional honest runtime
paths:

```text
gmgn_policy:
  scripts/gmgn_policy.py accepts optional evidence_identity and emits module_group=gmgn_policy without changing the
  returned policy object.

worker_health:
  scripts/source_resonance_shadow.py emits module_group=worker_health per candidate write, using the source_resonance
  worker heartbeat/provider/error state.
```

### Code changes
```text
sentiment-arbitrage-system/scripts/gmgn_policy.py
  - added GMGN_POLICY_VERSION = gmgn_paper_policy.v1
  - added emit_gmgn_policy_evidence(policy, identity)
  - evaluate_gmgn_lotto_policy(..., evidence_identity=None) now emits best-effort evidence only when identity exists
  - policy return value is unchanged

sentiment-arbitrage-system/scripts/paper_trade_monitor.py
  - passes evidence_identity at four existing gmgn_policy call sites where token_ca + signal_ts are already available

sentiment-arbitrage-system/scripts/source_resonance_shadow.py
  - emits worker_health alongside source_resonance candidate evidence

sentiment-arbitrage-system/test_runtime_final_evidence.py
  - extended tests for gmgn_policy and worker_health evidence
```

### Runtime behavior
```text
No evidence is written unless:
  RUNTIME_FINAL_EVIDENCE_LOG is set

If set, writes are:
  append-only
  non-fatal
  required-field validated
  label-free

If token_ca or signal_ts is missing:
  gmgn_policy evidence is not emitted.

If worker_health only has stale/global evidence:
  it does not cover. The wired source_resonance path emits per candidate instead.
```

### Current same-window rerun
```text
Current historical runtime raw log still has no same-window rows:
  /Users/boliu/sentiment-arbitrage-system/data/runtime_final_evidence.jsonl

Export chain:
  /usr/bin/python3 scripts/runtime_final_evidence.py export ...
  node scripts/export-runtime-final-emitters-window.js --runtime-final-export ...

Result:
  exported = 0
  joined_n = 0
  covered_modules = []
  still all 6 runtime modules FINAL in runtime-final-emitter-window-goal12/

Movement:
  covered_before = 44
  covered_after  = 44
  moved          = 0
  reason         = no runtime raw evidence exists yet for this historical same-window pack

Closure:
  60 = 44 covered / 8 blocked / 8 intentionally_excluded
```

### Verification
```text
/usr/bin/python3 -m py_compile \
  scripts/runtime_final_evidence.py \
  scripts/source_resonance_shadow.py \
  scripts/gmgn_policy.py \
  scripts/paper_trade_monitor.py
  => OK

Manual invariant with /usr/bin/python3:
  - source_resonance valid payload writes and exports
  - gmgn_policy valid payload writes and exports
  - gmgn_policy without identity does not emit
  - worker_health missing required fields fail
  - source_resonance_shadow.upsert_candidate emits both source_resonance and worker_health
  => OK

Goal 10 exporter regression:
  node --check scripts/export-runtime-final-emitters-window.js
  node --test tests/export-runtime-final-emitters-window.test.mjs
  => 3/3 pass
```

### Acceptance status
```text
1. gmgn_policy evidence emission wired: PASS
2. worker_health evidence emission wired: PASS
3. valid rows emit and invalid rows do not cover: PASS
4. Goal 10 exporter rerun: PASS
5. newly covered modules require proof: PASS (none covered in current historical window)
6. still-blocked modules retain FINAL reasons: PASS
7. no strategy/live/gate/entry/exit/size/mode behavior changed: PASS
8. EV remains fail-closed: PASS
9. exact movement recorded: PASS
```

### Remaining work
```text
To see movement, deploy/run with:
  RUNTIME_FINAL_EVIDENCE_LOG=/app/data/runtime_final_evidence.jsonl

Expected fresh-window movement after runtime runs:
  source_resonance -> covered
  gmgn_policy      -> covered for candidates whose policy was evaluated with token_ca + signal_ts
  worker_health    -> covered for candidates written by source_resonance_shadow

Remaining runtime blockers after fresh evidence should be the governance/manifest set:
  training_manifest
  holdout_negative_controls
  assumptions_false_negative_budget

External remains separate:
  smart_money
  curve_pumpfun
```

```text
GOAL_12_COMPLETE (§15.34 gmgn_policy + worker_health runtime emitters)
GMGN_POLICY_WIRED | WORKER_HEALTH_WIRED | EXPORT_CHAIN_RERUN | CURRENT_WINDOW_ZERO_ROWS_FAIL_CLOSED
CLOSURE_STABLE_44_8_8 | EV_FAIL_CLOSED | NO_STRATEGY_CHANGE | NO_LIVE_CALL
```

---

## §15.35 NEXT GOAL — Goal 13: overnight runtime evidence ingest gate

### Why this exists
The overnight run is only useful after its artifacts are pulled into the audit chain.

Local check after the overnight run found no usable local overnight evidence:

```text
missing:
  sentiment-arbitrage-system/data/runtime_final_evidence.jsonl
  sentiment-arbitrage-system/data/source-resonance.log

latest local fullnet pack:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z

local paper_trades.db:
  May 6 old file, not an overnight trading ledger
```

So Goal 13 is not a strategy change. It is an ingest gate:

```text
remote/runtime artifacts -> existing runtime_final_evidence export -> existing fullnet join -> closure/EV verdict
```

### Scope
Research-only.

Allowed:

```text
1. Pull or accept copied overnight artifacts from runtime/Zeabur.
2. Verify RUNTIME_FINAL_EVIDENCE_LOG was enabled during the run.
3. Run existing Goal 10-12 exporters against the overnight window.
4. Produce a fresh readout showing whether runtime evidence joined.
```

Not allowed:

```text
1. No strategy threshold changes.
2. No entry/mode promotion.
3. No live execution changes.
4. No FBR/RED-bar/Stage2A edge claims before entered/fill/exit evidence exists.
5. No synthetic rows to make modules look covered.
```

### Required inputs
At minimum, collect these from the overnight runtime environment:

```text
/app/data/runtime_final_evidence.jsonl
paper trades / ledger DB for the same window
premium/raw signal snapshot for the same window
source resonance / GMGN runtime artifacts if present
deployment env proof showing:
  RUNTIME_FINAL_EVIDENCE_LOG=/app/data/runtime_final_evidence.jsonl
```

If `runtime_final_evidence.jsonl` does not exist remotely, Goal 13 exits with:

```text
OVERNIGHT_EVIDENCE_NOT_ENABLED
```

and the next action is only:

```text
set RUNTIME_FINAL_EVIDENCE_LOG
restart runtime
rerun a fresh observation window
```

### Execution steps

Use existing scripts. Do not create a new framework.

```bash
cd /Users/boliu/sas-research

# 1. Put copied runtime evidence in the evidence pack.
mkdir -p /Users/boliu/sas-data-room/fullnet-evidence-pack-overnight/runtime-ingest

# 2. Export runtime final evidence for the exact overnight window.
/usr/bin/python3 /Users/boliu/sentiment-arbitrage-system/scripts/runtime_final_evidence.py export \
  --raw-log /Users/boliu/sas-data-room/fullnet-evidence-pack-overnight/runtime-ingest/runtime_final_evidence.jsonl \
  --fullnet-row /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z/fullnet-row-v2-final-blockers/row.jsonl \
  --window-start-ts <OVERNIGHT_WINDOW_START_SEC> \
  --window-end-ts <OVERNIGHT_WINDOW_END_SEC> \
  --out /Users/boliu/sas-data-room/fullnet-evidence-pack-overnight/runtime-ingest/runtime-final-export.jsonl

# 3. Join back into the fullnet runtime emitter layer.
node scripts/export-runtime-final-emitters-window.js \
  --pack-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z \
  --runtime-final-export /Users/boliu/sas-data-room/fullnet-evidence-pack-overnight/runtime-ingest/runtime-final-export.jsonl \
  --out-dir /Users/boliu/sas-data-room/fullnet-evidence-pack-overnight/runtime-final-emitter-window
```

If a fresh fullnet pack for the overnight window exists, use that pack instead of the 20260621 historical pack.

### Required readout

Goal 13 must report:

```text
runtime_final_raw_rows
runtime_final_exported_rows
joined_rows
covered_modules_before
covered_modules_after
still_blocked_modules
entered_count
ledger_count
exit_count
friction_count
actual_ev_eligible_count
actual_net_ev
```

### Decision rule

```text
Case A: no remote runtime_final_evidence.jsonl
  verdict = EVIDENCE_LOG_NOT_ENABLED
  action  = set env + rerun; do not analyze strategy

Case B: raw rows exist, joined_rows = 0
  verdict = IDENTITY_OR_WINDOW_JOIN_BROKEN
  action  = fix token_ca/signal_ts/premium_signal_id/window alignment

Case C: joined_rows > 0, entered_count = 0
  verdict = RUNTIME_OBSERVABILITY_MOVED_BUT_EV_STILL_BLOCKED
  action  = update module closure only; no edge claims

Case D: joined_rows > 0 and entered/fill/ledger/exit/friction all exist
  verdict = EV_ANALYSIS_ALLOWED
  action  = then run entry-mode/FBR/RED-bar/Stage2A effectiveness analysis
```

### Acceptance criteria

```text
1. Overnight artifact presence is explicitly proven or explicitly missing.
2. Same-window boundary is recorded.
3. Runtime evidence export is rerunnable.
4. Join count is reported.
5. Closure movement is reported, even if movement is zero.
6. EV remains null unless entered+fill+ledger+exit+friction are all valid.
7. No strategy/live/gate/entry/exit/size/mode files are modified.
```

```text
GOAL_13_READY_TO_EXECUTE (§15.35 overnight runtime evidence ingest gate)
OVERNIGHT_ARTIFACT_FIRST | EXPORT_THEN_JOIN | NO_STRATEGY_CHANGE | EV_FAIL_CLOSED_UNLESS_LEDGER_COMPLETE
```

---

## §15.36 STATUS — Goal 13 local preflight: overnight artifact not present locally (2026-06-23)

Goal 13 was started with the local artifacts available after the overnight run.

### Local evidence check

```text
runtime_final_evidence_exists:
  false
  checked: sentiment-arbitrage-system/data/runtime_final_evidence.jsonl

overnight_pack_exists:
  false
  checked: sas-data-room/fullnet-evidence-pack-overnight

latest usable local fullnet pack:
  sas-data-room/fullnet-evidence-pack-fresh-20260621T123643Z

local paper_trades.db:
  sentiment-arbitrage-system/data/paper_trades.db
  mtime: May 6
  verdict: stale, not overnight ledger evidence
```

### Config check

Local deploy/start files did not contain `RUNTIME_FINAL_EVIDENCE_LOG`.

Code confirms the writer is disabled unless the env var is set:

```text
scripts/runtime_final_evidence.py:
  Disabled unless RUNTIME_FINAL_EVIDENCE_LOG is set.
  missing env -> runtime_final_evidence_log_not_configured
```

### Smoke artifact check

The only 2026-06-22 local data-room artifact is:

```text
sas-data-room/fullnet-multiday-smoke-20260622T000000Z/live-fullnet-multiday-summary.json
```

It is not overnight runtime evidence. It references the old 2026-06-21 pack and remains fail-closed:

```text
generated_from_n_days = 1
total_signals = 329
dog_n = 29
entered_total_n = 0
phase5_verdict = ENTRY_BRIDGE_GAP
multi_day_verdict = INSUFFICIENT_DAYS
```

### Verdict

```text
GOAL_13_CASE = A
verdict      = EVIDENCE_LOG_NOT_ENABLED_OR_NOT_SYNCED
closure      = unchanged
EV           = null / blocked
strategy     = no analysis allowed
```

This is not evidence that the strategy failed overnight. It means the overnight run is not yet connected to the audit chain.

### Next action

Do one of the following, in this order:

```text
1. If the run was on Zeabur/remote:
   pull /app/data/runtime_final_evidence.jsonl and same-window DB/snapshot artifacts, then rerun §15.35.

2. If the file does not exist remotely:
   set RUNTIME_FINAL_EVIDENCE_LOG=/app/data/runtime_final_evidence.jsonl
   restart runtime
   rerun a fresh observation window

3. Only after joined_rows > 0:
   update module closure.

4. Only after entered+fill+ledger+exit+friction exist:
   run EV / entry-mode / FBR / RED-bar / Stage2A effectiveness analysis.
```

```text
GOAL_13_PREFLIGHT_COMPLETE (§15.36)
LOCAL_OVERNIGHT_ARTIFACT_ABSENT | ENV_NOT_CONFIGURED_LOCALLY | SMOKE_IS_OLD_1DAY_SUMMARY
NO_STRATEGY_ANALYSIS | EV_FAIL_CLOSED | NEXT_REMOTE_PULL_OR_ENABLE_ENV
```

---

## §15.37 STATUS — Goal 14: enable runtime final evidence log by default in Zeabur startup (2026-06-23)

Goal 13 showed the overnight run could not be audited locally because `runtime_final_evidence.jsonl` was absent and the
runtime evidence writer is disabled unless `RUNTIME_FINAL_EVIDENCE_LOG` is set.

Goal 14 applies the smallest runtime-side fix: set a default evidence-log path in the Zeabur startup wrapper.

### Code change

```text
sentiment-arbitrage-system/scripts/run_zeabur_services.sh
  after mkdir -p /app/data /app/logs:
    RUNTIME_FINAL_EVIDENCE_LOG defaults to /app/data/runtime_final_evidence.jsonl
    env var is exported for child processes
    startup prints the active evidence log path

sentiment-arbitrage-system/.env.example
  documents:
    RUNTIME_FINAL_EVIDENCE_LOG=./data/runtime_final_evidence.jsonl
```

### Why this is safe

```text
1. No strategy threshold changed.
2. No entry/mode/exit/size behavior changed.
3. No live trading path changed.
4. The existing writer remains append-only and non-fatal.
5. User-provided RUNTIME_FINAL_EVIDENCE_LOG still wins; startup only fills the default if unset.
```

### Verification

```text
bash -n scripts/run_zeabur_services.sh
  PASS

grep confirms:
  scripts/run_zeabur_services.sh exports RUNTIME_FINAL_EVIDENCE_LOG
  .env.example documents RUNTIME_FINAL_EVIDENCE_LOG
```

### Next run expectation

After redeploy/restart, the runtime should write:

```text
/app/data/runtime_final_evidence.jsonl
```

Then rerun §15.35 Goal 13 ingest:

```text
runtime_final_evidence.jsonl -> runtime_final_evidence.py export -> export-runtime-final-emitters-window.js -> closure readout
```

### Acceptance status

```text
GOAL_14_COMPLETE (§15.37)
RUNTIME_FINAL_EVIDENCE_LOG_DEFAULTED | ZEABUR_CHILD_PROCESSES_INHERIT_ENV
NO_STRATEGY_CHANGE | NO_LIVE_EXECUTION_CHANGE | NEXT_RERUN_AND_INGEST
```
