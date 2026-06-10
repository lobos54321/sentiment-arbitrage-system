# 五专家独立审计 — 合并判决(2026-06-10)

> 方式: 5 个互相隔离的只读审计 agent(微观结构/量化/执行/退出/SRE),统一 data room(frozen 快照 `/private/tmp/sas-audit-download/` + repo @ 8bdf1248),统一 memo 格式(瓶颈/证据强度/不可下结论/最小实验/反方结论)。
> 完整 memo 可经 agent 续问: 微观结构 af79db28252529192 / 量化 ab2532befc65f5209 / 执行 ac0e9fd18d26459b8 / 退出 ae5ed210c7e65ba4c / SRE a93c136c92c0b52b0。

## 0. 判决(核心问题: 距 60/60/200 卡在哪层?)

**主瓶颈 = 信号源问题(狗走的是一条 price-only 数据轨道),被两个工程问题放大(入口黑盒 + 证据链会被重建清零)。不是执行问题;策略问题目前不可判;"目标不现实"未被证明——但 exit replay 的方向性 ~20% capture 暗示 60% capture 大概率还需要动 exit 架构。**

五份独立 memo 收敛到同一个上游事实(各自用不同 SQL 独立验证):
- **33 只 frozen sustained 金银狗,0/33 在 raw_price_bars_1m 有任何 gmgn/amm bar;狗相关 522 条 bar 全部来自 gecko 且 volume=0(仅 3 条非零)**(微观结构+量化双验证);
- **external_alpha_snapshots(gmgn 管线,1794 行)对狗 token 0 命中;source_resonance/gmgn_pre_seen 在系统级 2-4% 命中,在狗上 0%**(量化)→ 狗来自 premium_signals(Telegram)→gecko price-only 轨道,**从未进入带 volume/liquidity/smart-money 的轨道**;
- 后果链: 唯一验证过的因子(entry volume)不可观测 → matrix/RR 在垃圾输入上拒人(狗 dud 一起拒,无区分力,干净强证据) → liquidity_unknown 9/10 卡死契约层(是"证据缺失"不是"真没深度") → sustained 判据的 volume_confirmed 分支全库 0 次触发(死代码) → **60/60/200 可行性当前是 unobservable(不可观测),不是 infeasible(不可达)**。

## 1. 主矛盾排序表

| 层级 | 当前状态 | 最大瓶颈 | 证据强度 | 下一步 |
|---|---|---|---|---|
| Raw discovery | 价格维度勉强可用(frozen kline_covered 26.6%);volume 维度对狗 0 覆盖 | 狗 token 从未触达 gmgn/amm 轨道;gecko volume 对狗 100% 失效 | **强**(双独立 SQL) | 实验 A: 狗轨道探测 |
| Executable | 12/14 有报价;但 funnel `quote_clean` 定义分叉污染主桶 | liquidity_unknown 9/10 = INFRA 证据缺失;hydrate outcome 未导出无法拆 INFRA/MARKET | 中-强 | 实验 B: 导出 provider_hydrate_outcome |
| Entry(gate) | matrix/RR 把狗和 dud 一起拒(无区分力) | 无可用 ex-ante 区分器输入("garbage in");**现在校准阈值无意义** | 强(无区分力)/不可判(有无 edge) | 等 A 解锁 volume 轴后重跑 dog-vs-dud |
| Hold/exit | 0 entered dogs;frozen-bars replay(n=21 token)方向性 capture ~20% | 死亡谷(8-50% floor)切早 + moonbag 只接住一半;与 60% capture 结构性张力 | 中(方向,1m 粒度粗) | 实验 C: replay 去重+canary 交叉验证 |
| Runtime | /health ok、零新损坏;**但 PID1 真身未知** | 三条已知入口路径全被代码逻辑排除 → 写 paper DB 的进程启动拓扑是黑盒 → **"f4106309 已验证生效"降级为存疑** | 强(黑盒事实) | 实验 D: /health 加 entrypoint_file 探针 |

## 2. 各专家头条(凝缩)

- **微观结构**: volume 维度"从未在测",不是"测出无效";sustained 标签纯靠 price-shape(volume 分支死代码),~1/4 狗靠最弱判据("没崩=过")通过;+50/+100 可成交性**无法验证**(零量+暴涨形态偏薄池签名);另发现 `pool_found=0` 占 80.7% 疑似字段传递 bug(便宜线索)。
- **量化**: matrix/RR 无区分力的对照设计是干净的(同桶、标签时间分离,无事后偏差),结论站得住;smart-money 因子管线是活的但对狗 0 覆盖 → **sourcing 覆盖问题,非因子工程问题**;60/60/200 三目标并非严格串联相乘(0.6³ 只适用于 capture 这一项;WR/ROI 是另两本账,partial-exit 可桥接),但当前 entered=0 → WR/ROI 数学上 undefined → "目标是否可达"**连输入都不存在**。
- **执行**: **funnel `quote_clean` 与 final_entry_contract 定义分叉是实锤**(10/10 狗 quote_clean=true 同时带 contract 必拒的 hard_blockers)→ "主桶=策略层拒的"这个表面结论被污染;liquidity_usd 的多级 fallback 链全部落空 = 上游 payload 系统性不带该字段;canary size=hydrate size=0.001 SOL(有代表性);**全仓库无任何 fill-vs-quote 偏差测量**;`provider_hydrate_outcome` 字段已存在但没导出到 audit——导出它即可拆 INFRA/MARKET(最高 ROI 的小修)。
- **退出**: 真跑了 frozen-bars replay(close-only、决策时刻正确、n=25 事件/21 token): **realized≥+50% 仅 ~20%、≥+100% 仅 8%**(方向性);死亡谷实证(OLBOS 在 +4.1% 被 floor 切出,600s 后 +55.9% 仍在涨);moonbag 触发 10 例只保住 5 例;40% 被 DOA/hard-SL 早杀但 1m 粒度无法区分真死 vs 对齐伪影;0.001 SOL 下 partial 手续费侵蚀未量化。
- **SRE**: **live PID1 既不是 health-bootstrap.js 也不是 run_zeabur_services.sh**(三条独立证据链: schema_version 字符串溯源 / runtime_role 的 `||=` 写入顺序 / EMBEDDED_DASHBOARD_ENABLED 矛盾)→ 真入口是平台层直接 `node src/index.js` 类调用,黑盒;**repo 内唯一 spawn paper_trade_monitor.py 的地方是 run_zeabur_services.sh(已被排除),但 paper DB 正在被持续写入** → 真实写者的启动路径未知 → f4106309 那次干净重启不能外推到日常重启(管道修复必然没生效);canonical ledger+熔断状态仍与 paper_trades.db 同生共死(分库债确认)。

## 3. 交叉质询发现的冲突(及裁决)

1. **"主桶=入场 gate 拒的" vs "主桶被定义分叉污染"**: 执行专家胜出——那 10 只里多数带 contract 必拒的 hard_blockers,根本没走到"策略选择不进"。裁决: 主矛盾从"gate 校准"上移到"证据缺失(liquidity/volume)",与量化/微观结构的 sourcing 结论合流。**funnel 的 quote_clean 需要改名/重定义(测量修复,非策略修复)。**
2. **wick:sustained 4:1(live 窗口) vs 1.49:1(frozen 全库)**: 未调和的口径差异(窗口/定义),挂起,不承重。
3. **"f4106309 已实测有效"(此前结论) vs SRE 降级**: SRE 胜出——那次干净重启是真的,但生产写者拓扑未知、管道修复没在跑,**不能宣布优雅关闭已在生产常态生效**。
4. **"修好上游就能 60% capture" vs replay ~20%**: 并存——上游(sourcing)是当前主矛盾,但 exit 架构是第二道墙;两者都要过,顺序是先上游。
5. 量化反方提醒: 2 只 would_enter 狗的 entered 记录可能被 06-09 重建清掉(capture 真值可能 >0)——重建对证据链的破坏再次承重。

## 4. 选定实验(≤3,全部 shadow/只读)

- **实验 A「狗轨道探测」(合并微观结构+量化提案;离线只读,现在就能跑)**: 对 33 只 frozen 狗 token: (a) presence——SQL 查 gmgn/external_alpha 层是否曾在 signal±30min 见过它们;(b) touch——用现有 fetchGmgnKlineWindow/helius 对其 signal 窗口拉 OHLCV,数多少能拿到非零 volume bar。**成功(≥1/3 可达): 是接线/优先级问题,修 observer 路由(便宜);失败(<1/5): 数据源覆盖不到这类 token,需链上 swap/bonding-curve 重建(立项级)。** 这决定下一刀是"修接线"还是"换源"。
- **实验 B+D「一次部署两个观测字段」(搭下一次真部署)**: B=把已存在的 `provider_hydrate_outcome/reason` 导出进 audit dog_rows(拆 liquidity_unknown 的 INFRA/MARKET,9/10 占比的最高 ROI 小修);D=/health 加 `entrypoint_file=process.argv[1]` + `npm_lifecycle_event`(一行,终结 PID1 黑盒)。都是只读字段导出,不动任何 gate/写路径。
- **实验 C「replay 扩展」(纯离线,脚本已在 /tmp/sas-replay)**: 去重到 21 独立 token 重跑;对 17 笔已 closed canary 跑同一 replay 与真实 exit_reason 分布对比(校验 close-only 简化有无系统性偏差)。通过则 ~20% 可作 hold-capture 基线。

## 5. 纪律(全员一致)

不动 gate/matrix/RR/exit 参数;不基于 live 滚动窗口下策略结论(frozen pack 为审计基准);在入口黑盒(D)解决前不宣布"底座已稳";样本 n≈10-33 只读方向。

## 5.5 补充实测(2026-06-10 当天,GeckoTerminal multi-token 探针,n=16/64 抽样)

**狗 = pump.fun 临毕业币,实锤。** 64 只 sustained 金银狗中 53 只地址带 `pump` 后缀;16 只抽样对照"信号时间 vs 池子创建时间": **14/16 信号时刻无可用 AMM 池**(10 只在毕业前 5-77 分钟、3 只毕业瞬间 ±1m、2 只永在曲线),仅 1 只(letsbonk→raydium)信号前已有池。明细: `/private/tmp/sas-audit-download/dog_pool_probe_table.md`。

含义(修正/确认前文):
1. volume=0 / liquidity_unknown / amm 轨道 0 触达,**全部是同一个结构性原因: 观测与决策窗口卡在 bonding-curve 阶段,AMM 证据当时不存在**——不是 gecko 故障,也大概率不是 hydrator 预算问题(修正执行 memo 的"INFRA 可修"倾向,待 provider_hydrate_outcome 导出定案);
2. **final_entry_contract 的可成交定义(AMM liquidity/spread)结构性排除曲线期 → 对这批狗 capture≈0 是定义的必然,不是 gate 参数太严**。要 60% capture 就必须能在曲线期形成证据并交易(或显式接受只抓毕业后段+重定义目标);
3. 早期 5-15m 逐bar volume 的真值只可能来自: **GMGN(原生覆盖 pump.fun 曲线)或 Helius 链上解码 pump.fun program**;gecko 曲线期有聚合量(token 级 vol24)但无逐bar量(522/522=0 实测);毕业后 pumpswap 池谁都行——**且 observer 毕业后没重选池(毕业后的 bar 也全 0 量)= 一个便宜的 wiring 修复点**;
4. 2026-06-11 已更新: GMGN touch 已能在本地用 repo 外 key 文件跑通,不需要服务器 shell。源天花板假设被推翻,下一步不是再问"GMGN 有没有量",而是做阶段分层与决策时刻重锚。

## 5.6 补充实测(2026-06-11,GMGN touch + stage-stratified audit)

新增只读审计工具: `scripts/run-gmgn-stage-stratified-audit.js`(本地 commit `2f2dcc18`,未单独部署)。它把 GMGN touch 的 dog/dud 结果按 volume visibility stage 分层,并可输出 decision-ts token 文件。

### A. GMGN 源可达性

- signal-anchor touch: 64/64 dog、216/216 dud 均能从 GMGN 拿到带量历史。
- decision-anchor touch: 41/41 dog、157/157 dud 均能从 GMGN 拿到带量历史。
- 因此"GMGN 源覆盖不到这些狗"不成立。主问题从"源天花板"改为"阶段混杂、锚点、标签质量和曲线期特征"。

### B. signal_ts 锚点的阶段混杂

报告: `/tmp/gmgn-stage-stratified-audit.json`

- dog first nonzero volume lag median = 1089s; dud median = -11s。
- AUC(lag dog > dud) = 0.6786。
- 全量 early_15m volume AUC = 0.3946,但 positive-only AUC = 0.7003。
- 解释: 全量 AUC 反向是 Simpson 悖论/阶段混杂。大量 dog 在 signal_ts 后 15m 内仍处于无 GMGN 逐bar量的暗区,而大量 dud 在 signal_ts 时已出量/已毕业。
- 在 `already_volume_visible_at_anchor` 子集内,early_15m volume AUC = 0.7556;dog median $94,529,dud median $18,664。
- 在 `volume_visible_within_5m` 子集内,early_15m volume AUC = 0.8;样本小(dog=5,dud=5),只读方向。

### C. decision_ts 锚点复测

报告: `/tmp/gmgn-stage-stratified-decision-anchor.json`

- decision matched cohort: 41 dog、157 dud。
- dog first nonzero volume lag median = 1063s; dud median = -11s。
- AUC(lag dog > dud) = 0.7039,比 signal_ts 更强。
- 全量 early_15m volume AUC = 0.4104,positive-only AUC = 0.7451。
- 在 `already_volume_visible_at_anchor` 子集内,early_15m volume AUC = 0.8375;dog median $60,986,dud median $10,349。
- 结论: "狗更常在决策时仍处暗区/临毕业前"不是 signal_ts 假象;锚到 decision_ts 后仍成立且更强。

### D. 标签质量债

- 25/64 dog 的 sustained peak 发生在首个 GMGN 非零 volume bar 之前(`curve_phase_unconfirmed`)。
- 这部分不是自动判假,因为 bonding curve 本身可交易;但当前没有逐笔成交确认,必须用 Helius/pump.fun 解码或 GMGN 历史曲线事件验证。
- 后续 data pack 必须带 `peak_confirmation_tier ∈ {volume_confirmed, curve_phase_unconfirmed}`。

### E. 修正后的高手问题

不再问"GMGN 有没有数据"或"early volume 是否无效"。新的最小问题是:

1. 在 decision_ts 当时,`volume_visibility_stage`/曲线进度/毕业邻近度是否是可用的事前特征?
2. 在同 stage 内,early_5m/early_15m volume、buy pressure、unique buyers 是否能区分 dog/dud?
3. 25/64 暗区峰能否被链上逐笔成交验真?
4. bonding-curve 阶段如果允许交易,final_entry_contract 应如何定义 executable/liq/spread/risk?

## 6. 对 60/60/200 的诚实回答

- **capture 60%**: 三层相乘逻辑适用于它(每层 ~84%);当前不可观测;replay 暗示 exit 架构本身可能把上限压在远低于 60% 的位置 → 大概率要"上游修复 + exit 重设计"两步。
- **WR 60% / ROI 200%**: 与 capture 不是同一本账(partial-exit 可桥接),当前 entered=0 → undefined。
- **结论**: "不现实"未被证明,"可达"更未被证明;它现在是 **unobservable**。让它变 observable 的路径 = 实验 A(volume/sourcing)→ 重跑 dog-vs-dud → 画前沿 → 选 operating point("三个大概率只能要两个"维持不变)。
