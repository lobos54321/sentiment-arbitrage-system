# 目标驱动控制系统 —— 完整解决方案基线

> 日期: 2026-06-09
> 状态: 基线 / "当前状态 + 前进序列" 的唯一参照
> 当前 main: `e4f87210 Prefer indexed raw path backfill before Helius`(worktree branch: runtime-stability-marker-guard)
> 一句话: **这不是"一个能打出 60/60/200 的策略",而是一套测量优先、目标驱动的交易控制系统——它端到端诚实测量自己的"抓狗漏斗",逐层定位唯一瓶颈、修它,只在被证明有 edge 处投真钱。60/60/200 是它测出来的一条前沿,不是硬凑的三个数。**

---

## 0. 这套"解决方案"到底是什么(先定义,避免误解)

- 不是: 一个圣杯策略 / 一组能同时满足 60%胜率 + 60%金银狗捕获 + 200%ROI 的参数。
- 是: 一套**能诚实回答"市场有多少 quote-clean 金银狗、我看见几只、买得到几只、进了几只、拿住几只"的测量+控制系统**;在每一层定位唯一主要矛盾、修复、验证;只在被测量证明有 edge 的地方投极小仓。
- 目标 60/60/200 = 这套系统**测出来的一条 precision/recall/ROI 前沿** + 由 controller 选的一个 operating point。**很可能"三个只能要两个"。**

业务目标常量(`config/strategy-goal.yaml`): `target_realized_win_rate=0.60`, `target_capture_rate_gold_silver=0.60`, `target_strategy_bucket_roi=2.00`(bucket ROI,非每天), gold=+100%, silver=+50%, 单笔最大亏损 20%, 仓位 0.001–0.003 SOL。

---

## 1. 核心模型: 三层漏斗(整个系统围绕它)

```
市场里存在狗
→ raw sustained dog            ← 第1层 Raw Discovery: 信号后真涨到 +50/+100,且"持续"非"插针"
→ has_decision_record?         ← 管线评估过它吗(没有 = pipeline coverage 缺口,和"被挡"是两回事)
→ quote_clean / executable?    ← 第2层 Executable Capture: 当时可不可成交
→ block_cause INFRA/MARKET/POLICY  ← 被挡的话谁挡的
→ would_enter → entered        ← 策略判定 + 真入场
→ held_to_silver/gold          ← 第3层 Hold Capture: 拿住到 +50/+100,没卖早
```

三本账: **Raw Discovery(看得见吗) × Executable Capture(买得到吗) × Hold Capture(拿得住吗)。**
端到端捕获 = 三者相乘。**关键数学: 要 60% 端到端,每层得 ~84%(0.6³ = 22% 那盆冷水)。**

口径定案: **统一用 rolling-24h**(对齐业务目标 + matured 占比更高 + 狗样本更多)。6h 只保留为短期 debug 工具,不作复盘主口径。

---

## 2. 支撑漏斗的三根支柱

### ① 可信测量底座(已建成,刚打赢"会失忆"那一仗)
- `raw_signal_outcomes.db` / `raw_price_bars_1m`: 独立 durable 库,不随 paper DB 失忆;
- indexed-first 价格路径(已上线 `e4f87210`): Gecko/GMGN 主力,Helius/on-chain 兜底,带 budget/backoff/cache/inflight 去重;
- 右删失纪律(只用 matured 信号,signal_ts ≤ now−120m);
- baseline 同源/置信度(provider+pool+unit+source_kind 一致才进主分母;按 lag 分置信);
- wick vs sustained 区分(插针不算真狗);price-unit / outlier 守卫;
- 诚实可观测: fresh / stale / required 三态分开,过期快照不冒充当前(masking 洞已闭);
- runtime safety: 损坏自动 quarantine+重建、marker 守卫(fail-closed)、`PRAGMA mmap_size=0`、JSONL 裁剪 → 断了"重启→损坏→死循环"。

### ② 控制闭环(已建,大多 shadow/audit,等测量证明后才逐步 enforce)
- `final_entry_contract`(fail-closed 统一硬门): 可成交 quote / route / liq / spread / security red / 同-lifecycle 曝险锁 / RR≥2 / -20% defined risk / mode_state / budget;
- `executable_sol_valuation`: 唯一估值原语,所有人共用;
- `canonical_trade_ledger`: 唯一 SOL 事实源,只认 realized SOL,不认 mark peak;
- -20% **realized** 击穿 → `loss_cap_breach` → circuit breaker → mode 降级回 shadow(reaction 已闭,且 mode_state 已喂进 entry 合约);
- goal controller: rolling-24h 判 win/recall/ROI/loss → mode 升降级。

### ③ 安全 / 纪律护栏
hard safety gate 永不绕过;AI 只做 advisory、不触发交易、不覆盖硬门;shadow-first;一次只动一个变量;不按 mark 盈亏加仓。

---

## 3. 五条不可动摇的原则(贯穿全程)

1. **先测量,再优化** —— 脏数据/失忆 DB 上不下任何策略结论;
2. **一次只攻一个主要矛盾**(矛盾排序) —— 修上游失效项,别同时摊十层;
3. **证实,不推测** —— 用一手数据;green dashboard / HTTP 200 ≠ 真相(被瞬时 502、假绿、快照 masking 坑过多次);
4. **造了就接线** —— 能力必须在真实路径上,否则是认知负担 + 漂移面("built-but-not-wired"是本项目反复出现的头号病根: canonical ledger 曾只 init、controller 曾 advisory、mode_state 曾没传参、P1-A observer 曾没合并、韧性脚本曾不是 PID1、连这份文档第一版都写错了 worktree);
5. **目标是可测前沿 + operating point,不是三个硬凑的数;24h 在线 ≠ 24h 有 edge**(无行情空仓是对的)。

---

## 4. 当前已验证状态(2026-06-09 快照, main @ e4f87210)

| 维度 | 状态 | 证据 |
|---|---|---|
| 运行底座 | 🟢 绿 | uptime ~13.3h 不重启;最近损坏仍 `2026-06-08T00:30:51Z`,之后零新损坏(~25h+ 干净);paper DB ok、~1.33GB 增长、无 integrity marker;signal freshness ok 0min;review snapshot fresh |
| Raw Discovery | 🟡🟢 可用 | 24h: sustained gold/silver **16 只**(gold 9 / silver 7)、eligible_event_rows **105**(过 50-100 门槛)、coverage **~34%**、early_15m ~68% |
| 价格源 | indexed-first 扛住 | 全 indexed_ohlcv(gecko ~3400 / gmgn ~360),bonding_curve=0,amm_pool=0;Helius 不再卡死(backoff/fallback 有效) |
| 第二层漏斗 | ⏳ 即将启动 | entered=0 / realized=0 —— **现在不能读成失败,因为第二层还没 JOIN** |
| 策略 / edge | ❌ 仍不可判 | 要等第二层拆完才有数据支撑的答案 |

**黄灯(只盯不动手):** ① coverage ~34% 掉(信号流量涨得比填补快,`no_raw_path_for_token` / `raw_path_after_early_window` 持续扩大才上 on-chain);② provider 偶发 rate_limited=1(fallback 仍有效);③ wick∶sustained ≈ 4:1(62:16,sourcing 偏插针的苗头,但要等 executable 证据才判)。

---

## 5. 完整前进序列(从这里到终态)

### A. 第二层(现在,只读 JOIN)
- `raw_signal_outcomes` LEFT JOIN `opportunity_events` / `a_class_decision_events`;
- join key 优先 `lifecycle_id`,否则 `token_ca + signal_ts` 有界窗口(一个 token 可能多次信号,要匹配对的那次);
- **按决策时刻匹配,不现查 quote**;复用已有 `block_cause`;**不新建一套执行观测系统**;
- **第一刀只回答一个问题: 16 只 sustained raw 狗里,几只当时进过 decision record?**(LEFT JOIN,未匹配的归 `no_decision_record` 单独成桶——这个桶大小直接决定下一个主矛盾)
- 16 只只够看**主导桶**(方向),不够给精确百分比。

### B. 决策树(第二层结果 → 下一个主矛盾)
- 多 `no_decision_record` → **管线没评估 raw observer 看见的信号**(pipeline coverage 缺口)→ 修信号→决策接入;
- 多有记录但 `not_quote_clean` → **可成交性**(provider/route/liq)→ 修证据/hydration;
- 多 quote-clean 但 `would_enter=0` → **入场 gate 太严**(matrix/RR/freshness/Markov)→ 校准;
- `would_enter` 但 `entered=0` → **enqueue/canary/governance 链路**;
- `entered` 但没 `held` → **退出/持有**(8%–50% 死亡谷、breakeven 太早 +8%、moonbag 太晚)→ 重构 exit。

### C. 逐层推进
每修完一层、样本攒够,再读下一层。**全程只读/shadow,直到某层被证明是真瓶颈且修复被验证。**

### D. 画前沿 + 选 operating point
三层都可测后,第一次能画出 precision/recall/ROI 前沿 → goal controller 选 operating point(大概率"三个只能要两个")。

### E. tiny live(最后)
仅在前沿可见 + operating point 选定 + 安全合约就位后,放 0.001 SOL tiny live(quote-clean / RR≥2 / 无 hard red / mode 未熔断 / daily budget / concurrent=1),只认 canonical ledger 复盘。

### F. 贯穿
de-bloat(冻结未验证层=可证明静默、清 clutter、**自声明** runtime map 不手写)+ 研究面与生产面在结构上隔离(研究 churn 不能搞垮执行)。

---

## 6. 还欠的债(底座绿,但有结构债,趁稳定时还)

1. **Zeabur PID1 仍是旧 inline 入口**(Node 侧 marker 守卫扛住了,不紧急;但要修对,否则 `run_zeabur_services.sh` 那套韧性不是真在跑——修时注意别让 maintenance loop 双跑;observer 若接 index.js,用独立子进程 spawn,别 inline 进事件循环);
2. **paper DB 分库**: `canonical_ledger` + `a_class_mode_runtime_state`(熔断状态)还在会被重置的 `paper_trades.db` → 一次损坏会同时清掉账本 + 安全刹车。分库 = 损坏可生还 + 闭合"熔断状态随 DB 一起丢"的安全洞;
3. **单写者纪律**(损坏根因: 并发写)。 (review snapshot worker 已是 `mode=ro` + `PRAGMA query_only=ON` ✓)
4. coverage / provider 黄灯**持续扩大才**上 on-chain swap 重建 / pump.fun bonding-curve 流(现在 indexed 在扛,提前上=又叠层)。

---

## 7. 终态:"解决"长什么样

不是"系统在赚钱"。是:
> **系统能端到端、诚实地回答"有多少 quote-clean 金银狗 / 看见几只 / 买得到几只 / 进了几只 / 拿住几只",由 controller 在被测量的前沿上选一个 operating point,只在有验证 edge 处投极小仓。**

到那时,"为什么没抓到金银狗"会有一个**分层、数据支撑**的答案(漏在 observability / pipeline / executable / gate / hold 哪一环),而不是猜测;60/60/200 会变成"前沿上能不能同时达到"的可计算问题——答案很可能是"不能全要,选两个"。

---

## 附: 关键文件索引
- 漏斗/观测: `src/analytics/raw-signal-outcomes.js`, `src/analytics/raw-path-observer.js`, `scripts/run-raw-path-observer.js`(P1-A,**已合并上线**), `scripts/a_class_block_cause.py`, `scripts/a_class_expected_rr.py`
- 控制/安全: `scripts/final_entry_contract.py`, `scripts/executable_sol_valuation.py`, `scripts/canonical_ledger.py`, `scripts/a_class_runtime_safety.py`, `scripts/strategy_goal_controller.py`
- 运行底座: `scripts/run_zeabur_services.sh`, `src/index.js`, `scripts/zeabur_preflight_cleanup.py`, `scripts/paper_review_snapshot_worker.py`(readonly + query_only)
- API/复盘: `/api/paper/raw-dog-discovery?hours=24`(主口径), `/api/paper/incident-artifacts`(损坏检查), `/health`, `/api/paper/storage-health`
- 相关记忆: `sas-strategy-diagnosis`(60/60/200 张力 + 退出引擎瓶颈 + 量化框架)
