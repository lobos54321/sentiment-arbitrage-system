# Raw Dog 隔离 Audit 规格

> 日期: 2026-06-09  
> 状态: Spec / 不进生产热路径  
> 依赖基线: `claudedocs/goal-driven-control-solution-baseline.md`  
> 一句话: **这个 audit 只回答一个问题: `quote_clean_no_would_enter` 里的 raw 狗,在决策时刻能不能和同样被 gate 拒掉的 quote-clean dud 区分开。它必须隔离运行,产出静态报告,绝不进入 `/health`、dashboard 实时请求、raw observer 或交易路径。**

---

## 0. 背景和当前事实

轻量第二层 JOIN 已经在线跑通,当前 rolling-24h 漏斗显示:

```text
raw sustained dogs: 19
no_decision_record: 3
quote_clean: 14
would_enter: 2

terminal buckets:
  quote_clean_no_would_enter: 12
  would_enter_not_entered: 2
  no_decision_record: 3
  not_quote_clean_MARKET: 1
  not_quote_clean_INFRA: 1
```

这说明当前主矛盾不是 raw path、pipeline coverage、provider/route,而是:

```text
能买的狗,为什么我们的入场 gate 没让它们进入 would_enter?
```

但不能直接放松 gate。事后是狗,不代表决策时刻可区分。下一刀必须只读验证:

```text
这批 quote-clean/no-would-enter 的狗,在决策时刻是否有 ex-ante 特征,
能和同窗口 quote-clean/no-would-enter 的 dud 分开?
```

---

## 1. 非目标

这个 audit 不做以下事情:

- 不修改策略参数、gate、RR、matrix、exit、mode 状态;
- 不触发交易、不 enqueue、不写 `paper_trades.db`;
- 不加入 `/health`、raw dog observer、dashboard 实时 endpoint 的现算路径;
- 不作为每次请求实时 JOIN 的 API;
- 不把第一次 12-20 只狗的结果读成精确统计结论。

---

## 2. 运行边界

必须满足以下硬约束:

1. **独立进程**  
   以 CLI/job 方式运行,例如:
   ```bash
   node scripts/run-raw-dog-decision-audit.js --hours 24 --max-duds 200
   ```
   不 inline 到 `src/index.js`、`dashboard-server.js`、observer interval 或 health server 的事件循环。

2. **只读连接**  
   所有 SQLite 连接必须:
   ```sql
   PRAGMA query_only = ON;
   PRAGMA mmap_size = 0;
   PRAGMA busy_timeout = 1500;
   ```
   用 `readonly` 打开 live DB;如果 live DB 竞争明显,改读 materialized export / snapshot,不要压 live writer。

3. **有界工作量**  
   默认窗口 `hours=24`;最大 `hours<=72`;默认 `max_duds=200`;最大 `max_duds<=1000`;token chunk 有上限;全局 timeout 有上限。

4. **物化报告**  
   audit 运行产物写静态文件,例如:
   ```text
   data/audits/raw-dog-decision/latest.json
   data/audits/raw-dog-decision/latest.md
   data/audits/raw-dog-decision/raw_dog_decision_audit_YYYYMMDD_HHMMSS.json
   ```
   dashboard 只能读这些静态报告,不能现算。

5. **失败不影响生产**  
   audit 失败只写 `status=failed` 报告或退出非零;不能影响 dashboard/health/worker/runtime safety。

---

## 3. 输入数据

### 3.1 Raw dog 分母

来源: `raw_signal_outcomes.db.raw_signal_outcomes`

主分母条件必须复用现有 raw denominator 口径:

```sql
observation_status = 'matured'
AND kline_covered = 1
AND baseline_confidence IN ('high', 'medium')
AND same_source_path = 1
AND outlier_flag = 0
AND sustained_evaluable = 1
```

raw 狗:

```sql
raw_primary_tier IN ('gold', 'silver')
```

dud 对照组:

```sql
raw_primary_tier NOT IN ('gold', 'silver')
```

dud 必须来自同一个窗口、同一个 raw denominator,并抽样/限量。默认按 `signal_ts DESC` 取最近 `max_duds`,以后可加固定随机 seed 采样。

### 3.2 决策时刻证据

来源:

- `paper_trades.db.opportunity_events`
- `paper_trades.db.a_class_decision_events`

JOIN 规则:

1. 优先 `lifecycle_id`;
2. fallback: `token_ca + signal_ts` 有界窗口;
3. 时间窗口:
   ```text
   signal_ts - 60s  到  min(signal_ts + 900s, signal_ts + time_to_sustained_peak_sec)
   ```
4. 只用决策时刻已落库字段,不现查 quote。

### 3.3 Ex-ante 特征

第一版只需要最小有效特征:

- gate/reject reason:
  - `hard_blockers_json`
  - `source_component`
  - `source_reason`
  - `reason`
  - `score` / matrix score bucket
  - `expected_rr` bucket
- entry volume:
  - 同源 raw path 第一根 1m bar 的 `volume`
  - signal 后 5m volume
  - signal 后 15m volume
- 可选但不作为第一版 blocker:
  - smart-money count / buy pressure,如果已在 `candidate_json` 或 `raw_payload_json` 里存在;
  - liquidity / spread,仅作上下文,不拿来替代 entry volume。

必须标注 feature coverage:

```text
entry_bar_volume_observed_n / group_n
smart_money_observed_n / group_n
```

缺字段 = `feature_unavailable`,不能当 0。

---

## 4. 输出报告

### 4.1 顶层 schema

```json
{
  "schema_version": "raw_dog_decision_audit.v1",
  "generated_at": "ISO8601",
  "window": {
    "hours": 24,
    "since_ts": 0,
    "until_ts": 0
  },
  "status": "ok | partial | failed",
  "inputs": {
    "raw_db_path": "...",
    "paper_db_path": "...",
    "raw_dogs_n": 0,
    "dud_candidates_n": 0,
    "dud_sample_n": 0
  },
  "funnel": {
    "raw_sustained_dogs": 0,
    "no_decision_record": 0,
    "quote_clean": 0,
    "quote_clean_no_would_enter": 0
  },
  "quote_clean_no_would_enter_audit": {
    "raw_dogs_n": 0,
    "comparison_duds_n": 0,
    "gate_reason_counts": {},
    "score_bands": {},
    "expected_rr_bands": {},
    "entry_volume": {},
    "smart_money": {},
    "dog_rows": [],
    "dud_summary": {}
  },
  "interpretation": {
    "dominant_observation": "...",
    "next_main_contradiction": "gate_too_strict | no_ex_ante_separation | evidence_unavailable | sample_too_small",
    "do_not_change_strategy": true
  }
}
```

### 4.2 必须输出的三类结果

1. **这 12/当前 N 只狗被哪个门拒**
   - `gate_reason_counts`
   - per-dog `best_decision_record`
   - `score_band`
   - `expected_rr_band`

2. **这些狗当时的 ex-ante 特征**
   - `entry_bar_volume`
   - `early_5m_volume`
   - `early_15m_volume`
   - optional smart-money features

3. **同窗口 dud 对照**
   - quote-clean/no-would-enter dud 数量;
   - dud 的 entry volume 分布;
   - dog vs dud 的粗方向比较。

---

## 5. 判读规则

只读方向,不读精确百分比。

### A. 狗和 dud 区分不开

迹象:

- dog 的 entry volume 分布和 dud 接近;
- gate reason 分布和 dud 接近;
- smart-money / buy-pressure 等 ex-ante 特征没有明显差异;
- feature coverage 低。

结论:

```text
主矛盾 = 没有可用 ex-ante edge / 特征或 sourcing 问题
动作 = 不放松 gate;继续找更早、更可区分的特征或信号源
```

### B. 狗明显可区分,但被无关 gate 拒掉

迹象:

- dog entry volume 明显高于 dud;
- dog 大量落在 entry-volume Q5 或高分位;
- reject reason 集中在和验证特征无关的门,例如某个 matrix/RR/freshness 过严;
- quote-clean 成立。

结论:

```text
主矛盾 = gate 在错的轴上拒绝高质量狗
动作 = 下一步才设计 shadow-only gate patch;仍先不 live 放松
```

### C. feature coverage 不足

迹象:

- entry volume observed_n 太低;
- smart-money observed_n 太低;
- dud 对照不足。

结论:

```text
主矛盾 = audit evidence unavailable
动作 = 补特征观测链,不调 gate
```

### D. 样本太小

迹象:

- raw_dogs_n < 30 或 quote_clean_no_would_enter raw dogs < 20。

结论:

```text
只读方向,不作策略结论;周期性重跑,等样本长大
```

---

## 6. 实现建议

第一版文件:

```text
scripts/run-raw-dog-decision-audit.js
src/analytics/raw-dog-decision-audit.js
tests/raw-dog-decision-audit.test.mjs
```

职责:

- `scripts/run-raw-dog-decision-audit.js`: CLI 参数、DB readonly 打开、写报告文件;
- `src/analytics/raw-dog-decision-audit.js`: pure functions,可单测;
- 复用现有 `src/analytics/raw-dog-decision-funnel.js` 的匹配逻辑和桶定义;
- 绝不从 dashboard 实时调用。

CLI 参数:

```text
--hours 24
--max-duds 200
--raw-db ./data/raw_signal_outcomes.db
--paper-db ./data/paper_trades.db
--out-dir ./data/audits/raw-dog-decision
--timeout-ms 60000
```

默认只写本地报告;以后若要 dashboard 展示,只读 `latest.json`。

---

## 7. 验收标准

1. 运行 audit 不影响 `/health` latency 和 availability;
2. audit 可在本地/线上一次性运行并在 timeout 内结束;
3. DB 连接全为 readonly + query_only;
4. 输出包含:
   - `quote_clean_no_would_enter` raw dog 列表;
   - gate reason counts;
   - entry volume summary;
   - dud 对照 summary;
   - interpretation guardrail;
5. audit 失败时只生成 failed report 或退出非零,不影响 runtime;
6. 不新增交易路径、不新增 live gate 行为。

---

## 8. 当前纪律

在这个 audit 有至少数个窗口报告之前:

- 不放松 gate;
- 不调 RR/matrix/freshness;
- 不改变 exit;
- 不把 12/19 这种小样本读成精确比例;
- 不把 dog-only 后验特征当作 ex-ante edge。

下一步只允许做:

```text
build isolated read-only audit → run once → inspect direction → wait for more samples
```

