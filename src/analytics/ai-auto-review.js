/**
 * AI 自动复盘模块 v2.0
 * 
 * 功能：
 * 1. 对比我们的交易 vs 金狗/银狗的表现
 * 2. 分析哪些因子的判断有问题
 * 3. 调用 AI 生成改进建议
 * 4. 自动调整因子阈值 ← 新增自动应用
 */

import Database from 'better-sqlite3';
import OpenAI from 'openai';
import thresholdConfig from '../utils/threshold-config.js';

class AIAutoReview {
  constructor(dbPath = './data/sentiment_arb.db') {
    this.db = new Database(dbPath);
  }

  /**
   * 执行自动复盘
   * @returns {Object} 复盘报告和建议调整
   */
  async runAutoReview(triggerReason = 'scheduled') {
    console.log('\n🤖 ========== AI 自动复盘开始 ==========\n');

    // 1. 获取交易数据
    const tradeData = this.getTradePerformance();
    const goldDogData = this.getGoldSilverDogs();

    if (tradeData.total < 10) {
      console.log(`[AI Review] 交易数据不足 (${tradeData.total}/10)，暂不复盘`);
      return { status: 'insufficient_data' };
    }

    // 2. 分析问题
    const analysis = this.analyzeProblems(tradeData, goldDogData);

    // 3. 生成 AI 建议
    const aiSuggestions = await this.getAISuggestions(tradeData, goldDogData, analysis);

    // 4. 执行调整
    const adjustments = this.executeAdjustments(aiSuggestions);

    // 5. 保存复盘记录到数据库
    this.saveReviewToDatabase(triggerReason, tradeData, analysis, aiSuggestions, adjustments);

    console.log('\n🤖 ========== AI 自动复盘完成 ==========\n');

    return {
      status: 'completed',
      tradeData,
      goldDogData,
      analysis,
      aiSuggestions,
      adjustments
    };
  }

  /**
   * 保存复盘记录到数据库
   */
  saveReviewToDatabase(triggerReason, tradeData, analysis, aiSuggestions, adjustments) {
    try {
      // 初始化表（如果不存在）
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS ai_review_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          review_time TEXT DEFAULT (datetime('now')),
          trigger_reason TEXT,
          trade_count INTEGER,
          win_rate REAL,
          avg_pnl REAL,
          problems_found TEXT,
          ai_key_insight TEXT,
          ai_root_cause TEXT,
          threshold_adjustments TEXT,
          risk_adjustments TEXT,
          factor_combo TEXT,
          priority_action TEXT,
          status TEXT DEFAULT 'completed'
        );
      `);

      const stmt = this.db.prepare(`
        INSERT INTO ai_review_history (
          trigger_reason, trade_count, win_rate, avg_pnl, 
          problems_found, ai_key_insight, ai_root_cause,
          threshold_adjustments, risk_adjustments, factor_combo, priority_action
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      stmt.run(
        triggerReason,
        tradeData.total,
        tradeData.winRate,
        tradeData.avgPnl,
        JSON.stringify(analysis),
        aiSuggestions.key_insight || null,
        aiSuggestions.root_cause || null,
        JSON.stringify(adjustments),
        JSON.stringify(aiSuggestions.risk_control_suggestions || null),
        aiSuggestions.factor_combo || null,
        aiSuggestions.priority_action || null
      );

      console.log('   💾 复盘记录已保存到数据库');
    } catch (e) {
      console.error('   ⚠️ 保存复盘记录失败:', e.message);
    }
  }

  /**
   * 获取交易表现数据
   */
  getTradePerformance() {
    // 总体统计
    const overall = this.db.prepare(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) as wins,
        AVG(pnl_percent) as avg_pnl,
        MAX(pnl_percent) as best,
        MIN(pnl_percent) as worst
      FROM positions 
      WHERE status = 'closed' AND pnl_percent IS NOT NULL
    `).get();

    // 按因子分组
    const byDogType = this.db.prepare(`
      SELECT 
        fp.dog_type,
        COUNT(*) as count,
        SUM(fp.is_winner) as wins,
        AVG(fp.pnl_percent) as avg_pnl
      FROM factor_performance fp
      WHERE fp.closed_at IS NOT NULL
      GROUP BY fp.dog_type
    `).all();

    // 按时机分组 (entry_timing_rating 列暂不存在，跳过)
    const byTiming = [];

    return {
      total: overall.total || 0,
      wins: overall.wins || 0,
      winRate: overall.total > 0 ? (overall.wins / overall.total * 100) : 0,
      avgPnl: overall.avg_pnl || 0,
      best: overall.best || 0,
      worst: overall.worst || 0,
      byDogType,
      byTiming
    };
  }

  /**
   * 计算数组中某字段的平均值
   */
  calculateAvg(arr, field) {
    if (!arr || arr.length === 0) return 'N/A';
    const sum = arr.reduce((a, b) => a + (b[field] || 0), 0);
    return (sum / arr.length).toFixed(1);
  }

  /**
   * 获取金狗/银狗数据（成功/失败/错过的机会）
   */
  getGoldSilverDogs() {
    // 成功案例（我们买了并且赚钱的）
    const winners = this.db.prepare(`
      SELECT 
        symbol, token_ca, sm_count, signal_count, total_score, decision, max_gain_24h,
        ai_score, market_cap, liquidity, actual_pnl, sm_trend, signal_velocity
      FROM signal_features sf
      WHERE sf.did_buy = 1 AND sf.is_winner = 1
      ORDER BY sf.actual_pnl DESC
      LIMIT 10
    `).all();

    const goldDogs = this.db.prepare(`
      SELECT 
        symbol, token_ca, sm_count, signal_count, total_score, decision, max_gain_24h,
        ai_score, market_cap, liquidity, sm_trend, signal_velocity
      FROM signal_features
      WHERE is_gold_dog = 1
      ORDER BY max_gain_24h DESC
      LIMIT 20
    `).all();

    const silverDogs = this.db.prepare(`
      SELECT 
        symbol, token_ca, sm_count, signal_count, total_score, decision, max_gain_24h,
        ai_score, market_cap, liquidity, sm_trend, signal_velocity
      FROM signal_features
      WHERE is_silver_dog = 1 AND is_gold_dog = 0
      ORDER BY max_gain_24h DESC
      LIMIT 20
    `).all();

    // 我们买了但亏损的
    const losses = this.db.prepare(`
      SELECT 
        symbol, token_ca, sm_count, signal_count, total_score, decision, max_gain_24h,
        ai_score, market_cap, liquidity, actual_pnl, sm_trend, signal_velocity
      FROM signal_features sf
      WHERE sf.did_buy = 1 AND sf.is_winner = 0
      ORDER BY sf.actual_pnl ASC
      LIMIT 10
    `).all();

    // 错过的金狗（我们没买但涨了10x+）
    const missed = this.db.prepare(`
      SELECT 
        symbol, token_ca, sm_count, signal_count, total_score, decision, max_gain_24h,
        ai_score, market_cap, liquidity, sm_trend, signal_velocity
      FROM signal_features
      WHERE is_gold_dog = 1 AND did_buy = 0
      ORDER BY max_gain_24h DESC
      LIMIT 10
    `).all();

    return { winners, goldDogs, silverDogs, losses, missed };
  }

  /**
   * 分析问题
   */
  analyzeProblems(tradeData, goldDogData) {
    const problems = [];

    // 1. 检查是否错过太多金狗
    const missedCount = goldDogData.missed?.length || 0;
    if (missedCount > 5) {
      // 分析错过金狗的共同特征
      const avgSmCount = goldDogData.missed.reduce((a, b) => a + (b.sm_count || 0), 0) / missedCount;
      const avgScore = goldDogData.missed.reduce((a, b) => a + (b.total_score || 0), 0) / missedCount;

      problems.push({
        type: 'MISSED_GOLD_DOGS',
        severity: 'HIGH',
        message: `错过了 ${missedCount} 个金狗，平均SM=${avgSmCount.toFixed(1)}，平均分=${avgScore.toFixed(0)}`,
        suggestion: avgScore < 60 ? '考虑降低买入阈值' : '因子阈值可能过于严格'
      });
    }

    // 2. 检查胜率问题 (目标 90%)
    if (tradeData.winRate < 90) {
      problems.push({
        type: 'LOW_WIN_RATE',
        severity: 'HIGH',
        message: `胜率仅 ${tradeData.winRate.toFixed(1)}%，低于90%目标`,
        suggestion: '需要更严格的入场条件或更好的时机把握'
      });
    }

    // 3. 检查各狗类型表现
    const trapPerf = tradeData.byDogType?.find(d => d.dog_type === 'TRAP');
    if (trapPerf && trapPerf.count > 0) {
      problems.push({
        type: 'TRAP_TRADES',
        severity: 'MEDIUM',
        message: `竟然交易了 ${trapPerf.count} 个 TRAP 类型，平均亏损 ${trapPerf.avg_pnl?.toFixed(1)}%`,
        suggestion: 'TRAP 检测可能有问题，需要复核背离度阈值'
      });
    }

    // 4. 检查时机问题
    const latePerf = tradeData.byTiming?.find(t => t.timing === 'LATE' || t.timing === 'RISKY');
    if (latePerf && latePerf.count > 3 && latePerf.avg_pnl < 0) {
      problems.push({
        type: 'BAD_TIMING',
        severity: 'MEDIUM',
        message: `LATE/RISKY 时机买入 ${latePerf.count} 次，平均亏损 ${latePerf.avg_pnl?.toFixed(1)}%`,
        suggestion: '市值阈值可能设置不当'
      });
    }

    console.log(`\n📊 问题分析: 发现 ${problems.length} 个问题`);
    problems.forEach(p => {
      console.log(`   [${p.severity}] ${p.type}: ${p.message}`);
    });

    return problems;
  }

  /**
   * 调用 AI 获取改进建议
   */
  async getAISuggestions(tradeData, goldDogData, problems) {
    const prompt = `
你是一个专业的量化交易策略分析师，专注于 Meme 币交易。

## 目标
- 胜率: 90%+
- 收益率: 1000%+
- 核心策略: 早期发现金狗，翻倍出本，剩余死拿 (Free Moonbag)

## 当前表现
- 总交易: ${tradeData.total} 笔
- 胜率: ${tradeData.winRate.toFixed(1)}% (目标 90%)
- 平均收益: ${tradeData.avgPnl.toFixed(1)}%
- 最佳: +${tradeData.best?.toFixed(0) || 0}%
- 最差: ${tradeData.worst?.toFixed(0) || 0}%

## 成功案例 (我们买了并且赚钱的)
${goldDogData.winners?.slice(0, 5).map(d =>
      `- ${d.symbol}: 赚${d.actual_pnl?.toFixed(0) || 0}%, SM=${d.sm_count}, 趋势=${d.sm_trend || 'N/A'}, 信号速度=${d.signal_velocity?.toFixed(1) || 'N/A'}, 市值=$${(d.market_cap / 1000).toFixed(0)}K`
    ).join('\n') || '暂无数据'}

## 失败案例 (我们买了但亏损的)
${goldDogData.losses?.slice(0, 5).map(d =>
      `- ${d.symbol}: 亏${d.actual_pnl?.toFixed(0) || 0}%, SM=${d.sm_count}, 趋势=${d.sm_trend || 'N/A'}, 信号速度=${d.signal_velocity?.toFixed(1) || 'N/A'}, 市值=$${(d.market_cap / 1000).toFixed(0)}K`
    ).join('\n') || '暂无数据'}

## 错过的金狗 (我们没买但涨了1000%+)
${goldDogData.missed?.slice(0, 5).map(d =>
      `- ${d.symbol}: 涨${d.max_gain_24h?.toFixed(0)}%, SM=${d.sm_count}, 趋势=${d.sm_trend || 'N/A'}, 信号速度=${d.signal_velocity?.toFixed(1) || 'N/A'}, 决策=${d.decision}`
    ).join('\n') || '暂无数据'}

## 因子定义
1. **聪明钱密度** = SM数量 × 平均买入额 / 市值(K)  → 越高说明大资金越看好
2. **舆论背离度** = SM数量 / 信号次数  → >1表示聪明钱先于散户入场(好信号)
3. **叙事健康度** = AI评分 × (流动性/市值×10)  → 兼顾叙事质量和资金安全
4. **SM趋势** = RISING(聪明钱在增加)/STABLE(稳定)/FALLING(在减少) → RISING是最佳入场时机
5. **信号速度** = 每分钟新增信号数 → 速度过快可能是FOMO信号，适中(0.5-2)较好

## 当前阈值配置
- 聪明钱密度 HIGH: >= 5 (亏损案例平均: ${this.calculateAvg(goldDogData.losses, 'sm_count')})
- 舆论背离度 STEALTH: >= 1.0
- 叙事健康度 STRONG: >= 10
- SM趋势: 优选 RISING 或 STABLE
- 市值范围: $30K-$300K (EARLY=$10-50K, PRIME=$50-100K, LATE=$150K+)

## 当前风控配置
- 止损: -50%
- 时间止损: 45分钟不涨就走
- 仓位: 0.1-0.5 SOL (按评分分层)

## 发现的问题
${problems.map(p => `- [${p.severity}] ${p.type}: ${p.message}`).join('\n')}

## 核心原则（必须遵守）
1. **降低阈值必须配合更严格的风控** - 门槛降低意味着噪音增加，必须用止损和仓位控制风险
2. **因子交叉验证** - 不能只调一个因子，要考虑因子组合效果
3. **渐进式调整** - 建议小幅调整(±20%)而非激进变动

## 请深入分析
1. 成功案例和失败案例的关键差异是什么？
2. 为什么错过了金狗？阈值设置有什么问题？
3. 如果建议降低阈值，必须同时给出配套的风控调整
4. 有没有新的因子组合可以更好地识别金狗？

返回 JSON 格式:
{
  "key_insight": "一句话核心发现",
  "root_cause": "问题根因分析",
  "threshold_suggestions": [
    {"factor": "因子名", "current": 当前值, "suggested": 建议值, "confidence": 0.8, "reason": "详细原因"}
  ],
  "risk_control_suggestions": {
    "stop_loss": {"current": "-50%", "suggested": "-30%", "reason": "门槛降低后需要更快止损"},
    "position_size": {"current": "0.2 SOL", "suggested": "0.1 SOL", "reason": "试探性小仓位"},
    "time_stop": {"current": "45min", "suggested": "30min", "reason": "快进快出"}
  },
  "factor_combo": "建议的因子组合规则，例如: SM>=3 且 背离>=1.2 才买入",
  "priority_action": "最重要的第一步改变",
  "new_factor_idea": "如果有新因子建议"
}
`.trim();

    try {
      console.log('\n🤖 正在调用 AI 分析...');

      // 检查是否配置了 API Key
      const apiKey = process.env.XAI_API_KEY || process.env.OPENAI_API_KEY;
      if (!apiKey) {
        console.log('⚠️  [AI Review] 没有配置 XAI_API_KEY 或 OPENAI_API_KEY，跳过 AI 分析');
        return { analysis: '未配置 API Key', suggestions: [] };
      }

      const client = new OpenAI({
        apiKey: apiKey,
        baseURL: process.env.XAI_API_KEY ? 'https://api.x.ai/v1' : undefined
      });

      const completion = await client.chat.completions.create({
        model: process.env.XAI_API_KEY ? 'grok-3-mini' : 'gpt-4o-mini',
        messages: [
          { role: 'system', content: '你是一个专业的量化交易策略分析师。请用中文回答。' },
          { role: 'user', content: prompt }
        ],
        temperature: 0.3,
        max_tokens: 2000
      });

      const content = completion.choices[0]?.message?.content || '';
      console.log('🤖 AI 响应:', content.slice(0, 200) + '...');

      // 尝试解析 JSON
      const jsonMatch = content.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        const suggestions = JSON.parse(jsonMatch[0]);
        console.log('🤖 AI 建议:', suggestions.key_insight);
        return suggestions;
      }

      return { analysis: content, suggestions: [] };
    } catch (error) {
      console.error(`[AI Review] AI 分析失败: ${error.message}`);
      return { analysis: 'AI 分析失败', suggestions: [] };
    }
  }

  /**
   * 执行 AI 建议的调整
   */
  executeAdjustments(aiSuggestions) {
    const adjustments = [];

    // 打印阈值调整建议
    const thresholdSuggestions = aiSuggestions.threshold_suggestions || aiSuggestions.suggestions || [];
    if (thresholdSuggestions.length > 0) {
      console.log('\n🔧 AI 建议的阈值调整:');
      for (const s of thresholdSuggestions) {
        console.log(`   ${s.factor}: ${s.current} → ${s.suggested} (${s.reason})`);
        adjustments.push(s);

        // 记录到数据库
        try {
          this.db.prepare(`
            INSERT INTO threshold_history (factor_name, old_value, new_value, reason)
            VALUES (?, ?, ?, ?)
          `).run(s.factor, String(s.current), String(s.suggested), `AI建议: ${s.reason}`);
        } catch (e) {
          // 忽略
        }
      }

      // 🚀 自动应用阈值调整
      console.log('\n🚀 正在自动应用阈值调整...');
      try {
        const result = thresholdConfig.applyAISuggestions(thresholdSuggestions);
        if (result.applied.length > 0) {
          console.log(`   ✅ 成功应用 ${result.applied.length} 个调整`);
        }
        if (result.rejected.length > 0) {
          console.log(`   ⚠️ 拒绝 ${result.rejected.length} 个调整（超过安全限制）`);
        }
      } catch (e) {
        console.log(`   ❌ 自动应用失败: ${e.message}`);
      }
    }

    // 打印风控调整建议
    const riskSuggestions = aiSuggestions.risk_control_suggestions;
    if (riskSuggestions) {
      console.log('\n🛡️  AI 建议的风控调整:');
      if (riskSuggestions.stop_loss) {
        console.log(`   止损: ${riskSuggestions.stop_loss.current} → ${riskSuggestions.stop_loss.suggested} (${riskSuggestions.stop_loss.reason})`);
      }
      if (riskSuggestions.position_size) {
        console.log(`   仓位: ${riskSuggestions.position_size.current} → ${riskSuggestions.position_size.suggested} (${riskSuggestions.position_size.reason})`);
      }
      if (riskSuggestions.time_stop) {
        console.log(`   时间止损: ${riskSuggestions.time_stop.current} → ${riskSuggestions.time_stop.suggested} (${riskSuggestions.time_stop.reason})`);
      }
    }

    // 打印因子组合建议
    if (aiSuggestions.factor_combo) {
      console.log('\n📊 建议的因子组合规则:');
      console.log(`   ${aiSuggestions.factor_combo}`);
    }

    // 打印优先行动
    if (aiSuggestions.priority_action) {
      console.log('\n⚡ 最重要的第一步:');
      console.log(`   ${aiSuggestions.priority_action}`);
    }

    return adjustments;
  }
}

export default AIAutoReview;
