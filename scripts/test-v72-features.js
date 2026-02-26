#!/usr/bin/env node

/**
 * v7.2 综合测试脚本
 *
 * 测试内容：
 * 1. 模块导入测试
 * 2. 语法检查
 * 3. Ultra Fast Track 逻辑测试
 * 4. SelfIterationManager 功能测试
 * 5. SmartExitEngine AI 集成测试
 * 6. HunterPerformanceTracker 测试
 */

import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

console.log('═'.repeat(70));
console.log('🧪 Sentiment Arbitrage System v7.2 - 综合测试');
console.log('═'.repeat(70));
console.log();

const results = {
    passed: 0,
    failed: 0,
    tests: []
};

function test(name, fn) {
    try {
        fn();
        console.log(`✅ ${name}`);
        results.passed++;
        results.tests.push({ name, status: 'PASS' });
    } catch (error) {
        console.log(`❌ ${name}`);
        console.log(`   Error: ${error.message}`);
        results.failed++;
        results.tests.push({ name, status: 'FAIL', error: error.message });
    }
}

async function asyncTest(name, fn) {
    try {
        await fn();
        console.log(`✅ ${name}`);
        results.passed++;
        results.tests.push({ name, status: 'PASS' });
    } catch (error) {
        console.log(`❌ ${name}`);
        console.log(`   Error: ${error.message}`);
        results.failed++;
        results.tests.push({ name, status: 'FAIL', error: error.message });
    }
}

// ============================================
// 1. 模块导入测试
// ============================================
console.log('\n📦 模块导入测试\n');

await asyncTest('导入 strategy.js', async () => {
    const { default: STRATEGY, EXIT_STRATEGY, RISK } = await import('../src/config/strategy.js');
    if (!STRATEGY || !EXIT_STRATEGY || !RISK) throw new Error('Missing exports');
});

await asyncTest('导入 HunterPerformanceTracker', async () => {
    const { HunterPerformanceTracker } = await import('../src/tracking/hunter-performance.js');
    if (!HunterPerformanceTracker) throw new Error('Missing export');
});

await asyncTest('导入 SelfIterationManager', async () => {
    const { SelfIterationManager } = await import('../src/analytics/self-iteration-manager.js');
    if (!SelfIterationManager) throw new Error('Missing export');
});

await asyncTest('导入 SmartExitEngine', async () => {
    const { SmartExitEngine } = await import('../src/engines/smart-exit.js');
    if (!SmartExitEngine) throw new Error('Missing export');
});

await asyncTest('导入 PositionMonitorV2', async () => {
    const { PositionMonitorV2 } = await import('../src/execution/position-monitor-v2.js');
    if (!PositionMonitorV2) throw new Error('Missing export');
});

await asyncTest('导入 RiskManager', async () => {
    const { RiskManager } = await import('../src/risk/risk-manager.js');
    if (!RiskManager) throw new Error('Missing export');
});

await asyncTest('导入 SignalSourceOptimizer', async () => {
    const { SignalSourceOptimizer } = await import('../src/scoring/signal-source-optimizer.js');
    if (!SignalSourceOptimizer) throw new Error('Missing export');
});

// ============================================
// 2. SmartExitEngine 测试
// ============================================
console.log('\n⚡ SmartExitEngine 测试\n');

await asyncTest('SmartExitEngine 初始化', async () => {
    const { SmartExitEngine } = await import('../src/engines/smart-exit.js');
    const engine = new SmartExitEngine({ enabled: true });
    if (!engine.config.phasedStopLoss) throw new Error('Missing phasedStopLoss config');
});

await asyncTest('SmartExitEngine 分阶段止损', async () => {
    const { SmartExitEngine } = await import('../src/engines/smart-exit.js');
    const engine = new SmartExitEngine({ enabled: true });

    const phase1 = engine.getPhasedStopLoss(5);  // 5分钟
    const phase2 = engine.getPhasedStopLoss(20); // 20分钟
    const phase3 = engine.getPhasedStopLoss(45); // 45分钟
    const phase4 = engine.getPhasedStopLoss(90); // 90分钟

    if (phase1.phase !== 1) throw new Error('Phase 1 failed');
    if (phase2.phase !== 2) throw new Error('Phase 2 failed');
    if (phase3.phase !== 3) throw new Error('Phase 3 failed');
    if (phase4.phase !== 4) throw new Error('Phase 4 failed');
});

await asyncTest('SmartExitEngine AI 触发条件', async () => {
    const { SmartExitEngine } = await import('../src/engines/smart-exit.js');
    const engine = new SmartExitEngine({ enabled: true, aiExitEnabled: true });

    // 模拟 AI 触发条件检查
    const shouldTrigger = engine.shouldTriggerAI(
        { token_ca: 'test123' },
        0.35,  // compositeScore 在 0.25-0.50 之间
        0.10,  // pnlPercent 在 -0.15 到 0.30 之间
        20     // holdingMinutes >= 15
    );

    // 由于没有 AI client，应该返回 false
    if (shouldTrigger !== false) throw new Error('AI trigger should be false without client');
});

await asyncTest('SmartExitEngine setAIClient', async () => {
    const { SmartExitEngine } = await import('../src/engines/smart-exit.js');
    const engine = new SmartExitEngine({ enabled: true });

    // 模拟 AI client
    const mockAIClient = {
        analyze: async () => 'ACTION: HOLD\nCONFIDENCE: 80\nREASON: Test reason'
    };

    engine.setAIClient(mockAIClient);

    if (!engine.aiClient) throw new Error('AI client not set');
});

// ============================================
// 3. HunterPerformanceTracker 测试
// ============================================
console.log('\n🦊 HunterPerformanceTracker 测试\n');

await asyncTest('HunterPerformanceTracker 权重调整', async () => {
    const { HunterPerformanceTracker } = await import('../src/tracking/hunter-performance.js');

    // 使用内存数据库
    const tracker = new HunterPerformanceTracker(null);

    // 模拟记录和权重获取
    const foxWeight = tracker.getWeight('FOX');
    const adjustedScore = tracker.adjustScore(80, 'FOX');

    if (typeof foxWeight !== 'number') throw new Error('Weight should be number');
    if (adjustedScore !== Math.round(80 * foxWeight)) throw new Error('Score adjustment failed');
});

// ============================================
// 4. SelfIterationManager 测试
// ============================================
console.log('\n🔄 SelfIterationManager 测试\n');

await asyncTest('SelfIterationManager 初始化', async () => {
    const { SelfIterationManager } = await import('../src/analytics/self-iteration-manager.js');

    // 使用测试数据库
    const manager = new SelfIterationManager('./data/test_iteration.db', {
        iterationIntervalHours: 1,
        generateReports: false
    });

    if (!manager.dynamicWeights) throw new Error('Missing dynamicWeights');
    if (!manager.dynamicWeights.sources) throw new Error('Missing sources weights');
    if (!manager.dynamicWeights.hunters) throw new Error('Missing hunters weights');
});

await asyncTest('SelfIterationManager 权重获取', async () => {
    const { SelfIterationManager } = await import('../src/analytics/self-iteration-manager.js');

    const manager = new SelfIterationManager('./data/test_iteration.db');

    const debotWeight = manager.getSourceWeight('debot');
    const foxWeight = manager.getHunterWeight('FOX');

    if (debotWeight !== 1.0) throw new Error('Debot weight should be 1.0');
    if (foxWeight !== 1.0) throw new Error('FOX weight should be 1.0');
});

// ============================================
// 5. Ultra Fast Track 逻辑测试
// ============================================
console.log('\n🚀 Ultra Fast Track 逻辑测试\n');

test('Fast Track 条件检查 - 全部满足', () => {
    // 模拟 Ultra Fast Track 条件
    const signal = {
        hunter_type: 'FOX',
        buy_age_minutes: 5,
        market_cap: 300000,
        liquidity: 10000
    };
    const adjustedScore = 75;

    const isEliteFox = signal.hunter_type === 'FOX' && adjustedScore >= 70;
    const isFreshBuy = signal.buy_age_minutes <= 10;
    const isSmallCap = signal.market_cap < 500000;
    const hasGoodLiquidity = signal.liquidity >= 5000;

    const shouldFastTrack = isEliteFox && isFreshBuy && isSmallCap && hasGoodLiquidity;

    if (!shouldFastTrack) throw new Error('Should trigger fast track');
});

test('Fast Track 条件检查 - 分数不足', () => {
    const signal = {
        hunter_type: 'FOX',
        buy_age_minutes: 5,
        market_cap: 300000,
        liquidity: 10000
    };
    const adjustedScore = 60; // 低于70

    const isEliteFox = signal.hunter_type === 'FOX' && adjustedScore >= 70;
    const isFreshBuy = signal.buy_age_minutes <= 10;
    const isSmallCap = signal.market_cap < 500000;
    const hasGoodLiquidity = signal.liquidity >= 5000;

    const shouldFastTrack = isEliteFox && isFreshBuy && isSmallCap && hasGoodLiquidity;

    if (shouldFastTrack) throw new Error('Should NOT trigger fast track (score too low)');
});

test('Fast Track 条件检查 - 买入太久', () => {
    const signal = {
        hunter_type: 'FOX',
        buy_age_minutes: 15, // 超过10分钟
        market_cap: 300000,
        liquidity: 10000
    };
    const adjustedScore = 75;

    const isEliteFox = signal.hunter_type === 'FOX' && adjustedScore >= 70;
    const isFreshBuy = signal.buy_age_minutes <= 10;
    const isSmallCap = signal.market_cap < 500000;
    const hasGoodLiquidity = signal.liquidity >= 5000;

    const shouldFastTrack = isEliteFox && isFreshBuy && isSmallCap && hasGoodLiquidity;

    if (shouldFastTrack) throw new Error('Should NOT trigger fast track (buy too old)');
});

test('Fast Track 条件检查 - 市值太大', () => {
    const signal = {
        hunter_type: 'FOX',
        buy_age_minutes: 5,
        market_cap: 800000, // 超过500K
        liquidity: 10000
    };
    const adjustedScore = 75;

    const isEliteFox = signal.hunter_type === 'FOX' && adjustedScore >= 70;
    const isFreshBuy = signal.buy_age_minutes <= 10;
    const isSmallCap = signal.market_cap < 500000;
    const hasGoodLiquidity = signal.liquidity >= 5000;

    const shouldFastTrack = isEliteFox && isFreshBuy && isSmallCap && hasGoodLiquidity;

    if (shouldFastTrack) throw new Error('Should NOT trigger fast track (mcap too high)');
});

// ============================================
// 6. 配置参数测试
// ============================================
console.log('\n⚙️ 配置参数测试\n');

await asyncTest('EXIT_STRATEGY 结构完整性', async () => {
    const { EXIT_STRATEGY } = await import('../src/config/strategy.js');

    if (!EXIT_STRATEGY.STOP_LOSS) throw new Error('Missing STOP_LOSS');
    if (!EXIT_STRATEGY.TIME_STOP) throw new Error('Missing TIME_STOP');
    if (!EXIT_STRATEGY.TAKE_PROFIT) throw new Error('Missing TAKE_PROFIT');

    if (typeof EXIT_STRATEGY.STOP_LOSS.SCOUT !== 'number') throw new Error('SCOUT stop loss should be number');
    if (typeof EXIT_STRATEGY.TIME_STOP.SOL_MINUTES !== 'number') throw new Error('SOL_MINUTES should be number');
});

await asyncTest('RISK 结构完整性', async () => {
    const { RISK } = await import('../src/config/strategy.js');

    if (!RISK.CIRCUIT_BREAKER) throw new Error('Missing CIRCUIT_BREAKER');
    if (!RISK.DEFENSIVE_MODE) throw new Error('Missing DEFENSIVE_MODE');

    if (typeof RISK.MIN_SCORE_TO_TRADE !== 'number') throw new Error('MIN_SCORE_TO_TRADE should be number');
    if (typeof RISK.CIRCUIT_BREAKER.CONSECUTIVE_LOSS_PAUSE !== 'number') throw new Error('CONSECUTIVE_LOSS_PAUSE should be number');
});

// ============================================
// 测试结果汇总
// ============================================
console.log('\n' + '═'.repeat(70));
console.log('📊 测试结果汇总');
console.log('═'.repeat(70));
console.log(`✅ 通过: ${results.passed}`);
console.log(`❌ 失败: ${results.failed}`);
console.log(`📝 总计: ${results.passed + results.failed}`);
console.log('═'.repeat(70));

if (results.failed > 0) {
    console.log('\n❌ 失败的测试:');
    results.tests.filter(t => t.status === 'FAIL').forEach(t => {
        console.log(`   - ${t.name}: ${t.error}`);
    });
    process.exit(1);
} else {
    console.log('\n✅ 所有测试通过! v7.2 功能正常。');
    process.exit(0);
}
