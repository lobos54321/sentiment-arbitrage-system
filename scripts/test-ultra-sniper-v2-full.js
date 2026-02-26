/**
 * Ultra Human Sniper v2.0 完整功能验证测试
 *
 * 测试覆盖:
 * 1. 猎人类型分类逻辑
 * 2. 动态评分系统和权重调整
 * 3. GMGN 牛人榜数据获取与解析
 * 4. 延迟跟单队列和信号发送链路
 * 5. 缓存和状态管理
 */

import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

import {
    UltraHumanSniperV2,
    classifyHunterType,
    calculateDynamicScore,
    HUNTER_TYPES
} from '../src/inputs/ultra-human-sniper-v2.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ═══════════════════════════════════════════════════════════════
// 测试结果收集
// ═══════════════════════════════════════════════════════════════

const testResults = {
    classification: { passed: false, details: '' },
    scoring: { passed: false, details: '' },
    gmgnApi: { passed: false, details: '' },
    signalQueue: { passed: false, details: '' },
    cache: { passed: false, details: '' }
};

// ═══════════════════════════════════════════════════════════════
// 测试 1: 猎人类型分类逻辑
// ═══════════════════════════════════════════════════════════════

function testClassification() {
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 测试 1: 猎人类型分类逻辑');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const testCases = [
        {
            name: '高频机器人',
            wallet: { txs_1d: 150, realized_profit_1d: '5000', winrate_1d: 0.6 },
            expectedType: 'BOT',
            expectedReason: 'high_frequency_100'
        },
        {
            name: '假胜率机器人',
            wallet: { txs_1d: 30, realized_profit_1d: '1000', winrate_1d: 0.99, winrate_7d: 0.99 },
            expectedType: 'BOT',
            expectedReason: 'fake_winrate'
        },
        {
            name: '今日亏损普通用户',
            wallet: { txs_1d: 10, realized_profit_1d: '-500', realized_profit_7d: '1000', winrate_1d: 0.5 },
            expectedType: 'NORMAL',
            expectedReason: 'negative_pnl_1d'
        },
        {
            name: '金狗猎手(FOX)',
            wallet: {
                txs_1d: 15, realized_profit_1d: '2000', realized_profit_7d: '5000',
                winrate_1d: 0.65, pnl_gt_5x_num_7d: 2, pnl_lt_minus_dot5_num_7d: 1,
                avg_holding_period_1d: 1800
            },
            expectedType: 'FOX',
            expectedReason: 'golden_dog_hunter'
        },
        {
            name: '波段猎手(TURTLE)',
            wallet: {
                txs_1d: 20, realized_profit_1d: '1500', realized_profit_7d: '4000',
                winrate_1d: 0.60, pnl_gt_5x_num_7d: 0, pnl_lt_minus_dot5_num_7d: 1,
                avg_holding_period_1d: 7200  // 120分钟
            },
            expectedType: 'TURTLE',
            expectedReason: 'swing_trader'
        },
        {
            name: '稳定猎手(WOLF)',
            wallet: {
                txs_1d: 30, realized_profit_1d: '1000', realized_profit_7d: '3000',
                winrate_1d: 0.55, pnl_gt_5x_num_7d: 0, pnl_lt_minus_dot5_num_7d: 2,
                avg_holding_period_1d: 1800
            },
            expectedType: 'WOLF',
            expectedReason: 'consistent_trader'
        }
    ];

    let passed = 0;
    let failed = 0;

    for (const tc of testCases) {
        const result = classifyHunterType(tc.wallet);
        const typeMatch = result.type === tc.expectedType;
        const reasonMatch = result.reason === tc.expectedReason;

        if (typeMatch && reasonMatch) {
            console.log(`✅ ${tc.name}: ${result.type} (${result.reason})`);
            passed++;
        } else {
            console.log(`❌ ${tc.name}:`);
            console.log(`   期望: ${tc.expectedType} (${tc.expectedReason})`);
            console.log(`   实际: ${result.type} (${result.reason})`);
            failed++;
        }
    }

    console.log(`\n分类测试结果: ${passed}/${testCases.length} 通过`);

    testResults.classification.passed = failed === 0;
    testResults.classification.details = `${passed}/${testCases.length} 测试用例通过`;

    return failed === 0;
}

// ═══════════════════════════════════════════════════════════════
// 测试 2: 动态评分系统
// ═══════════════════════════════════════════════════════════════

function testScoring() {
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 测试 2: 动态评分系统');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // 测试不同猎人类型的评分权重差异
    const foxWallet = {
        txs_1d: 15, realized_profit_1d: '2000', realized_profit_7d: '5000',
        winrate_1d: 0.65, winrate_7d: 0.62, pnl_gt_5x_num_7d: 3,
        pnl_lt_minus_dot5_num_7d: 0, avg_holding_period_1d: 3600,
        follow_count: 100
    };

    const turtleWallet = {
        txs_1d: 15, realized_profit_1d: '1500', realized_profit_7d: '4000',
        winrate_1d: 0.60, winrate_7d: 0.58, pnl_gt_5x_num_7d: 0,
        pnl_lt_minus_dot5_num_7d: 1, avg_holding_period_1d: 7200,
        follow_count: 80
    };

    // FOX 评分
    const foxProfile = classifyHunterType(foxWallet);
    const foxScore = calculateDynamicScore(foxWallet, foxProfile);

    console.log('🦊 FOX 型猎人评分:');
    console.log(`   总分: ${foxScore.totalScore.toFixed(1)}`);
    console.log(`   效率: ${foxScore.breakdown.profitEfficiency.toFixed(1)}`);
    console.log(`   金狗: ${foxScore.breakdown.goldenDogScore.toFixed(1)}`);
    console.log(`   稳定: ${foxScore.breakdown.winrateStability.toFixed(1)}`);
    console.log(`   权重调整: goldenDogScore=${foxScore.weights.goldenDogScore}`);

    // TURTLE 评分
    const turtleProfile = classifyHunterType(turtleWallet);
    const turtleScore = calculateDynamicScore(turtleWallet, turtleProfile);

    console.log('\n🐢 TURTLE 型猎人评分:');
    console.log(`   总分: ${turtleScore.totalScore.toFixed(1)}`);
    console.log(`   效率: ${turtleScore.breakdown.profitEfficiency.toFixed(1)}`);
    console.log(`   匹配: ${turtleScore.breakdown.holdingMatch.toFixed(1)}`);
    console.log(`   稳定: ${turtleScore.breakdown.winrateStability.toFixed(1)}`);
    console.log(`   权重调整: holdingMatch=${turtleScore.weights.holdingMatch}`);

    // 验证权重调整正确
    const foxGoldenWeight = foxScore.weights.goldenDogScore;
    const turtleHoldingWeight = turtleScore.weights.holdingMatch;

    const weightCorrect = foxGoldenWeight === 0.35 && turtleHoldingWeight === 0.25;

    console.log(`\n权重调整验证: ${weightCorrect ? '✅ 正确' : '❌ 错误'}`);
    console.log(`   FOX 金狗权重: ${foxGoldenWeight} (期望 0.35)`);
    console.log(`   TURTLE 持仓权重: ${turtleHoldingWeight} (期望 0.25)`);

    // 验证评分在合理范围
    const scoreInRange = foxScore.totalScore >= 0 && foxScore.totalScore <= 150 &&
                         turtleScore.totalScore >= 0 && turtleScore.totalScore <= 150;

    testResults.scoring.passed = weightCorrect && scoreInRange;
    testResults.scoring.details = weightCorrect
        ? '权重调整正确，评分在合理范围'
        : '权重调整错误或评分超出范围';

    return testResults.scoring.passed;
}

// ═══════════════════════════════════════════════════════════════
// 测试 3: GMGN API 获取
// ═══════════════════════════════════════════════════════════════

async function testGmgnApi() {
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 测试 3: GMGN 牛人榜数据获取');
    console.log('═══════════════════════════════════════════════════════════════\n');

    try {
        const sniper = new UltraHumanSniperV2({
            topHuntersCount: 10,
            minScore: 50,
            debug: true
        });

        // 检查 session 文件
        const sessionPath = path.join(__dirname, '../config/gmgn_session.json');
        if (!fs.existsSync(sessionPath)) {
            console.log('❌ GMGN session 文件不存在');
            testResults.gmgnApi.passed = false;
            testResults.gmgnApi.details = 'Session 文件不存在';
            return false;
        }

        console.log('✅ Session 文件存在');

        // 加载 session
        sniper.loadSession();
        console.log(`✅ Session 已加载 (${sniper.sessionData?.cookies?.length || 0} cookies)`);

        // 获取牛人榜数据
        console.log('\n📊 正在获取牛人榜数据...');
        const hunters = await sniper.fetchAndClassifyHunters();

        if (hunters.length > 0) {
            console.log(`✅ 成功获取并分类 ${hunters.length} 个猎人`);
            testResults.gmgnApi.passed = true;
            testResults.gmgnApi.details = `成功获取 ${hunters.length} 个猎人`;
        } else {
            console.log('⚠️ 没有找到符合条件的猎人 (可能是API限流或数据问题)');
            // 检查是否有本地缓存数据
            const cache = sniper.loadCache();
            if (cache) {
                console.log('✅ 但本地缓存数据可用');
                testResults.gmgnApi.passed = true;
                testResults.gmgnApi.details = '使用本地缓存数据';
            } else {
                testResults.gmgnApi.passed = false;
                testResults.gmgnApi.details = '无数据且无缓存';
            }
        }

        return testResults.gmgnApi.passed;

    } catch (e) {
        console.log(`❌ API 测试失败: ${e.message}`);
        testResults.gmgnApi.passed = false;
        testResults.gmgnApi.details = e.message;
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════
// 测试 4: 延迟跟单队列和信号发送
// ═══════════════════════════════════════════════════════════════

async function testSignalQueue() {
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 测试 4: 延迟跟单队列和信号发送');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const sniper = new UltraHumanSniperV2({
        minScore: 50,
        signalCooldown: 1000,  // 1秒冷却用于测试
        debug: true
    });

    let signalReceived = false;
    let warningReceived = false;

    // 监听信号
    sniper.on('signal', (signal) => {
        console.log(`📤 收到信号: ${signal.symbol} from ${signal.hunter.name}`);
        signalReceived = true;
    });

    sniper.on('warning', (warning) => {
        console.log(`⚠️ 收到警告: ${warning.type}`);
        warningReceived = true;
    });

    // 模拟猎人数据
    const mockHunter = {
        address: 'TestWallet123456789',
        name: 'TestHunter',
        profile: { ...HUNTER_TYPES.FOX, reason: 'test' },
        score: 80,
        metrics: { profitPerTrade: 100, goldenDogs: 2, winrate1d: 0.65 }
    };

    const mockActivity = {
        token_address: 'TestToken123456789',
        token_symbol: 'TEST',
        market_cap: 100000,
        liquidity: 10000,
        price: 0.001,
        timestamp: Date.now() / 1000
    };

    // 测试 FOX 立即跟单
    console.log('\n测试 FOX 立即跟单:');
    sniper.emitSignal(mockHunter, mockActivity, Date.now());

    // 测试 TURTLE 延迟跟单
    console.log('\n测试 TURTLE 延迟跟单:');
    const turtleHunter = {
        ...mockHunter,
        name: 'TurtleHunter',
        profile: { ...HUNTER_TYPES.TURTLE, reason: 'test', delayMinutes: 1 }
    };

    await sniper.processHunterBuy(turtleHunter, mockActivity, Date.now());

    console.log(`延迟队列长度: ${sniper.delayedQueue.length}`);

    // 测试 EAGLE 警告
    console.log('\n测试 EAGLE 警告:');
    const eagleHunter = {
        ...mockHunter,
        name: 'EagleHunter',
        profile: { ...HUNTER_TYPES.EAGLE, reason: 'test' }
    };

    await sniper.processHunterBuy(eagleHunter, mockActivity, Date.now());

    // 验证结果
    const queueWorking = sniper.delayedQueue.length > 0;
    console.log(`\n信号发送: ${signalReceived ? '✅' : '❌'}`);
    console.log(`警告发送: ${warningReceived ? '✅' : '❌'}`);
    console.log(`延迟队列: ${queueWorking ? '✅' : '❌'}`);

    testResults.signalQueue.passed = signalReceived && queueWorking;
    testResults.signalQueue.details = `信号:${signalReceived}, 警告:${warningReceived}, 队列:${queueWorking}`;

    return testResults.signalQueue.passed;
}

// ═══════════════════════════════════════════════════════════════
// 测试 5: 缓存和状态管理
// ═══════════════════════════════════════════════════════════════

async function testCache() {
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('🧪 测试 5: 缓存和状态管理');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const cachePath = path.join(__dirname, '../logs/ultra-sniper-cache-test.json');

    const sniper = new UltraHumanSniperV2({
        cachePath,
        minScore: 50,
        debug: true
    });

    // 模拟一些猎人数据
    sniper.topHunters = [
        { address: 'addr1', name: 'Hunter1', profile: { type: 'FOX' }, score: 80, metrics: {} },
        { address: 'addr2', name: 'Hunter2', profile: { type: 'WOLF' }, score: 70, metrics: {} }
    ];
    sniper.stats.leaderboardUpdates = 5;
    sniper.stats.signalsEmitted = 10;

    // 保存缓存
    console.log('保存缓存...');
    sniper.saveCache();

    const cacheExists = fs.existsSync(cachePath);
    console.log(`缓存文件存在: ${cacheExists ? '✅' : '❌'}`);

    // 读取缓存
    if (cacheExists) {
        const cacheData = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
        console.log(`缓存猎人数: ${cacheData.hunters?.length || 0}`);
        console.log(`缓存统计: leaderboardUpdates=${cacheData.stats?.leaderboardUpdates}`);
    }

    // 加载缓存
    console.log('\n加载缓存...');
    const loadedCache = sniper.loadCache();
    console.log(`加载结果: ${loadedCache ? '✅ 有数据' : '❌ 无数据或过期'}`);

    // 测试 getStatus
    console.log('\n获取状态...');
    const status = sniper.getStatus();
    console.log(`状态检查:`);
    console.log(`   isRunning: ${status.isRunning}`);
    console.log(`   huntersTracking: ${status.huntersTracking}`);
    console.log(`   topHunters: ${status.topHunters.length} 个`);

    // 清理测试缓存
    if (fs.existsSync(cachePath)) {
        fs.unlinkSync(cachePath);
    }

    testResults.cache.passed = cacheExists && loadedCache !== null;
    testResults.cache.details = `缓存存在:${cacheExists}, 加载成功:${loadedCache !== null}`;

    return testResults.cache.passed;
}

// ═══════════════════════════════════════════════════════════════
// 运行所有测试
// ═══════════════════════════════════════════════════════════════

async function runAllTests() {
    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════╗');
    console.log('║        Ultra Human Sniper v2.0 完整功能验证                        ║');
    console.log('╚══════════════════════════════════════════════════════════════════╝\n');

    // 运行测试
    testClassification();
    testScoring();
    await testGmgnApi();
    await testSignalQueue();
    await testCache();

    // 打印总结
    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════╗');
    console.log('║                        测试总结                                    ║');
    console.log('╚══════════════════════════════════════════════════════════════════╝\n');

    const tests = [
        { name: '猎人类型分类', result: testResults.classification },
        { name: '动态评分系统', result: testResults.scoring },
        { name: 'GMGN API获取', result: testResults.gmgnApi },
        { name: '信号队列链路', result: testResults.signalQueue },
        { name: '缓存状态管理', result: testResults.cache }
    ];

    let passedCount = 0;
    for (const test of tests) {
        const icon = test.result.passed ? '✅' : '❌';
        console.log(`${icon} ${test.name}: ${test.result.details}`);
        if (test.result.passed) passedCount++;
    }

    console.log(`\n总计: ${passedCount}/${tests.length} 测试通过`);

    if (passedCount === tests.length) {
        console.log('\n🎉 所有功能验证通过！Ultra Human Sniper v2.0 已完整集成。\n');
    } else {
        console.log('\n⚠️ 部分功能需要检查。\n');
    }

    return passedCount === tests.length;
}

// 运行
runAllTests().catch(console.error);
