/**
 * 测试 GMGN 牛人榜 API 和 DeBot 雷达信号
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testGmgnLeaderboard() {
    console.log('\n========== 测试 GMGN 牛人榜 API ==========\n');

    // 1. 加载 Session
    const sessionPath = './config/gmgn_session.json';
    if (!fs.existsSync(sessionPath)) {
        console.log('❌ Session 文件不存在');
        return false;
    }

    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    console.log('✅ Session 已加载');
    console.log('   Cookies 数量:', sessionData?.cookies?.length || 0);
    console.log('   创建时间:', sessionData?.created_at || 'N/A');

    // 2. 构造请求
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    console.log('\n📡 请求牛人榜数据...');

    const params = new URLSearchParams({
        device_id: DEVICE_ID,
        client_id: CLIENT_ID,
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const url = `https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?tag=smart_degen&limit=20&${params}`;

    try {
        const response = await fetch(url, {
            headers: {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Cookie': cookieStr,
                'Origin': 'https://gmgn.ai',
                'Referer': 'https://gmgn.ai/'
            }
        });

        console.log('HTTP Status:', response.status);

        if (!response.ok) {
            console.log('❌ 请求失败:', response.status);
            const text = await response.text();
            console.log('响应内容:', text.slice(0, 500));
            return false;
        }

        const data = await response.json();

        if (data.code && data.code !== 0) {
            console.log('❌ API 错误码:', data.code, data.msg || '');
            return false;
        }

        const wallets = data.data?.rank || data.data || [];
        console.log('\n✅ 成功获取', wallets.length, '个钱包数据');

        if (wallets.length === 0) {
            console.log('⚠️ 返回数据为空，可能需要重新登录');
            return false;
        }

        // 显示前10个
        console.log('\n📊 Top 10 钱包:');
        console.log('─'.repeat(80));
        wallets.slice(0, 10).forEach((w, i) => {
            const name = w.twitter_name || w.name || (w.wallet_address || '').slice(0, 8);
            const winrate = ((w.winrate_7d || 0) * 100).toFixed(0);
            const pnl7d = Number(w.realized_profit_7d) || 0;
            const pnl1d = Number(w.realized_profit_1d) || 0;
            const txs = w.txs_1d || 0;
            const goldenDogs = w.pnl_gt_5x_num_7d || 0;

            console.log(`   ${i+1}. ${String(name).padEnd(15).slice(0,15)} | 胜率:${winrate.padStart(3)}% | 7D: $${String(Math.round(pnl7d)).padStart(8)} | 1D: $${String(Math.round(pnl1d)).padStart(6)} | 日TX:${String(txs).padStart(3)} | 金狗:${goldenDogs}`);
        });
        console.log('─'.repeat(80));

        return true;

    } catch (e) {
        console.log('❌ 请求异常:', e.message);
        return false;
    }
}

async function testDebotRadar() {
    console.log('\n========== 测试 DeBot 雷达信号 API ==========\n');

    // 1. 加载 DeBot Session
    const sessionPath = './config/debot_session.json';
    if (!fs.existsSync(sessionPath)) {
        console.log('❌ DeBot Session 文件不存在');
        return false;
    }

    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    console.log('✅ DeBot Session 已加载');
    console.log('   Cookies 数量:', sessionData?.cookies?.length || 0);
    console.log('   创建时间:', sessionData?.created_at || 'N/A');

    // 构造 Cookie
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

    // 生成请求 ID
    const requestId = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });

    // 测试多个 DeBot API 端点 (正确的端点格式)
    const endpoints = [
        { name: '热力图 (Heatmap)', url: `https://debot.ai/api/community/signal/channel/heatmap?request_id=${requestId}&chain=solana` },
        { name: '热门排行 (Activity Rank)', url: `https://debot.ai/api/community/signal/activity/rank?request_id=${requestId}&chain=solana` }
    ];

    let successCount = 0;

    for (const ep of endpoints) {
        console.log(`\n📡 测试: ${ep.name}...`);

        try {
            const response = await fetch(ep.url, {
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Cookie': cookieStr,
                    'Origin': 'https://debot.ai',
                    'Referer': 'https://debot.ai/'
                }
            });

            console.log('   HTTP Status:', response.status);

            if (!response.ok) {
                console.log('   ❌ 请求失败');
                continue;
            }

            const data = await response.json();

            if (data.code && data.code !== 0 && data.code !== 200) {
                console.log('   ❌ API 错误:', data.code, data.msg || data.message || '');
                continue;
            }

            const items = data.data || data.list || data.result || [];
            const count = Array.isArray(items) ? items.length : (items ? 1 : 0);

            console.log('   ✅ 获取到', count, '条数据');

            // 显示部分数据
            if (Array.isArray(items) && items.length > 0) {
                const sample = items[0];
                const keys = Object.keys(sample).slice(0, 5);
                console.log('   示例字段:', keys.join(', '));
            }

            successCount++;

        } catch (e) {
            console.log('   ❌ 请求异常:', e.message);
        }
    }

    return successCount > 0;
}

async function main() {
    console.log('🔍 开始 API 连接测试...\n');
    console.log('═'.repeat(80));

    const gmgnOk = await testGmgnLeaderboard();
    const debotOk = await testDebotRadar();

    console.log('\n' + '═'.repeat(80));
    console.log('\n📋 测试结果汇总:');
    console.log('   GMGN 牛人榜:', gmgnOk ? '✅ 正常' : '❌ 异常');
    console.log('   DeBot 雷达:', debotOk ? '✅ 正常' : '❌ 异常');
    console.log('');

    if (!gmgnOk) {
        console.log('💡 如果 GMGN 失败，请运行: node scripts/gmgn-login-setup.js');
    }
    if (!debotOk) {
        console.log('💡 如果 DeBot 失败，请运行: node scripts/debot-login-setup.js');
    }
}

main().catch(console.error);
