/**
 * 测试 GMGN 雷达 - 命中代币 PNL API
 */

import fs from 'fs';

const DEVICE_ID = '1d29f750-687f-42e1-851d-59a43e5d2ffa';
const CLIENT_ID = 'gmgn_web_test';

async function testHitTokensAPI() {
    console.log('\n========== 测试 GMGN 命中代币 PNL API ==========\n');

    const sessionPath = './config/gmgn_session.json';
    if (!fs.existsSync(sessionPath)) {
        console.log('❌ Session 文件不存在');
        return;
    }

    const sessionData = JSON.parse(fs.readFileSync(sessionPath, 'utf8'));
    const cookies = sessionData?.cookies || [];
    const cookieStr = cookies
        .filter(c => c.domain && c.domain.includes('gmgn'))
        .map(c => `${c.name}=${c.value}`)
        .join('; ');

    const params = new URLSearchParams({
        device_id: DEVICE_ID,
        client_id: CLIENT_ID,
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai',
        'Referer': 'https://gmgn.ai/trade/ZAxgSuiP?chain=sol'
    };

    // 可能的命中代币 API 端点
    const endpoints = [
        // 钱包持仓/命中代币
        {
            name: '钱包命中代币 (Wallet Holdings)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet/sol/holdings?${params}`
        },
        // 钱包交易历史
        {
            name: '钱包交易历史 (Wallet Trades)',
            url: `https://gmgn.ai/defi/quotation/v1/wallet_activity/sol?type=buy&limit=20&${params}`
        },
        // 追踪代币 PNL
        {
            name: '追踪代币 PNL (Tracked Tokens)',
            url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/tracked?${params}`
        },
        // 信号命中记录
        {
            name: '信号命中 (Signal Hits)',
            url: `https://gmgn.ai/defi/quotation/v1/signals/sol/hits?${params}`
        },
        // 雷达命中
        {
            name: '雷达命中 (Radar Hits)',
            url: `https://gmgn.ai/defi/quotation/v1/radar/sol/hits?${params}`
        },
        // 收藏代币
        {
            name: '收藏代币 (Favorite Tokens)',
            url: `https://gmgn.ai/defi/quotation/v1/tokens/sol/favorites?${params}`
        },
        // 用户 PNL
        {
            name: '用户 PNL (User PNL)',
            url: `https://gmgn.ai/defi/quotation/v1/user/pnl?chain=sol&${params}`
        },
        // 命中统计
        {
            name: '命中统计 (Hit Stats)',
            url: `https://gmgn.ai/defi/quotation/v1/stats/sol/hits?${params}`
        }
    ];

    console.log('Session Cookies:', cookies.length);
    console.log('');

    for (const ep of endpoints) {
        console.log(`📡 测试: ${ep.name}...`);

        try {
            const response = await fetch(ep.url, { headers });
            console.log('   Status:', response.status);

            if (response.ok) {
                const data = await response.json();

                if (data.code === 0 || !data.code) {
                    const items = data.data?.list || data.data?.tokens || data.data?.hits || data.data || [];
                    const count = Array.isArray(items) ? items.length : (typeof items === 'object' ? Object.keys(items).length : 0);
                    console.log('   ✅ 成功! 数据量:', count);

                    // 打印部分数据结构
                    if (count > 0 || (typeof items === 'object' && Object.keys(items).length > 0)) {
                        console.log('   📊 数据示例:');
                        const sample = Array.isArray(items) ? items[0] : items;
                        const keys = Object.keys(sample || {}).slice(0, 8);
                        console.log('      字段:', keys.join(', '));

                        // 如果有 PNL 相关字段，显示
                        if (sample) {
                            const pnlFields = Object.keys(sample).filter(k =>
                                k.toLowerCase().includes('pnl') ||
                                k.toLowerCase().includes('profit') ||
                                k.toLowerCase().includes('hit')
                            );
                            if (pnlFields.length > 0) {
                                console.log('      PNL字段:', pnlFields.join(', '));
                            }
                        }
                    }
                } else {
                    console.log('   ⚠️ API 返回错误:', data.code, data.msg || '');
                }
            } else {
                console.log('   ❌ 请求失败');
            }
        } catch (e) {
            console.log('   ❌ 异常:', e.message);
        }

        console.log('');
    }
}

testHitTokensAPI().catch(console.error);
