import fs from 'fs';

// 读取 session
const session = JSON.parse(fs.readFileSync('./config/gmgn_session.json'));
const cookies = session.cookies || [];
const cookieStr = cookies
    .filter(c => c.domain && c.domain.includes('gmgn'))
    .map(c => `${c.name}=${c.value}`)
    .join('; ');

// 提取 token
let authToken = '';
if (session.origins) {
    for (const origin of session.origins) {
        if (origin.origin && origin.origin.includes('gmgn') && origin.localStorage) {
            for (const item of origin.localStorage) {
                if (item.name && item.name.includes('token') && item.value) {
                    try {
                        const data = JSON.parse(item.value);
                        if (data.access_token) authToken = data.access_token;
                    } catch (e) {
                        if (item.value.startsWith('eyJ')) authToken = item.value;
                    }
                }
            }
        }
    }
}

console.log('Cookie length:', cookieStr.length);
console.log('Auth token:', authToken ? 'Found (' + authToken.slice(0, 30) + '...)' : 'Not found');

// 测试 API
async function testApi(endpoint, name) {
    const params = new URLSearchParams({
        device_id: '1d29f750-687f-42e1-851d-59a43e5d2ffa',
        client_id: 'gmgn_web_test',
        from_app: 'gmgn',
        app_ver: '20260101',
        tz_name: 'Australia/Brisbane',
        app_lang: 'en',
        os: 'web'
    });

    const sep = endpoint.includes('?') ? '&' : '?';
    const url = `https://gmgn.ai${endpoint}${sep}${params}`;

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Cookie': cookieStr,
        'Origin': 'https://gmgn.ai',
        'Referer': 'https://gmgn.ai/'
    };
    if (authToken) headers['Authorization'] = `Bearer ${authToken}`;

    try {
        const res = await fetch(url, { headers });
        const data = await res.json();
        console.log(`\n=== ${name} (${res.status}) ===`);
        if (data.data) {
            if (Array.isArray(data.data)) {
                console.log(`返回 ${data.data.length} 条数据`);
                if (data.data[0]) {
                    console.log('首条数据字段:', Object.keys(data.data[0]).join(', '));
                    // 打印一些关键字段
                    const first = data.data[0];
                    if (first.wallet_address) console.log('钱包地址:', first.wallet_address);
                    if (first.winrate !== undefined) console.log('胜率:', first.winrate);
                    if (first.realized_profit !== undefined) console.log('已实现利润:', first.realized_profit);
                    if (first.pnl_7d !== undefined) console.log('7天PnL:', first.pnl_7d);
                }
            } else if (data.data.rank) {
                console.log(`排行榜返回 ${data.data.rank.length} 条数据`);
                if (data.data.rank[0]) {
                    console.log('首条数据字段:', Object.keys(data.data.rank[0]).join(', '));
                }
            } else {
                console.log('数据字段:', Object.keys(data.data).slice(0, 15).join(', '));
            }
        } else {
            console.log('响应:', JSON.stringify(data).slice(0, 300));
        }
    } catch (e) {
        console.log(`\n=== ${name} ERROR ===`);
        console.log(e.message);
    }
}

// 测试多个 API 端点
console.log('\n🔍 测试 GMGN HTTP APIs...\n');

await testApi('/defi/quotation/v1/rank/sol/wallets/7d?tag=smart_degen&limit=10', '牛人榜 7天 (defi)');
await testApi('/defi/quotation/v1/rank/sol/wallets/1d?tag=smart_degen&limit=10', '牛人榜 1天 (defi)');
await testApi('/api/v1/smartmoney/rank?chain=sol&period=7d&limit=5', 'SmartMoney 排行');

// 测试单个钱包数据
const testWallet = 'DCiH9TPjehrx2B5SCJG1QbdsXQ7kDL4AQGBv5RbqGGMm';
await testApi(`/defi/quotation/v1/smartmoney/sol/walletNew/${testWallet}`, '钱包持仓数据');
await testApi(`/api/v1/wallet/info?chain=sol&address=${testWallet}`, '钱包信息');

// 测试钱包交易活动 (用于实时追踪)
await testApi(`/defi/quotation/v1/wallet_activity/sol?type=buy&wallet=${testWallet}&limit=10`, '钱包买入活动');
await testApi(`/defi/quotation/v1/wallet_activity/sol?type=sell&wallet=${testWallet}&limit=10`, '钱包卖出活动');
await testApi(`/defi/quotation/v1/wallet/sol/${testWallet}/activities?limit=10`, '钱包活动 v1');

console.log('\n✅ 测试完成');
