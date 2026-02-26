/**
 * Find Missed Golden Dogs Analysis v2
 *
 * 1. 提取日志中被过滤的token
 * 2. 查询这些token的当前价格
 * 3. 对比入场时的价格，找出真正涨了的金狗
 * 4. 分析为什么被过滤
 */

import fs from 'fs';
import path from 'path';

// GMGN API 配置
const GMGN_API_BASE = 'https://gmgn.ai/defi/quotation/v1';

// 从日志提取被过滤的token信息 (v2: 两阶段解析)
function extractFilteredTokensFromLog(logPath) {
    const content = fs.readFileSync(logPath, 'utf-8');
    const lines = content.split('\n');

    const filteredTokens = new Map();

    // 第一步: 建立 symbol -> address 的映射 (从 market/metrics 行)
    const symbolToAddress = new Map();

    // 辅助函数: 根据地址格式判断链
    function detectChain(address) {
        if (address.startsWith('0x')) {
            return 'bsc';  // BSC 地址以 0x 开头
        } else if (address.endsWith('pump') || address.length > 30) {
            return 'solana';  // Pump.fun 地址以 pump 结尾，SOL地址较长
        }
        return 'unknown';
    }

    for (const line of lines) {
        if (line.includes('market/metrics')) {
            const tokenMatch = line.match(/"token":"([^"]+)"/);
            const symbolMatch = line.match(/"symbol":"([^"]+)"/);
            const chainMatch = line.match(/"chain":"([^"]+)"/);

            if (tokenMatch && symbolMatch) {
                const address = tokenMatch[1];
                const symbol = symbolMatch[1];
                let chain = chainMatch ? chainMatch[1] : 'unknown';

                // 双重验证: 使用地址格式确认链类型
                const detectedChain = detectChain(address);
                if (chain === 'unknown' || (chain !== detectedChain && detectedChain !== 'unknown')) {
                    chain = detectedChain;
                }

                // 存储映射关系
                if (!symbolToAddress.has(symbol)) {
                    symbolToAddress.set(symbol, { address, chain });
                }
            }
        }

        // 也从 story/latest 提取 BSC 地址
        if (line.includes('story/latest') && line.includes('ca_address')) {
            const caMatch = line.match(/"ca_address":"([^"]+)"/);
            const nameMatch = line.match(/"name":"([^"]+)"/);

            if (caMatch && nameMatch) {
                const address = caMatch[1];
                const name = nameMatch[1];
                const chain = detectChain(address);
                if (!symbolToAddress.has(name)) {
                    symbolToAddress.set(name, { address, chain });
                }
            }
        }
    }

    console.log(`   📋 建立了 ${symbolToAddress.size} 个 symbol -> address 映射`);

    // 第二步: 找出被过滤的token
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        // 匹配 "不入池" 的记录
        if (line.includes('不入池')) {
            // 格式: ⏭️ [Filter] Hacker 七维分 23 < 30，不入池
            const match = line.match(/\[Filter\]\s+(.+?)\s+七维分\s+(\d+)/);
            if (match) {
                const symbol = match[1].trim();
                const score = parseInt(match[2]);

                // 从映射表获取地址
                const addressInfo = symbolToAddress.get(symbol);

                if (addressInfo && !filteredTokens.has(addressInfo.address)) {
                    // 向上搜索获取更多信息 (市值、CrossValidator决策等)
                    let initialMcap = null;
                    let crossValidatorScore = null;
                    let crossValidatorDecision = null;

                    for (let j = i - 1; j >= Math.max(0, i - 25); j--) {
                        const prevLine = lines[j];

                        // 提取市值
                        if (prevLine.includes('市值$') || prevLine.includes('市值大')) {
                            const mcapMatch = prevLine.match(/市值[大]*\$?([\d.]+)K/);
                            if (mcapMatch) {
                                initialMcap = parseFloat(mcapMatch[1]) * 1000;
                            }
                        }

                        // 提取 CrossValidator 分数
                        if (prevLine.includes('[CrossValidator]') && prevLine.includes('总分')) {
                            const scoreMatch = prevLine.match(/总分:\s*(\d+)/);
                            if (scoreMatch) {
                                crossValidatorScore = parseInt(scoreMatch[1]);
                            }
                        }

                        // 提取 CrossValidator 决策
                        if (prevLine.includes('[CrossValidator]') && prevLine.includes('决策')) {
                            const decMatch = prevLine.match(/决策:\s*(BUY|WATCH|SKIP)/);
                            if (decMatch) {
                                crossValidatorDecision = decMatch[1];
                            }
                        }
                    }

                    filteredTokens.set(addressInfo.address, {
                        symbol,
                        address: addressInfo.address,
                        chain: addressInfo.chain === 'solana' ? 'SOL' : addressInfo.chain.toUpperCase(),
                        score,
                        crossValidatorScore,
                        crossValidatorDecision,
                        filterReason: `七维分 ${score} < 30`,
                        logTime: new Date().toISOString(),
                        initialMcap
                    });
                }
            }
        }
    }

    return Array.from(filteredTokens.values());
}

// 提取系统买入的token信息
function extractBoughtTokensFromLog(logPath) {
    const content = fs.readFileSync(logPath, 'utf-8');
    const lines = content.split('\n');

    const boughtTokens = [];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];

        if (line.includes('毕业了')) {
            const match = line.match(/\[GRADUATE\]\s+(\S+)\s+毕业了/);
            if (match) {
                const symbol = match[1];
                boughtTokens.push({
                    symbol,
                    action: 'BOUGHT',
                    logTime: new Date().toISOString()
                });
            }
        }
    }

    return boughtTokens;
}

// 使用 GMGN API 查询token当前价格
async function fetchTokenPrice(address, chain) {
    const chainParam = chain === 'SOL' ? 'sol' : 'bsc';

    try {
        const url = `${GMGN_API_BASE}/tokens/realtime/${chainParam}/${address}`;
        const response = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
        });

        if (!response.ok) {
            return null;
        }

        const data = await response.json();
        if (data.code === 0 && data.data) {
            return {
                price: data.data.price,
                marketCap: data.data.market_cap,
                liquidity: data.data.liquidity,
                volume24h: data.data.volume_24h,
                priceChange24h: data.data.price_change_24h
            };
        }
        return null;
    } catch (error) {
        console.error(`Error fetching ${address}: ${error.message}`);
        return null;
    }
}

// 延迟函数
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// 主分析函数
async function analyzeFilteredTokens() {
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('         寻找错过的金狗 - 真实数据分析 v2');
    console.log('═══════════════════════════════════════════════════════════════\n');

    const logPath = path.join(process.cwd(), 'logs/restart_v75_20260113_002033.log');

    if (!fs.existsSync(logPath)) {
        console.error('日志文件不存在:', logPath);
        return;
    }

    // 1. 提取被过滤的token
    console.log('📋 步骤1: 提取被过滤的token...');
    const filteredTokens = extractFilteredTokensFromLog(logPath);
    console.log(`   找到 ${filteredTokens.length} 个被过滤的token\n`);

    if (filteredTokens.length > 0) {
        console.log('   前5个被过滤的token:');
        filteredTokens.slice(0, 5).forEach((t, i) => {
            console.log(`   ${i+1}. ${t.symbol} (${t.chain}) - 七维分:${t.score}, CV决策:${t.crossValidatorDecision || 'N/A'}, CV分数:${t.crossValidatorScore || 'N/A'}`);
        });
        console.log('');
    }

    // 2. 提取系统买入的token
    console.log('📋 步骤2: 提取系统买入的token...');
    const boughtTokens = extractBoughtTokensFromLog(logPath);
    console.log(`   系统买入了 ${boughtTokens.length} 个token: ${boughtTokens.map(t => t.symbol).join(', ')}\n`);

    // 3. 查询被过滤token的当前价格
    console.log('📋 步骤3: 查询被过滤token的当前价格...');
    console.log('   (使用 GMGN API，每个请求间隔 500ms)\n');

    const results = [];
    let checkedCount = 0;
    const maxCheck = Math.min(filteredTokens.length, 50); // 最多查50个

    for (const token of filteredTokens.slice(0, maxCheck)) {
        checkedCount++;
        process.stdout.write(`   [${checkedCount}/${maxCheck}] 查询 ${token.symbol}...`);

        const priceData = await fetchTokenPrice(token.address, token.chain);

        if (priceData) {
            results.push({
                ...token,
                currentMcap: priceData.marketCap,
                currentPrice: priceData.price,
                priceChange24h: priceData.priceChange24h,
                liquidity: priceData.liquidity
            });
            console.log(` MC=$${(priceData.marketCap / 1000).toFixed(1)}K, 24h: ${priceData.priceChange24h?.toFixed(1) || 'N/A'}%`);
        } else {
            results.push({
                ...token,
                currentMcap: null,
                status: 'DEAD_OR_UNAVAILABLE'
            });
            console.log(' ❌ 无法获取 (可能已死)');
        }

        await delay(500); // 避免 rate limit
    }

    // 4. 分析结果
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('📊 分析结果');
    console.log('═══════════════════════════════════════════════════════════════\n');

    // 按市值排序，找出可能的金狗
    const aliveTokens = results.filter(t => t.currentMcap);
    aliveTokens.sort((a, b) => (b.currentMcap || 0) - (a.currentMcap || 0));

    console.log('🏆 被过滤token当前市值排名 (可能的错过金狗):');
    console.log('─────────────────────────────────────────────────────────────────');

    aliveTokens.forEach((token, index) => {
        const mcapK = (token.currentMcap / 1000).toFixed(1);
        const change = token.priceChange24h ? `${token.priceChange24h.toFixed(1)}%` : 'N/A';
        const goldenTag = token.currentMcap > 500000 ? '🥇 潜在金狗' :
                          token.currentMcap > 200000 ? '🥈 潜在银狗' :
                          token.currentMcap > 100000 ? '🥉 值得关注' : '';

        console.log(`${index + 1}. ${token.symbol} (${token.chain}) ${goldenTag}`);
        console.log(`   当前市值: $${mcapK}K | 24h变化: ${change}`);
        console.log(`   七维分: ${token.score} | CV决策: ${token.crossValidatorDecision || 'N/A'} (${token.crossValidatorScore || 'N/A'}分)`);
        console.log(`   初始市值: $${token.initialMcap ? (token.initialMcap/1000).toFixed(1) + 'K' : 'N/A'}`);
        console.log(`   过滤原因: ${token.filterReason}`);
        console.log(`   合约: ${token.address.substring(0, 20)}...`);
        console.log('');
    });

    // 5. 统计分析
    const deadCount = results.filter(t => !t.currentMcap).length;
    const aliveCount = aliveTokens.length;
    const potentialGold = aliveTokens.filter(t => t.currentMcap > 500000).length;
    const potentialSilver = aliveTokens.filter(t => t.currentMcap > 200000 && t.currentMcap <= 500000).length;

    console.log('═══════════════════════════════════════════════════════════════');
    console.log('📈 统计汇总');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`检查token总数: ${results.length}`);
    console.log(`存活token: ${aliveCount} (${results.length > 0 ? (aliveCount / results.length * 100).toFixed(1) : 0}%)`);
    console.log(`已死/不可用: ${deadCount} (${results.length > 0 ? (deadCount / results.length * 100).toFixed(1) : 0}%)`);
    console.log(`潜在金狗 (MC>$500K): ${potentialGold}`);
    console.log(`潜在银狗 (MC>$200K): ${potentialSilver}`);

    // 6. 保存结果
    const outputPath = path.join(process.cwd(), 'data/missed-golden-dogs-analysis.json');
    fs.writeFileSync(outputPath, JSON.stringify({
        analysisTime: new Date().toISOString(),
        summary: {
            totalChecked: results.length,
            alive: aliveCount,
            dead: deadCount,
            potentialGold,
            potentialSilver
        },
        filteredTokens: results,
        boughtTokens
    }, null, 2));

    console.log(`\n💾 结果已保存到: ${outputPath}`);

    // 7. 关键发现
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('💡 关键发现');
    console.log('═══════════════════════════════════════════════════════════════');

    // 分析被过滤但CrossValidator说BUY的token表现
    const cvBuyButFiltered = aliveTokens.filter(t => t.crossValidatorDecision === 'BUY');
    if (cvBuyButFiltered.length > 0) {
        console.log(`\n⚠️ CrossValidator说BUY但被七维分过滤的token (共${cvBuyButFiltered.length}个):`);
        cvBuyButFiltered.slice(0, 5).forEach((t, i) => {
            console.log(`   ${i+1}. ${t.symbol}: CV说BUY(${t.crossValidatorScore}分), 七维分${t.score}过滤, 现MC=$${(t.currentMcap/1000).toFixed(1)}K`);
        });

        const cvBuyGoldCount = cvBuyButFiltered.filter(t => t.currentMcap > 500000).length;
        if (cvBuyGoldCount > 0) {
            console.log(`\n   🔥 其中 ${cvBuyGoldCount} 个涨成了金狗 (MC>$500K)!`);
            console.log('   → 说明七维分过滤太严格，可能需要调整阈值');
        }
    }

    if (potentialGold > 0) {
        console.log(`\n⚠️  发现 ${potentialGold} 个被过滤的token现在市值超过$500K`);
        console.log('   这些可能是系统错过的金狗，需要进一步分析过滤原因');
    } else if (potentialSilver > 0) {
        console.log(`\n⚠️  发现 ${potentialSilver} 个被过滤的token现在市值超过$200K`);
        console.log('   这些可能是系统错过的银狗');
    } else {
        console.log('\n✅ 大部分被过滤的token现在都已死或市值很低');
        console.log('   七维分<30的过滤规则可能是合理的');
    }
}

// 运行分析
analyzeFilteredTokens().catch(console.error);
